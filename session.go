package hermesacp

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/coder/websocket"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/observer"
	"github.com/savid/acp-go-core/process"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-hermes/internal/hermes"
)

const (
	sessionAbortTimeout    = 5 * time.Second
	sessionSettleTimeout   = 60 * time.Second
	sessionShutdownTimeout = 10 * time.Second
	sessionShutdownGrace   = 2 * time.Second
)

// session owns one native conversation and at most one serve generation.
type session struct {
	agent                 *Agent
	id                    acp.SessionId
	nativeID              string
	cwd                   string
	additionalDirectories []string
	options               HermesOptions
	rawEvents             *wire.RawEvents
	agentDir              string
	gate                  chan struct{}
	mu                    sync.Mutex
	runtime               *runtime
	model                 string
	effort                string
	models                hermes.ModelOptionsResult
	title                 string
	updatedAt             string
	// persisted marks a successfully committed mirror.
	persisted bool
	closing   bool
	closeDone chan struct{}
	closeErr  error
	poison    string
	turn      *turn
	cycle     *cycle
	dialogs   map[string]*dialog
	callbacks sync.WaitGroup
	openMu    sync.Mutex
	mirrorMu  sync.Mutex
	lcMu      sync.Mutex
	lc        lifecycle.Publisher
}

// runtime binds one gateway connection to its native live-session identity.
type runtime struct {
	// ending prevents another operation from using this runtime during teardown.
	ending       bool
	proc         *process.Process
	observe      *observer.Observer
	client       *hermes.Client
	endpoint     hermes.Endpoint
	liveID       string
	cancel       context.CancelFunc
	bound        chan struct{}
	bindOnce     sync.Once
	done         chan struct{}
	controls     chan func()
	controlsDone chan struct{}
}

type cycle struct {
	lifecycle.Cycle
	cancelled bool
	settling  bool
	terminal  bool
	state     cycleState
	failure   error
	done      chan struct{}
}

type turnEnd int

const (
	turnRunning turnEnd = iota
	turnSettled
	turnTransportEnded
)

type turn struct {
	cycle
	submission lifecycle.Submission
	// cancel ends this turn's own context; only cancel, timeout, and close
	// call it, so a peer prompt's refusal cannot end a live turn.
	cancel      context.CancelFunc
	accepted    bool
	ended       turnEnd
	settled     chan struct{}
	settleOnce  sync.Once
	finished    chan struct{}
	ready       chan struct{}
	disposition string
	watermark   uint64
	floor       uint64
	ownsEvents  bool
}

func (t *turn) settle(end turnEnd) {
	t.settleOnce.Do(func() { t.ended = end; close(t.settled) })
}

type dialog struct{ cancel context.CancelCauseFunc }

var errDialogCancelled = errors.New("dialog cancelled by the session")

// launch starts the authenticated loopback transport in the session's native home.
func (s *session) launch(ctx context.Context) (*runtime, error) {
	executable, err := s.agent.ensureExecutable(ctx)
	if err != nil {
		return nil, err
	}

	endpoint, err := hermes.NewEndpoint()
	if err != nil {
		return nil, s.startFailure(ctx, err)
	}

	environment := s.agent.environment(s.options.Env, map[string]string{hermes.EnvSessionToken: endpoint.Token})
	environment.ExtraPathDirs = s.options.ExtraPathDirs

	env, err := environment.Build()
	if err != nil {
		return nil, err
	}

	if seedErr := process.WriteSeedFiles(s.agentDir, s.agent.options.SeedFiles); seedErr != nil {
		if refusal := wire.SeedFileRefusal(seedErr); refusal != nil {
			return nil, refusal
		}

		return nil, s.startFailure(ctx, seedErr)
	}

	proc, err := process.Start(ctx, process.Request{Executable: executable, Args: endpoint.Args(), Env: env, Dir: s.cwd})
	if err != nil {
		return nil, s.startFailure(ctx, err)
	}

	go func() { _, _ = io.Copy(io.Discard, proc.Stdout()) }()

	readyCtx, readyCancel := context.WithTimeout(ctx, sessionSettleTimeout)
	defer readyCancel()

	// A child that is already gone can never dial or answer, so its exit ends
	// the readiness window instead of leaving it to the settle timeout.
	watching := make(chan struct{})
	defer close(watching)

	go func() {
		select {
		case <-proc.Done():
			readyCancel()
		case <-watching:
		}
	}()

	client, err := endpoint.Connect(readyCtx)
	if err != nil {
		_ = proc.Kill()
		_ = proc.Close()

		return nil, s.startFailure(ctx, launchFailure(proc, err))
	}

	for {
		select {
		case delivery, ok := <-client.Deliveries():
			if !ok || delivery.Err != nil {
				_ = client.Close(websocket.StatusNormalClosure, "startup failed")
				_ = proc.Kill()
				_ = proc.Close()

				return nil, s.startFailure(ctx, launchFailure(proc, errors.New("gateway closed before readiness")))
			}

			if delivery.Event != nil && delivery.Event.Type == "gateway.ready" {
				goto ready
			}
		case <-readyCtx.Done():
			_ = client.Close(websocket.StatusNormalClosure, "startup timed out")
			_ = proc.Kill()
			_ = proc.Close()

			return nil, s.startFailure(ctx, launchFailure(proc, readyCtx.Err()))
		}
	}

ready:
	readCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))

	rt := &runtime{proc: proc, observe: s.agent.observe, client: client, endpoint: endpoint, cancel: cancel, bound: make(chan struct{}), done: make(chan struct{}), controls: make(chan func(), 256), controlsDone: make(chan struct{})}

	s.mu.Lock()
	closing := s.closing

	if !closing {
		s.runtime = rt
	}
	s.mu.Unlock()

	// A close that began during this launch has already sampled the runtime it
	// stops, so a process bound now would outlive the session.
	if closing {
		_ = client.Close(websocket.StatusNormalClosure, "closing")

		cancel()
		s.reap(ctx, proc, s.agent.observe)

		_ = proc.Close()

		return nil, wire.UnknownSession()
	}

	go func() {
		defer close(rt.controlsDone)

		for job := range rt.controls {
			job()
		}
	}()
	go s.pump(readCtx, rt)

	return rt, nil
}

// launchFailure names the real reason a startup ended: a child that is already
// gone, otherwise the transport error the caller observed.
func launchFailure(proc *process.Process, err error) error {
	select {
	case <-proc.Done():
		reason := "hermes exited before the gateway was ready"
		if tail := proc.StderrTail(); tail != "" {
			reason += ": " + tail
		}

		return errors.New(reason)
	default:
		return err
	}
}

func (s *session) startFailure(ctx context.Context, err error) error {
	s.agent.log.ErrorContext(ctx, "hermes session start failed", slog.String("reason", err.Error()))

	return wire.InternalFailure(vendor, internalClassNativeStart)
}

// configureRuntime binds the stored identity and applies session-scoped native options.
func (s *session) configureRuntime(ctx context.Context, rt *runtime, model, expectID string) error {
	if expectID == "" {
		created, err := rt.client.CreateSession(ctx, map[string]any{fieldCwd: s.cwd, fieldSource: nativeSource, "close_on_disconnect": true})
		if err != nil {
			return s.startFailure(ctx, err)
		}

		rt.liveID = created.SessionID

		s.nativeID = created.StoredSessionID

		s.id = acp.SessionId(created.StoredSessionID)
		if s.id == "" || rt.liveID == "" {
			return s.startFailure(ctx, errors.New("native session identity missing"))
		}

		if err := s.persistDraft(ctx, rt); err != nil {
			return s.startFailure(ctx, err)
		}
	} else {
		restored, err := rt.client.ResumeSession(ctx, expectID, map[string]any{fieldSource: nativeSource, "close_on_disconnect": true})
		if err != nil {
			return s.agent.restoreRefused(ctx, s.id, err)
		}

		if restored.StoredKey() != expectID {
			return s.agent.restoreRefused(ctx, s.id, errors.New("native session identity changed"))
		}

		rt.liveID = restored.SessionID
		if err := rt.client.Call(ctx, "session.cwd.set", map[string]any{nativeSessionIDKey: rt.liveID, fieldCwd: s.cwd}, nil); err != nil {
			return s.startFailure(ctx, err)
		}
	}

	if rt.liveID == "" || s.id == "" {
		return s.startFailure(ctx, errors.New("native session identity missing"))
	}

	if model != "" {
		if err := rt.client.SetModel(ctx, rt.liveID, model); err != nil {
			return wire.Unsupported(wire.MetaOptionPath(vendor, metaModelKey))
		}
	}

	if err := rt.client.AwaitSessionBuild(ctx, rt.liveID); err != nil {
		return s.startFailure(ctx, err)
	}

	if s.options.Effort != "" {
		if _, err := rt.client.SetReasoning(ctx, rt.liveID, s.options.Effort); err != nil {
			return wire.Unsupported(wire.MetaOptionPath(vendor, metaEffortKey))
		}
	}

	return s.refreshModels(ctx, rt)
}

func (s *session) ensureRuntime(ctx context.Context) (*runtime, error) {
	s.mu.Lock()
	rt := s.runtime
	ending := rt != nil && rt.ending
	s.mu.Unlock()

	if ending {
		select {
		case <-rt.done:
			return s.ensureRuntime(ctx)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if rt != nil {
		return rt, nil
	}

	stored, err := s.agent.loadStored(ctx, s.id)
	if err != nil {
		return nil, err
	}

	if !stored.found {
		return nil, wire.RestoreFailed(vendor)
	}

	rt, err = s.launch(ctx)
	if err != nil {
		return nil, err
	}

	if _, err := s.hydrate(ctx, rt, stored); err != nil {
		s.stopRuntime(context.WithoutCancel(ctx), rt)

		return nil, err
	}

	if err := s.configureRuntime(ctx, rt, s.model, s.nativeID); err != nil {
		s.stopRuntime(context.WithoutCancel(ctx), rt)

		return nil, err
	}

	if err := s.openStream(ctx, rt); err != nil {
		s.stopRuntime(context.WithoutCancel(ctx), rt)

		return nil, err
	}

	return rt, nil
}

// pump preserves gateway order and drops records for every unbound live identity.
func (s *session) pump(ctx context.Context, rt *runtime) {
	defer close(rt.done)

	select {
	case <-rt.bound:
	case <-ctx.Done():
		s.runtimeEnded(ctx, rt)

		return
	}

	for {
		select {
		case delivery, ok := <-rt.client.Deliveries():
			if !ok || delivery.Err != nil {
				s.runtimeEnded(ctx, rt)

				return
			}

			if delivery.Event != nil && delivery.Event.SessionID == rt.liveID {
				s.handleEvent(ctx, rt, *delivery.Event)
			}
		case <-ctx.Done():
			s.runtimeEnded(ctx, rt)

			return
		}
	}
}

func (s *session) handleEvent(ctx context.Context, rt *runtime, event hermes.Event) {
	s.emitRawEvent(ctx, event)
	s.mu.Lock()
	t, c, closing := s.turn, s.cycle, s.closing
	pending := t
	current := s.runtime == rt
	s.mu.Unlock()

	if !current || (closing && t == nil && c == nil) {
		return
	}

	if event.Type == "request.cancel" {
		id := rt.liveID + ":" + hermes.String(event.Payload, "id")

		s.mu.Lock()
		d := s.dialogs[id]
		s.mu.Unlock()

		if d != nil {
			d.cancel(errDialogCancelled)
		}

		return
	}

	if t != nil {
		select {
		case <-t.ready:
		case <-ctx.Done():
			return
		}

		if t.disposition == promptStreaming && event.InboundSequence > t.floor {
			t.ownsEvents = true
		}

		if t.disposition == promptQueued && event.Type == eventMessageStart && event.InboundSequence > t.watermark {
			if c != nil {
				s.recordFailure(c, wire.TurnFailed(vendor, wire.TurnFailure{Cause: wire.CauseTransport, Message: "queued prompt started before the active cycle settled"}))
				s.dropRuntime(rt)

				return
			}

			t.ownsEvents = true
		}

		if !t.ownsEvents {
			t = nil
		}
	}

	if t != nil {
		if bearsWork(event) {
			s.acceptTurn(ctx, t)
		}

		settled, err := s.projectEvent(ctx, rt, &t.cycle, event)
		s.recordFailure(&t.cycle, err)

		if settled {
			s.beginSettlement(&t.cycle)
			t.settle(turnSettled)

			select {
			case <-t.finished:
			case <-ctx.Done():
			}
		}

		return
	}

	if c == nil && bearsWork(event) {
		c = s.openAgentCycle(ctx, rt, pending)
	}

	if c == nil {
		return
	}

	settled, err := s.projectEvent(ctx, rt, c, event)
	s.recordFailure(c, err)

	if settled {
		settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionSettleTimeout)
		defer cancel()

		s.beginSettlement(c)
		s.emitUsage(settleCtx, &c.state)

		if err := s.commitMirror(settleCtx, rt); err != nil {
			s.recordFailure(c, s.mirrorFailure(err))
			s.fenceStream()
			s.dropRuntime(rt)
		}

		verdict := s.judgeCycle(c, s.claimCancellation(c))
		_ = s.lc.Idle(settleCtx, c.Cycle, verdict.stopReason, verdict.outcome)
		close(c.done)
		s.mu.Lock()
		if s.cycle == c {
			s.cycle = nil
		}
		s.mu.Unlock()
	}
}

// openAgentCycle reserves native work while a queued request, if any, waits
// for its own start. A newly installed prompt cannot share that reservation.
func (s *session) openAgentCycle(ctx context.Context, rt *runtime, pending *turn) *cycle {
	c := &cycle{Cycle: s.lc.NewAgentCycle(), done: make(chan struct{})}
	s.mu.Lock()

	waiting := pending != nil && s.turn == pending && pending.disposition == promptQueued && !pending.accepted && !pending.ownsEvents
	if (s.turn != nil && !waiting) || s.cycle != nil || s.closing || s.runtime != rt {
		s.mu.Unlock()

		return nil
	}

	s.cycle = c
	s.mu.Unlock()
	s.recordFailure(c, s.lc.OpenAgentCycle(ctx, c.Cycle))
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.runtime != rt || s.cycle != c || s.closing {
		return nil
	}

	return c
}

func (s *session) runtimeEnded(ctx context.Context, rt *runtime) {
	s.reap(ctx, rt.proc, rt.observe)

	s.mu.Lock()
	if s.runtime != rt {
		s.mu.Unlock()

		return
	}

	rt.ending = true
	t, c, closing := s.turn, s.cycle, s.closing
	s.cycle = nil
	s.mu.Unlock()
	s.cancelDialogs()
	close(rt.controls)
	<-rt.controlsDone
	s.callbacks.Wait()

	if c != nil {
		verdict := cycleVerdict{outcome: lifecycle.OutcomeFailed}
		if closing {
			verdict = cycleVerdict{outcome: lifecycle.OutcomeCancelled, stopReason: lifecycle.StopReasonCancelled}
		}

		_ = s.lc.Idle(ctx, c.Cycle, verdict.stopReason, verdict.outcome)
		close(c.done)
	}

	// A turn still running settles as transport-ended and fences the stream
	// itself once it has published its terminal idle. A turn that already
	// reached its terminal result publishes nothing more, so the generation is
	// fenced here.
	if t != nil {
		select {
		case <-t.settled:
		default:
			s.mu.Lock()
			if s.runtime == rt {
				s.runtime = nil
			}
			s.mu.Unlock()

			t.settle(turnTransportEnded)

			_ = rt.proc.Close()

			return
		}
	}

	s.openMu.Lock()
	s.mu.Lock()
	if s.runtime == rt {
		if !s.closing {
			s.lc.Fence()
		}

		s.runtime = nil
	}
	s.mu.Unlock()
	s.openMu.Unlock()

	_ = rt.proc.Close()
}

// dropRuntime releases the binding from inside the pump: the gateway is closed
// and the read context cancelled, so the pump's own exit reaps the child and
// the next operation relaunches.
func (s *session) dropRuntime(rt *runtime) {
	_ = rt.client.Close(websocket.StatusNormalClosure, "closing")
	rt.cancel()
}

func (s *session) stopRuntime(ctx context.Context, rt *runtime) {
	_ = rt.client.Close(websocket.StatusNormalClosure, "closing")
	rt.cancel()
	s.reap(ctx, rt.proc, rt.observe)

	joinCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionShutdownTimeout)
	defer cancel()

	select {
	case <-rt.done:
	case <-joinCtx.Done():
	}

	_ = rt.proc.Close()
}

// reap ends one hermes process: the group is signalled and the root is waited
// for within the shutdown bound, killed when it does not stop in time.
func (s *session) reap(ctx context.Context, proc *process.Process, observe *observer.Observer) {
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionShutdownTimeout)
	defer cancel()

	if err := proc.Shutdown(shutdownCtx, sessionShutdownGrace); err != nil {
		_ = proc.Kill()
	}

	_, waitErr := proc.Wait(shutdownCtx)
	observe.RecordProcessExit(ctx, "exited", waitErr)
}

// abort interrupts the native run under a bounded context detached from the
// caller's cancellation.
func (s *session) abort(ctx context.Context, rt *runtime) {
	abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionAbortTimeout)
	defer cancel()

	if err := rt.client.Interrupt(abortCtx, rt.liveID); err != nil {
		s.agent.log.DebugContext(abortCtx, "hermes abort failed", slog.String("session_id", string(s.id)))
	}
}

// cancel marks the foreground cancelled, ends its dialogs, and interrupts
// native work. The interrupt is joined by the session's shutdown ladder.
func (s *session) cancel(ctx context.Context) {
	s.mu.Lock()
	t, c := s.turn, s.cycle
	rt := s.runtime

	if t != nil {
		if c == nil {
			c = &t.cycle
		} else {
			t.cancelled = true
		}
	}

	if c == nil || c.cancelled || c.terminal {
		s.mu.Unlock()

		if t != nil && c != &t.cycle {
			t.cancel()
		}

		return
	}

	c.cancelled = true

	interrupt := !c.settling && rt != nil && !s.closing && !rt.ending
	if interrupt {
		s.callbacks.Add(1)
	}
	s.mu.Unlock()

	if t != nil {
		t.cancel()
	}

	s.cancelDialogs()

	if interrupt {
		go func() {
			defer s.callbacks.Done()

			s.abort(ctx, rt)
		}()
	}
}

// beginSettlement closes callback admission before joining native interrupts
// and dialogs, so none can reach a later foreground on this runtime.
func (s *session) beginSettlement(c *cycle) {
	s.mu.Lock()
	c.settling = true
	s.mu.Unlock()
	s.cancelDialogs()
	s.callbacks.Wait()
}

// claimCancellation fixes the cancellation verdict before terminal delivery.
func (s *session) claimCancellation(c *cycle) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	c.terminal = true

	return c.cancelled
}

// cycleCancelled reads cancellation under the foreground admission lock.
func (s *session) cycleCancelled(c *cycle) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return c.cancelled
}

func (s *session) registerDialog(id string, cancel context.CancelCauseFunc) func() {
	s.mu.Lock()
	if s.closing || s.runtime == nil || ((s.turn != nil && (s.turn.cancelled || s.turn.settling)) || (s.cycle != nil && (s.cycle.cancelled || s.cycle.settling))) {
		s.mu.Unlock()
		cancel(errDialogCancelled)

		return func() {}
	}

	if s.dialogs == nil {
		s.dialogs = make(map[string]*dialog)
	}

	s.callbacks.Add(1)

	entry := &dialog{cancel: cancel}
	s.dialogs[id] = entry
	s.mu.Unlock()

	return sync.OnceFunc(func() {
		defer s.callbacks.Done()

		s.mu.Lock()
		if s.dialogs[id] == entry {
			delete(s.dialogs, id)
		}
		s.mu.Unlock()
	})
}

func (s *session) cancelDialogs() {
	s.mu.Lock()

	dialogs := make([]*dialog, 0, len(s.dialogs))
	for _, d := range s.dialogs {
		dialogs = append(dialogs, d)
	}
	s.mu.Unlock()

	for _, d := range dialogs {
		d.cancel(errDialogCancelled)
	}
}

// admissionError reports why a session admits no further work: it is closing
// or poisoned.
func (s *session) admissionError() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch {
	case s.poison != "":
		return wire.SessionPoisoned(vendor, s.poison)
	case s.closing:
		return wire.UnknownSession()
	default:
		return nil
	}
}

// poisonSession fences every operation but close and delete.
func (s *session) poisonSession(ctx context.Context, cause string) {
	s.mu.Lock()

	first := s.poison == ""
	if first {
		s.poison = cause
	}
	s.mu.Unlock()

	if !first {
		return
	}

	s.agent.log.ErrorContext(ctx, "hermes session poisoned",
		slog.String("session_id", string(s.id)), slog.String("cause", cause))
}

// acquireGate admits one foreground operation. limit names the backpressure
// token a refusal carries.
func (s *session) acquireGate(limit string) (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closing {
		return nil, wire.UnknownSession()
	}

	return wire.AcquireSessionGate(s.gate, limit)
}

// close interrupts and joins session work, captures native state, then stops its server.
func (s *session) close(ctx context.Context) error {
	s.mu.Lock()
	if s.closing {
		done := s.closeDone
		s.mu.Unlock()
		<-done

		return s.closeErr
	}

	s.closing = true
	joinEstablishment := !s.persisted
	s.closeDone = make(chan struct{})

	t, rt, closingCycle := s.turn, s.runtime, s.cycle
	if t != nil {
		t.cancelled = true
	}
	s.mu.Unlock()

	if t != nil {
		t.cancel()
	}

	s.cancelDialogs()
	s.callbacks.Wait()

	if t != nil {
		if rt != nil {
			s.abort(ctx, rt)
		}

		select {
		case <-t.finished:
		case <-time.After(sessionSettleTimeout):
		}
	}

	if closingCycle != nil && rt != nil {
		s.abort(ctx, rt)

		select {
		case <-closingCycle.done:
		case <-time.After(sessionAbortTimeout):
			s.stopRuntime(ctx, rt)
		}
	}

	// An initial mirror may not have started yet; its establishment owns the gate.
	if joinEstablishment {
		s.gate <- struct{}{}
		defer func() { <-s.gate }()
	}

	s.mu.Lock()
	persisted := s.persisted
	s.mu.Unlock()

	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionSettleTimeout)
	defer cancel()

	var errs []error

	if rt != nil {
		if persisted {
			if err := s.commitMirror(commitCtx, rt); err != nil {
				errs = append(errs, err)
			}
		}

		s.stopRuntime(commitCtx, rt)
	}

	s.fenceStream()
	s.mu.Lock()
	s.closeErr = errors.Join(errs...)
	close(s.closeDone)
	s.mu.Unlock()

	return s.closeErr
}
