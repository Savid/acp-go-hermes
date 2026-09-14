package hermesacp

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/coder/websocket"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/process"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-hermes/internal/hermes"
)

const (
	sessionAbortTimeout    = 5 * time.Second
	sessionSettleTimeout   = 60 * time.Second
	sessionShutdownTimeout = 10 * time.Second
	sessionShutdownGrace   = 2 * time.Second
	stderrTailBytes        = 8 << 10
)

// session owns one native conversation and at most one serve generation.
type session struct {
	agent                 *Agent
	id                    acp.SessionId
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
	closing               bool
	closeDone             chan struct{}
	closeErr              error
	poison                string
	turn                  *turn
	cycle                 *cycle
	dialogs               map[string]*dialog
	callbacks             sync.WaitGroup
	mirrorMu              sync.Mutex
	lcMu                  sync.Mutex
	lc                    lifecycleState
}

// runtime binds one gateway connection to its native live-session identity.
type runtime struct {
	proc         *process.Process
	client       *hermes.Client
	endpoint     hermes.Endpoint
	liveID       string
	stderr       *stderrTail
	cancel       context.CancelFunc
	bound        chan struct{}
	bindOnce     sync.Once
	done         chan struct{}
	controls     chan func()
	controlsDone chan struct{}
}

type cycle struct {
	turnID  string
	cycleID string
	origin  lifecycle.Cause
	state   cycleState
	failure error
	done    chan struct{}
}

type turnEnd int

const (
	turnRunning turnEnd = iota
	turnSettled
	turnTransportEnded
)

type turn struct {
	cycle
	submission  lifecycle.Submission
	accepted    bool
	cancelled   bool
	timedOut    bool
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

// stderrTail retains the last bytes hermes wrote to stderr.
type stderrTail struct {
	mu   sync.Mutex
	data []byte
}

func (t *stderrTail) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.data = append(t.data, p...)
	if len(t.data) > stderrTailBytes {
		t.data = t.data[len(t.data)-stderrTailBytes:]
	}

	return len(p), nil
}

// lastLine is the final non-empty stderr line, which is where a dying harness
// names its reason.
func (t *stderrTail) lastLine() string {
	t.mu.Lock()
	defer t.mu.Unlock()

	lines := strings.Split(strings.TrimSpace(string(t.data)), "\n")

	return strings.TrimSpace(lines[len(lines)-1])
}

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
		return nil, s.startFailure(ctx, seedErr)
	}

	proc, err := process.Start(ctx, process.Request{Executable: executable, Args: endpoint.Args(), Env: env, Dir: s.cwd})
	if err != nil {
		return nil, s.startFailure(ctx, err)
	}

	stderr := &stderrTail{}
	go func() { _, _ = io.Copy(stderr, proc.Stderr()) }()
	go func() { _, _ = io.Copy(io.Discard, proc.Stdout()) }()

	readyCtx, readyCancel := context.WithTimeout(ctx, sessionSettleTimeout)
	defer readyCancel()

	client, err := endpoint.Connect(readyCtx)
	if err != nil {
		_ = proc.Kill()
		_ = proc.Close()

		return nil, s.startFailure(ctx, err)
	}

	for {
		select {
		case delivery, ok := <-client.Deliveries():
			if !ok || delivery.Err != nil {
				_ = client.Close(websocket.StatusNormalClosure, "startup failed")
				_ = proc.Kill()
				_ = proc.Close()

				return nil, s.startFailure(ctx, errors.New("gateway closed before readiness"))
			}

			if delivery.Event != nil && delivery.Event.Type == "gateway.ready" {
				goto ready
			}
		case <-readyCtx.Done():
			_ = client.Close(websocket.StatusNormalClosure, "startup timed out")
			_ = proc.Kill()
			_ = proc.Close()

			return nil, s.startFailure(ctx, readyCtx.Err())
		}
	}

ready:
	readCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))

	rt := &runtime{proc: proc, client: client, endpoint: endpoint, stderr: stderr, cancel: cancel, bound: make(chan struct{}), done: make(chan struct{}), controls: make(chan func(), 256), controlsDone: make(chan struct{})}

	s.mu.Lock()
	s.runtime = rt
	s.mu.Unlock()

	go func() {
		defer close(rt.controlsDone)

		for job := range rt.controls {
			job()
		}
	}()
	go s.pump(readCtx, rt)

	return rt, nil
}

func (s *session) startFailure(ctx context.Context, err error) error {
	s.agent.log.ErrorContext(ctx, "Hermes session start failed", slog.String("reason", err.Error()))

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

	if err := rt.client.AwaitSessionBuild(ctx, rt.liveID); err != nil {
		return s.startFailure(ctx, err)
	}

	if model != "" {
		if err := rt.client.SetModel(ctx, rt.liveID, model); err != nil {
			return wire.Unsupported(metaOptionPath(metaModelKey))
		}
	}

	if s.options.Effort != "" {
		if _, err := rt.client.SetReasoning(ctx, rt.liveID, s.options.Effort); err != nil {
			return wire.Unsupported(metaOptionPath(metaEffortKey))
		}
	}

	return s.refreshModels(ctx, rt)
}

func (s *session) ensureRuntime(ctx context.Context) (*runtime, error) {
	s.mu.Lock()
	rt := s.runtime
	s.mu.Unlock()

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

	if err := s.configureRuntime(ctx, rt, s.model, string(s.id)); err != nil {
		s.stopRuntime(context.WithoutCancel(ctx), rt)

		return nil, err
	}

	if err := s.publishOpen(ctx); err != nil {
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
	current := s.runtime == rt
	s.mu.Unlock()

	if !current || (closing && t == nil && c == nil) {
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

		if t.disposition == promptQueued && event.Type == "message.start" && event.InboundSequence > t.watermark {
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
		if err != nil && t.failure == nil {
			t.failure = err
		}

		if settled {
			s.cancelDialogs()
			s.callbacks.Wait()
			t.settle(turnSettled)

			select {
			case <-t.finished:
			case <-ctx.Done():
			}
		}

		return
	}

	if c == nil && bearsWork(event) {
		c = &cycle{origin: lifecycle.CauseActivity, done: make(chan struct{})}
		if err := s.lcOpenAgentCycle(ctx, c); err != nil {
			c.failure = err
		}

		s.mu.Lock()
		s.cycle = c
		s.mu.Unlock()
	}

	if c == nil {
		return
	}

	settled, err := s.projectEvent(ctx, rt, c, event)
	if err != nil && c.failure == nil {
		c.failure = err
	}

	if settled {
		settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionSettleTimeout)
		defer cancel()

		s.cancelDialogs()
		s.callbacks.Wait()
		s.emitUsage(settleCtx, &c.state)

		if err := s.commitMirror(settleCtx); err != nil {
			c.failure = s.mirrorFailure(err)
		}

		_ = s.lcIdle(settleCtx, c, judgeCycle(c, false))
		close(c.done)
		s.mu.Lock()
		if s.cycle == c {
			s.cycle = nil
		}
		s.mu.Unlock()
	}
}

func (s *session) runtimeEnded(ctx context.Context, rt *runtime) {
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionShutdownTimeout)
	defer cancel()

	if err := rt.proc.Shutdown(shutdownCtx, sessionShutdownGrace); err != nil {
		_ = rt.proc.Kill()
	}

	s.mu.Lock()
	if s.runtime != rt {
		s.mu.Unlock()

		return
	}

	s.runtime = nil
	t, c, closing := s.turn, s.cycle, s.closing
	s.cycle = nil
	s.mu.Unlock()
	s.cancelDialogs()
	close(rt.controls)
	<-rt.controlsDone
	s.callbacks.Wait()

	if t != nil {
		t.settle(turnTransportEnded)

		return
	}

	if c != nil {
		verdict := cycleVerdict{outcome: lifecycle.OutcomeFailed}
		if closing {
			verdict = cycleVerdict{outcome: lifecycle.OutcomeCancelled, stopReason: lifecycle.StopReasonCancelled}
		}

		_ = s.lcIdle(ctx, c, verdict)
		close(c.done)
	}

	if !closing {
		s.lcFence()
	}
}

func (s *session) stopRuntime(ctx context.Context, rt *runtime) {
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionShutdownTimeout)
	defer cancel()

	_ = rt.client.Close(websocket.StatusNormalClosure, "closing")
	rt.cancel()

	if err := rt.proc.Shutdown(shutdownCtx, sessionShutdownGrace); err != nil {
		_ = rt.proc.Kill()
	}

	select {
	case <-rt.done:
	case <-shutdownCtx.Done():
	}

	_ = rt.proc.Close()
}

// abort interrupts the native run under a bounded context detached from the
// caller's cancellation.
func (s *session) abort(ctx context.Context, rt *runtime) {
	abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionAbortTimeout)
	defer cancel()

	if err := rt.client.Interrupt(abortCtx, rt.liveID); err != nil {
		s.agent.log.DebugContext(abortCtx, "hermes abort failed", slog.String(nativeSessionIDKey, string(s.id)))
	}
}

// cancel implements session/cancel: it cancels the in-flight turn, resolves
// its pending dialogs, and interrupts hermes. It is a silent no-op with no turn.
func (s *session) cancel(ctx context.Context) {
	s.mu.Lock()
	t := s.turn
	rt := s.runtime

	if t == nil || t.cancelled {
		s.mu.Unlock()

		return
	}

	t.cancelled = true
	s.mu.Unlock()

	s.cancelDialogs()

	if rt != nil {
		s.abort(ctx, rt)
	}
}

// timeout ends a turn that exceeded the configured deadline.
func (s *session) timeout(ctx context.Context, t *turn) {
	s.mu.Lock()
	rt := s.runtime

	if s.turn != t || t.cancelled || t.timedOut {
		s.mu.Unlock()

		return
	}

	t.timedOut = true
	s.mu.Unlock()

	s.cancelDialogs()

	if rt != nil {
		s.abort(ctx, rt)

		select {
		case <-t.settled:
		case <-time.After(sessionAbortTimeout):
			rt.cancel()
			_ = rt.proc.Kill()
		}
	}
}

func (s *session) registerDialog(id string, cancel context.CancelCauseFunc) func() {
	s.mu.Lock()
	if s.closing || s.runtime == nil || (s.turn != nil && (s.turn.cancelled || s.turn.timedOut)) {
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
		slog.String(nativeSessionIDKey, string(s.id)), slog.String("cause", cause))
}

// acquireGate admits one foreground operation. limit names the backpressure
// token a refusal carries.
func (s *session) acquireGate(limit string) (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closing {
		return nil, wire.UnknownSession()
	}

	select {
	case s.gate <- struct{}{}:
		return func() { <-s.gate }, nil
	default:
		return nil, wire.Backpressure(limit)
	}
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
	s.closeDone = make(chan struct{})

	t, rt, closingCycle := s.turn, s.runtime, s.cycle
	if t != nil {
		t.cancelled = true
	}
	s.mu.Unlock()
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

	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionSettleTimeout)
	defer cancel()

	var errs []error

	if rt != nil {
		if err := s.commitMirror(commitCtx); err != nil {
			errs = append(errs, err)
		}

		s.stopRuntime(commitCtx, rt)
	}

	s.lcFence()
	s.mu.Lock()
	s.closeErr = errors.Join(errs...)
	close(s.closeDone)
	s.mu.Unlock()

	return s.closeErr
}
