package hermesacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"

	"github.com/coder/acp-go-sdk"
)

const maxDeferredStreamOpens = 64

var errLifecycleResponseCancelled = errors.New("hermes lifecycle response was cancelled")

type deferredStreamOpenState uint8

const (
	deferredStreamOpenPending deferredStreamOpenState = iota + 1
	deferredStreamOpenWriting
	deferredStreamOpenCancelled
)

type deferredStreamOpen struct {
	session       *session
	token         string
	state         deferredStreamOpenState
	done          sync.Once
	afterResponse func()
	onCancel      func()
	cancelOnce    sync.Once
	agent         *Agent
}

func (o *deferredStreamOpen) finish() {
	o.done.Do(o.agent.streamOpenWait.Done)
}

func (o *deferredStreamOpen) cancel() {
	o.cancelOnce.Do(func() {
		if o.onCancel != nil {
			o.onCancel()
		}
	})
}

// deferStreamOpen records that a session's opening `lifecycle_snapshot` is owed.
// A stream's first event is ordered after the establishing `session/new`,
// `session/load`, `session/resume`, or fork response, so the snapshot is queued
// here and delivered by the write barrier that observes that response leave.
func (a *Agent) deferStreamOpen(ctx context.Context, session *session) (*deferredStreamOpen, bool, error) {
	if session.lifecycleStream() == nil {
		return nil, false, nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return nil, false, acp.NewInvalidRequest(map[string]any{jsonFieldError: agentClosedMessage})
	}

	return a.deferStreamOpenLocked(ctx, session)
}

func (a *Agent) deferStreamOpenLocked(ctx context.Context, session *session) (*deferredStreamOpen, bool, error) {
	identity, exact := ctx.Value(lifecycleRequestIdentityKey{}).(lifecycleRequestIdentity)
	if !exact || identity.token == "" {
		return nil, false, nil
	}

	a.streamOpenMu.Lock()
	defer a.streamOpenMu.Unlock()

	pending := 0

	for _, candidate := range a.streamOpens {
		if candidate.state != deferredStreamOpenCancelled {
			pending++
		}
	}

	if pending >= maxDeferredStreamOpens {
		return nil, false, acp.NewInvalidRequest(map[string]any{jsonFieldError: valBackpressure, keyLimit: "lifecycle_responses"})
	}

	owed := &deferredStreamOpen{
		session: session,
		token:   identity.token,
		state:   deferredStreamOpenPending,
		agent:   a,
	}
	a.streamOpenWait.Add(1)
	a.streamOpens = append(a.streamOpens, owed)

	return owed, true, nil
}

func (a *Agent) beginStreamOpenWrite(response []byte) error {
	token, ok := lifecycleResponseRequestToken(response)
	if !ok {
		return nil
	}

	a.streamOpenMu.Lock()
	defer a.streamOpenMu.Unlock()

	for _, candidate := range a.streamOpens {
		if candidate.token != token {
			continue
		}

		switch candidate.state {
		case deferredStreamOpenCancelled:
			return errLifecycleResponseCancelled
		case deferredStreamOpenPending:
			candidate.state = deferredStreamOpenWriting

			return nil
		case deferredStreamOpenWriting:
			continue
		}
	}

	return nil
}

// completeStreamOpenWrite releases only the opening snapshot established by
// this exact response. Delivery runs off the writing goroutine because the
// notification it sends takes the same serialized writer the caller just left.
func (a *Agent) completeStreamOpenWrite(response []byte, writeErr error) {
	token, ok := lifecycleResponseRequestToken(response)
	if !ok {
		return
	}

	a.streamOpenMu.Lock()

	var owed *deferredStreamOpen

	for index, candidate := range a.streamOpens {
		if candidate.token != token || candidate.state != deferredStreamOpenWriting {
			continue
		}

		owed = candidate

		a.streamOpens = append(a.streamOpens[:index], a.streamOpens[index+1:]...)

		break
	}
	a.streamOpenMu.Unlock()

	if owed == nil {
		return
	}

	if writeErr != nil {
		owed.session.lifecycleStream().fence()
		owed.cancel()
		owed.finish()

		return
	}

	go func() {
		defer owed.finish()
		defer owed.cancel()
		defer recoverAgentGoroutine(context.Background(), a.log, "Hermes lifecycle stream open")

		a.openDeferredStream(owed.session)

		if owed.afterResponse != nil {
			owed.afterResponse()
		}
	}()
}

func (a *Agent) cancelStreamOpens() {
	a.streamOpenMu.Lock()
	cancelled := make([]*deferredStreamOpen, 0, len(a.streamOpens))

	for _, owed := range a.streamOpens {
		if owed.state != deferredStreamOpenPending {
			continue
		}

		owed.state = deferredStreamOpenCancelled
		cancelled = append(cancelled, owed)
	}
	a.streamOpenMu.Unlock()

	for _, owed := range cancelled {
		owed.session.lifecycleStream().fence()
		owed.cancel()
		owed.finish()
	}
}

func (a *Agent) abandonStreamOpen(owed *deferredStreamOpen) {
	if owed == nil {
		return
	}

	a.streamOpenMu.Lock()
	for index, candidate := range a.streamOpens {
		if candidate != owed {
			continue
		}

		a.streamOpens = append(a.streamOpens[:index], a.streamOpens[index+1:]...)

		break
	}
	a.streamOpenMu.Unlock()
	owed.cancel()
	owed.finish()
}

func lifecycleResponseRequestToken(frame []byte) (string, bool) {
	var response struct {
		Method string                     `json:"method"`
		Result map[string]json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(frame, &response); err != nil || response.Method != "" || response.Result == nil {
		return "", false
	}

	var token string
	if err := json.Unmarshal(response.Result[lifecycleRequestMarkerField], &token); err != nil || token == "" {
		return "", false
	}

	return token, true
}

// stripLifecycleResponseMarker removes the private correlation member before a
// lifecycle response crosses the ACP wire. It reports whether it transformed
// the frame so responseOrderedWriter can preserve io.Writer's input-length
// contract even though the physical write is shorter.
func stripLifecycleResponseMarker(frame []byte) ([]byte, bool, error) {
	var response map[string]json.RawMessage
	if err := json.Unmarshal(frame, &response); err != nil {
		return frame, false, nil //nolint:nilerr // Non-JSON writes carry no private marker and remain the underlying writer's concern.
	}

	var result map[string]json.RawMessage
	if err := json.Unmarshal(response["result"], &result); err != nil || result == nil {
		return frame, false, nil //nolint:nilerr // Notifications and error responses have no marked result to strip.
	}

	marker, marked := result[lifecycleRequestMarkerField]
	if !marked {
		return frame, false, nil
	}

	var token string
	if err := json.Unmarshal(marker, &token); err != nil || token == "" {
		return nil, true, errors.New("lifecycle response has malformed private identity")
	}

	delete(result, lifecycleRequestMarkerField)

	encodedResult, _ := json.Marshal(result) // Every raw member was proved valid by the enclosing frame decode.

	response["result"] = encodedResult

	clean, _ := json.Marshal(response) // The response contains only raw members from the valid enclosing frame.

	if bytes.HasSuffix(frame, []byte{'\n'}) {
		clean = append(clean, '\n')
	}

	return clean, true, nil
}

// openDeferredStream emits one owed snapshot. A stream this adapter cannot open
// truthfully is fenced by the emitter itself, so the failure is recorded and the
// session goes on without a stream rather than opening one whose first event
// lies.
func (a *Agent) openDeferredStream(session *session) {
	ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	defer cancel()

	if err := session.lifecycleStream().ensureLifecycleOpened(ctx); err != nil {
		a.log.DebugContext(ctx, "open Hermes lifecycle stream failed", slog.String("classification", "lifecycle_open_failed"))
	}
}

// awaitStreamOpens waits for every released opening snapshot to finish. Agent
// close uses it so no lifecycle notification is still in flight once the
// connection is gone.
func (a *Agent) awaitStreamOpens() {
	a.streamOpenWait.Wait()
}

// responseOrderedWriter is the narrowest ordered hook the transport allows: it
// reports each completed frame, which is what lets an opening snapshot be
// ordered after the establishing response rather than merely after the handler
// that produced it.
type responseOrderedWriter struct {
	writer    io.Writer
	starting  func([]byte) error
	completed func([]byte, error)

	mu           sync.Mutex
	closed       bool
	active       int
	drained      chan struct{}
	closeAttempt *responseWriterCloseAttempt
}

type responseWriterCloseAttempt struct {
	done chan struct{}
	err  error
}

func (o *responseOrderedWriter) Write(frame []byte) (int, error) {
	if err := o.beginWrite(); err != nil {
		return 0, io.ErrClosedPipe
	}
	defer o.endWrite()

	if o.starting != nil {
		if err := o.starting(frame); err != nil {
			return 0, err
		}
	}

	physical, transformed, err := stripLifecycleResponseMarker(frame)
	if err != nil {
		if o.completed != nil {
			o.completed(frame, err)
		}

		return 0, err
	}

	written, err := o.writer.Write(physical)
	if err == nil && written != len(physical) {
		err = io.ErrShortWrite
	}

	if o.completed != nil {
		o.completed(frame, err)
	}

	if transformed {
		if err != nil {
			return 0, err
		}

		return len(frame), nil
	}

	return written, err
}

func (o *responseOrderedWriter) beginWrite() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return io.ErrClosedPipe
	}

	if o.active == 0 {
		o.drained = make(chan struct{})
	}

	o.active++

	return nil
}

func (o *responseOrderedWriter) endWrite() {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.active--

	if o.active == 0 {
		close(o.drained)
		o.drained = nil
	}
}

func (o *responseOrderedWriter) PrepareClose(ctx context.Context) error {
	o.mu.Lock()
	if o.active == 0 {
		o.mu.Unlock()

		return nil
	}

	drained := o.drained
	closer, _ := o.writer.(io.Closer)
	o.mu.Unlock()

	select {
	case <-drained:
		return nil
	case <-ctx.Done():
	}

	var abortErr error
	if closer != nil {
		abortErr = closer.Close()
	}

	<-drained

	return abortErr
}

// Close interrupts the transport-owned writer when it supports Close, then
// joins every response write that was admitted before the close.
func (o *responseOrderedWriter) Close(ctx context.Context) error {
	if o == nil {
		return nil
	}

	o.mu.Lock()
	if attempt := o.closeAttempt; attempt != nil {
		o.mu.Unlock()
		<-attempt.done

		return attempt.err
	}

	attempt := &responseWriterCloseAttempt{done: make(chan struct{})}
	o.closeAttempt = attempt
	o.closed = true

	closer, _ := o.writer.(io.Closer)

	active := o.active
	drained := o.drained
	o.mu.Unlock()

	if closer != nil {
		attempt.err = closer.Close()
	}

	if active != 0 {
		<-drained
	}

	attempt.err = errors.Join(attempt.err, ctx.Err())

	o.mu.Lock()
	close(attempt.done)
	o.mu.Unlock()

	return attempt.err
}
