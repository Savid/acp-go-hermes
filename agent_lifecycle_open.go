package hermesacp

import (
	"context"
	"io"
	"log/slog"
)

// deferStreamOpen records that a session's opening `lifecycle_snapshot` is owed.
// A stream's first event is ordered after the establishing `session/new`,
// `session/load`, `session/resume`, or fork response, so the snapshot is queued
// here and delivered by the write barrier that observes that response leave.
func (a *Agent) deferStreamOpen(session *session) {
	if session.lifecycleStream() == nil {
		return
	}

	a.streamOpenMu.Lock()
	a.streamOpens = append(a.streamOpens, session)
	a.streamOpenMu.Unlock()
}

// releaseStreamOpens delivers every owed opening snapshot. The local connection
// calls it after each frame it writes, so the establishing response is on the
// transport before the snapshot that follows it. Delivery runs off the writing
// goroutine because the notification it sends takes the same serialized writer
// the caller is still inside.
func (a *Agent) releaseStreamOpens() {
	a.streamOpenMu.Lock()
	owed := a.streamOpens
	a.streamOpens = nil
	a.streamOpenMu.Unlock()

	if len(owed) == 0 {
		return
	}

	a.streamOpenWait.Add(1)

	go func() {
		defer a.streamOpenWait.Done()
		defer recoverAgentGoroutine(context.Background(), a.log, "Hermes lifecycle stream open")

		for _, session := range owed {
			a.openDeferredStream(session)
		}
	}()
}

// openDeferredStream emits one owed snapshot. A stream this adapter cannot open
// truthfully is fenced by the emitter itself, so the failure is recorded and the
// session goes on without a stream rather than opening one whose first event
// lies.
func (a *Agent) openDeferredStream(session *session) {
	ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	defer cancel()

	if err := session.lifecycleStream().ensureLifecycleOpened(ctx); err != nil {
		a.log.DebugContext(ctx, "open Hermes lifecycle stream failed",
			slog.String(jsonFieldSessionID, string(session.id)),
			slog.String(jsonFieldError, err.Error()),
		)
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
	writer  io.Writer
	written func()
}

func (o responseOrderedWriter) Write(frame []byte) (int, error) {
	written, err := o.writer.Write(frame)
	if err != nil {
		return written, err
	}

	o.written()

	return written, nil
}
