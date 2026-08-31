package hermesacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
	"github.com/savid/acp-go-hermes/internal/lifecycle"
	"github.com/stretchr/testify/require"
)

func TestLifecycleOpeningIsOrderedAfterResponse(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	})
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	client := newFakeHermesClient()
	session := testSession(agent, client)
	require.NoError(t, session.openLifecycleStream())

	withoutStream := testSession(newTestAgent(), newFakeHermesClient())
	owed, deferred, err := agent.deferStreamOpen(lifecycleRequestContext(t.Context(), 1), withoutStream)
	require.NoError(t, err)
	require.False(t, deferred)
	require.Nil(t, owed)
	releaseLifecycleOpening(t, agent, withoutStream, 1)
	requireStreamOpenDeferred(t, agent, lifecycleRequestContext(t.Context(), 2), session)
	require.Zero(t, conn.updateCount())
	agent.completeStreamOpenWrite([]byte(`{"jsonrpc":"2.0","id":99,"result":{}}`), nil)
	require.Zero(t, conn.updateCount())
	releaseLifecycleOpening(t, agent, session, 2)
	agent.awaitStreamOpens()
	require.Equal(t, 1, conn.updateCount())

	called := 0
	var output bytes.Buffer
	writer := responseOrderedWriter{writer: &output, completed: func([]byte, error) { called++ }}
	written, err := writer.Write([]byte("response"))
	require.NoError(t, err)
	require.Equal(t, len("response"), written)
	require.Equal(t, "response", output.String())
	require.Equal(t, 1, called)

	failing := responseOrderedWriter{writer: failingWriter{}, completed: func([]byte, error) { called++ }}
	_, err = failing.Write([]byte("response"))
	require.Error(t, err)
	require.Equal(t, 2, called)

	short := responseOrderedWriter{writer: shortWriter{}, completed: func([]byte, error) { called++ }}
	_, err = short.Write([]byte("response"))
	require.ErrorIs(t, err, io.ErrShortWrite)
	require.Equal(t, 3, called)
}

func releaseLifecycleOpening(t *testing.T, agent *Agent, session *session, id int) {
	t.Helper()

	frame := lifecycleOpeningFrame(t, session, id)
	require.NoError(t, agent.beginStreamOpenWrite(frame))
	agent.completeStreamOpenWrite(frame, nil)
}

func lifecycleOpeningFrame(t *testing.T, session *session, id int) []byte {
	t.Helper()

	return lifecycleOpeningFrameWithToken(t, session, id, "test-lifecycle-request-"+strconv.Itoa(id))
}

func lifecycleOpeningFrameWithToken(t *testing.T, session *session, id int, token string) []byte {
	t.Helper()

	frame, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"result": map[string]any{
			"_meta":                     lifecycleResponseMeta(session.snapshot()),
			lifecycleRequestMarkerField: token,
		},
	})
	require.NoError(t, err)

	return frame
}

func lifecycleRequestContext(ctx context.Context, id int) context.Context {
	requestID := strconv.Itoa(id)

	return lifecycleRequestContextWithToken(ctx, id, "test-lifecycle-request-"+requestID)
}

func requireStreamOpenDeferred(t *testing.T, agent *Agent, ctx context.Context, session *session) {
	t.Helper()

	owed, deferred, err := agent.deferStreamOpen(ctx, session)
	require.NoError(t, err)
	require.True(t, deferred)
	require.NotNil(t, owed)
}

func lifecycleRequestContextWithToken(ctx context.Context, _ int, token string) context.Context {
	return context.WithValue(ctx, lifecycleRequestIdentityKey{}, lifecycleRequestIdentity{token: token})
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

type shortWriter struct{}

func (shortWriter) Write(value []byte) (int, error) { return len(value) / 2, nil }

type gatedLifecycleWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

type cancellableLifecycleWriter struct {
	entered chan struct{}
	cancel  chan struct{}
	once    sync.Once
}

type blockingCloseWriter struct {
	entered chan struct{}
	release chan struct{}
	done    chan struct{}
}

type blockingWriteAndCloseWriter struct {
	writeEntered chan struct{}
	writeRelease chan struct{}
	closeEntered chan struct{}
	closeRelease chan struct{}
	closeErr     error
}

func (w *blockingWriteAndCloseWriter) Write(value []byte) (int, error) {
	close(w.writeEntered)
	<-w.writeRelease

	return len(value), nil
}

func (w *blockingWriteAndCloseWriter) Close() error {
	close(w.closeEntered)
	<-w.closeRelease

	return w.closeErr
}

func (w *blockingCloseWriter) Write(value []byte) (int, error) { return len(value), nil }

func (w *blockingCloseWriter) Close() error {
	close(w.entered)
	<-w.release
	close(w.done)

	return nil
}

type failingCloseWriter struct{ err error }

func (w failingCloseWriter) Write(value []byte) (int, error) { return len(value), nil }
func (w failingCloseWriter) Close() error                    { return w.err }

type observedDoneContext struct {
	observed chan struct{}
	done     chan struct{}
	once     sync.Once
}

func (*observedDoneContext) Deadline() (time.Time, bool) { return time.Time{}, false }

func (*observedDoneContext) Err() error { return nil }

func (*observedDoneContext) Value(any) any { return nil }

func (c *observedDoneContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })

	return c.done
}

func (w *cancellableLifecycleWriter) Write([]byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.cancel

	return 0, io.ErrClosedPipe
}

func (w *cancellableLifecycleWriter) Close() error {
	select {
	case <-w.cancel:
	default:
		close(w.cancel)
	}

	return nil
}

type transportClosingRecordingClient struct {
	*recordingAgentClient
	writer *responseOrderedWriter
}

func (c *transportClosingRecordingClient) PrepareTransportClose(ctx context.Context) error {
	return c.writer.PrepareClose(ctx)
}

func (c *transportClosingRecordingClient) CloseTransport(ctx context.Context) error {
	return c.writer.Close(ctx)
}

func (w *gatedLifecycleWriter) Write(value []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release

	return len(value), nil
}

func TestLifecycleOpeningWaitsForItsCompleteExactResponse(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	})
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	client := newFakeHermesClient()
	session := testSession(agent, client)
	require.NoError(t, session.openLifecycleStream())
	requireStreamOpenDeferred(t, agent, lifecycleRequestContext(t.Context(), 17), session)
	frame := lifecycleOpeningFrame(t, session, 17)

	gated := &gatedLifecycleWriter{entered: make(chan struct{}), release: make(chan struct{})}
	writer := responseOrderedWriter{
		writer: gated, starting: agent.beginStreamOpenWrite, completed: agent.completeStreamOpenWrite,
	}
	written := make(chan error, 1)
	go func() {
		_, err := writer.Write(frame)
		written <- err
	}()
	<-gated.entered

	agent.completeStreamOpenWrite([]byte(`{"jsonrpc":"2.0","id":18,"result":{}}`), nil)
	require.Zero(t, conn.updateCount())

	close(gated.release)
	require.NoError(t, <-written)
	agent.awaitStreamOpens()
	require.Equal(t, 1, conn.updateCount())
}

func TestActiveLoadReplayStartsAfterItsExactResponse(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	})
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	client := newFakeHermesClient()
	client.messages = []nativehermes.NativeMessage{{
		Info:  nativehermes.NativeMessageInfo{ID: historyMessageID(0), SessionID: "native-1", Role: valAssistant},
		Parts: []nativehermes.Part{{ID: "history-part", SessionID: "native-1", MessageID: historyMessageID(0), Type: valText, Text: "history"}},
	}}
	session := testSession(agent, client)
	require.NoError(t, session.openLifecycleStream())
	require.NoError(t, session.lifecycleStream().ensureLifecycleOpened(t.Context()))
	require.NoError(t, agent.storeStartedSession(session))
	baseline := conn.updateCount()

	requestCtx := lifecycleRequestContext(t.Context(), 71)
	_, err := agent.LoadSession(requestCtx, LoadSessionRequest(session.id, session.cwd))
	require.NoError(t, err)
	require.Equal(t, baseline, conn.updateCount(), "active replay notified before its response")

	frame := lifecycleOpeningFrame(t, session, 71)
	var output bytes.Buffer
	writer := &responseOrderedWriter{
		writer: &output, starting: agent.beginStreamOpenWrite, completed: agent.completeStreamOpenWrite,
	}
	_, err = writer.Write(frame)
	require.NoError(t, err)
	agent.awaitStreamOpens()
	require.Equal(t, baseline+1, conn.updateCount())
	require.NotContains(t, output.String(), lifecycleRequestMarkerField)
	require.NoError(t, agent.Close())
}

func TestAgentCloseJoinsBlockedLifecycleResponseWrite(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	})
	client := newFakeHermesClient()
	session := testSession(agent, client)
	require.NoError(t, session.openLifecycleStream())
	require.NoError(t, agent.storeStartedSession(session))
	requireStreamOpenDeferred(t, agent, lifecycleRequestContext(t.Context(), 23), session)
	frame := lifecycleOpeningFrame(t, session, 23)

	gated := &cancellableLifecycleWriter{entered: make(chan struct{}), cancel: make(chan struct{})}
	writer := &responseOrderedWriter{
		writer: gated, starting: agent.beginStreamOpenWrite, completed: agent.completeStreamOpenWrite,
	}
	conn := &transportClosingRecordingClient{recordingAgentClient: newRecordingAgentClient(), writer: writer}
	agent.setAgentClient(conn)
	written := make(chan error, 1)
	go func() {
		_, err := writer.Write(frame)
		written <- err
	}()
	<-gated.entered

	closed := make(chan error, 1)
	go func() {
		closed <- agent.Close()
	}()
	require.ErrorIs(t, <-written, io.ErrClosedPipe)
	require.NoError(t, <-closed)
	require.Zero(t, conn.updateCount())
	require.True(t, session.lifecycleStream().fenced())
	require.Equal(t, 1, client.closeCount())
}

func TestAgentClosePreservesAdmittedFullLifecycleResponse(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	})
	client := newFakeHermesClient()
	session := testSession(agent, client)
	require.NoError(t, session.openLifecycleStream())
	require.NoError(t, agent.storeStartedSession(session))
	_, _, err := agent.deferStreamOpen(lifecycleRequestContextWithToken(t.Context(), 0, "duplicate-json-rpc-id-first"), session)
	require.NoError(t, err)
	frame := lifecycleOpeningFrameWithToken(t, session, 77, "duplicate-json-rpc-id-first")

	gated := &gatedLifecycleWriter{entered: make(chan struct{}), release: make(chan struct{})}
	writer := &responseOrderedWriter{
		writer: gated, starting: agent.beginStreamOpenWrite, completed: agent.completeStreamOpenWrite,
	}
	conn := &transportClosingRecordingClient{recordingAgentClient: newRecordingAgentClient(), writer: writer}
	agent.setAgentClient(conn)
	written := make(chan error, 1)
	go func() {
		_, writeErr := writer.Write(frame)
		written <- writeErr
	}()
	<-gated.entered

	closed := make(chan error, 1)
	go func() { closed <- agent.Close() }()
	close(gated.release)
	require.NoError(t, <-written)
	require.NoError(t, <-closed)
	require.Equal(t, 1, conn.updateCount())
	require.Equal(t, 1, client.closeCount())
}

func TestResponseOrderedWriterCloseBranches(t *testing.T) {
	var absent *responseOrderedWriter
	require.NoError(t, absent.Close(t.Context()))

	idle := &responseOrderedWriter{writer: io.Discard}
	require.NoError(t, idle.Close(t.Context()))
	require.NoError(t, idle.Close(t.Context()))
	_, err := idle.Write([]byte("closed"))
	require.ErrorIs(t, err, io.ErrClosedPipe)

	want := errors.New("close failed")
	failing := &responseOrderedWriter{writer: failingCloseWriter{err: want}}
	require.ErrorIs(t, failing.Close(t.Context()), want)

	gated := &gatedLifecycleWriter{entered: make(chan struct{}), release: make(chan struct{})}
	nonclosable := &responseOrderedWriter{writer: gated}
	written := make(chan error, 1)
	go func() {
		_, writeErr := nonclosable.Write([]byte("blocked"))
		written <- writeErr
	}()
	<-gated.entered
	waitCtx, cancelWait := context.WithCancel(t.Context())
	cancelWait()
	closeDone := make(chan error, 1)
	go func() { closeDone <- nonclosable.Close(waitCtx) }()
	select {
	case err := <-closeDone:
		t.Fatalf("close returned before its admitted write: %v", err)
	default:
	}
	close(gated.release)
	require.NoError(t, <-written)
	require.ErrorIs(t, <-closeDone, context.Canceled)

	drainingGate := &gatedLifecycleWriter{entered: make(chan struct{}), release: make(chan struct{})}
	draining := &responseOrderedWriter{writer: drainingGate}
	drainingWrite := make(chan error, 1)
	go func() {
		_, writeErr := draining.Write([]byte("blocked"))
		drainingWrite <- writeErr
	}()
	<-drainingGate.entered
	drainCtx := &observedDoneContext{
		observed: make(chan struct{}), done: make(chan struct{}),
	}
	drained := make(chan error, 1)
	go func() { drained <- draining.PrepareClose(drainCtx) }()
	<-drainCtx.observed
	close(drainingGate.release)
	require.NoError(t, <-drainingWrite)
	require.NoError(t, <-drained)

	abortable := &cancellableLifecycleWriter{entered: make(chan struct{}), cancel: make(chan struct{})}
	aborting := &responseOrderedWriter{writer: abortable}
	abortedWrite := make(chan error, 1)
	go func() {
		_, writeErr := aborting.Write([]byte("blocked"))
		abortedWrite <- writeErr
	}()
	<-abortable.entered
	require.NoError(t, aborting.Close(t.Context()))
	require.ErrorIs(t, <-abortedWrite, io.ErrClosedPipe)

	blocking := &blockingCloseWriter{entered: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{})}
	blockedCloser := &responseOrderedWriter{writer: blocking}
	closeCtx, cancelClose := context.WithCancel(t.Context())
	blockingCloseDone := make(chan error, 1)
	go func() { blockingCloseDone <- blockedCloser.Close(closeCtx) }()
	<-blocking.entered
	cancelClose()
	select {
	case err := <-blockingCloseDone:
		t.Fatalf("close returned while the transport abort was still live: %v", err)
	default:
	}
	close(blocking.release)
	<-blocking.done
	require.ErrorIs(t, <-blockingCloseDone, context.Canceled)
}

func TestResponseOrderedWriterConcurrentCloseJoinsOneExactAttempt(t *testing.T) {
	want := errors.New("transport close failed")
	transport := &blockingWriteAndCloseWriter{
		writeEntered: make(chan struct{}),
		writeRelease: make(chan struct{}),
		closeEntered: make(chan struct{}),
		closeRelease: make(chan struct{}),
		closeErr:     want,
	}
	writer := &responseOrderedWriter{writer: transport}
	writeDone := make(chan error, 1)
	go func() {
		_, err := writer.Write([]byte("admitted"))
		writeDone <- err
	}()
	<-transport.writeEntered

	firstCtx, cancelFirst := context.WithCancel(t.Context())
	cancelFirst()
	closed := make(chan error, 2)
	go func() { closed <- writer.Close(firstCtx) }()
	<-transport.closeEntered
	go func() { closed <- writer.Close(t.Context()) }()

	close(transport.closeRelease)
	select {
	case err := <-closed:
		t.Fatalf("close returned while an admitted write was live: %v", err)
	default:
	}

	close(transport.writeRelease)
	require.NoError(t, <-writeDone)
	first := <-closed
	second := <-closed
	require.ErrorIs(t, first, want)
	require.ErrorIs(t, first, context.Canceled)
	require.Equal(t, first, second)
	require.Equal(t, first, writer.Close(t.Context()))
}

func TestLifecycleResponseWriteFailureDischargesOpening(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	})
	session := testSession(agent, newFakeHermesClient())
	require.NoError(t, session.openLifecycleStream())
	requireStreamOpenDeferred(t, agent, lifecycleRequestContext(t.Context(), 29), session)
	frame := lifecycleOpeningFrame(t, session, 29)

	writer := responseOrderedWriter{
		writer: failingWriter{}, starting: agent.beginStreamOpenWrite, completed: agent.completeStreamOpenWrite,
	}
	_, err := writer.Write(frame)
	require.ErrorIs(t, err, io.ErrClosedPipe)
	require.True(t, session.lifecycleStream().fenced())
	require.NoError(t, agent.Close())
}

func TestAgentCloseCancelsUnwrittenLifecycleOpening(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	})
	session := testSession(agent, newFakeHermesClient())
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	require.NoError(t, session.openLifecycleStream())
	requireStreamOpenDeferred(t, agent, lifecycleRequestContext(t.Context(), 31), session)
	frame := lifecycleOpeningFrame(t, session, 31)

	require.NoError(t, agent.Close())
	require.True(t, session.lifecycleStream().fenced())

	var output bytes.Buffer
	writer := responseOrderedWriter{
		writer: &output, starting: agent.beginStreamOpenWrite, completed: agent.completeStreamOpenWrite,
	}
	written, err := writer.Write(frame)
	require.ErrorIs(t, err, errLifecycleResponseCancelled)
	require.Zero(t, written)
	require.Empty(t, output.Bytes())
	require.Zero(t, conn.updateCount())
}

func TestLifecycleResponsesCorrelateByExactRequestIdentity(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	})
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)

	methods := []string{"session/new", "session/load", "session/resume", "session/fork"}
	for index, method := range methods {
		var frames [2][]byte
		var sessions [2]acp.SessionId
		for request := range 2 {
			suffix := []string{"-first", "-second"}[request]
			logicalID := acp.SessionId("logical-" + method + suffix)
			nativeID := "same-native-" + method
			client := newFakeHermesClient()
			native := testNativeSession(nativeID)
			session := newSession(agent, logicalID, "/tmp/project", nil, nil, native, client, sessionMeta{}, idmapRecord{
				SessionID: string(logicalID), NativeSessionID: nativeID, Format: SessionStoreFormat,
			})
			require.NoError(t, session.openLifecycleStream())
			require.NoError(t, agent.storeStartedSession(session))
			requestID := index*2 + request + 1
			_, _, err := agent.deferStreamOpen(lifecycleRequestContext(t.Context(), requestID), session)
			require.NoError(t, err)
			frames[request] = lifecycleOpeningFrame(t, session, requestID)
			sessions[request] = logicalID
		}
		firstWaiting := make(chan struct{})
		releaseFirst := make(chan struct{})
		firstDone := make(chan error, 1)
		go func() {
			close(firstWaiting)
			<-releaseFirst
			err := agent.beginStreamOpenWrite(frames[0])
			if err == nil {
				agent.completeStreamOpenWrite(frames[0], nil)
			}
			firstDone <- err
		}()
		<-firstWaiting
		require.NoError(t, agent.beginStreamOpenWrite(frames[1]))
		agent.completeStreamOpenWrite(frames[1], nil)
		close(releaseFirst)
		require.NoError(t, <-firstDone)
		agent.awaitStreamOpens()
		conn.mu.Lock()
		got := []acp.SessionId{
			conn.updates[len(conn.updates)-2].SessionId,
			conn.updates[len(conn.updates)-1].SessionId,
		}
		conn.mu.Unlock()
		require.ElementsMatch(t, []acp.SessionId{sessions[1], sessions[0]}, got)
	}
	agent.awaitStreamOpens()
	require.Equal(t, len(methods)*2, conn.updateCount())
	require.Empty(t, agent.streamOpens)
	require.NoError(t, agent.Close())
}

type lifecycleOpenBarrierClient struct {
	*recordingAgentClient
	delivered chan acp.SessionNotification
}

func (c *lifecycleOpenBarrierClient) SessionUpdate(
	ctx context.Context,
	notification acp.SessionNotification,
) error {
	err := c.recordingAgentClient.SessionUpdate(ctx, notification)
	c.delivered <- notification

	return err
}

func TestDuplicateLifecycleResponseIDsKeepExactOpaqueOwnership(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	})
	conn := &lifecycleOpenBarrierClient{
		recordingAgentClient: newRecordingAgentClient(),
		delivered:            make(chan acp.SessionNotification, 2),
	}
	agent.setAgentClient(conn)

	inputGate := newConnectionInputGate(strings.NewReader(""))
	local := &localAgentConnection{inputGate: inputGate}
	requestContext := func(method string) context.Context {
		line := []byte(`{"jsonrpc":"2.0","id":77,"method":"` + method + `","params":{}}`)
		stamped, err := inputGate.stampLifecycleRequest(line)
		require.NoError(t, err)
		var request struct {
			Params json.RawMessage `json:"params"`
		}
		require.NoError(t, json.Unmarshal(stamped, &request))

		ctx := local.bindLifecycleRequest(t.Context(), &request.Params)
		identity, exact := ctx.Value(lifecycleRequestIdentityKey{}).(lifecycleRequestIdentity)
		require.True(t, exact)
		require.NotEmpty(t, identity.token)

		return ctx
	}
	firstCtx := requestContext(acp.AgentMethodSessionNew)
	secondCtx := requestContext(acp.AgentMethodSessionResume)
	firstIdentity, firstExact := firstCtx.Value(lifecycleRequestIdentityKey{}).(lifecycleRequestIdentity)
	secondIdentity, secondExact := secondCtx.Value(lifecycleRequestIdentityKey{}).(lifecycleRequestIdentity)
	require.True(t, firstExact)
	require.True(t, secondExact)
	require.NotEqual(t, firstIdentity.token, secondIdentity.token)

	makeSession := func(id acp.SessionId, requestCtx context.Context) (*session, []byte) {
		native := testNativeSession("native-" + string(id))
		session := newSession(agent, id, "/tmp/project", nil, nil, native, newFakeHermesClient(), sessionMeta{}, idmapRecord{
			SessionID: string(id), NativeSessionID: native.ID, Format: SessionStoreFormat,
		})
		require.NoError(t, session.openLifecycleStream())
		_, _, err := agent.deferStreamOpen(requestCtx, session)
		require.NoError(t, err)

		marked, err := markLifecycleResponse(requestCtx, map[string]any{
			"_meta": lifecycleResponseMeta(session.snapshot()),
		})
		require.NoError(t, err)
		frame, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 77, "result": marked})
		require.NoError(t, err)

		return session, frame
	}

	first, firstFrame := makeSession("logical-first", firstCtx)
	second, secondFrame := makeSession("logical-second", secondCtx)

	var wire bytes.Buffer
	writer := responseOrderedWriter{
		writer: &wire, starting: agent.beginStreamOpenWrite, completed: agent.completeStreamOpenWrite,
	}

	// The second operation completes first even though both outstanding requests
	// carry JSON-RPC id 77. Its private token must release only its own snapshot.
	_, err := writer.Write(secondFrame)
	require.NoError(t, err)
	require.Equal(t, second.id, (<-conn.delivered).SessionId)
	require.False(t, first.lifecycleStream().live())

	_, err = writer.Write(firstFrame)
	require.NoError(t, err)
	require.Equal(t, first.id, (<-conn.delivered).SessionId)
	agent.awaitStreamOpens()

	require.NotContains(t, wire.String(), lifecycleRequestMarkerField)
	require.NotContains(t, wire.String(), firstIdentity.token)
	require.NotContains(t, wire.String(), secondIdentity.token)
	require.Empty(t, agent.streamOpens)
}

func TestLifecycleResponseFailureFencesOnlyItsExactRequestIdentity(t *testing.T) {
	for _, test := range []struct {
		name   string
		writer io.Writer
		want   error
	}{
		{name: "short", writer: shortWriter{}, want: io.ErrShortWrite},
		{name: "failed", writer: failingWriter{}, want: io.ErrClosedPipe},
	} {
		t.Run(test.name, func(t *testing.T) {
			agent := newTestAgent()
			agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
				Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
			})
			conn := newRecordingAgentClient()
			agent.setAgentClient(conn)

			makeSession := func(id acp.SessionId, requestID int) *session {
				native := testNativeSession("same-native")
				s := newSession(agent, id, "/tmp/project", nil, nil, native, newFakeHermesClient(), sessionMeta{}, idmapRecord{
					SessionID: string(id), NativeSessionID: native.ID, Format: SessionStoreFormat,
				})
				require.NoError(t, s.openLifecycleStream())
				require.NoError(t, agent.storeStartedSession(s))
				_, _, err := agent.deferStreamOpen(lifecycleRequestContext(t.Context(), requestID), s)
				require.NoError(t, err)

				return s
			}
			failed := makeSession("logical-a", 41)
			succeeded := makeSession("logical-b", 42)

			failedFrame := lifecycleOpeningFrame(t, failed, 41)
			failureWriter := responseOrderedWriter{
				writer: test.writer, starting: agent.beginStreamOpenWrite, completed: agent.completeStreamOpenWrite,
			}
			_, err := failureWriter.Write(failedFrame)
			require.ErrorIs(t, err, test.want)
			require.True(t, failed.lifecycleStream().fenced())

			releaseLifecycleOpening(t, agent, succeeded, 42)
			agent.awaitStreamOpens()
			require.False(t, succeeded.lifecycleStream().fenced())
			require.Equal(t, 1, conn.updateCount())
			conn.mu.Lock()
			require.Equal(t, succeeded.id, conn.updates[0].SessionId)
			conn.mu.Unlock()
			require.NoError(t, agent.Close())
		})
	}
}

func TestLifecycleOpeningObligationsAreBoundedAndRetired(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	})
	session := testSession(agent, newFakeHermesClient())
	require.NoError(t, session.openLifecycleStream())

	owed := make([]*deferredStreamOpen, 0, maxDeferredStreamOpens)
	for index := range maxDeferredStreamOpens {
		obligation, _, err := agent.deferStreamOpen(lifecycleRequestContext(t.Context(), index+1), session)
		require.NoError(t, err)
		owed = append(owed, obligation)
	}
	if _, _, err := agent.deferStreamOpen(lifecycleRequestContext(t.Context(), maxDeferredStreamOpens+1), session); err == nil {
		t.Fatal("unbounded lifecycle response obligations were admitted")
	}
	for _, obligation := range owed {
		agent.abandonStreamOpen(obligation)
	}
	require.Empty(t, agent.streamOpens)
	require.NoError(t, session.Close(context.Background()))
}

func TestActiveReuseRefusalsRemainExactAndReleaseAdmission(t *testing.T) {
	t.Run("agent close and registry replacement", func(t *testing.T) {
		agent := newTestAgent()
		session := testSession(agent, newFakeHermesClient())
		agent.sessions[session.id] = session
		cancelled, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := agent.completeActiveReuse(cancelled, session.id, session, false, func() {}); err == nil {
			t.Fatal("cancelled active reuse completed")
		}

		agent = newTestAgent()
		if _, err := agent.completeActiveReuse(t.Context(), session.id, session, false, func() {}); err == nil {
			t.Fatal("replaced active reuse completed")
		}
	})

	t.Run("lifecycle response backpressure", func(t *testing.T) {
		agent := newTestAgent()
		agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
			Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
		})
		session := testSession(agent, newFakeHermesClient())
		require.NoError(t, session.openLifecycleStream())
		require.NoError(t, agent.storeStartedSession(session))
		for index := range maxDeferredStreamOpens {
			_, _, err := agent.deferStreamOpen(lifecycleRequestContext(t.Context(), index+1), session)
			require.NoError(t, err)
		}

		_, err := agent.ResumeSession(lifecycleRequestContext(t.Context(), maxDeferredStreamOpens+1), ResumeSessionRequest(session.id, session.cwd))
		require.Error(t, err)
		session.mu.Lock()
		reuseDone := session.reuseDone
		session.mu.Unlock()
		require.Nil(t, reuseDone)

		agent.cancelStreamOpens()
		agent.awaitStreamOpens()
		require.NoError(t, session.Close(t.Context()))
	})
}

func TestLifecycleOpeningRemainingHardCutBranches(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	})
	session := testSession(agent, newFakeHermesClient())
	require.NoError(t, session.openLifecycleStream())
	require.NoError(t, agent.Close())
	if _, _, err := agent.deferStreamOpen(lifecycleRequestContext(t.Context(), 1), session); err == nil {
		t.Fatal("closed agent deferred a lifecycle opening")
	}

	agent = newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	})
	session = testSession(agent, newFakeHermesClient())
	require.NoError(t, session.openLifecycleStream())
	owed, _, err := agent.deferStreamOpen(lifecycleRequestContext(t.Context(), 2), session)
	require.NoError(t, err)
	owed.state = deferredStreamOpenWriting
	require.NoError(t, agent.beginStreamOpenWrite(lifecycleOpeningFrame(t, session, 2)))
	owed.state = deferredStreamOpenPending
	agent.abandonStreamOpen(owed)

	missing := &deferredStreamOpen{agent: agent, state: deferredStreamOpenPending}
	agent.streamOpenWait.Add(1)
	agent.abandonStreamOpen(missing)
	other := &deferredStreamOpen{agent: agent, state: deferredStreamOpenPending}
	owed = &deferredStreamOpen{agent: agent, state: deferredStreamOpenPending}
	agent.streamOpenWait.Add(1)
	agent.streamOpens = []*deferredStreamOpen{other, owed}
	agent.abandonStreamOpen(owed)
	agent.streamOpens = nil
	agent.abandonStreamOpen(nil)

	for _, frame := range [][]byte{
		[]byte(`{`),
		[]byte(`{"jsonrpc":"2.0","id":1,"error":{}}`),
		[]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`),
	} {
		clean, transformed, stripErr := stripLifecycleResponseMarker(frame)
		require.NoError(t, stripErr)
		require.False(t, transformed)
		require.Equal(t, frame, clean)
	}
	badMarker := []byte(`{"jsonrpc":"2.0","id":1,"result":{"` + lifecycleRequestMarkerField + `":null}}`)
	if _, transformed, stripErr := stripLifecycleResponseMarker(badMarker); stripErr == nil || !transformed {
		t.Fatalf("malformed marker = transformed %v, err %v", transformed, stripErr)
	}
	markedLine := append(lifecycleOpeningFrame(t, session, 2), '\n')
	clean, transformed, err := stripLifecycleResponseMarker(markedLine)
	require.NoError(t, err)
	require.True(t, transformed)
	require.True(t, bytes.HasSuffix(clean, []byte{'\n'}))

	completed := 0
	writer := responseOrderedWriter{writer: io.Discard, completed: func([]byte, error) { completed++ }}
	if _, err := writer.Write(badMarker); err == nil || completed != 1 {
		t.Fatalf("malformed private response write = completed %d, err %v", completed, err)
	}
	writer = responseOrderedWriter{writer: failingWriter{}}
	if written, err := writer.Write(lifecycleOpeningFrame(t, session, 2)); err == nil || written != 0 {
		t.Fatalf("transformed failed write = %d, %v", written, err)
	}
}

func TestActiveLifecycleReuseAdmissionCannotCrossAgentClose(t *testing.T) {
	for _, method := range []string{"load", "resume"} {
		t.Run(method, func(t *testing.T) {
			agent := newTestAgent()
			agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
				Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
			})
			client := newFakeHermesClient()
			session := testSession(agent, client)
			require.NoError(t, session.openLifecycleStream())
			require.NoError(t, agent.storeStartedSession(session))

			admitted := make(chan struct{})
			cancelled := make(chan struct{})
			requestCtx := context.WithValue(t.Context(), activeReuseAdmissionHookKey{}, func(ctx context.Context) {
				close(admitted)
				<-ctx.Done()
				close(cancelled)
			})
			responseReady := make(chan error, 1)
			go func() {
				if method == "load" {
					_, err := agent.LoadSession(requestCtx, acp.LoadSessionRequest{
						SessionId: session.id, Cwd: session.cwd, McpServers: []acp.McpServer{},
					})
					responseReady <- err

					return
				}
				_, err := agent.ResumeSession(requestCtx, acp.ResumeSessionRequest{
					SessionId: session.id, Cwd: session.cwd, McpServers: []acp.McpServer{},
				})
				responseReady <- err
			}()

			<-admitted
			require.True(t, agent.mu.TryLock(), "active reuse held Agent.mu across post-admission work")
			agent.mu.Unlock()

			closed := make(chan error, 1)
			go func() { closed <- agent.Close() }()
			<-cancelled
			require.Error(t, <-responseReady)
			require.NoError(t, <-closed)
		})
	}
}

func TestActiveLifecycleReuseCannotCrossSuccessfulSessionClose(t *testing.T) {
	for _, method := range []string{"load", "resume"} {
		t.Run(method, func(t *testing.T) {
			store := NewInMemorySessionStore()
			agent := newTestAgent(WithSessionStore(store))
			agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
				Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
			})
			conn := newRecordingAgentClient()
			agent.setAgentClient(conn)
			client := newFakeHermesClient()
			session := testSession(agent, client)
			require.NoError(t, session.openLifecycleStream())
			require.NoError(t, session.lifecycleStream().ensureLifecycleOpened(t.Context()))
			require.NoError(t, agent.storeStartedSession(session))
			baselineUpdates := conn.updateCount()

			admitted := make(chan struct{})
			cancelled := make(chan struct{})
			requestCtx := context.WithValue(lifecycleRequestContext(t.Context(), 81), activeReuseAdmissionHookKey{}, func(ctx context.Context) {
				close(admitted)
				<-ctx.Done()
				close(cancelled)
			})
			requestDone := make(chan error, 1)
			go func() {
				if method == "load" {
					_, err := agent.LoadSession(requestCtx, LoadSessionRequest(session.id, session.cwd))
					requestDone <- err

					return
				}
				_, err := agent.ResumeSession(requestCtx, ResumeSessionRequest(session.id, session.cwd))
				requestDone <- err
			}()
			<-admitted

			closeDone := make(chan error, 1)
			go func() {
				_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
				closeDone <- err
			}()
			<-cancelled
			require.True(t, agent.mu.TryLock(), "session close held Agent.mu while joining reuse")
			agent.mu.Unlock()
			require.Error(t, <-requestDone)
			require.NoError(t, <-closeDone)
			require.Nil(t, agent.activeSession(session.id))
			require.Equal(t, baselineUpdates, conn.updateCount())
			require.Empty(t, agent.streamOpens)

			entries, err := store.Load(t.Context(), SessionKey{SessionID: string(session.id)})
			require.NoError(t, err)
			before := append([]SessionStoreEntry(nil), entries...)
			client.emitEvent(nativehermes.TurnEvent{Type: nativehermes.EventGatewayRaw})
			require.Equal(t, baselineUpdates, conn.updateCount())
			after, err := store.Load(t.Context(), SessionKey{SessionID: string(session.id)})
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestSessionCloseCancelsAndJoinsActiveReplay(t *testing.T) {
	agent := newTestAgent(WithSessionStore(NewInMemorySessionStore()))
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	})
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	client := newFakeHermesClient()
	replayEntered := make(chan struct{})
	replayCancelled := make(chan struct{})
	client.messagesFunc = func(ctx context.Context, _ string) ([]nativehermes.NativeMessage, error) {
		close(replayEntered)
		<-ctx.Done()
		close(replayCancelled)

		return nil, ctx.Err()
	}
	session := testSession(agent, client)
	require.NoError(t, session.openLifecycleStream())
	require.NoError(t, session.lifecycleStream().ensureLifecycleOpened(t.Context()))
	require.NoError(t, agent.storeStartedSession(session))
	baseline := conn.updateCount()

	requestCtx := lifecycleRequestContext(t.Context(), 91)
	_, err := agent.LoadSession(requestCtx, LoadSessionRequest(session.id, session.cwd))
	require.NoError(t, err)
	frame := lifecycleOpeningFrame(t, session, 91)
	writer := &responseOrderedWriter{
		writer: io.Discard, starting: agent.beginStreamOpenWrite, completed: agent.completeStreamOpenWrite,
	}
	_, err = writer.Write(frame)
	require.NoError(t, err)
	<-replayEntered

	closeDone := make(chan error, 1)
	go func() {
		_, closeErr := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
		closeDone <- closeErr
	}()
	<-replayCancelled
	require.NoError(t, <-closeDone)
	agent.awaitStreamOpens()
	require.Nil(t, agent.activeSession(session.id))
	require.Equal(t, 1, client.closeCount())
	require.Equal(t, baseline, conn.updateCount())
}

func TestSuccessfulSessionCloseCannotPrecedeActiveReuseResponse(t *testing.T) {
	for index, method := range []string{"load", "resume"} {
		t.Run(method, func(t *testing.T) {
			agent := newTestAgent(WithSessionStore(NewInMemorySessionStore()))
			agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
				Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
			})
			conn := newRecordingAgentClient()
			agent.setAgentClient(conn)
			client := newFakeHermesClient()
			client.messages = []nativehermes.NativeMessage{{
				Info: nativehermes.NativeMessageInfo{ID: historyMessageID(0), SessionID: "native-1", Role: valAssistant},
				Parts: []nativehermes.Part{{
					ID: "late-history", SessionID: "native-1", MessageID: historyMessageID(0), Type: valText, Text: "must not arrive",
				}},
			}}
			session := testSession(agent, client)
			require.NoError(t, session.openLifecycleStream())
			require.NoError(t, session.lifecycleStream().ensureLifecycleOpened(t.Context()))
			require.NoError(t, agent.storeStartedSession(session))
			baseline := conn.updateCount()

			requestID := 101 + index
			admission := make(chan context.Context, 1)
			requestCtx := context.WithValue(lifecycleRequestContext(t.Context(), requestID), activeReuseAdmissionHookKey{}, func(ctx context.Context) {
				admission <- ctx
			})
			if method == "load" {
				_, err := agent.LoadSession(requestCtx, LoadSessionRequest(session.id, session.cwd))
				require.NoError(t, err)
			} else {
				_, err := agent.ResumeSession(requestCtx, ResumeSessionRequest(session.id, session.cwd))
				require.NoError(t, err)
			}
			reuseCtx := <-admission

			closeDone := make(chan error, 1)
			go func() {
				_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
				closeDone <- err
			}()
			<-reuseCtx.Done()
			select {
			case err := <-closeDone:
				t.Fatalf("session close preceded active reuse response: %v", err)
			default:
			}

			frame := lifecycleOpeningFrame(t, session, requestID)
			writer := &responseOrderedWriter{
				writer: io.Discard, starting: agent.beginStreamOpenWrite, completed: agent.completeStreamOpenWrite,
			}
			_, err := writer.Write(frame)
			require.NoError(t, err)
			require.NoError(t, <-closeDone)
			agent.awaitStreamOpens()
			require.Equal(t, baseline, conn.updateCount())
			require.Nil(t, agent.activeSession(session.id))
		})
	}
}

func TestLifecycleOpenFailureFencesStream(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	})
	conn := newRecordingAgentClient()
	conn.updateErr = errors.New("opening failed")
	agent.setAgentClient(conn)
	session := testSession(agent, newFakeHermesClient())
	require.NoError(t, session.openLifecycleStream())
	agent.openDeferredStream(session)
	require.True(t, session.lifecycleStream().fenced())
}

func TestLifecycleOpenFailureLogOmitsArbitraryErrorText(t *testing.T) {
	const secret = "lifecycle-open-secret-sentinel"
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	agent := newTestAgent(WithLogger(logger))
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	})
	conn := newRecordingAgentClient()
	conn.updateErr = errors.New(secret)
	agent.setAgentClient(conn)
	session := testSession(agent, newFakeHermesClient())
	require.NoError(t, session.openLifecycleStream())

	agent.openDeferredStream(session)
	require.True(t, session.lifecycleStream().fenced())
	require.Contains(t, logs.String(), "classification=lifecycle_open_failed")
	require.NotContains(t, logs.String(), "sessionId")
	require.False(t, strings.Contains(logs.String(), secret), logs.String())
}
