package hermesacp

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/savid/acp-go-hermes/internal/lifecycle"
	"github.com/stretchr/testify/require"
)

func TestLifecycleOpeningIsOrderedAfterResponse(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Versions: []int{lifecycle.Version}, ActivityKinds: []lifecycle.ActivityKind{},
	})
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	session := testSession(agent, newFakeHermesClient())
	require.NoError(t, session.openLifecycleStream())

	withoutStream := testSession(newTestAgent(), newFakeHermesClient())
	agent.deferStreamOpen(withoutStream)
	agent.releaseStreamOpens()
	agent.deferStreamOpen(session)
	require.Zero(t, conn.updateCount())
	agent.releaseStreamOpens()
	agent.awaitStreamOpens()
	require.Equal(t, 1, conn.updateCount())

	called := 0
	var output bytes.Buffer
	writer := responseOrderedWriter{writer: &output, written: func() { called++ }}
	written, err := writer.Write([]byte("response"))
	require.NoError(t, err)
	require.Equal(t, len("response"), written)
	require.Equal(t, "response", output.String())
	require.Equal(t, 1, called)

	failing := responseOrderedWriter{writer: failingWriter{}, written: func() { called++ }}
	_, err = failing.Write([]byte("response"))
	require.Error(t, err)
	require.Equal(t, 1, called)
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestLifecycleOpenFailureFencesStream(t *testing.T) {
	agent := newTestAgent()
	agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Versions: []int{lifecycle.Version}, ActivityKinds: []lifecycle.ActivityKind{},
	})
	conn := newRecordingAgentClient()
	conn.updateErr = errors.New("opening failed")
	agent.setAgentClient(conn)
	session := testSession(agent, newFakeHermesClient())
	require.NoError(t, session.openLifecycleStream())
	agent.openDeferredStream(session)
	require.True(t, session.lifecycleStream().fenced())
}
