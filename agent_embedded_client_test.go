package hermesacp

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

// embeddedTestClient is a full embedding client: it answers permissions and
// elicitations and accepts extension notifications.
type embeddedTestClient struct {
	pipeACPClient

	notifyMu      sync.Mutex
	notifications []string
	streamed      string
}

// SessionUpdate records the assistant text the embedded agent streamed, which
// is the fact this path exists to deliver.
func (c *embeddedTestClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	if chunk := notification.Update.AgentMessageChunk; chunk != nil && chunk.Content.Text != nil {
		c.notifyMu.Lock()
		c.streamed += chunk.Content.Text.Text
		c.notifyMu.Unlock()
	}

	return c.pipeACPClient.SessionUpdate(ctx, notification)
}

func (c *embeddedTestClient) streamedText() string {
	c.notifyMu.Lock()
	defer c.notifyMu.Unlock()

	return c.streamed
}

func (c *embeddedTestClient) ExtensionNotification(_ context.Context, method string, _ any) error {
	c.notifyMu.Lock()
	defer c.notifyMu.Unlock()

	c.notifications = append(c.notifications, method)

	return nil
}

func (c *embeddedTestClient) notificationMethods() []string {
	c.notifyMu.Lock()
	defer c.notifyMu.Unlock()

	return append([]string(nil), c.notifications...)
}

func (c *embeddedTestClient) updateCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.updates
}

func (c *embeddedTestClient) elicitationRequests() []acp.UnstableCreateElicitationRequest {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]acp.UnstableCreateElicitationRequest(nil), c.elicitations...)
}

// bareEmbeddedClient implements the required client surface and nothing else,
// which is what makes the optional surfaces observable as optional.
type bareEmbeddedClient struct {
	mu      sync.Mutex
	updates int
}

var _ acp.Client = (*bareEmbeddedClient)(nil)

func (*bareEmbeddedClient) ReadTextFile(context.Context, acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
	return acp.ReadTextFileResponse{}, nil
}

func (*bareEmbeddedClient) WriteTextFile(context.Context, acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
	return acp.WriteTextFileResponse{}, nil
}

func (*bareEmbeddedClient) RequestPermission(context.Context, acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected("once")}, nil
}

func (c *bareEmbeddedClient) SessionUpdate(context.Context, acp.SessionNotification) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.updates++

	return nil
}

func (*bareEmbeddedClient) CreateTerminal(context.Context, acp.CreateTerminalRequest) (acp.CreateTerminalResponse, error) {
	return acp.CreateTerminalResponse{}, nil
}

func (*bareEmbeddedClient) KillTerminal(context.Context, acp.KillTerminalRequest) (acp.KillTerminalResponse, error) {
	return acp.KillTerminalResponse{}, nil
}

func (*bareEmbeddedClient) TerminalOutput(context.Context, acp.TerminalOutputRequest) (acp.TerminalOutputResponse, error) {
	return acp.TerminalOutputResponse{}, nil
}

func (*bareEmbeddedClient) ReleaseTerminal(context.Context, acp.ReleaseTerminalRequest) (acp.ReleaseTerminalResponse, error) {
	return acp.ReleaseTerminalResponse{}, nil
}

func (*bareEmbeddedClient) WaitForTerminalExit(context.Context, acp.WaitForTerminalExitRequest) (acp.WaitForTerminalExitResponse, error) {
	return acp.WaitForTerminalExitResponse{}, nil
}

// TestEmbeddedClientStreamsWithoutServe is the embedding path's streaming
// proof. A directly constructed Agent publishes the same outbound traffic a
// stdio peer receives — session updates, permission requests, elicitations
// carrying the reserved route object, and extension notifications — with no
// JSON-RPC transport anywhere in the test.
func TestEmbeddedClientStreamsWithoutServe(t *testing.T) {
	host := &embeddedTestClient{}
	agent := newTestAgent(WithClient(host))
	native := newFakeHermesClient()
	native.sendMessage = func(_ context.Context, id string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		return nativehermes.NativeMessage{
			Info:  nativehermes.NativeMessageInfo{ID: "assistant-1", SessionID: id, Role: "assistant", Finish: "stop"},
			Parts: []nativehermes.Part{{SessionID: id, MessageID: "assistant-1", Type: "text", Text: "embedded reply"}},
		}, nil
	}
	session := testSession(t, agent, native)

	agent.mu.Lock()
	agent.sessions[session.id] = session
	agent.mu.Unlock()

	require.NotNil(t, agent.connection(), "WithClient installs the outbound surface at construction")

	response, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "embedded-turn", "reply"))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
	require.Positive(t, host.updateCount(), "an embedded agent streams its session updates")
	require.Equal(t, "embedded reply", host.streamedText())

	conn := agent.connection()

	permission, err := conn.RequestPermission(t.Context(), acp.RequestPermissionRequest{SessionId: session.id})
	require.NoError(t, err)
	require.NotNil(t, permission.Outcome.Selected)

	requestID := "request-1"
	_, err = conn.CreateElicitation(t.Context(), acp.UnstableCreateElicitationRequest{
		Form: &acp.UnstableCreateElicitationForm{Message: "answer"},
	}, elicitationScope{SessionID: session.id, TurnNonce: "embedded-turn", RequestID: &requestID})
	require.NoError(t, err)

	requests := host.elicitationRequests()
	require.Len(t, requests, 1)
	require.Equal(t, map[string]any{
		routeFieldVer:  routeVersion,
		routeFieldID:   session.id,
		routeFieldTurn: "embedded-turn",
		routeFieldReq:  requestID,
	}, requests[0].Form.Meta[routeMetaKey], "the embedded elicitation carries the same reserved route object the wire does")

	require.NoError(t, conn.NotifyExtension(t.Context(), RawEventMethod, map[string]any{"sessionId": session.id}))
	require.Equal(t, []string{RawEventMethod}, host.notificationMethods())

	select {
	case <-conn.Done():
		t.Fatal("embedded client reported done before the agent closed")
	default:
	}

	require.NoError(t, agent.Close())

	select {
	case <-conn.Done():
	default:
		t.Fatal("embedded client stayed live after the agent closed")
	}
}

// TestEmbeddedClientOptionalSurfaces pins what a client that implements only the
// required surface gets: notifications it cannot receive are dropped rather than
// failing a turn, and an elicitation it cannot answer is refused rather than
// left pending forever.
func TestEmbeddedClientOptionalSurfaces(t *testing.T) {
	host := &bareEmbeddedClient{}
	agent := newTestAgent(WithClient(host))
	conn := agent.connection()
	require.NotNil(t, conn)

	require.NoError(t, conn.NotifyExtension(t.Context(), RawEventMethod, map[string]any{}))

	written := make(chan error, 1)
	_, err := conn.CreateElicitationRegistered(t.Context(), acp.UnstableCreateElicitationRequest{
		Form: &acp.UnstableCreateElicitationForm{Message: "answer"},
	}, elicitationScope{SessionID: "session-1", TurnNonce: "turn"}, written)
	require.ErrorIs(t, err, errElicitationUnsupported)
	require.ErrorIs(t, <-written, errElicitationUnsupported)

	// A scope no route object can be built from is refused before the client is
	// ever called, and an unusable request shape is refused with it.
	full := &embeddedTestClient{}
	fullAgent := newTestAgent(WithClient(full))
	_, err = fullAgent.connection().CreateElicitation(t.Context(),
		acp.UnstableCreateElicitationRequest{}, elicitationScope{SessionID: "session-1", TurnNonce: "turn"})
	require.Error(t, err)
	_, err = fullAgent.connection().CreateElicitation(t.Context(),
		acp.UnstableCreateElicitationRequest{Form: &acp.UnstableCreateElicitationForm{}}, elicitationScope{})
	require.Error(t, err)
	require.Empty(t, full.elicitationRequests())

	// CloseTransport is idempotent: the agent closes it, and a second close
	// must not panic on an already-closed channel.
	require.NoError(t, agent.Close())
	embedded, ok := conn.(*embeddedAgentClient)
	require.True(t, ok)
	require.NoError(t, embedded.CloseTransport(t.Context()))
}

// TestEmbeddedClientURLElicitationCarriesTheRouteObject pins the second
// elicitation shape, which stamps its own copy of the reserved object.
func TestEmbeddedClientURLElicitationCarriesTheRouteObject(t *testing.T) {
	host := &embeddedTestClient{}
	agent := newTestAgent(WithClient(host))
	callID := acp.ToolCallId("tool-1")

	_, err := agent.connection().UnstableCreateElicitation(t.Context(), acp.UnstableCreateElicitationRequest{
		Url: &acp.UnstableCreateElicitationUrl{ElicitationId: "elicit-1", Message: "open", Url: "https://example.test"},
	})
	require.Error(t, err, "an unscoped elicitation cannot be routed")

	_, err = agent.connection().CreateElicitation(t.Context(), acp.UnstableCreateElicitationRequest{
		Url: &acp.UnstableCreateElicitationUrl{ElicitationId: "elicit-1", Message: "open", Url: "https://example.test"},
	}, elicitationScope{SessionID: "session-1", TurnNonce: "turn", ToolCallID: callID})
	require.NoError(t, err)

	requests := host.elicitationRequests()
	require.Len(t, requests, 1)
	require.Equal(t, map[string]any{
		routeFieldVer:  routeVersion,
		routeFieldID:   acp.SessionId("session-1"),
		routeFieldTurn: "turn",
		routeFieldTool: callID,
	}, requests[0].Url.Meta[routeMetaKey])
}

// TestServeRefusesASuppliedClient keeps the two client surfaces from being
// installed on one agent: Serve owns the connection it builds.
func TestServeRefusesASuppliedClient(t *testing.T) {
	err := Serve(t.Context(), strings.NewReader(""), io.Discard, WithClient(&bareEmbeddedClient{}))
	require.ErrorIs(t, err, errServeClientSupplied)
}

// TestEmbeddedClientRefusesBeyondItsClientCallLimit pins the admission gate the
// embedded surface shares with the JSON-RPC one: an outbound client call over
// the configured limit is refused, and a registered caller learns the request
// was never delivered instead of waiting for a write that will not happen.
func TestEmbeddedClientRefusesBeyondItsClientCallLimit(t *testing.T) {
	host := &embeddedTestClient{}
	agent := newTestAgent(WithClient(host), WithConcurrencyLimits(ConcurrencyLimits{MaxConcurrentClientCalls: 1}))
	conn := agent.connection()

	held, err := agent.acquireClientCall(t.Context())
	require.NoError(t, err)

	defer held()

	permissionWritten := make(chan error, 1)
	_, err = conn.RequestPermissionRegistered(t.Context(),
		acp.RequestPermissionRequest{SessionId: "session-1"}, permissionWritten)
	require.Error(t, err)
	require.Error(t, <-permissionWritten)

	elicitationWritten := make(chan error, 1)
	_, err = conn.CreateElicitationRegistered(t.Context(), acp.UnstableCreateElicitationRequest{
		Form: &acp.UnstableCreateElicitationForm{Message: "answer"},
	}, elicitationScope{SessionID: "session-1", TurnNonce: "turn"}, elicitationWritten)
	require.Error(t, err)
	require.Error(t, <-elicitationWritten)

	require.Zero(t, host.updateCount())
	require.Empty(t, host.elicitationRequests())
}
