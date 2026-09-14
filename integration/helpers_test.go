//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	hermesacp "github.com/savid/acp-go-hermes"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-core/wire"
)

const testTimeout = 120 * time.Second
const permissionOptionAllow acp.PermissionOptionId = "once"

// recorder is the ACP client the tests observe the agent through.
type recorder struct {
	mu          sync.Mutex
	updates     []acp.SessionNotification
	raw         []json.RawMessage
	permissions []acp.RequestPermissionRequest
	answer      func(acp.RequestPermissionRequest) acp.RequestPermissionResponse
	elicit      func(acp.UnstableCreateElicitationRequest) (acp.UnstableCreateElicitationResponse, error)
	changed     chan struct{}
}

var (
	_ acp.Client                 = (*recorder)(nil)
	_ acp.ExtensionMethodHandler = (*recorder)(nil)
)

func newRecorder() *recorder {
	return &recorder{
		changed: make(chan struct{}, 1),
		answer: func(acp.RequestPermissionRequest) acp.RequestPermissionResponse {
			return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected(permissionOptionAllow)}
		},
	}
}

func (r *recorder) signal() {
	select {
	case r.changed <- struct{}{}:
	default:
	}
}

func (r *recorder) SessionUpdate(_ context.Context, params acp.SessionNotification) error {
	r.mu.Lock()
	r.updates = append(r.updates, params)
	r.mu.Unlock()
	r.signal()

	return nil
}

func (r *recorder) RequestPermission(_ context.Context, params acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	r.mu.Lock()
	r.permissions = append(r.permissions, params)
	answer := r.answer
	r.mu.Unlock()
	r.signal()

	return answer(params), nil
}

func (r *recorder) UnstableCreateElicitation(_ context.Context, params acp.UnstableCreateElicitationRequest) (acp.UnstableCreateElicitationResponse, error) {
	r.mu.Lock()
	elicit := r.elicit
	r.mu.Unlock()

	if elicit == nil {
		return acp.UnstableCreateElicitationResponse{}, errors.New("no elicitation handler")
	}

	return elicit(params)
}

func (r *recorder) HandleExtensionMethod(_ context.Context, method string, params json.RawMessage) (any, error) {
	if method == hermesacp.RawEventMethod {
		r.mu.Lock()
		r.raw = append(r.raw, append(json.RawMessage(nil), params...))
		r.mu.Unlock()
		r.signal()
	}

	return map[string]any{}, nil
}

func (*recorder) NotifyExtension(context.Context, string, any) error { return nil }

func (*recorder) ReadTextFile(context.Context, acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
	return acp.ReadTextFileResponse{}, errors.New("unsupported")
}

func (*recorder) WriteTextFile(context.Context, acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
	return acp.WriteTextFileResponse{}, errors.New("unsupported")
}

func (*recorder) CreateTerminal(context.Context, acp.CreateTerminalRequest) (acp.CreateTerminalResponse, error) {
	return acp.CreateTerminalResponse{}, errors.New("unsupported")
}

func (*recorder) KillTerminal(context.Context, acp.KillTerminalRequest) (acp.KillTerminalResponse, error) {
	return acp.KillTerminalResponse{}, errors.New("unsupported")
}

func (*recorder) TerminalOutput(context.Context, acp.TerminalOutputRequest) (acp.TerminalOutputResponse, error) {
	return acp.TerminalOutputResponse{}, errors.New("unsupported")
}

func (*recorder) ReleaseTerminal(context.Context, acp.ReleaseTerminalRequest) (acp.ReleaseTerminalResponse, error) {
	return acp.ReleaseTerminalResponse{}, errors.New("unsupported")
}

func (*recorder) WaitForTerminalExit(context.Context, acp.WaitForTerminalExitRequest) (acp.WaitForTerminalExitResponse, error) {
	return acp.WaitForTerminalExitResponse{}, errors.New("unsupported")
}

// snapshot returns the notifications recorded so far.
func (r *recorder) snapshot() []acp.SessionNotification {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]acp.SessionNotification(nil), r.updates...)
}

// waitFor blocks until condition holds over the recorded notifications.
func (r *recorder) waitFor(t *testing.T, condition func([]acp.SessionNotification) bool) {
	t.Helper()

	deadline := time.After(testTimeout)

	for {
		if condition(r.snapshot()) {
			return
		}

		select {
		case <-r.changed:
		case <-deadline:
			t.Fatalf("condition not met; %d notifications recorded", len(r.snapshot()))
		}
	}
}

func (r *recorder) waitForCount(t *testing.T, count int) {
	t.Helper()
	r.waitFor(t, func(updates []acp.SessionNotification) bool { return len(updates) >= count })
}

// harness serves an agent over pipes to a recording client.
type harness struct {
	t        *testing.T
	conn     *acp.ClientSideConnection
	rec      *recorder
	cancel   context.CancelFunc
	served   chan error
	stopOnce sync.Once
	stop     func()
}

func newHarness(t *testing.T, extra ...hermesacp.Option) *harness {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	clientReader, agentWriter := io.Pipe()
	agentReader, clientWriter := io.Pipe()
	rec := newRecorder()
	served := make(chan error, 1)

	go func() { served <- hermesacp.Serve(ctx, agentReader, agentWriter, extra...) }()

	conn := acp.NewClientSideConnection(rec, clientWriter, clientReader)
	conn.SetLogger(slog.New(slog.DiscardHandler))

	h := &harness{t: t, conn: conn, rec: rec, cancel: cancel, served: served}

	h.stop = func() {
		h.stopOnce.Do(func() {
			cancel()
			_ = clientWriter.Close()
			select {
			case <-served:
			case <-time.After(testTimeout):
				t.Error("hermesacp.Serve did not return")
			}
		})
	}
	t.Cleanup(h.stop)

	return h
}

func (h *harness) ctx() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	h.t.Cleanup(cancel)

	return ctx
}

func (h *harness) initialize(opts ...func(*acp.InitializeRequest)) acp.InitializeResponse {
	h.t.Helper()

	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	for _, opt := range opts {
		opt(&request)
	}

	resp, err := h.conn.Initialize(h.ctx(), request)
	require.NoError(h.t, err)

	return resp
}

func withLifecycle() func(*acp.InitializeRequest) {
	return func(request *acp.InitializeRequest) {
		request.Meta = map[string]any{wire.LifecycleKey: map[string]any{"version": 1}}
	}
}

func withFormElicitation() func(*acp.InitializeRequest) {
	return func(request *acp.InitializeRequest) {
		request.ClientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
	}
}

func (h *harness) newSession(opts ...hermesacp.SessionRequestOption) acp.NewSessionResponse {
	h.t.Helper()

	resp, err := h.conn.NewSession(h.ctx(), hermesacp.NewSessionRequest(h.t.TempDir(), opts...))
	require.NoError(h.t, err)

	return resp
}

func (h *harness) prompt(sessionID acp.SessionId, text string, meta map[string]any) (acp.PromptResponse, error) {
	h.t.Helper()

	request := hermesacp.TextPromptRequest(sessionID, text)
	request.Meta = meta

	return h.conn.Prompt(h.ctx(), request)
}

// promptMeta stamps the lifecycle prompt correlation.
func promptMeta(n int) map[string]any {
	return map[string]any{wire.LifecycleKey: map[string]any{
		"version": 1, "submission": map[string]any{"submissionId": fmt.Sprintf("sub-%d", n), "clientNonce": fmt.Sprintf("non-%d", n)},
	}}
}

// requestErrorData decodes the data member of a JSON-RPC error.
func requestErrorData(t *testing.T, err error) map[string]any {
	t.Helper()

	var reqErr *acp.RequestError
	require.ErrorAs(t, err, &reqErr)

	data, ok := reqErr.Data.(map[string]any)
	if !ok {
		encoded, marshalErr := json.Marshal(reqErr.Data)
		require.NoError(t, marshalErr)
		require.NoError(t, json.Unmarshal(encoded, &data))
	}

	return data
}

func requestErrorCode(t *testing.T, err error) int {
	t.Helper()

	var reqErr *acp.RequestError
	require.ErrorAs(t, err, &reqErr)

	return reqErr.Code
}

// agentText concatenates streamed agent message text.
func agentText(updates []acp.SessionNotification) string {
	var text strings.Builder

	for _, update := range updates {
		if chunk := update.Update.AgentMessageChunk; chunk != nil && chunk.Content.Text != nil {
			text.WriteString(chunk.Content.Text.Text)
		}
	}

	return text.String()
}

// lifecycleEvents extracts the lifecycle envelopes in delivery order.
func lifecycleEvents(updates []acp.SessionNotification) []map[string]any {
	events := make([]map[string]any, 0)

	for _, update := range updates {
		envelope, ok := update.Meta[wire.LifecycleKey].(map[string]any)
		if !ok {
			continue
		}

		event, _ := envelope["event"].(map[string]any)
		events = append(events, event)
	}

	return events
}

func eventTypes(events []map[string]any) []string {
	types := make([]string, 0, len(events))
	for _, event := range events {
		kind, _ := event["type"].(string)
		state, _ := event["state"].(string)

		if action, ok := event["action"].(map[string]any); ok {
			state, _ = action["state"].(string)
		}

		if state != "" {
			kind += ":" + state
		}

		types = append(types, kind)
	}

	return types
}
