package hermesacp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
	"github.com/savid/acp-go-hermes/internal/lifecycle"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

// absTestPath builds a host-absolute path from POSIX-looking segments, so a
// test states "an absolute working directory" rather than a spelling only one
// platform accepts.
func absTestPath(segments ...string) string {
	root := "/"
	if runtime.GOOS == "windows" {
		root = `C:\`
	}

	return filepath.Join(append([]string{root}, segments...)...)
}

// awaitTestSignal waits for a rendezvous the rest of a test cannot proceed
// without, so a step that never runs fails the test where it stalled instead of
// hanging the whole package until its timeout.
func awaitTestSignal(t *testing.T, signal <-chan struct{}, what string) {
	t.Helper()

	select {
	case <-signal:
	case <-time.After(30 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func newTestAgent(opts ...Option) *Agent {
	return NewAgent(opts...)
}

// sessionMetaFromLifecycle decodes lifecycle meta through an agent with no
// provider-auth root, which is the configuration every test that does not set
// one runs under.
func sessionMetaFromLifecycle(meta map[string]any) (sessionMeta, error) {
	return newTestAgent().sessionMetaFromLifecycle(meta)
}

type fakeHermesClient struct {
	mu sync.Mutex

	xdg nativehermes.XDGDirs

	createSession     nativehermes.Session
	getSession        nativehermes.Session
	listSessions      []nativehermes.Session
	persistedSessions []nativehermes.Session
	forkSession       nativehermes.Session
	messages          []nativehermes.NativeMessage
	providers         nativehermes.ProvidersResponse

	configProviderCalls int
	setModelCalls       []fakeModelSelection
	setModelErr         error

	permissionReplies []fakePermissionReply
	questionReplies   []fakeQuestionReply
	questionRejects   []fakeQuestionReject
	getSessionIDs     []string

	createSessionFunc func(context.Context, string) (nativehermes.Session, error)
	sendMessage       func(context.Context, string, nativehermes.MessageRequest) (nativehermes.NativeMessage, error)
	messagesFunc      func(context.Context, string) ([]nativehermes.NativeMessage, error)

	aborts               []string
	deleted              []string
	closed               bool
	closeCalls           int
	deliveries           chan nativehermes.TurnDelivery
	createErr            error
	getErr               error
	listErr              error
	deleteErr            error
	messagesErr          error
	skipHistory          bool
	skipAssistantHistory bool
	abortErr             error
	forkErr              error
	forkCalls            int
	providersErr         error
	replyErr             error
	closeErr             error
	reloadErr            error
	reloadCalls          int
	reloadFunc           func(context.Context, string) error
	closeFunc            func(context.Context) error

	authProviders         []nativehermes.AuthProvider
	authProvidersErr      error
	authStart             nativehermes.AuthStart
	authStartErr          error
	authStartFunc         func(context.Context, string) (nativehermes.AuthStart, error)
	authSubmitErr         error
	authSubmits           []string
	authPoll              nativehermes.AuthPoll
	authPollErr           error
	authPollFunc          func(context.Context, string, string) (nativehermes.AuthPoll, error)
	authCancelled         []string
	authCancelFlowErr     error
	authDisconnected      []string
	authDisconnectErr     error
	providerAuthSupported *bool
	promptCycles          uint64
	beforePromptDispatch  func()
	promptTerminal        *nativehermes.TurnEvent
	omitPromptTerminal    bool
	afterPromptTerminal   func()
}

type fakeModelSelection struct {
	sessionID string
	value     string
}

func (c *fakeHermesClient) ProviderAuthSupported() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.providerAuthSupported == nil || *c.providerAuthSupported
}

func (c *fakeHermesClient) AuthProviders(context.Context) ([]nativehermes.AuthProvider, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.authProviders, c.authProvidersErr
}

func (c *fakeHermesClient) AuthDisconnect(_ context.Context, providerID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.authDisconnected = append(c.authDisconnected, providerID)

	return c.authDisconnectErr
}

func (c *fakeHermesClient) AuthStart(ctx context.Context, providerID string) (nativehermes.AuthStart, error) {
	c.mu.Lock()
	fn := c.authStartFunc
	start := c.authStart
	err := c.authStartErr
	c.mu.Unlock()

	if fn != nil {
		return fn(ctx, providerID)
	}

	return start, err
}

func (c *fakeHermesClient) AuthSubmit(_ context.Context, _ string, _ string, input string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.authSubmits = append(c.authSubmits, input)

	return c.authSubmitErr
}

func (c *fakeHermesClient) AuthPollFlow(ctx context.Context, providerID string, nativeSessionID string) (nativehermes.AuthPoll, error) {
	c.mu.Lock()
	fn := c.authPollFunc
	poll := c.authPoll
	err := c.authPollErr
	c.mu.Unlock()

	if fn != nil {
		return fn(ctx, providerID, nativeSessionID)
	}

	return poll, err
}

func (c *fakeHermesClient) AuthCancelFlow(_ context.Context, nativeSessionID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.authCancelled = append(c.authCancelled, nativeSessionID)

	return c.authCancelFlowErr
}

type fakePermissionReply struct {
	sessionID string
	requestID string
	reply     string
	message   string
}

type fakeQuestionReply struct {
	sessionID string
	requestID string
	answers   [][]string
}

type fakeQuestionReject struct {
	sessionID string
	requestID string
}

func newFakeHermesClient() *fakeHermesClient {
	return &fakeHermesClient{
		deliveries: make(chan nativehermes.TurnDelivery, 32),
	}
}

func (c *fakeHermesClient) Close(ctx context.Context) error {
	c.mu.Lock()
	c.closed = true
	c.closeCalls++
	closeFunc := c.closeFunc
	c.mu.Unlock()
	if closeFunc != nil {
		return closeFunc(ctx)
	}

	return c.closeErr
}

func (c *fakeHermesClient) CreateSession(ctx context.Context, title string) (nativehermes.Session, error) {
	if c.createSessionFunc != nil {
		return c.createSessionFunc(ctx, title)
	}

	return c.createSession, c.createErr
}

func (c *fakeHermesClient) CreateSessionWithDraft(
	ctx context.Context,
	title string,
	bind func(nativehermes.SessionDraft) error,
) (nativehermes.Session, error) {
	native, err := c.CreateSession(ctx, title)
	if err != nil {
		return nativehermes.Session{}, err
	}
	if err := bind(nativehermes.SessionDraft{LiveSessionID: "live-" + native.ID, StoredSessionID: native.ID}); err != nil {
		return nativehermes.Session{}, err
	}

	return native, nil
}

func (c *fakeHermesClient) GetSession(_ context.Context, id string) (nativehermes.Session, error) {
	c.mu.Lock()
	c.getSessionIDs = append(c.getSessionIDs, id)
	c.mu.Unlock()

	return c.getSession, c.getErr
}

func (c *fakeHermesClient) ListSessions(context.Context, string) ([]nativehermes.Session, error) {
	return append([]nativehermes.Session(nil), c.listSessions...), c.listErr
}

func (c *fakeHermesClient) PersistedSessions(context.Context) ([]nativehermes.Session, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]nativehermes.Session(nil), c.persistedSessions...), c.listErr
}

func (c *fakeHermesClient) DeleteSession(_ context.Context, id string) error {
	c.mu.Lock()
	c.deleted = append(c.deleted, id)
	for index := range c.persistedSessions {
		if c.persistedSessions[index].ID == id {
			c.persistedSessions = append(c.persistedSessions[:index], c.persistedSessions[index+1:]...)

			break
		}
	}
	c.mu.Unlock()

	return c.deleteErr
}

func (c *fakeHermesClient) ReloadMCP(ctx context.Context, id string) error {
	c.mu.Lock()
	c.reloadCalls++
	reload := c.reloadFunc
	c.mu.Unlock()
	if reload != nil {
		return reload(ctx, id)
	}

	return c.reloadErr
}

func (c *fakeHermesClient) SendMessage(ctx context.Context, id string, req nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
	c.mu.Lock()
	c.promptCycles++
	promptCycle := c.promptCycles
	cycleID := fmt.Sprintf("fake/%s/cycle-%d", id, promptCycle)
	c.mu.Unlock()
	if c.beforePromptDispatch != nil {
		c.beforePromptDispatch()
	}
	if err := nativehermes.NotifyPromptDispatch(ctx, nativehermes.PromptDispatchInfo{
		CycleID: cycleID, TransportGeneration: 1,
	}); err != nil {
		return nativehermes.NativeMessage{}, err
	}

	var (
		message nativehermes.NativeMessage
		err     error
	)
	if c.sendMessage != nil {
		message, err = c.sendMessage(ctx, id, req)
	} else {
		message = nativehermes.NativeMessage{Info: nativehermes.NativeMessageInfo{ID: "assistant-1", SessionID: id, Role: "assistant", Finish: "stop"}}
	}
	if err != nil {
		return message, err
	}

	c.mu.Lock()
	if !c.skipHistory {
		user := nativehermes.NativeMessage{Info: nativehermes.NativeMessageInfo{
			ID:        historyMessageID(len(c.messages)),
			SessionID: id,
			Role:      "user",
		}}
		c.messages = append(c.messages, user)

		if !c.skipAssistantHistory {
			history := message
			history.Info.ID = historyMessageID(len(c.messages))
			history.Info.SessionID = id
			c.messages = append(c.messages, history)
		}
	}
	c.mu.Unlock()

	if !c.omitPromptTerminal {
		copyMessage := message
		terminal := nativehermes.TurnEvent{
			Type:                nativehermes.EventCycleComplete,
			TransportGeneration: 1,
			CycleID:             cycleID,
			Origin:              nativehermes.CycleOriginPrompt,
			Message:             &copyMessage,
		}
		if c.promptTerminal != nil {
			terminal = *c.promptTerminal
			terminal.TransportGeneration = 1
			terminal.CycleID = cycleID
			terminal.Origin = nativehermes.CycleOriginPrompt
		}
		c.emitEvent(terminal)
	}
	if c.afterPromptTerminal != nil {
		c.afterPromptTerminal()
	}

	return message, nil
}

// promptDispatchCount reports how many prompts actually reached the harness.
func (c *fakeHermesClient) promptDispatchCount() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.promptCycles
}

func (c *fakeHermesClient) Messages(ctx context.Context, id string) ([]nativehermes.NativeMessage, error) {
	if c.messagesFunc != nil {
		return c.messagesFunc(ctx, id)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]nativehermes.NativeMessage(nil), c.messages...), c.messagesErr
}

func (c *fakeHermesClient) Abort(_ context.Context, id string) error {
	c.mu.Lock()
	c.aborts = append(c.aborts, id)
	c.mu.Unlock()

	return c.abortErr
}

func (c *fakeHermesClient) Fork(context.Context, string, string) (nativehermes.Session, error) {
	c.mu.Lock()
	c.forkCalls++
	c.mu.Unlock()

	return c.forkSession, c.forkErr
}

func (c *fakeHermesClient) ForkWithBaseline(ctx context.Context, id string, marker string, _ []string) (nativehermes.Session, error) {
	return c.Fork(ctx, id, marker)
}

func (c *fakeHermesClient) forkCallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.forkCalls
}

func (c *fakeHermesClient) ConfigProviders(context.Context) (nativehermes.ProvidersResponse, error) {
	c.mu.Lock()
	c.configProviderCalls++
	c.mu.Unlock()

	return c.providers, c.providersErr
}

func (c *fakeHermesClient) SetModel(_ context.Context, sessionID string, value string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.setModelCalls = append(c.setModelCalls, fakeModelSelection{sessionID: sessionID, value: value})

	return c.setModelErr
}

// configProviderCallCount reports how many native model.options enumerations
// this client has been asked for.
func (c *fakeHermesClient) configProviderCallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.configProviderCalls
}

func (c *fakeHermesClient) ReplyPermission(_ context.Context, req nativehermes.PermissionRequest, reply string, message string) error {
	c.mu.Lock()
	c.permissionReplies = append(c.permissionReplies, fakePermissionReply{
		sessionID: req.SessionID,
		requestID: req.ID,
		reply:     reply,
		message:   message,
	})
	c.mu.Unlock()

	return c.replyErr
}

func (c *fakeHermesClient) ReplyQuestion(_ context.Context, req nativehermes.QuestionRequest, answers [][]string) error {
	copied := make([][]string, len(answers))
	for i := range answers {
		copied[i] = append([]string(nil), answers[i]...)
	}
	c.mu.Lock()
	c.questionReplies = append(c.questionReplies, fakeQuestionReply{sessionID: req.SessionID, requestID: req.ID, answers: copied})
	c.mu.Unlock()

	return c.replyErr
}

func (c *fakeHermesClient) RejectQuestion(_ context.Context, req nativehermes.QuestionRequest) error {
	c.mu.Lock()
	c.questionRejects = append(c.questionRejects, fakeQuestionReject{sessionID: req.SessionID, requestID: req.ID})
	c.mu.Unlock()

	return c.replyErr
}

func (c *fakeHermesClient) Deliveries() <-chan nativehermes.TurnDelivery {
	return c.deliveries
}

func (c *fakeHermesClient) emitEvent(event nativehermes.TurnEvent) {
	copyEvent := event
	c.deliveries <- nativehermes.TurnDelivery{Event: &copyEvent}
}

func (c *fakeHermesClient) emitError(err error) {
	c.deliveries <- nativehermes.TurnDelivery{Err: err}
}

func (c *fakeHermesClient) XDGDirs() nativehermes.XDGDirs {
	return c.xdg
}

func (c *fakeHermesClient) abortCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.aborts)
}

func (c *fakeHermesClient) closeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.closeCalls
}

func (c *fakeHermesClient) permissionReply(index int) fakePermissionReply {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.permissionReplies[index]
}

func (c *fakeHermesClient) permissionReplyCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.permissionReplies)
}

func (c *fakeHermesClient) questionReply(index int) fakeQuestionReply {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.questionReplies[index]
}

func (c *fakeHermesClient) questionReplyCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.questionReplies)
}

func (c *fakeHermesClient) questionRejectCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.questionRejects)
}

type recordingAgentClient struct {
	done chan struct{}

	mu sync.Mutex

	updates      []acp.SessionNotification
	extensions   []extensionNotification
	permissions  []acp.RequestPermissionRequest
	elicitations []acp.UnstableCreateElicitationRequest
	scopes       []elicitationScope

	permission               acp.RequestPermissionResponse
	elicitation              acp.UnstableCreateElicitationResponse
	permissionStarted        chan struct{}
	permissionRelease        chan struct{}
	permissionIgnoreContext  bool
	elicitationStarted       chan struct{}
	elicitationRelease       chan struct{}
	elicitationIgnoreContext bool
	permErr                  error
	elicitErr                error
	permissionWriteErr       error
	elicitationWriteErr      error
	permissionBeforeReturn   func()
	elicitationBeforeReturn  func()
	updateErr                error
	notifyErr                error
}

type extensionNotification struct {
	method string
	params any
}

func newRecordingAgentClient() *recordingAgentClient {
	return &recordingAgentClient{
		done:       make(chan struct{}),
		permission: acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected("once")},
		elicitation: acp.UnstableCreateElicitationResponse{
			Accept: &acp.UnstableCreateElicitationAccept{Action: "accept", Content: map[string]any{}},
		},
	}
}

func (c *recordingAgentClient) Done() <-chan struct{} {
	return c.done
}

func (c *recordingAgentClient) UnstableCreateElicitation(
	ctx context.Context,
	request acp.UnstableCreateElicitationRequest,
) (acp.UnstableCreateElicitationResponse, error) {
	return c.CreateElicitation(ctx, request, elicitationScope{})
}

func (c *recordingAgentClient) CreateElicitation(
	ctx context.Context,
	request acp.UnstableCreateElicitationRequest,
	scope elicitationScope,
) (acp.UnstableCreateElicitationResponse, error) {
	return c.CreateElicitationRegistered(ctx, request, scope, nil)
}

func (c *recordingAgentClient) CreateElicitationRegistered(
	ctx context.Context,
	request acp.UnstableCreateElicitationRequest,
	scope elicitationScope,
	written chan<- error,
) (acp.UnstableCreateElicitationResponse, error) {
	c.mu.Lock()
	c.elicitations = append(c.elicitations, request)
	c.scopes = append(c.scopes, scope)
	resp := c.elicitation
	err := c.elicitErr
	started := c.elicitationStarted
	release := c.elicitationRelease
	ignoreContext := c.elicitationIgnoreContext
	writeErr := c.elicitationWriteErr
	beforeReturn := c.elicitationBeforeReturn
	c.mu.Unlock()
	if written != nil {
		written <- writeErr
	}
	if writeErr != nil {
		return acp.UnstableCreateElicitationResponse{}, writeErr
	}
	signalTestHook(started)
	if release != nil {
		if ignoreContext {
			<-release

			return resp, err
		}
		select {
		case <-release:
		case <-ctx.Done():
			return acp.UnstableCreateElicitationResponse{}, ctx.Err()
		}
	}
	if beforeReturn != nil {
		beforeReturn()
	}

	return resp, err
}

func (c *recordingAgentClient) RequestPermission(ctx context.Context, request acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	return c.RequestPermissionRegistered(ctx, request, nil)
}

func (c *recordingAgentClient) RequestPermissionRegistered(
	ctx context.Context,
	request acp.RequestPermissionRequest,
	written chan<- error,
) (acp.RequestPermissionResponse, error) {
	c.mu.Lock()
	c.permissions = append(c.permissions, request)
	resp := c.permission
	err := c.permErr
	started := c.permissionStarted
	release := c.permissionRelease
	ignoreContext := c.permissionIgnoreContext
	writeErr := c.permissionWriteErr
	beforeReturn := c.permissionBeforeReturn
	c.mu.Unlock()
	if written != nil {
		written <- writeErr
	}
	if writeErr != nil {
		return acp.RequestPermissionResponse{}, writeErr
	}
	signalTestHook(started)
	if release != nil {
		if ignoreContext {
			<-release

			return resp, err
		}
		select {
		case <-release:
		case <-ctx.Done():
			return acp.RequestPermissionResponse{}, ctx.Err()
		}
	}
	if beforeReturn != nil {
		beforeReturn()
	}

	return resp, err
}

func (c *recordingAgentClient) SessionUpdate(_ context.Context, notification acp.SessionNotification) error {
	c.mu.Lock()
	c.updates = append(c.updates, notification)
	err := c.updateErr
	c.mu.Unlock()

	return err
}

func (c *recordingAgentClient) NotifyExtension(_ context.Context, method string, params any) error {
	c.mu.Lock()
	c.extensions = append(c.extensions, extensionNotification{method: method, params: params})
	err := c.notifyErr
	c.mu.Unlock()

	return err
}

func (c *recordingAgentClient) updateCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.updates)
}

func (c *recordingAgentClient) extensionCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.extensions)
}

func (c *recordingAgentClient) extensionsFor(method string) []extensionNotification {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]extensionNotification, 0, len(c.extensions))
	for _, ext := range c.extensions {
		if ext.method == method {
			out = append(out, ext)
		}
	}

	return out
}

func (c *recordingAgentClient) permissionRequestCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.permissions)
}

func signalTestHook(ch chan struct{}) {
	if ch == nil {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

func testNativeSession(id string) nativehermes.Session {
	native := nativehermes.Session{ID: id, Title: "Test"}
	native.Model.ProviderID = "openai"
	native.Model.ModelID = "gpt-test"
	native.Time.Updated = 1_700_000_000_000

	return native
}

func testSession(agent *Agent, client *fakeHermesClient) *session {
	if client.xdg.Root == "" {
		root, err := os.MkdirTemp("", "acp-go-hermes-test-*")
		if err == nil {
			client.xdg, _ = testGenerationXDG(root)
		}
	}

	return newSession(agent, "session-1", absTestPath("tmp", "project"), nil, nil, testNativeSession("native-1"), client, sessionMeta{}, idmapRecord{
		SessionID:       "session-1",
		NativeSessionID: "native-1",
		Format:          SessionStoreFormat,
	})
}

const testControlCycleID = "test-control-cycle"

// beginTestControlTurn obtains control ownership from a cycle-start frame
// consumed by the permanent pump, matching the only production admission path.
func beginTestControlTurn(t *testing.T, s *session, ctx context.Context, nonce string) context.Context {
	t.Helper()

	s.pumpMu.Lock()
	pumpClient := s.pumpClient
	s.pumpMu.Unlock()
	client, ok := pumpClient.(*fakeHermesClient)
	if !ok {
		t.Fatal("control test requires the ordered fake Hermes source")
	}
	originalNonce := s.newPumpNonce
	s.newPumpNonce = func() (string, error) { return nonce, nil }
	if stream := s.lifecycleStream(); stream != nil {
		if err := stream.ensureLifecycleOpened(ctx); err != nil {
			t.Fatalf("open lifecycle source for control cycle: %v", err)
		}
	}
	client.emitEvent(nativehermes.TurnEvent{
		Type:                nativehermes.EventCycleStarted,
		CycleID:             testControlCycleID,
		TransportGeneration: 1,
		Origin:              nativehermes.CycleOriginActivity,
	})
	if err := s.synchronizePump(ctx); err != nil {
		t.Fatalf("project control cycle start: %v", err)
	}
	s.newPumpNonce = originalNonce
	route := s.routeForEvent(nativehermes.TurnEvent{
		CycleID:             testControlCycleID,
		TransportGeneration: 1,
	})
	if route == nil {
		t.Fatal("permanent pump did not publish control ownership")
	}
	t.Cleanup(func() { s.removePumpRoute(route) })

	return withPumpRoute(ctx, route)
}

func testHermesQuestionRequest(id string) nativehermes.QuestionRequest {
	return nativehermes.QuestionRequest{
		ID:                  id,
		SessionID:           "native-1",
		CycleID:             testControlCycleID,
		TransportGeneration: 1,
	}
}

type errorReader struct {
	err error
}

func (r errorReader) Read([]byte) (int, error) {
	return 0, r.err
}

type errorReadCloser struct {
	err error
}

func (r errorReadCloser) Read([]byte) (int, error) {
	return 0, r.err
}

func (r errorReadCloser) Close() error {
	return nil
}

type modelSetterTestServer struct {
	nativehermes.Server
	err error
}

func (s modelSetterTestServer) SetModel(context.Context, string, string) error { return s.err }

type sessionOperationErrorReader struct{ err error }

func (r sessionOperationErrorReader) Read([]byte) (int, error) { return 0, r.err }

var _ io.Reader = sessionOperationErrorReader{}

type sessionOperationServerOnly struct{ nativehermes.Server }

type sessionOperationFaultStore struct {
	base           SessionStore
	loadErrors     map[string]error
	listSubkeysErr error
	deleteErr      error
}

func (s sessionOperationFaultStore) Append(ctx context.Context, key SessionKey, entries []SessionStoreEntry) error {
	return s.base.Append(ctx, key, entries)
}

func (s sessionOperationFaultStore) Load(ctx context.Context, key SessionKey) ([]SessionStoreEntry, error) {
	if err := s.loadErrors[key.Subpath]; err != nil {
		return nil, err
	}

	return s.base.Load(ctx, key)
}

func (s sessionOperationFaultStore) Replace(ctx context.Context, key SessionKey, replacements []SessionStoreReplacement) error {
	return s.base.Replace(ctx, key, replacements)
}

func (s sessionOperationFaultStore) Delete(ctx context.Context, key SessionKey) error {
	if s.deleteErr != nil {
		return s.deleteErr
	}

	return s.base.Delete(ctx, key)
}

func (s sessionOperationFaultStore) ListSessions(ctx context.Context) ([]SessionSummary, error) {
	return s.base.ListSessions(ctx)
}

func (s sessionOperationFaultStore) ListSubkeys(ctx context.Context, key SessionKey) ([]string, error) {
	if s.listSubkeysErr != nil {
		return nil, s.listSubkeysErr
	}

	return s.base.ListSubkeys(ctx, key)
}

// lifecycleFailingAgentClient fails the failAt-th lifecycle envelope delivery,
// exercising the fence path a stream takes when one event cannot be delivered.
type lifecycleFailingAgentClient struct {
	*recordingAgentClient
	failAt int
	seen   int
}

func (c *lifecycleFailingAgentClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	if _, lifecycleUpdate := notification.Meta[lifecycle.MetaKey]; lifecycleUpdate {
		c.seen++
		if c.seen == c.failAt {
			return errors.New("lifecycle delivery failed")
		}
	}

	return c.recordingAgentClient.SessionUpdate(ctx, notification)
}

// newLifecycleActionSession opens a negotiated lifecycle stream inside an
// in-flight turn, accepting the submission only when accepted is true.
func newLifecycleActionSession(t *testing.T, accepted bool) (*session, *recordingAgentClient, context.Context) {
	t.Helper()

	agent := newTestAgent()
	require.NoError(t, agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version: lifecycle.Version, ActivityKinds: []lifecycle.ActivityKind{},
	}))
	agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	session := testSession(agent, newFakeHermesClient())
	if err := session.openLifecycleStream(); err != nil {
		t.Fatalf("open lifecycle stream: %v", err)
	}
	turnCtx := beginTestControlTurn(t, session, t.Context(), "turn")
	session.mu.Lock()
	session.turnInFlight = true
	session.turnEpoch = 1
	session.turnSettlement = turnSettlementOpen
	session.mu.Unlock()
	if accepted {
		if err := session.lifecycleStream().accept(turnCtx, lifecycle.Submission{
			SubmissionID: "submission", ClientNonce: "nonce",
		}); err != nil {
			t.Fatalf("accept lifecycle submission: %v", err)
		}
	}

	return session, conn, turnCtx
}

func setLifecycleDeliveryError(conn *recordingAgentClient) {
	conn.mu.Lock()
	conn.updateErr = errors.New("lifecycle delivery failed")
	conn.mu.Unlock()
}

func sessionTurnEpoch(session *session) uint64 {
	session.mu.Lock()
	defer session.mu.Unlock()

	return session.turnEpoch
}

// testGenerationXDG mints one runtime generation under parent the way the
// adapter does. The adapter's scratch parent always exists by the time a
// generation is minted under it, so a test standing in for the adapter creates
// the parent first.
func testGenerationXDG(parent string) (nativehermes.XDGDirs, error) {
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return nativehermes.XDGDirs{}, err
	}

	return nativehermes.CreateGenerationXDGDirs(parent)
}
