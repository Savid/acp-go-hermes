package hermesacp

import (
	"context"
	"os"
	"sync"

	"github.com/coder/acp-go-sdk"
)

type fakeHermesClient struct {
	mu sync.Mutex

	xdg xdgDirs

	createSession nativeSession
	getSession    nativeSession
	listSessions  []nativeSession
	forkSession   nativeSession
	messages      []nativeMessage
	todos         []nativeTodo
	providers     providersResponse
	agents        []nativeAgent

	pendingPermissions []permissionRequest
	permissionReplies  []fakePermissionReply
	pendingQuestions   []questionRequest
	questionReplies    []fakeQuestionReply
	questionRejects    []fakeQuestionReject

	createSessionFunc func(context.Context, string) (nativeSession, error)
	sendMessage       func(context.Context, string, hermesMessageRequest) (nativeMessage, error)

	aborts         []string
	deleted        []string
	closed         bool
	events         chan hermesEvent
	errs           chan error
	createErr      error
	getErr         error
	listErr        error
	deleteErr      error
	messagesErr    error
	abortErr       error
	forkErr        error
	todosErr       error
	providersErr   error
	agentsErr      error
	permissionsErr error
	questionsErr   error
	replyErr       error
	closeErr       error
}

type fakePermissionReply struct {
	sessionID string
	requestID string
	route     permissionRoute
	reply     string
	message   string
}

type fakeQuestionReply struct {
	sessionID string
	requestID string
	route     questionRoute
	answers   [][]string
}

type fakeQuestionReject struct {
	sessionID string
	requestID string
	route     questionRoute
}

func newFakeHermesClient() *fakeHermesClient {
	return &fakeHermesClient{
		events: make(chan hermesEvent, 16),
		errs:   make(chan error, 16),
	}
}

func (c *fakeHermesClient) Close(context.Context) error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return c.closeErr
}

func (c *fakeHermesClient) CreateSession(ctx context.Context, title string) (nativeSession, error) {
	if c.createSessionFunc != nil {
		return c.createSessionFunc(ctx, title)
	}
	return c.createSession, c.createErr
}

func (c *fakeHermesClient) GetSession(context.Context, string) (nativeSession, error) {
	return c.getSession, c.getErr
}

func (c *fakeHermesClient) ListSessions(context.Context, string) ([]nativeSession, error) {
	return append([]nativeSession(nil), c.listSessions...), c.listErr
}

func (c *fakeHermesClient) DeleteSession(_ context.Context, id string) error {
	c.mu.Lock()
	c.deleted = append(c.deleted, id)
	c.mu.Unlock()
	return c.deleteErr
}

func (c *fakeHermesClient) SendMessage(ctx context.Context, id string, req hermesMessageRequest) (nativeMessage, error) {
	if c.sendMessage != nil {
		return c.sendMessage(ctx, id, req)
	}
	return nativeMessage{Info: nativeMessageInfo{ID: "assistant-1", SessionID: id, Role: "assistant", Finish: "stop"}}, nil
}

func (c *fakeHermesClient) Messages(context.Context, string) ([]nativeMessage, error) {
	return append([]nativeMessage(nil), c.messages...), c.messagesErr
}

func (c *fakeHermesClient) Abort(_ context.Context, id string) error {
	c.mu.Lock()
	c.aborts = append(c.aborts, id)
	c.mu.Unlock()
	return c.abortErr
}

func (c *fakeHermesClient) Fork(context.Context, string, string) (nativeSession, error) {
	return c.forkSession, c.forkErr
}

func (c *fakeHermesClient) Todos(context.Context, string) ([]nativeTodo, error) {
	return append([]nativeTodo(nil), c.todos...), c.todosErr
}

func (c *fakeHermesClient) ConfigProviders(context.Context) (providersResponse, error) {
	return c.providers, c.providersErr
}

func (c *fakeHermesClient) Agents(context.Context) ([]nativeAgent, error) {
	return append([]nativeAgent(nil), c.agents...), c.agentsErr
}

func (c *fakeHermesClient) PendingPermissions(context.Context) ([]permissionRequest, error) {
	return append([]permissionRequest(nil), c.pendingPermissions...), c.permissionsErr
}

func (c *fakeHermesClient) ReplyPermission(_ context.Context, req permissionRequest, reply string, message string) error {
	c.mu.Lock()
	c.permissionReplies = append(c.permissionReplies, fakePermissionReply{
		sessionID: req.SessionID,
		requestID: req.ID,
		route:     req.route(),
		reply:     reply,
		message:   message,
	})
	c.mu.Unlock()
	return c.replyErr
}

func (c *fakeHermesClient) PendingQuestions(context.Context) ([]questionRequest, error) {
	return append([]questionRequest(nil), c.pendingQuestions...), c.questionsErr
}

func (c *fakeHermesClient) ReplyQuestion(_ context.Context, req questionRequest, answers [][]string) error {
	copied := make([][]string, len(answers))
	for i := range answers {
		copied[i] = append([]string(nil), answers[i]...)
	}
	c.mu.Lock()
	c.questionReplies = append(c.questionReplies, fakeQuestionReply{sessionID: req.SessionID, requestID: req.ID, route: req.route(), answers: copied})
	c.mu.Unlock()
	return c.replyErr
}

func (c *fakeHermesClient) RejectQuestion(_ context.Context, req questionRequest) error {
	c.mu.Lock()
	c.questionRejects = append(c.questionRejects, fakeQuestionReject{sessionID: req.SessionID, requestID: req.ID, route: req.route()})
	c.mu.Unlock()
	return c.replyErr
}

func (c *fakeHermesClient) Events() <-chan hermesEvent {
	return c.events
}

func (c *fakeHermesClient) EventErrors() <-chan error {
	return c.errs
}

func (c *fakeHermesClient) XDGDirs() xdgDirs {
	return c.xdg
}

func (c *fakeHermesClient) abortCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.aborts)
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
	c.mu.Lock()
	c.elicitations = append(c.elicitations, request)
	c.scopes = append(c.scopes, scope)
	resp := c.elicitation
	err := c.elicitErr
	started := c.elicitationStarted
	release := c.elicitationRelease
	ignoreContext := c.elicitationIgnoreContext
	c.mu.Unlock()
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
	return resp, err
}

func (c *recordingAgentClient) RequestPermission(ctx context.Context, request acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	c.mu.Lock()
	c.permissions = append(c.permissions, request)
	resp := c.permission
	err := c.permErr
	started := c.permissionStarted
	release := c.permissionRelease
	ignoreContext := c.permissionIgnoreContext
	c.mu.Unlock()
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

func testNativeSession(id string) nativeSession {
	native := nativeSession{ID: id, Title: "Test", Agent: "build"}
	native.Model.ProviderID = "openai"
	native.Model.ModelID = "gpt-test"
	native.Time.Updated = 1_700_000_000_000
	return native
}

func testSession(agent *Agent, client *fakeHermesClient) *session {
	if client.xdg.Root == "" {
		root, err := os.MkdirTemp("", "acp-go-hermes-test-*")
		if err == nil {
			client.xdg, _ = createXDGDirs(root, "session-1")
		}
	}
	return newSession(agent, "session-1", "/tmp/project", nil, testNativeSession("native-1"), client, sessionMeta{}, idmapRecord{
		SessionID:       "session-1",
		NativeSessionID: "native-1",
		Format:          SessionStoreFormat,
	})
}
