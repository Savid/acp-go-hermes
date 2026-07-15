package hermesacp

import (
	"context"
	"os"
	"sync"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

	"github.com/coder/acp-go-sdk"
)

type fakeHermesClient struct {
	mu sync.Mutex

	xdg nativehermes.XDGDirs

	createSession nativehermes.Session
	getSession    nativehermes.Session
	listSessions  []nativehermes.Session
	forkSession   nativehermes.Session
	messages      []nativehermes.NativeMessage
	todos         []nativehermes.Todo
	providers     nativehermes.ProvidersResponse

	pendingPermissions []nativehermes.PermissionRequest
	permissionReplies  []fakePermissionReply
	pendingQuestions   []nativehermes.QuestionRequest
	questionReplies    []fakeQuestionReply
	questionRejects    []fakeQuestionReject

	createSessionFunc func(context.Context, string) (nativehermes.Session, error)
	sendMessage       func(context.Context, string, nativehermes.MessageRequest) (nativehermes.NativeMessage, error)

	aborts         []string
	deleted        []string
	closed         bool
	events         chan nativehermes.TurnEvent
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
	permissionsErr error
	questionsErr   error
	replyErr       error
	closeErr       error
	reloadErr      error
	reloadCalls    int
	reloadFunc     func(context.Context, string) error
	closeFunc      func(context.Context) error
}

type fakePermissionReply struct {
	sessionID string
	requestID string
	route     nativehermes.PermissionRoute
	reply     string
	message   string
}

type fakeQuestionReply struct {
	sessionID string
	requestID string
	route     nativehermes.QuestionRoute
	answers   [][]string
}

type fakeQuestionReject struct {
	sessionID string
	requestID string
	route     nativehermes.QuestionRoute
}

func newFakeHermesClient() *fakeHermesClient {
	return &fakeHermesClient{
		events: make(chan nativehermes.TurnEvent, 16),
		errs:   make(chan error, 16),
	}
}

func (c *fakeHermesClient) Close(ctx context.Context) error {
	c.mu.Lock()
	c.closed = true
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

func (c *fakeHermesClient) GetSession(context.Context, string) (nativehermes.Session, error) {
	return c.getSession, c.getErr
}

func (c *fakeHermesClient) ListSessions(context.Context, string) ([]nativehermes.Session, error) {
	return append([]nativehermes.Session(nil), c.listSessions...), c.listErr
}

func (c *fakeHermesClient) DeleteSession(_ context.Context, id string) error {
	c.mu.Lock()
	c.deleted = append(c.deleted, id)
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
	if c.sendMessage != nil {
		return c.sendMessage(ctx, id, req)
	}

	return nativehermes.NativeMessage{Info: nativehermes.NativeMessageInfo{ID: "assistant-1", SessionID: id, Role: "assistant", Finish: "stop"}}, nil
}

func (c *fakeHermesClient) Messages(context.Context, string) ([]nativehermes.NativeMessage, error) {
	return append([]nativehermes.NativeMessage(nil), c.messages...), c.messagesErr
}

func (c *fakeHermesClient) Abort(_ context.Context, id string) error {
	c.mu.Lock()
	c.aborts = append(c.aborts, id)
	c.mu.Unlock()

	return c.abortErr
}

func (c *fakeHermesClient) Fork(context.Context, string, string) (nativehermes.Session, error) {
	return c.forkSession, c.forkErr
}

func (c *fakeHermesClient) Todos(context.Context, string) ([]nativehermes.Todo, error) {
	return append([]nativehermes.Todo(nil), c.todos...), c.todosErr
}

func (c *fakeHermesClient) ConfigProviders(context.Context) (nativehermes.ProvidersResponse, error) {
	return c.providers, c.providersErr
}

func (c *fakeHermesClient) PendingPermissions(context.Context) ([]nativehermes.PermissionRequest, error) {
	return append([]nativehermes.PermissionRequest(nil), c.pendingPermissions...), c.permissionsErr
}

func (c *fakeHermesClient) ReplyPermission(_ context.Context, req nativehermes.PermissionRequest, reply string, message string) error {
	c.mu.Lock()
	c.permissionReplies = append(c.permissionReplies, fakePermissionReply{
		sessionID: req.SessionID,
		requestID: req.ID,
		route:     req.Route(),
		reply:     reply,
		message:   message,
	})
	c.mu.Unlock()

	return c.replyErr
}

func (c *fakeHermesClient) PendingQuestions(context.Context) ([]nativehermes.QuestionRequest, error) {
	return append([]nativehermes.QuestionRequest(nil), c.pendingQuestions...), c.questionsErr
}

func (c *fakeHermesClient) ReplyQuestion(_ context.Context, req nativehermes.QuestionRequest, answers [][]string) error {
	copied := make([][]string, len(answers))
	for i := range answers {
		copied[i] = append([]string(nil), answers[i]...)
	}
	c.mu.Lock()
	c.questionReplies = append(c.questionReplies, fakeQuestionReply{sessionID: req.SessionID, requestID: req.ID, route: req.Route(), answers: copied})
	c.mu.Unlock()

	return c.replyErr
}

func (c *fakeHermesClient) RejectQuestion(_ context.Context, req nativehermes.QuestionRequest) error {
	c.mu.Lock()
	c.questionRejects = append(c.questionRejects, fakeQuestionReject{sessionID: req.SessionID, requestID: req.ID, route: req.Route()})
	c.mu.Unlock()

	return c.replyErr
}

func (c *fakeHermesClient) Events() <-chan nativehermes.TurnEvent {
	return c.events
}

func (c *fakeHermesClient) EventErrors() <-chan error {
	return c.errs
}

func (c *fakeHermesClient) XDGDirs() nativehermes.XDGDirs {
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
	native := nativehermes.Session{ID: id, Title: "Test", Agent: "build"}
	native.Model.ProviderID = "openai"
	native.Model.ModelID = "gpt-test"
	native.Time.Updated = 1_700_000_000_000

	return native
}

func testSession(agent *Agent, client *fakeHermesClient) *session {
	if client.xdg.Root == "" {
		root, err := os.MkdirTemp("", "acp-go-hermes-test-*")
		if err == nil {
			client.xdg, _ = nativehermes.CreateXDGDirs(root, "session-1")
		}
	}

	return newSession(agent, "session-1", "/tmp/project", nil, nil, testNativeSession("native-1"), client, sessionMeta{}, idmapRecord{
		SessionID:       "session-1",
		NativeSessionID: "native-1",
		Format:          SessionStoreFormat,
	})
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
