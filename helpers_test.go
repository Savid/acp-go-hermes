package hermesacp

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

	"github.com/coder/acp-go-sdk"
)

// testIsolationIdentity is the identity every adapter-level fixture isolates
// to. The effective identity is unusable as-is: the policy forbids UID or GID
// zero, so a root test runner would be rejected before reaching anything under
// test. Zero is replaced rather than the whole identity so an unprivileged
// runner keeps isolating to itself.
func testIsolationIdentity() (uint32, uint32) {
	uid, gid := os.Geteuid(), os.Getegid()
	if uid == 0 {
		uid = 1
	}
	if gid == 0 {
		gid = 1
	}

	return uint32(uid), uint32(gid)
}

// newTestAgent builds an agent in the ordinary default configuration: no
// WithProcessIsolation, so native work runs as the identity the test process
// already holds. This is what a host that configures nothing gets, so it is
// what most tests should exercise.
func newTestAgent(opts ...Option) *Agent {
	return NewAgent(opts...)
}

// newIsolatedTestAgent builds an agent with an explicit hardened policy, for
// the tests whose subject is that policy. An unprivileged test runner names its
// own identity; a root runner substitutes a nonzero target. The test-only
// no-credential seam lets adapter tests exercise strict-policy threading without
// pretending that this fixture satisfies the production trusted-root launch
// preconditions.
func newIsolatedTestAgent(opts ...Option) *Agent {
	uid, gid := testIsolationIdentity()
	base := make([]Option, 0, 2+len(opts))
	base = append(base,
		WithProcessIsolation(ProcessIsolation{
			UID: uid, GID: gid,
			BaseEnvironment:     map[string]string{"PATH": os.Getenv("PATH"), "HOME": os.Getenv("HOME")},
			StandaloneOwnerID:   "acp-go-hermes-tests",
			StandaloneStateRoot: filepath.Clean(os.TempDir()),
		}),
		func(options *Options) {
			options.testOnlyNoCredential = true
			options.testOnlyIdentityLockRoot = testIdentityLockRoot()
		},
	)

	return NewAgent(append(base, opts...)...)
}

// testNativeOwnedDir materializes a directory the native-owned predicate
// admits: mode exactly 0700, owned by the isolated identity, under an ancestry
// that identity can traverse. t.TempDir cannot stand in for it — its leaf is
// created 0777&^umask and its parent is 0700, so under umask 022 the mode is
// wrong and the ancestry is closed to any identity but the runner's.
func testNativeOwnedDir(t *testing.T, name string) string {
	t.Helper()
	parent, err := os.MkdirTemp("", "acp-go-hermes-native-owned-")
	if err != nil {
		t.Fatalf("create native-owned parent: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(parent) })
	if err = os.Chmod(parent, 0o711); err != nil {
		t.Fatalf("make native-owned parent traversable: %v", err)
	}

	home := filepath.Join(parent, name)
	if err = os.Mkdir(home, 0o700); err != nil {
		t.Fatalf("create native-owned directory: %v", err)
	}
	if err = os.Chmod(home, 0o700); err != nil {
		t.Fatalf("protect native-owned directory: %v", err)
	}

	uid, gid := testIsolationIdentity()
	if uid != uint32(os.Geteuid()) || gid != uint32(os.Getegid()) {
		if err = os.Chown(home, int(uid), int(gid)); err != nil {
			t.Fatalf("hand native-owned directory to the isolated identity: %v", err)
		}
	}

	return home
}

func testIdentityLockRoot() string {
	root := filepath.Join(os.TempDir(), "acp-go-hermes-agent-identities-"+strconv.Itoa(os.Getpid()))
	if err := os.Mkdir(root, 0o700); err != nil && !os.IsExist(err) {
		panic(err)
	}

	return root
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
	todos             []nativehermes.Todo
	providers         nativehermes.ProvidersResponse

	configProviderCalls int
	setModelCalls       []fakeModelSelection
	setModelErr         error

	pendingPermissions []nativehermes.PermissionRequest
	permissionReplies  []fakePermissionReply
	pendingQuestions   []nativehermes.QuestionRequest
	questionReplies    []fakeQuestionReply
	questionRejects    []fakeQuestionReject
	getSessionIDs      []string

	createSessionFunc func(context.Context, string) (nativehermes.Session, error)
	sendMessage       func(context.Context, string, nativehermes.MessageRequest) (nativehermes.NativeMessage, error)
	messagesFunc      func(context.Context, string) ([]nativehermes.NativeMessage, error)

	aborts               []string
	deleted              []string
	closed               bool
	closeCalls           int
	events               chan nativehermes.TurnEvent
	errs                 chan error
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
	todosErr             error
	providersErr         error
	permissionsErr       error
	questionsErr         error
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

	return message, nil
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

func (c *fakeHermesClient) Todos(context.Context, string) ([]nativehermes.Todo, error) {
	return append([]nativehermes.Todo(nil), c.todos...), c.todosErr
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

func (c *fakeHermesClient) SharedSessionOwnerProcessIdentity() (int, string, error) {
	pid := os.Getpid()
	identity, err := nativehermes.InspectProcess(pid)
	if err != nil {
		return 0, "", err
	}

	return pid, identity.StartTime, nil
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
