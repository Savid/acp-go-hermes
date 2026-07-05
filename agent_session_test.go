package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
)

// toggleReplaceStore wraps InMemorySessionStore and can be switched to fail all
// Replace calls, simulating a disk-full/store outage after native success.
type toggleReplaceStore struct {
	*InMemorySessionStore
	mu   sync.Mutex
	fail bool
}

func (s *toggleReplaceStore) setFail(fail bool) {
	s.mu.Lock()
	s.fail = fail
	s.mu.Unlock()
}

func (s *toggleReplaceStore) Replace(ctx context.Context, main SessionKey, replacements []SessionStoreReplacement) error {
	s.mu.Lock()
	fail := s.fail
	s.mu.Unlock()
	if fail {
		return errors.New("replace failed")
	}
	return s.InMemorySessionStore.Replace(ctx, main, replacements)
}

// TestNewSessionSnapshotFailureLeavesNoOrphan proves HW2: a failed initial
// snapshot removes the session from the active map and native state.
func TestNewSessionSnapshotFailureLeavesNoOrphan(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	cwd := t.TempDir()
	store := &toggleReplaceStore{InMemorySessionStore: NewInMemorySessionStore(), fail: true}
	createClient := newFakeHermesClient()
	createClient.createSession = testNativeSession("native-created")
	createClient.getSession = createClient.createSession
	// A native close error during cleanup is logged, not surfaced.
	createClient.closeErr = errors.New("close boom")
	agent := NewAgent(WithHome(root), WithSessionStore(store), func(options *Options) {
		options.clientFactory = func(_ context.Context, opts hermesStartOptions) (hermesClient, error) {
			xdg, err := createXDGDirs(opts.Root, string(opts.ACPSessionID))
			if err != nil {
				return nil, err
			}
			createClient.xdg = xdg
			return createClient, nil
		}
	})

	if _, err := agent.NewSession(ctx, NewSessionRequest(cwd)); err == nil {
		t.Fatal("NewSession snapshot failure was ignored")
	}
	agent.mu.Lock()
	activeCount := len(agent.sessions)
	agent.mu.Unlock()
	if activeCount != 0 {
		t.Fatalf("failed session still active: %d", activeCount)
	}
	listResp, err := agent.ListSessions(ctx, acp.ListSessionsRequest{})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(listResp.Sessions) != 0 {
		t.Fatalf("failed session still listable: %#v", listResp.Sessions)
	}
	if !createClient.closed {
		t.Fatal("native client not closed after failed snapshot")
	}
	if len(createClient.deleted) != 1 || createClient.deleted[0] != "native-created" {
		t.Fatalf("native session not deleted: %#v", createClient.deleted)
	}
	if _, err := os.Stat(createClient.xdg.Root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("XDG root not removed after failed snapshot: %v", err)
	}
}

// TestForkSnapshotFailureLeavesNoOrphan proves HW2 for the fork path.
func TestForkSnapshotFailureLeavesNoOrphan(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	cwd := t.TempDir()
	store := &toggleReplaceStore{InMemorySessionStore: NewInMemorySessionStore()}
	parent := newFakeHermesClient()
	parent.createSession = testNativeSession("native-parent")
	parent.getSession = parent.createSession
	parent.forkSession = testNativeSession("native-child")
	child := newFakeHermesClient()
	child.getSession = testNativeSession("native-child")
	factoryCalls := 0
	agent := NewAgent(WithHome(root), WithSessionStore(store), func(options *Options) {
		options.clientFactory = func(_ context.Context, opts hermesStartOptions) (hermesClient, error) {
			factoryCalls++
			client := parent
			if factoryCalls > 1 {
				client = child
			}
			xdg := opts.ExistingXDG
			if xdg.Root == "" {
				var err error
				xdg, err = createXDGDirs(opts.Root, string(opts.ACPSessionID))
				if err != nil {
					return nil, err
				}
			}
			client.xdg = xdg
			return client, nil
		}
	})

	parentResp, err := agent.NewSession(ctx, NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	store.setFail(true)
	rawFork, err := json.Marshal(ForkSessionRequest(parentResp.SessionId, cwd))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agent.HandleExtensionMethod(ctx, ForkSessionMethod, rawFork); err == nil {
		t.Fatal("fork snapshot failure was ignored")
	}
	agent.mu.Lock()
	activeCount := len(agent.sessions)
	agent.mu.Unlock()
	if activeCount != 1 {
		t.Fatalf("fork left extra active session: %d", activeCount)
	}
	if _, err := agent.session(parentResp.SessionId); err != nil {
		t.Fatalf("parent session lost after failed fork: %v", err)
	}
	if !child.closed {
		t.Fatal("child native client not closed after failed fork snapshot")
	}
	if len(child.deleted) != 1 || child.deleted[0] != "native-child" {
		t.Fatalf("child native session not deleted: %#v", child.deleted)
	}
	if _, err := os.Stat(child.xdg.Root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("child XDG root not removed after failed fork snapshot: %v", err)
	}
}

func TestAgentSessionLifecycleConfigDeleteAndForkLineage(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	parent := newFakeHermesClient()
	parent.createSession = testNativeSession("native-parent")
	parent.getSession = parent.createSession
	parent.forkSession = testNativeSession("native-child")
	parent.providers = testProviders()
	child := newFakeHermesClient()
	child.getSession = testNativeSession("native-child")
	child.providers = parent.providers
	store := NewInMemorySessionStore()
	factoryCalls := 0
	agent := NewAgent(
		WithHome(root),
		WithSessionStore(store),
		func(options *Options) {
			options.clientFactory = func(_ context.Context, opts hermesStartOptions) (hermesClient, error) {
				factoryCalls++
				client := parent
				if factoryCalls > 1 {
					client = child
				}
				xdg := opts.ExistingXDG
				if xdg.Root == "" {
					var err error
					xdg, err = createXDGDirs(opts.Root, string(opts.ACPSessionID))
					if err != nil {
						return nil, err
					}
				}
				client.xdg = xdg
				return client, nil
			}
		},
	)
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	cwd := t.TempDir()

	newResp, err := agent.NewSession(ctx, NewSessionRequest(cwd, WithSessionHermesOptions(NewHermesOptions(
		WithHermesModel("openai/gpt-test"),
	))))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if newResp.SessionId == "" || len(newResp.ConfigOptions) != 1 {
		t.Fatalf("new response = %#v", newResp)
	}
	if _, err := agent.SetSessionConfigOption(ctx, SetModelRequest(newResp.SessionId, "openai/gpt-other")); err != nil {
		t.Fatalf("SetModel: %v", err)
	}
	if _, err := agent.SetSessionConfigOption(ctx, SetConfigOptionRequest(newResp.SessionId, acp.SessionConfigId("mode"), "plan")); err == nil {
		t.Fatal("mode config option unexpectedly accepted")
	}
	if _, err := agent.SetSessionConfigOption(ctx, acp.SetSessionConfigOptionRequest{
		Boolean: &acp.SetSessionConfigOptionBoolean{SessionId: newResp.SessionId, ConfigId: configModel, Type: "boolean", Value: true},
	}); err == nil {
		t.Fatal("boolean config option unexpectedly accepted")
	}
	listResp, err := agent.ListSessions(ctx, ListSessionsRequest(WithListSessionsCwd(cwd)))
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(listResp.Sessions) != 1 {
		t.Fatalf("list sessions = %#v", listResp.Sessions)
	}
	if err := os.WriteFile(filepath.Join(parent.xdg.Root, "state.db"), []byte("parent-state"), 0o600); err != nil {
		t.Fatalf("seed parent state db: %v", err)
	}

	rawFork, err := json.Marshal(ForkSessionRequest(newResp.SessionId, cwd))
	if err != nil {
		t.Fatal(err)
	}
	forkAny, err := agent.HandleExtensionMethod(ctx, ForkSessionMethod, rawFork)
	if err != nil {
		t.Fatalf("fork extension: %v", err)
	}
	forkResp := forkAny.(acp.UnstableForkSessionResponse)
	if forkResp.SessionId == "" || forkResp.SessionId == newResp.SessionId {
		t.Fatalf("fork response = %#v", forkResp)
	}
	idEntries, err := store.Load(ctx, SessionKey{SessionID: string(forkResp.SessionId), Subpath: idmapSubpath})
	if err != nil {
		t.Fatalf("load child idmap: %v", err)
	}
	var idmap idmapRecord
	if err := json.Unmarshal(idEntries[len(idEntries)-1], &idmap); err != nil {
		t.Fatal(err)
	}
	if idmap.ParentSessionID != string(newResp.SessionId) || idmap.NativeParentSessionID != "native-parent" {
		t.Fatalf("child idmap lineage = %#v", idmap)
	}
	if data, err := os.ReadFile(filepath.Join(child.xdg.Root, "state.db")); err != nil || string(data) != "parent-state" {
		t.Fatalf("child state db clone = %q err=%v", data, err)
	}

	if _, err := agent.CloseSession(ctx, acp.CloseSessionRequest{SessionId: newResp.SessionId}); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	if _, err := agent.UnstableDeleteSession(ctx, DeleteSessionRequest(forkResp.SessionId)); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
	if len(child.deleted) != 1 || child.deleted[0] != "native-child" {
		t.Fatalf("native delete calls = %#v", child.deleted)
	}
	if _, err := os.Stat(child.xdg.Root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deleted child XDG root still exists: %v", err)
	}
	if _, err := agent.LoadSession(ctx, LoadSessionRequest(forkResp.SessionId, cwd)); err == nil {
		t.Fatal("deleted session loaded")
	}
	if parent.xdg.Root == "" || child.xdg.Root == "" || filepath.Dir(parent.xdg.Root) != root {
		t.Fatalf("xdg roots parent=%#v child=%#v root=%q", parent.xdg, child.xdg, root)
	}
}

func TestLoadSessionHydratesStoredSnapshot(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := NewInMemorySessionStore()
	sourceClient := newFakeHermesClient()
	sourceXDG, err := createXDGDirs(root, "source")
	if err != nil {
		t.Fatal(err)
	}
	sourceClient.xdg = sourceXDG
	agent := NewAgent(WithHome(root), WithSessionStore(store))
	session := testSession(agent, sourceClient)
	session.cwd = root
	if err := session.snapshotToStore(ctx); err != nil {
		t.Fatalf("snapshotToStore: %v", err)
	}

	loadedClient := newFakeHermesClient()
	loadedClient.getSession = testNativeSession("native-1")
	loadedClient.providers = testProviders()
	var replayPart nativePart
	if err := json.Unmarshal([]byte(`{"id":"part-1","sessionID":"native-1","messageID":"user-1","type":"text","text":"hello"}`), &replayPart); err != nil {
		t.Fatal(err)
	}
	loadedClient.messages = []nativeMessage{{
		Info:  nativeMessageInfo{ID: "user-1", SessionID: "native-1", Role: "user"},
		Parts: []nativePart{replayPart},
	}}
	agent.options.clientFactory = func(_ context.Context, opts hermesStartOptions) (hermesClient, error) {
		loadedClient.xdg = opts.ExistingXDG
		return loadedClient, nil
	}
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	resp, err := agent.LoadSession(ctx, LoadSessionRequest("session-1", root))
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if resp.Meta[hermesMetaKey] == nil || conn.updateCount() != 1 {
		t.Fatalf("load resp=%#v updates=%#v", resp, conn.updates)
	}
}

func TestCloseSessionSkipsSnapshotWhileTurnPending(t *testing.T) {
	ctx := context.Background()
	store := newCountingSessionStore()
	client := newFakeHermesClient()
	started := make(chan struct{})
	client.sendMessage = func(ctx context.Context, _ string, _ hermesMessageRequest) (nativeMessage, error) {
		close(started)
		<-ctx.Done()
		return nativeMessage{}, ctx.Err()
	}
	agent := NewAgent(WithSessionStore(store))
	session := testSession(agent, client)
	agent.mu.Lock()
	agent.sessions[session.id] = session
	agent.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		_, err := session.Prompt(ctx, TextPromptRequest(session.id, "blocked"))
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("prompt did not start")
	}
	if _, err := agent.CloseSession(ctx, acp.CloseSessionRequest{SessionId: session.id}); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("prompt returned error after close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("prompt did not finish after close")
	}
	if got := store.replaceCount(); got != 0 {
		t.Fatalf("close wrote snapshot during blocked turn: %d", got)
	}
}

// TestSnapshotFencedAfterAcquireTurn parks a turn after acquireTurn but before
// beginTurn and asserts no Replace happens (HW3 turn-in-flight snapshot fence).
func TestSnapshotFencedAfterAcquireTurn(t *testing.T) {
	ctx := context.Background()
	store := newCountingSessionStore()
	client := newFakeHermesClient()
	agent := NewAgent(WithSessionStore(store))
	session := testSession(agent, client)
	agent.mu.Lock()
	agent.sessions[session.id] = session
	agent.mu.Unlock()

	release, err := session.acquireTurn(ctx)
	if err != nil {
		t.Fatalf("acquireTurn: %v", err)
	}
	if reason := session.snapshotBlockedReason(); reason != "turn" {
		t.Fatalf("fence not set after acquireTurn: %q", reason)
	}
	if err := session.snapshotToStore(ctx); err == nil {
		t.Fatal("snapshot ran while turn in flight")
	}
	if _, err := agent.CloseSession(ctx, acp.CloseSessionRequest{SessionId: session.id}); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	if got := store.replaceCount(); got != 0 {
		t.Fatalf("Replace happened while turn in flight: %d", got)
	}
	release()
	if reason := session.snapshotBlockedReason(); reason != "" {
		t.Fatalf("fence not cleared after release: %q", reason)
	}
}

// TestActiveLoadResumeReusesSession proves HW1/X1: active session/load and
// session/resume validate the request first, reuse the active session without
// starting a second native process, do not overwrite the map, and reject
// invalid/mismatched active requests with the cold-path error.
func TestActiveLoadResumeReusesSession(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	cwd := t.TempDir()
	store := NewInMemorySessionStore()
	client := newFakeHermesClient()
	client.createSession = testNativeSession("native-1")
	client.getSession = client.createSession
	factoryCalls := 0
	agent := NewAgent(WithHome(root), WithSessionStore(store), func(options *Options) {
		options.clientFactory = func(_ context.Context, opts hermesStartOptions) (hermesClient, error) {
			factoryCalls++
			xdg := opts.ExistingXDG
			if xdg.Root == "" {
				var err error
				xdg, err = createXDGDirs(opts.Root, string(opts.ACPSessionID))
				if err != nil {
					return nil, err
				}
			}
			client.xdg = xdg
			return client, nil
		}
	})
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)

	newResp, err := agent.NewSession(ctx, NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	id := newResp.SessionId
	if factoryCalls != 1 {
		t.Fatalf("factory calls after new = %d", factoryCalls)
	}
	active := agent.activeSession(id)

	if _, err := agent.LoadSession(ctx, LoadSessionRequest(id, cwd)); err != nil {
		t.Fatalf("active LoadSession: %v", err)
	}
	if _, err := agent.ResumeSession(ctx, ResumeSessionRequest(id, cwd)); err != nil {
		t.Fatalf("active ResumeSession: %v", err)
	}
	if factoryCalls != 1 {
		t.Fatalf("active load/resume started a second native process: %d", factoryCalls)
	}
	if agent.activeSession(id) != active {
		t.Fatal("active load/resume overwrote the session map entry")
	}

	// Validation runs before the active-session reuse: bad _meta, relative
	// cwd, and SSE MCP all return the cold-path error without reuse.
	badMeta := map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{"unknown": "x"}}}
	if _, err := agent.LoadSession(ctx, LoadSessionRequest(id, cwd, WithSessionMeta(badMeta))); err == nil {
		t.Fatal("active load accepted bad _meta")
	}
	if _, err := agent.ResumeSession(ctx, ResumeSessionRequest(id, cwd, WithSessionMeta(badMeta))); err == nil {
		t.Fatal("active resume accepted bad _meta")
	}
	if _, err := agent.LoadSession(ctx, LoadSessionRequest(id, "relative-cwd")); err == nil {
		t.Fatal("active load accepted relative cwd")
	}
	sse := acp.McpServer{Sse: &acp.McpServerSseInline{Name: "sse", Url: "https://sse.example"}}
	if _, err := agent.LoadSession(ctx, LoadSessionRequest(id, cwd, WithSessionMCPServers(sse))); err == nil {
		t.Fatal("active load accepted SSE MCP server")
	}
	if _, err := agent.ResumeSession(ctx, ResumeSessionRequest(id, cwd, WithSessionMCPServers(sse))); err == nil {
		t.Fatal("active resume accepted SSE MCP server")
	}
	if _, err := agent.LoadSession(ctx, LoadSessionRequest(id, filepath.Join(cwd, "other"))); err == nil {
		t.Fatal("active load accepted mismatched cwd")
	}
	if factoryCalls != 1 {
		t.Fatalf("invalid active load/resume started a native process: %d", factoryCalls)
	}

	if err := agent.Close(); err != nil {
		t.Fatalf("Agent.Close: %v", err)
	}
	if !client.closed {
		t.Fatal("Agent.Close did not close the single native client")
	}
}

func TestAgentSessionLifecycleErrorBranches(t *testing.T) {
	ctx := context.Background()
	cwd := t.TempDir()

	t.Run("new session validation and native errors", func(t *testing.T) {
		closed := NewAgent()
		if err := closed.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := closed.NewSession(ctx, NewSessionRequest(cwd)); err == nil {
			t.Fatal("closed agent accepted new session")
		}
		if _, err := NewAgent().NewSession(ctx, NewSessionRequest("relative")); err == nil {
			t.Fatal("relative cwd accepted")
		}
		if _, err := NewAgent().NewSession(ctx, NewSessionRequest(cwd, WithSessionMCPServers(acp.McpServer{Sse: &acp.McpServerSseInline{Name: "sse", Url: "http://example.test"}}))); err == nil {
			t.Fatal("SSE MCP accepted")
		}
		if _, err := NewAgent().NewSession(ctx, NewSessionRequest(cwd, WithSessionMeta(map[string]any{hermesMetaKey: map[string]any{"unknown": true}}))); err == nil {
			t.Fatal("bad meta accepted")
		}
		factoryErr := NewAgent(func(options *Options) {
			options.clientFactory = func(context.Context, hermesStartOptions) (hermesClient, error) {
				return nil, errors.New("factory failed")
			}
		})
		if _, err := factoryErr.NewSession(ctx, NewSessionRequest(cwd)); err == nil {
			t.Fatal("factory error ignored")
		}
		createErrClient := newFakeHermesClient()
		createErrClient.createErr = errors.New("create failed")
		agent := NewAgent(func(options *Options) {
			options.clientFactory = func(_ context.Context, opts hermesStartOptions) (hermesClient, error) {
				createErrClient.xdg, _ = createXDGDirs(opts.Root, string(opts.ACPSessionID))
				return createErrClient, nil
			}
		})
		if _, err := agent.NewSession(ctx, NewSessionRequest(cwd)); err == nil {
			t.Fatal("create error ignored")
		}
		if !createErrClient.closed {
			t.Fatal("create error did not close client")
		}
	})

	t.Run("load resume and close errors", func(t *testing.T) {
		agent := NewAgent()
		if _, err := agent.LoadSession(ctx, LoadSessionRequest("", cwd)); err == nil {
			t.Fatal("empty load id accepted")
		}
		if _, err := agent.ResumeSession(ctx, ResumeSessionRequest("s", cwd, WithSessionMCPServers(acp.McpServer{Sse: &acp.McpServerSseInline{Name: "sse", Url: "http://example.test"}}))); err == nil {
			t.Fatal("resume accepted unsupported MCP")
		}
		if _, err := agent.LoadSession(ctx, LoadSessionRequest("missing", cwd)); err == nil {
			t.Fatal("unknown load succeeded")
		}
		if _, err := agent.CloseSession(ctx, acp.CloseSessionRequest{SessionId: "missing"}); err == nil {
			t.Fatal("unknown close succeeded")
		}
		if _, err := agent.UnstableDeleteSession(ctx, DeleteSessionRequest("")); err == nil {
			t.Fatal("empty delete accepted")
		}
		errStoreAgent := NewAgent(WithSessionStore(&errorSessionStore{err: errors.New("store failed")}))
		if _, err := errStoreAgent.ListSessions(ctx, ListSessionsRequest()); err == nil {
			t.Fatal("list ignored store error")
		}
		if _, err := errStoreAgent.UnstableDeleteSession(ctx, DeleteSessionRequest("s")); err == nil {
			t.Fatal("delete ignored store error")
		}
	})
}

func TestAgentLoadResumeListPaginationAndForkErrors(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	cwd := t.TempDir()
	store := NewInMemorySessionStore()
	sourceClient := newFakeHermesClient()
	sourceXDG, err := createXDGDirs(root, "source")
	if err != nil {
		t.Fatal(err)
	}
	sourceClient.xdg = sourceXDG
	seedAgent := NewAgent(WithHome(root), WithSessionStore(store))
	seed := testSession(seedAgent, sourceClient)
	seed.cwd = cwd
	if err := seed.snapshotToStore(ctx); err != nil {
		t.Fatalf("snapshotToStore: %v", err)
	}

	loadedClient := newFakeHermesClient()
	loadedClient.getSession = testNativeSession("native-1")
	loadedClient.providers = testProviders()
	loadAgent := NewAgent(WithHome(root), WithSessionStore(store), func(options *Options) {
		options.clientFactory = func(_ context.Context, opts hermesStartOptions) (hermesClient, error) {
			loadedClient.xdg = opts.ExistingXDG
			return loadedClient, nil
		}
	})
	// Cold cwd mismatch (session not yet active) is rejected after hydrate.
	if _, err := loadAgent.LoadSession(ctx, LoadSessionRequest("session-1", t.TempDir())); err == nil {
		t.Fatal("cold cwd mismatch load succeeded")
	}
	if _, err := loadAgent.ResumeSession(ctx, ResumeSessionRequest("session-1", cwd)); err != nil {
		t.Fatalf("ResumeSession: %v", err)
	}
	// Active cwd mismatch (session now active) is rejected before reuse.
	if _, err := loadAgent.LoadSession(ctx, LoadSessionRequest("session-1", t.TempDir())); err == nil {
		t.Fatal("active cwd mismatch load succeeded")
	}

	getErrClient := newFakeHermesClient()
	getErrClient.getErr = errors.New("get failed")
	getErrAgent := NewAgent(WithHome(root), WithSessionStore(store), func(options *Options) {
		options.clientFactory = func(_ context.Context, opts hermesStartOptions) (hermesClient, error) {
			getErrClient.xdg = opts.ExistingXDG
			return getErrClient, nil
		}
	})
	if _, err := getErrAgent.LoadSession(ctx, LoadSessionRequest("session-1", cwd)); err == nil {
		t.Fatal("get error load succeeded")
	}
	if !getErrClient.closed {
		t.Fatal("get error did not close client")
	}

	listStore := NewInMemorySessionStore()
	for i := 0; i < listSessionsPageSize+2; i++ {
		id := fmt.Sprintf("stored-%02d", i)
		entry, _ := json.Marshal(stateSnapshot{
			Format:              SessionStoreFormat,
			CapturedAtUnixMilli: int64(10_000 - i),
			Session:             stateSnapshotSession{SessionID: id, Cwd: cwd, Title: id},
		})
		if err := listStore.Replace(ctx, SessionKey{SessionID: id}, []SessionStoreReplacement{{Key: SessionKey{SessionID: id}, Entries: []SessionStoreEntry{entry}}}); err != nil {
			t.Fatalf("replace list store: %v", err)
		}
	}
	listAgent := NewAgent(WithSessionStore(listStore))
	listResp, err := listAgent.ListSessions(ctx, ListSessionsRequest(WithListSessionsCwd(cwd)))
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(listResp.Sessions) != listSessionsPageSize || listResp.NextCursor == nil {
		t.Fatalf("list resp len=%d next=%v", len(listResp.Sessions), listResp.NextCursor)
	}
	if _, err := listAgent.ListSessions(ctx, ListSessionsRequest(WithListSessionsCursor("bad"))); err == nil {
		t.Fatal("bad cursor accepted")
	}
	cursor := "999"
	empty, err := listAgent.ListSessions(ctx, ListSessionsRequest(WithListSessionsCursor(cursor)))
	if err != nil || len(empty.Sessions) != 0 || empty.NextCursor != nil {
		t.Fatalf("empty page = %#v err=%v", empty, err)
	}

	parentClient := newFakeHermesClient()
	parentClient.forkErr = errors.New("fork failed")
	parent := testSession(NewAgent(), parentClient)
	parentAgent := parent.agent
	parentAgent.mu.Lock()
	parentAgent.sessions[parent.id] = parent
	parentAgent.mu.Unlock()
	if _, err := parentAgent.HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest(parent.id, cwd))); err == nil {
		t.Fatal("fork error ignored")
	}
	if _, err := parentAgent.HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest("missing", cwd))); err == nil {
		t.Fatal("missing parent fork succeeded")
	}
	if _, err := parentAgent.HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest(parent.id, "relative"))); err == nil {
		t.Fatal("relative fork cwd accepted")
	}
}

func TestAgentHelperAndLifecycleBranchCoverage(t *testing.T) {
	ctx := context.Background()
	cwd := t.TempDir()

	invalidOptions := NewAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: -1}))
	if _, err := invalidOptions.Initialize(ctx, acp.InitializeRequest{}); err == nil {
		t.Fatal("invalid construction options were not reported during initialize")
	}
	invalidOptions.options.SessionStore = nil
	if invalidOptions.sessionStore() == nil {
		t.Fatal("nil session store did not fall back")
	}
	storeCtx, cancel := invalidOptions.sessionStoreContext(ctx)
	cancel()
	select {
	case <-storeCtx.Done():
	default:
		t.Fatal("store context was not cancellable")
	}
	invalidOptions.options.SessionStoreLoadTimeout = 0
	defaultStoreCtx, defaultCancel := invalidOptions.sessionStoreContext(ctx)
	defaultCancel()
	select {
	case <-defaultStoreCtx.Done():
	default:
		t.Fatal("default store context was not cancellable")
	}

	closed := NewAgent()
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := closed.HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest("s", cwd))); err == nil {
		t.Fatal("closed agent accepted extension method")
	}

	limitAgent := NewAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))
	first := testSession(limitAgent, newFakeHermesClient())
	if err := limitAgent.storeStartedSession(first); err != nil {
		t.Fatalf("store first session: %v", err)
	}
	second := testSession(limitAgent, newFakeHermesClient())
	second.id = "second"
	if err := limitAgent.storeStartedSession(second); err == nil {
		t.Fatal("active-session backpressure was not enforced")
	}
	if limitAgent.removeSessionIf(second.id, second) {
		t.Fatal("removeSessionIf removed a non-current session")
	}
	if _, err := limitAgent.session("deleted"); err == nil {
		t.Fatal("unknown deleted session unexpectedly resolved before deletion mark")
	}
	limitAgent.deleted["deleted"] = struct{}{}
	if _, err := limitAgent.session("deleted"); err == nil {
		t.Fatal("deleted session unexpectedly resolved")
	}
	limitAgent.closed = true
	if err := limitAgent.storeStartedSession(second); err == nil {
		t.Fatal("closed agent stored session")
	}

	client := newFakeHermesClient()
	defaultSession := newSession(NewAgent(), "wrapper", cwd, nil, nativeSession{ID: "native"}, client, sessionMeta{}, idmapRecord{})
	if defaultSession.title != "Hermes session" || defaultSession.idmap.SessionID != "wrapper" ||
		defaultSession.idmap.NativeSessionID != "native" || defaultSession.idmap.Format != SessionStoreFormat {
		t.Fatalf("default session fields = %#v", defaultSession)
	}
	if defaultSession.modelSelector() != nil {
		t.Fatal("empty model selector was not nil")
	}
	defaultSession.Close(ctx)
	if err := defaultSession.Close(ctx); err != nil {
		t.Fatalf("second session close: %v", err)
	}

	queued := defaultSession.turnQueue()
	queued <- struct{}{}
	cancelled, cancelAcquire := context.WithCancel(ctx)
	cancelAcquire()
	if _, err := defaultSession.acquireTurn(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled acquireTurn error = %v", err)
	}
	if _, err := defaultSession.acquireTurn(ctx); err == nil {
		t.Fatal("prompt backpressure was not enforced")
	}
	<-queued
	if provider, model := splitModelValue("", "provider", "model"); provider != "provider" || model != "model" {
		t.Fatalf("empty split fallback = %q/%q", provider, model)
	}

	createClient := newFakeHermesClient()
	createClient.createSession = testNativeSession("native-created")
	snapshotErrAgent := NewAgent(
		WithSessionStore(&errorSessionStore{err: errors.New("replace failed")}),
		func(options *Options) {
			options.clientFactory = func(_ context.Context, opts hermesStartOptions) (hermesClient, error) {
				var err error
				createClient.xdg, err = createXDGDirs(opts.Root, string(opts.ACPSessionID))
				if err != nil {
					return nil, err
				}
				return createClient, nil
			}
		},
	)
	if _, err := snapshotErrAgent.NewSession(ctx, NewSessionRequest(cwd)); err == nil {
		t.Fatal("snapshot store error during new session was ignored")
	}
	if !createClient.closed {
		t.Fatal("snapshot failure did not close new session client")
	}

	store := NewInMemorySessionStore()
	sourceClient := newFakeHermesClient()
	sourceXDG, err := createXDGDirs(t.TempDir(), "source")
	if err != nil {
		t.Fatal(err)
	}
	sourceClient.xdg = sourceXDG
	seedAgent := NewAgent(WithSessionStore(store))
	seed := testSession(seedAgent, sourceClient)
	seed.cwd = cwd
	if err := seed.snapshotToStore(ctx); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}
	replayErrClient := newFakeHermesClient()
	replayErrClient.getSession = testNativeSession("native-1")
	replayErrClient.messagesErr = errors.New("messages failed")
	replayErrAgent := NewAgent(WithSessionStore(store), func(options *Options) {
		options.clientFactory = func(_ context.Context, opts hermesStartOptions) (hermesClient, error) {
			replayErrClient.xdg = opts.ExistingXDG
			return replayErrClient, nil
		}
	})
	if _, err := replayErrAgent.LoadSession(ctx, LoadSessionRequest("session-1", cwd)); err == nil {
		t.Fatal("load replay error was ignored")
	}

	if _, err := closed.ResumeSession(ctx, ResumeSessionRequest("session-1", cwd)); err == nil {
		t.Fatal("closed agent resumed session")
	}
	if _, err := NewAgent(WithHome(string([]byte{0}))).LoadSession(ctx, LoadSessionRequest("session-1", cwd)); err == nil {
		t.Fatal("invalid home root did not fail load")
	}
	if _, err := NewAgent(WithSessionStore(&errorSessionStore{err: errors.New("load failed")})).LoadSession(ctx, LoadSessionRequest("session-1", cwd)); err == nil {
		t.Fatal("hydrate store error was ignored")
	}

	parentClient := newFakeHermesClient()
	parentClient.forkSession = testNativeSession("native-child")
	parent := testSession(NewAgent(WithHome(string([]byte{0}))), parentClient)
	parentAgent := parent.agent
	parentAgent.sessions[parent.id] = parent
	if _, err := parentAgent.HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest(parent.id, cwd))); err == nil {
		t.Fatal("invalid fork home root did not fail")
	}
	if _, err := NewAgent().HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest(parent.id, cwd, WithSessionMCPServers(acp.McpServer{Sse: &acp.McpServerSseInline{Name: "sse"}})))); err == nil {
		t.Fatal("unstable fork accepted SSE MCP")
	}
	if _, err := NewAgent().HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest(parent.id, cwd, WithSessionMCPServers(acp.McpServer{Acp: &acp.McpServerAcpInline{Name: "acp"}})))); err == nil {
		t.Fatal("unstable fork accepted ACP MCP")
	}
	if _, err := NewAgent().HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest(parent.id, cwd, WithSessionMeta(map[string]any{hermesMetaKey: map[string]any{"bad": true}})))); err == nil {
		t.Fatal("unstable fork accepted invalid meta")
	}

	validTarget, err := createXDGDirs(t.TempDir(), "target")
	if err != nil {
		t.Fatal(err)
	}
	if err := cloneHermesStateDB(xdgDirs{Root: string([]byte{0})}, validTarget); err == nil {
		t.Fatal("cloneHermesStateDB accepted invalid source")
	}
	restoreStateStoreSeams(t)
	stateRemoveAll = func(string) error { return errors.New("remove failed") }
	validSource, err := createXDGDirs(t.TempDir(), "source")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(validSource.Root, "state.db"), []byte("state"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cloneHermesStateDB(validSource, validTarget); err == nil {
		t.Fatal("cloneHermesStateDB ignored decode error")
	}
}

func TestAgentRemainingLifecycleBranches(t *testing.T) {
	ctx := context.Background()
	cwd := t.TempDir()

	t.Run("new session id and store errors", func(t *testing.T) {
		defaultClient := newFakeHermesClient()
		defaultClient.createSession = nativeSession{ID: "native-default", Title: "Default"}
		var defaultModel string
		defaultAgent := NewAgent(WithDefaultModel("openai/gpt-default"), func(options *Options) {
			options.clientFactory = func(_ context.Context, opts hermesStartOptions) (hermesClient, error) {
				defaultModel = opts.DefaultModel
				var err error
				defaultClient.xdg, err = createXDGDirs(opts.Root, string(opts.ACPSessionID))
				if err != nil {
					return nil, err
				}
				return defaultClient, nil
			}
		})
		defaultResp, err := defaultAgent.NewSession(ctx, NewSessionRequest(cwd))
		if err != nil {
			t.Fatalf("NewSession with default model: %v", err)
		}
		if defaultModel != "openai/gpt-default" {
			t.Fatalf("start default model = %q", defaultModel)
		}
		defaultMeta, _ := defaultResp.Meta[hermesMetaKey].(map[string]any)
		if defaultMeta["modelId"] != "openai/gpt-default" {
			t.Fatalf("default model meta = %#v", defaultMeta)
		}

		oldReader := sessionIDRandReader
		sessionIDRandReader = errorReader{err: errors.New("id failed")}
		if _, err := NewAgent().NewSession(ctx, NewSessionRequest(cwd)); err == nil {
			t.Fatal("NewSession ignored session id error")
		}
		sessionIDRandReader = oldReader

		client := newFakeHermesClient()
		client.createSession = testNativeSession("native-created")
		agent := NewAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}), func(options *Options) {
			options.clientFactory = func(_ context.Context, opts hermesStartOptions) (hermesClient, error) {
				var err error
				client.xdg, err = createXDGDirs(opts.Root, string(opts.ACPSessionID))
				if err != nil {
					return nil, err
				}
				return client, nil
			}
		})
		agent.sessions["existing"] = testSession(agent, newFakeHermesClient())
		if _, err := agent.NewSession(ctx, NewSessionRequest(cwd)); err == nil {
			t.Fatal("NewSession ignored storeStartedSession error")
		}
		if !client.closed {
			t.Fatal("storeStartedSession error did not close client")
		}
	})

	t.Run("load validation and startup errors", func(t *testing.T) {
		agent := NewAgent()
		if _, err := agent.LoadSession(ctx, LoadSessionRequest("s", "relative")); err == nil {
			t.Fatal("LoadSession accepted relative cwd")
		}
		if _, err := agent.LoadSession(ctx, LoadSessionRequest("s", cwd, WithSessionMCPServers(acp.McpServer{Sse: &acp.McpServerSseInline{Name: "sse"}}))); err == nil {
			t.Fatal("LoadSession accepted SSE MCP")
		}
		if _, err := agent.LoadSession(ctx, LoadSessionRequest("s", cwd, WithSessionMeta(map[string]any{hermesMetaKey: map[string]any{"bad": true}}))); err == nil {
			t.Fatal("LoadSession accepted invalid meta")
		}

		store := validHydrateStore(t, ctx)
		factoryErrAgent := NewAgent(WithSessionStore(store), func(options *Options) {
			options.clientFactory = func(context.Context, hermesStartOptions) (hermesClient, error) {
				return nil, errors.New("factory failed")
			}
		})
		if _, err := factoryErrAgent.LoadSession(ctx, LoadSessionRequest("s", cwd)); err == nil {
			t.Fatal("LoadSession ignored client factory error")
		}

		loadedClient := newFakeHermesClient()
		loadedClient.getSession = testNativeSession("n")
		limitAgent := NewAgent(WithSessionStore(store), WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}), func(options *Options) {
			options.clientFactory = func(_ context.Context, opts hermesStartOptions) (hermesClient, error) {
				loadedClient.xdg = opts.ExistingXDG
				return loadedClient, nil
			}
		})
		limitAgent.sessions["existing"] = testSession(limitAgent, newFakeHermesClient())
		if _, err := limitAgent.LoadSession(ctx, LoadSessionRequest("s", cwd)); err == nil {
			t.Fatal("LoadSession ignored storeStartedSession error")
		}
	})

	t.Run("list filters and errors", func(t *testing.T) {
		closed := NewAgent()
		if err := closed.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := closed.ListSessions(ctx, ListSessionsRequest()); err == nil {
			t.Fatal("closed agent listed sessions")
		}
		relative := "relative"
		if _, err := NewAgent().ListSessions(ctx, acp.ListSessionsRequest{Cwd: &relative}); err == nil {
			t.Fatal("ListSessions accepted relative cwd")
		}

		store := NewInMemorySessionStore()
		for _, item := range []struct {
			id  string
			cwd string
		}{
			{"active", cwd},
			{"deleted", cwd},
			{"other-cwd", t.TempDir()},
		} {
			entry, _ := json.Marshal(stateSnapshot{
				Format:              SessionStoreFormat,
				CapturedAtUnixMilli: 100,
				Session:             stateSnapshotSession{SessionID: item.id, Cwd: item.cwd, Title: item.id},
			})
			if err := store.Replace(ctx, SessionKey{SessionID: item.id}, []SessionStoreReplacement{{Key: SessionKey{SessionID: item.id}, Entries: []SessionStoreEntry{entry}}}); err != nil {
				t.Fatal(err)
			}
		}
		agent := NewAgent(WithSessionStore(store))
		active := testSession(agent, newFakeHermesClient())
		active.id = "active"
		active.cwd = cwd
		agent.sessions[active.id] = active
		otherActive := testSession(agent, newFakeHermesClient())
		otherActive.id = "other-active"
		otherActive.cwd = t.TempDir()
		agent.sessions[otherActive.id] = otherActive
		agent.deleted["deleted"] = struct{}{}
		resp, err := agent.ListSessions(ctx, ListSessionsRequest(WithListSessionsCwd(cwd)))
		if err != nil {
			t.Fatalf("ListSessions: %v", err)
		}
		if len(resp.Sessions) != 1 || resp.Sessions[0].SessionId != "active" {
			t.Fatalf("filtered list = %#v", resp.Sessions)
		}
	})

	t.Run("delete active close error", func(t *testing.T) {
		client := newFakeHermesClient()
		client.closeErr = errors.New("close failed")
		agent := NewAgent()
		session := testSession(agent, client)
		agent.sessions[session.id] = session
		if _, err := agent.UnstableDeleteSession(ctx, DeleteSessionRequest(session.id)); err == nil {
			t.Fatal("delete ignored active close error")
		}
	})

	t.Run("delete active ignores native delete error after tombstone", func(t *testing.T) {
		client := newFakeHermesClient()
		client.deleteErr = errors.New("native delete failed")
		agent := NewAgent()
		session := testSession(agent, client)
		agent.sessions[session.id] = session
		if _, err := agent.UnstableDeleteSession(ctx, DeleteSessionRequest(session.id)); err != nil {
			t.Fatalf("delete returned native delete error: %v", err)
		}
		if len(client.deleted) != 1 || client.deleted[0] != "native-1" {
			t.Fatalf("native delete attempts = %#v", client.deleted)
		}
	})

	t.Run("deleted cleanup retry entrypoints", func(t *testing.T) {
		root := t.TempDir()
		for _, name := range []string{"list", "load", "resume", "delete"} {
			t.Run(name, func(t *testing.T) {
				agent := NewAgent(WithHome(root))
				xdg, err := createXDGDirs(root, name)
				if err != nil {
					t.Fatal(err)
				}
				agent.deleteCleanup[acp.SessionId(name)] = deleteCleanupRecord{
					SessionID: acp.SessionId(name),
					XDGRoot:   xdg.Root,
				}
				switch name {
				case "list":
					_, _ = agent.ListSessions(ctx, ListSessionsRequest())
				case "load":
					agent.deleted[acp.SessionId(name)] = struct{}{}
					_, _ = agent.LoadSession(ctx, LoadSessionRequest(acp.SessionId(name), cwd))
				case "resume":
					agent.deleted[acp.SessionId(name)] = struct{}{}
					_, _ = agent.ResumeSession(ctx, ResumeSessionRequest(acp.SessionId(name), cwd))
				case "delete":
					_, _ = agent.UnstableDeleteSession(ctx, DeleteSessionRequest(acp.SessionId(name)))
				}
				if _, err := os.Stat(xdg.Root); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("%s cleanup did not remove XDG root: %v", name, err)
				}
				if _, ok := agent.deleteCleanup[acp.SessionId(name)]; ok {
					t.Fatalf("%s cleanup metadata was not cleared", name)
				}
			})
		}
	})

	t.Run("deleted cleanup helper branches", func(t *testing.T) {
		agent := NewAgent(WithHome(t.TempDir()))
		agent.rememberDeleteCleanup(deleteCleanupRecord{})
		if len(agent.deleteCleanup) != 0 {
			t.Fatalf("empty cleanup record was remembered: %#v", agent.deleteCleanup)
		}
		agent.forgetDeleteCleanupIfDone("")

		xdg, err := createXDGDirs(agent.options.Home, "keep")
		if err != nil {
			t.Fatal(err)
		}
		agent.deleteCleanup["keep"] = deleteCleanupRecord{SessionID: "keep", XDGRoot: xdg.Root}
		agent.forgetDeleteCleanupIfDone("keep")
		if _, ok := agent.deleteCleanup["keep"]; !ok {
			t.Fatal("cleanup metadata was forgotten while XDG root still existed")
		}

		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if err := agent.retryDeletedSessionCleanup(cancelled); err == nil {
			t.Fatal("cancelled cleanup retry returned nil")
		}
		errorAgent := NewAgent()
		errorAgent.deleteCleanup["bad"] = deleteCleanupRecord{SessionID: "bad", XDGRoot: string([]byte{0})}
		if err := errorAgent.retryDeletedSessionCleanup(ctx); err == nil {
			t.Fatal("cleanup error retry returned nil")
		}
		for _, name := range []string{"list", "load", "delete"} {
			t.Run("entrypoint retry error "+name, func(t *testing.T) {
				entryAgent := NewAgent(WithHome(t.TempDir()))
				entryXDG, err := createXDGDirs(entryAgent.options.Home, name)
				if err != nil {
					t.Fatal(err)
				}
				entryAgent.deleteCleanup[acp.SessionId(name)] = deleteCleanupRecord{
					SessionID: acp.SessionId(name),
					XDGRoot:   entryXDG.Root,
				}
				switch name {
				case "list":
					_, _ = entryAgent.ListSessions(cancelled, ListSessionsRequest())
				case "load":
					_, _ = entryAgent.LoadSession(cancelled, LoadSessionRequest(acp.SessionId(name), cwd))
				case "delete":
					_, _ = entryAgent.UnstableDeleteSession(cancelled, DeleteSessionRequest(acp.SessionId(name)))
				}
			})
		}

		if err := agent.cleanupDeletedSession(deleteCleanupRecord{}); err != nil {
			t.Fatalf("empty cleanup err = %v", err)
		}
		if err := agent.cleanupDeletedSession(deleteCleanupRecord{SessionID: "bad", XDGRoot: string([]byte{0})}); err == nil {
			t.Fatal("invalid cleanup root returned nil")
		}
	})

	t.Run("fork errors", func(t *testing.T) {
		oldReader := sessionIDRandReader
		t.Cleanup(func() { sessionIDRandReader = oldReader })

		parentClient := newFakeHermesClient()
		parentClient.forkSession = testNativeSession("native-child")
		parentClient.xdg, _ = createXDGDirs(t.TempDir(), "parent")
		parentAgent := NewAgent()
		parent := testSession(parentAgent, parentClient)
		parentAgent.sessions[parent.id] = parent

		sessionIDRandReader = errorReader{err: errors.New("id failed")}
		if _, err := parentAgent.HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest(parent.id, cwd))); err == nil {
			t.Fatal("fork ignored session id error")
		}
		sessionIDRandReader = oldReader

		parentClient.xdg = xdgDirs{Root: string([]byte{0})}
		if _, err := parentAgent.HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest(parent.id, cwd))); err == nil {
			t.Fatal("fork ignored state db clone error")
		}
		parentClient.xdg, _ = createXDGDirs(t.TempDir(), "parent")

		factoryErrAgent := NewAgent(func(options *Options) {
			options.clientFactory = func(context.Context, hermesStartOptions) (hermesClient, error) {
				return nil, errors.New("child factory failed")
			}
		})
		factoryParent := testSession(factoryErrAgent, parentClient)
		factoryErrAgent.sessions[factoryParent.id] = factoryParent
		if _, err := factoryErrAgent.HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest(factoryParent.id, cwd))); err == nil {
			t.Fatal("fork ignored child factory error")
		}

		getErrClient := newFakeHermesClient()
		getErrClient.getErr = errors.New("get failed")
		getErrAgent := NewAgent(func(options *Options) {
			options.clientFactory = func(_ context.Context, opts hermesStartOptions) (hermesClient, error) {
				getErrClient.xdg = opts.ExistingXDG
				return getErrClient, nil
			}
		})
		getParent := testSession(getErrAgent, parentClient)
		getErrAgent.sessions[getParent.id] = getParent
		if _, err := getErrAgent.HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest(getParent.id, cwd))); err == nil {
			t.Fatal("fork ignored child get error")
		}
		if !getErrClient.closed {
			t.Fatal("child get error did not close client")
		}

		limitChild := newFakeHermesClient()
		limitChild.getSession = testNativeSession("native-child")
		limitAgent := NewAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}), func(options *Options) {
			options.clientFactory = func(_ context.Context, opts hermesStartOptions) (hermesClient, error) {
				limitChild.xdg = opts.ExistingXDG
				return limitChild, nil
			}
		})
		limitParent := testSession(limitAgent, parentClient)
		limitAgent.sessions[limitParent.id] = limitParent
		if _, err := limitAgent.HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest(limitParent.id, cwd))); err == nil {
			t.Fatal("fork ignored active-session limit")
		}

		snapshotErrChild := newFakeHermesClient()
		snapshotErrChild.getSession = testNativeSession("native-child")
		snapshotErrAgent := NewAgent(WithSessionStore(&errorSessionStore{err: errors.New("replace failed")}), func(options *Options) {
			options.clientFactory = func(_ context.Context, opts hermesStartOptions) (hermesClient, error) {
				snapshotErrChild.xdg = opts.ExistingXDG
				return snapshotErrChild, nil
			}
		})
		snapshotParent := testSession(snapshotErrAgent, parentClient)
		snapshotErrAgent.sessions[snapshotParent.id] = snapshotParent
		if _, err := snapshotErrAgent.HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest(snapshotParent.id, cwd))); err == nil {
			t.Fatal("fork ignored snapshot error")
		}
		if !snapshotErrChild.closed {
			t.Fatal("fork snapshot error did not close child")
		}
	})

	t.Run("client factory defaults and env merge", func(t *testing.T) {
		defaultAgent := NewAgent()
		defaultAgent.options.clientFactory = nil
		if _, err := defaultAgent.newHermesClient(ctx, "s", cwd, sessionMeta{}, xdgDirs{Root: filepath.Join(t.TempDir(), "root")}); err == nil {
			t.Fatal("default client factory unexpectedly succeeded with incomplete XDG")
		}
		var captured hermesStartOptions
		agent := NewAgent(func(options *Options) {
			options.clientFactory = func(_ context.Context, opts hermesStartOptions) (hermesClient, error) {
				captured = opts
				return newFakeHermesClient(), nil
			}
		})
		if _, err := agent.newHermesClient(ctx, "s", cwd, sessionMeta{Env: map[string]string{"A": "1"}}, xdgDirs{}); err != nil {
			t.Fatalf("newHermesClient env: %v", err)
		}
		if captured.Env["A"] != "1" {
			t.Fatalf("captured env = %#v", captured.Env)
		}
	})
}

func testProviders() providersResponse {
	return providersResponse{Providers: []providerInfo{{
		ID:   "openai",
		Name: "OpenAI",
		Models: map[string]providerModel{
			"gpt-test": {
				ID:   "gpt-test",
				Name: "GPT Test",
				Limit: map[string]any{
					"context": float64(1000),
					"output":  float64(200),
				},
				Reasoning: true,
				ToolCall:  true,
			},
			"gpt-other": {ID: "gpt-other", Name: "GPT Other"},
		},
	}}}
}
