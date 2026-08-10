package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

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
	// A native close error during cleanup is joined with the snapshot failure.
	createClient.closeErr = errors.New("close boom")
	agent := newTestAgent(WithScratchDir(root), WithSessionStore(store), func(options *Options) {
		options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
			xdg, err := nativehermes.CreateXDGDirs(opts.Root, string(opts.ACPSessionID))
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
	agent := newTestAgent(WithScratchDir(root), WithSessionStore(store), func(options *Options) {
		options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
			factoryCalls++
			client := parent
			if factoryCalls > 1 {
				client = child
			}
			xdg := opts.ExistingXDG
			if xdg.Root == "" {
				var err error
				xdg, err = nativehermes.CreateXDGDirs(opts.Root, string(opts.ACPSessionID))
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
	agent := newTestAgent(
		WithScratchDir(root),
		WithSessionStore(store),
		func(options *Options) {
			options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
				factoryCalls++
				client := parent
				if factoryCalls > 1 {
					client = child
				}
				xdg := opts.ExistingXDG
				if xdg.Root == "" {
					var err error
					xdg, err = nativehermes.CreateXDGDirs(opts.Root, string(opts.ACPSessionID))
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
	if _, err2 := agent.SetSessionConfigOption(ctx, SetModelRequest(newResp.SessionId, "openai/gpt-other")); err2 != nil {
		t.Fatalf("SetModel: %v", err2)
	}
	if _, err3 := agent.SetSessionConfigOption(ctx, SetConfigOptionRequest(newResp.SessionId, acp.SessionConfigId("mode"), "plan")); err3 == nil {
		t.Fatal("mode config option unexpectedly accepted")
	}
	if _, err4 := agent.SetSessionConfigOption(ctx, acp.SetSessionConfigOptionRequest{
		Boolean: &acp.SetSessionConfigOptionBoolean{SessionId: newResp.SessionId, ConfigId: configModel, Type: "boolean", Value: true},
	}); err4 == nil {
		t.Fatal("boolean config option unexpectedly accepted")
	}
	listResp, err := agent.ListSessions(ctx, ListSessionsRequest(WithListSessionsCwd(cwd)))
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(listResp.Sessions) != 1 {
		t.Fatalf("list sessions = %#v", listResp.Sessions)
	}
	if err5 := os.WriteFile(filepath.Join(parent.xdg.Root, "state.db"), []byte("parent-state"), 0o600); err5 != nil {
		t.Fatalf("seed parent state db: %v", err5)
	}

	forkResp := forkSessionAndAssertLineage(ctx, t, agent, store, child, newResp.SessionId, cwd)

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
	if parent.xdg.Root == "" || child.xdg.Root == "" || parent.xdg.Root == child.xdg.Root ||
		filepath.Dir(parent.xdg.Root) != agent.options.ScratchDir ||
		!strings.HasPrefix(filepath.Base(parent.xdg.Root), "acp-go-hermes-runtime-") ||
		!strings.HasPrefix(filepath.Base(child.xdg.Root), "acp-go-hermes-runtime-") {
		t.Fatalf("xdg roots parent=%#v child=%#v home=%q", parent.xdg, child.xdg, agent.homeRoot())
	}
}

func forkSessionAndAssertLineage(ctx context.Context, t *testing.T, agent *Agent, store *InMemorySessionStore, child *fakeHermesClient, parentID acp.SessionId, cwd string) acp.UnstableForkSessionResponse {
	t.Helper()

	rawFork, err := json.Marshal(ForkSessionRequest(parentID, cwd))
	if err != nil {
		t.Fatal(err)
	}
	forkAny, err := agent.HandleExtensionMethod(ctx, ForkSessionMethod, rawFork)
	if err != nil {
		t.Fatalf("fork extension: %v", err)
	}
	forkResp, forkOK := forkAny.(acp.UnstableForkSessionResponse)
	if !forkOK || forkResp.SessionId == "" || forkResp.SessionId == parentID {
		t.Fatalf("fork response = %#v", forkAny)
	}
	idEntries, err := store.Load(ctx, SessionKey{SessionID: string(forkResp.SessionId), Subpath: idmapSubpath})
	if err != nil {
		t.Fatalf("load child idmap: %v", err)
	}
	var idmap idmapRecord
	if err := json.Unmarshal(idEntries[len(idEntries)-1], &idmap); err != nil {
		t.Fatal(err)
	}
	if idmap.ParentSessionID != string(parentID) || idmap.NativeParentSessionID != "native-parent" {
		t.Fatalf("child idmap lineage = %#v", idmap)
	}
	if data, err := os.ReadFile(filepath.Join(child.xdg.Root, "state.db")); err != nil || string(data) != "parent-state" {
		t.Fatalf("child state db clone = %q err=%v", data, err)
	}

	return forkResp
}

func TestLoadSessionHydratesStoredSnapshot(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := NewInMemorySessionStore()
	sourceClient := newFakeHermesClient()
	sourceXDG, err := nativehermes.CreateXDGDirs(root, "source")
	if err != nil {
		t.Fatal(err)
	}
	sourceClient.xdg = sourceXDG
	agent := newTestAgent(WithScratchDir(root), WithSessionStore(store))
	session := testSession(agent, sourceClient)
	session.cwd = root
	mcpServer := HTTPMCPServer("wagie", "http://127.0.0.1/mcp", nil)
	session.mcpServers = []acp.McpServer{mcpServer}
	if err6 := session.snapshotToStore(ctx); err6 != nil {
		t.Fatalf("snapshotToStore: %v", err6)
	}

	loadedClient := newFakeHermesClient()
	loadedClient.getSession = testNativeSession("native-1")
	loadedClient.providers = testProviders()
	var replayPart nativehermes.Part
	if err7 := json.Unmarshal([]byte(`{"id":"part-1","sessionID":"native-1","messageID":"user-1","type":"text","text":"hello"}`), &replayPart); err7 != nil {
		t.Fatal(err7)
	}
	loadedClient.messages = []nativehermes.NativeMessage{{
		Info:  nativehermes.NativeMessageInfo{ID: "history-1", SessionID: "native-1", Role: "user"},
		Parts: []nativehermes.Part{replayPart},
	}}
	loadPathDir := t.TempDir()
	var loadedStart nativehermes.StartOptions
	agent.options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
		loadedStart = opts
		loadedClient.xdg = opts.ExistingXDG

		return loadedClient, nil
	}
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	resp, err := agent.LoadSession(ctx, LoadSessionRequest(
		"session-1",
		root,
		WithSessionMCPServers(mcpServer),
		WithSessionHermesOptions(HermesOptions{
			Env:           map[string]string{"WAGIE_API_TOKEN": "loaded-bearer"},
			ExtraPathDirs: []string{loadPathDir},
		}),
	))
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if resp.Meta[hermesMetaKey] == nil || conn.updateCount() != 1 {
		t.Fatalf("load resp=%#v updates=%#v", resp, conn.updates)
	}
	if loadedStart.SessionEnv["WAGIE_API_TOKEN"] != "loaded-bearer" || !slices.Equal(loadedStart.ExtraPathDirs, []string{loadPathDir}) {
		t.Fatalf("loaded carrier = env %#v dirs %#v", loadedStart.SessionEnv, loadedStart.ExtraPathDirs)
	}
	loaded := agent.activeSession("session-1")
	for _, nonce := range []string{"loaded-continuation-1", "loaded-continuation-2"} {
		if _, err := loaded.Prompt(ctx, TextPromptRequest(loaded.id, nonce, "continue")); err != nil {
			t.Fatalf("loaded Prompt(%s): %v", nonce, err)
		}
	}
	loadedClient.mu.Lock()
	reloads := loadedClient.reloadCalls
	loadedClient.mu.Unlock()
	if reloads != 1 {
		t.Fatalf("loaded session MCP reloads = %d, want one before continuation", reloads)
	}
}

func TestResumeRuntimeForTurnFailureAndSuccessBranches(t *testing.T) { //nolint:gocyclo,maintidx // One lifecycle audit keeps every fail-closed branch explicit.
	t.Run("poisoned", func(t *testing.T) {
		session, _, _ := newResumeRuntimeTestSession(t)
		session.poisonCause = "earlier failure"
		if err := session.resumeRuntimeForTurnLocked(t.Context()); err == nil || !strings.Contains(err.Error(), "session_poisoned") {
			t.Fatalf("poisoned resume error = %v", err)
		}
	})

	t.Run("closed", func(t *testing.T) {
		session, _, _ := newResumeRuntimeTestSession(t)
		session.closed = true
		if err := session.resumeRuntimeForTurnLocked(t.Context()); err == nil || !strings.Contains(err.Error(), "session closed") {
			t.Fatalf("closed resume error = %v", err)
		}
	})

	t.Run("retained unproven root", func(t *testing.T) {
		session, agent, _ := newResumeRuntimeTestSession(t)
		agent.retainIncompleteHermesRoot(session.id, session.client.XDGDirs().Root)
		if err := session.resumeRuntimeForTurnLocked(t.Context()); err == nil || !strings.Contains(err.Error(), "hermes_process_containment_incomplete") {
			t.Fatalf("unproven-root resume error = %v", err)
		}
	})

	t.Run("scratch admission", func(t *testing.T) {
		wantErr := errors.New("scratch full")
		session, _, _ := newResumeRuntimeTestSession(t, WithRuntimeResourceHooks(RuntimeResourceHooks{
			ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) { return nil, wantErr },
		}))
		if err := session.resumeRuntimeForTurnLocked(t.Context()); !errors.Is(err, wantErr) {
			t.Fatalf("scratch admission error = %v", err)
		}
	})

	t.Run("xdg creation", func(t *testing.T) {
		blockedRoot := filepath.Join(t.TempDir(), "file")
		if err := os.WriteFile(blockedRoot, []byte("blocked"), 0o600); err != nil {
			t.Fatal(err)
		}
		session, _, _ := newResumeRuntimeTestSession(t, WithScratchDir(blockedRoot))
		if err := session.resumeRuntimeForTurnLocked(t.Context()); err == nil {
			t.Fatal("resume accepted an unusable XDG parent")
		}
	})

	t.Run("store load", func(t *testing.T) {
		wantErr := errors.New("store unavailable")
		session, _, _ := newResumeRuntimeTestSession(t, WithSessionStore(&errorSessionStore{err: wantErr}))
		if err := session.resumeRuntimeForTurnLocked(t.Context()); !errors.Is(err, wantErr) {
			t.Fatalf("store load error = %v", err)
		}
	})

	t.Run("missing checkpoint", func(t *testing.T) {
		session, _, store := newResumeRuntimeTestSession(t)
		if err := store.Delete(t.Context(), SessionKey{SessionID: string(session.id), Subpath: SessionStoreMainSubpath}); err != nil {
			t.Fatal(err)
		}
		if err := session.resumeRuntimeForTurnLocked(t.Context()); err == nil || !strings.Contains(err.Error(), "state is missing") {
			t.Fatalf("missing-checkpoint resume error = %v", err)
		}
	})

	t.Run("identity drift", func(t *testing.T) {
		session, _, store := newResumeRuntimeTestSession(t)
		snapshot := resumeRuntimeSnapshot(session)
		snapshot.Session.NativeSessionID = "other-native"
		replaceResumeRuntimeRecords(t, store, idmapRecord{
			SessionID:       string(session.id),
			NativeSessionID: "other-native",
			Format:          SessionStoreFormat,
		}, snapshot)
		if err := session.resumeRuntimeForTurnLocked(t.Context()); err == nil || !strings.Contains(err.Error(), "stored session identity drift") {
			t.Fatalf("identity-drift resume error = %v", err)
		}
	})

	t.Run("cwd drift", func(t *testing.T) {
		session, _, store := newResumeRuntimeTestSession(t)
		snapshot := resumeRuntimeSnapshot(session)
		snapshot.Session.Cwd = "/different/project"
		replaceResumeRuntimeRecords(t, store, session.idmap, snapshot)
		if err := session.resumeRuntimeForTurnLocked(t.Context()); err == nil || !strings.Contains(err.Error(), "stored cwd drift") {
			t.Fatalf("cwd-drift resume error = %v", err)
		}
	})

	t.Run("ordinary startup failure", func(t *testing.T) {
		wantErr := errors.New("startup")
		session, agent, _ := newResumeRuntimeTestSession(t)
		agent.options.clientFactory = func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
			return nil, wantErr
		}
		if err := session.resumeRuntimeForTurnLocked(t.Context()); !errors.Is(err, wantErr) {
			t.Fatalf("startup error = %v", err)
		}
	})

	t.Run("unproven startup failure", func(t *testing.T) {
		session, agent, _ := newResumeRuntimeTestSession(t)
		agent.options.clientFactory = func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
			return nil, nativehermes.ErrProcessContainmentIncomplete
		}
		if err := session.resumeRuntimeForTurnLocked(t.Context()); err == nil || !strings.Contains(err.Error(), "hermes_process_containment_incomplete") {
			t.Fatalf("unproven startup error = %v", err)
		}
		if err := agent.rejectIncompleteHermesSession(session.id); !errors.Is(err, nativehermes.ErrProcessContainmentIncomplete) {
			t.Fatalf("unproven root was not retained: %v", err)
		}
	})

	t.Run("native lookup failure", func(t *testing.T) {
		wantErr := errors.New("get session")
		session, agent, _ := newResumeRuntimeTestSession(t)
		client := newFakeHermesClient()
		client.getErr = wantErr
		installResumeRuntimeFactory(agent, client)
		if err := session.resumeRuntimeForTurnLocked(t.Context()); !errors.Is(err, wantErr) {
			t.Fatalf("native lookup error = %v", err)
		}
		if client.closeCount() != 1 {
			t.Fatalf("failed replacement close count = %d", client.closeCount())
		}
	})

	t.Run("native identity drift", func(t *testing.T) {
		session, agent, _ := newResumeRuntimeTestSession(t)
		client := newFakeHermesClient()
		client.getSession = testNativeSession("different-native")
		installResumeRuntimeFactory(agent, client)
		if err := session.resumeRuntimeForTurnLocked(t.Context()); err == nil || !strings.Contains(err.Error(), "native session id drift") {
			t.Fatalf("native identity error = %v", err)
		}
		if client.closeCount() != 1 {
			t.Fatalf("drifted replacement close count = %d", client.closeCount())
		}
	})

	t.Run("closed during startup", func(t *testing.T) {
		session, agent, _ := newResumeRuntimeTestSession(t)
		client := newFakeHermesClient()
		client.getSession = testNativeSession("native-1")
		agent.options.clientFactory = func(_ context.Context, options nativehermes.StartOptions) (nativehermes.Server, error) {
			client.xdg = options.ExistingXDG
			session.mu.Lock()
			session.closed = true
			session.mu.Unlock()

			return client, nil
		}
		if err := session.resumeRuntimeForTurnLocked(t.Context()); err == nil || !strings.Contains(err.Error(), "session closed") {
			t.Fatalf("concurrent close resume error = %v", err)
		}
		if client.closeCount() != 1 {
			t.Fatalf("closed replacement close count = %d", client.closeCount())
		}
	})

	t.Run("successful replacement drains historical channels", func(t *testing.T) {
		session, agent, store := newResumeRuntimeTestSession(t)
		snapshot := resumeRuntimeSnapshot(session)
		snapshot.Terminal = &stateSnapshotTerminal{MessageID: "history-2", Role: valAssistant, Finish: "stop"}
		replaceResumeRuntimeRecords(t, store, session.idmap, snapshot)
		client := newFakeHermesClient()
		client.getSession = testNativeSession("native-1")
		client.events <- nativehermes.TurnEvent{Type: "historical"}
		client.errs <- errors.New("historical")
		installResumeRuntimeFactory(agent, client)
		if err := session.resumeRuntimeForTurnLocked(t.Context()); err != nil {
			t.Fatalf("resume runtime: %v", err)
		}
		if session.runtimeNeedsResume {
			t.Fatal("successful replacement still needs resume")
		}
		if terminal := session.committedTerminalState(); terminal.MessageID != "history-2" {
			t.Fatalf("resumed terminal baseline = %#v", terminal)
		}
		select {
		case event := <-client.events:
			t.Fatalf("historical event was not drained: %#v", event)
		default:
		}
		select {
		case err := <-client.errs:
			t.Fatalf("historical event error was not drained: %v", err)
		default:
		}
		if err := session.Close(t.Context()); err != nil {
			t.Fatalf("close replacement: %v", err)
		}
	})
}

func TestRuntimeResumePreservesExtraPathDirs(t *testing.T) {
	session, agent, _ := newResumeRuntimeTestSession(t)
	first := t.TempDir()
	second := t.TempDir()

	session.mu.Lock()
	session.env = map[string]string{"WAGIE_API_TOKEN": "rotated", "WAGIE_OPERATION_ID": "operation-2"}
	session.extraPathDirs = []string{first, second}
	session.mu.Unlock()

	client := newFakeHermesClient()
	client.getSession = testNativeSession("native-1")
	var captured nativehermes.StartOptions
	agent.options.clientFactory = func(_ context.Context, options nativehermes.StartOptions) (nativehermes.Server, error) {
		captured = options
		client.xdg = options.ExistingXDG

		return client, nil
	}

	if err := session.resumeRuntimeForTurnLocked(t.Context()); err != nil {
		t.Fatalf("resume runtime: %v", err)
	}
	if captured.SessionEnv["WAGIE_API_TOKEN"] != "rotated" || captured.SessionEnv["WAGIE_OPERATION_ID"] != "operation-2" {
		t.Fatalf("resumed session env = %#v", captured.SessionEnv)
	}
	if !slices.Equal(captured.ExtraPathDirs, []string{first, second}) {
		t.Fatalf("resumed extra path dirs = %#v", captured.ExtraPathDirs)
	}

	session.mu.Lock()
	session.extraPathDirs[0] = t.TempDir()
	session.env["WAGIE_API_TOKEN"] = "mutated"
	session.mu.Unlock()
	if captured.ExtraPathDirs[0] != first || captured.SessionEnv["WAGIE_API_TOKEN"] != "rotated" {
		t.Fatalf("resumed carrier aliases session state: dirs %#v env %#v", captured.ExtraPathDirs, captured.SessionEnv)
	}

	if err := session.Close(t.Context()); err != nil {
		t.Fatalf("close resumed session: %v", err)
	}
}

func newResumeRuntimeTestSession(t *testing.T, options ...Option) (*session, *Agent, *InMemorySessionStore) {
	t.Helper()
	store := NewInMemorySessionStore()
	base := make([]Option, 0, 2+len(options))
	base = append(base, WithScratchDir(t.TempDir()), WithSessionStore(store))
	base = append(base, options...)
	agent := newTestAgent(base...)
	session := testSession(agent, newFakeHermesClient())
	session.runtimeNeedsResume = true
	replaceResumeRuntimeRecords(t, store, session.idmap, resumeRuntimeSnapshot(session))

	return session, agent, store
}

func resumeRuntimeSnapshot(session *session) stateSnapshot {
	return stateSnapshot{
		Format:              SessionStoreFormat,
		CapturedAtUnixMilli: 1,
		Session: stateSnapshotSession{
			SessionID:       string(session.id),
			NativeSessionID: session.idmap.NativeSessionID,
			Cwd:             session.cwd,
			Model: stateSnapshotModel{
				ProviderID: session.providerID,
				ModelID:    session.modelID,
			},
		},
		Terminal: &stateSnapshotTerminal{},
		Archives: map[string]archiveInfo{},
		Wrapper:  &stateSnapshotWrapper{},
	}
}

func replaceResumeRuntimeRecords(t *testing.T, store *InMemorySessionStore, idmap idmapRecord, snapshot stateSnapshot) {
	t.Helper()
	main := SessionKey{SessionID: snapshot.Session.SessionID, Subpath: SessionStoreMainSubpath}
	if err := store.Replace(t.Context(), main, []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{mustStateJSON(t, snapshot)}},
		{Key: SessionKey{SessionID: snapshot.Session.SessionID, Subpath: idmapSubpath}, Entries: []SessionStoreEntry{mustStateJSON(t, idmap)}},
	}); err != nil {
		t.Fatal(err)
	}
}

func installResumeRuntimeFactory(agent *Agent, client *fakeHermesClient) {
	agent.options.clientFactory = func(_ context.Context, options nativehermes.StartOptions) (nativehermes.Server, error) {
		client.xdg = options.ExistingXDG

		return client, nil
	}
}

func TestCloseSessionSkipsSnapshotWhileTurnPending(t *testing.T) {
	ctx := context.Background()
	store := newCountingSessionStore()
	client := newFakeHermesClient()
	started := make(chan struct{})
	client.sendMessage = func(ctx context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		close(started)
		<-ctx.Done()

		return nativehermes.NativeMessage{}, ctx.Err()
	}
	agent := newTestAgent(WithSessionStore(store))
	session := testSession(agent, client)
	agent.mu.Lock()
	agent.sessions[session.id] = session
	agent.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		_, err := session.Prompt(ctx, TextPromptRequest(session.id, "turn-blocked", "blocked"))
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

func TestCloseSessionSnapshotsBeforeNativeRootRemoval(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	xdg, createErr := nativehermes.CreateXDGDirs(root, "close-snapshot")
	if createErr != nil {
		t.Fatal(createErr)
	}
	if writeErr := os.WriteFile(filepath.Join(xdg.Root, "state.db"), []byte("durable state"), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}

	store := NewInMemorySessionStore()
	client := newFakeHermesClient()
	client.xdg = xdg
	client.closeFunc = func(context.Context) error { return os.RemoveAll(xdg.Root) }
	agent := newTestAgent(WithSessionStore(store))
	session := testSession(agent, client)
	agent.mu.Lock()
	agent.sessions[session.id] = session
	agent.mu.Unlock()

	if _, closeErr := agent.CloseSession(ctx, acp.CloseSessionRequest{SessionId: session.id}); closeErr != nil {
		t.Fatalf("CloseSession: %v", closeErr)
	}
	if _, statErr := os.Stat(xdg.Root); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("native close did not remove root: %v", statErr)
	}
	entries, err := store.Load(ctx, SessionKey{SessionID: string(session.id), Subpath: stateDBSubpath})
	if err != nil || len(entries) == 0 {
		t.Fatalf("state DB was not captured before close: entries=%d err=%v", len(entries), err)
	}
}

// TestSnapshotFencedAfterAcquireTurn parks a turn after acquireTurn but before
// beginTurn and asserts no Replace happens (HW3 turn-in-flight snapshot fence).
func TestSnapshotFencedAfterAcquireTurn(t *testing.T) {
	ctx := context.Background()
	store := newCountingSessionStore()
	client := newFakeHermesClient()
	agent := newTestAgent(WithSessionStore(store))
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
	additionalDir := t.TempDir()
	firstPathDir := t.TempDir()
	secondPathDir := t.TempDir()
	extraPathDirs := []string{firstPathDir, secondPathDir}
	httpMCP := HTTPMCPServer("http", "https://mcp.example.test", map[string]string{"X-Test": "1"})
	startOptions := []SessionRequestOption{
		WithSessionAdditionalDirectories(additionalDir),
		WithSessionMCPServers(httpMCP),
		WithSessionHermesOptions(HermesOptions{
			Model:         "openai/gpt-test",
			Env:           map[string]string{"A": "B"},
			ExtraPathDirs: extraPathDirs,
		}),
	}
	store := NewInMemorySessionStore()
	client := newFakeHermesClient()
	client.createSession = testNativeSession("native-1")
	client.getSession = client.createSession
	factoryCalls := 0
	agent := newTestAgent(WithScratchDir(root), WithSessionStore(store), func(options *Options) {
		options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
			factoryCalls++
			xdg := opts.ExistingXDG
			if xdg.Root == "" {
				var err error
				xdg, err = nativehermes.CreateXDGDirs(opts.Root, string(opts.ACPSessionID))
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

	newResp, err := agent.NewSession(ctx, NewSessionRequest(cwd, append(startOptions, WithSessionRawEvents(true))...))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	id := newResp.SessionId
	if factoryCalls != 1 {
		t.Fatalf("factory calls after new = %d", factoryCalls)
	}
	active := agent.activeSession(id)

	if _, err8 := agent.LoadSession(ctx, LoadSessionRequest(id, cwd, append(startOptions, WithSessionRawEvents(false))...)); err8 != nil {
		t.Fatalf("active LoadSession: %v", err8)
	}
	if active.snapshot().rawMessages.Enabled() {
		t.Fatal("active LoadSession did not apply rawEvent=false")
	}
	if _, err9 := agent.ResumeSession(ctx, ResumeSessionRequest(id, cwd, append(startOptions, WithSessionRawEvents(true))...)); err9 != nil {
		t.Fatalf("active ResumeSession: %v", err9)
	}
	if !active.snapshot().rawMessages.Enabled() {
		t.Fatal("active ResumeSession did not apply rawEvent=true")
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
	if _, err10 := agent.LoadSession(ctx, LoadSessionRequest(id, cwd, WithSessionMeta(badMeta))); err10 == nil {
		t.Fatal("active load accepted bad _meta")
	}
	if _, err11 := agent.ResumeSession(ctx, ResumeSessionRequest(id, cwd, WithSessionMeta(badMeta))); err11 == nil {
		t.Fatal("active resume accepted bad _meta")
	}
	if _, err12 := agent.LoadSession(ctx, LoadSessionRequest(id, "relative-cwd")); err12 == nil {
		t.Fatal("active load accepted relative cwd")
	}
	sse := acp.McpServer{Sse: &acp.McpServerSseInline{Name: "sse", Url: "https://sse.example"}}
	if _, err13 := agent.LoadSession(ctx, LoadSessionRequest(id, cwd, WithSessionMCPServers(sse))); err13 == nil {
		t.Fatal("active load accepted SSE MCP server")
	}
	if _, err14 := agent.ResumeSession(ctx, ResumeSessionRequest(id, cwd, WithSessionMCPServers(sse))); err14 == nil {
		t.Fatal("active resume accepted SSE MCP server")
	}
	_, err = agent.LoadSession(ctx, LoadSessionRequest(id, filepath.Join(cwd, "other"), startOptions...))
	requireLifecycleMismatch(t, err, jsonFieldCwd)
	_, err = agent.LoadSession(ctx, LoadSessionRequest(id, cwd, WithSessionMCPServers(httpMCP), WithSessionHermesOptions(HermesOptions{Model: "openai/gpt-test", Env: map[string]string{"A": "B"}, ExtraPathDirs: extraPathDirs})))
	requireLifecycleMismatch(t, err, "additionalDirectories")
	_, err = agent.LoadSession(ctx, LoadSessionRequest(id, cwd, WithSessionAdditionalDirectories(additionalDir), WithSessionMCPServers(HTTPMCPServer("other", "https://other.example.test", nil)), WithSessionHermesOptions(HermesOptions{Model: "openai/gpt-test", Env: map[string]string{"A": "B"}, ExtraPathDirs: extraPathDirs})))
	requireLifecycleMismatch(t, err, "mcpServers")
	_, err = agent.ResumeSession(ctx, ResumeSessionRequest(id, cwd, WithSessionAdditionalDirectories(additionalDir), WithSessionMCPServers(httpMCP), WithSessionHermesOptions(HermesOptions{Model: "openai/gpt-test", Env: map[string]string{"A": "changed"}, ExtraPathDirs: extraPathDirs})))
	requireLifecycleMismatch(t, err, "_meta.hermes.options.env")
	_, err = agent.ResumeSession(ctx, ResumeSessionRequest(id, cwd, WithSessionAdditionalDirectories(additionalDir), WithSessionMCPServers(httpMCP), WithSessionHermesOptions(HermesOptions{Model: "openai/gpt-test", ExtraPathDirs: extraPathDirs})))
	requireLifecycleMismatch(t, err, "_meta.hermes.options.env")
	_, err = agent.ResumeSession(ctx, ResumeSessionRequest(id, cwd, WithSessionAdditionalDirectories(additionalDir), WithSessionMCPServers(httpMCP), WithSessionHermesOptions(HermesOptions{Model: "openai/gpt-test", Env: map[string]string{"A": "B"}, ExtraPathDirs: []string{secondPathDir, firstPathDir}})))
	requireLifecycleMismatch(t, err, hermesExtraPathDirsOptionPath)
	_, err = agent.ResumeSession(ctx, ResumeSessionRequest(id, cwd, WithSessionAdditionalDirectories(additionalDir), WithSessionMCPServers(httpMCP), WithSessionHermesOptions(HermesOptions{Model: "openai/other", Env: map[string]string{"A": "B"}, ExtraPathDirs: extraPathDirs})))
	requireLifecycleMismatch(t, err, "_meta.hermes.options.model")
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

func TestActiveLifecycleRejectsExtraPathDirsMismatch(t *testing.T) {
	first := t.TempDir()
	second := t.TempDir()
	cwd := t.TempDir()
	session := newSession(
		newTestAgent(),
		"active-carrier",
		cwd,
		nil,
		nil,
		testNativeSession("native-carrier"),
		newFakeHermesClient(),
		sessionMeta{ExtraPathDirs: []string{first, second}},
		idmapRecord{},
	)

	if err := applyActiveLifecycleRequest(session, cwd, nil, nil, sessionMeta{ExtraPathDirs: []string{first, second}}); err != nil {
		t.Fatalf("equal ordered paths rejected: %v", err)
	}
	err := applyActiveLifecycleRequest(session, cwd, nil, nil, sessionMeta{ExtraPathDirs: []string{second, first}})
	requireLifecycleMismatch(t, err, hermesExtraPathDirsOptionPath)
}

func requireLifecycleMismatch(t *testing.T, err error, field string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected lifecycle mismatch for %s", field)
	}
	var reqErr *acp.RequestError
	if !errors.As(err, &reqErr) {
		t.Fatalf("mismatch error type = %T", err)
	}
	data, ok := reqErr.Data.(map[string]any)
	if !ok {
		t.Fatalf("mismatch error data = %#v", reqErr.Data)
	}
	if data["error"] != "mismatch" || data["field"] != field {
		t.Fatalf("mismatch error data = %#v want field %q", data, field)
	}
}

func requireUnknownSession(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected unknown-session error")
	}
	var reqErr *acp.RequestError
	if !errors.As(err, &reqErr) {
		t.Fatalf("unknown-session error type = %T", err)
	}
	if reqErr.Code != -32602 {
		t.Fatalf("unknown-session code = %d, want -32602", reqErr.Code)
	}
	data, ok := reqErr.Data.(map[string]any)
	if !ok || data["error"] != "unknown session" || data["field"] != "sessionId" {
		t.Fatalf("unknown-session data = %#v", reqErr.Data)
	}
}

func TestAgentSessionLifecycleErrorBranches(t *testing.T) {
	ctx := context.Background()
	cwd := t.TempDir()

	t.Run("new session validation and native errors", func(t *testing.T) {
		closed := newTestAgent()
		if err := closed.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := closed.NewSession(ctx, NewSessionRequest(cwd)); err == nil {
			t.Fatal("closed agent accepted new session")
		}
		if _, err := newTestAgent().NewSession(ctx, NewSessionRequest("relative")); err == nil {
			t.Fatal("relative cwd accepted")
		}
		if _, err := newTestAgent().NewSession(ctx, NewSessionRequest(cwd, WithSessionMCPServers(acp.McpServer{Sse: &acp.McpServerSseInline{Name: "sse", Url: "http://example.test"}}))); err == nil {
			t.Fatal("SSE MCP accepted")
		}
		if _, err := newTestAgent().NewSession(ctx, NewSessionRequest(cwd, WithSessionMeta(map[string]any{hermesMetaKey: map[string]any{"unknown": true}}))); err == nil {
			t.Fatal("bad meta accepted")
		}
		factoryErr := newTestAgent(func(options *Options) {
			options.clientFactory = func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
				return nil, errors.New("factory failed")
			}
		})
		if _, err := factoryErr.NewSession(ctx, NewSessionRequest(cwd)); err == nil {
			t.Fatal("factory error ignored")
		}
		createErrClient := newFakeHermesClient()
		createErrClient.createErr = errors.New("create failed")
		agent := newTestAgent(func(options *Options) {
			options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
				createErrClient.xdg, _ = nativehermes.CreateXDGDirs(opts.Root, string(opts.ACPSessionID))

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
		agent := newTestAgent()
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
		errStoreAgent := newTestAgent(WithSessionStore(&errorSessionStore{err: errors.New("store failed")}))
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
	sourceXDG, err := nativehermes.CreateXDGDirs(root, "source")
	if err != nil {
		t.Fatal(err)
	}
	sourceClient.xdg = sourceXDG
	seedAgent := newTestAgent(WithScratchDir(root), WithSessionStore(store))
	seed := testSession(seedAgent, sourceClient)
	seed.cwd = cwd
	if err15 := seed.snapshotToStore(ctx); err15 != nil {
		t.Fatalf("snapshotToStore: %v", err15)
	}

	loadedClient := newFakeHermesClient()
	loadedClient.getSession = testNativeSession("native-rotated")
	loadedClient.providers = testProviders()
	loadAgent := newTestAgent(WithScratchDir(root), WithSessionStore(store), func(options *Options) {
		options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
			loadedClient.xdg = opts.ExistingXDG

			return loadedClient, nil
		}
	})
	// Cold cwd mismatch (session not yet active) is rejected after hydrate.
	if _, err16 := loadAgent.LoadSession(ctx, LoadSessionRequest("session-1", t.TempDir())); err16 == nil {
		t.Fatal("cold cwd mismatch load succeeded")
	}
	if _, err17 := loadAgent.ResumeSession(ctx, ResumeSessionRequest("session-1", cwd)); err17 != nil {
		t.Fatalf("ResumeSession: %v", err17)
	}
	resumed := loadAgent.activeSession("session-1")
	if resumed == nil {
		t.Fatal("ResumeSession did not register active session")
	}
	if got := resumed.snapshot().idmap.NativeSessionID; got != "native-rotated" {
		t.Fatalf("rotated active idmap native id = %q", got)
	}
	if err18 := resumed.snapshotToStore(ctx); err18 != nil {
		t.Fatalf("snapshot rotated resume: %v", err18)
	}
	rotatedEntries, err := store.Load(ctx, SessionKey{SessionID: "session-1", Subpath: idmapSubpath})
	if err != nil {
		t.Fatalf("load rotated idmap: %v", err)
	}
	var rotatedIDMap idmapRecord
	if err19 := json.Unmarshal(rotatedEntries[len(rotatedEntries)-1], &rotatedIDMap); err19 != nil {
		t.Fatal(err19)
	}
	if rotatedIDMap.NativeSessionID != "native-rotated" {
		t.Fatalf("persisted rotated idmap native id = %q", rotatedIDMap.NativeSessionID)
	}
	// Active cwd mismatch (session now active) is rejected before reuse.
	if _, err20 := loadAgent.LoadSession(ctx, LoadSessionRequest("session-1", t.TempDir())); err20 == nil {
		t.Fatal("active cwd mismatch load succeeded")
	}

	getErrClient := newFakeHermesClient()
	getErrClient.getErr = errors.New("get failed")
	getErrAgent := newTestAgent(WithScratchDir(root), WithSessionStore(store), func(options *Options) {
		options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
			getErrClient.xdg = opts.ExistingXDG

			return getErrClient, nil
		}
	})
	if _, err21 := getErrAgent.LoadSession(ctx, LoadSessionRequest("session-1", cwd)); err21 == nil {
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
		if err22 := listStore.Replace(ctx, SessionKey{SessionID: id}, []SessionStoreReplacement{{Key: SessionKey{SessionID: id}, Entries: []SessionStoreEntry{entry}}}); err22 != nil {
			t.Fatalf("replace list store: %v", err22)
		}
	}
	listAgent := newTestAgent(WithSessionStore(listStore))
	listResp, err := listAgent.ListSessions(ctx, ListSessionsRequest(WithListSessionsCwd(cwd)))
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(listResp.Sessions) != listSessionsPageSize || listResp.NextCursor == nil {
		t.Fatalf("list resp len=%d next=%v", len(listResp.Sessions), listResp.NextCursor)
	}
	secondPage, err := listAgent.ListSessions(ctx, ListSessionsRequest(WithListSessionsCursor(*listResp.NextCursor)))
	if err != nil || len(secondPage.Sessions) != 2 || secondPage.NextCursor != nil {
		t.Fatalf("second page = %#v err=%v", secondPage, err)
	}
	if _, err23 := listAgent.ListSessions(ctx, ListSessionsRequest(WithListSessionsCursor("bad"))); err23 == nil {
		t.Fatal("bad cursor accepted")
	}
	if _, err24 := listAgent.ListSessions(ctx, ListSessionsRequest(WithListSessionsCursor("!not-base64!"))); err24 == nil {
		t.Fatal("non-base64 cursor accepted")
	}
	if _, err25 := listAgent.ListSessions(ctx, ListSessionsRequest(WithListSessionsCursor(encodeListCursor(999)))); err25 == nil {
		t.Fatal("past-end cursor accepted")
	}

	parentClient := newFakeHermesClient()
	parentClient.forkErr = errors.New("fork failed")
	parent := testSession(newTestAgent(), parentClient)
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

	invalidOptions := newTestAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: -1}))
	if _, err := invalidOptions.Initialize(ctx, acp.InitializeRequest{}); err == nil {
		t.Fatal("invalid construction options were not reported during initialize")
	}
	invalidImageOptions := newTestAgent(WithImageLimits(ImageLimits{MaxInputBytesPerImage: -1}))
	if _, err := invalidImageOptions.Initialize(ctx, acp.InitializeRequest{}); err == nil {
		t.Fatal("negative image limit was not reported during initialize")
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

	closed := newTestAgent()
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := closed.HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest("s", cwd))); err == nil {
		t.Fatal("closed agent accepted extension method")
	}

	limitAgent := newTestAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))
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
	defaultSession := newSession(newTestAgent(), "wrapper", cwd, nil, nil, nativehermes.Session{ID: "native"}, client, sessionMeta{}, idmapRecord{})
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

	testAgentSnapshotAndForkFailureBranches(ctx, t, cwd, closed)
}

func testAgentSnapshotAndForkFailureBranches(ctx context.Context, t *testing.T, cwd string, closed *Agent) {
	t.Helper()

	createClient := newFakeHermesClient()
	createClient.createSession = testNativeSession("native-created")
	snapshotErrAgent := newTestAgent(
		WithSessionStore(&errorSessionStore{err: errors.New("replace failed")}),
		func(options *Options) {
			options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
				var err error
				createClient.xdg, err = nativehermes.CreateXDGDirs(opts.Root, string(opts.ACPSessionID))
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
	sourceXDG, err := nativehermes.CreateXDGDirs(t.TempDir(), "source")
	if err != nil {
		t.Fatal(err)
	}
	sourceClient.xdg = sourceXDG
	seedAgent := newTestAgent(WithSessionStore(store))
	seed := testSession(seedAgent, sourceClient)
	seed.cwd = cwd
	if err24 := seed.snapshotToStore(ctx); err24 != nil {
		t.Fatalf("seed snapshot: %v", err24)
	}
	replayErrClient := newFakeHermesClient()
	replayErrClient.getSession = testNativeSession("native-1")
	replayErrClient.messagesErr = errors.New("messages failed")
	replayErrAgent := newTestAgent(WithSessionStore(store), func(options *Options) {
		options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
			replayErrClient.xdg = opts.ExistingXDG

			return replayErrClient, nil
		}
	})
	if _, err25 := replayErrAgent.LoadSession(ctx, LoadSessionRequest("session-1", cwd)); err25 == nil {
		t.Fatal("load replay error was ignored")
	}

	if _, err26 := closed.ResumeSession(ctx, ResumeSessionRequest("session-1", cwd)); err26 == nil {
		t.Fatal("closed agent resumed session")
	}
	if _, err27 := newTestAgent(WithScratchDir(string([]byte{0}))).LoadSession(ctx, LoadSessionRequest("session-1", cwd)); err27 == nil {
		t.Fatal("invalid scratch root did not fail load")
	}
	if _, errNS := newTestAgent(WithScratchDir(string([]byte{0}))).NewSession(ctx, NewSessionRequest(cwd)); errNS == nil {
		t.Fatal("invalid scratch root did not fail new session")
	}
	if _, err28 := newTestAgent(WithSessionStore(&errorSessionStore{err: errors.New("load failed")})).LoadSession(ctx, LoadSessionRequest("session-1", cwd)); err28 == nil {
		t.Fatal("hydrate store error was ignored")
	}

	parentClient := newFakeHermesClient()
	parentClient.forkSession = testNativeSession("native-child")
	parent := testSession(newTestAgent(WithScratchDir(string([]byte{0}))), parentClient)
	parentAgent := parent.agent
	parentAgent.sessions[parent.id] = parent
	if _, err29 := parentAgent.HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest(parent.id, cwd))); err29 == nil {
		t.Fatal("invalid fork scratch root did not fail")
	}
	if _, err30 := newTestAgent().HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest(parent.id, cwd, WithSessionMCPServers(acp.McpServer{Sse: &acp.McpServerSseInline{Name: "sse"}})))); err30 == nil {
		t.Fatal("unstable fork accepted SSE MCP")
	}
	if _, err31 := newTestAgent().HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest(parent.id, cwd, WithSessionMCPServers(acp.McpServer{Acp: &acp.McpServerAcpInline{Name: "acp"}})))); err31 == nil {
		t.Fatal("unstable fork accepted ACP MCP")
	}
	if _, err32 := newTestAgent().HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest(parent.id, cwd, WithSessionMeta(map[string]any{hermesMetaKey: map[string]any{"bad": true}})))); err32 == nil {
		t.Fatal("unstable fork accepted invalid meta")
	}

	validTarget, err := nativehermes.CreateXDGDirs(t.TempDir(), "target")
	if err != nil {
		t.Fatal(err)
	}
	if err33 := cloneHermesStateDB("", nativehermes.XDGDirs{Root: string([]byte{0})}, validTarget); err33 == nil {
		t.Fatal("cloneHermesStateDB accepted invalid source")
	}
	restoreStateStoreSeams(t)
	stateRemoveAll = func(string) error { return errors.New("remove failed") }
	validSource, err := nativehermes.CreateXDGDirs(t.TempDir(), "source")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(validSource.Root, "state.db"), []byte("state"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cloneHermesStateDB("", validSource, validTarget); err == nil {
		t.Fatal("cloneHermesStateDB ignored decode error")
	}
}

func TestAgentNewSessionIDAndStoreErrors(t *testing.T) {
	ctx := context.Background()
	cwd := t.TempDir()

	t.Run("new session id and store errors", func(t *testing.T) {
		defaultClient := newFakeHermesClient()
		defaultClient.createSession = nativehermes.Session{ID: "native-default", Title: "Default"}
		var defaultModel string
		defaultAgent := newTestAgent(WithDefaultModel("openai/gpt-default"), func(options *Options) {
			options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
				defaultModel = opts.DefaultModel
				var err error
				defaultClient.xdg, err = nativehermes.CreateXDGDirs(opts.Root, string(opts.ACPSessionID))
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
		if _, err := newTestAgent().NewSession(ctx, NewSessionRequest(cwd)); err == nil {
			t.Fatal("NewSession ignored session id error")
		}
		sessionIDRandReader = oldReader

		client := newFakeHermesClient()
		client.createSession = testNativeSession("native-created")
		agent := newTestAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}), func(options *Options) {
			options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
				var err error
				client.xdg, err = nativehermes.CreateXDGDirs(opts.Root, string(opts.ACPSessionID))
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
}

func TestAgentRemainingLifecycleBranches(t *testing.T) {
	ctx := context.Background()
	cwd := t.TempDir()

	t.Run("load validation and startup errors", func(t *testing.T) {
		agent := newTestAgent()
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
		factoryErrAgent := newTestAgent(WithSessionStore(store), func(options *Options) {
			options.clientFactory = func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
				return nil, errors.New("factory failed")
			}
		})
		if _, err := factoryErrAgent.LoadSession(ctx, LoadSessionRequest("s", cwd)); err == nil {
			t.Fatal("LoadSession ignored client factory error")
		}

		loadedClient := newFakeHermesClient()
		loadedClient.getSession = testNativeSession("n")
		limitAgent := newTestAgent(WithSessionStore(store), WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}), func(options *Options) {
			options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
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
		closed := newTestAgent()
		if err := closed.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := closed.ListSessions(ctx, ListSessionsRequest()); err == nil {
			t.Fatal("closed agent listed sessions")
		}
		relative := "relative"
		if _, err := newTestAgent().ListSessions(ctx, acp.ListSessionsRequest{Cwd: &relative}); err == nil {
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
		agent := newTestAgent(WithSessionStore(store))
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
		agent := newTestAgent()
		session := testSession(agent, client)
		agent.sessions[session.id] = session
		if _, err := agent.UnstableDeleteSession(ctx, DeleteSessionRequest(session.id)); err == nil {
			t.Fatal("delete ignored active close error")
		}
	})

	t.Run("delete active ignores native delete error after tombstone", func(t *testing.T) {
		client := newFakeHermesClient()
		client.deleteErr = errors.New("native delete failed")
		agent := newTestAgent()
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
				agent := newTestAgent(WithScratchDir(root))
				xdg, err := nativehermes.CreateXDGDirs(root, name)
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
}

func TestAgentDeletedCleanupHelperBranches(t *testing.T) {
	ctx := context.Background()
	cwd := t.TempDir()

	t.Run("deleted cleanup helper branches", func(t *testing.T) {
		agent := newTestAgent(WithScratchDir(t.TempDir()))
		agent.rememberDeleteCleanup(deleteCleanupRecord{})
		if len(agent.deleteCleanup) != 0 {
			t.Fatalf("empty cleanup record was remembered: %#v", agent.deleteCleanup)
		}
		agent.forgetDeleteCleanupIfDone("")

		xdg, err := nativehermes.CreateXDGDirs(agent.homeRoot(), "keep")
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
		errorAgent := newTestAgent()
		errorAgent.deleteCleanup["bad"] = deleteCleanupRecord{SessionID: "bad", XDGRoot: string([]byte{0})}
		if err := errorAgent.retryDeletedSessionCleanup(ctx); err == nil {
			t.Fatal("cleanup error retry returned nil")
		}
		for _, name := range []string{"list", "load", "delete"} {
			t.Run("entrypoint retry error "+name, func(t *testing.T) {
				entryAgent := newTestAgent(WithScratchDir(t.TempDir()))
				entryXDG, err := nativehermes.CreateXDGDirs(entryAgent.options.ScratchDir, name)
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
}

func TestAgentForkErrorBranches(t *testing.T) {
	ctx := context.Background()
	cwd := t.TempDir()

	t.Run("fork errors", func(t *testing.T) {
		oldReader := sessionIDRandReader
		t.Cleanup(func() { sessionIDRandReader = oldReader })

		parentClient := newFakeHermesClient()
		parentClient.forkSession = testNativeSession("native-child")
		parentClient.xdg, _ = nativehermes.CreateXDGDirs(t.TempDir(), "parent")
		parentAgent := newTestAgent()
		parent := testSession(parentAgent, parentClient)
		parentAgent.sessions[parent.id] = parent

		sessionIDRandReader = errorReader{err: errors.New("id failed")}
		if _, err := parentAgent.HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest(parent.id, cwd))); err == nil {
			t.Fatal("fork ignored session id error")
		}
		sessionIDRandReader = oldReader

		parentClient.xdg = nativehermes.XDGDirs{Root: string([]byte{0})}
		if _, err := parentAgent.HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest(parent.id, cwd))); err == nil {
			t.Fatal("fork ignored state db clone error")
		}
		parentClient.xdg, _ = nativehermes.CreateXDGDirs(t.TempDir(), "parent")

		factoryErrAgent := newTestAgent(func(options *Options) {
			options.clientFactory = func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
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
		getErrAgent := newTestAgent(func(options *Options) {
			options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
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
		limitAgent := newTestAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}), func(options *Options) {
			options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
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
		snapshotErrAgent := newTestAgent(WithSessionStore(&errorSessionStore{err: errors.New("replace failed")}), func(options *Options) {
			options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
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
}

func TestAgentClientFactoryKeepsStaticAndSessionCarrierSeparate(t *testing.T) {
	ctx := context.Background()
	cwd := t.TempDir()

	t.Run("client factory defaults and env merge", func(t *testing.T) {
		defaultAgent := newTestAgent()
		defaultAgent.options.clientFactory = nil
		if _, err := defaultAgent.newHermesClient(ctx, "s", cwd, sessionMeta{}, nativehermes.XDGDirs{Root: filepath.Join(t.TempDir(), "root")}); err == nil {
			t.Fatal("default client factory unexpectedly succeeded with incomplete XDG")
		}
		var captured nativehermes.StartOptions
		extraPathDir := t.TempDir()
		agent := newTestAgent(WithEnv(map[string]string{"BASE": "static"}), func(options *Options) {
			options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
				captured = opts

				return newFakeHermesClient(), nil
			}
		})
		if _, err := agent.newHermesClient(ctx, "s", cwd, sessionMeta{
			Env:           map[string]string{"A": "1"},
			ExtraPathDirs: []string{extraPathDir},
		}, nativehermes.XDGDirs{}); err != nil {
			t.Fatalf("newHermesClient env: %v", err)
		}
		if captured.Env["BASE"] != "static" || captured.Env["A"] != "" {
			t.Fatalf("captured static env = %#v", captured.Env)
		}
		if captured.SessionEnv["A"] != "1" {
			t.Fatalf("captured session env = %#v", captured.SessionEnv)
		}
		if len(captured.ExtraPathDirs) != 1 || captured.ExtraPathDirs[0] != extraPathDir {
			t.Fatalf("captured extra path dirs = %#v", captured.ExtraPathDirs)
		}
	})
}

func TestConcurrentSessionCarriersNeverCross(t *testing.T) {
	agent := newTestAgent(WithScratchDir(t.TempDir()))
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var (
		mu       sync.Mutex
		captured = map[string]nativehermes.StartOptions{}
	)
	agent.options.clientFactory = func(_ context.Context, options nativehermes.StartOptions) (nativehermes.Server, error) {
		entered <- struct{}{}
		<-release
		mu.Lock()
		captured[string(options.ACPSessionID)] = options
		mu.Unlock()
		client := newFakeHermesClient()
		client.xdg = options.ExistingXDG

		return client, nil
	}

	type result struct {
		id     string
		client nativehermes.Server
		err    error
	}
	results := make(chan result, 2)
	starts := []struct {
		id    string
		token string
		dir   string
		cwd   string
	}{
		{id: "session-a", token: "bearer-a", dir: t.TempDir(), cwd: t.TempDir()},
		{id: "session-b", token: "bearer-b", dir: t.TempDir(), cwd: t.TempDir()},
	}
	for _, start := range starts {
		go func() {
			client, err := agent.newHermesClient(t.Context(), acp.SessionId(start.id), start.cwd, sessionMeta{
				Env:           map[string]string{"WAGIE_API_TOKEN": start.token},
				ExtraPathDirs: []string{start.dir},
			}, nativehermes.XDGDirs{})
			results <- result{id: start.id, client: client, err: err}
		}()
	}
	<-entered
	<-entered
	close(release)

	for range starts {
		result := <-results
		if result.err != nil {
			t.Fatalf("start %s: %v", result.id, result.err)
		}
		t.Cleanup(func() { _ = result.client.Close(context.Background()) })
	}

	mu.Lock()
	defer mu.Unlock()
	for _, start := range starts {
		options := captured[start.id]
		if options.SessionEnv["WAGIE_API_TOKEN"] != start.token || !slices.Equal(options.ExtraPathDirs, []string{start.dir}) {
			t.Fatalf("carrier %s crossed: env %#v dirs %#v", start.id, options.SessionEnv, options.ExtraPathDirs)
		}
	}
}

func TestClosedSessionRebindUsesRotatedCarrier(t *testing.T) {
	oldDir := t.TempDir()
	newDir := t.TempDir()
	cwd := t.TempDir()
	store := NewInMemorySessionStore()
	first := newFakeHermesClient()
	first.createSession = testNativeSession("native-rebind")
	first.getSession = first.createSession
	second := newFakeHermesClient()
	second.getSession = testNativeSession("native-rebind")
	clients := []*fakeHermesClient{first, second}
	var starts []nativehermes.StartOptions
	agent := newTestAgent(WithScratchDir(t.TempDir()), WithSessionStore(store), func(options *Options) {
		options.clientFactory = func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
			starts = append(starts, start)
			client := clients[len(starts)-1]
			client.xdg = start.ExistingXDG

			return client, nil
		}
	})

	created, err := agent.NewSession(t.Context(), NewSessionRequest(cwd, WithSessionHermesOptions(HermesOptions{
		Env:           map[string]string{"WAGIE_API_TOKEN": "old-bearer"},
		ExtraPathDirs: []string{oldDir},
	})))
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	if _, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: created.SessionId}); err != nil {
		t.Fatalf("close session: %v", err)
	}
	if _, err := agent.ResumeSession(t.Context(), ResumeSessionRequest(created.SessionId, cwd, WithSessionHermesOptions(HermesOptions{
		Env:           map[string]string{"WAGIE_API_TOKEN": "new-bearer"},
		ExtraPathDirs: []string{newDir},
	}))); err != nil {
		t.Fatalf("resume session: %v", err)
	}
	if len(starts) != 2 {
		t.Fatalf("native starts = %d", len(starts))
	}
	if starts[1].SessionEnv["WAGIE_API_TOKEN"] != "new-bearer" || slices.Contains(starts[1].ExtraPathDirs, oldDir) || !slices.Equal(starts[1].ExtraPathDirs, []string{newDir}) {
		t.Fatalf("rotated start = env %#v dirs %#v", starts[1].SessionEnv, starts[1].ExtraPathDirs)
	}
	if err := agent.Close(); err != nil {
		t.Fatalf("close agent: %v", err)
	}
}

func TestForkSessionCarriesChildEnvironmentAndPath(t *testing.T) {
	cwd := t.TempDir()
	childDir := t.TempDir()
	parent := newFakeHermesClient()
	parent.createSession = testNativeSession("native-parent-carrier")
	parent.getSession = parent.createSession
	parent.forkSession = testNativeSession("native-child-carrier")
	child := newFakeHermesClient()
	child.getSession = testNativeSession("native-child-carrier")
	clients := []*fakeHermesClient{parent, child}
	var starts []nativehermes.StartOptions
	agent := newTestAgent(WithScratchDir(t.TempDir()), WithSessionStore(NewInMemorySessionStore()), func(options *Options) {
		options.clientFactory = func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
			starts = append(starts, start)
			client := clients[len(starts)-1]
			client.xdg = start.ExistingXDG

			return client, nil
		}
	})
	created, err := agent.NewSession(t.Context(), NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	if writeErr := os.WriteFile(filepath.Join(parent.xdg.Root, "state.db"), []byte("parent-state"), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	_, err = agent.forkSession(t.Context(), ForkSessionRequest(created.SessionId, cwd, WithSessionHermesOptions(HermesOptions{
		Env:           map[string]string{"WAGIE_API_TOKEN": "child-bearer"},
		ExtraPathDirs: []string{childDir},
	})))
	if err != nil {
		t.Fatalf("fork session: %v", err)
	}
	if len(starts) != 2 || starts[1].SessionEnv["WAGIE_API_TOKEN"] != "child-bearer" || !slices.Equal(starts[1].ExtraPathDirs, []string{childDir}) {
		t.Fatalf("child start = %#v", starts)
	}
	if err := agent.Close(); err != nil {
		t.Fatalf("close agent: %v", err)
	}
}

func TestIsolatedSessionsShareOnlyTheDurableProviderAuthHome(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	authHome := testNativeOwnedDir(t, "native-auth")
	agent := newTestAgent(
		WithScratchDir(t.TempDir()),
		WithProviderAuthRoot(t.TempDir()),
		WithProviderAuthHome(authHome),
		WithEnv(map[string]string{"HERMES_AUTH_HOME": "/ignored-agent-value"}),
	)

	var captured []nativehermes.StartOptions
	agent.options.clientFactory = func(_ context.Context, options nativehermes.StartOptions) (nativehermes.Server, error) {
		captured = append(captured, options)
		client := newFakeHermesClient()
		client.xdg = options.ExistingXDG

		return client, nil
	}

	first, err := agent.newHermesClient(
		ctx,
		"session-a",
		t.TempDir(),
		sessionMeta{Env: map[string]string{"HERMES_AUTH_HOME": "/ignored-session-value"}},
		nativehermes.XDGDirs{},
	)
	if err != nil {
		t.Fatalf("first client: %v", err)
	}
	t.Cleanup(func() { _ = first.Close(context.Background()) })

	second, err := agent.newHermesClient(
		ctx,
		"session-b",
		t.TempDir(),
		sessionMeta{},
		nativehermes.XDGDirs{},
	)
	if err != nil {
		t.Fatalf("second client: %v", err)
	}
	t.Cleanup(func() { _ = second.Close(context.Background()) })

	if len(captured) != 2 {
		t.Fatalf("captured starts = %d", len(captured))
	}

	if captured[0].ExistingXDG.Root == captured[1].ExistingXDG.Root {
		t.Fatalf("sessions share runtime home %q", captured[0].ExistingXDG.Root)
	}

	for _, options := range captured {
		if options.ProviderAuthHome != agent.options.ProviderAuthHome {
			t.Fatalf("provider auth home = %q, want %q", options.ProviderAuthHome, agent.options.ProviderAuthHome)
		}
		if _, present := options.Env["HERMES_AUTH_HOME"]; present {
			t.Fatalf("user environment retained protected auth home: %#v", options.Env)
		}
		if _, present := options.SessionEnv["HERMES_AUTH_HOME"]; present {
			t.Fatalf("session environment retained protected auth home: %#v", options.SessionEnv)
		}
	}
}

func testProviders() nativehermes.ProvidersResponse {
	return nativehermes.ProvidersResponse{Providers: []nativehermes.ProviderInfo{{
		ID:   "openai",
		Name: "OpenAI",
		Models: map[string]nativehermes.ProviderModel{
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

// TestAgentRejectsHomeOption proves the isolation contract: Home stays in the
// option surface but its value is rejected with the uniform unsupported-option
// error on every session-establishing path.
func TestAgentRejectsHomeOption(t *testing.T) {
	ctx := context.Background()
	cwd := t.TempDir()
	agent := newTestAgent(WithHome(t.TempDir()))

	_, newErr := agent.NewSession(ctx, NewSessionRequest(cwd))
	requireUnsupportedField(t, newErr, optionFieldHome, "new session")

	_, loadErr := agent.LoadSession(ctx, LoadSessionRequest("session-1", cwd))
	requireUnsupportedField(t, loadErr, optionFieldHome, "load session")

	_, resumeErr := agent.ResumeSession(ctx, ResumeSessionRequest("session-1", cwd))
	requireUnsupportedField(t, resumeErr, optionFieldHome, "resume session")

	_, forkErr := agent.HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest("session-1", cwd)))
	requireUnsupportedField(t, forkErr, optionFieldHome, "fork session")
}

func TestAgentRejectsProviderAuthDirectHomeOption(t *testing.T) {
	ctx := context.Background()
	cwd := t.TempDir()
	agent := newTestAgent(WithProviderAuthDirectHome(t.TempDir()))

	_, newErr := agent.NewSession(ctx, NewSessionRequest(cwd))
	requireUnsupportedField(t, newErr, optionFieldProviderAuthDirectHome, "new session")

	_, loadErr := agent.LoadSession(ctx, LoadSessionRequest("session-1", cwd))
	requireUnsupportedField(t, loadErr, optionFieldProviderAuthDirectHome, "load session")

	_, resumeErr := agent.ResumeSession(ctx, ResumeSessionRequest("session-1", cwd))
	requireUnsupportedField(t, resumeErr, optionFieldProviderAuthDirectHome, "resume session")

	_, forkErr := agent.HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest("session-1", cwd)))
	requireUnsupportedField(t, forkErr, optionFieldProviderAuthDirectHome, "fork session")
}

// TestAgentRejectsUnvalidatedOptionsWithoutInitialize proves the handshake is not
// the only door: an embedded host that opens a session directly is still refused
// when an option failed validation at construction, on every
// session-establishing path.
func TestAgentRejectsUnvalidatedOptionsWithoutInitialize(t *testing.T) {
	ctx := context.Background()
	cwd := t.TempDir()
	agent := newTestAgent(
		WithExecutablePath(filepath.Join(t.TempDir(), "absent-hermes")),
		WithImageLimits(ImageLimits{MaxOutputBytesPerImage: -1}),
	)

	_, newErr := agent.NewSession(ctx, NewSessionRequest(cwd))
	_, loadErr := agent.LoadSession(ctx, LoadSessionRequest("session-1", cwd))
	_, resumeErr := agent.ResumeSession(ctx, ResumeSessionRequest("session-1", cwd))
	_, forkErr := agent.HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest("session-1", cwd)))

	for name, err := range map[string]error{
		"new session":    newErr,
		"load session":   loadErr,
		"resume session": resumeErr,
		"fork session":   forkErr,
	} {
		var reqErr *acp.RequestError
		if !errors.As(err, &reqErr) || !strings.Contains(fmt.Sprint(reqErr.Data), "image limits must be non-negative") {
			t.Fatalf("%s error = %#v", name, err)
		}
	}
}
