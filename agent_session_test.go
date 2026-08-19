//nolint:gocyclo // Lifecycle branch matrices intentionally share setup.
package hermesacp

import (
	"context"
	cryptorand "crypto/rand"
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
	"github.com/savid/acp-go-hermes/internal/lifecycle"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

// toggleReplaceStore wraps InMemorySessionStore and can be switched to fail all
// Replace calls, simulating a disk-full/store outage after native success.
type toggleReplaceStore struct {
	*InMemorySessionStore
	mu   sync.Mutex
	fail bool
}

type ackLostReplaceStore struct {
	*InMemorySessionStore
}

func (s *ackLostReplaceStore) Replace(ctx context.Context, main SessionKey, replacements []SessionStoreReplacement) error {
	if err := s.InMemorySessionStore.Replace(ctx, main, replacements); err != nil {
		return err
	}

	return errors.New("replace acknowledgement lost")
}

func TestNewAndForkReconcileCommittedReplaceAcknowledgementLoss(t *testing.T) {
	ctx := t.Context()
	home := t.TempDir()
	store := &ackLostReplaceStore{InMemorySessionStore: NewInMemorySessionStore()}
	newClient := newFakeHermesClient()
	newClient.createSession = testNativeSession("native-new")
	agent := newTestAgent(WithSessionStore(store), WithSharedHermesHome(home), func(options *Options) {
		options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
			newClient.xdg = opts.ExistingXDG

			return newClient, nil
		}
	})
	created, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession after committed acknowledgement loss: %v", err)
	}
	if agent.activeSession(created.SessionId) == nil {
		t.Fatal("committed NewSession was not registered")
	}

	childClient := newFakeHermesClient()
	childClient.getSession = testNativeSession("native-child")
	parentClient := newFakeHermesClient()
	parentClient.forkSession = testNativeSession("native-child")
	agent.options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
		childClient.xdg = opts.ExistingXDG

		return childClient, nil
	}
	parent := testSession(agent, parentClient)
	parent.id = "parent-for-ack-loss"
	parent.idmap.SessionID = string(parent.id)
	parent.idmap.NativeSessionID = "native-parent"
	agent.sessions[parent.id] = parent
	forked, err := agent.forkSession(ctx, ForkSessionRequest(parent.id, t.TempDir()))
	if err != nil {
		t.Fatalf("Fork after committed acknowledgement loss: %v", err)
	}
	if agent.activeSession(forked.SessionId) == nil || len(parentClient.deleted) != 0 {
		t.Fatalf("committed fork registration/deletion = %#v/%#v", agent.activeSession(forked.SessionId), parentClient.deleted)
	}
}

func TestForkBindsResolvedModelBeforePublishingChild(t *testing.T) {
	for _, test := range []struct {
		name          string
		options       []SessionRequestOption
		parentModel   string
		expectedBind  string
		expectedModel string
	}{
		{
			name: "inherited", parentModel: "xai-oauth/grok-code-fast-1",
			expectedBind: "xai-oauth/grok-code-fast-1", expectedModel: "xai-oauth/grok-code-fast-1",
		},
		{
			name: "explicit", parentModel: "xai-oauth/grok-code-fast-1",
			options: []SessionRequestOption{WithSessionHermesOptions(HermesOptions{
				Model: "openai-codex/gpt-5.4",
			})},
			expectedBind: "openai-codex/gpt-5.4", expectedModel: "openai-codex/gpt-5.4",
		},
		{name: "native default when parent is unselected", expectedModel: "openai/gpt-test"},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := NewInMemorySessionStore()
			parentClient := newFakeHermesClient()
			parentClient.forkSession = testNativeSession("native-child")
			childClient := newFakeHermesClient()
			childClient.getSession = testNativeSession("native-child")
			agent := newTestAgent(WithScratchDir(t.TempDir()), WithSessionStore(store), func(options *Options) {
				options.clientFactory = func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
					childClient.xdg = start.ExistingXDG

					return childClient, nil
				}
			})
			t.Cleanup(func() { _ = agent.Close() })

			parent := testSession(agent, parentClient)
			parent.providerID, parent.modelID = splitModelValue(test.parentModel, "", "")
			agent.sessions[parent.id] = parent
			if err := os.WriteFile(filepath.Join(parentClient.xdg.Root, "state.db"), []byte("parent-state"), 0o600); err != nil {
				t.Fatal(err)
			}

			response, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir(), test.options...))
			if err != nil {
				t.Fatalf("fork: %v", err)
			}
			childClient.mu.Lock()
			setCalls := append([]fakeModelSelection(nil), childClient.setModelCalls...)
			childClient.mu.Unlock()
			if test.expectedBind == "" {
				if len(setCalls) != 0 {
					t.Fatalf("unselected parent forced native model binds = %#v", setCalls)
				}
			} else if len(setCalls) != 1 || setCalls[0] != (fakeModelSelection{sessionID: "native-child", value: test.expectedBind}) {
				t.Fatalf("native model binds = %#v", setCalls)
			}
			published := agent.activeSession(response.SessionId)
			if published == nil {
				t.Fatal("fork child was not published")
			}
			if got := published.currentModel(); got != test.expectedModel {
				t.Fatalf("published model = %q", got)
			}
			meta, _ := response.Meta[hermesMetaKey].(map[string]any)
			if meta["modelId"] != test.expectedModel {
				t.Fatalf("response model meta = %#v", meta)
			}
			summaries, err := store.ListSessions(t.Context())
			if err != nil || len(summaries) != 1 || summaries[0].SessionID != string(response.SessionId) {
				t.Fatalf("stored fork summaries = %#v err=%v", summaries, err)
			}
		})
	}
}

func TestForkModelBindFailureCleansChildBeforePublication(t *testing.T) {
	store := NewInMemorySessionStore()
	parentClient := newFakeHermesClient()
	parentClient.forkSession = testNativeSession("native-child")
	childClient := newFakeHermesClient()
	childClient.getSession = testNativeSession("native-child")
	childClient.setModelErr = errors.New("model bind failed")
	agent := newTestAgent(WithScratchDir(t.TempDir()), WithSessionStore(store), func(options *Options) {
		options.clientFactory = func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
			childClient.xdg = start.ExistingXDG

			return childClient, nil
		}
	})
	parent := testSession(agent, parentClient)
	agent.sessions[parent.id] = parent
	if err := os.WriteFile(filepath.Join(parentClient.xdg.Root, "state.db"), []byte("parent-state"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil || !strings.Contains(err.Error(), "bind Hermes fork model") {
		t.Fatalf("fork model bind error = %v", err)
	}
	if !childClient.closed || len(parentClient.deleted) != 1 || parentClient.deleted[0] != "native-child" {
		t.Fatalf("failed bind cleanup childClosed=%v parentDeleted=%#v", childClient.closed, parentClient.deleted)
	}
	agent.mu.Lock()
	activeCount := len(agent.sessions)
	agent.mu.Unlock()
	if activeCount != 1 {
		t.Fatalf("failed bind published child: active=%d", activeCount)
	}
	summaries, err := store.ListSessions(t.Context())
	if err != nil || len(summaries) != 0 {
		t.Fatalf("failed bind stored child: %#v err=%v", summaries, err)
	}
}

func TestForkModelBindFailureRetainsChildWithUnprovenContainment(t *testing.T) {
	parentClient := newFakeHermesClient()
	parentClient.forkSession = testNativeSession("native-child")
	childClient := newFakeHermesClient()
	childClient.getSession = testNativeSession("native-child")
	childClient.setModelErr = errors.New("model bind failed")
	childClient.closeErr = nativehermes.ErrProcessContainmentIncomplete
	agent := newTestAgent(WithScratchDir(t.TempDir()), func(options *Options) {
		options.clientFactory = func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
			childClient.xdg = start.ExistingXDG

			return childClient, nil
		}
	})
	parent := testSession(agent, parentClient)
	agent.sessions[parent.id] = parent
	if err := os.WriteFile(filepath.Join(parentClient.xdg.Root, "state.db"), []byte("parent-state"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); !errors.Is(err, nativehermes.ErrProcessContainmentIncomplete) {
		t.Fatalf("fork unproven containment error = %v", err)
	}
	if !childClient.closed || len(parentClient.deleted) != 0 {
		t.Fatalf("unproven child was destructively cleaned: closed=%v deleted=%#v", childClient.closed, parentClient.deleted)
	}
	agent.mu.Lock()
	activeCount := len(agent.sessions)
	retainedCount := len(agent.incompleteRoots)
	agent.mu.Unlock()
	if activeCount != 1 || retainedCount != 1 {
		t.Fatalf("unproven failed bind publication/retention = active %d retained %d", activeCount, retainedCount)
	}
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
			xdg, err := testGenerationXDG(opts.ScratchParent)
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
				xdg, err = testGenerationXDG(opts.ScratchParent)
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
					xdg, err = testGenerationXDG(opts.ScratchParent)
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
	_, err4 := agent.SetSessionConfigOption(ctx, acp.SetSessionConfigOptionRequest{
		Boolean: &acp.SetSessionConfigOptionBoolean{SessionId: newResp.SessionId, ConfigId: configModel, Type: "boolean", Value: true},
	})
	// "type" is the discriminator that selected the boolean variant, and the
	// only JSON path a host can act on: neither union variant is itself a wire
	// field.
	requireUnsupportedField(t, err4, keyType, "boolean config option")
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
		t.Fatalf("xdg roots parent=%#v child=%#v scratch=%q", parent.xdg, child.xdg, agent.options.ScratchDir)
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
	sourceXDG, err := testGenerationXDG(root)
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

func TestCloseSessionWaitsForPendingTurnSettlement(t *testing.T) {
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
	if got := store.replaceCount(); got != 1 {
		t.Fatalf("settlement Replace count = %d, want 1", got)
	}
}

func TestCloseSessionSnapshotsBeforeNativeRootRemoval(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	xdg, createErr := testGenerationXDG(root)
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

func TestCloseWaitsForAdmittedTurnBeforeSnapshot(t *testing.T) {
	ctx := context.Background()
	store := newCountingSessionStore()
	client := newFakeHermesClient()
	agent := newTestAgent(WithSessionStore(store))
	session := testSession(agent, client)
	agent.mu.Lock()
	agent.sessions[session.id] = session
	agent.mu.Unlock()

	release, settlement, err := session.acquireTurn(ctx)
	if err != nil {
		t.Fatalf("acquireTurn: %v", err)
	}
	if reason := session.snapshotBlockedReason(); reason != "turn" {
		t.Fatalf("fence not set after acquireTurn: %q", reason)
	}
	if err := session.snapshotToStore(ctx); err == nil {
		t.Fatal("snapshot ran while turn in flight")
	}
	closeDone := make(chan error, 1)
	go func() {
		_, closeErr := agent.CloseSession(ctx, acp.CloseSessionRequest{SessionId: session.id})
		closeDone <- closeErr
	}()

	deadline := time.Now().Add(time.Second)
	for {
		session.mu.Lock()
		closing := session.lifecycleClosing
		session.mu.Unlock()
		if closing {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("CloseSession did not close prompt admission")
		}

		time.Sleep(time.Millisecond)
	}

	if _, _, prepareErr := session.preparePromptTurn(ctx, "close-admission"); prepareErr == nil ||
		!strings.Contains(prepareErr.Error(), valSessionClosed) {
		t.Fatalf("turn admitted after CloseSession: %v", prepareErr)
	}
	select {
	case closeErr := <-closeDone:
		t.Fatalf("CloseSession did not wait for admitted turn: %v", closeErr)
	default:
	}
	if got := store.replaceCount(); got != 0 {
		t.Fatalf("Replace happened before turn settlement: %d", got)
	}

	release()
	settlement.complete()
	if closeErr := <-closeDone; closeErr != nil {
		t.Fatalf("CloseSession: %v", closeErr)
	}
	if got := store.replaceCount(); got != 1 {
		t.Fatalf("post-settlement Replace count = %d, want 1", got)
	}
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
				xdg, err = testGenerationXDG(opts.ScratchParent)
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
				createErrClient.xdg, _ = testGenerationXDG(opts.ScratchParent)

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
	sourceXDG, err := testGenerationXDG(root)
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
	if _, _, err := defaultSession.acquireTurn(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled acquireTurn error = %v", err)
	}
	if _, _, err := defaultSession.acquireTurn(ctx); err == nil {
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
				createClient.xdg, err = testGenerationXDG(opts.ScratchParent)
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
	sourceXDG, err := testGenerationXDG(t.TempDir())
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

	validTarget, err := testGenerationXDG(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err33 := cloneHermesStateDB("", nativehermes.XDGDirs{Root: string([]byte{0})}, validTarget); err33 == nil {
		t.Fatal("cloneHermesStateDB accepted invalid source")
	}
	restoreStateStoreSeams(t)
	stateRemoveAll = func(string) error { return errors.New("remove failed") }
	validSource, err := testGenerationXDG(t.TempDir())
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
				defaultClient.xdg, err = testGenerationXDG(opts.ScratchParent)
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
				client.xdg, err = testGenerationXDG(opts.ScratchParent)
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
		if client.closed {
			t.Fatal("active-session admission launched and then closed a client")
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

	t.Run("delete tombstones before it inspects the session", func(t *testing.T) {
		client := newFakeHermesClient()
		store := NewInMemorySessionStore()
		agent := newTestAgent(WithSessionStore(store))
		session := testSession(agent, client)
		session.cancel = func() {}
		session.turnEpoch = 1
		agent.sessions[session.id] = session
		entry, _ := json.Marshal(stateSnapshot{Format: SessionStoreFormat, Session: stateSnapshotSession{SessionID: string(session.id)}})
		if err := store.Replace(ctx, SessionKey{SessionID: string(session.id)}, []SessionStoreReplacement{{Key: SessionKey{SessionID: string(session.id)}, Entries: []SessionStoreEntry{entry}}}); err != nil {
			t.Fatal(err)
		}
		if _, err := agent.UnstableDeleteSession(ctx, DeleteSessionRequest(session.id)); err != nil {
			t.Fatalf("delete over a live cancel func = %v", err)
		}
		if len(client.deleted) != 1 {
			t.Fatalf("native delete attempts = %#v", client.deleted)
		}
		if _, ok := agent.sessions[session.id]; ok {
			t.Fatal("delete left the in-memory session addressable")
		}
		if entries, err := store.Load(ctx, SessionKey{SessionID: string(session.id)}); err != nil || len(entries) != 0 {
			t.Fatalf("delete left store state behind: %d entries, err=%v", len(entries), err)
		}
	})

	t.Run("delete active propagates native delete error after tombstone", func(t *testing.T) {
		client := newFakeHermesClient()
		client.deleteErr = errors.New("native delete failed")
		agent := newTestAgent()
		session := testSession(agent, client)
		agent.sessions[session.id] = session
		if _, err := agent.UnstableDeleteSession(ctx, DeleteSessionRequest(session.id)); !errors.Is(err, client.deleteErr) {
			t.Fatalf("delete error = %v, want native delete error", err)
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
				xdg, err := testGenerationXDG(root)
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

// TestDeleteCancelsAnActivePromptAndNoLaterWriteRecreatesTheRow pins the delete
// order: the tombstone is durable first and the id is hidden with it, then the
// active turn is cancelled and settled, then the runtime is torn down. An active
// prompt is a thing delete settles, not a ground to refuse on — and the commit
// that settlement owes lands after the tombstone, so it must publish nothing.
func TestDeleteCancelsAnActivePromptAndNoLaterWriteRecreatesTheRow(t *testing.T) {
	client := newFakeHermesClient()
	started := make(chan struct{})
	client.sendMessage = func(ctx context.Context, _ string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		close(started)
		<-ctx.Done()

		return nativehermes.NativeMessage{}, ctx.Err()
	}

	store := NewInMemorySessionStore()
	agent := newTestAgent(WithSessionStore(store), WithScratchDir(t.TempDir()))
	session := testSession(agent, client)
	agent.mu.Lock()
	agent.sessions[session.id] = session
	agent.mu.Unlock()

	key := SessionKey{SessionID: string(session.id)}
	require.NoError(t, session.snapshotToStore(t.Context()))
	entries, err := store.Load(t.Context(), key)
	require.NoError(t, err)
	require.NotEmpty(t, entries, "durable row missing before delete")

	type promptOutcome struct {
		resp acp.PromptResponse
		err  error
	}

	promptDone := make(chan promptOutcome, 1)

	go func() {
		resp, promptErr := session.Prompt(context.Background(), TextPromptRequest(session.id, "delete-active", "hang"))
		promptDone <- promptOutcome{resp: resp, err: promptErr}
	}()
	<-started

	_, err = agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(session.id))
	require.NoError(t, err, "delete refused an active prompt")

	out := <-promptDone
	require.NoError(t, out.err)
	require.Equal(t, acp.StopReasonCancelled, out.resp.StopReason, "delete did not cancel the active turn")

	// The settlement's terminal commit ran after the tombstone. Nothing it wrote
	// may clear a tombstone it did not create.
	entries, err = store.Load(t.Context(), key)
	require.NoError(t, err)
	require.Empty(t, entries, "a post-tombstone commit recreated the deleted row")

	require.True(t, agent.isDeleted(session.id))

	agent.mu.Lock()
	_, live := agent.sessions[session.id]
	agent.mu.Unlock()
	require.False(t, live, "delete left the session addressable")

	listed, err := agent.ListSessions(t.Context(), ListSessionsRequest())
	require.NoError(t, err)

	for _, info := range listed.Sessions {
		require.NotEqual(t, session.id, info.SessionId, "deleted session was listed")
	}

	// Deleting the same id again silently succeeds.
	_, err = agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(session.id))
	require.NoError(t, err, "delete was not idempotent")
}

// TestInstallRefusesATombstoneItDidNotCreate pins the install lock's own
// tombstone re-check. The entry check a load or resume ran is only a guess by
// the time there is something to install, and the marker it re-reads is the
// same one the durable publish guard reads: an install that cleared it would
// un-hide the id and let the next commit rewrite the row the delete removed.
func TestInstallRefusesATombstoneItDidNotCreate(t *testing.T) {
	client := newFakeHermesClient()
	store := NewInMemorySessionStore()
	agent := newTestAgent(WithSessionStore(store), WithScratchDir(t.TempDir()))
	session := testSession(agent, client)

	agent.mu.Lock()
	agent.sessions[session.id] = session
	agent.mu.Unlock()

	key := SessionKey{SessionID: string(session.id)}
	require.NoError(t, session.snapshotToStore(t.Context()))

	entries, err := store.Load(t.Context(), key)
	require.NoError(t, err)
	require.NotEmpty(t, entries, "durable row missing before delete")

	_, err = agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(session.id))
	require.NoError(t, err)
	require.True(t, agent.isDeleted(session.id), "the delete set the tombstone marker")

	// Exactly what a load, resume, or fork that started before the delete does
	// when it finally reaches its install step.
	late := testSession(agent, newFakeHermesClient())
	late.id = session.id

	installErr := agent.storeStartedSession(late)
	require.Error(t, installErr, "a late install published a tombstoned id")

	var refusal *acp.RequestError
	require.ErrorAs(t, installErr, &refusal)
	require.Equal(t, acp.NewInvalidParams(map[string]any{
		jsonFieldError: valUnknownSession, keyField: jsonFieldSessionID,
	}), refusal, "a tombstoned id answers with the uniform unknown-session refusal")

	require.True(t, agent.isDeleted(session.id), "a late install cleared a tombstone it did not create")

	agent.mu.Lock()
	_, live := agent.sessions[session.id]
	agent.mu.Unlock()
	require.False(t, live, "a refused install left the id live")

	// The durable guard is that same marker, so it still holds.
	require.NoError(t, late.snapshotToStore(t.Context()))

	entries, err = store.Load(t.Context(), key)
	require.NoError(t, err)
	require.Empty(t, entries, "the deleted row was durably resurrected")

	listed, err := agent.ListSessions(t.Context(), ListSessionsRequest())
	require.NoError(t, err)

	for _, info := range listed.Sessions {
		require.NotEqual(t, session.id, info.SessionId, "a deleted session was listed after a late install")
	}
}

// TestLoadLosingTheRaceToADeleteInstallsNothing drives the same rule through the
// whole load transaction: the delete completes after the scratch reservation,
// the generation, the archive hydration, the native-owner acquisition, and the
// runtime launch have all happened. However far the preparation got, the delete
// wins — the prepared replacement is torn down and the caller is told what every
// other door tells it about a deleted id.
func TestLoadLosingTheRaceToADeleteInstallsNothing(t *testing.T) {
	ctx := t.Context()
	cwd := t.TempDir()
	store := validHydrateStore(t, ctx)
	loaded := newFakeHermesClient()
	loaded.getSession = testNativeSession("n")

	var (
		agent      *Agent
		deleteOnce sync.Once
		deleteErr  error
	)

	agent = newTestAgent(WithSessionStore(store), WithScratchDir(t.TempDir()), func(options *Options) {
		options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
			loaded.xdg = opts.ExistingXDG
			deleteOnce.Do(func() {
				_, deleteErr = agent.UnstableDeleteSession(ctx, DeleteSessionRequest("s"))
			})

			return loaded, nil
		}
	})

	_, err := agent.LoadSession(ctx, LoadSessionRequest("s", cwd))
	require.NoError(t, deleteErr, "the delete this load raced failed")
	require.Error(t, err, "a load that lost the race to a delete installed its session")

	var refusal *acp.RequestError
	require.ErrorAs(t, err, &refusal)
	require.Equal(t, acp.NewInvalidParams(map[string]any{
		jsonFieldError: valUnknownSession, keyField: jsonFieldSessionID,
	}), refusal, "a deleted id must be wire-indistinguishable from one that never existed")

	require.True(t, agent.isDeleted("s"), "the losing install cleared the deletion marker")
	require.Positive(t, loaded.closeCount(), "the prepared replacement was left running")

	agent.mu.Lock()
	_, live := agent.sessions["s"]
	agent.mu.Unlock()
	require.False(t, live, "a refused install left the session addressable")

	entries, loadErr := store.Load(ctx, SessionKey{SessionID: "s"})
	require.NoError(t, loadErr)
	require.Empty(t, entries, "the deleted row was durably resurrected")
}

// TestLoadRacingDeleteResurrectsNothing races the two for real. Either order is
// legal — a load that installed before the tombstone landed keeps the id, and
// the delete then closes it as the active session it is — but no interleaving
// may leave a live session, or a durable row, behind a tombstoned id.
func TestLoadRacingDeleteResurrectsNothing(t *testing.T) {
	cwd := t.TempDir()

	for attempt := range 8 {
		t.Run(fmt.Sprintf("attempt-%d", attempt), func(t *testing.T) {
			ctx := t.Context()
			store := validHydrateStore(t, ctx)
			loaded := newFakeHermesClient()
			loaded.getSession = testNativeSession("n")
			agent := newTestAgent(WithSessionStore(store), WithScratchDir(t.TempDir()), func(options *Options) {
				options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
					loaded.xdg = opts.ExistingXDG

					return loaded, nil
				}
			})

			start := make(chan struct{})
			var wait sync.WaitGroup

			wait.Add(2)

			go func() {
				defer wait.Done()
				<-start
				_, _ = agent.LoadSession(ctx, LoadSessionRequest("s", cwd))
			}()

			go func() {
				defer wait.Done()
				<-start
				_, _ = agent.UnstableDeleteSession(ctx, DeleteSessionRequest("s"))
			}()

			close(start)
			wait.Wait()

			require.True(t, agent.isDeleted("s"), "the delete's marker did not survive the race")

			agent.mu.Lock()
			_, live := agent.sessions["s"]
			agent.mu.Unlock()
			require.False(t, live, "a tombstoned id was left naming a live session")

			entries, loadErr := store.Load(ctx, SessionKey{SessionID: "s"})
			require.NoError(t, loadErr)
			require.Empty(t, entries, "the deleted row was durably resurrected")

			listed, listErr := agent.ListSessions(ctx, ListSessionsRequest())
			require.NoError(t, listErr)
			require.Empty(t, listed.Sessions, "a deleted session was listed after the race")
		})
	}
}

// installOnDeleteStore installs a session under the deleted id at the exact
// instant the tombstone is being made durable — the window between the delete's
// read of the active set and the lock that hides the id.
type installOnDeleteStore struct {
	*InMemorySessionStore
	once    sync.Once
	install func()
}

func (s *installOnDeleteStore) Delete(ctx context.Context, key SessionKey) error {
	s.once.Do(s.install)

	return s.InMemorySessionStore.Delete(ctx, key)
}

// TestDeleteClosesASessionInstalledInsideItsTombstoneWindow pins the other side
// of the same race. An install that reached the lock first is legal, and the id
// it published is the one this delete is about: the delete owns its teardown,
// because once the marker is set nothing else can ever reach that runtime.
func TestDeleteClosesASessionInstalledInsideItsTombstoneWindow(t *testing.T) {
	client := newFakeHermesClient()

	var (
		agent *Agent
		late  *session
	)

	store := &installOnDeleteStore{InMemorySessionStore: NewInMemorySessionStore()}
	store.install = func() {
		agent.mu.Lock()
		agent.sessions[late.id] = late
		agent.mu.Unlock()
	}

	agent = newTestAgent(WithSessionStore(store), WithScratchDir(t.TempDir()))
	late = testSession(agent, client)

	_, err := agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(late.id))
	require.NoError(t, err)

	require.True(t, agent.isDeleted(late.id))
	require.Positive(t, client.closeCount(), "the delete left a live runtime behind a tombstoned id")

	agent.mu.Lock()
	_, live := agent.sessions[late.id]
	agent.mu.Unlock()
	require.False(t, live, "a tombstoned id was left naming a live session")
}

// TestDeleteSurfacesTeardownErrorsWithTheSessionAlreadyHidden pins the tail of
// the same order: a teardown that fails is reported, but only after the
// tombstone is durable, and the session stays hidden either way.
func TestDeleteSurfacesTeardownErrorsWithTheSessionAlreadyHidden(t *testing.T) {
	client := newFakeHermesClient()
	client.closeErr = errors.New("runtime close failed")
	store := NewInMemorySessionStore()
	agent := newTestAgent(WithSessionStore(store), WithScratchDir(t.TempDir()))
	session := testSession(agent, client)
	agent.mu.Lock()
	agent.sessions[session.id] = session
	agent.mu.Unlock()

	key := SessionKey{SessionID: string(session.id)}
	require.NoError(t, session.snapshotToStore(t.Context()))

	_, err := agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(session.id))
	require.ErrorIs(t, err, client.closeErr, "teardown error was not surfaced")

	require.True(t, agent.isDeleted(session.id), "failed teardown left the session untombstoned")

	entries, loadErr := store.Load(t.Context(), key)
	require.NoError(t, loadErr)
	require.Empty(t, entries, "failed teardown left the durable row behind")

	agent.mu.Lock()
	_, live := agent.sessions[session.id]
	agent.mu.Unlock()
	require.False(t, live, "failed teardown left the session addressable")
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
		// An id nothing is remembered for is nothing to forget, and its runtime
		// root is not derivable from the id: only the remembered record names it.
		agent.forgetDeleteCleanupIfDone("never-remembered")

		xdg, err := testGenerationXDG(agent.options.ScratchDir)
		if err != nil {
			t.Fatal(err)
		}
		agent.deleteCleanup["keep"] = deleteCleanupRecord{SessionID: "keep", XDGRoot: xdg.Root}
		agent.forgetDeleteCleanupIfDone("keep")
		if _, ok := agent.deleteCleanup["keep"]; !ok {
			t.Fatal("cleanup metadata was forgotten while XDG root still existed")
		}

		agent.deleteCleanup["gone"] = deleteCleanupRecord{SessionID: "gone", XDGRoot: filepath.Join(xdg.Root, "removed")}
		agent.forgetDeleteCleanupIfDone("gone")
		if _, ok := agent.deleteCleanup["gone"]; ok {
			t.Fatal("cleanup metadata survived a runtime root that is already gone")
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
				entryXDG, err := testGenerationXDG(entryAgent.options.ScratchDir)
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
		parentClient.xdg, _ = testGenerationXDG(t.TempDir())
		parentAgent := newTestAgent()
		parent := testSession(parentAgent, parentClient)
		parentAgent.sessions[parent.id] = parent

		sessionIDRandReader = errorReader{err: errors.New("id failed")}
		forkCalls := parentClient.forkCallCount()
		if _, err := parentAgent.HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest(parent.id, cwd))); err == nil {
			t.Fatal("fork ignored session id error")
		}
		if parentClient.forkCallCount() != forkCalls {
			t.Fatal("fork mutated native state before ACP ID preparation")
		}
		sessionIDRandReader = oldReader

		parentClient.xdg = nativehermes.XDGDirs{Root: string([]byte{0})}
		forkCalls = parentClient.forkCallCount()
		if _, err := parentAgent.HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest(parent.id, cwd))); err == nil {
			t.Fatal("fork ignored state db clone error")
		}
		if parentClient.forkCallCount() != forkCalls+1 || len(parentClient.deleted) == 0 || parentClient.deleted[len(parentClient.deleted)-1] != "native-child" {
			t.Fatal("post-branch clone failure did not compensate the durable child")
		}
		parentClient.xdg, _ = testGenerationXDG(t.TempDir())

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
		if len(parentClient.deleted) == 0 || parentClient.deleted[len(parentClient.deleted)-1] != "native-child" {
			t.Fatalf("fork factory failure did not compensate durable child: %#v", parentClient.deleted)
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
		if len(parentClient.deleted) == 0 || parentClient.deleted[len(parentClient.deleted)-1] != "native-child" {
			t.Fatalf("fork get failure did not compensate durable child: %#v", parentClient.deleted)
		}

		driftChild := newFakeHermesClient()
		driftChild.getSession = testNativeSession("native-drift")
		driftAgent := newTestAgent(func(options *Options) {
			options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
				driftChild.xdg = opts.ExistingXDG

				return driftChild, nil
			}
		})
		driftParent := testSession(driftAgent, parentClient)
		driftAgent.sessions[driftParent.id] = driftParent
		if _, err := driftAgent.HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest(driftParent.id, cwd))); err == nil || !strings.Contains(err.Error(), "native session drift") {
			t.Fatalf("fork native drift error = %v", err)
		}
		if !driftChild.closed || len(parentClient.deleted) == 0 || parentClient.deleted[len(parentClient.deleted)-1] != "native-child" {
			t.Fatalf("fork drift cleanup childClosed=%v parentDeleted=%#v", driftChild.closed, parentClient.deleted)
		}

		busyParent := testSession(parentAgent, parentClient)
		busyParent.mu.Lock()
		busyParent.turnInFlight = true
		busyParent.mu.Unlock()
		parentAgent.sessions[busyParent.id] = busyParent
		forkCalls = parentClient.forkCallCount()
		if _, err := parentAgent.HandleExtensionMethod(ctx, ForkSessionMethod, mustJSON(t, ForkSessionRequest(busyParent.id, cwd))); err == nil || !strings.Contains(err.Error(), "session cannot be forked") {
			t.Fatalf("busy fork error = %v", err)
		}
		if parentClient.forkCallCount() != forkCalls {
			t.Fatal("busy parent reached native fork")
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

func TestSharedHermesHomePreservesPerSessionWrapperGenerations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	authHome := testNativeOwnedDir(t, "native-auth")
	agent := newTestAgent(
		WithScratchDir(t.TempDir()),
		WithProviderAuthRoot(t.TempDir()),
		WithSharedHermesHome(authHome),
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
		sessionMeta{Env: map[string]string{"WAGIE_OPERATION_ID": "first"}},
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
		if options.SharedHermesHome != agent.options.SharedHermesHome {
			t.Fatalf("shared Hermes home = %q, want %q", options.SharedHermesHome, agent.options.SharedHermesHome)
		}
	}
	if captured[0].SessionEnv["WAGIE_OPERATION_ID"] != "first" || captured[1].SessionEnv["WAGIE_OPERATION_ID"] != "" {
		t.Fatalf("per-session environments were not preserved: %#v", captured)
	}
}

func TestSharedHermesHomeAdmitsRotatedMCPSecretsByRedactedShape(t *testing.T) {
	agent := newTestAgent(WithSharedHermesHome(t.TempDir()))
	first := []acp.McpServer{
		StdioMCPServer("stdio", "runner", nil, map[string]string{"TOKEN": "first-stdio"}),
		HTTPMCPServer("http", "https://mcp.example.test", map[string]string{"Authorization": "first-http"}),
	}
	second := []acp.McpServer{
		StdioMCPServer("stdio", "runner", nil, map[string]string{"TOKEN": "second-stdio"}),
		HTTPMCPServer("http", "https://mcp.example.test", map[string]string{"Authorization": "second-http"}),
	}
	if err := agent.admitSharedHermesConfig(first); err != nil {
		t.Fatalf("first MCP shape: %v", err)
	}
	if err := agent.admitSharedHermesConfig(second); err != nil {
		t.Fatalf("rotated secrets changed shared MCP shape: %v", err)
	}
	encoded, err := json.Marshal(agent.sharedMCPServers)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"first-stdio", "first-http", "second-stdio", "second-http"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("redacted shared MCP admission retained %q: %s", secret, encoded)
		}
	}
	if err := agent.admitSharedHermesConfig([]acp.McpServer{StdioMCPServer("different", "runner", nil, map[string]string{"TOKEN": "third"})}); err == nil {
		t.Fatal("different MCP structure was admitted")
	}
}

func testProviders() nativehermes.ProvidersResponse {
	return nativehermes.ProvidersResponse{Providers: []nativehermes.ProviderInfo{{
		ID:   "openai",
		Name: "OpenAI",
		Models: map[string]nativehermes.ProviderModel{
			"gpt-test":  {ID: "gpt-test", Name: "GPT Test"},
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
		if !errors.As(err, &reqErr) || reqErr.Code != -32603 ||
			!strings.Contains(fmt.Sprint(reqErr.Data), "image limits must be non-negative") {
			t.Fatalf("%s error = %#v", name, err)
		}
	}
}

type faultSessionSetLease struct {
	err      error
	releases int
}

func (l *faultSessionSetLease) Release() error {
	l.releases++

	return l.err
}

func installFaultSessionSetLease(t *testing.T, wantMode nativehermes.SharedSessionSetLockMode) *faultSessionSetLease {
	t.Helper()

	lease := &faultSessionSetLease{err: errors.New("release acknowledgement lost")}
	previous := acquireSharedSessionSetLock
	acquireSharedSessionSetLock = func(
		_ context.Context,
		_ string,
		mode nativehermes.SharedSessionSetLockMode,
	) (sharedSessionSetLease, error) {
		if mode != wantMode {
			t.Fatalf("session-set lock mode = %v, want %v", mode, wantMode)
		}

		return lease, nil
	}
	t.Cleanup(func() { acquireSharedSessionSetLock = previous })

	return lease
}

func TestCommittedSharedLifecycleIgnoresLostLockReleaseAcknowledgement(t *testing.T) {
	t.Run("new", func(t *testing.T) {
		lease := installFaultSessionSetLease(t, nativehermes.SharedSessionSetLockExclusive)
		client := newFakeHermesClient()
		client.createSession = testNativeSession("native-new")
		agent := newSharedHomeLifecycleAgent(t, client)

		response, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
		if err != nil {
			t.Fatalf("committed new session: %v", err)
		}
		if lease.releases != 1 {
			t.Fatalf("release calls = %d, want 1", lease.releases)
		}
		if session := agent.activeSession(response.SessionId); session != nil {
			_ = session.Close(t.Context())
		}
	})

	t.Run("fork", func(t *testing.T) {
		lease := installFaultSessionSetLease(t, nativehermes.SharedSessionSetLockExclusive)
		parentClient := newFakeHermesClient()
		parentClient.forkSession = testNativeSession("native-child")
		childClient := newFakeHermesClient()
		childClient.getSession = testNativeSession("native-child")
		agent := newTestAgent(
			WithScratchDir(t.TempDir()),
			WithSharedHermesHome(t.TempDir()),
			WithSessionStore(NewInMemorySessionStore()),
		)
		agent.options.clientFactory = func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
			childClient.xdg = start.ExistingXDG

			return childClient, nil
		}
		parent := testSession(agent, parentClient)
		parent.id = "parent"
		parent.idmap.SessionID = "parent"
		parent.idmap.NativeSessionID = "native-parent"
		agent.sessions[parent.id] = parent

		response, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir()))
		if err != nil {
			t.Fatalf("committed fork: %v", err)
		}
		if lease.releases != 1 {
			t.Fatalf("release calls = %d, want 1", lease.releases)
		}
		if session := agent.activeSession(response.SessionId); session != nil {
			_ = session.Close(t.Context())
		}
	})

	t.Run("prompt", func(t *testing.T) {
		lease := installFaultSessionSetLease(t, nativehermes.SharedSessionSetLockShared)
		client := newFakeHermesClient()
		agent := newTestAgent(WithSharedHermesHome(t.TempDir()), WithSessionStore(NewInMemorySessionStore()))
		agent.setAgentClient(newRecordingAgentClient())
		session := testSession(agent, client)

		response, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "release-ack", "hello"))
		if err != nil || response.StopReason != acp.StopReasonEndTurn {
			t.Fatalf("committed prompt response = %+v, err = %v", response, err)
		}
		if lease.releases != 1 {
			t.Fatalf("release calls = %d, want 1", lease.releases)
		}
	})
}

func newSharedHomeLifecycleAgent(t *testing.T, client *fakeHermesClient, options ...Option) *Agent {
	t.Helper()
	all := make([]Option, 0, 3+len(options))
	all = append(all, WithScratchDir(t.TempDir()), WithSharedHermesHome(t.TempDir()), WithSessionStore(NewInMemorySessionStore()))
	all = append(all, options...)
	agent := newTestAgent(all...)
	agent.options.clientFactory = func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
		client.xdg = start.ExistingXDG

		return client, nil
	}

	return agent
}

func TestSharedNewSessionTransactionFailureEdges(t *testing.T) {
	t.Run("baseline and operation entropy", func(t *testing.T) {
		client := newFakeHermesClient()
		client.persistedSessions = []nativehermes.Session{{ID: "existing"}}
		agent := newSharedHomeLifecycleAgent(t, client)
		previousSessionReader, previousOperationReader := sessionIDRandReader, cryptorand.Reader
		sessionIDRandReader = strings.NewReader(strings.Repeat("x", 16))
		cryptorand.Reader = sessionOperationErrorReader{err: errors.New("operation entropy")}
		t.Cleanup(func() {
			sessionIDRandReader = previousSessionReader
			cryptorand.Reader = previousOperationReader
		})
		if _, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir())); err == nil {
			t.Fatal("operation-id entropy failure ignored")
		}
	})

	t.Run("session set lock", func(t *testing.T) {
		client := newFakeHermesClient()
		client.createSession = testNativeSession("native")
		agent := newSharedHomeLifecycleAgent(t, client)
		lock, err := nativehermes.AcquireSharedSessionSetLock(t.Context(), agent.options.SharedHermesHome, nativehermes.SharedSessionSetLockExclusive)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = lock.Release() }()
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		if _, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir())); err == nil {
			t.Fatal("contended session-set lock succeeded")
		}
	})

	t.Run("pending recovery", func(t *testing.T) {
		client := newFakeHermesClient()
		client.createSession = testNativeSession("native")
		agent := newSharedHomeLifecycleAgent(t, client)
		control, err := nativehermes.EnsureSharedHermesAdapterControlDir(agent.options.SharedHermesHome)
		if err != nil {
			t.Fatal(err)
		}
		operations := filepath.Join(control, sessionOperationDirectoryName)
		if err := os.MkdirAll(operations, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(operations, "unexpected"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir())); err == nil {
			t.Fatal("malformed pending recovery ignored")
		}
	})

	t.Run("persisted inventory", func(t *testing.T) {
		client := newFakeHermesClient()
		client.listErr = errors.New("inventory")
		agent := newSharedHomeLifecycleAgent(t, client)
		if _, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir())); err == nil {
			t.Fatal("inventory failure ignored")
		}
	})

	t.Run("begin journal", func(t *testing.T) {
		client := newFakeHermesClient()
		agent := newSharedHomeLifecycleAgent(t, client)
		previous := sessionOperationMkdir
		sessionOperationMkdir = func(string, os.FileMode) error { return errors.New("journal mkdir") }
		t.Cleanup(func() { sessionOperationMkdir = previous })
		if _, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir())); err == nil {
			t.Fatal("journal begin failure ignored")
		}
	})

	t.Run("advance journal", func(t *testing.T) {
		client := newFakeHermesClient()
		agent := newSharedHomeLifecycleAgent(t, client)
		previous := sessionOperationNow
		calls := 0
		sessionOperationNow = func() time.Time {
			calls++
			if calls > 1 {
				return time.UnixMilli(1)
			}

			return time.Now()
		}
		t.Cleanup(func() { sessionOperationNow = previous })
		if _, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir())); err == nil {
			t.Fatal("journal phase failure ignored")
		}
	})

	t.Run("native claim", func(t *testing.T) {
		client := newFakeHermesClient()
		client.createSession = testNativeSession("native-conflict")
		agent := newSharedHomeLifecycleAgent(t, client)
		owner, err := nativehermes.AcquireSharedNativeSessionOwner(agent.options.SharedHermesHome, "native-conflict")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = owner.Release() }()
		if _, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir())); err == nil {
			t.Fatal("conflicting native claim succeeded")
		}
	})

	t.Run("active registration after commit", func(t *testing.T) {
		client := newFakeHermesClient()
		client.createSessionFunc = func(context.Context, string) (nativehermes.Session, error) {
			return testNativeSession("native-capacity"), nil
		}
		agent := newSharedHomeLifecycleAgent(t, client)
		client.createSessionFunc = func(context.Context, string) (nativehermes.Session, error) {
			agent.mu.Lock()
			for index := 0; index < agent.options.ConcurrencyLimits.MaxActiveSessions; index++ {
				agent.sessions[acp.SessionId("capacity-"+string(rune('a'+index)))] = nil
			}
			agent.mu.Unlock()

			return testNativeSession("native-capacity"), nil
		}
		if _, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir())); err == nil {
			t.Fatal("post-commit registration capacity failure ignored")
		}
		agent.mu.Lock()
		clear(agent.sessions)
		agent.mu.Unlock()
	})

	t.Run("committed journal cleanup", func(t *testing.T) {
		client := newFakeHermesClient()
		client.createSession = testNativeSession("native-success")
		agent := newSharedHomeLifecycleAgent(t, client)
		previous := sessionOperationRemoveAll
		sessionOperationRemoveAll = func(path string) error {
			if filepath.Base(filepath.Dir(path)) == sessionOperationDirectoryName {
				return errors.New("retain journal")
			}

			return os.RemoveAll(path)
		}
		response, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
		sessionOperationRemoveAll = previous
		t.Cleanup(func() { sessionOperationRemoveAll = previous })
		if err != nil {
			t.Fatalf("journal cleanup blocked success: %v", err)
		}
		if session := agent.activeSession(response.SessionId); session != nil {
			_ = session.Close(t.Context())
		}
	})
}

func TestSharedLoadAndRuntimeResumeEdges(t *testing.T) {
	newLoadAgent := func(t *testing.T, factory func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error)) *Agent {
		t.Helper()
		agent := newTestAgent(
			WithScratchDir(t.TempDir()),
			WithSharedHermesHome(t.TempDir()),
			WithSessionStore(validHydrateStore(t, t.Context())),
		)
		agent.options.clientFactory = factory

		return agent
	}

	t.Run("load success without native archive", func(t *testing.T) {
		client := newFakeHermesClient()
		client.getSession = testNativeSession("n")
		agent := newLoadAgent(t, func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
			client.xdg = start.ExistingXDG

			return client, nil
		})
		response, err := agent.LoadSession(t.Context(), LoadSessionRequest("s", t.TempDir()))
		if err != nil || response.Meta == nil {
			t.Fatalf("shared load response=%+v err=%v", response, err)
		}
		if session := agent.activeSession("s"); session != nil {
			_ = session.Close(t.Context())
		}
	})

	t.Run("load owner conflict", func(t *testing.T) {
		agent := newLoadAgent(t, func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
			t.Fatal("factory called after owner conflict")

			return nil, errors.New("unreachable factory")
		})
		owner, err := nativehermes.AcquireSharedNativeSessionOwner(agent.options.SharedHermesHome, "n")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = owner.Release() }()
		if _, err := agent.LoadSession(t.Context(), LoadSessionRequest("s", t.TempDir())); err == nil {
			t.Fatal("conflicting load owner succeeded")
		}
	})

	t.Run("load startup containment", func(t *testing.T) {
		agent := newLoadAgent(t, func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
			return nil, nativehermes.ErrProcessContainmentIncomplete
		})
		if _, err := agent.LoadSession(t.Context(), LoadSessionRequest("s", t.TempDir())); !errors.Is(err, nativehermes.ErrProcessContainmentIncomplete) {
			t.Fatalf("containment error=%v", err)
		}
	})

	t.Run("load startup ordinary failure", func(t *testing.T) {
		agent := newLoadAgent(t, func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
			return nil, errors.New("start")
		})
		if _, err := agent.LoadSession(t.Context(), LoadSessionRequest("s", t.TempDir())); err == nil {
			t.Fatal("ordinary startup failure ignored")
		}
	})

	t.Run("load active registration", func(t *testing.T) {
		client := newFakeHermesClient()
		client.getSession = testNativeSession("n")
		var agent *Agent
		agent = newLoadAgent(t, func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
			client.xdg = start.ExistingXDG
			agent.mu.Lock()
			for index := 0; index < agent.options.ConcurrencyLimits.MaxActiveSessions; index++ {
				agent.sessions[acp.SessionId("load-capacity-"+string(rune('a'+index)))] = nil
			}
			agent.mu.Unlock()

			return client, nil
		})
		if _, err := agent.LoadSession(t.Context(), LoadSessionRequest("s", t.TempDir())); err == nil {
			t.Fatal("load registration capacity failure ignored")
		}
		agent.mu.Lock()
		clear(agent.sessions)
		agent.mu.Unlock()
	})

	t.Run("runtime resume success", func(t *testing.T) {
		client := newFakeHermesClient()
		client.getSession = testNativeSession("n")
		agent := newLoadAgent(t, func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
			client.xdg = start.ExistingXDG

			return client, nil
		})
		session := newSession(agent, "s", "", nil, nil, testNativeSession("n"), newFakeHermesClient(), sessionMeta{}, validHydrateIDMap())
		session.runtimeNeedsResume = true
		if err := session.resumeRuntimeForTurnLocked(t.Context()); err != nil {
			t.Fatal(err)
		}
		if session.runtimeNeedsResume {
			t.Fatal("runtime remained fenced")
		}
		_ = session.Close(t.Context())
	})

	t.Run("runtime owner conflict", func(t *testing.T) {
		agent := newLoadAgent(t, func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
			t.Fatal("factory called after owner conflict")

			return nil, errors.New("unreachable factory")
		})
		session := newSession(agent, "s", "", nil, nil, testNativeSession("n"), newFakeHermesClient(), sessionMeta{}, validHydrateIDMap())
		session.runtimeNeedsResume = true
		owner, err := nativehermes.AcquireSharedNativeSessionOwner(agent.options.SharedHermesHome, "n")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = owner.Release() }()
		if err := session.resumeRuntimeForTurnLocked(t.Context()); err == nil {
			t.Fatal("conflicting resume owner succeeded")
		}
	})

	for name, startErr := range map[string]error{
		"runtime ordinary start": errors.New("start"),
		"runtime containment":    nativehermes.ErrProcessContainmentIncomplete,
	} {
		t.Run(name, func(t *testing.T) {
			agent := newLoadAgent(t, func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
				return nil, startErr
			})
			session := newSession(agent, "s", "", nil, nil, testNativeSession("n"), newFakeHermesClient(), sessionMeta{}, validHydrateIDMap())
			session.runtimeNeedsResume = true
			if err := session.resumeRuntimeForTurnLocked(t.Context()); err == nil {
				t.Fatalf("%s failure ignored", name)
			}
		})
	}
}

//nolint:gocyclo // Shared fork transaction failures intentionally share one setup matrix.
func TestSharedForkTransactionFailureEdges(t *testing.T) {
	newForkAgent := func(t *testing.T, parentClient *fakeHermesClient, factory func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error)) (*Agent, *session) {
		t.Helper()
		agent := newTestAgent(WithScratchDir(t.TempDir()), WithSharedHermesHome(t.TempDir()), WithSessionStore(NewInMemorySessionStore()))
		agent.options.clientFactory = factory
		parent := testSession(agent, parentClient)
		parent.id = "parent"
		parent.idmap.SessionID = "parent"
		parent.idmap.NativeSessionID = "native-parent"
		agent.sessions[parent.id] = parent

		return agent, parent
	}

	t.Run("inventory capability", func(t *testing.T) {
		base := newFakeHermesClient()
		parentClient := sessionOperationServerOnly{Server: base}
		agent := newTestAgent(WithScratchDir(t.TempDir()), WithSharedHermesHome(t.TempDir()), WithSessionStore(NewInMemorySessionStore()))
		parent := newSession(agent, "parent", t.TempDir(), nil, nil, testNativeSession("native-parent"), parentClient, sessionMeta{}, idmapRecord{SessionID: "parent", NativeSessionID: "native-parent", Format: SessionStoreFormat})
		agent.sessions[parent.id] = parent
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("fork without persisted inventory succeeded")
		}
	})

	t.Run("shared MCP admission", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		agent, parent := newForkAgent(t, parentClient, nil)
		if err := agent.admitSharedHermesConfig([]acp.McpServer{StdioMCPServer("existing", "command", nil, nil)}); err != nil {
			t.Fatal(err)
		}
		request := ForkSessionRequest(parent.id, t.TempDir(), WithSessionMCPServers(StdioMCPServer("different", "command", nil, nil)))
		if _, err := agent.forkSession(t.Context(), request); err == nil {
			t.Fatal("changed shared MCP config accepted")
		}
	})

	t.Run("closed parent", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		agent, parent := newForkAgent(t, parentClient, nil)
		parent.closed = true
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("closed parent forked")
		}
	})

	t.Run("session set lock", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		agent, parent := newForkAgent(t, parentClient, nil)
		lock, err := nativehermes.AcquireSharedSessionSetLock(t.Context(), agent.options.SharedHermesHome, nativehermes.SharedSessionSetLockExclusive)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = lock.Release() }()
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		if _, err := agent.forkSession(ctx, ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("contended fork session-set lock succeeded")
		}
	})

	t.Run("pending recovery", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		agent, parent := newForkAgent(t, parentClient, nil)
		control, err := nativehermes.EnsureSharedHermesAdapterControlDir(agent.options.SharedHermesHome)
		if err != nil {
			t.Fatal(err)
		}
		operations := filepath.Join(control, sessionOperationDirectoryName)
		if err := os.MkdirAll(operations, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(operations, "unexpected"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("fork pending recovery failure ignored")
		}
	})

	t.Run("inventory read", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		parentClient.listErr = errors.New("inventory")
		agent, parent := newForkAgent(t, parentClient, nil)
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("fork inventory failure ignored")
		}
	})

	t.Run("journal begin", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		agent, parent := newForkAgent(t, parentClient, nil)
		previous := sessionOperationMkdir
		sessionOperationMkdir = func(string, os.FileMode) error { return errors.New("journal") }
		t.Cleanup(func() { sessionOperationMkdir = previous })
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("fork journal failure ignored")
		}
	})

	t.Run("operation entropy", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		agent, parent := newForkAgent(t, parentClient, nil)
		previousSessionReader, previousOperationReader := sessionIDRandReader, cryptorand.Reader
		sessionIDRandReader = strings.NewReader(strings.Repeat("x", 16))
		cryptorand.Reader = sessionOperationErrorReader{err: errors.New("operation entropy")}
		t.Cleanup(func() {
			sessionIDRandReader = previousSessionReader
			cryptorand.Reader = previousOperationReader
		})
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("fork operation-id entropy failure ignored")
		}
	})

	t.Run("journal mutating update", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		agent, parent := newForkAgent(t, parentClient, nil)
		previous := sessionOperationNow
		calls := 0
		sessionOperationNow = func() time.Time {
			calls++
			if calls > 1 {
				return time.UnixMilli(1)
			}

			return time.Now()
		}
		t.Cleanup(func() { sessionOperationNow = previous })
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("fork mutating journal update failure ignored")
		}
	})

	t.Run("journal child identification", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		parentClient.forkSession = testNativeSession("native-child")
		agent, parent := newForkAgent(t, parentClient, nil)
		previous := sessionOperationRename
		journalWrites := 0
		sessionOperationRename = func(source, target string) error {
			if filepath.Base(target) == sessionOperationJournalName {
				journalWrites++
				if journalWrites == 3 {
					return errors.New("identify")
				}
			}

			return previous(source, target)
		}
		t.Cleanup(func() { sessionOperationRename = previous })
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("fork child journal identification failure ignored")
		}
	})

	t.Run("branch no delta", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		parentClient.persistedSessions = []nativehermes.Session{{ID: "native-parent"}}
		parentClient.forkErr = errors.New("branch")
		agent, parent := newForkAgent(t, parentClient, nil)
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("branch failure ignored")
		}
	})

	t.Run("child owner conflict", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		parentClient.forkSession = testNativeSession("native-child")
		agent, parent := newForkAgent(t, parentClient, nil)
		owner, err := nativehermes.AcquireSharedNativeSessionOwner(agent.options.SharedHermesHome, "native-child")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = owner.Release() }()
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("conflicting fork child owner succeeded")
		}
	})

	t.Run("child startup", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		parentClient.forkSession = testNativeSession("native-child")
		agent, parent := newForkAgent(t, parentClient, func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
			return nil, errors.New("start child")
		})
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("child startup failure ignored")
		}
	})

	t.Run("child startup containment", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		parentClient.forkSession = testNativeSession("native-child")
		agent, parent := newForkAgent(t, parentClient, func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
			return nil, nativehermes.ErrProcessContainmentIncomplete
		})
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); !errors.Is(err, nativehermes.ErrProcessContainmentIncomplete) {
			t.Fatalf("child containment error=%v", err)
		}
	})

	t.Run("child native drift", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		parentClient.forkSession = testNativeSession("native-child")
		childClient := newFakeHermesClient()
		childClient.getSession = testNativeSession("different")
		agent, parent := newForkAgent(t, parentClient, func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
			childClient.xdg = start.ExistingXDG

			return childClient, nil
		})
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("child native drift accepted")
		}
	})

	t.Run("child drift containment close", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		parentClient.forkSession = testNativeSession("native-child")
		childClient := newFakeHermesClient()
		childClient.getSession = testNativeSession("different")
		childClient.closeErr = nativehermes.ErrProcessContainmentIncomplete
		agent, parent := newForkAgent(t, parentClient, func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
			childClient.xdg = start.ExistingXDG

			return childClient, nil
		})
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("drift containment close accepted")
		}
	})

	t.Run("snapshot cleanup containment", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		parentClient.forkSession = testNativeSession("native-child")
		childClient := newFakeHermesClient()
		childClient.getSession = testNativeSession("native-child")
		childClient.closeErr = nativehermes.ErrProcessContainmentIncomplete
		agent, parent := newForkAgent(t, parentClient, func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
			childClient.xdg = start.ExistingXDG

			return childClient, nil
		})
		agent.options.SessionStore = &toggleReplaceStore{InMemorySessionStore: NewInMemorySessionStore(), fail: true}
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("snapshot cleanup containment ignored")
		}
	})

	t.Run("active registration after commit", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		parentClient.forkSession = testNativeSession("native-child")
		childClient := newFakeHermesClient()
		childClient.getSession = testNativeSession("native-child")
		var agent *Agent
		var parent *session
		agent, parent = newForkAgent(t, parentClient, func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
			childClient.xdg = start.ExistingXDG
			agent.mu.Lock()
			for len(agent.sessions) < agent.options.ConcurrencyLimits.MaxActiveSessions {
				agent.sessions[acp.SessionId("fork-capacity-"+string(rune('a'+len(agent.sessions))))] = nil
			}
			agent.mu.Unlock()

			return childClient, nil
		})
		if _, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir())); err == nil {
			t.Fatal("fork registration capacity failure ignored")
		}
		agent.mu.Lock()
		clear(agent.sessions)
		agent.mu.Unlock()
	})

	t.Run("committed journal cleanup", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		parentClient.forkSession = testNativeSession("native-child")
		childClient := newFakeHermesClient()
		childClient.getSession = testNativeSession("native-child")
		agent, parent := newForkAgent(t, parentClient, func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
			childClient.xdg = start.ExistingXDG

			return childClient, nil
		})
		previous := sessionOperationRemoveAll
		sessionOperationRemoveAll = func(path string) error {
			if filepath.Base(filepath.Dir(path)) == sessionOperationDirectoryName {
				return errors.New("retain journal")
			}

			return os.RemoveAll(path)
		}
		response, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir()))
		sessionOperationRemoveAll = previous
		t.Cleanup(func() { sessionOperationRemoveAll = previous })
		if err != nil {
			t.Fatalf("fork journal cleanup blocked success: %v", err)
		}
		if child := agent.activeSession(response.SessionId); child != nil {
			_ = child.Close(t.Context())
		}
	})
}

func TestDeleteActiveSessionStoreFailureUnlocksLifecycle(t *testing.T) {
	wantErr := errors.New("delete")
	store := sessionOperationFaultStore{base: NewInMemorySessionStore(), deleteErr: wantErr}
	agent := newTestAgent(WithSessionStore(store))
	session := testSession(agent, newFakeHermesClient())
	agent.sessions[session.id] = session
	if _, err := agent.UnstableDeleteSession(t.Context(), acp.UnstableDeleteSessionRequest{SessionId: session.id}); !errors.Is(err, wantErr) {
		t.Fatalf("delete error=%v", err)
	}
	locked := make(chan bool)
	go func() {
		session.lifecycleMu.Lock()
		acquired := true
		session.lifecycleMu.Unlock()
		locked <- acquired
	}()
	select {
	case <-locked:
	case <-time.After(time.Second):
		t.Fatal("delete store failure left lifecycle locked")
	}
}

func TestSharedClientAdmissionAndOwnerBindingEdges(t *testing.T) {
	agent := newTestAgent(WithScratchDir(t.TempDir()), WithSharedHermesHome(t.TempDir()))
	if err := agent.admitSharedHermesConfig(nil); err != nil {
		t.Fatal(err)
	}
	badServers := []acp.McpServer{StdioMCPServer("different", "command", nil, nil)}
	if _, err := agent.newHermesClient(t.Context(), "session", t.TempDir(), sessionMeta{}, nativehermes.XDGDirs{}, badServers); err == nil {
		t.Fatal("changed shared MCP config accepted during client start")
	}

	owner, err := nativehermes.AcquireSharedNativeSessionOwner(agent.options.SharedHermesHome, "native")
	if err != nil {
		t.Fatal(err)
	}
	agent.options.clientFactory = func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
		client := newFakeHermesClient()
		client.xdg = start.ExistingXDG

		return sessionOperationServerOnly{Server: client}, nil
	}
	parent, err := ensureScratchParent(agent.options.ScratchDir)
	if err != nil {
		t.Fatal(err)
	}
	xdg, err := createHermesGeneration(parent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agent.newHermesClientWithScratchOwner(t.Context(), "session", t.TempDir(), sessionMeta{}, xdg, func() {}, owner); err == nil {
		t.Fatal("server without owner process identity accepted")
	}
}

// TestAdmitSharedHermesConfigSurfacesRedactionFailure pins the seam that strips
// per-process MCP secrets before they can reach the shared operator config: a
// redaction that fails must abort admission rather than admit the raw servers.
func TestAdmitSharedHermesConfigSurfacesRedactionFailure(t *testing.T) {
	wantErr := errors.New("redaction")
	previous := redactSharedHermesMCPServers
	redactSharedHermesMCPServers = func([]acp.McpServer) ([]acp.McpServer, error) { return nil, wantErr }
	t.Cleanup(func() { redactSharedHermesMCPServers = previous })

	agent := newTestAgent(WithSharedHermesHome(t.TempDir()))
	if err := agent.admitSharedHermesConfig(nil); !errors.Is(err, wantErr) {
		t.Fatalf("redaction error = %v", err)
	}
}

type failNthIDReader struct {
	reads  int
	failAt int
}

func (r *failNthIDReader) Read(buffer []byte) (int, error) {
	r.reads++
	if r.reads == r.failAt {
		return 0, errors.New("lifecycle stream id failed")
	}

	for index := range buffer {
		buffer[index] = byte(r.reads + index)
	}

	return len(buffer), nil
}

func installFailingLifecycleIDReader(t *testing.T, failAt int) {
	t.Helper()

	original := sessionIDRandReader
	t.Cleanup(func() { sessionIDRandReader = original })
	sessionIDRandReader = &failNthIDReader{failAt: failAt}
}

func TestSessionConstructionCleansUpWhenLifecycleStreamIDFails(t *testing.T) {
	negotiated := lifecycle.Negotiated{
		Versions: []int{lifecycle.Version}, ActivityKinds: []lifecycle.ActivityKind{},
	}

	t.Run("new", func(t *testing.T) {
		client := newFakeHermesClient()
		client.createSession = testNativeSession("native-new")
		agent := newTestAgent(WithScratchDir(t.TempDir()), func(options *Options) {
			options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
				xdg, err := testGenerationXDG(opts.ScratchParent)
				if err != nil {
					return nil, err
				}
				client.xdg = xdg

				return client, nil
			}
		})
		agent.retainNegotiatedLifecycle(negotiated)
		installFailingLifecycleIDReader(t, 2)

		_, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
		require.ErrorContains(t, err, "lifecycle stream id failed")
		require.True(t, client.closed)
	})

	t.Run("load", func(t *testing.T) {
		store := NewInMemorySessionStore()
		client := newFakeHermesClient()
		client.getSession = testNativeSession("native-1")
		agent := newTestAgent(WithScratchDir(t.TempDir()), WithSessionStore(store), func(options *Options) {
			options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
				client.xdg = opts.ExistingXDG

				return client, nil
			}
		})
		seed := testSession(agent, newFakeHermesClient())
		require.NoError(t, seed.snapshotToStore(t.Context()))
		agent.retainNegotiatedLifecycle(negotiated)
		installFailingLifecycleIDReader(t, 1)

		_, err := agent.loadOrResumeSession(t.Context(), seed.id, seed.cwd, nil, nil, nil)
		require.ErrorContains(t, err, "lifecycle stream id failed")
		require.True(t, client.closed)
	})

	t.Run("fork with incomplete cleanup", func(t *testing.T) {
		parentClient := newFakeHermesClient()
		parentClient.forkSession = testNativeSession("native-child")
		childClient := newFakeHermesClient()
		childClient.getSession = testNativeSession("native-child")
		childClient.closeErr = nativehermes.ErrProcessContainmentIncomplete
		agent := newTestAgent(WithScratchDir(t.TempDir()), func(options *Options) {
			options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
				childClient.xdg = opts.ExistingXDG

				return childClient, nil
			}
		})
		parent := testSession(agent, parentClient)
		agent.sessions[parent.id] = parent
		agent.retainNegotiatedLifecycle(negotiated)
		installFailingLifecycleIDReader(t, 2)

		_, err := agent.forkSession(t.Context(), acp.UnstableForkSessionRequest{
			SessionId: parent.id, Cwd: t.TempDir(),
		})
		require.ErrorContains(t, err, "lifecycle stream id failed")
		require.ErrorIs(t, err, nativehermes.ErrProcessContainmentIncomplete)
	})
}
