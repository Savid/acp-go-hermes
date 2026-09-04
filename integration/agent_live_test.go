//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	hermesacp "github.com/savid/acp-go-hermes"
)

const (
	stateDBSubpath = "state-db"
	idmapSubpath   = "idmap"
)

// idmapLineage mirrors the lineage fields of the hermes-state-db-v1 idmap row.
type idmapLineage struct {
	ParentSessionID       string `json:"parentSessionId"`
	NativeParentSessionID string `json:"nativeParentSessionId"`
}

func TestLiveAgentStoreRestore(t *testing.T) {
	requireRunLiveTokens(t)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	store := hermesacp.NewInMemorySessionStore()
	home := t.TempDir()
	agent := startInProcessAgent(t, ctx,
		hermesacp.WithScratchDir(home),
		hermesacp.WithSessionStore(store),
		hermesacp.WithSeedFiles(liveTokenSeedFiles()),
		hermesacp.WithEnv(liveTokenEnv(nil)),
	)
	cwd := t.TempDir()
	newResp, err := agent.conn.NewSession(ctx, hermesacp.NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if _, err := agent.conn.Prompt(ctx, hermesacp.TextPromptRequest(newResp.SessionId, "turn-store-one", "Reply with exactly ACP_HERMES_STORE_ONE.")); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if entries, err := store.Load(ctx, hermesacp.SessionKey{SessionID: string(newResp.SessionId), Subpath: stateDBSubpath}); err != nil || len(entries) == 0 {
		t.Fatalf("state-db snapshot entries=%d err=%v", len(entries), err)
	}
	if err := agent.stop(); err != nil {
		t.Fatalf("Close first agent: %v", err)
	}
	if err := os.RemoveAll(home); err != nil {
		t.Fatalf("remove native home root: %v", err)
	}

	restoreHome := t.TempDir()
	restored := startInProcessAgent(t, ctx,
		hermesacp.WithScratchDir(restoreHome),
		hermesacp.WithSessionStore(store),
		hermesacp.WithSeedFiles(liveTokenSeedFiles()),
		hermesacp.WithEnv(liveTokenEnv(nil)),
	)
	if _, err := restored.conn.LoadSession(ctx, hermesacp.LoadSessionRequest(newResp.SessionId, cwd)); err != nil {
		t.Fatalf("LoadSession after native delete: %v", err)
	}
	restoredRoot := requireRestoredHermesStateDB(t, restoreHome, newResp.SessionId)
	if _, err := restored.conn.Prompt(ctx, hermesacp.TextPromptRequest(newResp.SessionId, "turn-store-two", "Reply with exactly ACP_HERMES_STORE_TWO.")); err != nil {
		t.Fatalf("Prompt after restore: %v", err)
	}
	if err := restored.stop(); err != nil {
		t.Fatalf("Close restored agent: %v", err)
	}
	requireRemovedHermesRoot(t, restoredRoot)
}

func TestLiveAgentForkStoreRestore(t *testing.T) {
	requireRunLiveTokens(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	store := hermesacp.NewInMemorySessionStore()
	home := t.TempDir()
	agent := startInProcessAgent(t, ctx,
		hermesacp.WithScratchDir(home),
		hermesacp.WithSessionStore(store),
		hermesacp.WithSeedFiles(liveTokenSeedFiles()),
		hermesacp.WithEnv(liveTokenEnv(nil)),
	)
	cwd := t.TempDir()
	parent, err := agent.conn.NewSession(ctx, hermesacp.NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if _, err := agent.conn.Prompt(ctx, hermesacp.TextPromptRequest(parent.SessionId, "turn-fork-parent", "Reply with exactly ACP_HERMES_FORK_PARENT.")); err != nil {
		t.Fatalf("Prompt parent: %v", err)
	}
	fork, err := hermesacp.CallForkSession(ctx, agent.conn, hermesacp.ForkSessionRequest(parent.SessionId, cwd))
	if err != nil {
		t.Fatalf("fork extension: %v", err)
	}
	if fork.SessionId == "" || fork.SessionId == parent.SessionId {
		t.Fatalf("fork response = %#v", fork)
	}
	idEntries, err := store.Load(ctx, hermesacp.SessionKey{SessionID: string(fork.SessionId), Subpath: idmapSubpath})
	if err != nil || len(idEntries) == 0 {
		t.Fatalf("fork idmap entries=%d err=%v", len(idEntries), err)
	}
	var idmap idmapLineage
	if err := json.Unmarshal(idEntries[len(idEntries)-1], &idmap); err != nil {
		t.Fatalf("unmarshal fork idmap: %v", err)
	}
	if idmap.ParentSessionID != string(parent.SessionId) || idmap.NativeParentSessionID == "" {
		t.Fatalf("fork idmap lineage = %#v", idmap)
	}
	if entries, err := store.Load(ctx, hermesacp.SessionKey{SessionID: string(fork.SessionId), Subpath: stateDBSubpath}); err != nil || len(entries) == 0 {
		t.Fatalf("fork state-db snapshot entries=%d err=%v", len(entries), err)
	}
	if _, err := agent.conn.Prompt(ctx, hermesacp.TextPromptRequest(fork.SessionId, "turn-fork-child", "Reply with exactly ACP_HERMES_FORK_CHILD.")); err != nil {
		t.Fatalf("Prompt child: %v", err)
	}
	if err := agent.stop(); err != nil {
		t.Fatalf("Close first agent: %v", err)
	}
	if err := os.RemoveAll(home); err != nil {
		t.Fatalf("remove native home root: %v", err)
	}

	restoreHome := t.TempDir()
	restored := startInProcessAgent(t, ctx,
		hermesacp.WithScratchDir(restoreHome),
		hermesacp.WithSessionStore(store),
		hermesacp.WithSeedFiles(liveTokenSeedFiles()),
		hermesacp.WithEnv(liveTokenEnv(nil)),
	)
	if _, err := restored.conn.LoadSession(ctx, hermesacp.LoadSessionRequest(fork.SessionId, cwd)); err != nil {
		t.Fatalf("LoadSession fork after native delete: %v", err)
	}
	restoredRoot := requireRestoredHermesStateDB(t, restoreHome, fork.SessionId)
	if _, err := restored.conn.Prompt(ctx, hermesacp.TextPromptRequest(fork.SessionId, "turn-fork-restored", "Reply with exactly ACP_HERMES_FORK_RESTORED.")); err != nil {
		t.Fatalf("Prompt fork after restore: %v", err)
	}
	if err := restored.stop(); err != nil {
		t.Fatalf("Close restored agent: %v", err)
	}
	requireRemovedHermesRoot(t, restoredRoot)
}

// requireRestoredHermesStateDB finds the one runtime generation the restore
// minted. A generation root is named per incarnation, not per session, so it is
// located under the scratch parent rather than derived from the session id.
func requireRestoredHermesStateDB(t *testing.T, scratch string, sessionID acp.SessionId) string {
	t.Helper()
	roots, globErr := filepath.Glob(filepath.Join(scratch, "acp-go-hermes-runtime-*"))
	if globErr != nil {
		t.Fatalf("locate restored Hermes runtime root for %q: %v", sessionID, globErr)
	}

	// A generation root's containment control directory is a sibling of the
	// generation rather than one of its own, so the glob matches both.
	generations := make([]string, 0, len(roots))
	for _, candidate := range roots {
		if !strings.HasSuffix(candidate, ".control") {
			generations = append(generations, candidate)
		}
	}

	if len(generations) != 1 {
		t.Fatalf("restored Hermes generation roots for %q = %v, want exactly one", sessionID, generations)
	}
	root := generations[0]
	path := filepath.Join(root, "state.db")
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read restored Hermes state DB %s: %v", path, err)
	}
	const sqliteHeader = "SQLite format 3\x00"
	if len(contents) < len(sqliteHeader) || string(contents[:len(sqliteHeader)]) != sqliteHeader {
		t.Fatalf("restored Hermes state DB %s is not SQLite (size=%d)", path, len(contents))
	}

	return root
}

func requireRemovedHermesRoot(t *testing.T, root string) {
	t.Helper()
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("Hermes runtime root %s remained after Close: %v", root, err)
	}
}
