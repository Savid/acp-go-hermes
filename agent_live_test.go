//go:build integration

package hermesacp

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLiveAgentStoreRestore(t *testing.T) {
	if os.Getenv("ACP_GO_HERMES_RUN_INTEGRATION") == "" || os.Getenv("ACP_GO_HERMES_RUN_LIVE_TOKENS") == "" {
		t.Skip("set ACP_GO_HERMES_RUN_INTEGRATION=1 and ACP_GO_HERMES_RUN_LIVE_TOKENS=1")
	}
	t.Setenv("ACP_GO_HERMES_FORCE_GATEWAY", "1")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	store := NewInMemorySessionStore()
	home := t.TempDir()
	agent := NewAgent(WithHome(home), WithSessionStore(store))
	cwd := t.TempDir()
	newResp, err := agent.NewSession(ctx, NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if _, err := agent.Prompt(ctx, TextPromptRequest(newResp.SessionId, "Reply with exactly ACP_HERMES_STORE_ONE.")); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if entries, err := store.Load(ctx, SessionKey{SessionID: string(newResp.SessionId), Subpath: stateDBSubpath}); err != nil || len(entries) == 0 {
		t.Fatalf("state-db snapshot entries=%d err=%v", len(entries), err)
	}
	if err := agent.Close(); err != nil {
		t.Fatalf("Close first agent: %v", err)
	}
	if err := os.RemoveAll(home); err != nil {
		t.Fatalf("remove native home root: %v", err)
	}

	restoreHome := t.TempDir()
	restored := NewAgent(WithHome(restoreHome), WithSessionStore(store))
	if _, err := restored.LoadSession(ctx, LoadSessionRequest(newResp.SessionId, cwd)); err != nil {
		t.Fatalf("LoadSession after native delete: %v", err)
	}
	if _, err := restored.Prompt(ctx, TextPromptRequest(newResp.SessionId, "Reply with exactly ACP_HERMES_STORE_TWO.")); err != nil {
		t.Fatalf("Prompt after restore: %v", err)
	}
	if err := restored.Close(); err != nil {
		t.Fatalf("Close restored agent: %v", err)
	}
	if matches, _ := filepath.Glob(filepath.Join(restoreHome, "*", "state.db")); len(matches) == 0 {
		t.Fatalf("restored home did not contain state.db under %s", restoreHome)
	}
}

func TestLiveAgentForkStoreRestore(t *testing.T) {
	if os.Getenv("ACP_GO_HERMES_RUN_INTEGRATION") == "" || os.Getenv("ACP_GO_HERMES_RUN_LIVE_TOKENS") == "" {
		t.Skip("set ACP_GO_HERMES_RUN_INTEGRATION=1 and ACP_GO_HERMES_RUN_LIVE_TOKENS=1")
	}
	t.Setenv("ACP_GO_HERMES_FORCE_GATEWAY", "1")
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	store := NewInMemorySessionStore()
	home := t.TempDir()
	agent := NewAgent(WithHome(home), WithSessionStore(store))
	cwd := t.TempDir()
	parent, err := agent.NewSession(ctx, NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if _, err := agent.Prompt(ctx, TextPromptRequest(parent.SessionId, "Reply with exactly ACP_HERMES_FORK_PARENT.")); err != nil {
		t.Fatalf("Prompt parent: %v", err)
	}
	fork, err := agent.forkSession(ctx, ForkSessionRequest(parent.SessionId, cwd))
	if err != nil {
		t.Fatalf("forkSession: %v", err)
	}
	if fork.SessionId == "" || fork.SessionId == parent.SessionId {
		t.Fatalf("fork response = %#v", fork)
	}
	idEntries, err := store.Load(ctx, SessionKey{SessionID: string(fork.SessionId), Subpath: idmapSubpath})
	if err != nil || len(idEntries) == 0 {
		t.Fatalf("fork idmap entries=%d err=%v", len(idEntries), err)
	}
	var idmap idmapRecord
	if err := json.Unmarshal(idEntries[len(idEntries)-1], &idmap); err != nil {
		t.Fatalf("unmarshal fork idmap: %v", err)
	}
	if idmap.ParentSessionID != string(parent.SessionId) || idmap.NativeParentSessionID == "" {
		t.Fatalf("fork idmap lineage = %#v", idmap)
	}
	if entries, err := store.Load(ctx, SessionKey{SessionID: string(fork.SessionId), Subpath: stateDBSubpath}); err != nil || len(entries) == 0 {
		t.Fatalf("fork state-db snapshot entries=%d err=%v", len(entries), err)
	}
	if _, err := agent.Prompt(ctx, TextPromptRequest(fork.SessionId, "Reply with exactly ACP_HERMES_FORK_CHILD.")); err != nil {
		t.Fatalf("Prompt child: %v", err)
	}
	if err := agent.Close(); err != nil {
		t.Fatalf("Close first agent: %v", err)
	}
	if err := os.RemoveAll(home); err != nil {
		t.Fatalf("remove native home root: %v", err)
	}

	restoreHome := t.TempDir()
	restored := NewAgent(WithHome(restoreHome), WithSessionStore(store))
	if _, err := restored.LoadSession(ctx, LoadSessionRequest(fork.SessionId, cwd)); err != nil {
		t.Fatalf("LoadSession fork after native delete: %v", err)
	}
	if _, err := restored.Prompt(ctx, TextPromptRequest(fork.SessionId, "Reply with exactly ACP_HERMES_FORK_RESTORED.")); err != nil {
		t.Fatalf("Prompt fork after restore: %v", err)
	}
	if err := restored.Close(); err != nil {
		t.Fatalf("Close restored agent: %v", err)
	}
	if matches, _ := filepath.Glob(filepath.Join(restoreHome, "*", "state.db")); len(matches) == 0 {
		t.Fatalf("restored fork home did not contain state.db under %s", restoreHome)
	}
}
