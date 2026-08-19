//go:build integration

package integration

import (
	"bufio"
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

// shippedResumeFixture is the transcript examples/resume-from-file ships and
// tells its readers to run as-is.
const shippedResumeFixture = "../examples/resume-from-file/session.jsonl"

// TestLiveShippedResumeFixtureLoads runs the promise the resume-from-file
// example makes: the shipped transcript loads through ACP on a machine that has
// never seen the session. Nothing here spends model tokens — the whole rung is
// hydrate, native-archive restore, and the gateway session.resume behind
// session/load. A fixture whose snapshot names no state-db archive reaches a
// fresh Hermes home that never heard of the native session, and the gateway
// answers 4007 instead, so this is the test that keeps the fixture honest.
func TestLiveShippedResumeFixtureLoads(t *testing.T) {
	requireRunIntegration(t)

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	sessionID, replacements := readShippedResumeFixture(t)

	store := hermesacp.NewInMemorySessionStore()
	if err := store.Replace(ctx, hermesacp.SessionKey{SessionID: sessionID}, replacements); err != nil {
		t.Fatalf("store.Replace: %v", err)
	}

	home := t.TempDir()
	agent := hermesacp.NewAgent(
		hermesacp.WithExecutablePath(integrationHermesPath(t)),
		hermesacp.WithScratchDir(home),
		hermesacp.WithSessionStore(store),
		integrationContainmentOption(),
	)
	defer func() {
		if err := agent.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	if _, err := agent.LoadSession(ctx, hermesacp.LoadSessionRequest(acp.SessionId(sessionID), t.TempDir())); err != nil {
		t.Fatalf("LoadSession with the shipped fixture: %v", err)
	}

	requireShippedFixtureStateDB(t, home)
}

// requireShippedFixtureStateDB proves the load restored the fixture's own
// Hermes database rather than starting an empty one. The generation root is
// named per incarnation, so it is located under the scratch parent; its
// containment control directory is a sibling, not a generation.
func requireShippedFixtureStateDB(t *testing.T, scratch string) {
	t.Helper()

	roots, err := filepath.Glob(filepath.Join(scratch, "acp-go-hermes-runtime-*"))
	if err != nil {
		t.Fatalf("locate restored Hermes runtime root: %v", err)
	}

	generations := make([]string, 0, len(roots))
	for _, root := range roots {
		if !strings.HasSuffix(root, ".control") {
			generations = append(generations, root)
		}
	}

	if len(generations) != 1 {
		t.Fatalf("restored Hermes generation roots = %v, want exactly one", generations)
	}

	path := filepath.Join(generations[0], "state.db")

	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read restored Hermes state DB %s: %v", path, err)
	}

	const sqliteHeader = "SQLite format 3\x00"
	if len(contents) < len(sqliteHeader) || string(contents[:len(sqliteHeader)]) != sqliteHeader {
		t.Fatalf("restored Hermes state DB %s is not SQLite (size=%d)", path, len(contents))
	}
}

// readShippedResumeFixture routes the shipped transcript's rows to the store
// keys session/load reads independently, exactly as the example's own reader
// does: the main snapshot, the archive chunks, and the id mapping.
func readShippedResumeFixture(t *testing.T) (string, []hermesacp.SessionStoreReplacement) {
	t.Helper()

	file, err := os.Open(filepath.FromSlash(shippedResumeFixture))
	if err != nil {
		t.Fatalf("open shipped fixture: %v", err)
	}
	defer file.Close()

	var (
		sessionID string
		main      []hermesacp.SessionStoreEntry
		archive   []hermesacp.SessionStoreEntry
		idmap     []hermesacp.SessionStoreEntry
	)

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		entry := hermesacp.SessionStoreEntry(line)

		var row struct {
			Data    string `json:"data"`
			Session *struct {
				SessionID string `json:"sessionId"`
				Cwd       string `json:"cwd"`
			} `json:"session"`
		}
		if err := json.Unmarshal(entry, &row); err != nil {
			t.Fatalf("decode shipped fixture row: %v", err)
		}

		switch {
		case row.Session != nil:
			if row.Session.Cwd != "" {
				t.Fatalf("shipped fixture binds cwd %q, which cannot exist on every machine", row.Session.Cwd)
			}

			sessionID = row.Session.SessionID
			main = append(main, entry)
		case row.Data != "":
			archive = append(archive, entry)
		default:
			idmap = append(idmap, entry)
		}
	}

	if err := scanner.Err(); err != nil {
		t.Fatalf("read shipped fixture: %v", err)
	}

	if sessionID == "" || len(main) != 1 || len(idmap) != 1 || len(archive) == 0 {
		t.Fatalf("shipped fixture rows: session=%q main=%d idmap=%d archive=%d", sessionID, len(main), len(idmap), len(archive))
	}

	return sessionID, []hermesacp.SessionStoreReplacement{
		{Key: hermesacp.SessionKey{SessionID: sessionID}, Entries: main},
		{Key: hermesacp.SessionKey{SessionID: sessionID, Subpath: stateDBSubpath}, Entries: archive},
		{Key: hermesacp.SessionKey{SessionID: sessionID, Subpath: idmapSubpath}, Entries: idmap},
	}
}
