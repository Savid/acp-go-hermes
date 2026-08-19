//nolint:gocyclo,govet // Snapshot fault matrices intentionally share setup and scoped errors.
package hermesacp

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
	"github.com/savid/acp-go-hermes/internal/lifecycle"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func TestSnapshotHydrateScrubsSQLiteCredentialTables(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	xdg, err := nativehermes.CreateXDGDirs(root, "session-1")
	if err != nil {
		t.Fatalf("nativehermes.CreateXDGDirs: %v", err)
	}
	dbPath := filepath.Join(xdg.Root, "state.db")
	seedSQLiteStore(t, dbPath)

	store := NewInMemorySessionStore()
	client := newFakeHermesClient()
	client.xdg = xdg
	client.todos = []nativehermes.Todo{{ID: "todo-1", Content: "Remember", Status: "pending", Priority: "medium"}}
	agent := newTestAgent(WithSessionStore(store))
	session := testSession(agent, client)
	if err2 := session.snapshotToStore(ctx); err2 != nil {
		t.Fatalf("snapshotToStore: %v", err2)
	}

	if countSQLiteRows(t, dbPath, "account") != 1 || countSQLiteRows(t, dbPath, "credential") != 1 {
		t.Fatal("snapshot modified live credential tables")
	}

	if err3 := os.RemoveAll(xdg.Root); err3 != nil {
		t.Fatalf("remove original xdg: %v", err3)
	}
	restored, err := nativehermes.CreateXDGDirs(root, "session-1-restored")
	if err != nil {
		t.Fatalf("create restored xdg: %v", err)
	}
	idmap, snapshot, ok, err := hydrateStateFromStore(ctx, store, "session-1", restored)
	if err != nil {
		t.Fatalf("hydrateStateFromStore: %v", err)
	}
	if !ok || idmap.NativeSessionID != "native-1" || snapshot.Format != SessionStoreFormat {
		t.Fatalf("hydrate result idmap=%#v snapshot=%#v ok=%v", idmap, snapshot, ok)
	}
	restoredDB := filepath.Join(restored.Root, "state.db")
	if countSQLiteRows(t, restoredDB, "account") != 0 {
		t.Fatal("account credentials round-tripped through store")
	}
	if countSQLiteRows(t, restoredDB, "credential") != 0 {
		t.Fatal("credential table rows round-tripped through store")
	}
	if countSQLiteRows(t, restoredDB, "message") != 1 {
		t.Fatal("non-credential table did not round-trip")
	}
}

// TestSharedHomeHydrateSkipsAndNextSnapshotPurgesPerSessionNativeArchive pins
// what a shared-home session does with a native state archive written by an
// isolated per-session home: it hydrates the metadata without restoring that
// archive into the shared residence, and its own next snapshot removes the
// archive from the store rather than carrying it forward.
func TestSharedHomeHydrateSkipsAndNextSnapshotPurgesPerSessionNativeArchive(t *testing.T) {
	ctx := t.Context()
	store := NewInMemorySessionStore()
	isolatedXDG, err := nativehermes.CreateXDGDirs(t.TempDir(), "isolated")
	if err != nil {
		t.Fatal(err)
	}
	seedSQLiteStore(t, filepath.Join(isolatedXDG.Root, "state.db"))
	isolatedClient := newFakeHermesClient()
	isolatedClient.xdg = isolatedXDG
	isolatedSession := testSession(newTestAgent(WithSessionStore(store)), isolatedClient)
	if err := isolatedSession.snapshotToStore(ctx); err != nil {
		t.Fatalf("write per-session-home snapshot: %v", err)
	}
	isolatedEntries, err := store.Load(ctx, SessionKey{SessionID: "session-1", Subpath: stateDBSubpath})
	if err != nil || len(isolatedEntries) == 0 {
		t.Fatalf("per-session-home archive entries = %d, err=%v", len(isolatedEntries), err)
	}

	wrapperXDG, err := nativehermes.CreateXDGDirs(t.TempDir(), "shared-wrapper")
	if err != nil {
		t.Fatal(err)
	}
	_, snapshot, ok, err := hydrateStateFromStoreWithoutNativeArchive(ctx, store, "session-1", wrapperXDG)
	if err != nil || !ok || snapshot.Session.NativeSessionID != "native-1" {
		t.Fatalf("metadata-only hydrate snapshot=%#v ok=%t err=%v", snapshot, ok, err)
	}
	if _, err := os.Stat(filepath.Join(wrapperXDG.Root, "state.db")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("per-session-home native archive restored into shared wrapper: %v", err)
	}

	sharedHome := t.TempDir()
	authSentinel := []byte("shared-auth-secret-sentinel")
	stateSentinel := []byte("shared-state-secret-sentinel")
	if err := os.WriteFile(filepath.Join(sharedHome, "auth.json"), authSentinel, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sharedHome, "state.db"), stateSentinel, 0o600); err != nil {
		t.Fatal(err)
	}
	sharedClient := newFakeHermesClient()
	sharedClient.xdg = wrapperXDG
	sharedAgent := newTestAgent(WithSessionStore(store), WithSharedHermesHome(sharedHome))
	sharedSession := testSession(sharedAgent, sharedClient)
	if err := sharedSession.snapshotToStore(ctx); err != nil {
		t.Fatalf("write shared snapshot: %v", err)
	}
	entries, err := store.Load(ctx, SessionKey{SessionID: "session-1", Subpath: stateDBSubpath})
	if err != nil || len(entries) != 0 {
		t.Fatalf("per-session-home state-db archive was not purged: entries=%d err=%v", len(entries), err)
	}
	mainEntries, err := store.Load(ctx, SessionKey{SessionID: "session-1", Subpath: SessionStoreMainSubpath})
	if err != nil || len(mainEntries) == 0 {
		t.Fatalf("shared main snapshot missing: %v", err)
	}
	var committed stateSnapshot
	if err := json.Unmarshal(mainEntries[len(mainEntries)-1], &committed); err != nil {
		t.Fatal(err)
	}
	if len(committed.Archives) != 0 {
		t.Fatalf("shared snapshot retained native archives: %#v", committed.Archives)
	}
	subkeys, err := store.ListSubkeys(ctx, SessionKey{SessionID: "session-1"})
	if err != nil {
		t.Fatal(err)
	}
	for _, subkey := range subkeys {
		stored, loadErr := store.Load(ctx, SessionKey{SessionID: "session-1", Subpath: subkey})
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		for _, entry := range stored {
			if bytes.Contains(entry, authSentinel) || bytes.Contains(entry, stateSentinel) {
				t.Fatalf("shared native secret reached store subkey %q", subkey)
			}
		}
	}
	emptyWrapper, err := nativehermes.CreateXDGDirs(t.TempDir(), "shared-empty-wrapper")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := hydrateStateFromStoreWithoutNativeArchive(ctx, store, "session-1", emptyWrapper); err != nil || !ok {
		t.Fatalf("metadata-only rehydrate ok=%t err=%v", ok, err)
	}
	for _, name := range []string{"auth.json", "state.db"} {
		if _, err := os.Stat(filepath.Join(emptyWrapper.Root, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("hydrate authored %s in wrapper: %v", name, err)
		}
	}
	for name, want := range map[string][]byte{"auth.json": authSentinel, "state.db": stateSentinel} {
		got, err := os.ReadFile(filepath.Join(sharedHome, name))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("hydrate changed shared %s: %q err=%v", name, got, err)
		}
	}
}

func TestHydrateStateDBArchiveRejectsTraversalAndBadChecksum(t *testing.T) {
	ctx := context.Background()
	store := NewInMemorySessionStore()
	main := SessionKey{SessionID: "s", Subpath: SessionStoreMainSubpath}
	idmapData, _ := json.Marshal(idmapRecord{SessionID: "s", NativeSessionID: "n", Format: SessionStoreFormat})
	snapshot := validHydrateSnapshot()
	snapshot.Archives = map[string]archiveInfo{"state-db": {Subpath: stateDBSubpath, SHA256: "bad"}}
	mainData, _ := json.Marshal(snapshot)
	badArchive, _ := json.Marshal(archiveEntry{Format: SessionStoreFormat, Encoding: "tar+zstd+base64", SHA256: "bad", Data: base64.StdEncoding.EncodeToString([]byte("not zstd"))})
	if err := store.Replace(ctx, main, []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{mainData}},
		{Key: SessionKey{SessionID: "s", Subpath: idmapSubpath}, Entries: []SessionStoreEntry{idmapData}},
		{Key: SessionKey{SessionID: "s", Subpath: stateDBSubpath}, Entries: []SessionStoreEntry{badArchive}},
	}); err != nil {
		t.Fatalf("Replace: %v", err)
	}
	_, _, ok, err := hydrateStateFromStore(ctx, store, "s", nativehermes.XDGDirs{
		Root:   filepath.Join(t.TempDir(), "root"),
		Data:   filepath.Join(t.TempDir(), "data"),
		Config: filepath.Join(t.TempDir(), "config"),
		Cache:  filepath.Join(t.TempDir(), "cache"),
		State:  filepath.Join(t.TempDir(), "state"),
	})
	if err == nil || ok {
		t.Fatal("bad archive checksum accepted")
	}

	if !sensitiveSQLiteName("access_token") || quoteSQLiteIdent(`a"b`) != `"a""b"` || quoteSQLiteString(`a'b`) != `'a''b'` {
		t.Fatal("SQLite helper checks failed")
	}
}

func TestDecodeArchiveRoundTripAndHelpers(t *testing.T) {
	archive := testTarZstd(t, []tar.Header{{
		Name:     "nested/file.txt",
		Typeflag: tar.TypeReg,
		Mode:     0o600,
		Size:     int64(len("body")),
	}}, map[string]string{"nested/file.txt": "body"})
	target := t.TempDir()
	if err := decodeXDGArchive(archive, target); err != nil {
		t.Fatalf("decodeXDGArchive: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(target, "nested", "file.txt"))
	if err != nil || string(data) != "body" {
		t.Fatalf("decoded file = %q err=%v", data, err)
	}

	if err := decodeXDGArchive([]byte("not zstd"), t.TempDir()); err == nil {
		t.Fatal("decode accepted invalid zstd")
	}
	if err := decodeXDGArchive(testTarZstd(t, []tar.Header{{Name: "../escape", Typeflag: tar.TypeReg, Size: 0}}, nil), t.TempDir()); err == nil {
		t.Fatal("decode accepted traversal")
	}
	if err := decodeXDGArchive(testTarZstd(t, []tar.Header{{Name: "/abs", Typeflag: tar.TypeReg, Size: 0}}, nil), t.TempDir()); err == nil {
		t.Fatal("decode accepted absolute path")
	}
}

func TestArchiveEntriesChunkAndReassembleDeterministically(t *testing.T) {
	data := make([]byte, archiveChunkBytes*2+17)
	for index := range data {
		data[index] = byte(index % 251)
	}
	sum := sha256.Sum256(data)
	sha := hex.EncodeToString(sum[:])

	entries, err := encodeArchiveEntries(data, sha)
	if err != nil {
		t.Fatalf("encodeArchiveEntries: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("archive chunks = %d, want 3", len(entries))
	}
	for index, raw := range entries {
		var entry archiveEntry
		if decodeErr := json.Unmarshal(raw, &entry); decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if entry.Sequence != index || entry.Final != (index == len(entries)-1) || entry.SHA256 != sha {
			t.Fatalf("chunk %d metadata = %#v", index, entry)
		}
	}

	decoded, err := decodeArchiveEntries(entries, archiveInfo{SHA256: sha, Bytes: len(data)})
	if err != nil || !bytes.Equal(decoded, data) {
		t.Fatalf("decodeArchiveEntries bytes=%d err=%v", len(decoded), err)
	}

	if _, err := decodeArchiveEntries([]SessionStoreEntry{entries[1], entries[0], entries[2]}, archiveInfo{SHA256: sha, Bytes: len(data)}); err == nil {
		t.Fatal("decodeArchiveEntries accepted out-of-order chunks")
	}
	if _, err := decodeArchiveEntries(entries[:2], archiveInfo{SHA256: sha, Bytes: len(data)}); err == nil {
		t.Fatal("decodeArchiveEntries accepted a missing final chunk")
	}

	assertArchiveEntryDecodeFailures(t)
}

func assertArchiveEntryDecodeFailures(t *testing.T) {
	t.Helper()

	data := []byte("archive")
	sum := sha256.Sum256(data)
	sha := hex.EncodeToString(sum[:])
	validEntry := mustStateJSON(t, archiveEntry{
		Format: SessionStoreFormat, Encoding: archiveEncodingTarZstdBase64, Final: true, SHA256: sha,
		Data: base64.StdEncoding.EncodeToString(data),
	})
	wrongSHA := strings.Repeat("0", sha256.Size*2)
	wrongChecksumEntry := mustStateJSON(t, archiveEntry{
		Format: SessionStoreFormat, Encoding: archiveEncodingTarZstdBase64, Final: true, SHA256: wrongSHA,
		Data: base64.StdEncoding.EncodeToString(data),
	})

	for name, test := range map[string]struct {
		entries []SessionStoreEntry
		info    archiveInfo
	}{
		"json": {
			entries: []SessionStoreEntry{json.RawMessage(`{`)},
			info:    archiveInfo{SHA256: sha, Bytes: 1},
		},
		"base64": {
			entries: []SessionStoreEntry{mustStateJSON(t, archiveEntry{
				Format: SessionStoreFormat, Encoding: archiveEncodingTarZstdBase64, Final: true, SHA256: sha, Data: "not-base64",
			})},
			info: archiveInfo{SHA256: sha, Bytes: 1},
		},
		"oversize metadata": {
			entries: []SessionStoreEntry{validEntry},
			info:    archiveInfo{SHA256: sha, Bytes: int(maxHydrateFileBytes + 1)},
		},
		"oversize chunk": {
			entries: []SessionStoreEntry{mustStateJSON(t, archiveEntry{
				Format: SessionStoreFormat, Encoding: archiveEncodingTarZstdBase64, Final: true, SHA256: sha,
				Data: base64.StdEncoding.EncodeToString(make([]byte, archiveChunkBytes+1)),
			})},
			info: archiveInfo{SHA256: sha, Bytes: archiveChunkBytes + 1},
		},
		"size": {
			entries: []SessionStoreEntry{validEntry},
			info:    archiveInfo{SHA256: sha, Bytes: len(data) + 1},
		},
		"checksum": {
			entries: []SessionStoreEntry{wrongChecksumEntry},
			info:    archiveInfo{SHA256: wrongSHA, Bytes: len(data)},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeArchiveEntries(test.entries, test.info); err == nil {
				t.Fatalf("decodeArchiveEntries accepted %s fault", name)
			}
		})
	}
}

func TestStateDBSnapshotHydrateRoundTrip(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	xdg, err := nativehermes.CreateXDGDirs(root, "session-1")
	if err != nil {
		t.Fatalf("nativehermes.CreateXDGDirs: %v", err)
	}
	for name, body := range map[string]string{
		"state.db":     "main",
		"state.db-wal": "wal",
		"state.db-shm": "shm",
		"state.db-bak": "ignored",
	} {
		if err4 := os.WriteFile(filepath.Join(xdg.Root, name), []byte(body), 0o600); err4 != nil {
			t.Fatalf("write %s: %v", name, err4)
		}
	}

	store := NewInMemorySessionStore()
	client := newFakeHermesClient()
	client.xdg = xdg
	session := testSession(newTestAgent(WithSessionStore(store)), client)
	if err5 := session.snapshotToStore(ctx); err5 != nil {
		t.Fatalf("snapshotToStore: %v", err5)
	}

	mainEntries, err := store.Load(ctx, SessionKey{SessionID: "session-1", Subpath: SessionStoreMainSubpath})
	if err != nil {
		t.Fatal(err)
	}
	var snapshot stateSnapshot
	if err6 := json.Unmarshal(mainEntries[len(mainEntries)-1], &snapshot); err6 != nil {
		t.Fatal(err6)
	}
	if snapshot.Archives["state-db"].Subpath != stateDBSubpath || len(snapshot.Archives) != 1 {
		t.Fatalf("snapshot archives = %#v", snapshot.Archives)
	}
	stateEntries, err := store.Load(ctx, SessionKey{SessionID: "session-1", Subpath: stateDBSubpath})
	if err != nil || len(stateEntries) != 1 {
		t.Fatalf("state-db entries = %d err=%v", len(stateEntries), err)
	}

	if err7 := os.RemoveAll(xdg.Root); err7 != nil {
		t.Fatal(err7)
	}
	restored, err := nativehermes.CreateXDGDirs(root, "session-1-restored")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := hydrateStateFromStore(ctx, store, "session-1", restored); err != nil || !ok {
		t.Fatalf("hydrate state-db ok=%v err=%v", ok, err)
	}
	for name, want := range map[string]string{"state.db": "main", "state.db-wal": "wal", "state.db-shm": "shm"} {
		data, err := os.ReadFile(filepath.Join(restored.Root, name))
		if err != nil || string(data) != want {
			t.Fatalf("restored %s = %q err=%v", name, data, err)
		}
	}
	if _, err := os.Stat(filepath.Join(restored.Root, "state.db-bak")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unexpected state.db-bak restored err=%v", err)
	}
}

func TestSnapshotToStoreRefusesPendingState(t *testing.T) {
	ctx := context.Background()
	for _, tt := range []struct {
		name string
		want string
		set  func(*session) func()
	}{
		{
			name: "turn",
			want: "turn",
			set: func(s *session) func() {
				s.beginTurn(ctx, "test-turn")

				return s.finishTurn
			},
		},
		{
			name: "permission",
			want: "permission",
			set: func(s *session) func() {
				s.pending["p"] = nativehermes.PermissionRequest{ID: "p", SessionID: "native-1"}

				return func() { s.pending = map[string]nativehermes.PermissionRequest{} }
			},
		},
		{
			name: "elicitation",
			want: "elicitation",
			set: func(s *session) func() {
				s.questions["q"] = nativehermes.QuestionRequest{ID: "q", SessionID: "native-1"}

				return func() { s.questions = map[string]nativehermes.QuestionRequest{} }
			},
		},
		{
			name: "generation",
			want: "generation",
			set: func(s *session) func() {
				s.activeMessageIDs["m"] = struct{}{}

				return func() { s.activeMessageIDs = map[string]struct{}{} }
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			session := testSession(newTestAgent(WithSessionStore(NewInMemorySessionStore())), newFakeHermesClient())
			cleanup := tt.set(session)
			defer cleanup()
			if err := session.snapshotToStore(ctx); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("snapshot pending %s err = %v", tt.name, err)
			}
		})
	}
}

func TestHydrateStateFromStoreErrors(t *testing.T) {
	ctx := context.Background()
	xdg, err := nativehermes.CreateXDGDirs(t.TempDir(), "hydrate")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := hydrateStateFromStore(ctx, NewInMemorySessionStore(), "missing", xdg); err != nil || ok {
		t.Fatalf("missing hydrate ok=%v err=%v", ok, err)
	}
	errStore := &errorSessionStore{err: errors.New("load failed")}
	if _, _, _, err := hydrateStateFromStore(ctx, errStore, "s", xdg); err == nil {
		t.Fatal("hydrate ignored load error")
	}

	store := NewInMemorySessionStore()
	main := SessionKey{SessionID: "s", Subpath: SessionStoreMainSubpath}
	if err := store.Replace(ctx, main, []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{json.RawMessage(`{"format":"bad"}`)}},
		{Key: SessionKey{SessionID: "s", Subpath: idmapSubpath}, Entries: []SessionStoreEntry{json.RawMessage(`{"format":"bad"}`)}},
	}); err != nil {
		t.Fatalf("replace bad format: %v", err)
	}
	if _, _, _, err := hydrateStateFromStore(ctx, store, "s", xdg); err == nil {
		t.Fatal("hydrate accepted bad format")
	}

	store = NewInMemorySessionStore()
	idmapData, _ := json.Marshal(idmapRecord{SessionID: "s", NativeSessionID: "n", Format: SessionStoreFormat})
	snapshot := validHydrateSnapshot()
	snapshot.Archives = map[string]archiveInfo{"state-db": {Subpath: stateDBSubpath, SHA256: "missing", Bytes: 1}}
	mainData, _ := json.Marshal(snapshot)
	if err := store.Replace(ctx, main, []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{mainData}},
		{Key: SessionKey{SessionID: "s", Subpath: idmapSubpath}, Entries: []SessionStoreEntry{idmapData}},
	}); err != nil {
		t.Fatalf("replace missing archive: %v", err)
	}
	if _, _, _, err := hydrateStateFromStore(ctx, store, "s", xdg); err == nil {
		t.Fatal("hydrate accepted missing archive")
	}
}

func TestHydrateStateDBArchiveFaults(t *testing.T) {
	ctx := context.Background()
	xdg, err := nativehermes.CreateXDGDirs(t.TempDir(), "hydrate")
	if err != nil {
		t.Fatal(err)
	}
	data := testTarZstd(t, []tar.Header{{Name: "state.db", Typeflag: tar.TypeReg, Mode: 0o600, Size: 4}}, map[string]string{"state.db": "body"})
	sum := sha256.Sum256(data)

	t.Run("load error", func(t *testing.T) {
		store := validStateDBHydrateStore(t, ctx, data, hex.EncodeToString(sum[:]))
		errStore := selectiveLoadErrorStore{
			SessionStore: store,
			key:          SessionKey{SessionID: "s", Subpath: stateDBSubpath},
			err:          errors.New("state-db load failed"),
		}
		if _, _, _, err := hydrateStateFromStore(ctx, errStore, "s", xdg); err == nil {
			t.Fatal("hydrate ignored state-db load error")
		}
	})

	for name, entry := range map[string]SessionStoreEntry{
		"missing": nil,
		"json":    json.RawMessage(`{`),
		"metadata": mustStateJSON(t, archiveEntry{
			Format:   "bad",
			Encoding: "tar+zstd+base64",
			Final:    true,
			SHA256:   hex.EncodeToString(sum[:]),
			Data:     base64.StdEncoding.EncodeToString(data),
		}),
		"base64": mustStateJSON(t, archiveEntry{
			Format:   SessionStoreFormat,
			Encoding: "tar+zstd+base64",
			Final:    true,
			Data:     "not base64",
		}),
		"checksum": mustStateJSON(t, archiveEntry{
			Format:   SessionStoreFormat,
			Encoding: "tar+zstd+base64",
			Final:    true,
			SHA256:   "bad",
			Data:     base64.StdEncoding.EncodeToString(data),
		}),
		"decode": mustStateJSON(t, archiveEntry{
			Format:   SessionStoreFormat,
			Encoding: "tar+zstd+base64",
			Final:    true,
			SHA256:   hex.EncodeToString(sha256Bytes([]byte("not zstd"))),
			Data:     base64.StdEncoding.EncodeToString([]byte("not zstd")),
		}),
		"traversal": mustStateJSON(t, archiveEntry{
			Format:   SessionStoreFormat,
			Encoding: "tar+zstd+base64",
			Final:    true,
			SHA256:   hex.EncodeToString(sha256Bytes(testTarZstd(t, []tar.Header{{Name: "../escape", Typeflag: tar.TypeReg, Size: 0}}, nil))),
			Data:     base64.StdEncoding.EncodeToString(testTarZstd(t, []tar.Header{{Name: "../escape", Typeflag: tar.TypeReg, Size: 0}}, nil)),
		}),
	} {
		t.Run(name, func(t *testing.T) {
			store := validStateDBHydrateStore(t, ctx, data, hex.EncodeToString(sum[:]))
			if entry == nil {
				replaceStateDBHydrateRecords(t, ctx, store, nil)
			} else {
				replaceStateDBHydrateRecords(t, ctx, store, []SessionStoreEntry{entry})
			}
			if _, _, _, err := hydrateStateFromStore(ctx, store, "s", xdg); err == nil {
				t.Fatalf("hydrate accepted state-db %s fault", name)
			}
		})
	}

	traversalArchive := testTarZstd(t, []tar.Header{{Name: "../escape", Typeflag: tar.TypeReg, Size: 0}}, nil)
	traversalSum := sha256.Sum256(traversalArchive)
	traversalStore := validStateDBHydrateStore(t, ctx, traversalArchive, hex.EncodeToString(traversalSum[:]))
	if _, _, _, err := hydrateStateFromStore(ctx, traversalStore, "s", xdg); err == nil {
		t.Fatal("hydrate accepted a traversal archive after entry validation")
	}
}

func TestHydrateStateAgreementRejectsMismatches(t *testing.T) {
	ctx := context.Background()
	xdg, err := nativehermes.CreateXDGDirs(t.TempDir(), "hydrate")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*idmapRecord, *stateSnapshot)
		want   string
	}{
		{
			name: "requested ACP id disagrees with idmap",
			mutate: func(idmap *idmapRecord, _ *stateSnapshot) {
				idmap.SessionID = "other"
			},
			want: "idmap session mismatch",
		},
		{
			name: "requested ACP id disagrees with snapshot",
			mutate: func(_ *idmapRecord, snapshot *stateSnapshot) {
				snapshot.Session.SessionID = "other"
			},
			want: "snapshot session mismatch",
		},
		{
			name: "native id disagrees",
			mutate: func(_ *idmapRecord, snapshot *stateSnapshot) {
				snapshot.Session.NativeSessionID = "other-native"
			},
			want: "native session mismatch",
		},
		{
			name: "parent ACP id disagrees",
			mutate: func(idmap *idmapRecord, snapshot *stateSnapshot) {
				idmap.ParentSessionID = "parent"
				snapshot.Session.ParentSessionID = "other-parent"
			},
			want: "parent session mismatch",
		},
		{
			name: "parent native id disagrees",
			mutate: func(idmap *idmapRecord, snapshot *stateSnapshot) {
				idmap.NativeParentSessionID = "native-parent"
				snapshot.Session.NativeParentSessionID = "other-native-parent"
			},
			want: "native parent session mismatch",
		},
		{
			name: "terminal summary is invalid",
			mutate: func(_ *idmapRecord, snapshot *stateSnapshot) {
				snapshot.Terminal = nil
			},
			want: "terminal summary",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := validHydrateStore(t, ctx)
			idmap := validHydrateIDMap()
			snapshot := validHydrateSnapshot()
			tt.mutate(&idmap, &snapshot)
			replaceHydrateRecords(t, ctx, store, idmap, snapshot)
			if _, _, _, err := hydrateStateFromStore(ctx, store, "s", xdg); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("hydrate mismatch err = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestSnapshotToStoreNilClientAndFileSQLiteErrors(t *testing.T) {
	if err := (&session{agent: newTestAgent(), client: nil}).snapshotToStore(context.Background()); err != nil {
		t.Fatalf("nil client snapshot: %v", err)
	}
	if _, ok, err := sqliteArchiveContent("", filepath.Join(t.TempDir(), "missing.db")); err == nil || ok {
		t.Fatalf("sqliteArchiveContent missing ok=%v err=%v", ok, err)
	}
	short := filepath.Join(t.TempDir(), "short.db")
	if err := os.WriteFile(short, []byte("short"), 0o600); err != nil {
		t.Fatal(err)
	}
	if ok, err := isSQLiteDatabase(short); err != nil || ok {
		t.Fatalf("short sqlite ok=%v err=%v", ok, err)
	}
	if err := copyFile(filepath.Join(t.TempDir(), "missing"), filepath.Join(t.TempDir(), "out"), 0o600); err == nil {
		t.Fatal("copyFile accepted missing source")
	}
	source := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(source, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := copyFile(source, string([]byte{0}), 0o600); err == nil {
		t.Fatal("copyFile accepted invalid target")
	}
	if err := scrubSQLiteCredentialTables(short); err == nil {
		t.Fatal("scrubSQLiteCredentialTables accepted non-sqlite")
	}
	dbPath := filepath.Join(t.TempDir(), "clean.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err8 := db.Exec(`CREATE TABLE regular (id TEXT PRIMARY KEY, body TEXT)`); err8 != nil {
		t.Fatal(err8)
	}
	if err9 := db.Close(); err9 != nil {
		t.Fatal(err9)
	}
	db, err = sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sensitive, err := sqliteTableIsCredentialBearing(context.Background(), db, "regular")
	if err != nil || sensitive {
		t.Fatalf("regular table sensitive=%v err=%v", sensitive, err)
	}
}

func TestSnapshotToStoreMarshalAndArchiveFaults(t *testing.T) {
	ctx := context.Background()

	t.Run("terminal history read error", func(t *testing.T) {
		session := snapshotFaultSession(t)
		client, ok := session.client.(*fakeHermesClient)
		if !ok {
			t.Fatal("snapshot client is not a fakeHermesClient")
		}
		client.messagesErr = errors.New("messages failed")
		if err := session.snapshotToStore(ctx); err == nil || !strings.Contains(err.Error(), "terminal history") {
			t.Fatalf("snapshot terminal read error = %v", err)
		}
	})

	t.Run("terminal history identity error", func(t *testing.T) {
		session := snapshotFaultSession(t)
		client, ok := session.client.(*fakeHermesClient)
		if !ok {
			t.Fatal("snapshot client is not a fakeHermesClient")
		}
		client.messages = []nativehermes.NativeMessage{
			testHistoryMessage("history-2", "native-1", valAssistant, "stop"),
		}
		if err := session.snapshotToStore(ctx); err == nil || !strings.Contains(err.Error(), "terminal history message") {
			t.Fatalf("snapshot terminal identity error = %v", err)
		}
	})

	t.Run("created at is initialized", func(t *testing.T) {
		session := snapshotFaultSession(t)
		session.idmap.CreatedAtUnixMilli = 0
		if err := session.snapshotToStore(ctx); err != nil {
			t.Fatalf("snapshotToStore: %v", err)
		}
		entries, err := session.agent.sessionStore().Load(ctx, SessionKey{SessionID: string(session.id), Subpath: idmapSubpath})
		if err != nil {
			t.Fatal(err)
		}
		var idmap idmapRecord
		if err := json.Unmarshal(entries[len(entries)-1], &idmap); err != nil {
			t.Fatal(err)
		}
		if idmap.CreatedAtUnixMilli == 0 {
			t.Fatal("CreatedAtUnixMilli was not initialized")
		}
	})

	t.Run("state db archive encode error", func(t *testing.T) {
		restoreStateStoreSeams(t)
		session := snapshotFaultSession(t)
		if err := os.WriteFile(filepath.Join(session.client.XDGDirs().Root, "state.db"), []byte("body"), 0o600); err != nil {
			t.Fatal(err)
		}
		stateLstat = func(string) (os.FileInfo, error) { return nil, errors.New("state db lstat failed") }
		if err := session.snapshotToStore(ctx); err == nil {
			t.Fatal("snapshot ignored state db archive error")
		}
	})

	t.Run("state db archive marshal error", func(t *testing.T) {
		restoreStateStoreSeams(t)
		session := snapshotFaultSession(t)
		if err := os.WriteFile(filepath.Join(session.client.XDGDirs().Root, "state.db"), []byte("body"), 0o600); err != nil {
			t.Fatal(err)
		}
		stateJSONMarshal = func(any) ([]byte, error) {
			return nil, errors.New("marshal failed")
		}
		if err := session.snapshotToStore(ctx); err == nil {
			t.Fatal("snapshot ignored state db archive marshal error")
		}
	})

	for name, tt := range map[string]struct {
		failAt int
		state  bool
	}{
		"state db archive entry": {failAt: 1, state: true},
		"main entry":             {failAt: 1},
		"idmap entry":            {failAt: 2},
	} {
		t.Run("marshal "+name, func(t *testing.T) {
			restoreStateStoreSeams(t)
			calls := 0
			stateJSONMarshal = func(value any) ([]byte, error) {
				calls++
				if calls == tt.failAt {
					return nil, errors.New("marshal failed")
				}

				return json.Marshal(value)
			}
			session := snapshotFaultSession(t)
			if tt.state {
				if err := os.WriteFile(filepath.Join(session.client.XDGDirs().Root, "state.db"), []byte("body"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := session.snapshotToStore(ctx); err == nil {
				t.Fatalf("snapshot ignored %s marshal error", name)
			}
		})
	}
}

func TestHydrateStateFromStoreFaults(t *testing.T) {
	ctx := context.Background()
	xdg, err := nativehermes.CreateXDGDirs(t.TempDir(), "hydrate")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("main load error", func(t *testing.T) {
		store := validHydrateStore(t, ctx)
		errStore := selectiveLoadErrorStore{SessionStore: store, key: SessionKey{SessionID: "s", Subpath: SessionStoreMainSubpath}, err: errors.New("main load failed")}
		if _, _, _, err := hydrateStateFromStore(ctx, errStore, "s", xdg); err == nil {
			t.Fatal("hydrate ignored main load error")
		}
	})

	t.Run("invalid idmap and main json", func(t *testing.T) {
		for name, replacements := range map[string][]SessionStoreReplacement{
			"idmap": {
				{Key: SessionKey{SessionID: "s", Subpath: idmapSubpath}, Entries: []SessionStoreEntry{json.RawMessage(`{`)}},
				{Key: SessionKey{SessionID: "s", Subpath: SessionStoreMainSubpath}, Entries: []SessionStoreEntry{mustStateJSON(t, validHydrateSnapshot())}},
			},
			"main": {
				{Key: SessionKey{SessionID: "s", Subpath: idmapSubpath}, Entries: []SessionStoreEntry{mustStateJSON(t, validHydrateIDMap())}},
				{Key: SessionKey{SessionID: "s", Subpath: SessionStoreMainSubpath}, Entries: []SessionStoreEntry{json.RawMessage(`{`)}},
			},
		} {
			t.Run(name, func(t *testing.T) {
				store := NewInMemorySessionStore()
				main := SessionKey{SessionID: "s", Subpath: SessionStoreMainSubpath}
				if err := store.Replace(ctx, main, replacements); err != nil {
					t.Fatal(err)
				}
				if _, _, _, err := hydrateStateFromStore(ctx, store, "s", xdg); err == nil {
					t.Fatal("hydrate accepted invalid json")
				}
			})
		}
	})

	t.Run("archive load and decode errors", func(t *testing.T) {
		for name, mutate := range map[string]func(*InMemorySessionStore){
			"load": func(store *InMemorySessionStore) {
				*store = *validHydrateStore(t, ctx)
			},
			"json": func(store *InMemorySessionStore) {
				replaceArchiveEntry(t, ctx, store, json.RawMessage(`{`))
			},
			"base64": func(store *InMemorySessionStore) {
				replaceArchiveEntry(t, ctx, store, mustStateJSON(t, archiveEntry{Format: SessionStoreFormat, Encoding: "tar+zstd+base64", Final: true, Data: "not base64"}))
			},
			"decode": func(store *InMemorySessionStore) {
				data := []byte("not zstd")
				sum := sha256.Sum256(data)
				replaceArchiveEntry(t, ctx, store, mustStateJSON(t, archiveEntry{
					Format:   SessionStoreFormat,
					Encoding: "tar+zstd+base64",
					Final:    true,
					SHA256:   hex.EncodeToString(sum[:]),
					Data:     base64.StdEncoding.EncodeToString(data),
				}))
			},
		} {
			t.Run(name, func(t *testing.T) {
				store := validHydrateStore(t, ctx)
				if name == "load" {
					errStore := selectiveLoadErrorStore{
						SessionStore: store,
						key:          SessionKey{SessionID: "s", Subpath: stateDBSubpath},
						err:          errors.New("archive load failed"),
					}
					if _, _, _, err := hydrateStateFromStore(ctx, errStore, "s", xdg); err == nil {
						t.Fatal("hydrate ignored archive load error")
					}

					return
				}
				mutate(store)
				if _, _, _, err := hydrateStateFromStore(ctx, store, "s", xdg); err == nil {
					t.Fatal("hydrate accepted bad archive")
				}
			})
		}
	})
}

func TestEncodeHermesStateDBArchiveFaults(t *testing.T) {
	if _, _, ok, err := encodeHermesStateDBArchive("", ""); err != nil || ok {
		t.Fatalf("empty root ok=%v err=%v", ok, err)
	}
	emptyRoot := t.TempDir()
	if _, _, ok, err := encodeHermesStateDBArchive("", emptyRoot); err != nil || ok {
		t.Fatalf("empty state db root ok=%v err=%v", ok, err)
	}
	dirRoot := t.TempDir()
	if err := os.Mkdir(filepath.Join(dirRoot, "state.db"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := encodeHermesStateDBArchive("", dirRoot); err != nil || ok {
		t.Fatalf("directory state db ok=%v err=%v", ok, err)
	}

	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "state.db"), []byte("body"), 0o600); err != nil {
		t.Fatal(err)
	}
	if archive, sha, ok, err := encodeHermesStateDBArchive("", root); err != nil || !ok || len(archive) == 0 || sha == "" {
		t.Fatalf("encodeHermesStateDBArchive ok=%v sha=%q len=%d err=%v", ok, sha, len(archive), err)
	}

	tests := map[string]func(){
		"initial lstat": func() {
			stateLstat = func(string) (os.FileInfo, error) { return nil, errors.New("lstat failed") }
		},
		"sqlite content": func() {
			stateSQLiteArchiveContent = func(string, string) ([]byte, bool, error) {
				return nil, false, errors.New("sqlite content failed")
			}
		},
		"second lstat": func() {
			originalLstat := stateLstat
			stateDBPath := filepath.Join(root, "state.db")
			stateDBCalls := 0
			stateLstat = func(path string) (os.FileInfo, error) {
				if path == stateDBPath {
					stateDBCalls++
				}
				if path == stateDBPath && stateDBCalls == 3 {
					return nil, errors.New("second lstat failed")
				}

				return originalLstat(path)
			}
		},
		"candidate lstat": func() {
			originalLstat := stateLstat
			walPath := filepath.Join(root, "state.db-wal")
			stateLstat = func(path string) (os.FileInfo, error) {
				if path == walPath {
					return nil, errors.New("candidate lstat failed")
				}

				return originalLstat(path)
			}
		},
		"header": func() {
			stateFileInfoHeader = func(os.FileInfo, string) (*tar.Header, error) { return nil, errors.New("header failed") }
		},
		"write header": func() {
			stateNewTarWriter = func(io.Writer) archiveTarWriter {
				return fakeTarWriter{writeHeaderErr: errors.New("write header failed")}
			}
		},
		"write scrubbed": func() {
			stateSQLiteArchiveContent = func(string, string) ([]byte, bool, error) { return []byte("scrubbed"), true, nil }
			stateNewTarWriter = func(io.Writer) archiveTarWriter {
				return fakeTarWriter{writeErr: errors.New("write failed")}
			}
		},
		"open": func() {
			stateSQLiteArchiveContent = func(string, string) ([]byte, bool, error) { return nil, false, nil }
			stateOpen = func(string) (io.ReadCloser, error) { return nil, errors.New("open failed") }
		},
		"copy": func() {
			stateCopy = func(io.Writer, io.Reader) (int64, error) { return 0, errors.New("copy failed") }
		},
		"file close": func() {
			stateOpen = func(string) (io.ReadCloser, error) {
				return fakeReadCloser{Reader: strings.NewReader("body"), closeErr: errors.New("close failed")}, nil
			}
		},
		"tar close": func() {
			stateNewTarWriter = func(io.Writer) archiveTarWriter {
				return fakeTarWriter{closeErr: errors.New("tar close failed")}
			}
		},
		"zstd new": func() {
			stateNewZstdWriter = func(io.Writer) (archiveZstdWriter, error) {
				return nil, errors.New("zstd new failed")
			}
		},
		"zstd write": func() {
			stateNewZstdWriter = func(io.Writer) (archiveZstdWriter, error) {
				return fakeZstdWriter{writeErr: errors.New("zstd write failed")}, nil
			}
		},
		"zstd close": func() {
			stateNewZstdWriter = func(io.Writer) (archiveZstdWriter, error) {
				return fakeZstdWriter{closeErr: errors.New("zstd close failed")}, nil
			}
		},
	}
	for name, setup := range tests {
		t.Run(name, func(t *testing.T) {
			restoreStateStoreSeams(t)
			setup()
			if _, _, _, err := encodeHermesStateDBArchive("", root); err == nil {
				t.Fatal("encodeHermesStateDBArchive ignored injected error")
			}
		})
	}
}

func TestDecodeXDGArchiveFaults(t *testing.T) {
	regularArchive := testTarZstd(t, []tar.Header{{Name: "dir/file.txt", Typeflag: tar.TypeReg, Mode: 0o600, Size: 4}}, map[string]string{"dir/file.txt": "body"})
	dirArchive := testTarZstd(t, []tar.Header{{Name: "dir", Typeflag: tar.TypeDir, Mode: 0o700}}, nil)
	bigArchive := testTarZstdPartial(t, tar.Header{Name: "big", Typeflag: tar.TypeReg, Mode: 0o600, Size: maxHydrateFileBytes + 1})
	badTarArchive := testZstdBytes(t, []byte("not a tar stream"))

	tests := map[string]struct {
		data  []byte
		setup func()
	}{
		"remove": {data: regularArchive, setup: func() {
			stateRemoveAll = func(string) error { return errors.New("remove failed") }
		}},
		"mkdir root": {data: regularArchive, setup: func() {
			stateMkdirAll = func(string, os.FileMode) error { return errors.New("mkdir failed") }
		}},
		"zstd": {data: []byte("not zstd"), setup: func() {
			stateNewZstdReader = func(io.Reader) (*zstd.Decoder, error) { return nil, errors.New("zstd failed") }
		}},
		"abs target": {data: regularArchive, setup: func() {
			stateAbs = func(string) (string, error) { return "", errors.New("abs failed") }
		}},
		"tar next": {data: badTarArchive},
		"abs child": {data: regularArchive, setup: func() {
			calls := 0
			stateAbs = func(path string) (string, error) {
				calls++
				if calls == 2 {
					return "", errors.New("child abs failed")
				}

				return filepath.Clean(path), nil
			}
		}},
		"escape": {data: regularArchive, setup: func() {
			calls := 0
			stateAbs = func(path string) (string, error) {
				calls++
				if calls == 2 {
					return filepath.Join(string(os.PathSeparator), "elsewhere"), nil
				}

				return filepath.Clean(path), nil
			}
		}},
		"dir mkdir": {data: dirArchive, setup: func() {
			calls := 0
			stateMkdirAll = func(string, os.FileMode) error {
				calls++
				if calls == 2 {
					return errors.New("dir mkdir failed")
				}

				return nil
			}
		}},
		"big file": {data: bigArchive},
		"parent mkdir": {data: regularArchive, setup: func() {
			calls := 0
			stateMkdirAll = func(string, os.FileMode) error {
				calls++
				if calls == 2 {
					return errors.New("parent mkdir failed")
				}

				return nil
			}
		}},
		"open file": {data: regularArchive, setup: func() {
			stateOpenFile = func(string, int, os.FileMode) (io.WriteCloser, error) {
				return nil, errors.New("open file failed")
			}
		}},
		"copy": {data: regularArchive, setup: func() {
			stateOpenFile = func(string, int, os.FileMode) (io.WriteCloser, error) {
				return fakeWriteCloser{}, nil
			}
			stateCopyN = func(io.Writer, io.Reader, int64) (int64, error) {
				return 0, errors.New("copy failed")
			}
		}},
		"short write": {data: regularArchive, setup: func() {
			stateOpenFile = func(string, int, os.FileMode) (io.WriteCloser, error) {
				return fakeWriteCloser{}, nil
			}
			stateCopyN = func(io.Writer, io.Reader, int64) (int64, error) { return 0, nil }
		}},
		"close": {data: regularArchive, setup: func() {
			stateOpenFile = func(string, int, os.FileMode) (io.WriteCloser, error) {
				return fakeWriteCloser{closeErr: errors.New("close failed")}, nil
			}
		}},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			restoreStateStoreSeams(t)
			if tc.setup != nil {
				tc.setup()
			}
			if err := decodeXDGArchive(tc.data, t.TempDir()); err == nil {
				t.Fatal("decodeXDGArchive ignored injected error")
			}
		})
	}
}

// TestSQLiteArchiveWaitsForConcurrentWriter pins the snapshot behaviour that
// matters in production: the live Hermes process is still writing state.db when
// a turn's terminal snapshot runs, and a rollback-journal commit locks the
// snapshot's read out. The archive must wait for that lock instead of failing
// the turn with SQLITE_BUSY.
func TestSQLiteArchiveWaitsForConcurrentWriter(t *testing.T) {
	restoreStateStoreSeams(t)

	scratchDir := t.TempDir()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	seedSQLiteStore(t, dbPath)

	writer, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}

	defer writer.Close()
	writer.SetMaxOpenConns(1)

	if _, err := writer.Exec("BEGIN EXCLUSIVE"); err != nil {
		t.Fatalf("BEGIN EXCLUSIVE: %v", err)
	}

	released := make(chan struct{})

	timer := time.AfterFunc(200*time.Millisecond, func() {
		if _, rollbackErr := writer.Exec("ROLLBACK"); rollbackErr != nil {
			t.Errorf("ROLLBACK: %v", rollbackErr)
		}

		close(released)
	})
	defer timer.Stop()

	data, ok, archiveErr := sqliteArchiveContent(scratchDir, dbPath)
	if archiveErr != nil || !ok || len(data) == 0 {
		t.Fatalf("sqliteArchiveContent under a held write lock: ok=%v len=%d err=%v", ok, len(data), archiveErr)
	}

	<-released
}

// TestSQLiteScrubWaitsForConcurrentWriter holds the scrub to the same rule as
// the archive read it follows: a lock another connection holds is waited on,
// never reported as a failed snapshot.
func TestSQLiteScrubWaitsForConcurrentWriter(t *testing.T) {
	restoreStateStoreSeams(t)

	dbPath := filepath.Join(t.TempDir(), "state.db")
	seedSQLiteStore(t, dbPath)

	writer, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}

	defer writer.Close()
	writer.SetMaxOpenConns(1)

	if _, err := writer.Exec("BEGIN EXCLUSIVE"); err != nil {
		t.Fatalf("BEGIN EXCLUSIVE: %v", err)
	}

	released := make(chan struct{})

	timer := time.AfterFunc(200*time.Millisecond, func() {
		if _, rollbackErr := writer.Exec("ROLLBACK"); rollbackErr != nil {
			t.Errorf("ROLLBACK: %v", rollbackErr)
		}

		close(released)
	})
	defer timer.Stop()

	if scrubErr := scrubSQLiteCredentialTables(dbPath); scrubErr != nil {
		t.Fatalf("scrubSQLiteCredentialTables under a held write lock: %v", scrubErr)
	}

	<-released
}

func TestSQLiteArchiveAndCopyFaults(t *testing.T) {
	tests := map[string]func(string){
		"mkdir temp": func(string) {
			stateMkdirTemp = func(string, string) (string, error) { return "", errors.New("mkdir temp failed") }
		},
		"vacuum open": func(string) {
			stateSQLOpen = func(string, string) (*sql.DB, error) { return nil, errors.New("vacuum open failed") }
		},
		"vacuum exec": func(string) {
			stateSQLOpen = func(string, string) (*sql.DB, error) { return openFaultSQL(t, "vacuum-into-error"), nil }
		},
		"scrub": func(string) {
			stateScrubSQLiteCredentialTables = func(string) error { return errors.New("scrub failed") }
		},
		"read": func(string) {
			stateReadFile = func(string) ([]byte, error) { return nil, errors.New("read failed") }
		},
	}
	for name, setup := range tests {
		t.Run(name, func(t *testing.T) {
			restoreStateStoreSeams(t)
			dbPath := filepath.Join(t.TempDir(), "store.db")
			seedSQLiteStore(t, dbPath)
			setup(dbPath)
			if _, ok, err := sqliteArchiveContent("", dbPath); err == nil || ok {
				t.Fatal("sqliteArchiveContent ignored injected error")
			}
		})
	}

	t.Run("scratch parent", func(t *testing.T) {
		restoreStateStoreSeams(t)
		dbPath := filepath.Join(t.TempDir(), "store.db")
		seedSQLiteStore(t, dbPath)
		if _, ok, err := sqliteArchiveContent(string([]byte{0}), dbPath); err == nil || ok {
			t.Fatal("sqliteArchiveContent ignored scratch parent error")
		}
	})

	source := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(source, []byte("body"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Run("sqlite read", func(t *testing.T) {
		restoreStateStoreSeams(t)
		stateOpen = func(string) (io.ReadCloser, error) {
			return errorReadCloser{err: errors.New("read failed")}, nil
		}
		if ok, err := isSQLiteDatabase("ignored"); err == nil || ok {
			t.Fatalf("isSQLiteDatabase read error ok=%v err=%v", ok, err)
		}
	})
	t.Run("copy", func(t *testing.T) {
		restoreStateStoreSeams(t)
		stateCopy = func(io.Writer, io.Reader) (int64, error) { return 0, errors.New("copy failed") }
		if err := copyFile(source, filepath.Join(t.TempDir(), "target"), 0o600); err == nil {
			t.Fatal("copyFile ignored copy error")
		}
	})
	t.Run("copy close", func(t *testing.T) {
		restoreStateStoreSeams(t)
		stateOpenFile = func(string, int, os.FileMode) (io.WriteCloser, error) {
			return fakeWriteCloser{closeErr: errors.New("close failed")}, nil
		}
		if err := copyFile(source, filepath.Join(t.TempDir(), "target"), 0o600); err == nil {
			t.Fatal("copyFile ignored close error")
		}
	})
}

func TestSQLiteScrubFaults(t *testing.T) {
	t.Run("open", func(t *testing.T) {
		restoreStateStoreSeams(t)
		stateSQLOpen = func(string, string) (*sql.DB, error) { return nil, errors.New("open failed") }
		if err := scrubSQLiteCredentialTables("ignored"); err == nil {
			t.Fatal("scrub ignored open error")
		}
	})

	for _, scenario := range []string{
		"scrub-exec-error",
		"credential-query-error",
		"scrub-delete-error",
		"scrub-vacuum-error",
		"scrub-final-checkpoint-error",
	} {
		t.Run(scenario, func(t *testing.T) {
			restoreStateStoreSeams(t)
			stateSQLOpen = func(string, string) (*sql.DB, error) { return openFaultSQL(t, scenario), nil }
			if err := scrubSQLiteCredentialTables("ignored"); err == nil {
				t.Fatal("scrub ignored injected SQL error")
			}
		})
	}

	for _, scenario := range []string{"credential-scan-error", "credential-rows-error", "table-query-error"} {
		t.Run(scenario, func(t *testing.T) {
			db := openFaultSQL(t, scenario)
			defer db.Close()
			if _, err := sqliteCredentialTables(context.Background(), db); err == nil {
				t.Fatal("sqliteCredentialTables ignored injected error")
			}
		})
	}

	for _, scenario := range []string{"table-scan-error", "table-rows-error"} {
		t.Run(scenario, func(t *testing.T) {
			db := openFaultSQL(t, scenario)
			defer db.Close()
			if _, err := sqliteTableIsCredentialBearing(context.Background(), db, "regular"); err == nil {
				t.Fatal("sqliteTableIsCredentialBearing ignored injected error")
			}
		})
	}
	db := openFaultSQL(t, "table-sensitive-column")
	defer db.Close()
	sensitive, err := sqliteTableIsCredentialBearing(context.Background(), db, "regular")
	if err != nil || !sensitive {
		t.Fatalf("sensitive column result = %v err=%v", sensitive, err)
	}
	db = openFaultSQL(t, "unused")
	defer db.Close()
	sensitive, err = sqliteTableIsCredentialBearing(context.Background(), db, "api_key_store")
	if err != nil || !sensitive {
		t.Fatalf("sensitive table name result = %v err=%v", sensitive, err)
	}
}

func restoreStateStoreSeams(t *testing.T) {
	t.Helper()
	jsonMarshal := stateJSONMarshal
	walkDir := stateWalkDir
	rel := stateRel
	lstat := stateLstat
	fileInfoHeader := stateFileInfoHeader
	newTarWriter := stateNewTarWriter
	open := stateOpen
	copyFn := stateCopy
	newZstdWriter := stateNewZstdWriter
	newZstdReader := stateNewZstdReader
	removeAll := stateRemoveAll
	mkdirAll := stateMkdirAll
	abs := stateAbs
	openFile := stateOpenFile
	copyN := stateCopyN
	mkdirTemp := stateMkdirTemp
	stat := stateStat
	readFile := stateReadFile
	copyFileFn := stateCopyFile
	sqliteArchiveContentFn := stateSQLiteArchiveContent
	scrubSQLiteCredentialTablesFn := stateScrubSQLiteCredentialTables
	sqlOpen := stateSQLOpen
	t.Cleanup(func() {
		stateJSONMarshal = jsonMarshal
		stateWalkDir = walkDir
		stateRel = rel
		stateLstat = lstat
		stateFileInfoHeader = fileInfoHeader
		stateNewTarWriter = newTarWriter
		stateOpen = open
		stateCopy = copyFn
		stateNewZstdWriter = newZstdWriter
		stateNewZstdReader = newZstdReader
		stateRemoveAll = removeAll
		stateMkdirAll = mkdirAll
		stateAbs = abs
		stateOpenFile = openFile
		stateCopyN = copyN
		stateMkdirTemp = mkdirTemp
		stateStat = stat
		stateReadFile = readFile
		stateCopyFile = copyFileFn
		stateSQLiteArchiveContent = sqliteArchiveContentFn
		stateScrubSQLiteCredentialTables = scrubSQLiteCredentialTablesFn
		stateSQLOpen = sqlOpen
	})
}

func snapshotFaultSession(t *testing.T) *session {
	t.Helper()
	root := t.TempDir()
	xdg, err := nativehermes.CreateXDGDirs(root, "session-1")
	if err != nil {
		t.Fatal(err)
	}
	client := newFakeHermesClient()
	client.xdg = xdg
	agent := newTestAgent(WithSessionStore(NewInMemorySessionStore()))

	return testSession(agent, client)
}

func validHydrateStore(t *testing.T, ctx context.Context) *InMemorySessionStore {
	t.Helper()
	store := NewInMemorySessionStore()
	data := testTarZstd(t, nil, nil)
	sum := sha256.Sum256(data)
	archive := mustStateJSON(t, archiveEntry{
		Format:   SessionStoreFormat,
		Encoding: "tar+zstd+base64",
		Final:    true,
		SHA256:   hex.EncodeToString(sum[:]),
		Data:     base64.StdEncoding.EncodeToString(data),
	})
	main := SessionKey{SessionID: "s", Subpath: SessionStoreMainSubpath}
	snapshot := validHydrateSnapshot()
	snapshot.Archives = map[string]archiveInfo{"state-db": {Subpath: stateDBSubpath, SHA256: hex.EncodeToString(sum[:]), Bytes: len(data)}}
	replacements := []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{mustStateJSON(t, snapshot)}},
		{Key: SessionKey{SessionID: "s", Subpath: idmapSubpath}, Entries: []SessionStoreEntry{mustStateJSON(t, validHydrateIDMap())}},
		{Key: SessionKey{SessionID: "s", Subpath: stateDBSubpath}, Entries: []SessionStoreEntry{archive}},
	}
	if err := store.Replace(ctx, main, replacements); err != nil {
		t.Fatal(err)
	}

	return store
}

func validStateDBHydrateStore(t *testing.T, ctx context.Context, data []byte, sha string) *InMemorySessionStore {
	t.Helper()
	store := NewInMemorySessionStore()
	main := SessionKey{SessionID: "s", Subpath: SessionStoreMainSubpath}
	snapshot := validHydrateSnapshot()
	snapshot.Archives = map[string]archiveInfo{"state-db": {Subpath: stateDBSubpath, SHA256: sha, Bytes: len(data)}}
	archive := mustStateJSON(t, archiveEntry{
		Format:   SessionStoreFormat,
		Encoding: "tar+zstd+base64",
		Sequence: 0,
		Final:    true,
		SHA256:   sha,
		Data:     base64.StdEncoding.EncodeToString(data),
	})
	if err := store.Replace(ctx, main, []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{mustStateJSON(t, snapshot)}},
		{Key: SessionKey{SessionID: "s", Subpath: idmapSubpath}, Entries: []SessionStoreEntry{mustStateJSON(t, validHydrateIDMap())}},
		{Key: SessionKey{SessionID: "s", Subpath: stateDBSubpath}, Entries: []SessionStoreEntry{archive}},
	}); err != nil {
		t.Fatal(err)
	}

	return store
}

func validHydrateIDMap() idmapRecord {
	return idmapRecord{
		SessionID:       "s",
		NativeSessionID: "n",
		Format:          SessionStoreFormat,
	}
}

func validHydrateSnapshot() stateSnapshot {
	return stateSnapshot{
		Format:              SessionStoreFormat,
		CapturedAtUnixMilli: 1,
		Session: stateSnapshotSession{
			SessionID:       "s",
			NativeSessionID: "n",
		},
		Terminal: &stateSnapshotTerminal{},
		Archives: map[string]archiveInfo{},
		Wrapper:  &stateSnapshotWrapper{},
	}
}

func replaceHydrateRecords(t *testing.T, ctx context.Context, store *InMemorySessionStore, idmap idmapRecord, snapshot stateSnapshot) {
	t.Helper()
	main := SessionKey{SessionID: "s", Subpath: SessionStoreMainSubpath}
	replacements := []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{mustStateJSON(t, snapshot)}},
		{Key: SessionKey{SessionID: "s", Subpath: idmapSubpath}, Entries: []SessionStoreEntry{mustStateJSON(t, idmap)}},
	}
	if entries, err := store.Load(ctx, SessionKey{SessionID: "s", Subpath: stateDBSubpath}); err == nil && len(entries) > 0 {
		replacements = append(replacements, SessionStoreReplacement{Key: SessionKey{SessionID: "s", Subpath: stateDBSubpath}, Entries: entries})
	} else if err != nil {
		t.Fatal(err)
	}
	if err := store.Replace(ctx, main, replacements); err != nil {
		t.Fatal(err)
	}
}

func replaceArchiveEntry(t *testing.T, ctx context.Context, store *InMemorySessionStore, entry SessionStoreEntry) {
	t.Helper()
	main := SessionKey{SessionID: "s", Subpath: SessionStoreMainSubpath}
	snapshot := validHydrateSnapshot()
	snapshot.Archives = map[string]archiveInfo{"state-db": {Subpath: stateDBSubpath}}
	replacements := []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{mustStateJSON(t, snapshot)}},
		{Key: SessionKey{SessionID: "s", Subpath: idmapSubpath}, Entries: []SessionStoreEntry{mustStateJSON(t, validHydrateIDMap())}},
		{Key: SessionKey{SessionID: "s", Subpath: stateDBSubpath}, Entries: []SessionStoreEntry{entry}},
	}
	if err := store.Replace(ctx, main, replacements); err != nil {
		t.Fatal(err)
	}
}

func replaceStateDBHydrateRecords(t *testing.T, ctx context.Context, store *InMemorySessionStore, stateEntries []SessionStoreEntry) {
	t.Helper()
	main := SessionKey{SessionID: "s", Subpath: SessionStoreMainSubpath}
	entries := []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{mustStateJSON(t, func() stateSnapshot {
			snapshot := validHydrateSnapshot()
			snapshot.Archives = map[string]archiveInfo{"state-db": {Subpath: stateDBSubpath}}

			return snapshot
		}())}},
		{Key: SessionKey{SessionID: "s", Subpath: idmapSubpath}, Entries: []SessionStoreEntry{mustStateJSON(t, validHydrateIDMap())}},
	}
	if stateEntries != nil {
		entries = append(entries, SessionStoreReplacement{Key: SessionKey{SessionID: "s", Subpath: stateDBSubpath}, Entries: stateEntries})
	}
	if err := store.Replace(ctx, main, entries); err != nil {
		t.Fatal(err)
	}
}

func sha256Bytes(data []byte) []byte {
	sum := sha256.Sum256(data)

	return sum[:]
}

func mustStateJSON(t *testing.T, value any) SessionStoreEntry {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}

	return SessionStoreEntry(data)
}

type selectiveLoadErrorStore struct {
	SessionStore
	key SessionKey
	err error
}

func (s selectiveLoadErrorStore) Load(ctx context.Context, key SessionKey) ([]SessionStoreEntry, error) {
	if key == s.key {
		return nil, s.err
	}

	return s.SessionStore.Load(ctx, key)
}

type fakeTarWriter struct {
	writeHeaderErr error
	writeErr       error
	closeErr       error
}

func (w fakeTarWriter) WriteHeader(*tar.Header) error {
	return w.writeHeaderErr
}

func (w fakeTarWriter) Write(p []byte) (int, error) {
	if w.writeErr != nil {
		return 0, w.writeErr
	}

	return len(p), nil
}

func (w fakeTarWriter) Close() error {
	return w.closeErr
}

type fakeZstdWriter struct {
	writeErr error
	closeErr error
}

func (w fakeZstdWriter) Write(p []byte) (int, error) {
	if w.writeErr != nil {
		return 0, w.writeErr
	}

	return len(p), nil
}

func (w fakeZstdWriter) Close() error {
	return w.closeErr
}

type fakeReadCloser struct {
	io.Reader
	closeErr error
}

func (r fakeReadCloser) Close() error {
	return r.closeErr
}

type fakeWriteCloser struct {
	closeErr error
}

func (w fakeWriteCloser) Write(p []byte) (int, error) {
	return len(p), nil
}

func (w fakeWriteCloser) Close() error {
	return w.closeErr
}

func testZstdBytes(t *testing.T, data []byte) []byte {
	t.Helper()
	var zbuf bytes.Buffer
	zw, err := zstd.NewWriter(&zbuf)
	if err != nil {
		t.Fatalf("zstd writer: %v", err)
	}
	if _, err := zw.Write(data); err != nil {
		t.Fatalf("zstd write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zstd close: %v", err)
	}

	return zbuf.Bytes()
}

func testTarZstdPartial(t *testing.T, header tar.Header) []byte {
	t.Helper()
	var tarbuf bytes.Buffer
	tw := tar.NewWriter(&tarbuf)
	if err := tw.WriteHeader(&header); err != nil {
		t.Fatalf("write partial header: %v", err)
	}

	return testZstdBytes(t, tarbuf.Bytes())
}

const faultSQLDriverName = "hermes_state_store_fault"

func init() {
	sql.Register(faultSQLDriverName, faultSQLDriver{})
}

func openFaultSQL(t *testing.T, scenario string) *sql.DB {
	t.Helper()
	db, err := sql.Open(faultSQLDriverName, scenario)
	if err != nil {
		t.Fatal(err)
	}

	return db
}

type faultSQLDriver struct{}

func (faultSQLDriver) Open(name string) (driver.Conn, error) {
	return &faultSQLConn{scenario: name}, nil
}

type faultSQLConn struct {
	scenario string
	exec     int
}

func (c *faultSQLConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare unsupported")
}

func (c *faultSQLConn) Close() error {
	return nil
}

func (c *faultSQLConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions unsupported")
}

func (c *faultSQLConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.exec++
	switch {
	case c.scenario == "scrub-exec-error":
		return nil, errors.New("exec failed")
	case c.scenario == "vacuum-into-error" && strings.HasPrefix(query, "VACUUM INTO"):
		return nil, errors.New("vacuum into failed")
	case c.scenario == "scrub-delete-error" && strings.HasPrefix(query, "DELETE FROM"):
		return nil, errors.New("delete failed")
	case c.scenario == "scrub-vacuum-error" && query == "VACUUM":
		return nil, errors.New("vacuum failed")
	case c.scenario == "scrub-final-checkpoint-error" && query == "PRAGMA wal_checkpoint(TRUNCATE)" && c.exec > 4:
		return nil, errors.New("final checkpoint failed")
	default:
		return driver.RowsAffected(0), nil
	}
}

func (c *faultSQLConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	switch {
	case strings.HasPrefix(query, "SELECT name"):
		switch c.scenario {
		case "credential-query-error":
			return nil, errors.New("credential query failed")
		case "credential-scan-error":
			return &faultRows{columns: []string{"name", "extra"}, rows: [][]driver.Value{{"account", "x"}}}, nil
		case "credential-rows-error":
			return &faultRows{columns: []string{"name"}, nextErr: errors.New("credential rows failed")}, nil
		case "table-query-error":
			return &faultRows{columns: []string{"name"}, rows: [][]driver.Value{{"regular"}}}, nil
		case "scrub-delete-error", "scrub-vacuum-error":
			return &faultRows{columns: []string{"name"}, rows: [][]driver.Value{{"account"}}}, nil
		default:
			return &faultRows{columns: []string{"name"}}, nil
		}
	case strings.HasPrefix(query, "PRAGMA table_info"):
		switch c.scenario {
		case "table-query-error":
			return nil, errors.New("table query failed")
		case "table-scan-error":
			return &faultRows{
				columns: []string{"cid", "name", "type", "notnull", "dflt_value", "pk", "extra"},
				rows:    [][]driver.Value{{int64(0), "body", "TEXT", int64(0), nil, int64(0), "x"}},
			}, nil
		case "table-rows-error":
			return &faultRows{columns: sqliteTableInfoColumns(), nextErr: errors.New("table rows failed")}, nil
		case "table-sensitive-column":
			return &faultRows{
				columns: sqliteTableInfoColumns(),
				rows:    [][]driver.Value{{int64(0), "access_token", "TEXT", int64(0), nil, int64(0)}},
			}, nil
		default:
			return &faultRows{
				columns: sqliteTableInfoColumns(),
				rows:    [][]driver.Value{{int64(0), "body", "TEXT", int64(0), nil, int64(0)}},
			}, nil
		}
	default:
		return &faultRows{columns: []string{"ignored"}}, nil
	}
}

type faultRows struct {
	columns []string
	rows    [][]driver.Value
	nextErr error
	index   int
}

func (r *faultRows) Columns() []string {
	return r.columns
}

func (r *faultRows) Close() error {
	return nil
}

func (r *faultRows) Next(dest []driver.Value) error {
	if r.nextErr != nil {
		err := r.nextErr
		r.nextErr = nil

		return err
	}
	if r.index >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.index])
	r.index++

	return nil
}

func sqliteTableInfoColumns() []string {
	return []string{"cid", "name", "type", "notnull", "dflt_value", "pk"}
}

func seedSQLiteStore(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, stmt := range []string{
		`CREATE TABLE account (id TEXT PRIMARY KEY, access_token TEXT, refresh_token TEXT)`,
		`CREATE TABLE credential (id TEXT PRIMARY KEY, value TEXT)`,
		`CREATE TABLE message (id TEXT PRIMARY KEY, body TEXT)`,
		`INSERT INTO account (id, access_token, refresh_token) VALUES ('acct', 'token', 'refresh')`,
		`INSERT INTO credential (id, value) VALUES ('cred', 'secret')`,
		`INSERT INTO message (id, body) VALUES ('msg', 'kept')`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("exec %q: %v", stmt, err)
		}
	}
}

func countSQLiteRows(t *testing.T, path string, table string) int {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow("SELECT count(*) FROM " + quoteSQLiteIdent(table)).Scan(&count); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}

	return count
}

type errorSessionStore struct {
	err error
}

func (s *errorSessionStore) Append(context.Context, SessionKey, []SessionStoreEntry) error {
	return s.err
}

func (s *errorSessionStore) Load(context.Context, SessionKey) ([]SessionStoreEntry, error) {
	return nil, s.err
}

func (s *errorSessionStore) Replace(context.Context, SessionKey, []SessionStoreReplacement) error {
	return s.err
}

func (s *errorSessionStore) Delete(context.Context, SessionKey) error {
	return s.err
}

func (s *errorSessionStore) ListSessions(context.Context) ([]SessionSummary, error) {
	return nil, s.err
}

func (s *errorSessionStore) ListSubkeys(context.Context, SessionKey) ([]string, error) {
	return nil, s.err
}

func testTarZstd(t *testing.T, headers []tar.Header, bodies map[string]string) []byte {
	t.Helper()
	var tarbuf bytes.Buffer
	tw := tar.NewWriter(&tarbuf)
	for _, header := range headers {
		if err := tw.WriteHeader(&header); err != nil {
			t.Fatalf("write header: %v", err)
		}
		if body := bodies[header.Name]; body != "" {
			if _, err := io.WriteString(tw, body); err != nil {
				t.Fatalf("write body: %v", err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}
	var zbuf bytes.Buffer
	zw, err := zstd.NewWriter(&zbuf)
	if err != nil {
		t.Fatalf("zstd writer: %v", err)
	}
	if _, err := zw.Write(tarbuf.Bytes()); err != nil {
		t.Fatalf("zstd write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zstd close: %v", err)
	}

	return zbuf.Bytes()
}

func TestSnapshotJournalAndStoreReconciliationEdges(t *testing.T) {
	t.Run("journal preparation", func(t *testing.T) {
		agent := newTestAgent(WithSessionStore(NewInMemorySessionStore()))
		session := testSession(agent, newFakeHermesClient())
		journal := newTestSessionOperationJournalWithLogical(t, t.TempDir(), sessionOperationKindNew, string(session.id))
		identifyTestNewSessionOperationJournal(t, journal)
		session.operationJournal = journal
		previous := sessionOperationCreateTemp
		sessionOperationCreateTemp = func(string, string) (sessionOperationFile, error) { return nil, errors.New("prepare") }
		t.Cleanup(func() { sessionOperationCreateTemp = previous })
		if err := session.snapshotToStore(t.Context()); err == nil {
			t.Fatal("journal preparation failure ignored")
		}
	})

	t.Run("commit marker retained", func(t *testing.T) {
		agent := newTestAgent(WithSessionStore(NewInMemorySessionStore()))
		session := testSession(agent, newFakeHermesClient())
		journal := newTestSessionOperationJournalWithLogical(t, t.TempDir(), sessionOperationKindNew, string(session.id))
		identifyTestNewSessionOperationJournal(t, journal)
		session.operationJournal = journal
		previous := sessionOperationRename
		sessionOperationRename = func(source, target string) error {
			if filepath.Base(target) == sessionOperationJournalName && journal.record.Phase == sessionOperationPhaseStoreCommitted {
				return errors.New("commit marker")
			}

			return previous(source, target)
		}
		t.Cleanup(func() { sessionOperationRename = previous })
		if err := session.snapshotToStore(t.Context()); err != nil {
			t.Fatalf("committed Store blocked by journal marker: %v", err)
		}
	})

	store := sessionOperationFaultStore{base: NewInMemorySessionStore(), listSubkeysErr: errors.New("list")}
	_, _, err := reconcileSessionStoreReplacement(t.Context(), store, SessionKey{SessionID: "logical"}, []SessionStoreReplacement{{
		Key: SessionKey{SessionID: "logical"}, Entries: []SessionStoreEntry{json.RawMessage(`{"main":true}`)},
	}})
	if err == nil {
		t.Fatal("replacement subkey listing failure ignored")
	}
}

func TestLifecycleSnapshotCaptureFailureBoundaries(t *testing.T) {
	t.Run("closed turn", func(t *testing.T) {
		session := testSession(newTestAgent(), newFakeHermesClient())
		session.closed = true
		_, err := session.captureSnapshotLocked(t.Context(), &terminalSnapshotRequirement{})
		require.ErrorContains(t, err, "closed")
	})

	t.Run("todo fallback", func(t *testing.T) {
		client := newFakeHermesClient()
		client.todosErr = errors.New("todos unavailable")
		session := testSession(newTestAgent(), client)
		session.committed.todos = []nativehermes.Todo{{Content: "committed"}}
		commit, err := session.captureSnapshotLocked(t.Context(), nil)
		require.NoError(t, err)
		require.Equal(t, session.committed.todos, commit.todos)
	})

	t.Run("settled archive read", func(t *testing.T) {
		storeErr := errors.New("archive unavailable")
		agent := newTestAgent(WithSessionStore(&errorSessionStore{err: storeErr}))
		session := testSession(agent, newFakeHermesClient())
		requirement := &terminalSnapshotRequirement{
			nativeUnavailable: true,
			settlementCapture: true,
			foreground: stateSnapshotForeground{
				StreamID: "stream", TurnID: "turn", CapturedAtUnixMilli: 1,
				Outcome: string(lifecycle.OutcomeFailed),
			},
		}
		_, err := session.captureSnapshotLocked(t.Context(), requirement)
		require.ErrorIs(t, err, storeErr)
	})

	t.Run("cancel after serialization", func(t *testing.T) {
		session := testSession(newTestAgent(), newFakeHermesClient())
		ctx, cancel := context.WithCancel(t.Context())
		originalMarshal := stateJSONMarshal
		t.Cleanup(func() { stateJSONMarshal = originalMarshal })
		calls := 0
		stateJSONMarshal = func(value any) ([]byte, error) {
			calls++
			encoded, err := json.Marshal(value)
			if calls == 2 {
				cancel()
			}

			return encoded, err
		}

		_, err := session.captureSnapshotLocked(ctx, nil)
		require.ErrorIs(t, err, context.Canceled)
	})
}

// TestShippedResumeExampleFixtureHydrates reads the fixture the resume example
// ships and drives it through session/load's own reader. The example's README
// and the get-started guide both promise that file loads as-is, and nothing but
// this test stands between that promise and a store-shape change: the fixture is
// data, so no compiler notices when the shape it was written in retires.
func TestShippedResumeExampleFixtureHydrates(t *testing.T) {
	const sessionID = "7f3a2b1c-9d0e-4a21-8b6c-1f0c5d6e7a80"

	data, err := os.ReadFile(filepath.Join("examples", "resume-from-file", "session.jsonl"))
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	require.Len(t, lines, 2, "the fixture is one main snapshot and one id mapping")

	mainEntry := SessionStoreEntry(lines[0])
	idmapEntry := SessionStoreEntry(lines[1])

	store := NewInMemorySessionStore()
	mainKey := SessionKey{SessionID: sessionID, Subpath: SessionStoreMainSubpath}
	require.NoError(t, store.Replace(t.Context(), mainKey, []SessionStoreReplacement{
		{Key: mainKey, Entries: []SessionStoreEntry{mainEntry}},
		{Key: SessionKey{SessionID: sessionID, Subpath: idmapSubpath}, Entries: []SessionStoreEntry{idmapEntry}},
	}))

	idmap, snapshot, ok, err := hydrateStateFromStore(t.Context(), store, sessionID, nativehermes.XDGDirs{Root: t.TempDir()})
	require.NoError(t, err)
	require.True(t, ok, "session/load would answer unknown_session for the shipped fixture")
	require.Equal(t, sessionID, idmap.SessionID)
	require.Equal(t, idmap.NativeSessionID, snapshot.Session.NativeSessionID)

	// A shipped fixture cannot name a directory that exists on the reader's
	// machine, and load refuses a snapshot whose cwd disagrees with the request.
	require.Empty(t, snapshot.Session.Cwd, "a shipped fixture binds no cwd")

	terminal, err := InspectSessionStoreTerminalState(sessionID, []SessionStoreEntry{mainEntry})
	require.NoError(t, err)
	require.Equal(t, SessionStoreTerminalState{}, terminal, "the fixture settles no turn of its own")
}
