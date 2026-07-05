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
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestSnapshotHydrateScrubsSQLiteCredentialTables(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	xdg, err := createXDGDirs(root, "session-1")
	if err != nil {
		t.Fatalf("createXDGDirs: %v", err)
	}
	dbPath := filepath.Join(xdg.Data, "hermes", "hermes.db")
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o700); err != nil {
		t.Fatal(err)
	}
	seedSQLiteStore(t, dbPath)

	store := NewInMemorySessionStore()
	client := newFakeHermesClient()
	client.xdg = xdg
	client.todos = []nativeTodo{{ID: "todo-1", Content: "Remember", Status: "pending", Priority: "medium"}}
	agent := NewAgent(WithSessionStore(store))
	session := testSession(agent, client)
	if err := session.snapshotToStore(ctx); err != nil {
		t.Fatalf("snapshotToStore: %v", err)
	}

	if countSQLiteRows(t, dbPath, "account") != 1 || countSQLiteRows(t, dbPath, "credential") != 1 {
		t.Fatal("snapshot modified live credential tables")
	}

	if err := os.RemoveAll(xdg.Root); err != nil {
		t.Fatalf("remove original xdg: %v", err)
	}
	restored, err := createXDGDirs(root, "session-1-restored")
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
	restoredDB := filepath.Join(restored.Data, "hermes", "hermes.db")
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

func TestDecodeXDGArchiveRejectsTraversalAndBadChecksum(t *testing.T) {
	ctx := context.Background()
	store := NewInMemorySessionStore()
	main := SessionKey{SessionID: "s", Subpath: SessionStoreMainSubpath}
	idmapData, _ := json.Marshal(idmapRecord{SessionID: "s", NativeSessionID: "n", Format: SessionStoreFormat})
	mainData, _ := json.Marshal(validHydrateSnapshot())
	badArchive, _ := json.Marshal(archiveEntry{Format: SessionStoreFormat, Encoding: "tar+zstd+base64", SHA256: "bad", Data: base64.StdEncoding.EncodeToString([]byte("not zstd"))})
	if err := store.Replace(ctx, main, []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{mainData}},
		{Key: SessionKey{SessionID: "s", Subpath: idmapSubpath}, Entries: []SessionStoreEntry{idmapData}},
		{Key: SessionKey{SessionID: "s", Subpath: xdgDataSubpath}, Entries: []SessionStoreEntry{badArchive}},
		{Key: SessionKey{SessionID: "s", Subpath: xdgConfigSubpath}, Entries: []SessionStoreEntry{badArchive}},
		{Key: SessionKey{SessionID: "s", Subpath: xdgCacheSubpath}, Entries: []SessionStoreEntry{badArchive}},
		{Key: SessionKey{SessionID: "s", Subpath: xdgStateSubpath}, Entries: []SessionStoreEntry{badArchive}},
	}); err != nil {
		t.Fatalf("Replace: %v", err)
	}
	_, _, ok, err := hydrateStateFromStore(ctx, store, "s", xdgDirs{
		Data:   filepath.Join(t.TempDir(), "data"),
		Config: filepath.Join(t.TempDir(), "config"),
		Cache:  filepath.Join(t.TempDir(), "cache"),
		State:  filepath.Join(t.TempDir(), "state"),
	})
	if err == nil || ok {
		t.Fatal("bad archive checksum accepted")
	}

	if !shouldExcludeStatePath("nested/auth.json") || !shouldExcludeStatePath("x/credential-store.json") || !shouldExcludeStatePath("server.lease") {
		t.Fatal("credential state path exclusion failed")
	}
	if !shouldSkipSQLiteCompanion("hermes.db-wal") || shouldSkipSQLiteCompanion("hermes.db") {
		t.Fatal("SQLite companion detection failed")
	}
	if !sensitiveSQLiteName("access_token") || quoteSQLiteIdent(`a"b`) != `"a""b"` || pathBase("a/b/c") != "c" {
		t.Fatal("SQLite helper checks failed")
	}
}

func TestStateStoreArchiveRoundTripAndHelpers(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	for path, body := range map[string]string{
		filepath.Join(root, "nested", "file.txt"): "body",
		filepath.Join(root, "auth.json"):          "secret",
		filepath.Join(root, "cache.db-wal"):       "wal",
	} {
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	archive, sha, err := encodeXDGArchive(root)
	if err != nil {
		t.Fatalf("encodeXDGArchive: %v", err)
	}
	sum := sha256.Sum256(archive)
	if sha != hex.EncodeToString(sum[:]) {
		t.Fatalf("sha = %q", sha)
	}
	target := t.TempDir()
	if err := decodeXDGArchive(archive, target); err != nil {
		t.Fatalf("decodeXDGArchive: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(target, "nested", "file.txt"))
	if err != nil || string(data) != "body" {
		t.Fatalf("decoded file = %q err=%v", data, err)
	}
	if _, err := os.Stat(filepath.Join(target, "auth.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("auth.json restored err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(target, "cache.db-wal")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("wal restored err=%v", err)
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

func TestHydrateStateFromStoreErrors(t *testing.T) {
	ctx := context.Background()
	xdg, err := createXDGDirs(t.TempDir(), "hydrate")
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
	mainData, _ := json.Marshal(validHydrateSnapshot())
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

func TestHydrateStateAgreementRejectsMismatches(t *testing.T) {
	ctx := context.Background()
	xdg, err := createXDGDirs(t.TempDir(), "hydrate")
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
	if err := (&session{agent: NewAgent(), client: nil}).snapshotToStore(context.Background()); err != nil {
		t.Fatalf("nil client snapshot: %v", err)
	}
	if _, _, err := encodeXDGArchive(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("encodeXDGArchive accepted missing root")
	}
	if _, ok, err := sqliteArchiveContent(filepath.Join(t.TempDir(), "missing.db")); err == nil || ok {
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
	if _, err := db.Exec(`CREATE TABLE regular (id TEXT PRIMARY KEY, body TEXT)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
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

	t.Run("archive encode error", func(t *testing.T) {
		restoreStateStoreSeams(t)
		stateWalkDir = func(string, fs.WalkDirFunc) error {
			return errors.New("walk failed")
		}
		if err := snapshotFaultSession(t).snapshotToStore(ctx); err == nil {
			t.Fatal("snapshot ignored archive error")
		}
	})

	for name, failAt := range map[string]int{
		"archive entry": 1,
		"main entry":    5,
		"idmap entry":   6,
	} {
		t.Run("marshal "+name, func(t *testing.T) {
			restoreStateStoreSeams(t)
			calls := 0
			stateJSONMarshal = func(value any) ([]byte, error) {
				calls++
				if calls == failAt {
					return nil, errors.New("marshal failed")
				}
				return json.Marshal(value)
			}
			if err := snapshotFaultSession(t).snapshotToStore(ctx); err == nil {
				t.Fatalf("snapshot ignored %s marshal error", name)
			}
		})
	}
}

func TestHydrateStateFromStoreFaults(t *testing.T) {
	ctx := context.Background()
	xdg, err := createXDGDirs(t.TempDir(), "hydrate")
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
				replaceArchiveEntry(t, ctx, store, xdgDataSubpath, json.RawMessage(`{`))
			},
			"base64": func(store *InMemorySessionStore) {
				replaceArchiveEntry(t, ctx, store, xdgDataSubpath, mustStateJSON(t, archiveEntry{Format: SessionStoreFormat, Data: "not base64"}))
			},
			"decode": func(store *InMemorySessionStore) {
				data := []byte("not zstd")
				sum := sha256.Sum256(data)
				replaceArchiveEntry(t, ctx, store, xdgDataSubpath, mustStateJSON(t, archiveEntry{
					Format: SessionStoreFormat,
					SHA256: hex.EncodeToString(sum[:]),
					Data:   base64.StdEncoding.EncodeToString(data),
				}))
			},
		} {
			t.Run(name, func(t *testing.T) {
				store := validHydrateStore(t, ctx)
				if name == "load" {
					errStore := selectiveLoadErrorStore{
						SessionStore: store,
						key:          SessionKey{SessionID: "s", Subpath: xdgDataSubpath},
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

func TestEncodeXDGArchiveFaults(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("body"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "credential-dir"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "credential-dir", "secret.txt"), []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	archive, _, err := encodeXDGArchive(root)
	if err != nil {
		t.Fatalf("encode credential dir: %v", err)
	}
	target := t.TempDir()
	if err := decodeXDGArchive(archive, target); err != nil {
		t.Fatalf("decode credential dir archive: %v", err)
	}
	if _, err := os.Stat(filepath.Join(target, "credential-dir")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("credential dir restored err = %v", err)
	}

	tests := map[string]func(){
		"walk entry": func() {
			stateWalkDir = func(root string, fn fs.WalkDirFunc) error {
				return fn(filepath.Join(root, "bad"), nil, errors.New("walk entry failed"))
			}
		},
		"rel": func() {
			stateRel = func(string, string) (string, error) { return "", errors.New("rel failed") }
		},
		"lstat": func() {
			stateLstat = func(string) (os.FileInfo, error) { return nil, errors.New("lstat failed") }
		},
		"header": func() {
			stateFileInfoHeader = func(os.FileInfo, string) (*tar.Header, error) { return nil, errors.New("header failed") }
		},
		"sqlite content": func() {
			stateSQLiteArchiveContent = func(string) ([]byte, bool, error) {
				return nil, false, errors.New("sqlite failed")
			}
		},
		"write header": func() {
			stateNewTarWriter = func(io.Writer) archiveTarWriter {
				return fakeTarWriter{writeHeaderErr: errors.New("write header failed")}
			}
		},
		"write scrubbed": func() {
			stateSQLiteArchiveContent = func(string) ([]byte, bool, error) { return []byte("scrubbed"), true, nil }
			stateNewTarWriter = func(io.Writer) archiveTarWriter {
				return fakeTarWriter{writeErr: errors.New("write failed")}
			}
		},
		"open": func() {
			stateSQLiteArchiveContent = func(string) ([]byte, bool, error) { return nil, false, nil }
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
			if _, _, err := encodeXDGArchive(root); err == nil {
				t.Fatal("encodeXDGArchive ignored injected error")
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

func TestSQLiteArchiveAndCopyFaults(t *testing.T) {
	tests := map[string]func(string){
		"mkdir temp": func(string) {
			stateMkdirTemp = func(string, string) (string, error) { return "", errors.New("mkdir temp failed") }
		},
		"copy db": func(string) {
			stateCopyFile = func(string, string, os.FileMode) error { return errors.New("copy failed") }
		},
		"copy companion": func(dbPath string) {
			if err := os.WriteFile(dbPath+"-wal", []byte("wal"), 0o600); err != nil {
				t.Fatal(err)
			}
			calls := 0
			stateCopyFile = func(source string, target string, mode os.FileMode) error {
				calls++
				if calls > 1 {
					return errors.New("copy companion failed")
				}
				return copyFile(source, target, mode)
			}
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
			if _, ok, err := sqliteArchiveContent(dbPath); err == nil || ok {
				t.Fatal("sqliteArchiveContent ignored injected error")
			}
		})
	}

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
	xdg, err := createXDGDirs(root, "session-1")
	if err != nil {
		t.Fatal(err)
	}
	client := newFakeHermesClient()
	client.xdg = xdg
	agent := NewAgent(WithSessionStore(NewInMemorySessionStore()))
	return testSession(agent, client)
}

func validHydrateStore(t *testing.T, ctx context.Context) *InMemorySessionStore {
	t.Helper()
	store := NewInMemorySessionStore()
	data := testTarZstd(t, nil, nil)
	sum := sha256.Sum256(data)
	archive := mustStateJSON(t, archiveEntry{
		Format: SessionStoreFormat,
		SHA256: hex.EncodeToString(sum[:]),
		Data:   base64.StdEncoding.EncodeToString(data),
	})
	main := SessionKey{SessionID: "s", Subpath: SessionStoreMainSubpath}
	replacements := make([]SessionStoreReplacement, 0, 6)
	replacements = append(replacements,
		SessionStoreReplacement{Key: main, Entries: []SessionStoreEntry{mustStateJSON(t, validHydrateSnapshot())}},
		SessionStoreReplacement{Key: SessionKey{SessionID: "s", Subpath: idmapSubpath}, Entries: []SessionStoreEntry{mustStateJSON(t, validHydrateIDMap())}},
	)
	for _, subpath := range []string{xdgDataSubpath, xdgConfigSubpath, xdgCacheSubpath, xdgStateSubpath} {
		replacements = append(replacements, SessionStoreReplacement{
			Key:     SessionKey{SessionID: "s", Subpath: subpath},
			Entries: []SessionStoreEntry{archive},
		})
	}
	if err := store.Replace(ctx, main, replacements); err != nil {
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
		Format: SessionStoreFormat,
		Session: stateSnapshotSession{
			SessionID:       "s",
			NativeSessionID: "n",
		},
	}
}

func replaceHydrateRecords(t *testing.T, ctx context.Context, store *InMemorySessionStore, idmap idmapRecord, snapshot stateSnapshot) {
	t.Helper()
	main := SessionKey{SessionID: "s", Subpath: SessionStoreMainSubpath}
	replacements := make([]SessionStoreReplacement, 0, 6)
	replacements = append(replacements,
		SessionStoreReplacement{Key: main, Entries: []SessionStoreEntry{mustStateJSON(t, snapshot)}},
		SessionStoreReplacement{Key: SessionKey{SessionID: "s", Subpath: idmapSubpath}, Entries: []SessionStoreEntry{mustStateJSON(t, idmap)}},
	)
	for _, candidate := range []string{xdgDataSubpath, xdgConfigSubpath, xdgCacheSubpath, xdgStateSubpath} {
		entries, err := store.Load(ctx, SessionKey{SessionID: "s", Subpath: candidate})
		if err != nil {
			t.Fatal(err)
		}
		replacements = append(replacements, SessionStoreReplacement{
			Key:     SessionKey{SessionID: "s", Subpath: candidate},
			Entries: entries,
		})
	}
	if err := store.Replace(ctx, main, replacements); err != nil {
		t.Fatal(err)
	}
}

func replaceArchiveEntry(t *testing.T, ctx context.Context, store *InMemorySessionStore, subpath string, entry SessionStoreEntry) {
	t.Helper()
	main := SessionKey{SessionID: "s", Subpath: SessionStoreMainSubpath}
	replacements := make([]SessionStoreReplacement, 0, 6)
	replacements = append(replacements,
		SessionStoreReplacement{Key: main, Entries: []SessionStoreEntry{mustStateJSON(t, validHydrateSnapshot())}},
		SessionStoreReplacement{Key: SessionKey{SessionID: "s", Subpath: idmapSubpath}, Entries: []SessionStoreEntry{mustStateJSON(t, validHydrateIDMap())}},
	)
	for _, candidate := range []string{xdgDataSubpath, xdgConfigSubpath, xdgCacheSubpath, xdgStateSubpath} {
		entries, err := store.Load(ctx, SessionKey{SessionID: "s", Subpath: candidate})
		if err != nil {
			t.Fatal(err)
		}
		if candidate == subpath {
			entries = []SessionStoreEntry{entry}
		}
		replacements = append(replacements, SessionStoreReplacement{
			Key:     SessionKey{SessionID: "s", Subpath: candidate},
			Entries: entries,
		})
	}
	if err := store.Replace(ctx, main, replacements); err != nil {
		t.Fatal(err)
	}
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
