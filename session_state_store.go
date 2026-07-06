//nolint:tagliatelle // Store metadata preserves Hermes native modelID/providerID spellings.
package hermesacp

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	_ "modernc.org/sqlite"
)

const maxHydrateFileBytes int64 = 128 * 1024 * 1024

type archiveTarWriter interface {
	io.Writer
	WriteHeader(*tar.Header) error
	Close() error
}

type archiveZstdWriter interface {
	io.Writer
	Close() error
}

var (
	stateJSONMarshal    = json.Marshal
	stateWalkDir        = filepath.WalkDir
	stateRel            = filepath.Rel
	stateLstat          = os.Lstat
	stateFileInfoHeader = tar.FileInfoHeader
	stateNewTarWriter   = func(w io.Writer) archiveTarWriter { return tar.NewWriter(w) }
	stateOpen           = func(name string) (io.ReadCloser, error) { return os.Open(name) }
	stateCopy           = io.Copy
	stateNewZstdWriter  = func(w io.Writer) (archiveZstdWriter, error) {
		return zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	}
	stateNewZstdReader = func(r io.Reader) (*zstd.Decoder, error) { return zstd.NewReader(r) }
	stateRemoveAll     = os.RemoveAll
	stateMkdirAll      = os.MkdirAll
	stateAbs           = filepath.Abs
	stateOpenFile      = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
		return os.OpenFile(name, flag, perm)
	}
	stateCopyN                       = io.CopyN
	stateMkdirTemp                   = os.MkdirTemp
	stateStat                        = os.Stat
	stateReadFile                    = os.ReadFile
	stateCopyFile                    = copyFile
	stateSQLiteArchiveContent        = sqliteArchiveContent
	stateScrubSQLiteCredentialTables = scrubSQLiteCredentialTables
	stateSQLOpen                     = sql.Open
)

type idmapRecord struct {
	SessionID             string `json:"sessionId"`
	NativeSessionID       string `json:"nativeSessionId"`
	ParentSessionID       string `json:"parentSessionId,omitempty"`
	NativeParentSessionID string `json:"nativeParentSessionId,omitempty"`
	Format                string `json:"format"`
	CreatedAtUnixMilli    int64  `json:"createdAtUnixMilli"`
	UpdatedAtUnixMilli    int64  `json:"updatedAtUnixMilli"`
}

type stateSnapshot struct {
	Format              string                 `json:"format"`
	CapturedAtUnixMilli int64                  `json:"capturedAtUnixMilli"`
	Session             stateSnapshotSession   `json:"session"`
	Archives            map[string]archiveInfo `json:"archives"`
	Wrapper             stateSnapshotWrapper   `json:"wrapper"`
}

type stateSnapshotSession struct {
	SessionID             string             `json:"sessionId"`
	NativeSessionID       string             `json:"nativeSessionId"`
	ParentSessionID       string             `json:"parentSessionId,omitempty"`
	NativeParentSessionID string             `json:"nativeParentSessionId,omitempty"`
	Cwd                   string             `json:"cwd"`
	Title                 string             `json:"title"`
	Model                 stateSnapshotModel `json:"model"`
}

type stateSnapshotModel struct {
	ProviderID string `json:"providerID,omitempty"`
	ModelID    string `json:"modelID,omitempty"`
	Agent      string `json:"agent,omitempty"`
}

type archiveInfo struct {
	Subpath string `json:"subpath"`
	SHA256  string `json:"sha256"`
	Bytes   int    `json:"bytes"`
}

type stateSnapshotWrapper struct {
	Todos              []nativeTodo `json:"todos"`
	PermissionsHistory []any        `json:"permissionsHistory"`
	PendingInput       bool         `json:"pendingInput"`
}

type archiveEntry struct {
	Format   string `json:"format"`
	Encoding string `json:"encoding"`
	Sequence int    `json:"sequence"`
	Final    bool   `json:"final"`
	SHA256   string `json:"sha256"`
	Data     string `json:"data"`
}

func (s *session) snapshotToStore(ctx context.Context) error {
	if err := s.ensureNotPoisoned(); err != nil {
		return err
	}

	if reason := s.snapshotBlockedReason(); reason != "" {
		return fmt.Errorf("cannot snapshot Hermes session while %s pending", reason)
	}

	snapshot := s.snapshot()
	if snapshot.client == nil {
		return nil
	}

	idmap := snapshot.idmap
	now := time.Now().UnixMilli()

	idmap.UpdatedAtUnixMilli = now
	if idmap.CreatedAtUnixMilli == 0 {
		idmap.CreatedAtUnixMilli = now
	}

	idmap.Format = SessionStoreFormat

	todos, _ := snapshot.client.Todos(ctx, idmap.NativeSessionID)
	main := stateSnapshot{
		Format:              SessionStoreFormat,
		CapturedAtUnixMilli: now,
		Session: stateSnapshotSession{
			SessionID:             idmap.SessionID,
			NativeSessionID:       idmap.NativeSessionID,
			ParentSessionID:       idmap.ParentSessionID,
			NativeParentSessionID: idmap.NativeParentSessionID,
			Cwd:                   snapshot.cwd,
			Title:                 snapshot.title,
			Model: stateSnapshotModel{
				ProviderID: snapshot.providerID,
				ModelID:    snapshot.modelID,
				Agent:      snapshot.mode,
			},
		},
		Archives: map[string]archiveInfo{},
		Wrapper: stateSnapshotWrapper{
			Todos:        todos,
			PendingInput: false,
		},
	}

	replacements := []SessionStoreReplacement{}
	mainKey := SessionKey{SessionID: string(s.id), Subpath: SessionStoreMainSubpath}

	xdg := snapshot.client.XDGDirs()
	if archive, sha, ok, err := encodeHermesStateDBArchive(xdg.Root); err != nil {
		return err
	} else if ok {
		main.Archives["state-db"] = archiveInfo{
			Subpath: stateDBSubpath,
			SHA256:  sha,
			Bytes:   len(archive),
		}

		entry, err := stateJSONMarshal(archiveEntry{
			Format:   SessionStoreFormat,
			Encoding: archiveEncodingTarZstdBase64,
			Sequence: 0,
			Final:    true,
			SHA256:   sha,
			Data:     base64.StdEncoding.EncodeToString(archive),
		})
		if err != nil {
			return err
		}

		replacements = append(replacements, SessionStoreReplacement{
			Key:     SessionKey{SessionID: string(s.id), Subpath: stateDBSubpath},
			Entries: []SessionStoreEntry{entry},
		})
	}

	mainEntry, err := stateJSONMarshal(main)
	if err != nil {
		return err
	}

	idmapEntry, err := stateJSONMarshal(idmap)
	if err != nil {
		return err
	}

	replacements = append(replacements,
		SessionStoreReplacement{Key: mainKey, Entries: []SessionStoreEntry{mainEntry}},
		SessionStoreReplacement{Key: SessionKey{SessionID: string(s.id), Subpath: idmapSubpath}, Entries: []SessionStoreEntry{idmapEntry}},
	)

	storeCtx, cancel := s.agent.sessionStoreContext(ctx)
	defer cancel()

	return s.agent.sessionStore().Replace(storeCtx, mainKey, replacements)
}

func hydrateStateFromStore(ctx context.Context, store SessionStore, sessionID string, xdg xdgDirs) (idmapRecord, stateSnapshot, bool, error) {
	idEntries, err := store.Load(ctx, SessionKey{SessionID: sessionID, Subpath: idmapSubpath})
	if err != nil {
		return idmapRecord{}, stateSnapshot{}, false, err
	}

	mainEntries, err := store.Load(ctx, SessionKey{SessionID: sessionID, Subpath: SessionStoreMainSubpath})
	if err != nil {
		return idmapRecord{}, stateSnapshot{}, false, err
	}

	if len(idEntries) == 0 || len(mainEntries) == 0 {
		return idmapRecord{}, stateSnapshot{}, false, nil
	}

	var idmap idmapRecord
	if err := json.Unmarshal(idEntries[len(idEntries)-1], &idmap); err != nil {
		return idmapRecord{}, stateSnapshot{}, false, err
	}

	var snapshot stateSnapshot
	if err := json.Unmarshal(mainEntries[len(mainEntries)-1], &snapshot); err != nil {
		return idmapRecord{}, stateSnapshot{}, false, err
	}

	if idmap.Format != SessionStoreFormat || snapshot.Format != SessionStoreFormat {
		return idmapRecord{}, stateSnapshot{}, false, fmt.Errorf("unsupported hermes store format")
	}

	if err := validateHydratedStateAgreement(sessionID, idmap, snapshot); err != nil {
		return idmapRecord{}, stateSnapshot{}, false, err
	}

	if _, ok := snapshot.Archives["state-db"]; ok {
		entries, err := store.Load(ctx, SessionKey{SessionID: sessionID, Subpath: stateDBSubpath})
		if err != nil {
			return idmapRecord{}, stateSnapshot{}, false, err
		}

		if len(entries) == 0 {
			return idmapRecord{}, stateSnapshot{}, false, fmt.Errorf("store missing archive %s", stateDBSubpath)
		}

		var archive archiveEntry
		if archiveErr := json.Unmarshal(entries[len(entries)-1], &archive); archiveErr != nil {
			return idmapRecord{}, stateSnapshot{}, false, archiveErr
		}

		if archive.Format != SessionStoreFormat || archive.Encoding != archiveEncodingTarZstdBase64 || !archive.Final {
			return idmapRecord{}, stateSnapshot{}, false, fmt.Errorf("invalid archive entry %s", stateDBSubpath)
		}

		data, err := base64.StdEncoding.DecodeString(archive.Data)
		if err != nil {
			return idmapRecord{}, stateSnapshot{}, false, err
		}

		sum := sha256.Sum256(data)
		if archive.SHA256 != "" && archive.SHA256 != hex.EncodeToString(sum[:]) {
			return idmapRecord{}, stateSnapshot{}, false, fmt.Errorf("archive checksum mismatch %s", stateDBSubpath)
		}

		if err := decodeXDGArchive(data, xdg.Root); err != nil {
			return idmapRecord{}, stateSnapshot{}, false, err
		}

		return idmap, snapshot, true, nil
	}

	return idmap, snapshot, true, nil
}

func (s *session) snapshotBlockedReason() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch {
	case s.turnInFlight || s.cancel != nil:
		return reasonTurn
	case len(s.pending) > 0:
		return reasonPermission
	case len(s.questions) > 0:
		return valElicitation
	case len(s.activeMessageIDs) > 0:
		return reasonGeneration
	default:
		return ""
	}
}

func encodeHermesStateDBArchive(root string) ([]byte, string, bool, error) {
	if root == "" {
		return nil, "", false, nil
	}

	type stateDBFile struct {
		name string
		data []byte
	}

	files := []stateDBFile{}

	stateDBPath := filepath.Join(root, fileStateDB)
	if info, err := stateLstat(stateDBPath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, "", false, err
		}
	} else if info.Mode().IsRegular() {
		if data, ok, err := stateSQLiteArchiveContent(stateDBPath); err != nil {
			return nil, "", false, err
		} else if ok {
			files = append(files, stateDBFile{name: fileStateDB, data: data})
		}
	}

	if len(files) == 0 {
		for _, name := range []string{fileStateDB, fileStateDBWAL, fileStateDBSHM} {
			path := filepath.Join(root, name)

			info, err := stateLstat(path)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					continue
				}

				return nil, "", false, err
			}

			if info.Mode().IsRegular() {
				files = append(files, stateDBFile{name: name})
			}
		}
	}

	if len(files) == 0 {
		return nil, "", false, nil
	}

	var tarbuf bytes.Buffer

	tw := stateNewTarWriter(&tarbuf)

	for _, item := range files {
		name := item.name
		path := filepath.Join(root, name)

		info, err := stateLstat(path)
		if err != nil {
			return nil, "", false, err
		}

		header, err := stateFileInfoHeader(info, "")
		if err != nil {
			return nil, "", false, err
		}

		header.Name = name
		header.Uid = 0
		header.Gid = 0
		header.Uname = ""
		header.Gname = ""
		header.ModTime = time.Unix(0, 0)
		header.AccessTime = time.Unix(0, 0)

		header.ChangeTime = time.Unix(0, 0)
		if item.data != nil {
			header.Size = int64(len(item.data))
		}

		if writeErr := tw.WriteHeader(header); writeErr != nil {
			return nil, "", false, writeErr
		}

		if item.data != nil {
			if _, writeErr := tw.Write(item.data); writeErr != nil {
				return nil, "", false, writeErr
			}

			continue
		}

		file, err := stateOpen(path)
		if err != nil {
			return nil, "", false, err
		}

		_, copyErr := stateCopy(tw, file)
		closeErr := file.Close()

		if copyErr != nil {
			return nil, "", false, copyErr
		}

		if closeErr != nil {
			return nil, "", false, closeErr
		}
	}

	if err := tw.Close(); err != nil {
		return nil, "", false, err
	}

	var zbuf bytes.Buffer

	zw, err := stateNewZstdWriter(&zbuf)
	if err != nil {
		return nil, "", false, err
	}

	if _, err := zw.Write(tarbuf.Bytes()); err != nil {
		zw.Close()

		return nil, "", false, err
	}

	if err := zw.Close(); err != nil {
		return nil, "", false, err
	}

	sum := sha256.Sum256(zbuf.Bytes())

	return zbuf.Bytes(), hex.EncodeToString(sum[:]), true, nil
}

func decodeXDGArchive(data []byte, target string) error {
	if err := stateRemoveAll(target); err != nil {
		return err
	}

	if err := stateMkdirAll(target, 0o700); err != nil {
		return err
	}

	zr, err := stateNewZstdReader(bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer zr.Close()

	tr := tar.NewReader(zr)

	cleanTarget, err := stateAbs(target)
	if err != nil {
		return err
	}

	for {
		header, err := tr.Next()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		if header.Name == "" || filepath.IsAbs(header.Name) || strings.Contains(header.Name, "..") {
			return fmt.Errorf("archive path rejected: %s", header.Name)
		}

		path := filepath.Join(cleanTarget, filepath.FromSlash(header.Name))

		cleanPath, err := stateAbs(path)
		if err != nil {
			return err
		}

		if cleanPath != cleanTarget && !strings.HasPrefix(cleanPath, cleanTarget+string(os.PathSeparator)) {
			return fmt.Errorf("archive path escapes target: %s", header.Name)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := stateMkdirAll(cleanPath, 0o700); err != nil {
				return err
			}
		case tar.TypeReg:
			if header.Size < 0 || header.Size > maxHydrateFileBytes {
				return fmt.Errorf("archive file %s has unsupported size %d", header.Name, header.Size)
			}

			if err := stateMkdirAll(filepath.Dir(cleanPath), 0o700); err != nil {
				return err
			}

			mode := header.FileInfo().Mode().Perm() & 0o700

			file, err := stateOpenFile(cleanPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
			if err != nil {
				return err
			}

			written, copyErr := stateCopyN(file, tr, header.Size)
			closeErr := file.Close()

			if copyErr != nil {
				return copyErr
			}

			if written != header.Size {
				return fmt.Errorf("archive file %s restored %d bytes, want %d", header.Name, written, header.Size)
			}

			if closeErr != nil {
				return closeErr
			}
		}
	}
}

func validateHydratedStateAgreement(sessionID string, idmap idmapRecord, snapshot stateSnapshot) error {
	if idmap.SessionID != sessionID {
		return fmt.Errorf("hermes store idmap session mismatch: %q != %q", idmap.SessionID, sessionID)
	}

	if snapshot.Session.SessionID != sessionID {
		return fmt.Errorf("hermes store snapshot session mismatch: %q != %q", snapshot.Session.SessionID, sessionID)
	}

	if snapshot.Session.NativeSessionID != idmap.NativeSessionID {
		return fmt.Errorf("hermes store idmap/main native session mismatch")
	}

	if snapshot.Session.ParentSessionID != idmap.ParentSessionID {
		return fmt.Errorf("hermes store idmap/main parent session mismatch")
	}

	if snapshot.Session.NativeParentSessionID != idmap.NativeParentSessionID {
		return fmt.Errorf("hermes store idmap/main native parent session mismatch")
	}

	return nil
}

func sqliteArchiveContent(path string) ([]byte, bool, error) {
	ok, err := isSQLiteDatabase(path)
	if err != nil || !ok {
		return nil, ok, err
	}

	tempDir, err := stateMkdirTemp("", "acp-go-hermes-sqlite-*")
	if err != nil {
		return nil, false, err
	}

	defer func() { _ = stateRemoveAll(tempDir) }()

	copyPath := filepath.Join(tempDir, "archive.db")
	if vacuumErr := vacuumSQLiteInto(path, copyPath); vacuumErr != nil {
		return nil, false, vacuumErr
	}

	if scrubErr := stateScrubSQLiteCredentialTables(copyPath); scrubErr != nil {
		return nil, false, scrubErr
	}

	data, err := stateReadFile(copyPath)
	if err != nil {
		return nil, false, err
	}

	return data, true, nil
}

func isSQLiteDatabase(path string) (bool, error) {
	file, err := stateOpen(path)
	if err != nil {
		return false, err
	}
	defer file.Close()

	header := make([]byte, 16)

	n, err := io.ReadFull(file, header)
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return n == len(header) && string(header) == "SQLite format 3\x00", nil
}

func vacuumSQLiteInto(source string, target string) error {
	db, err := stateSQLOpen("sqlite", source)
	if err != nil {
		return err
	}

	ctx := context.Background()
	_, execErr := db.ExecContext(ctx, "VACUUM INTO "+quoteSQLiteString(target)) // #nosec G202 -- target is string-literal quoted before interpolation.
	closeErr := db.Close()

	if execErr != nil {
		return execErr
	}

	return closeErr
}

func copyFile(source string, target string, mode os.FileMode) error {
	in, err := stateOpen(source)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := stateOpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}

	_, copyErr := stateCopy(out, in)
	closeErr := out.Close()

	if copyErr != nil {
		return copyErr
	}

	return closeErr
}

func scrubSQLiteCredentialTables(path string) error {
	db, err := stateSQLOpen("sqlite", path)
	if err != nil {
		return err
	}
	defer db.Close()

	ctx := context.Background()
	for _, statement := range []string{
		"PRAGMA foreign_keys=OFF",
		"PRAGMA secure_delete=ON",
		"PRAGMA wal_checkpoint(TRUNCATE)",
		"PRAGMA journal_mode=DELETE",
	} {
		if _, execErr := db.ExecContext(ctx, statement); execErr != nil {
			return execErr
		}
	}

	tables, err := sqliteCredentialTables(ctx, db)
	if err != nil {
		return err
	}

	for _, table := range tables {
		statement := "DELETE FROM " + quoteSQLiteIdent(table) // #nosec G202 -- table names come from sqlite_master and are identifier-quoted.
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}

	if len(tables) > 0 {
		if _, err := db.ExecContext(ctx, "VACUUM"); err != nil {
			return err
		}
	}

	if _, err := db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return err
	}

	return nil
}

func sqliteCredentialTables(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}

		sensitive, err := sqliteTableIsCredentialBearing(ctx, db, name)
		if err != nil {
			return nil, err
		}

		if sensitive {
			tables = append(tables, name)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tables, nil
}

func sqliteTableIsCredentialBearing(ctx context.Context, db *sql.DB, table string) (bool, error) {
	switch strings.ToLower(table) {
	case tableAccount, "control_account", "credential", "session_share":
		return true, nil
	}

	if sensitiveSQLiteName(table) {
		return true, nil
	}

	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+quoteSQLiteIdent(table)+")")
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid          int
			name         string
			columnType   string
			notNull      int
			defaultValue sql.NullString
			primaryKey   int
		)
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return false, err
		}

		if sensitiveSQLiteName(name) {
			return true, nil
		}
	}

	if err := rows.Err(); err != nil {
		return false, err
	}

	return false, nil
}

func sensitiveSQLiteName(value string) bool {
	value = strings.ToLower(value)
	for _, marker := range []string{"credential", valSecret, "access_token", "refresh_token", "api_key", "apikey", "private_key", "password"} {
		if strings.Contains(value, marker) {
			return true
		}
	}

	return false
}

func quoteSQLiteIdent(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func quoteSQLiteString(value string) string {
	return `'` + strings.ReplaceAll(value, `'`, `''`) + `'`
}
