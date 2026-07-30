package hermesacp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// Ledger record states. The proof a residence answer can carry is a total
// function of these and a native probe, so the set is closed.
const (
	authLedgerIntent    = "intent"
	authLedgerConfirmed = "confirmed"
)

// Closed proofSource enum. Native presence alone is never enough: without
// durable provenance binding it to this connection generation the honest answer
// is not_confirmed.
const (
	authProofConfirmedPresent = "confirmed_present"
	authProofConfirmedAbsent  = "confirmed_absent"
	authProofNotConfirmed     = "not_confirmed"
)

const (
	authLedgerVendorDir = "hermes"
	authLedgerLeafDir   = "ledger"
	authLedgerFileMode  = 0o600
	authLedgerDirMode   = 0o700
)

// authLedgerRecord is the whole content a ledger entry may carry. It never
// holds credential material, authorization URLs, user codes, prompt answers, or
// native text.
type authLedgerRecord struct {
	ProviderID         string `json:"providerId"`
	ConnectionID       string `json:"connectionId"`
	Revision           int64  `json:"revision"`
	BindingGeneration  int64  `json:"bindingGeneration"`
	FlowID             string `json:"flowId"`
	AuthorizeRequestID string `json:"authorizeRequestId"`
	State              string `json:"state"`
	CreatedAt          int64  `json:"createdAt"`
	UpdatedAt          int64  `json:"updatedAt"`
}

var (
	ledgerMkdirAll = os.MkdirAll
	ledgerChmod    = os.Chmod
	ledgerStat     = os.Stat
	ledgerRename   = os.Rename
	ledgerOpen     = os.Open
	ledgerReadFile = os.ReadFile
	ledgerReadDir  = os.ReadDir
	ledgerRemove   = os.Remove
	ledgerMarshal  = json.Marshal
	ledgerEvalPath = filepath.EvalSymlinks
)

// ledgerFile is the file surface an atomic ledger write drives.
type ledgerFile interface {
	Name() string
	Write([]byte) (int, error)
	Chmod(os.FileMode) error
	Sync() error
	Close() error
}

var ledgerCreateTemp = func(dir string, pattern string) (ledgerFile, error) {
	return os.CreateTemp(dir, pattern)
}

// authLedger is the durable values-free record of which connection lineage
// owns a provider in one native credential residence.
type authLedger struct {
	dir string
}

// validateProviderAuthRoots requires the ledger and native residence as one
// pair and rejects relative paths.
func validateProviderAuthRoots(options Options) error {
	if (options.ProviderAuthRoot == "") != (options.ProviderAuthHome == "") {
		return errors.New("provider auth requires both root and home")
	}

	if options.ProviderAuthRoot != "" && !filepath.IsAbs(options.ProviderAuthRoot) {
		return fmt.Errorf("provider auth root must be an absolute path")
	}

	if options.ProviderAuthHome != "" && !filepath.IsAbs(options.ProviderAuthHome) {
		return fmt.Errorf("provider auth home must be an absolute path")
	}

	return nil
}

// authLedgerRootConfigured reports whether the host supplied a durable ledger
// root at all, which is what separates a surface nobody asked for from one that
// was asked for and could not be prepared.
func authLedgerRootConfigured(options Options) bool {
	return options.ProviderAuthRoot != "" && options.ProviderAuthHome != ""
}

func prepareProviderAuthHome(path string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", errors.New("provider auth home must be an absolute path")
	}

	clean := filepath.Clean(path)
	if err := ledgerMkdirAll(clean, authLedgerDirMode); err != nil {
		return "", fmt.Errorf("create provider auth home: %w", err)
	}

	if err := ledgerChmod(clean, authLedgerDirMode); err != nil {
		return "", fmt.Errorf("restrict provider auth home: %w", err)
	}

	resolved, err := ledgerEvalPath(clean)
	if err != nil {
		return "", fmt.Errorf("resolve provider auth home: %w", err)
	}

	return resolved, nil
}

// newAuthLedger resolves and validates the configured durable root. A root that
// does not exist and cannot be created, is not a directory, or is not writable
// leaves the provider-auth surface unadvertised, exactly as an unset one does.
func newAuthLedger(options Options) (*authLedger, error) {
	root := options.ProviderAuthRoot
	if !filepath.IsAbs(root) {
		return nil, errors.New("provider auth root must be an absolute path")
	}

	if !filepath.IsAbs(options.ProviderAuthHome) {
		return nil, errors.New("provider auth home must be an absolute path")
	}

	// The operator-configured root is restricted as well as the leaf: a
	// pre-existing directory arrives with whatever mode its creator chose.
	if err := ledgerMkdirAll(root, authLedgerDirMode); err != nil {
		return nil, fmt.Errorf("create provider auth root: %w", err)
	}

	if err := ledgerChmod(root, authLedgerDirMode); err != nil {
		return nil, fmt.Errorf("restrict provider auth root: %w", err)
	}

	dir := filepath.Join(root, authLedgerVendorDir, authLedgerHomeKey(options.ProviderAuthHome), authLedgerLeafDir)
	if err := ledgerMkdirAll(dir, authLedgerDirMode); err != nil {
		return nil, fmt.Errorf("create provider auth ledger root: %w", err)
	}

	if err := ledgerChmod(dir, authLedgerDirMode); err != nil {
		return nil, fmt.Errorf("restrict provider auth ledger root: %w", err)
	}

	info, err := ledgerStat(dir)
	if err != nil {
		return nil, fmt.Errorf("inspect provider auth ledger root: %w", err)
	}

	if !info.IsDir() {
		return nil, errors.New("provider auth ledger root is not a directory")
	}

	probe, err := ledgerCreateTemp(dir, "writable-")
	if err != nil {
		return nil, fmt.Errorf("verify provider auth ledger root is writable: %w", err)
	}

	name := probe.Name()

	return &authLedger{dir: dir}, errors.Join(probe.Close(), ledgerRemove(name))
}

func authLedgerHomeKey(home string) string {
	sum := sha256.Sum256([]byte(filepath.Clean(home)))

	return hex.EncodeToString(sum[:])[:16]
}

func (l *authLedger) path(providerID string) string {
	sum := sha256.Sum256([]byte(providerID))

	return filepath.Join(l.dir, hex.EncodeToString(sum[:])[:32]+".json")
}

func (l *authLedger) read(providerID string) (authLedgerRecord, bool, error) {
	contents, err := ledgerReadFile(l.path(providerID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return authLedgerRecord{}, false, nil
		}

		return authLedgerRecord{}, false, fmt.Errorf("read provider auth ledger entry: %w", err)
	}

	var record authLedgerRecord
	if err := json.Unmarshal(contents, &record); err != nil {
		return authLedgerRecord{}, false, fmt.Errorf("decode provider auth ledger entry: %w", err)
	}

	return record, true, nil
}

// write persists a record atomically and durably: a temporary file in the same
// directory, fsynced, renamed over its target, with the directory fsynced
// after. Persisted here means fsynced, never merely written.
func (l *authLedger) write(record authLedgerRecord) error {
	contents, err := ledgerMarshal(record)
	if err != nil {
		return fmt.Errorf("encode provider auth ledger entry: %w", err)
	}

	file, err := ledgerCreateTemp(l.dir, "entry-")
	if err != nil {
		return fmt.Errorf("create provider auth ledger entry: %w", err)
	}

	temp := file.Name()

	if err := writeLedgerFile(file, contents); err != nil {
		return errors.Join(fmt.Errorf("write provider auth ledger entry: %w", err), ledgerRemove(temp))
	}

	if err := ledgerRename(temp, l.path(record.ProviderID)); err != nil {
		return errors.Join(fmt.Errorf("commit provider auth ledger entry: %w", err), ledgerRemove(temp))
	}

	return l.syncDir()
}

func writeLedgerFile(file ledgerFile, contents []byte) error {
	if _, err := file.Write(contents); err != nil {
		return errors.Join(err, file.Close())
	}

	if err := file.Chmod(authLedgerFileMode); err != nil {
		return errors.Join(err, file.Close())
	}

	if err := file.Sync(); err != nil {
		return errors.Join(err, file.Close())
	}

	return file.Close()
}

func (l *authLedger) syncDir() error {
	dir, err := ledgerOpen(l.dir)
	if err != nil {
		return fmt.Errorf("open provider auth ledger root: %w", err)
	}

	return errors.Join(dir.Sync(), dir.Close())
}

func (l *authLedger) list() ([]authLedgerRecord, error) {
	entries, err := ledgerReadDir(l.dir)
	if err != nil {
		return nil, fmt.Errorf("list provider auth ledger: %w", err)
	}

	records := make([]authLedgerRecord, 0, len(entries))

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		contents, err := ledgerReadFile(filepath.Join(l.dir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read provider auth ledger entry: %w", err)
		}

		var record authLedgerRecord
		if err := json.Unmarshal(contents, &record); err != nil {
			return nil, fmt.Errorf("decode provider auth ledger entry: %w", err)
		}

		records = append(records, record)
	}

	sort.Slice(records, func(i, j int) bool { return records[i].ProviderID < records[j].ProviderID })

	return records, nil
}

type authInventoryEntry struct {
	ProviderID        string `json:"providerId"`
	ConnectionID      string `json:"connectionId"`
	Revision          int64  `json:"revision"`
	BindingGeneration int64  `json:"bindingGeneration"`
	ProofSource       string `json:"proofSource"`
}

type authInventoryResult struct {
	Entries []authInventoryEntry `json:"entries"`
}

// inventory combines confirmed lineage with the native catalog's values-free
// logged-in status. Neither source is sufficient on its own.
func (p *providerAuth) inventory(ctx context.Context, params json.RawMessage) (any, error) {
	fields, err := authParamFields(params, authFieldSessionID)
	if err != nil {
		return nil, err
	}

	sessionID, err := authRequiredString(fields, authFieldSessionID)
	if err != nil {
		return nil, err
	}

	session, err := p.authSession(sessionID)
	if err != nil {
		return nil, err
	}

	client := session.authNativeClient()
	if client == nil {
		return nil, authFailed(authCauseTransport, "", "", "")
	}

	providers, err := client.AuthProviders(ctx)
	if err != nil {
		return nil, authFailed(authNativeCause(err), "", "", "")
	}

	loggedIn := make(map[string]bool, len(providers))
	for _, provider := range providers {
		loggedIn[provider.ID] = provider.LoggedIn
	}

	records, err := p.ledger.list()
	if err != nil {
		return nil, authFailed(authCauseProcess, "", "", "")
	}

	entries := make([]authInventoryEntry, 0, len(records))

	for _, record := range records {
		if record.State != authLedgerConfirmed {
			continue
		}

		entries = append(entries, authInventoryEntry{
			ProviderID:        record.ProviderID,
			ConnectionID:      record.ConnectionID,
			Revision:          record.Revision,
			BindingGeneration: record.BindingGeneration,
			ProofSource:       authProofSource(record.State, loggedIn[record.ProviderID]),
		})
	}

	return authInventoryResult{Entries: entries}, nil
}

// authProofSource is the total function of ledger state and native probe. A
// sibling reports exactly the cell the two select and never chooses a value.
func authProofSource(state string, present bool) string {
	if state != authLedgerConfirmed {
		return authProofNotConfirmed
	}

	if present {
		return authProofConfirmedPresent
	}

	return authProofConfirmedAbsent
}
