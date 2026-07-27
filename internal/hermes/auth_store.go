package hermes

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Native store layout. The pool is an ordered array per provider, so an entry is
// addressed by index and a labelled slot is appended rather than replaced: a
// same-label re-add duplicates instead of overwriting.
const (
	authStoreFile     = "auth.json"
	authPKCEStoreFile = ".anthropic_oauth.json"

	authStoreFileMode = 0o600
	authStoreDirMode  = 0o700

	authPoolSection      = "credential_pool"
	authProvidersSection = "providers"

	authFieldLabel        = "label"
	authFieldAuthType     = "auth_type"
	authFieldAccessToken  = "access_token"
	authFieldRefreshToken = "refresh_token"
	authFieldExpiresAt    = "expires_at"
	authFieldExpiresIn    = "expires_in"
	authFieldTokens       = "tokens"
)

// authStoreHandle is the file surface an atomic store write drives.
type authStoreHandle interface {
	Name() string
	Write([]byte) (int, error)
	Chmod(os.FileMode) error
	Sync() error
	Close() error
}

var (
	authStoreReadFile = os.ReadFile
	authStoreMkdirAll = os.MkdirAll
	authStoreRename   = os.Rename
	authStoreRemove   = os.Remove
	authStoreOpen     = os.Open
	authStoreCreate   = func(dir string, pattern string) (authStoreHandle, error) {
		return os.CreateTemp(dir, pattern)
	}
)

type authPoolEntry map[string]json.RawMessage

type authStoreDocument struct {
	sections map[string]json.RawMessage
	pool     map[string][]authPoolEntry
}

// AuthSlotPresent reports whether the reserved slot for this connection holds a
// non-empty access token. Presence of any other pool entry is not presence of
// this slot.
func AuthSlotPresent(home string, providerID string, label string) (bool, error) {
	_, ok, err := AuthReadSlot(home, providerID, label)

	return ok, err
}

// AuthReadSlot reads token material out of the reserved slot and nothing else.
func AuthReadSlot(home string, providerID string, label string) (AuthMaterial, bool, error) {
	document, err := authReadStore(home)
	if err != nil {
		return AuthMaterial{}, false, err
	}

	index := authFindSlot(document.pool[providerID], label)
	if index < 0 {
		return AuthMaterial{}, false, nil
	}

	material := authMaterialFromEntry(document.pool[providerID][index])
	if material.AccessToken == "" {
		return AuthMaterial{}, false, nil
	}

	return material, true, nil
}

// AuthWriteSlot installs token material into the reserved slot. An existing slot
// with the same label is removed first, because appending a second entry under
// one label duplicates the credential rather than replacing it.
func AuthWriteSlot(home string, providerID string, label string, material AuthMaterial) error {
	document, err := authReadStore(home)
	if err != nil {
		return err
	}

	entries := authRemoveSlots(document.pool[providerID], label)
	document.pool[providerID] = append(entries, authEntryFromMaterial(label, material))

	return authWriteStore(home, document)
}

// AuthMigrateSlot moves the entry a completed native flow just wrote into the
// reserved slot: it copies the token material, removes the original by index,
// and appends the labelled slot. The order is load-bearing — an idempotent
// re-add under the same label leaves two live copies of one credential.
func AuthMigrateSlot(home string, providerID string, label string) (bool, error) {
	document, err := authReadStore(home)
	if err != nil {
		return false, err
	}

	entries := document.pool[providerID]

	index := authFindUnlabelled(entries)
	if index < 0 {
		return false, nil
	}

	material := authMaterialFromEntry(entries[index])
	if material.AccessToken == "" {
		return false, nil
	}

	remaining := make([]authPoolEntry, 0, len(entries))
	remaining = append(remaining, entries[:index]...)
	remaining = append(remaining, entries[index+1:]...)

	document.pool[providerID] = append(authRemoveSlots(remaining, label), authEntryFromMaterial(label, material))

	return true, authWriteStore(home, document)
}

// AuthRemoveSlot removes the reserved slot and reports whether one was found.
func AuthRemoveSlot(home string, providerID string, label string) (bool, error) {
	document, err := authReadStore(home)
	if err != nil {
		return false, err
	}

	entries := document.pool[providerID]

	remaining := authRemoveSlots(entries, label)
	if len(remaining) == len(entries) {
		return false, nil
	}

	if len(remaining) == 0 {
		delete(document.pool, providerID)
	} else {
		document.pool[providerID] = remaining
	}

	return true, authWriteStore(home, document)
}

// AuthReadFlowExpiry reads the access-token expiry out of the second residence a
// completed flow writes. The two flows disagree on both location and field
// naming, and neither is the pool entry: the device path records only a relative
// lifetime under the provider section, while the pkce path writes an absolute
// value into its own mode-0600 file and leaves the provider section empty.
func AuthReadFlowExpiry(home string, providerID string, flow string) (AuthMaterial, bool, error) {
	switch flow {
	case AuthFlowDeviceCode:
		return authReadDeviceExpiry(home, providerID)
	case AuthFlowPKCE:
		return authReadPKCEExpiry(home)
	default:
		return AuthMaterial{}, false, nil
	}
}

func authReadDeviceExpiry(home string, providerID string) (AuthMaterial, bool, error) {
	document, err := authReadStore(home)
	if err != nil {
		return AuthMaterial{}, false, err
	}

	var providers map[string]struct {
		Tokens map[string]json.RawMessage `json:"tokens"`
	}

	raw, ok := document.sections[authProvidersSection]
	if !ok {
		return AuthMaterial{}, false, nil
	}

	if err := json.Unmarshal(raw, &providers); err != nil {
		return AuthMaterial{}, false, fmt.Errorf("decode hermes provider tokens: %w", err)
	}

	tokens := providers[providerID].Tokens
	if tokens == nil {
		return AuthMaterial{}, false, nil
	}

	seconds, ok := authNumberField(tokens, authFieldExpiresIn)
	if !ok {
		return AuthMaterial{}, false, nil
	}

	return AuthMaterial{ExpiresIn: secondsToDuration(seconds)}, true, nil
}

func authReadPKCEExpiry(home string) (AuthMaterial, bool, error) {
	contents, err := authStoreReadFile(filepath.Join(home, authPKCEStoreFile))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return AuthMaterial{}, false, nil
		}

		return AuthMaterial{}, false, fmt.Errorf("read hermes pkce credential file: %w", err)
	}

	var payload struct {
		ExpiresAt float64 `json:"expiresAt"`
	}

	if err := json.Unmarshal(contents, &payload); err != nil {
		return AuthMaterial{}, false, fmt.Errorf("decode hermes pkce credential file: %w", err)
	}

	if payload.ExpiresAt <= 0 {
		return AuthMaterial{}, false, nil
	}

	return AuthMaterial{AccessExpiresAt: int64(payload.ExpiresAt)}, true, nil
}

func authFindSlot(entries []authPoolEntry, label string) int {
	for index, entry := range entries {
		if authStringField(entry, authFieldLabel) == label {
			return index
		}
	}

	return -1
}

// authFindUnlabelled selects the newest entry no adapter label claims. A
// completed native flow appends its entry, so the last unlabelled one is the
// entry this flow produced; ambient, environment, and operator entries keep
// their position and are never selected once a labelled slot exists for them.
func authFindUnlabelled(entries []authPoolEntry) int {
	for index := len(entries) - 1; index >= 0; index-- {
		if !AuthSlotLabelPrefix(authStringField(entries[index], authFieldLabel)) {
			return index
		}
	}

	return -1
}

func authRemoveSlots(entries []authPoolEntry, label string) []authPoolEntry {
	remaining := make([]authPoolEntry, 0, len(entries))

	for _, entry := range entries {
		if authStringField(entry, authFieldLabel) == label {
			continue
		}

		remaining = append(remaining, entry)
	}

	return remaining
}

func authMaterialFromEntry(entry authPoolEntry) AuthMaterial {
	material := AuthMaterial{
		AuthType:     authStringField(entry, authFieldAuthType),
		AccessToken:  authStringField(entry, authFieldAccessToken),
		RefreshToken: authStringField(entry, authFieldRefreshToken),
	}

	if value, ok := authNumberField(entry, authFieldExpiresAt); ok {
		material.AccessExpiresAt = int64(value)
	}

	if material.AuthType == "" {
		material.AuthType = AuthTypeOAuth
	}

	return material
}

// authEntryFromMaterial builds a fresh pool entry. Native bookkeeping — request
// counters, source tags, fingerprints — is owned by hermes and is never copied
// forward from an entry this adapter did not write.
func authEntryFromMaterial(label string, material AuthMaterial) authPoolEntry {
	entry := authPoolEntry{}
	authSetString(entry, authFieldLabel, label)
	authSetString(entry, authFieldAuthType, material.AuthType)
	authSetString(entry, authFieldAccessToken, material.AccessToken)

	if material.RefreshToken != "" {
		authSetString(entry, authFieldRefreshToken, material.RefreshToken)
	}

	if material.AccessExpiresAt > 0 {
		entry[authFieldExpiresAt] = json.RawMessage(fmt.Sprintf("%d", material.AccessExpiresAt))
	} else {
		entry[authFieldExpiresAt] = json.RawMessage("null")
	}

	return entry
}

func authStringField(entry authPoolEntry, name string) string {
	raw, ok := entry[name]
	if !ok {
		return ""
	}

	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return ""
	}

	return value
}

func authNumberField(entry map[string]json.RawMessage, name string) (float64, bool) {
	raw, ok := entry[name]
	if !ok {
		return 0, false
	}

	var value float64
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, false
	}

	return value, true
}

func authSetString(entry authPoolEntry, name string, value string) {
	// Marshalling a Go string never fails; invalid UTF-8 is replaced rather
	// than rejected.
	encoded, _ := json.Marshal(value)
	entry[name] = encoded
}

func authReadStore(home string) (*authStoreDocument, error) {
	document := &authStoreDocument{
		sections: map[string]json.RawMessage{},
		pool:     map[string][]authPoolEntry{},
	}

	contents, err := authStoreReadFile(filepath.Join(home, authStoreFile))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return document, nil
		}

		return nil, fmt.Errorf("read hermes credential store: %w", err)
	}

	if len(contents) == 0 {
		return document, nil
	}

	if err := json.Unmarshal(contents, &document.sections); err != nil {
		return nil, fmt.Errorf("decode hermes credential store: %w", err)
	}

	raw, ok := document.sections[authPoolSection]
	if !ok {
		return document, nil
	}

	if err := json.Unmarshal(raw, &document.pool); err != nil {
		return nil, fmt.Errorf("decode hermes credential pool: %w", err)
	}

	return document, nil
}

// authWriteStore replaces the store atomically and durably. Sections this
// adapter does not own are written back byte-for-byte.
func authWriteStore(home string, document *authStoreDocument) error {
	// Both values are already-decoded raw JSON, so neither encode can fail.
	encodedPool, _ := json.Marshal(document.pool)
	document.sections[authPoolSection] = encodedPool
	contents, _ := json.Marshal(document.sections)

	if err := authStoreMkdirAll(home, authStoreDirMode); err != nil {
		return fmt.Errorf("create hermes credential store root: %w", err)
	}

	file, err := authStoreCreate(home, "auth-")
	if err != nil {
		return fmt.Errorf("create hermes credential store: %w", err)
	}

	temp := file.Name()

	if err := authSyncStoreFile(file, contents); err != nil {
		return errors.Join(fmt.Errorf("write hermes credential store: %w", err), authStoreRemove(temp))
	}

	if err := authStoreRename(temp, filepath.Join(home, authStoreFile)); err != nil {
		return errors.Join(fmt.Errorf("commit hermes credential store: %w", err), authStoreRemove(temp))
	}

	return authSyncStoreDir(home)
}

func authSyncStoreFile(file authStoreHandle, contents []byte) error {
	if _, err := file.Write(contents); err != nil {
		return errors.Join(err, file.Close())
	}

	if err := file.Chmod(authStoreFileMode); err != nil {
		return errors.Join(err, file.Close())
	}

	if err := file.Sync(); err != nil {
		return errors.Join(err, file.Close())
	}

	return file.Close()
}

func authSyncStoreDir(home string) error {
	dir, err := authStoreOpen(home)
	if err != nil {
		return fmt.Errorf("open hermes credential store root: %w", err)
	}

	return errors.Join(dir.Sync(), dir.Close())
}

// AuthAnchorExpiry resolves an absolute access-token expiry. An absolute native
// value is used as it stands; a relative lifetime is anchored at the moment of
// the read, because nothing in the store records an issued-at and the access
// token's own claim is never decoded.
func AuthAnchorExpiry(now time.Time, material AuthMaterial) int64 {
	if material.AccessExpiresAt > 0 {
		return material.AccessExpiresAt
	}

	if material.ExpiresIn > 0 {
		return now.Add(material.ExpiresIn).UnixMilli()
	}

	return 0
}
