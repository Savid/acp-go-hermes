package hermes

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// defaultAuthStoreCreate captures the package default so a reset restores the
// exact function the non-test build uses rather than an equivalent copy.
var defaultAuthStoreCreate = authStoreCreate

func restoreAuthStoreHooks(t *testing.T) {
	t.Helper()

	reset := func() {
		authStoreReadFile = os.ReadFile
		authStoreMkdirAll = os.MkdirAll
		authStoreRename = os.Rename
		authStoreRemove = os.Remove
		authStoreOpen = os.Open
		authStoreCreate = defaultAuthStoreCreate
	}

	reset()
	t.Cleanup(reset)
}

func writeAuthStore(t *testing.T, home string, document map[string]any) {
	t.Helper()

	contents, err := json.Marshal(document)
	if err != nil {
		t.Fatalf("marshal store: %v", err)
	}

	if err := os.WriteFile(filepath.Join(home, authStoreFile), contents, 0o600); err != nil {
		t.Fatalf("write store: %v", err)
	}
}

func readAuthStore(t *testing.T, home string) map[string]any {
	t.Helper()

	contents, err := os.ReadFile(filepath.Join(home, authStoreFile))
	if err != nil {
		t.Fatalf("read store: %v", err)
	}

	var document map[string]any
	if err := json.Unmarshal(contents, &document); err != nil {
		t.Fatalf("decode store: %v", err)
	}

	return document
}

func poolEntries(t *testing.T, home string, providerID string) []any {
	t.Helper()

	pool, _ := readAuthStore(t, home)[authPoolSection].(map[string]any)
	entries, _ := pool[providerID].([]any)

	return entries
}

func TestAuthSlotRoundTripOnAnAbsentStore(t *testing.T) {
	t.Parallel()

	home := t.TempDir()

	if present, err := AuthSlotPresent(home, "xai", AuthSlotLabel("c1")); err != nil || present {
		t.Fatalf("absent store reported present = %v, %v", present, err)
	}

	material := AuthMaterial{AuthType: AuthTypeOAuth, AccessToken: "access", RefreshToken: "refresh", AccessExpiresAt: 7}
	if err := AuthWriteSlot(home, "xai", AuthSlotLabel("c1"), material); err != nil {
		t.Fatalf("AuthWriteSlot: %v", err)
	}

	read, present, err := AuthReadSlot(home, "xai", AuthSlotLabel("c1"))
	if err != nil || !present || read != material {
		t.Fatalf("AuthReadSlot = %#v, %v, %v", read, present, err)
	}

	info, err := os.Stat(filepath.Join(home, authStoreFile))
	if err != nil {
		t.Fatalf("stat store: %v", err)
	}

	if info.Mode().Perm() != authStoreFileMode {
		t.Fatalf("store mode = %v", info.Mode().Perm())
	}
}

func TestAuthWriteSlotReplacesRatherThanDuplicates(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	label := AuthSlotLabel("c1")

	for _, token := range []string{"first", "second", "third"} {
		if err := AuthWriteSlot(home, "xai", label, AuthMaterial{AuthType: AuthTypeOAuth, AccessToken: token}); err != nil {
			t.Fatalf("AuthWriteSlot: %v", err)
		}
	}

	entries := poolEntries(t, home, "xai")
	if len(entries) != 1 {
		t.Fatalf("a same-label re-add duplicated the slot: %#v", entries)
	}

	read, _, err := AuthReadSlot(home, "xai", label)
	if err != nil || read.AccessToken != "third" {
		t.Fatalf("AuthReadSlot = %#v, %v", read, err)
	}
}

func TestAuthMigrateSlotRemovesTheOriginalByIndex(t *testing.T) {
	t.Parallel()

	home := t.TempDir()

	writeAuthStore(t, home, map[string]any{
		authPoolSection: map[string]any{
			"xai": []any{
				map[string]any{"auth_type": "oauth", "access_token": "ambient", "source": "gh", "request_count": 4},
				map[string]any{"auth_type": "oauth", "access_token": "completed", "refresh_token": "r", "expires_at": nil, "secret_fingerprint": "abc"},
			},
		},
		"other_section": map[string]any{"kept": true},
	})

	label := AuthSlotLabel("c1")

	migrated, err := AuthMigrateSlot(home, "xai", label)
	if err != nil || !migrated {
		t.Fatalf("AuthMigrateSlot = %v, %v", migrated, err)
	}

	entries := poolEntries(t, home, "xai")
	if len(entries) != 2 {
		t.Fatalf("entries = %#v", entries)
	}

	first, _ := entries[0].(map[string]any)
	if first["access_token"] != "ambient" || first["source"] != "gh" {
		t.Fatalf("the ambient entry was disturbed: %#v", first)
	}

	second, _ := entries[1].(map[string]any)
	if second["label"] != label || second["access_token"] != "completed" {
		t.Fatalf("reserved slot = %#v", second)
	}

	for _, bookkeeping := range []string{"secret_fingerprint", "request_count", "source"} {
		if _, present := second[bookkeeping]; present {
			t.Fatalf("the reserved slot carried native bookkeeping %q: %#v", bookkeeping, second)
		}
	}

	if readAuthStore(t, home)["other_section"] == nil {
		t.Fatal("a section the adapter does not own was dropped")
	}

	// Migrating again finds only labelled and ambient entries; the ambient one
	// is never claimed once it is the newest unlabelled entry, so the second
	// migration moves it and the first slot is replaced rather than duplicated.
	if _, err := AuthMigrateSlot(home, "xai", label); err != nil {
		t.Fatalf("second migrate: %v", err)
	}

	if entries := poolEntries(t, home, "xai"); len(entries) != 1 {
		t.Fatalf("a second migration duplicated the reserved label: %#v", entries)
	}
}

func TestAuthMigrateSlotFindsNothingToMigrate(t *testing.T) {
	t.Parallel()

	home := t.TempDir()

	if migrated, err := AuthMigrateSlot(home, "xai", AuthSlotLabel("c1")); err != nil || migrated {
		t.Fatalf("empty pool migrated = %v, %v", migrated, err)
	}

	writeAuthStore(t, home, map[string]any{
		authPoolSection: map[string]any{"xai": []any{map[string]any{"auth_type": "oauth", "access_token": ""}}},
	})

	if migrated, err := AuthMigrateSlot(home, "xai", AuthSlotLabel("c1")); err != nil || migrated {
		t.Fatalf("an empty token migrated = %v, %v", migrated, err)
	}
}

func TestAuthRemoveSlotIsScopedToTheReservedLabel(t *testing.T) {
	t.Parallel()

	home := t.TempDir()

	writeAuthStore(t, home, map[string]any{
		authPoolSection: map[string]any{
			"xai": []any{map[string]any{"auth_type": "oauth", "access_token": "ambient"}},
		},
	})

	label := AuthSlotLabel("c1")

	if removed, err := AuthRemoveSlot(home, "xai", label); err != nil || removed {
		t.Fatalf("an absent slot reported removed = %v, %v", removed, err)
	}

	if err := AuthWriteSlot(home, "xai", label, AuthMaterial{AuthType: AuthTypeOAuth, AccessToken: "brokered"}); err != nil {
		t.Fatalf("AuthWriteSlot: %v", err)
	}

	removed, err := AuthRemoveSlot(home, "xai", label)
	if err != nil || !removed {
		t.Fatalf("AuthRemoveSlot = %v, %v", removed, err)
	}

	entries := poolEntries(t, home, "xai")
	if len(entries) != 1 {
		t.Fatalf("removal touched the ambient entry: %#v", entries)
	}

	// Removing the only entry drops the provider key rather than leaving an
	// empty array behind.
	if err := AuthWriteSlot(home, "solo", label, AuthMaterial{AuthType: AuthTypeOAuth, AccessToken: "t"}); err != nil {
		t.Fatalf("AuthWriteSlot: %v", err)
	}

	if _, err := AuthRemoveSlot(home, "solo", label); err != nil {
		t.Fatalf("AuthRemoveSlot: %v", err)
	}

	pool, _ := readAuthStore(t, home)[authPoolSection].(map[string]any)
	if _, present := pool["solo"]; present {
		t.Fatalf("an emptied provider survived: %#v", pool)
	}
}

func TestAuthReadFlowExpiryIsFlowSpecific(t *testing.T) {
	t.Parallel()

	home := t.TempDir()

	if _, ok, err := AuthReadFlowExpiry(home, "xai", "unknown-flow"); err != nil || ok {
		t.Fatalf("an unknown flow reported an expiry = %v, %v", ok, err)
	}

	if _, ok, err := AuthReadFlowExpiry(home, "xai", AuthFlowDeviceCode); err != nil || ok {
		t.Fatalf("an absent provider section reported an expiry = %v, %v", ok, err)
	}

	if _, ok, err := AuthReadFlowExpiry(home, "anthropic", AuthFlowPKCE); err != nil || ok {
		t.Fatalf("an absent pkce file reported an expiry = %v, %v", ok, err)
	}

	writeAuthStore(t, home, map[string]any{
		authProvidersSection: map[string]any{
			"xai":   map[string]any{"tokens": map[string]any{"expires_in": 21600, "id_token": "ignored"}},
			"nous":  map[string]any{"tokens": map[string]any{"token_type": "Bearer"}},
			"other": map[string]any{},
		},
	})

	material, ok, err := AuthReadFlowExpiry(home, "xai", AuthFlowDeviceCode)
	if err != nil || !ok || material.ExpiresIn != 21600*time.Second {
		t.Fatalf("device expiry = %#v, %v, %v", material, ok, err)
	}

	if material.AccessExpiresAt != 0 {
		t.Fatalf("the device path invented an absolute expiry: %#v", material)
	}

	if _, okLocal, errLocal := AuthReadFlowExpiry(home, "nous", AuthFlowDeviceCode); errLocal != nil || okLocal {
		t.Fatalf("a token object with no expires_in reported one = %v, %v", ok, err)
	}

	if _, okLocal, errLocal := AuthReadFlowExpiry(home, "other", AuthFlowDeviceCode); errLocal != nil || okLocal {
		t.Fatalf("a provider with no tokens reported an expiry = %v, %v", ok, err)
	}

	pkceFile := filepath.Join(home, authPKCEStoreFile)
	if errLocal := os.WriteFile(pkceFile, []byte(`{"accessToken":"a","refreshToken":"r","expiresAt":1750000000000}`), 0o600); errLocal != nil {
		t.Fatalf("write pkce file: %v", err)
	}

	material, ok, err = AuthReadFlowExpiry(home, "anthropic", AuthFlowPKCE)
	if err != nil || !ok || material.AccessExpiresAt != 1750000000000 {
		t.Fatalf("pkce expiry = %#v, %v, %v", material, ok, err)
	}

	if err := os.WriteFile(pkceFile, []byte(`{"expiresAt":0}`), 0o600); err != nil {
		t.Fatalf("write pkce file: %v", err)
	}

	if _, ok, err := AuthReadFlowExpiry(home, "anthropic", AuthFlowPKCE); err != nil || ok {
		t.Fatalf("a zero pkce expiry was reported = %v, %v", ok, err)
	}
}

func TestAuthReadFlowExpiryFailurePaths(t *testing.T) {
	restoreAuthStoreHooks(t)

	home := t.TempDir()

	if err := os.WriteFile(filepath.Join(home, authStoreFile), []byte(`{"providers":7}`), 0o600); err != nil {
		t.Fatalf("write store: %v", err)
	}

	if _, _, err := AuthReadFlowExpiry(home, "xai", AuthFlowDeviceCode); err == nil {
		t.Fatal("a malformed providers section decoded")
	}

	if err := os.WriteFile(filepath.Join(home, authPKCEStoreFile), []byte(`{`), 0o600); err != nil {
		t.Fatalf("write pkce file: %v", err)
	}

	if _, _, err := AuthReadFlowExpiry(home, "anthropic", AuthFlowPKCE); err == nil {
		t.Fatal("a malformed pkce file decoded")
	}

	authStoreReadFile = func(string) ([]byte, error) { return nil, errors.New("read") }

	if _, _, err := AuthReadFlowExpiry(home, "anthropic", AuthFlowPKCE); err == nil {
		t.Fatal("a pkce read failure was reported clean")
	}

	if _, _, err := AuthReadFlowExpiry(home, "xai", AuthFlowDeviceCode); err == nil {
		t.Fatal("a store read failure was reported clean")
	}
}

func TestAuthReadStoreFailurePaths(t *testing.T) {
	restoreAuthStoreHooks(t)

	home := t.TempDir()

	if err := os.WriteFile(filepath.Join(home, authStoreFile), nil, 0o600); err != nil {
		t.Fatalf("write empty store: %v", err)
	}

	if _, ok, err := AuthReadSlot(home, "xai", AuthSlotLabel("c1")); err != nil || ok {
		t.Fatalf("an empty store reported a slot = %v, %v", ok, err)
	}

	if err := os.WriteFile(filepath.Join(home, authStoreFile), []byte(`{"credential_pool":7}`), 0o600); err != nil {
		t.Fatalf("write store: %v", err)
	}

	if _, _, err := AuthReadSlot(home, "xai", AuthSlotLabel("c1")); err == nil {
		t.Fatal("a malformed pool decoded")
	}

	if _, err := AuthRemoveSlot(home, "xai", AuthSlotLabel("c1")); err == nil {
		t.Fatal("a malformed pool was removed from")
	}

	if _, err := AuthMigrateSlot(home, "xai", AuthSlotLabel("c1")); err == nil {
		t.Fatal("a malformed pool was migrated")
	}

	if err := AuthWriteSlot(home, "xai", AuthSlotLabel("c1"), AuthMaterial{AccessToken: "t"}); err == nil {
		t.Fatal("a malformed pool was written to")
	}

	if err := os.WriteFile(filepath.Join(home, authStoreFile), []byte(`[]`), 0o600); err != nil {
		t.Fatalf("write store: %v", err)
	}

	if _, _, err := AuthReadSlot(home, "xai", AuthSlotLabel("c1")); err == nil {
		t.Fatal("a non-object store decoded")
	}

	authStoreReadFile = func(string) ([]byte, error) { return nil, errors.New("read") }

	if _, _, err := AuthReadSlot(home, "xai", AuthSlotLabel("c1")); err == nil {
		t.Fatal("a store read failure was reported clean")
	}
}

func TestAuthWriteStoreFailurePaths(t *testing.T) {
	restoreAuthStoreHooks(t)

	home := t.TempDir()
	label := AuthSlotLabel("c1")
	material := AuthMaterial{AuthType: AuthTypeOAuth, AccessToken: "t"}

	authStoreMkdirAll = func(string, os.FileMode) error { return errors.New("mkdir") }
	if err := AuthWriteSlot(home, "xai", label, material); err == nil {
		t.Fatal("a mkdir failure was reported clean")
	}

	restoreAuthStoreHooks(t)

	authStoreCreate = func(string, string) (authStoreHandle, error) { return nil, errors.New("temp") }
	if err := AuthWriteSlot(home, "xai", label, material); err == nil {
		t.Fatal("a temp-file failure was reported clean")
	}

	restoreAuthStoreHooks(t)

	authStoreRename = func(string, string) error { return errors.New("rename") }
	if err := AuthWriteSlot(home, "xai", label, material); err == nil {
		t.Fatal("a rename failure was reported clean")
	}

	restoreAuthStoreHooks(t)

	authStoreOpen = func(string) (*os.File, error) { return nil, errors.New("open") }
	if err := AuthWriteSlot(home, "xai", label, material); err == nil {
		t.Fatal("a directory sync failure was reported clean")
	}
}

// stubStoreHandle fails one step of the atomic store write so every guard in
// authSyncStoreFile is exercised.
type stubStoreHandle struct {
	writeErr error
	chmodErr error
	syncErr  error
	closed   int
}

func (h *stubStoreHandle) Name() string { return "stub" }

func (h *stubStoreHandle) Write([]byte) (int, error) { return 0, h.writeErr }

func (h *stubStoreHandle) Chmod(os.FileMode) error { return h.chmodErr }

func (h *stubStoreHandle) Sync() error { return h.syncErr }

func (h *stubStoreHandle) Close() error {
	h.closed++

	return nil
}

func TestAuthSyncStoreFileClosesOnEveryFailure(t *testing.T) {
	t.Parallel()

	cases := []*stubStoreHandle{
		{writeErr: errors.New("write")},
		{chmodErr: errors.New("chmod")},
		{syncErr: errors.New("sync")},
	}

	for _, handle := range cases {
		if err := authSyncStoreFile(handle, []byte("{}")); err == nil {
			t.Fatal("failure ignored")
		}

		if handle.closed != 1 {
			t.Fatalf("closed %d times", handle.closed)
		}
	}

	clean := &stubStoreHandle{}
	if err := authSyncStoreFile(clean, []byte("{}")); err != nil {
		t.Fatalf("clean write: %v", err)
	}
}

func TestAuthWriteStoreReportsAFailedFileWrite(t *testing.T) {
	restoreAuthStoreHooks(t)

	home := t.TempDir()
	authStoreCreate = func(string, string) (authStoreHandle, error) {
		return &stubStoreHandle{writeErr: errors.New("write")}, nil
	}

	if err := AuthWriteSlot(home, "xai", AuthSlotLabel("c1"), AuthMaterial{AccessToken: "t"}); err == nil {
		t.Fatal("a failed file write was reported clean")
	}
}

func TestAuthEntryFieldHelpers(t *testing.T) {
	t.Parallel()

	entry := authPoolEntry{
		"label":       json.RawMessage(`"x"`),
		"broken":      json.RawMessage(`{`),
		"number":      json.RawMessage(`5`),
		"not-numeric": json.RawMessage(`"five"`),
	}

	if authStringField(entry, "label") != "x" {
		t.Fatal("string field not read")
	}

	if authStringField(entry, "absent") != "" || authStringField(entry, "broken") != "" {
		t.Fatal("a malformed or absent string field was read")
	}

	if value, ok := authNumberField(entry, "number"); !ok || value != 5 {
		t.Fatalf("number field = %v, %v", value, ok)
	}

	if _, ok := authNumberField(entry, "absent"); ok {
		t.Fatal("an absent number field was read")
	}

	if _, ok := authNumberField(entry, "not-numeric"); ok {
		t.Fatal("a non-numeric field was read as a number")
	}

	built := authEntryFromMaterial("label", AuthMaterial{AccessToken: "t"})
	if string(built[authFieldExpiresAt]) != "null" {
		t.Fatalf("an absent expiry was not recorded as null: %s", built[authFieldExpiresAt])
	}

	if _, present := built[authFieldRefreshToken]; present {
		t.Fatal("an absent refresh token was written")
	}

	material := authMaterialFromEntry(authPoolEntry{"access_token": json.RawMessage(`"t"`)})
	if material.AuthType != AuthTypeOAuth {
		t.Fatalf("an untyped entry did not default to oauth: %#v", material)
	}

	unset := authPoolEntry{}
	authSetString(unset, "key", "value")

	if authStringField(unset, "key") != "value" {
		t.Fatal("authSetString did not record the value")
	}
}

func TestAuthAnchorExpiryPrefersTheAbsoluteValue(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)

	if got := AuthAnchorExpiry(now, AuthMaterial{AccessExpiresAt: 42}); got != 42 {
		t.Fatalf("absolute expiry = %d", got)
	}

	want := now.Add(time.Hour).UnixMilli()
	if got := AuthAnchorExpiry(now, AuthMaterial{ExpiresIn: time.Hour}); got != want {
		t.Fatalf("anchored expiry = %d, want %d", got, want)
	}

	if got := AuthAnchorExpiry(now, AuthMaterial{}); got != 0 {
		t.Fatalf("an absent expiry became %d", got)
	}
}

func TestAuthReadSlotIgnoresAnEmptyReservedToken(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	label := AuthSlotLabel("c1")

	writeAuthStore(t, home, map[string]any{
		authPoolSection: map[string]any{
			"xai": []any{map[string]any{"label": label, "auth_type": "oauth", "access_token": ""}},
		},
	})

	if _, present, err := AuthReadSlot(home, "xai", label); err != nil || present {
		t.Fatalf("an empty reserved token reported present = %v, %v", present, err)
	}
}
