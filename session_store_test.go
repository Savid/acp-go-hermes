package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// TestInMemoryStoreEnforcesTombstoneFinality pins the store's own last word on a
// deleted id. Both writing verbs answer it: neither `Append` nor `Replace` may
// clear a tombstone it did not create, because the deleted state is the answer
// every reader of this store is owed and an adapter-level deletion marker is
// only one process's memory of it.
func TestInMemoryStoreEnforcesTombstoneFinality(t *testing.T) {
	ctx := context.Background()
	store := NewInMemorySessionStore()
	main := SessionKey{SessionID: "s1", Subpath: SessionStoreMainSubpath}
	idmap := SessionKey{SessionID: "s1", Subpath: "idmap"}

	if err := store.Replace(ctx, main, []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{json.RawMessage(`{"format":"hermes-state-db-v1"}`)}},
		{Key: idmap, Entries: []SessionStoreEntry{json.RawMessage(`{"sessionId":"s1"}`)}},
	}); err != nil {
		t.Fatalf("seed replace: %v", err)
	}
	if err := store.Delete(ctx, main); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if err := store.Replace(ctx, main, []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{json.RawMessage(`{"format":"hermes-state-db-v1"}`)}},
		{Key: idmap, Entries: []SessionStoreEntry{json.RawMessage(`{"sessionId":"s1"}`)}},
	}); err != nil {
		t.Fatalf("replace over a tombstone must succeed without writing: %v", err)
	}
	if err := store.Append(ctx, main, []SessionStoreEntry{json.RawMessage(`{"appended":true}`)}); err != nil {
		t.Fatalf("append over a tombstone must succeed without writing: %v", err)
	}

	for _, key := range []SessionKey{main, idmap} {
		loaded, err := store.Load(ctx, key)
		if err != nil {
			t.Fatalf("load %q: %v", key.Subpath, err)
		}
		if len(loaded) != 0 {
			t.Fatalf("a write cleared a tombstone it did not create at %q: %#v", key.Subpath, loaded)
		}
	}

	sessions, err := store.ListSessions(ctx)
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(sessions) != 0 {
		t.Fatalf("a tombstoned session was listed after a later write: %#v", sessions)
	}

	subkeys, err := store.ListSubkeys(ctx, main)
	if err != nil {
		t.Fatalf("ListSubkeys: %v", err)
	}
	if len(subkeys) != 0 {
		t.Fatalf("a tombstoned session listed subkeys after a later write: %#v", subkeys)
	}

	// A different session is untouched by the tombstone on this one.
	other := SessionKey{SessionID: "s2", Subpath: SessionStoreMainSubpath}
	if replaceErr := store.Replace(ctx, other, []SessionStoreReplacement{
		{Key: other, Entries: []SessionStoreEntry{json.RawMessage(`{"format":"hermes-state-db-v1"}`)}},
	}); replaceErr != nil {
		t.Fatalf("replace an untombstoned session: %v", replaceErr)
	}
	loaded, err := store.Load(ctx, other)
	if err != nil || len(loaded) != 1 {
		t.Fatalf("untombstoned session load = %#v err=%v", loaded, err)
	}
}

func TestInMemoryStoreReplaceTombstonesUnlistedSubpaths(t *testing.T) {
	ctx := context.Background()
	store := NewInMemorySessionStore()
	main := SessionKey{SessionID: "s1", Subpath: SessionStoreMainSubpath}
	if err := store.Replace(ctx, main, []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{json.RawMessage(`{"format":"hermes-state-db-v1"}`)}},
		{Key: SessionKey{SessionID: "s1", Subpath: "idmap"}, Entries: []SessionStoreEntry{json.RawMessage(`{"sessionId":"s1"}`)}},
		{Key: SessionKey{SessionID: "s1", Subpath: "old"}, Entries: []SessionStoreEntry{json.RawMessage(`{}`)}},
	}); err != nil {
		t.Fatalf("first replace: %v", err)
	}
	if err := store.Replace(ctx, main, []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{json.RawMessage(`{"format":"hermes-state-db-v1"}`)}},
		{Key: SessionKey{SessionID: "s1", Subpath: "idmap"}, Entries: []SessionStoreEntry{json.RawMessage(`{"sessionId":"s1"}`)}},
	}); err != nil {
		t.Fatalf("second replace: %v", err)
	}
	subkeys, err := store.ListSubkeys(ctx, main)
	if err != nil {
		t.Fatalf("ListSubkeys: %v", err)
	}
	if len(subkeys) != 1 || subkeys[0] != "idmap" {
		t.Fatalf("subkeys = %#v", subkeys)
	}
	old, err := store.Load(ctx, SessionKey{SessionID: "s1", Subpath: "old"})
	if err != nil {
		t.Fatalf("Load old: %v", err)
	}
	if len(old) != 0 {
		t.Fatalf("old subpath visible: %#v", old)
	}
}

func TestInMemoryStoreReplaceEmptyEntryKeySurvives(t *testing.T) {
	ctx := context.Background()
	store := NewInMemorySessionStore()
	main := SessionKey{SessionID: "s1", Subpath: SessionStoreMainSubpath}
	sub := SessionKey{SessionID: "s1", Subpath: "idmap"}
	if err := store.Replace(ctx, main, []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{json.RawMessage(`{"format":"hermes-state-db-v1"}`)}},
		{Key: sub},
	}); err != nil {
		t.Fatalf("replace: %v", err)
	}

	subkeys, err := store.ListSubkeys(ctx, main)
	if err != nil {
		t.Fatalf("ListSubkeys: %v", err)
	}
	if len(subkeys) != 1 || subkeys[0] != "idmap" {
		t.Fatalf("listed empty-entry subkey dropped: %#v", subkeys)
	}

	loaded, err := store.Load(ctx, sub)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded) != 0 {
		t.Fatalf("empty-entry subkey had entries: %#v", loaded)
	}

	if err = store.Append(ctx, sub, []SessionStoreEntry{json.RawMessage(`{"x":1}`)}); err != nil {
		t.Fatalf("append to survived subkey: %v", err)
	}
	loaded, err = store.Load(ctx, sub)
	if err != nil || len(loaded) != 1 {
		t.Fatalf("append after survive: %#v err=%v", loaded, err)
	}

	sessions, err := store.ListSessions(ctx)
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(sessions) != 1 || sessions[0].SessionID != "s1" {
		t.Fatalf("main not listed after empty-entry replace: %#v", sessions)
	}
}

func TestInMemoryStoreAppendLoadDeleteListAndErrors(t *testing.T) {
	ctx := context.Background()
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	key := SessionKey{SessionID: "s1", Subpath: SessionStoreMainSubpath}
	subkey := SessionKey{SessionID: "s1", Subpath: "idmap"}
	testInMemoryStoreNilAndCanceledCalls(t, ctx, cancelled, key)

	store := &InMemorySessionStore{}
	if err := store.Append(ctx, key, nil); err != nil {
		t.Fatalf("append empty: %v", err)
	}
	entry := SessionStoreEntry(`{
			"capturedAtUnixMilli": 200,
			"session": {"cwd": "/repo", "title": "Stored", "nativeSessionId": "native-1"}
		}`)
	if err := store.Append(ctx, key, []SessionStoreEntry{entry}); err != nil {
		t.Fatalf("append main: %v", err)
	}
	entry[0] = '['
	loaded, err := store.Load(ctx, key)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if string(loaded[0]) == string(entry) {
		t.Fatal("store entry was not cloned")
	}
	loaded[0][0] = '['
	loadedAgain, err := store.Load(ctx, key)
	if err != nil {
		t.Fatalf("load again: %v", err)
	}
	if loadedAgain[0][0] == '[' {
		t.Fatal("loaded entry was not cloned")
	}
	if err2 := store.Append(ctx, subkey, []SessionStoreEntry{json.RawMessage(`{"sub":true}`)}); err2 != nil {
		t.Fatalf("append subkey: %v", err2)
	}
	if err3 := store.Append(ctx, SessionKey{SessionID: "s0", Subpath: SessionStoreMainSubpath}, []SessionStoreEntry{json.RawMessage(`{bad}`)}); err3 != nil {
		t.Fatalf("append invalid summary: %v", err3)
	}
	summaries, err := store.ListSessions(ctx)
	if err != nil {
		t.Fatalf("list sessions: %v", err)
	}
	var stored *SessionSummary
	for i := range summaries {
		if summaries[i].SessionID == "s1" {
			stored = &summaries[i]
		}
	}
	if len(summaries) != 2 || stored == nil || stored.Cwd != "/repo" || stored.Title != "Stored" {
		t.Fatalf("summaries = %#v", summaries)
	}
	subkeys, err := store.ListSubkeys(ctx, key)
	if err != nil {
		t.Fatalf("list subkeys: %v", err)
	}
	if len(subkeys) != 1 || subkeys[0] != "idmap" {
		t.Fatalf("subkeys = %#v", subkeys)
	}
	testInMemoryStoreTombstonedAppends(t, ctx, store, key, subkey)
	testInMemoryStoreTieOrderingAndNilTombstones(t, ctx)
}

func testInMemoryStoreNilAndCanceledCalls(t *testing.T, ctx, cancelled context.Context, key SessionKey) {
	t.Helper()

	var nilStore *InMemorySessionStore

	for name, fn := range map[string]func(context.Context) error{
		"append": func(ctx context.Context) error {
			return nilStore.Append(ctx, key, []SessionStoreEntry{json.RawMessage(`{}`)})
		},
		"load": func(ctx context.Context) error {
			_, err := nilStore.Load(ctx, key)

			return err
		},
		"replace": func(ctx context.Context) error {
			return nilStore.Replace(ctx, key, []SessionStoreReplacement{{Key: key, Entries: []SessionStoreEntry{json.RawMessage(`{}`)}}})
		},
		"delete": func(ctx context.Context) error { return nilStore.Delete(ctx, key) },
		"list": func(ctx context.Context) error {
			_, err := nilStore.ListSessions(ctx)

			return err
		},
		"subkeys": func(ctx context.Context) error {
			_, err := nilStore.ListSubkeys(ctx, key)

			return err
		},
	} {
		t.Run(name+" canceled", func(t *testing.T) {
			if err := fn(cancelled); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled err = %v", err)
			}
		})
		t.Run(name+" nil", func(t *testing.T) {
			if err := fn(ctx); err == nil {
				t.Fatal("nil store call succeeded")
			}
		})
	}
}

func testInMemoryStoreTombstonedAppends(t *testing.T, ctx context.Context, store *InMemorySessionStore, key, subkey SessionKey) {
	t.Helper()

	if err4 := store.Delete(ctx, SessionKey{SessionID: "missing", Subpath: "sub"}); err4 != nil {
		t.Fatalf("delete missing: %v", err4)
	}
	if err5 := store.Delete(ctx, subkey); err5 != nil {
		t.Fatalf("delete subkey: %v", err5)
	}
	if err6 := store.Append(ctx, subkey, []SessionStoreEntry{json.RawMessage(`{"ignored":true}`)}); err6 != nil {
		t.Fatalf("append tombstoned subkey: %v", err6)
	}
	loadedSubkey, err := store.Load(ctx, subkey)
	if err != nil {
		t.Fatalf("load tombstoned subkey: %v", err)
	}
	if len(loadedSubkey) != 0 {
		t.Fatalf("tombstoned subkey loaded entries: %#v", loadedSubkey)
	}
	if err7 := store.Delete(ctx, key); err7 != nil {
		t.Fatalf("delete main: %v", err7)
	}
	if err8 := store.Append(ctx, SessionKey{SessionID: "s1", Subpath: "other"}, []SessionStoreEntry{json.RawMessage(`{"ignored":true}`)}); err8 != nil {
		t.Fatalf("append tombstoned main subkey: %v", err8)
	}
	loadedMain, err := store.Load(ctx, key)
	if err != nil {
		t.Fatalf("load tombstoned main: %v", err)
	}
	if len(loadedMain) != 0 {
		t.Fatalf("tombstoned main loaded entries: %#v", loadedMain)
	}
}

func testInMemoryStoreTieOrderingAndNilTombstones(t *testing.T, ctx context.Context) {
	t.Helper()

	tieStore := NewInMemorySessionStore()
	for _, id := range []string{"b", "a"} {
		if err9 := tieStore.Append(ctx, SessionKey{SessionID: id, Subpath: SessionStoreMainSubpath}, []SessionStoreEntry{json.RawMessage(`{}`)}); err9 != nil {
			t.Fatalf("append tie %s: %v", id, err9)
		}
	}
	tieStore.mu.Lock()
	tieStore.updatedAt[SessionKey{SessionID: "a", Subpath: SessionStoreMainSubpath}] = 1
	tieStore.updatedAt[SessionKey{SessionID: "b", Subpath: SessionStoreMainSubpath}] = 1
	tieStore.mu.Unlock()
	tied, err := tieStore.ListSessions(ctx)
	if err != nil {
		t.Fatalf("list tied sessions: %v", err)
	}
	if len(tied) != 2 || tied[0].SessionID != "a" || tied[1].SessionID != "b" {
		t.Fatalf("tied summaries = %#v", tied)
	}
	if (&InMemorySessionStore{}).isTombstonedLocked(SessionKey{SessionID: "s"}) {
		t.Fatal("nil tombstones reported tombstoned")
	}
}

func TestInMemoryStoreReplaceValidation(t *testing.T) {
	ctx := context.Background()
	store := NewInMemorySessionStore()
	main := SessionKey{SessionID: "s1", Subpath: SessionStoreMainSubpath}
	for name, replacements := range map[string][]SessionStoreReplacement{
		"missing main":  {{Key: SessionKey{SessionID: "s1", Subpath: "sub"}, Entries: []SessionStoreEntry{json.RawMessage(`{}`)}}},
		"wrong session": {{Key: SessionKey{SessionID: "s2", Subpath: SessionStoreMainSubpath}, Entries: []SessionStoreEntry{json.RawMessage(`{}`)}}},
		"duplicate main": {
			{Key: main, Entries: []SessionStoreEntry{json.RawMessage(`{}`)}},
			{Key: main, Entries: []SessionStoreEntry{json.RawMessage(`{}`)}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := store.Replace(ctx, main, replacements); err == nil {
				t.Fatal("replace unexpectedly succeeded")
			}
		})
	}
	if err := store.Replace(ctx, SessionKey{}, nil); err == nil || err.Error() != "session id is required" {
		t.Fatalf("replace empty session id error = %v", err)
	}
	if err := store.Replace(ctx, SessionKey{SessionID: "s1", Subpath: "sub"}, nil); err == nil {
		t.Fatal("replace accepted non-main subpath")
	}
	if err := store.Replace(ctx, main, []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{json.RawMessage(`{"ok":true}`)}},
		{Key: SessionKey{SessionID: "s1", Subpath: "empty"}, Entries: nil},
	}); err != nil {
		t.Fatalf("replace with empty subkey: %v", err)
	}
}

// TestInMemoryStoreReplaceIsOneSessionsGeneration is the store-contract
// conformance test for the two shapes a single Replace may not carry.
//
// Every Replace is one session's whole generation: it sweeps that session's
// keys and writes exactly the listed ones, atomically. A replacement naming a
// different session would ride that sweep into a session the call does not
// state, and a key listed twice states two contents for one key with no rule
// for choosing between them. Both are refused before anything is written, and
// each refusal names the offending key in full so the caller can fix the exact
// replacement rather than re-deriving which one was wrong.
func TestInMemoryStoreReplaceIsOneSessionsGeneration(t *testing.T) {
	ctx := context.Background()
	main := SessionKey{SessionID: "s1", Subpath: SessionStoreMainSubpath}
	idmap := SessionKey{SessionID: "s1", Subpath: "idmap"}
	foreign := SessionKey{SessionID: "s2", Subpath: "idmap"}

	seed := func(t *testing.T) *InMemorySessionStore {
		t.Helper()

		store := NewInMemorySessionStore()
		if err := store.Replace(ctx, main, []SessionStoreReplacement{
			{Key: main, Entries: []SessionStoreEntry{json.RawMessage(`{"generation":1}`)}},
			{Key: idmap, Entries: []SessionStoreEntry{json.RawMessage(`{"idmap":1}`)}},
		}); err != nil {
			t.Fatalf("seed replace: %v", err)
		}
		if err := store.Replace(ctx, SessionKey{SessionID: "s2", Subpath: SessionStoreMainSubpath}, []SessionStoreReplacement{
			{Key: SessionKey{SessionID: "s2", Subpath: SessionStoreMainSubpath}, Entries: []SessionStoreEntry{json.RawMessage(`{"peer":1}`)}},
			{Key: foreign, Entries: []SessionStoreEntry{json.RawMessage(`{"peer-idmap":1}`)}},
		}); err != nil {
			t.Fatalf("seed peer replace: %v", err)
		}

		return store
	}

	// The offending replacement is last in both cases, so a store that wrote as
	// it walked would already have committed the two valid ones before refusing.
	for _, test := range []struct {
		name         string
		replacements []SessionStoreReplacement
		wantNamed    string
	}{
		{
			name: "foreign session",
			replacements: []SessionStoreReplacement{
				{Key: main, Entries: []SessionStoreEntry{json.RawMessage(`{"generation":2}`)}},
				{Key: idmap, Entries: []SessionStoreEntry{json.RawMessage(`{"idmap":2}`)}},
				{Key: foreign, Entries: []SessionStoreEntry{json.RawMessage(`{"peer-idmap":2}`)}},
			},
			wantNamed: storeKeyName(foreign),
		},
		{
			name: "duplicate key",
			replacements: []SessionStoreReplacement{
				{Key: main, Entries: []SessionStoreEntry{json.RawMessage(`{"generation":2}`)}},
				{Key: idmap, Entries: []SessionStoreEntry{json.RawMessage(`{"idmap":2}`)}},
				{Key: idmap, Entries: []SessionStoreEntry{json.RawMessage(`{"idmap":3}`)}},
			},
			wantNamed: storeKeyName(idmap),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := seed(t)

			err := store.Replace(ctx, main, test.replacements)
			if err == nil {
				t.Fatal("replace accepted a generation that is not one session's")
			}
			if !strings.Contains(err.Error(), test.wantNamed) {
				t.Fatalf("refusal = %v, want it to name %s", err, test.wantNamed)
			}

			// Nothing was written: every key both sessions held still carries the
			// content the seed committed, and no key gained a generation.
			for key, want := range map[SessionKey]string{
				main:    `{"generation":1}`,
				idmap:   `{"idmap":1}`,
				foreign: `{"peer-idmap":1}`,
				{SessionID: "s2", Subpath: SessionStoreMainSubpath}: `{"peer":1}`,
			} {
				entries, loadErr := store.Load(ctx, key)
				if loadErr != nil {
					t.Fatalf("load %s: %v", storeKeyName(key), loadErr)
				}
				if len(entries) != 1 || string(entries[0]) != want {
					t.Fatalf("%s entries = %s, want the seeded %s", storeKeyName(key), entries, want)
				}
			}

			// The refused session's key set is untouched too, so the sweep the
			// generation would have run never started.
			subkeys, subErr := store.ListSubkeys(ctx, main)
			if subErr != nil {
				t.Fatalf("list subkeys: %v", subErr)
			}
			if len(subkeys) != 1 || subkeys[0] != idmap.Subpath {
				t.Fatalf("subkeys after refusal = %v, want only %q", subkeys, idmap.Subpath)
			}
		})
	}
}

func TestInMemoryStoreEmptySessionIDKeys(t *testing.T) {
	ctx := context.Background()
	store := NewInMemorySessionStore()

	err := store.Append(ctx, SessionKey{}, []SessionStoreEntry{json.RawMessage(`{}`)})
	if err == nil || !strings.Contains(err.Error(), "session id is required") {
		t.Fatalf("append empty session id error = %v, want session id is required", err)
	}
	err = store.Append(ctx, SessionKey{Subpath: "idmap"}, []SessionStoreEntry{json.RawMessage(`{}`)})
	if err == nil || !strings.Contains(err.Error(), "session id is required") {
		t.Fatalf("append empty session id subkey error = %v, want session id is required", err)
	}

	// Delete of an empty-SessionID key is a pure no-op: no error and no
	// tombstone that would swallow later writes.
	if deleteErr := store.Delete(ctx, SessionKey{}); deleteErr != nil {
		t.Fatalf("delete empty session id: %v", deleteErr)
	}
	if deleteErr := store.Delete(ctx, SessionKey{Subpath: "idmap"}); deleteErr != nil {
		t.Fatalf("delete empty session id subkey: %v", deleteErr)
	}
	store.mu.Lock()
	tombstones := len(store.tombstones)
	store.mu.Unlock()
	if tombstones != 0 {
		t.Fatalf("tombstones after empty-key deletes = %d, want 0", tombstones)
	}

	key := SessionKey{SessionID: "s1", Subpath: SessionStoreMainSubpath}
	if appendErr := store.Append(ctx, key, []SessionStoreEntry{json.RawMessage(`{}`)}); appendErr != nil {
		t.Fatalf("append after empty-key deletes: %v", appendErr)
	}
	entries, loadErr := store.Load(ctx, key)
	if loadErr != nil || len(entries) != 1 {
		t.Fatalf("load after empty-key deletes entries=%d err=%v", len(entries), loadErr)
	}
}
