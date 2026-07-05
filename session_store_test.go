package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

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

func TestInMemoryStoreAppendLoadDeleteListAndErrors(t *testing.T) {
	ctx := context.Background()
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	key := SessionKey{SessionID: "s1", Subpath: SessionStoreMainSubpath}
	subkey := SessionKey{SessionID: "s1", Subpath: "idmap"}
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
	if err := store.Append(ctx, subkey, []SessionStoreEntry{json.RawMessage(`{"sub":true}`)}); err != nil {
		t.Fatalf("append subkey: %v", err)
	}
	if err := store.Append(ctx, SessionKey{SessionID: "s0", Subpath: SessionStoreMainSubpath}, []SessionStoreEntry{json.RawMessage(`{bad}`)}); err != nil {
		t.Fatalf("append invalid summary: %v", err)
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
	if err := store.Delete(ctx, SessionKey{SessionID: "missing", Subpath: "sub"}); err != nil {
		t.Fatalf("delete missing: %v", err)
	}
	if err := store.Delete(ctx, subkey); err != nil {
		t.Fatalf("delete subkey: %v", err)
	}
	if err := store.Append(ctx, subkey, []SessionStoreEntry{json.RawMessage(`{"ignored":true}`)}); err != nil {
		t.Fatalf("append tombstoned subkey: %v", err)
	}
	loadedSubkey, err := store.Load(ctx, subkey)
	if err != nil {
		t.Fatalf("load tombstoned subkey: %v", err)
	}
	if len(loadedSubkey) != 0 {
		t.Fatalf("tombstoned subkey loaded entries: %#v", loadedSubkey)
	}
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("delete main: %v", err)
	}
	if err := store.Append(ctx, SessionKey{SessionID: "s1", Subpath: "other"}, []SessionStoreEntry{json.RawMessage(`{"ignored":true}`)}); err != nil {
		t.Fatalf("append tombstoned main subkey: %v", err)
	}
	loadedMain, err := store.Load(ctx, key)
	if err != nil {
		t.Fatalf("load tombstoned main: %v", err)
	}
	if len(loadedMain) != 0 {
		t.Fatalf("tombstoned main loaded entries: %#v", loadedMain)
	}

	tieStore := NewInMemorySessionStore()
	for _, id := range []string{"b", "a"} {
		if err := tieStore.Append(ctx, SessionKey{SessionID: id, Subpath: SessionStoreMainSubpath}, []SessionStoreEntry{json.RawMessage(`{}`)}); err != nil {
			t.Fatalf("append tie %s: %v", id, err)
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
	if err := store.Replace(ctx, SessionKey{}, nil); err == nil {
		t.Fatal("replace accepted missing main session id")
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
