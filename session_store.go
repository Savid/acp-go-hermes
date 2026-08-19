package hermesacp

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	SessionStoreMainSubpath = ""
	SessionStoreFormat      = "hermes-state-db-v1"
)

type SessionStoreEntry = json.RawMessage

type SessionKey struct {
	SessionID string
	Subpath   string
}

type SessionSummary struct {
	SessionID          string
	UpdatedAtUnixMilli int64
	Cwd                string
	Title              string
	Meta               map[string]any
}

type SessionStoreReplacement struct {
	Key     SessionKey
	Entries []SessionStoreEntry
}

type SessionStore interface {
	Append(ctx context.Context, key SessionKey, entries []SessionStoreEntry) error
	Load(ctx context.Context, key SessionKey) ([]SessionStoreEntry, error)
	Replace(ctx context.Context, main SessionKey, replacements []SessionStoreReplacement) error
	Delete(ctx context.Context, key SessionKey) error
	ListSessions(ctx context.Context) ([]SessionSummary, error)
	ListSubkeys(ctx context.Context, key SessionKey) ([]string, error)
}

type InMemorySessionStore struct {
	mu         sync.Mutex
	entries    map[SessionKey][]SessionStoreEntry
	updatedAt  map[SessionKey]int64
	tombstones map[SessionKey]int64
}

var _ SessionStore = (*InMemorySessionStore)(nil)

func NewInMemorySessionStore() *InMemorySessionStore {
	return &InMemorySessionStore{
		entries:    make(map[SessionKey][]SessionStoreEntry),
		updatedAt:  make(map[SessionKey]int64),
		tombstones: make(map[SessionKey]int64),
	}
}

func (s *InMemorySessionStore) Append(ctx context.Context, key SessionKey, entries []SessionStoreEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if s == nil {
		return fmt.Errorf("nil InMemorySessionStore")
	}

	if len(entries) == 0 {
		return nil
	}

	if key.SessionID == "" {
		return fmt.Errorf("session id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensureLocked()

	if s.isTombstonedLocked(key) {
		return nil
	}

	for _, entry := range entries {
		s.entries[key] = append(s.entries[key], cloneStoreEntry(entry))
	}

	s.updatedAt[key] = time.Now().UnixMilli()

	return nil
}

func (s *InMemorySessionStore) Load(ctx context.Context, key SessionKey) ([]SessionStoreEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if s == nil {
		return nil, fmt.Errorf("nil InMemorySessionStore")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isTombstonedLocked(key) {
		return nil, nil
	}

	return cloneStoreEntries(s.entries[key]), nil
}

func (s *InMemorySessionStore) Replace(ctx context.Context, main SessionKey, replacements []SessionStoreReplacement) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if s == nil {
		return fmt.Errorf("nil InMemorySessionStore")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensureLocked()

	if main.SessionID == "" {
		return fmt.Errorf("session id is required")
	}

	if main.Subpath != SessionStoreMainSubpath {
		return fmt.Errorf("main subpath must be %q", SessionStoreMainSubpath)
	}

	mainCount := 0
	listed := make(map[SessionKey]struct{}, len(replacements))

	for _, replacement := range replacements {
		if replacement.Key.SessionID != main.SessionID {
			return fmt.Errorf("replacement key does not match main session")
		}

		// One Replace states each key's whole content exactly once. A key listed
		// twice states two contents for it, and nothing in the call says which one
		// the caller meant, so the write is refused rather than settled by
		// position: silently keeping the last one would commit a generation the
		// caller never asked for.
		if _, duplicate := listed[replacement.Key]; duplicate {
			return fmt.Errorf("replacement key %q is listed more than once", replacement.Key.Subpath)
		}

		listed[replacement.Key] = struct{}{}

		if replacement.Key.Subpath == SessionStoreMainSubpath {
			mainCount++
		}
	}

	if mainCount != 1 {
		return fmt.Errorf("replacements must include the main key exactly once")
	}

	// A tombstone is final, and the store is where that finality lives: an
	// adapter-level deletion marker is one process's memory, while the deleted
	// state is the answer every reader of this store is owed. A generation
	// addressed to a session `Delete` removed writes nothing, clears nothing, and
	// succeeds — the same terms `Append` already answers on.
	if s.isTombstonedLocked(main) {
		return nil
	}

	now := time.Now().UnixMilli()

	for candidate := range s.entries {
		if candidate.SessionID == main.SessionID {
			delete(s.entries, candidate)
			delete(s.updatedAt, candidate)
			s.tombstones[candidate] = now
		}
	}

	for _, replacement := range replacements {
		// A listed key survives even when its entries are empty: exactly the
		// listed keys stay live (present in Load/ListSessions/ListSubkeys).
		s.entries[replacement.Key] = cloneStoreEntries(replacement.Entries)
		s.updatedAt[replacement.Key] = now
		delete(s.tombstones, replacement.Key)
	}

	return nil
}

func (s *InMemorySessionStore) Delete(ctx context.Context, key SessionKey) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if s == nil {
		return fmt.Errorf("nil InMemorySessionStore")
	}

	// Deleting a key without a session id is a pure no-op: it never plants a
	// tombstone that would silently swallow later writes.
	if key.SessionID == "" {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensureLocked()

	now := time.Now().UnixMilli()
	matched := false

	for candidate := range s.entries {
		if candidate.SessionID != key.SessionID {
			continue
		}

		if key.Subpath != SessionStoreMainSubpath && candidate.Subpath != key.Subpath {
			continue
		}

		delete(s.entries, candidate)
		delete(s.updatedAt, candidate)
		s.tombstones[candidate] = now
		matched = true
	}

	if !matched {
		s.tombstones[key] = now
	}

	if key.Subpath == SessionStoreMainSubpath {
		s.tombstones[mainSessionKey(key.SessionID)] = now
	}

	return nil
}

func (s *InMemorySessionStore) ListSessions(ctx context.Context) ([]SessionSummary, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if s == nil {
		return nil, fmt.Errorf("nil InMemorySessionStore")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	summaries := make([]SessionSummary, 0)

	for key, entries := range s.entries {
		if key.SessionID == "" || key.Subpath != SessionStoreMainSubpath || s.isTombstonedLocked(key) {
			continue
		}

		summary := SessionSummary{
			SessionID:          key.SessionID,
			UpdatedAtUnixMilli: s.updatedAt[key],
		}
		if len(entries) > 0 {
			summary = summaryFromStoreEntry(summary, entries[len(entries)-1])
		}

		summaries = append(summaries, summary)
	}

	slices.SortFunc(summaries, func(left, right SessionSummary) int {
		if byTime := cmp.Compare(right.UpdatedAtUnixMilli, left.UpdatedAtUnixMilli); byTime != 0 {
			return byTime
		}

		return strings.Compare(left.SessionID, right.SessionID)
	})

	return summaries, nil
}

func (s *InMemorySessionStore) ListSubkeys(ctx context.Context, key SessionKey) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if s == nil {
		return nil, fmt.Errorf("nil InMemorySessionStore")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	subpaths := make([]string, 0)

	for candidate := range s.entries {
		if candidate.SessionID != key.SessionID || candidate.Subpath == SessionStoreMainSubpath || s.isTombstonedLocked(candidate) {
			continue
		}

		subpaths = append(subpaths, candidate.Subpath)
	}

	slices.Sort(subpaths)

	return subpaths, nil
}

func (s *InMemorySessionStore) ensureLocked() {
	if s.entries == nil {
		s.entries = make(map[SessionKey][]SessionStoreEntry)
	}

	if s.updatedAt == nil {
		s.updatedAt = make(map[SessionKey]int64)
	}

	if s.tombstones == nil {
		s.tombstones = make(map[SessionKey]int64)
	}
}

func (s *InMemorySessionStore) isTombstonedLocked(key SessionKey) bool {
	if s.tombstones == nil {
		return false
	}

	if _, ok := s.tombstones[key]; ok {
		return true
	}

	if key.Subpath != SessionStoreMainSubpath {
		_, ok := s.tombstones[mainSessionKey(key.SessionID)]

		return ok
	}

	return false
}

func cloneStoreEntry(entry SessionStoreEntry) SessionStoreEntry {
	return append(SessionStoreEntry(nil), entry...)
}

func cloneStoreEntries(entries []SessionStoreEntry) []SessionStoreEntry {
	if len(entries) == 0 {
		return nil
	}

	clone := make([]SessionStoreEntry, 0, len(entries))
	for _, entry := range entries {
		clone = append(clone, cloneStoreEntry(entry))
	}

	return clone
}

func mainSessionKey(sessionID string) SessionKey {
	return SessionKey{SessionID: sessionID, Subpath: SessionStoreMainSubpath}
}

func summaryFromStoreEntry(summary SessionSummary, entry SessionStoreEntry) SessionSummary {
	var snapshot stateSnapshot
	if err := json.Unmarshal(entry, &snapshot); err != nil {
		return summary
	}

	if snapshot.CapturedAtUnixMilli > 0 {
		summary.UpdatedAtUnixMilli = snapshot.CapturedAtUnixMilli
	}

	summary.Cwd = snapshot.Session.Cwd
	summary.Title = snapshot.Session.Title
	summary.Meta = map[string]any{
		hermesMetaKey: map[string]any{
			hermesNativeIDMetaKey: snapshot.Session.NativeSessionID,
			"stored":              true,
			keySource:             "hermes-state",
		},
	}

	return summary
}
