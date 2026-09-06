//go:build !windows

// Every test in this file drives the shared Hermes home. Windows refuses that
// home — its inherited session-owner lock handles are unavailable — so the
// surface these tests exercise does not exist there; the Windows expectation is
// the refusal itself, proven once beside the code that makes it.

package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

//nolint:gocyclo // Recovery cases intentionally share one production-protocol fixture matrix.
func TestRecoverPendingSharedSessionOperations(t *testing.T) {
	t.Run("exact store commits and removes journal", func(t *testing.T) {
		home := durableTempDir(t)
		store := NewInMemorySessionStore()
		agent := newTestAgent(WithSharedHermesHome(home), WithSessionStore(store))
		journal := newRecoveryTestJournal(t, home, sessionOperationKindNew, "logical", nil)
		phase := sessionOperationPhaseNativeIdentified
		nativeID, liveID := "native-new", "live-new"
		if err := journal.update(sessionOperationJournalPatch{Phase: &phase, NativeSessionID: &nativeID, LiveSessionID: &liveID}); err != nil {
			t.Fatal(err)
		}
		replacements := []SessionStoreReplacement{{
			Key:     SessionKey{SessionID: "logical", Subpath: SessionStoreMainSubpath},
			Entries: []SessionStoreEntry{[]byte(`{"format":"test"}`)},
		}}
		if err := journal.prepareReplacements(replacements); err != nil {
			t.Fatal(err)
		}
		if err := store.Replace(t.Context(), replacements[0].Key, replacements); err != nil {
			t.Fatal(err)
		}
		if err := agent.recoverPendingSharedSessionOperations(t.Context(), home, newFakeHermesClient()); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(journal.directory); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("committed journal remains: %v", err)
		}
	})

	t.Run("absent fork exact delta is deleted", func(t *testing.T) {
		home := durableTempDir(t)
		agent := newTestAgent(WithSharedHermesHome(home), WithSessionStore(NewInMemorySessionStore()))
		journal := newRecoveryTestJournal(t, home, sessionOperationKindFork, "child", []string{"parent"})
		client := newFakeHermesClient()
		client.persistedSessions = []nativehermes.Session{{ID: "parent"}, {ID: "native-child", Title: journal.record.Marker}}
		if err := agent.recoverPendingSharedSessionOperations(t.Context(), home, client); err != nil {
			t.Fatal(err)
		}
		if len(client.deleted) != 1 || client.deleted[0] != "native-child" {
			t.Fatalf("deleted = %#v", client.deleted)
		}
		if _, err := os.Stat(journal.directory); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("recovered journal remains: %v", err)
		}
	})

	t.Run("absent new known native id is deleted", func(t *testing.T) {
		home := durableTempDir(t)
		agent := newTestAgent(WithSharedHermesHome(home), WithSessionStore(NewInMemorySessionStore()))
		journal := newRecoveryTestJournal(t, home, sessionOperationKindNew, "logical", nil)
		nativeID := "native-new"
		liveID := "live-new"
		phase := sessionOperationPhaseNativeIdentified
		if err := journal.update(sessionOperationJournalPatch{Phase: &phase, NativeSessionID: &nativeID, LiveSessionID: &liveID}); err != nil {
			t.Fatal(err)
		}
		client := newFakeHermesClient()
		client.persistedSessions = []nativehermes.Session{{ID: nativeID}}
		if err := agent.recoverPendingSharedSessionOperations(t.Context(), home, client); err != nil {
			t.Fatal(err)
		}
		if len(client.deleted) != 1 || client.deleted[0] != nativeID {
			t.Fatalf("deleted = %#v", client.deleted)
		}
	})

	t.Run("empty new journal never infers an unrelated durable row", func(t *testing.T) {
		home := durableTempDir(t)
		agent := newTestAgent(WithSharedHermesHome(home), WithSessionStore(NewInMemorySessionStore()))
		journal := newRecoveryTestJournal(t, home, sessionOperationKindNew, "logical", nil)
		client := newFakeHermesClient()
		client.persistedSessions = []nativehermes.Session{{ID: "unrelated", Title: journal.record.FinalTitle}}
		if err := agent.recoverPendingSharedSessionOperations(t.Context(), home, client); err != nil {
			t.Fatal(err)
		}
		if len(client.deleted) != 0 {
			t.Fatalf("empty New recovery deleted unrelated session: %#v", client.deleted)
		}
	})

	t.Run("live logical owner fences same-process recovery", func(t *testing.T) {
		home := durableTempDir(t)
		agent := newTestAgent(WithSharedHermesHome(home), WithSessionStore(NewInMemorySessionStore()))
		journal := newRecoveryTestJournal(t, home, sessionOperationKindNew, "logical", nil)
		nativeID, liveID := "native-new", "live-new"
		phase := sessionOperationPhaseNativeIdentified
		if err := journal.update(sessionOperationJournalPatch{Phase: &phase, NativeSessionID: &nativeID, LiveSessionID: &liveID}); err != nil {
			t.Fatal(err)
		}
		owner, err := nativehermes.AcquireSharedACPSessionOwner(home, "logical")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = owner.Release() }()
		client := newFakeHermesClient()
		client.persistedSessions = []nativehermes.Session{{ID: nativeID}}
		if err := agent.recoverPendingSharedSessionOperations(t.Context(), home, client); err == nil {
			t.Fatal("live logical owner did not fence recovery")
		}
		if len(client.deleted) != 0 {
			t.Fatalf("fenced recovery deleted %#v", client.deleted)
		}
	})

	t.Run("absent fork with no delta clears pre-effect journal", func(t *testing.T) {
		home := durableTempDir(t)
		agent := newTestAgent(WithSharedHermesHome(home), WithSessionStore(NewInMemorySessionStore()))
		journal := newRecoveryTestJournal(t, home, sessionOperationKindFork, "child", []string{"parent"})
		client := newFakeHermesClient()
		client.persistedSessions = []nativehermes.Session{{ID: "parent"}}
		if err := agent.recoverPendingSharedSessionOperations(t.Context(), home, client); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(journal.directory); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("no-delta journal remains: %v", err)
		}
	})

	t.Run("ambiguous fork delta fails closed", func(t *testing.T) {
		home := durableTempDir(t)
		agent := newTestAgent(WithSharedHermesHome(home), WithSessionStore(NewInMemorySessionStore()))
		journal := newRecoveryTestJournal(t, home, sessionOperationKindFork, "child", []string{"parent"})
		client := newFakeHermesClient()
		client.persistedSessions = []nativehermes.Session{{ID: "parent"}, {ID: "child-a"}, {ID: "child-b"}}
		if err := agent.recoverPendingSharedSessionOperations(t.Context(), home, client); !errors.Is(err, ErrSessionOperationAmbiguous) {
			t.Fatalf("recovery error = %v", err)
		}
		if len(client.deleted) != 0 {
			t.Fatalf("ambiguous recovery deleted %#v", client.deleted)
		}
		if _, err := os.Stat(journal.directory); err != nil {
			t.Fatalf("ambiguous journal was removed: %v", err)
		}
	})

	t.Run("fork marker mismatch preserves external row", func(t *testing.T) {
		home := durableTempDir(t)
		agent := newTestAgent(WithSharedHermesHome(home), WithSessionStore(NewInMemorySessionStore()))
		journal := newRecoveryTestJournal(t, home, sessionOperationKindFork, "child", []string{"parent"})
		client := newFakeHermesClient()
		client.persistedSessions = []nativehermes.Session{{ID: "parent"}, {ID: "external", Title: "not-our-marker"}}
		if err := agent.recoverPendingSharedSessionOperations(t.Context(), home, client); !errors.Is(err, ErrSessionOperationAmbiguous) {
			t.Fatalf("marker mismatch error = %v", err)
		}
		if len(client.deleted) != 0 {
			t.Fatalf("marker mismatch deleted %#v", client.deleted)
		}
		if _, err := os.Stat(journal.directory); err != nil {
			t.Fatalf("marker mismatch journal was removed: %v", err)
		}
	})

	t.Run("persisted inventory error retains journal", func(t *testing.T) {
		home := durableTempDir(t)
		agent := newTestAgent(WithSharedHermesHome(home), WithSessionStore(NewInMemorySessionStore()))
		journal := newRecoveryTestJournal(t, home, sessionOperationKindFork, "child", []string{"parent"})
		client := newFakeHermesClient()
		client.listErr = errors.New("inventory unavailable")
		if err := agent.recoverPendingSharedSessionOperations(t.Context(), home, client); err == nil {
			t.Fatal("inventory failure was ignored")
		}
		if len(client.deleted) != 0 {
			t.Fatalf("inventory failure deleted %#v", client.deleted)
		}
		if _, err := os.Stat(journal.directory); err != nil {
			t.Fatalf("inventory failure removed journal: %v", err)
		}
	})

	t.Run("operation owner fences recovery", func(t *testing.T) {
		home := durableTempDir(t)
		agent := newTestAgent(WithSharedHermesHome(home), WithSessionStore(NewInMemorySessionStore()))
		journal := newRecoveryTestJournal(t, home, sessionOperationKindFork, "child", []string{"parent"})
		owner, err := nativehermes.AcquireSharedNativeSessionOwner(home, "parent")
		if err != nil {
			t.Fatal(err)
		}
		client := newFakeHermesClient()
		client.persistedSessions = []nativehermes.Session{{ID: "parent"}}
		if err := agent.recoverPendingSharedSessionOperations(t.Context(), home, client); err == nil {
			t.Fatal("held parent owner did not fence dead-origin recovery")
		}
		if len(client.deleted) != 0 {
			t.Fatalf("owner-fenced recovery deleted %#v", client.deleted)
		}
		if err := owner.Release(); err != nil {
			t.Fatal(err)
		}
		if err := agent.recoverPendingSharedSessionOperations(t.Context(), home, client); err != nil {
			t.Fatalf("recovery after owner release: %v", err)
		}
		if _, err := os.Stat(journal.directory); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("recovered journal remains: %v", err)
		}
	})

	t.Run("store unavailable retains prepared journal", func(t *testing.T) {
		home := durableTempDir(t)
		agent := newTestAgent(WithSharedHermesHome(home), WithSessionStore(sessionOperationUnavailableStore{}))
		journal := newRecoveryTestJournal(t, home, sessionOperationKindNew, "logical", nil)
		nativeID, liveID := "native-new", "live-new"
		phase := sessionOperationPhaseNativeIdentified
		if err := journal.update(sessionOperationJournalPatch{Phase: &phase, NativeSessionID: &nativeID, LiveSessionID: &liveID}); err != nil {
			t.Fatal(err)
		}
		if err := journal.prepareReplacements([]SessionStoreReplacement{{
			Key: SessionKey{SessionID: "logical", Subpath: SessionStoreMainSubpath}, Entries: []SessionStoreEntry{[]byte(`{"format":"test"}`)},
		}}); err != nil {
			t.Fatal(err)
		}
		client := newFakeHermesClient()
		if err := agent.recoverPendingSharedSessionOperations(t.Context(), home, client); !errors.Is(err, ErrSessionOperationStoreUnavailable) {
			t.Fatalf("unavailable Store error = %v", err)
		}
		if len(client.deleted) != 0 {
			t.Fatalf("unavailable Store recovery deleted %#v", client.deleted)
		}
		if _, err := os.Stat(journal.directory); err != nil {
			t.Fatalf("unavailable Store recovery removed journal: %v", err)
		}
	})

	t.Run("exact store cleanup failures are nonblocking and retryable", func(t *testing.T) {
		home := durableTempDir(t)
		store := NewInMemorySessionStore()
		agent := newTestAgent(WithSharedHermesHome(home), WithSessionStore(store))
		journal := newRecoveryTestJournal(t, home, sessionOperationKindNew, "logical", nil)
		nativeID, liveID := "native-new", "live-new"
		phase := sessionOperationPhaseNativeIdentified
		if err := journal.update(sessionOperationJournalPatch{Phase: &phase, NativeSessionID: &nativeID, LiveSessionID: &liveID}); err != nil {
			t.Fatal(err)
		}
		replacements := []SessionStoreReplacement{{
			Key: SessionKey{SessionID: "logical", Subpath: SessionStoreMainSubpath}, Entries: []SessionStoreEntry{[]byte(`{"format":"test"}`)},
		}}
		if err := journal.prepareReplacements(replacements); err != nil {
			t.Fatal(err)
		}
		if err := store.Replace(t.Context(), replacements[0].Key, replacements); err != nil {
			t.Fatal(err)
		}

		originalRename := sessionOperationRename
		t.Cleanup(func() { sessionOperationRename = originalRename })
		sessionOperationRename = func(_, _ string) error { return errors.New("injected commit-marker failure") }
		if err := agent.recoverPendingSharedSessionOperations(t.Context(), home, newFakeHermesClient()); err != nil {
			t.Fatalf("exact Store was blocked by journal marker failure: %v", err)
		}
		sessionOperationRename = originalRename
		if _, err := os.Stat(journal.directory); err != nil {
			t.Fatalf("marker failure removed journal: %v", err)
		}

		originalRemoveAll := sessionOperationRemoveAll
		t.Cleanup(func() { sessionOperationRemoveAll = originalRemoveAll })
		sessionOperationRemoveAll = func(path string) error {
			if path == journal.directory {
				return errors.New("injected committed-journal removal failure")
			}

			return originalRemoveAll(path)
		}
		if err := agent.recoverPendingSharedSessionOperations(t.Context(), home, newFakeHermesClient()); err != nil {
			t.Fatalf("exact Store was blocked by committed-journal removal failure: %v", err)
		}
		sessionOperationRemoveAll = originalRemoveAll
		if _, err := os.Stat(journal.directory); err != nil {
			t.Fatalf("committed-journal removal failure removed journal: %v", err)
		}
		if err := agent.recoverPendingSharedSessionOperations(t.Context(), home, newFakeHermesClient()); err != nil {
			t.Fatalf("exact Store cleanup retry: %v", err)
		}
		if _, err := os.Stat(journal.directory); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("cleanup retry left journal: %v", err)
		}
	})

	t.Run("recovered journal removal retries without deleting twice", func(t *testing.T) {
		home := durableTempDir(t)
		agent := newTestAgent(WithSharedHermesHome(home), WithSessionStore(NewInMemorySessionStore()))
		journal := newRecoveryTestJournal(t, home, sessionOperationKindNew, "logical", nil)
		nativeID, liveID := "native-new", "live-new"
		phase := sessionOperationPhaseNativeIdentified
		if err := journal.update(sessionOperationJournalPatch{Phase: &phase, NativeSessionID: &nativeID, LiveSessionID: &liveID}); err != nil {
			t.Fatal(err)
		}
		client := newFakeHermesClient()
		client.persistedSessions = []nativehermes.Session{{ID: nativeID}}
		originalRemoveAll := sessionOperationRemoveAll
		t.Cleanup(func() { sessionOperationRemoveAll = originalRemoveAll })
		sessionOperationRemoveAll = func(path string) error {
			if path == journal.directory {
				return errors.New("injected journal removal failure")
			}

			return originalRemoveAll(path)
		}
		if err := agent.recoverPendingSharedSessionOperations(t.Context(), home, client); err == nil {
			t.Fatal("journal removal failure was ignored before publication")
		}
		sessionOperationRemoveAll = originalRemoveAll
		if len(client.deleted) != 1 || client.deleted[0] != nativeID {
			t.Fatalf("first recovery deletes = %#v", client.deleted)
		}
		if err := agent.recoverPendingSharedSessionOperations(t.Context(), home, client); err != nil {
			t.Fatalf("journal removal retry: %v", err)
		}
		if len(client.deleted) != 1 {
			t.Fatalf("recovery repeated native delete: %#v", client.deleted)
		}
		if _, err := os.Stat(journal.directory); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("cleanup retry left journal: %v", err)
		}
	})
}

func newRecoveryTestJournal(
	t *testing.T,
	home string,
	kind sessionOperationKind,
	logicalID string,
	baseline []string,
) *sessionOperationJournal {
	t.Helper()
	operationID, err := newSessionOperationID()
	if err != nil {
		t.Fatal(err)
	}
	journal, err := beginSessionOperationJournal(home, sessionOperationJournalFields{
		OperationID: operationID, Kind: kind, Mode: sessionOperationModeShared,
		LogicalSessionID: logicalID, ParentLogicalSessionID: "parent-logical",
		ParentNativeSessionID: "parent", Marker: operationID, FinalTitle: "title-" + logicalID,
		BaselineNativeSessionIDs: baseline,
	})
	if err != nil {
		t.Fatal(err)
	}
	phase := sessionOperationPhaseMutating
	if err := journal.update(sessionOperationJournalPatch{Phase: &phase}); err != nil {
		t.Fatal(err)
	}

	return journal
}

type sessionOperationRecoveryServer struct {
	*fakeHermesClient
	listCalls      int
	secondListErr  error
	retainOnDelete bool
}

func (s *sessionOperationRecoveryServer) PersistedSessions(ctx context.Context) ([]nativehermes.Session, error) {
	s.listCalls++
	if s.listCalls > 1 && s.secondListErr != nil {
		return nil, s.secondListErr
	}

	return s.fakeHermesClient.PersistedSessions(ctx)
}

func (s *sessionOperationRecoveryServer) DeleteSession(ctx context.Context, id string) error {
	if s.retainOnDelete {
		return nil
	}

	return s.fakeHermesClient.DeleteSession(ctx, id)
}

func TestSessionOperationRecoveryRemainingFailures(t *testing.T) {
	if err := newTestAgent().recoverPendingSharedSessionOperations(t.Context(), "relative", newFakeHermesClient()); err == nil {
		t.Fatal("journal discovery failure ignored")
	}

	t.Run("pending recovery requires inventory", func(t *testing.T) {
		home := durableTempDir(t)
		_ = newRecoveryTestJournal(t, home, sessionOperationKindNew, "logical", nil)
		client := sessionOperationServerOnly{Server: newFakeHermesClient()}
		if err := newTestAgent(WithSessionStore(NewInMemorySessionStore())).recoverPendingSharedSessionOperations(t.Context(), home, client); err == nil {
			t.Fatal("pending recovery without inventory accepted")
		}
	})

	t.Run("ambiguous store", func(t *testing.T) {
		home := durableTempDir(t)
		store := NewInMemorySessionStore()
		agent := newTestAgent(WithSessionStore(store))
		journal := newRecoveryTestJournal(t, home, sessionOperationKindNew, "logical", nil)
		nativeID, liveID := "native", "live"
		phase := sessionOperationPhaseNativeIdentified
		if err := journal.update(sessionOperationJournalPatch{Phase: &phase, NativeSessionID: &nativeID, LiveSessionID: &liveID}); err != nil {
			t.Fatal(err)
		}
		replacements := []SessionStoreReplacement{{
			Key: SessionKey{SessionID: "logical", Subpath: SessionStoreMainSubpath}, Entries: []SessionStoreEntry{json.RawMessage(`{"main":true}`)},
		}, {
			Key: SessionKey{SessionID: "logical", Subpath: "idmap"}, Entries: []SessionStoreEntry{json.RawMessage(`{"id":true}`)},
		}}
		if err := journal.prepareReplacements(replacements); err != nil {
			t.Fatal(err)
		}
		if err := store.Append(t.Context(), replacements[0].Key, []SessionStoreEntry{json.RawMessage(`{"different":true}`)}); err != nil {
			t.Fatal(err)
		}
		if err := agent.recoverPendingSharedSessionOperations(t.Context(), home, newFakeHermesClient()); !errors.Is(err, ErrSessionOperationAmbiguous) {
			t.Fatalf("ambiguous store error=%v", err)
		}
	})

	setupNative := func(t *testing.T) (string, *sessionOperationJournal) {
		t.Helper()
		home := durableTempDir(t)
		journal := newRecoveryTestJournal(t, home, sessionOperationKindNew, "logical", nil)
		nativeID, liveID := "native", "live"
		phase := sessionOperationPhaseNativeIdentified
		if err := journal.update(sessionOperationJournalPatch{Phase: &phase, NativeSessionID: &nativeID, LiveSessionID: &liveID}); err != nil {
			t.Fatal(err)
		}

		return home, journal
	}

	t.Run("child owner", func(t *testing.T) {
		home, _ := setupNative(t)
		owner, err := nativehermes.AcquireSharedNativeSessionOwner(home, "native")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = owner.Release() }()
		client := newFakeHermesClient()
		client.persistedSessions = []nativehermes.Session{{ID: "native"}}
		if err := newTestAgent(WithSessionStore(NewInMemorySessionStore())).recoverPendingSharedSessionOperations(t.Context(), home, client); err == nil {
			t.Fatal("held child owner did not fence recovery")
		}
	})

	t.Run("delete", func(t *testing.T) {
		home, _ := setupNative(t)
		client := newFakeHermesClient()
		client.persistedSessions = []nativehermes.Session{{ID: "native"}}
		client.deleteErr = errors.New("delete")
		if err := newTestAgent(WithSessionStore(NewInMemorySessionStore())).recoverPendingSharedSessionOperations(t.Context(), home, client); err == nil {
			t.Fatal("delete failure ignored")
		}
	})

	t.Run("verify inventory", func(t *testing.T) {
		home, _ := setupNative(t)
		client := &sessionOperationRecoveryServer{fakeHermesClient: newFakeHermesClient(), secondListErr: errors.New("verify")}
		client.persistedSessions = []nativehermes.Session{{ID: "native"}}
		if err := newTestAgent(WithSessionStore(NewInMemorySessionStore())).recoverPendingSharedSessionOperations(t.Context(), home, client); err == nil {
			t.Fatal("verification inventory failure ignored")
		}
	})

	t.Run("native remains", func(t *testing.T) {
		home, _ := setupNative(t)
		client := &sessionOperationRecoveryServer{fakeHermesClient: newFakeHermesClient(), retainOnDelete: true}
		client.persistedSessions = []nativehermes.Session{{ID: "native"}}
		if err := newTestAgent(WithSessionStore(NewInMemorySessionStore())).recoverPendingSharedSessionOperations(t.Context(), home, client); !errors.Is(err, ErrSessionOperationAmbiguous) {
			t.Fatalf("remaining native error=%v", err)
		}
	})
}
