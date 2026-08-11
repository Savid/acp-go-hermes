package hermesacp

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

const sessionOperationRecoveryOriginHelperEnv = "ACP_GO_HERMES_RECOVERY_ORIGIN_HELPER"

func TestSessionOperationRecoveryOriginHelper(t *testing.T) {
	identityPath := os.Getenv(sessionOperationRecoveryOriginHelperEnv)
	if identityPath == "" {
		return
	}

	origin, err := nativehermes.CurrentDurableProcessIdentity()
	if err != nil {
		t.Fatal(err)
	}

	data, err := json.Marshal(origin)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(identityPath, data, 0o600); err != nil {
		t.Fatal(err)
	}

	select {}
}

//nolint:gocyclo // Recovery cases intentionally share one production-protocol fixture matrix.
func TestRecoverPendingSharedSessionOperations(t *testing.T) {
	t.Run("exact store commits and removes journal", func(t *testing.T) {
		home := t.TempDir()
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
		home := t.TempDir()
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
		home := t.TempDir()
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
		home := t.TempDir()
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
		home := t.TempDir()
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
		home := t.TempDir()
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
		home := t.TempDir()
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
		home := t.TempDir()
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
		home := t.TempDir()
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

	t.Run("foreign live origin refuses without deleting", func(t *testing.T) {
		home := t.TempDir()
		agent := newTestAgent(WithSharedHermesHome(home), WithSessionStore(NewInMemorySessionStore()))
		journal := newRecoveryTestJournal(t, home, sessionOperationKindNew, "logical", nil)
		origin, stop := startRecoveryOriginHelper(t)
		defer stop()
		if updateErr := journal.update(sessionOperationJournalPatch{Origin: &origin}); updateErr != nil {
			t.Fatal(updateErr)
		}
		client := newFakeHermesClient()
		if err := agent.recoverPendingSharedSessionOperations(t.Context(), home, client); !errors.Is(err, ErrSessionOperationAmbiguous) {
			t.Fatalf("live-origin recovery error = %v", err)
		}
		if len(client.deleted) != 0 {
			t.Fatalf("live-origin recovery deleted %#v", client.deleted)
		}
		if _, err := os.Stat(journal.directory); err != nil {
			t.Fatalf("live-origin recovery removed journal: %v", err)
		}
	})

	t.Run("dead origin still requires operation owner", func(t *testing.T) {
		home := t.TempDir()
		agent := newTestAgent(WithSharedHermesHome(home), WithSessionStore(NewInMemorySessionStore()))
		journal := newRecoveryTestJournal(t, home, sessionOperationKindFork, "child", []string{"parent"})
		origin, err := nativehermes.CurrentDurableProcessIdentity()
		if err != nil {
			t.Fatal(err)
		}
		origin.KernelStartTime += "-proven-reused"
		if updateErr := journal.update(sessionOperationJournalPatch{Origin: &origin}); updateErr != nil {
			t.Fatal(updateErr)
		}
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
		home := t.TempDir()
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
		home := t.TempDir()
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
		home := t.TempDir()
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

func startRecoveryOriginHelper(t *testing.T) (nativehermes.DurableProcessIdentity, func()) {
	t.Helper()
	identityPath := filepath.Join(t.TempDir(), "origin.json")
	command := exec.Command(os.Args[0], "-test.run=^TestSessionOperationRecoveryOriginHelper$")
	command.Env = append(os.Environ(), sessionOperationRecoveryOriginHelperEnv+"="+identityPath)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		_ = command.Process.Kill()
		_ = command.Wait()
	}
	t.Cleanup(stop)

	deadline := time.Now().Add(5 * time.Second)
	for {
		data, err := os.ReadFile(identityPath)
		if err == nil {
			var origin nativehermes.DurableProcessIdentity
			if unmarshalErr := json.Unmarshal(data, &origin); unmarshalErr != nil {
				stop()
				t.Fatal(unmarshalErr)
			}

			return origin, stop
		}
		if !errors.Is(err, os.ErrNotExist) {
			stop()
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			stop()
			t.Fatal("timed out waiting for recovery-origin helper")
		}
		time.Sleep(10 * time.Millisecond)
	}
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
	origin, err := nativehermes.CurrentDurableProcessIdentity()
	if err != nil {
		t.Fatal(err)
	}
	journal, err := beginSessionOperationJournal(home, sessionOperationJournalFields{
		OperationID: operationID, Kind: kind, Mode: sessionOperationModeShared,
		LogicalSessionID: logicalID, ParentLogicalSessionID: "parent-logical",
		ParentNativeSessionID: "parent", Marker: operationID, FinalTitle: "title-" + logicalID,
		BaselineNativeSessionIDs: baseline, Origin: origin,
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
