//go:build !windows

package hermesacp

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
)

// The tests below drive the shared Hermes home. Windows refuses that home —
// its inherited session-owner lock handles are unavailable — so the surface
// they exercise does not exist there; the Windows expectation is the refusal
// itself, proven once beside the code that makes it.

func TestSnapshotJournalAndStoreReconciliationEdges(t *testing.T) {
	t.Run("journal preparation", func(t *testing.T) {
		agent := newTestAgent(WithSessionStore(NewInMemorySessionStore()))
		session := testSession(t, agent, newFakeHermesClient())
		journal := newTestSessionOperationJournalWithLogical(t, durableTempDir(t), sessionOperationKindNew, string(session.id))
		identifyTestNewSessionOperationJournal(t, journal)
		session.operationJournal = journal
		previous := sessionOperationCreateTemp
		sessionOperationCreateTemp = func(string, string) (sessionOperationFile, error) { return nil, errors.New("prepare") }
		t.Cleanup(func() { sessionOperationCreateTemp = previous })
		if err := session.snapshotToStore(t.Context()); err == nil {
			t.Fatal("journal preparation failure ignored")
		}
	})

	t.Run("commit marker retained", func(t *testing.T) {
		agent := newTestAgent(WithSessionStore(NewInMemorySessionStore()))
		session := testSession(t, agent, newFakeHermesClient())
		journal := newTestSessionOperationJournalWithLogical(t, durableTempDir(t), sessionOperationKindNew, string(session.id))
		identifyTestNewSessionOperationJournal(t, journal)
		session.operationJournal = journal
		previous := sessionOperationRename
		sessionOperationRename = func(source, target string) error {
			if filepath.Base(target) == sessionOperationJournalName && journal.record.Phase == sessionOperationPhaseStoreCommitted {
				return errors.New("commit marker")
			}

			return previous(source, target)
		}
		t.Cleanup(func() { sessionOperationRename = previous })
		if err := session.snapshotToStore(t.Context()); err != nil {
			t.Fatalf("committed Store blocked by journal marker: %v", err)
		}
	})

	store := sessionOperationFaultStore{base: NewInMemorySessionStore(), listSubkeysErr: errors.New("list")}
	_, _, err := reconcileSessionStoreReplacement(t.Context(), store, SessionKey{SessionID: "logical"}, []SessionStoreReplacement{{
		Key: SessionKey{SessionID: "logical"}, Entries: []SessionStoreEntry{json.RawMessage(`{"main":true}`)},
	}})
	if err == nil {
		t.Fatal("replacement subkey listing failure ignored")
	}
}
