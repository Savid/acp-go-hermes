package hermesacp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

// recoverPendingSharedSessionOperations reconciles every prior operation while
// the caller holds the shared home's exclusive session-set lock. It never
// guesses: exact Store bytes win, and native cleanup requires an exhaustive
// official session.list delta.
func (a *Agent) recoverPendingSharedSessionOperations(
	ctx context.Context,
	home string,
	client nativehermes.Server,
) error {
	journals, err := findPendingSessionOperationJournals(home, "", "")
	if err != nil {
		return err
	}

	lister, ok := client.(nativehermes.PersistedSessionLister)
	if !ok && len(journals) != 0 {
		return errors.New("pending Hermes session recovery requires persisted session inventory")
	}

	currentOrigin, err := sessionOperationCurrentProcessIdentity()
	if err != nil {
		return err
	}

	for _, journal := range journals {
		storeState := sessionOperationStoreAbsent

		if journal.record.Prepared != nil {
			storeCtx, cancel := a.sessionStoreContext(context.WithoutCancel(ctx))
			storeState, err = inspectPreparedSessionOperationStore(storeCtx, a.sessionStore(), journal)

			cancel()

			if err != nil {
				return err
			}
		}

		switch storeState {
		case sessionOperationStoreExact:
			if journal.record.Phase != sessionOperationPhaseStoreCommitted {
				if commitErr := journal.markStoreCommitted(); commitErr != nil {
					a.log.DebugContext(ctx, "retain exactly committed Hermes session-operation journal", slog.String("error", commitErr.Error()))

					continue
				}
			}

			if removeErr := journal.removeCommitted(); removeErr != nil {
				a.log.DebugContext(ctx, "retain committed Hermes session-operation journal for cleanup", slog.String("error", removeErr.Error()))
			}

			continue
		case sessionOperationStoreAmbiguous:
			return fmt.Errorf("%w: session operation %q has partial or different Store state", ErrSessionOperationAmbiguous, journal.record.OperationID)
		}

		if journal.record.Origin != currentOrigin {
			gone, inspectErr := sessionOperationProcessIdentityGone(journal.record.Origin)
			if inspectErr != nil {
				return inspectErr
			}

			if !gone {
				return fmt.Errorf("%w: session operation %q still has a live adapter claimant", ErrSessionOperationAmbiguous, journal.record.OperationID)
			}
		}

		if err := a.recoverAbsentSharedSessionOperation(ctx, home, client, lister, journal, true); err != nil {
			return err
		}
	}

	return nil
}

func (a *Agent) recoverAbsentSharedSessionOperation(
	ctx context.Context,
	home string,
	client nativehermes.Server,
	lister nativehermes.PersistedSessionLister,
	journal *sessionOperationJournal,
	claimOperation bool,
) (returnErr error) {
	var (
		operationOwner *nativehermes.SharedSessionOwner
		err            error
	)

	if claimOperation {
		switch journal.record.Kind {
		case sessionOperationKindNew:
			operationOwner, err = nativehermes.AcquireSharedACPSessionOwner(home, nativehermes.ACPSessionIDString(journal.record.LogicalSessionID))
		case sessionOperationKindFork:
			operationOwner, err = nativehermes.AcquireSharedNativeSessionOwner(home, journal.record.ParentNativeSessionID)
		}

		if err != nil {
			return fmt.Errorf("claim Hermes session-operation process owner: %w", err)
		}

		defer func() { returnErr = errors.Join(returnErr, operationOwner.Release()) }()
	}

	persisted, err := lister.PersistedSessions(ctx)
	if err != nil {
		return err
	}

	nativeID := journal.record.NativeSessionID
	if nativeID == "" && journal.record.Kind == sessionOperationKindFork {
		ids := make([]string, 0, len(persisted))
		for index := range persisted {
			ids = append(ids, persisted[index].ID)
		}

		var found bool

		nativeID, found, err = sessionOperationBaselineDelta(journal.record.BaselineNativeSessionIDs, ids)
		if err != nil {
			return err
		}

		if !found {
			nativeID = ""
		}
	}

	nativePresent := false

	if nativeID != "" {
		matchedMarker := false

		for index := range persisted {
			if persisted[index].ID != nativeID {
				continue
			}

			nativePresent = true

			if persisted[index].Title == journal.record.Marker {
				matchedMarker = true

				break
			}
		}

		if journal.record.Kind == sessionOperationKindFork && nativePresent && !matchedMarker {
			return fmt.Errorf("%w: recovered fork candidate %q does not carry the operation marker", ErrSessionOperationAmbiguous, nativeID)
		}
	}

	var childOwner *nativehermes.SharedSessionOwner
	if nativePresent {
		childOwner, err = nativehermes.AcquireSharedNativeSessionOwner(home, nativeID)
		if err != nil {
			return fmt.Errorf("claim recovered Hermes session %q: %w", nativeID, err)
		}
		defer func() { returnErr = errors.Join(returnErr, childOwner.Release()) }()

		if err := client.DeleteSession(ctx, nativeID); err != nil {
			return fmt.Errorf("delete recovered Hermes session %q: %w", nativeID, err)
		}

		remaining, verifyErr := lister.PersistedSessions(ctx)
		if verifyErr != nil {
			return verifyErr
		}

		for index := range remaining {
			if remaining[index].ID == nativeID {
				return fmt.Errorf("%w: recovered Hermes session %q remains durable", ErrSessionOperationAmbiguous, nativeID)
			}
		}
	}

	return journal.removeRecovered()
}
