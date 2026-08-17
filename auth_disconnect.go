package hermesacp

import (
	"context"
	"encoding/json"
)

// disconnect fences the provider's exact durable lineage, removes the provider
// from Hermes' native credential residence, then records a generation-bumped
// durable removed tombstone. Hermes'
// native DELETE treats an already-absent credential as success, so an invalid
// or externally removed credential can always be cleaned without weakening the
// lineage fence around a replacement login.
func (p *providerAuth) disconnect(ctx context.Context, params json.RawMessage) (any, error) {
	fields, err := authParamFields(params,
		authFieldSessionID, authFieldProviderID, authFieldConnectionID,
		authFieldBindingGeneration)
	if err != nil {
		return nil, err
	}

	sessionID, err := authRequiredString(fields, authFieldSessionID)
	if err != nil {
		return nil, err
	}

	providerID, err := authRequiredString(fields, authFieldProviderID)
	if err != nil {
		return nil, err
	}

	connectionID, err := authRequiredConnectionID(fields)
	if err != nil {
		return nil, err
	}

	bindingGeneration, err := authRequiredInt64(fields, authFieldBindingGeneration)
	if err != nil {
		return nil, err
	}

	session, err := p.authSession(sessionID)
	if err != nil {
		return nil, err
	}

	releaseProvider, acquired := p.lockProvider(ctx, providerID)
	if !acquired {
		return nil, authFailed(authCauseTimeout, providerID, "", "")
	}
	defer releaseProvider()

	providerLease, lockErr := p.ledger.acquireProviderLease(ctx, providerID)
	if lockErr != nil {
		return nil, authFailed(authCauseTimeout, providerID, "", "")
	}
	defer func() { _ = providerLease.Release() }()

	releaseLedger, acquired := p.lockLedger(ctx, providerID)
	if !acquired {
		return nil, authFailed(authCauseTimeout, providerID, "", "")
	}
	defer releaseLedger()

	record, present, err := p.ledger.read(providerID)
	if err != nil {
		return nil, authFailed(authCauseProcess, providerID, "", "")
	}

	if !present || record.ConnectionID != connectionID ||
		record.BindingGeneration != bindingGeneration ||
		record.State != authLedgerConfirmed {
		return nil, authFailed(authCauseBindingConflict, providerID, "", "")
	}

	client := session.authNativeClient()
	if client == nil {
		return nil, authFailed(authCauseTransport, providerID, "", "")
	}

	callCtx, cancel := context.WithTimeout(ctx, authNativeCallTimeout)
	defer cancel()

	if err := client.AuthDisconnect(callCtx, providerID); err != nil {
		return nil, authFailed(authNativeCause(err), providerID, "", "")
	}

	// Native DELETE is idempotent and reports an already-absent credential as
	// success. Keep the old confirmed generation until that call succeeds: if
	// the call or final write fails, a retry still carries the exact old fence
	// and can repeat the same deletion safely.
	record.BindingGeneration++
	record.State = authLedgerRemoved

	record.UpdatedAt = authNow().UnixMilli()
	if err := p.ledger.write(record); err != nil {
		return nil, authFailed(authCauseProcess, providerID, "", "")
	}

	return struct{}{}, nil
}
