package hermesacp

import (
	"context"
	"encoding/json"
)

func (p *providerAuth) disconnect(ctx context.Context, params json.RawMessage) (any, error) {
	fields, err := authParamFields(params,
		authFieldSessionID,
		authFieldProviderID,
		authFieldConnectionID,
		authFieldBindingGeneration,
	)
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
	if err != nil || bindingGeneration <= 0 {
		return nil, invalidAuthField(authFieldBindingGeneration)
	}

	session, err := p.authSession(sessionID)
	if err != nil {
		return nil, err
	}

	client := session.authNativeClient()
	if client == nil {
		return nil, authFailed(authCauseTransport, providerID, "", "")
	}

	releaseProvider, acquired := p.lockProvider(ctx, providerID)
	if !acquired {
		return nil, authFailed(authCauseTimeout, providerID, "", "")
	}
	defer releaseProvider()

	releaseLedger, acquired := p.lockLedger(ctx, providerID)
	if !acquired {
		return nil, authFailed(authCauseTimeout, providerID, "", "")
	}
	defer releaseLedger()

	record, ok, err := p.ledger.read(providerID)
	if err != nil {
		return nil, authFailed(authCauseProcess, providerID, "", "")
	}

	if !ok ||
		record.State != authLedgerConfirmed ||
		record.ConnectionID != connectionID ||
		record.BindingGeneration != bindingGeneration {
		return nil, authFailed(authCauseBindingConflict, providerID, "", "")
	}

	record.BindingGeneration++
	record.UpdatedAt = authNow().UnixMilli()
	record.State = authLedgerIntent

	if writeErr := p.ledger.write(record); writeErr != nil {
		return nil, authFailed(authCauseProcess, providerID, "", "")
	}

	p.cancelProviderFlows(ctx, providerID)

	callCtx, cancel := context.WithTimeout(ctx, authNativeCallTimeout)
	defer cancel()

	if disconnectErr := client.AuthDisconnect(callCtx, providerID); disconnectErr != nil {
		return nil, authFailed(authNativeCause(disconnectErr), providerID, "", "")
	}

	providers, err := client.AuthProviders(callCtx)
	if err != nil {
		return nil, authFailed(authNativeCause(err), providerID, "", "")
	}

	for _, provider := range providers {
		if provider.ID == providerID && provider.LoggedIn {
			return nil, authFailed(authCauseNativeVeto, providerID, "", "")
		}
	}

	record.State = authLedgerRemoved
	record.UpdatedAt = authNow().UnixMilli()

	if writeErr := p.ledger.write(record); writeErr != nil {
		return nil, authFailed(authCauseProcess, providerID, "", "")
	}

	return struct{}{}, nil
}

func (p *providerAuth) cancelProviderFlows(ctx context.Context, providerID string) {
	p.mu.Lock()

	flows := make([]*authFlow, 0)
	for key, flow := range p.flows {
		if key.providerID != providerID {
			continue
		}

		delete(p.flows, key)

		flow.state = authStateCancelled
		flow.reason = authReasonSuperseded
		flow.stopCompleter()
		flows = append(flows, flow)
	}
	p.mu.Unlock()

	for _, flow := range flows {
		p.cancelNative(ctx, flow)
	}
}
