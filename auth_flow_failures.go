package hermesacp

import "context"

func (p *providerAuth) confirmCause(flow *authFlow) string {
	if err := p.ledger.write(authConfirmation(flow)); err != nil {
		return authCauseProcess
	}

	return ""
}

func (p *providerAuth) lineageCause(flow *authFlow) string {
	prior, present, err := p.ledger.read(flow.providerID)
	if err != nil {
		return authCauseProcess
	}

	if present && authLedgerAdvancedPast(prior, authConfirmation(flow)) {
		return authCauseBindingConflict
	}

	return ""
}

func authConfirmation(flow *authFlow) authLedgerRecord {
	return authLedgerRecord{
		ProviderID:         flow.providerID,
		ConnectionID:       flow.connectionID,
		Revision:           flow.revision,
		BindingGeneration:  flow.bindingGeneration,
		FlowID:             flow.id,
		AuthorizeRequestID: flow.authorizeRequestID,
		State:              authLedgerConfirmed,
		CreatedAt:          flow.createdAt,
		UpdatedAt:          authNow().UnixMilli(),
	}
}

func authLedgerAdvancedPast(prior authLedgerRecord, record authLedgerRecord) bool {
	if prior.BindingGeneration != record.BindingGeneration {
		return prior.BindingGeneration > record.BindingGeneration
	}

	return prior.Revision > record.Revision
}

func (p *providerAuth) abandonedCause(flow *authFlow) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	switch {
	case !authTerminal(flow.state):
		return "", false
	case flow.state == authStateCancelled:
		return authCauseFlowCancelled, true
	default:
		return authCauseFlowState, true
	}
}

func (p *providerAuth) failSettled(ctx context.Context, flow *authFlow, cause string, materialInFlight bool) error {
	if abandoned, ok := p.abandonedCause(flow); ok {
		return authFailed(abandoned, flow.providerID, flow.method.ID, flow.id)
	}

	return p.fail(ctx, flow, cause, materialInFlight)
}

func (p *providerAuth) fail(ctx context.Context, flow *authFlow, cause string, materialInFlight bool) error {
	if state, reason := authFlowTransition(cause, materialInFlight); state != "" {
		if p.terminalizeState(flow, state, reason) {
			p.cancelNativeFlow(ctx, flow.session, flow)
			p.releaseFlowProviderLease(flow)
		}
	}

	return authFailed(cause, flow.providerID, flow.method.ID, flow.id)
}
