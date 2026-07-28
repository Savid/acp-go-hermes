package hermesacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

// ProviderCredentialType selects one variant of the closed credential union.
type ProviderCredentialType string

// ProviderCredentialHermesOAuth is the only variant this adapter brokers: the
// native token material for one reserved slot, reinstalled unchanged.
const ProviderCredentialHermesOAuth ProviderCredentialType = "hermesOauth"

// Native auth kinds the hermesOauth variant carries.
const (
	ProviderAuthTypeAPIKey = "api_key"
	ProviderAuthTypeOAuth  = "oauth"
)

// Wire member names of the credential union.
const (
	credentialFieldType            = "type"
	credentialFieldAccessExpiresAt = "accessExpiresAt"
	credentialFieldAuthType        = "authType"
	credentialFieldAccessToken     = "accessToken"
	credentialFieldRefreshToken    = "refreshToken"
)

// ProviderHermesOAuthCredential is the variant this adapter brokers. It carries
// a refresh token only for a provider whose refresh token stays valid after use.
type ProviderHermesOAuthCredential struct {
	AuthType        string `json:"authType"`
	AccessToken     string `json:"accessToken"`
	RefreshToken    string `json:"refreshToken,omitempty"`
	AccessExpiresAt int64  `json:"accessExpiresAt,omitempty"`
}

// ProviderCredential is the closed, flat credential union. It marshals to one
// object whose type member selects the variant carrying the rest of the fields.
type ProviderCredential struct {
	Type        ProviderCredentialType
	HermesOAuth *ProviderHermesOAuthCredential
}

// ProviderAuthBinding is one connection generation and the credential bound to
// it.
type ProviderAuthBinding struct {
	ConnectionID      string             `json:"connectionId"`
	Revision          int64              `json:"revision"`
	BindingGeneration int64              `json:"bindingGeneration"`
	Credential        ProviderCredential `json:"credential"`
}

var errProviderCredentialInvalid = errors.New("provider credential is not a valid member of the closed union")

func (credential ProviderCredential) MarshalJSON() ([]byte, error) {
	if credential.Type != ProviderCredentialHermesOAuth || credential.HermesOAuth == nil {
		return nil, errProviderCredentialInvalid
	}

	return json.Marshal(struct {
		Type ProviderCredentialType `json:"type"`
		ProviderHermesOAuthCredential
	}{credential.Type, *credential.HermesOAuth})
}

// UnmarshalJSON decodes strictly: an unknown field, a duplicate field, an empty
// required string, and a variant this adapter does not broker are all rejected
// rather than partially decoded.
func (credential *ProviderCredential) UnmarshalJSON(data []byte) error {
	fields, err := strictCredentialFields(data)
	if err != nil {
		return err
	}

	raw, ok := fields[credentialFieldType]
	if !ok {
		return errProviderCredentialInvalid
	}

	var kind string
	if err := json.Unmarshal(raw, &kind); err != nil {
		return errProviderCredentialInvalid
	}

	if ProviderCredentialType(kind) != ProviderCredentialHermesOAuth {
		return errProviderCredentialInvalid
	}

	delete(fields, credentialFieldType)

	return credential.decodeHermesOAuth(fields)
}

func (credential *ProviderCredential) decodeHermesOAuth(fields map[string]json.RawMessage) error {
	var variant ProviderHermesOAuthCredential
	if err := decodeCredentialVariant(fields, []string{credentialFieldAuthType, credentialFieldAccessToken, credentialFieldRefreshToken, credentialFieldAccessExpiresAt}, &variant); err != nil {
		return err
	}

	if variant.AccessToken == "" {
		return errProviderCredentialInvalid
	}

	if variant.AuthType != ProviderAuthTypeAPIKey && variant.AuthType != ProviderAuthTypeOAuth {
		return errProviderCredentialInvalid
	}

	*credential = ProviderCredential{Type: ProviderCredentialHermesOAuth, HermesOAuth: &variant}

	return nil
}

func decodeCredentialVariant(fields map[string]json.RawMessage, allowed []string, out any) error {
	permitted := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		permitted[name] = struct{}{}
	}

	for name := range fields {
		if _, ok := permitted[name]; !ok {
			return errProviderCredentialInvalid
		}
	}

	encoded, err := json.Marshal(fields)
	if err != nil {
		return errProviderCredentialInvalid
	}

	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(out); err != nil {
		return errProviderCredentialInvalid
	}

	return nil
}

// strictCredentialFields walks the object once so a duplicate key is rejected
// rather than silently winning.
func strictCredentialFields(data []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))

	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errProviderCredentialInvalid
	}

	fields := map[string]json.RawMessage{}

	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, errProviderCredentialInvalid
		}

		key, _ := keyToken.(string)
		if _, duplicate := fields[key]; duplicate {
			return nil, errProviderCredentialInvalid
		}

		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, errProviderCredentialInvalid
		}

		fields[key] = value
	}

	if _, err := decoder.Token(); err != nil {
		return nil, errProviderCredentialInvalid
	}

	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errProviderCredentialInvalid
	}

	return fields, nil
}

type authCredentialResult struct {
	ConnectionID      string             `json:"connectionId"`
	Revision          int64              `json:"revision"`
	BindingGeneration int64              `json:"bindingGeneration"`
	Credential        ProviderCredential `json:"credential"`
}

// credential harvests exactly one slot: the reserved one this connection's own
// ledger entry names. Ambient, environment, and gh-derived pool entries carry
// no reserved label, so they are unharvestable rather than merely unreported.
func (p *providerAuth) credential(_ context.Context, params json.RawMessage) (any, error) {
	session, flow, err := p.addressedFlowLeg(params)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	state := flow.state
	harvested := flow.harvested
	p.mu.Unlock()

	if state != authStateAuthenticated && state != authStateSaved {
		return nil, authFailed(authCauseFlowState, flow.providerID, flow.method.ID, flow.id)
	}

	if harvested {
		return nil, authFailed(authCauseFlowState, flow.providerID, flow.method.ID, flow.id)
	}

	record, ok, err := p.ledger.read(flow.providerID)
	if err != nil || !ok {
		return nil, authFailed(authCauseHarvestFailed, flow.providerID, flow.method.ID, flow.id)
	}

	if record.ConnectionID != flow.connectionID || record.Revision != flow.revision || record.BindingGeneration != flow.bindingGeneration {
		return nil, authFailed(authCauseHarvestFailed, flow.providerID, flow.method.ID, flow.id)
	}

	home := session.authHome()
	if home == "" {
		return nil, authFailed(authCauseTransport, flow.providerID, flow.method.ID, flow.id)
	}

	material, ok, err := authReadSlot(home, flow.providerID, authSlotLabel(flow.connectionID))
	if err != nil || !ok {
		return nil, authFailed(authCauseHarvestFailed, flow.providerID, flow.method.ID, flow.id)
	}

	if !authCacheable(flow.providerID, material.RefreshToken) {
		return nil, authFailed(authCausePolicy, flow.providerID, flow.method.ID, flow.id)
	}

	expiry, _, err := authReadFlowExpiry(home, flow.providerID, flow.method.Flow)
	if err != nil {
		return nil, authFailed(authCauseHarvestFailed, flow.providerID, flow.method.ID, flow.id)
	}

	material.AccessExpiresAt = nativehermes.AuthAnchorExpiry(authNow(), expiry)

	variant, err := hermesOAuthCredential(material)
	if err != nil {
		return nil, authFailed(authCauseHarvestFailed, flow.providerID, flow.method.ID, flow.id)
	}

	p.mu.Lock()
	flow.harvested = true
	p.mu.Unlock()

	return authCredentialResult{
		ConnectionID:      flow.connectionID,
		Revision:          flow.revision,
		BindingGeneration: flow.bindingGeneration,
		Credential:        variant,
	}, nil
}

// hermesOAuthCredential converts native token material into the wire variant.
// Only reinjection material crosses: native bookkeeping — source tags, request
// counters, fingerprints, identity artifacts — is owned by hermes and is never
// harvested.
func hermesOAuthCredential(material nativehermes.AuthMaterial) (ProviderCredential, error) {
	if material.AccessToken == "" {
		return ProviderCredential{}, errProviderCredentialInvalid
	}

	if material.AuthType != ProviderAuthTypeAPIKey && material.AuthType != ProviderAuthTypeOAuth {
		return ProviderCredential{}, errProviderCredentialInvalid
	}

	return ProviderCredential{Type: ProviderCredentialHermesOAuth, HermesOAuth: &ProviderHermesOAuthCredential{
		AuthType:        material.AuthType,
		AccessToken:     material.AccessToken,
		RefreshToken:    material.RefreshToken,
		AccessExpiresAt: material.AccessExpiresAt,
	}}, nil
}

// disconnect bumps the binding generation before it touches anything else, then
// removes only the exactly-fenced reserved slot and verifies absence. It never
// removes an ambient, environment, or differently fenced entry, and it promises
// no provider-side revocation.
func (p *providerAuth) disconnect(_ context.Context, params json.RawMessage) (any, error) {
	fields, err := authParamFields(params, authFieldSessionID, authFieldProviderID, authFieldConnectionID, authFieldBindingGeneration)
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

	connectionID, err := authRequiredString(fields, authFieldConnectionID)
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

	record, ok, err := p.ledger.read(providerID)
	if err != nil {
		return nil, authFailed(authCauseHarvestFailed, providerID, "", "")
	}

	if !ok || record.ConnectionID != connectionID || record.BindingGeneration != bindingGeneration {
		return nil, authFailed(authCauseBindingConflict, providerID, "", "")
	}

	record.BindingGeneration++
	record.UpdatedAt = authNow().UnixMilli()
	record.State = authLedgerIntent

	if err := p.ledger.write(record); err != nil {
		return nil, authFailed(authCauseProcess, providerID, "", "")
	}

	home := session.authHome()
	if home == "" {
		return nil, authFailed(authCauseTransport, providerID, "", "")
	}

	label := authSlotLabel(connectionID)

	if _, err := authRemoveSlot(home, providerID, label); err != nil {
		return nil, authFailed(authCauseHarvestFailed, providerID, "", "")
	}

	if present, err := authSlotPresent(home, providerID, label); err != nil || present {
		return nil, authFailed(authCauseHarvestFailed, providerID, "", "")
	}

	record.State = authLedgerRemoved
	record.UpdatedAt = authNow().UnixMilli()

	if err := p.ledger.write(record); err != nil {
		return nil, authFailed(authCauseProcess, providerID, "", "")
	}

	return struct{}{}, nil
}

// Values-free injection outcomes reported on the lifecycle response.
const (
	authInjectionApplied  = "applied"
	authInjectionNoop     = "noop"
	authInjectionConflict = "conflict"
)

// inject installs the host's bound credentials into the reserved slots of a
// native home before the harness first reads it. A binding whose refresh token
// would be invalidated by the provider on its next refresh is refused, so
// nothing this writes can be killed by a refresh the adapter never sees. Every
// binding is evaluated: one refused or stale binding must not deny the host the
// other providers it configured.
func (p *providerAuth) inject(home string, bindings map[string]ProviderAuthBinding) string {
	outcome := authInjectionNoop

	for _, providerID := range sortedBindingKeys(bindings) {
		switch p.injectOne(home, providerID, bindings[providerID]) {
		case authInjectionConflict:
			outcome = authInjectionConflict
		case authInjectionApplied:
			if outcome != authInjectionConflict {
				outcome = authInjectionApplied
			}
		}
	}

	return outcome
}

func (p *providerAuth) injectOne(home string, providerID string, binding ProviderAuthBinding) string {
	if binding.Credential.Type != ProviderCredentialHermesOAuth || binding.Credential.HermesOAuth == nil {
		return authInjectionConflict
	}

	if !authCacheable(providerID, binding.Credential.HermesOAuth.RefreshToken) {
		return authInjectionConflict
	}

	record, hasRecord, err := p.ledger.read(providerID)
	if err != nil {
		return authInjectionConflict
	}

	if hasRecord && record.State != authLedgerRemoved && record.ConnectionID != binding.ConnectionID {
		return authInjectionConflict
	}

	label := authSlotLabel(binding.ConnectionID)

	resident, present, err := authReadSlot(home, providerID, label)
	if err != nil {
		return authInjectionConflict
	}

	material := nativehermes.AuthMaterial{
		AuthType:        binding.Credential.HermesOAuth.AuthType,
		AccessToken:     binding.Credential.HermesOAuth.AccessToken,
		RefreshToken:    binding.Credential.HermesOAuth.RefreshToken,
		AccessExpiresAt: binding.Credential.HermesOAuth.AccessExpiresAt,
	}

	if present {
		if resident != material || (hasRecord && record.Revision != binding.Revision) {
			return authInjectionConflict
		}

		return authInjectionNoop
	}

	if err := authWriteSlot(home, providerID, label, material); err != nil {
		return authInjectionConflict
	}

	now := authNow().UnixMilli()
	created := now

	if hasRecord {
		created = record.CreatedAt
	}

	if err := p.ledger.write(authLedgerRecord{
		ProviderID:        providerID,
		ConnectionID:      binding.ConnectionID,
		Revision:          binding.Revision,
		BindingGeneration: binding.BindingGeneration,
		State:             authLedgerConfirmed,
		CreatedAt:         created,
		UpdatedAt:         now,
	}); err != nil {
		return authInjectionConflict
	}

	return authInjectionApplied
}

func sortedBindingKeys(bindings map[string]ProviderAuthBinding) []string {
	keys := make([]string, 0, len(bindings))
	for key := range bindings {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	return keys
}
