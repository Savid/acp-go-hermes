package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

	"github.com/coder/acp-go-sdk"
)

// Session-config option vocabulary.
const (
	keyValue                       = "value"
	keyConfigID                    = "configId"
	valHermesModelSelectionRefused = "hermes_model_selection_refused"
)

func (a *Agent) SetSessionConfigOption(ctx context.Context, params acp.SetSessionConfigOptionRequest) (acp.SetSessionConfigOptionResponse, error) {
	// The reserved lifecycle literal is refused before this surface's own
	// handling, variant refusal included: `session/set_session_config_option`
	// never carries the key, and a family literal is never foreign and never a
	// no-op. Either union variant can carry `_meta`, so the refusal reads
	// whichever one the host actually sent.
	if err := rejectLifecycleMeta(sessionConfigOptionMeta(params)); err != nil {
		return acp.SetSessionConfigOptionResponse{}, err
	}

	if params.Boolean != nil {
		// Neither union variant is a wire field; the request is discriminated by
		// "type", so "type" is the only JSON path that names the boolean variant
		// this agent does not implement.
		return acp.SetSessionConfigOptionResponse{}, unsupportedField(keyType)
	}

	if params.ValueId == nil {
		return acp.SetSessionConfigOptionResponse{}, unsupportedField(keyValue)
	}

	session, err := a.session(params.ValueId.SessionId)
	if err != nil {
		return acp.SetSessionConfigOptionResponse{}, err
	}

	if err := session.ensureNotPoisoned(); err != nil {
		return acp.SetSessionConfigOptionResponse{}, err
	}

	value := string(params.ValueId.Value)
	if value == "" {
		return acp.SetSessionConfigOptionResponse{}, unsupportedField(keyValue)
	}

	var options []acp.SessionConfigOption

	switch params.ValueId.ConfigId {
	case configModel:
		// A shape refusal, not a value gate. It asks whether the string can name
		// a provider and a model, never whether that model exists — Hermes owns
		// the second question, and the allowlist this surface used to carry
		// answered it here and got it wrong. The refusal is the wrapper's own
		// rather than the gateway client's, because a client that publishes no
		// model setter would otherwise take an unqualified value straight into
		// session state. The active-resume door refuses the same shape through
		// the same predicate, so no door admits what another refuses.
		if nativehermes.ModelSelectionShapeError(value) != nil {
			return acp.SetSessionConfigOptionResponse{}, unsupportedField(keyValue)
		}

		snapshot := session.snapshot()

		client := snapshot.client
		if managed, ok := client.(*managedHermesServer); ok {
			client = managed.Server
		}

		if setter, supported := client.(interface {
			SetModel(context.Context, string, string) error
		}); supported {
			if err := setter.SetModel(ctx, snapshot.idmap.NativeSessionID, value); err != nil {
				var nativeRefusal *nativehermes.RPCError
				if errors.As(err, &nativeRefusal) {
					return acp.SetSessionConfigOptionResponse{}, &mappedWireError{
						wire: acp.NewInvalidParams(map[string]any{
							jsonFieldError: valHermesModelSelectionRefused,
							keyField:       keyValue,
						}),
						cause: err,
					}
				}

				return acp.SetSessionConfigOptionResponse{}, err
			}
		}

		session.setModel(value)

		// The catalogue is a host menu, not an accepted set. Read it only after
		// Hermes has answered the mutation, then publish that menu against the
		// exact value sent. A failed read cannot retroactively refuse a native
		// selection that already succeeded.
		options = session.configOptions(ctx)
	default:
		return acp.SetSessionConfigOptionResponse{}, unsupportedField(keyConfigID)
	}

	_ = session.emitUpdate(ctx, acp.SessionUpdate{
		ConfigOptionUpdate: &acp.SessionConfigOptionUpdate{ConfigOptions: options},
	})

	return acp.SetSessionConfigOptionResponse{ConfigOptions: options}, nil
}

// sessionConfigOptionMeta reads the request `_meta` from whichever union
// variant carries it. The variant is not a wire field, so a request that
// deserialized into neither carries no metadata at all.
func sessionConfigOptionMeta(params acp.SetSessionConfigOptionRequest) map[string]any {
	switch {
	case params.Boolean != nil:
		return params.Boolean.Meta
	case params.ValueId != nil:
		return params.ValueId.Meta
	default:
		return nil
	}
}

func (s *session) configOptions(ctx context.Context) []acp.SessionConfigOption {
	providers, ok := s.configProviders(ctx)
	if !ok {
		return nil
	}

	return s.configOptionsFrom(providers)
}

// configProvidersTimeout bounds one native model.options read. The gateway
// answers that call by listing models at every authenticated provider, so a
// provider endpoint that accepts a connection and then never answers holds the
// read open for as long as the caller allows. Establishing a session is such a
// caller, and its context carries whatever deadline the host chose, which may
// be none: unbounded here means a session that never establishes.
//
// The bound is generous rather than tuned. A cold gateway reaches this read in
// a few seconds on a healthy link and slower on a poor one, and losing the
// enumeration costs the host its model list, so the deadline exists to convert
// a hang into a degraded session rather than to keep establishment brisk.
const configProvidersTimeout = 30 * time.Second

// configProviders performs one native model.options read, bounded by
// configProvidersTimeout. A session with no live gateway, a gateway that
// refused the read, and a read that outlived its bound all report no
// enumeration: the config surface publishes what the harness answered or
// nothing.
func (s *session) configProviders(ctx context.Context) (nativehermes.ProvidersResponse, bool) {
	client := s.snapshot().client
	if client == nil {
		return nativehermes.ProvidersResponse{}, false
	}

	ctx, cancel := context.WithTimeout(ctx, configProvidersTimeout)
	defer cancel()

	providers, err := client.ConfigProviders(ctx)
	if err != nil {
		return nativehermes.ProvidersResponse{}, false
	}

	return providers, true
}

// configOptionsFrom builds the published option list from an enumeration the
// caller already read, against the session's current selection.
func (s *session) configOptionsFrom(providers nativehermes.ProvidersResponse) []acp.SessionConfigOption {
	model := modelConfigOption(s.snapshot(), providers)
	if model.Select == nil {
		return nil
	}

	return []acp.SessionConfigOption{model}
}

func modelConfigOption(snapshot sessionSnapshot, providers nativehermes.ProvidersResponse) acp.SessionConfigOption {
	category := acp.SessionConfigOptionCategoryModel
	current := snapshot.modelValue()

	groups := make(acp.SessionConfigSelectOptionsGrouped, 0, len(providers.Providers))
	for _, provider := range providers.Providers {
		if provider.ID == "" || len(provider.Models) == 0 {
			continue
		}

		keys := make([]string, 0, len(provider.Models))
		for key := range provider.Models {
			keys = append(keys, key)
		}

		slices.Sort(keys)

		group := acp.SessionConfigSelectGroup{
			Group: acp.SessionConfigGroupId(provider.ID),
			Name:  firstNonEmpty(provider.Name, provider.ID),
		}
		for _, key := range keys {
			model := provider.Models[key]
			modelID := firstNonEmpty(model.ID, key)

			value := modelSelectionValue(provider.ID, modelID)
			if current == "" {
				current = value
			}

			group.Options = append(group.Options, acp.SessionConfigSelectOption{
				Name:  firstNonEmpty(model.Name, value),
				Value: acp.SessionConfigValueId(value),
				Meta:  map[string]any{hermesMetaKey: map[string]any{"modelId": value}},
			})
		}

		if len(group.Options) > 0 {
			groups = append(groups, group)
		}
	}

	if len(groups) == 0 {
		if current == "" {
			return acp.SessionConfigOption{}
		}

		options := acp.SessionConfigSelectOptionsUngrouped{{Name: current, Value: acp.SessionConfigValueId(current)}}

		return acp.SessionConfigOption{Select: &acp.SessionConfigOptionSelect{
			Id:           configModel,
			Name:         "Model",
			Category:     &category,
			Type:         configTypeSelect,
			CurrentValue: acp.SessionConfigValueId(current),
			Options:      acp.SessionConfigSelectOptions{Ungrouped: &options},
		}}
	}

	return acp.SessionConfigOption{Select: &acp.SessionConfigOptionSelect{
		Id:           configModel,
		Name:         "Model",
		Category:     &category,
		Type:         configTypeSelect,
		CurrentValue: acp.SessionConfigValueId(current),
		Options:      acp.SessionConfigSelectOptions{Grouped: &groups},
	}}
}

func modelSelectionValue(providerID string, modelID string) string {
	return nativehermes.ModelSelectionValue(providerID, modelID)
}

func (snapshot sessionSnapshot) modelValue() string {
	return modelSelectionValue(snapshot.providerID, snapshot.modelID)
}

func unstableConfigOptions(options []acp.SessionConfigOption) []acp.UnstableSessionConfigOption {
	if len(options) == 0 {
		return nil
	}

	out := make([]acp.UnstableSessionConfigOption, 0, len(options))
	for _, option := range options {
		data, err := json.Marshal(option)
		if err != nil {
			continue
		}

		var unstable acp.UnstableSessionConfigOption
		if err := json.Unmarshal(data, &unstable); err == nil {
			out = append(out, unstable)
		}
	}

	return out
}
