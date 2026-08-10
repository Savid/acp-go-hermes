package hermesacp

import (
	"context"
	"encoding/json"
	"slices"
	"strings"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

	"github.com/coder/acp-go-sdk"
)

// Session-config option vocabulary.
const (
	keyValue = "value"
)

func (a *Agent) SetSessionConfigOption(ctx context.Context, params acp.SetSessionConfigOptionRequest) (acp.SetSessionConfigOptionResponse, error) {
	if params.Boolean != nil {
		return acp.SetSessionConfigOptionResponse{}, acp.NewInvalidParams(map[string]any{jsonFieldError: valUnsupported, keyField: keyValue})
	}

	if params.ValueId == nil {
		return acp.SetSessionConfigOptionResponse{}, acp.NewInvalidParams(map[string]any{keyField: keyValue})
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
		return acp.SetSessionConfigOptionResponse{}, acp.NewInvalidParams(map[string]any{keyField: keyValue})
	}

	var options []acp.SessionConfigOption

	switch params.ValueId.ConfigId {
	case configModel:
		// One native model.options read answers the whole call. The value check
		// and the answer are the same enumeration read once, because the model
		// selection between them is a local mutation that changes which option
		// is current and nothing about which options exist. Reading twice made
		// one selection cost two full provider enumerations over the gateway,
		// which is what put a healthy harness outside a host's probe budget.
		providers, ok := session.configProviders(ctx)
		if !ok || !hasConfigValue(session.configOptionsFrom(providers), configModel, value) {
			return acp.SetSessionConfigOptionResponse{}, acp.NewInvalidParams(map[string]any{keyField: keyValue})
		}

		session.setModel(value)

		options = session.configOptionsFrom(providers)
	default:
		return acp.SetSessionConfigOptionResponse{}, acp.NewInvalidParams(map[string]any{keyField: "configId"})
	}

	_ = session.emitUpdate(ctx, acp.SessionUpdate{
		ConfigOptionUpdate: &acp.SessionConfigOptionUpdate{ConfigOptions: options},
	})

	return acp.SetSessionConfigOptionResponse{ConfigOptions: options}, nil
}

// hasConfigValue reports whether an already-read option list publishes value
// under configID. It takes the list rather than reading one so its caller
// controls how many native enumerations the surrounding call costs.
func hasConfigValue(options []acp.SessionConfigOption, configID acp.SessionConfigId, value string) bool {
	for _, option := range options {
		if option.Select == nil || option.Select.Id != configID {
			continue
		}

		if option.Select.Options.Ungrouped != nil {
			for _, item := range *option.Select.Options.Ungrouped {
				if string(item.Value) == value {
					return true
				}
			}
		}

		if option.Select.Options.Grouped != nil {
			for _, group := range *option.Select.Options.Grouped {
				for _, item := range group.Options {
					if string(item.Value) == value {
						return true
					}
				}
			}
		}
	}

	return false
}

func (s *session) configOptions(ctx context.Context) []acp.SessionConfigOption {
	providers, ok := s.configProviders(ctx)
	if !ok {
		return nil
	}

	return s.configOptionsFrom(providers)
}

// configProviders performs one native model.options read. A session with no
// live gateway, and a gateway that refused the read, both report no
// enumeration: the config surface publishes what the harness answered or
// nothing.
func (s *session) configProviders(ctx context.Context) (nativehermes.ProvidersResponse, bool) {
	client := s.snapshot().client
	if client == nil {
		return nativehermes.ProvidersResponse{}, false
	}

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

// contextWindow resolves the true context-window size in tokens for the
// session's current model, or 0 when the harness does not advertise it.
func (s *session) contextWindow(ctx context.Context) int {
	snapshot := s.snapshot()
	if snapshot.client == nil {
		return 0
	}

	providers, err := snapshot.client.ConfigProviders(ctx)
	if err != nil {
		return 0
	}

	for _, provider := range providers.Providers {
		if provider.ID != snapshot.providerID {
			continue
		}

		for key := range provider.Models {
			model := provider.Models[key]
			if firstNonEmpty(model.ID, key) != snapshot.modelID {
				continue
			}

			if n, ok := nativehermes.IntFromNumber(model.Limit["context"]); ok {
				return n
			}
		}
	}

	return 0
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

			value := provider.ID + "/" + modelID
			if current == "" {
				current = value
			}

			group.Options = append(group.Options, acp.SessionConfigSelectOption{
				Name:  firstNonEmpty(model.Name, value),
				Value: acp.SessionConfigValueId(value),
				Meta:  map[string]any{hermesMetaKey: modelMeta(provider.ID, modelID, model)},
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

func (snapshot sessionSnapshot) modelValue() string {
	return joinModelValue(snapshot.providerID, snapshot.modelID)
}

func modelMeta(providerID string, modelID string, model nativehermes.ProviderModel) map[string]any {
	meta := map[string]any{"modelId": providerID + "/" + modelID}
	if n, ok := nativehermes.IntFromNumber(model.Limit["context"]); ok {
		meta["contextWindow"] = n
	}

	if n, ok := nativehermes.IntFromNumber(model.Limit["output"]); ok {
		meta["maxOutputTokens"] = n
	}

	efforts := supportedEfforts(model)
	if len(efforts) > 0 {
		meta["supportedEffortLevels"] = efforts
	}

	return meta
}

func supportedEfforts(model nativehermes.ProviderModel) []string {
	seen := map[string]struct{}{}

	for key, raw := range model.Options {
		if !strings.Contains(strings.ToLower(key), "effort") {
			continue
		}

		for _, value := range optionStringValues(raw) {
			seen[value] = struct{}{}
		}
	}

	out := make([]string, 0, len(seen))
	for value := range seen {
		out = append(out, value)
	}

	slices.Sort(out)

	return out
}

func optionStringValues(raw any) []string {
	switch value := raw.(type) {
	case []string:
		return compactNonEmptyStrings(value)
	case []any:
		out := make([]string, 0, len(value))
		for _, item := range value {
			if str, _ := item.(string); str != "" {
				out = append(out, str)
			}
		}

		return compactNonEmptyStrings(out)
	case map[string]any:
		for _, key := range []string{metaOptionsKey, "values", "enum"} {
			if values := optionStringValues(value[key]); len(values) > 0 {
				return values
			}
		}
	}

	return nil
}

func compactNonEmptyStrings(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}

	slices.Sort(out)

	return slices.Compact(out)
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
