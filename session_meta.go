package hermesacp

import (
	"github.com/coder/acp-go-sdk"
)

type sessionMeta struct {
	Model        string
	Env          map[string]string
	OutputSchema any
	RawMessages  rawMessageConfig
	// ProviderAuth carries the bindings the host asked the adapter to install
	// into the session's native home before hermes first reads it. Supplied
	// records whether the option key was present at all, which is what
	// separates an injection that ran and matched from one that never ran.
	ProviderAuth         map[string]ProviderAuthBinding
	ProviderAuthSupplied bool
	// injectionOutcome is the shared cell the native launch fills with the
	// values-free tri-state. It is a pointer because this value travels by copy
	// through the launch path.
	injectionOutcome *string
}

// withProviderAuth attaches injection bindings and allocates the cell the
// launch path fills. Bindings that do not exist are not an injection: the
// lifecycle response then carries no outcome at all.
func (meta sessionMeta) withProviderAuth(bindings map[string]ProviderAuthBinding) sessionMeta {
	meta.ProviderAuth = bindings
	if len(bindings) > 0 {
		meta.ProviderAuthSupplied = true
		meta.injectionOutcome = new(string)
	}

	return meta
}

// injection reports the recorded injection outcome, or the empty string when no
// injection was requested.
func (meta sessionMeta) injection() string {
	if meta.injectionOutcome == nil {
		return ""
	}

	return *meta.injectionOutcome
}

func (a *Agent) sessionMetaFromLifecycle(meta map[string]any) (sessionMeta, error) {
	if err := validateLifecycleMeta(meta, a.providerAuth != nil); err != nil {
		return sessionMeta{}, err
	}

	options, err := hermesOptionsFromMeta(meta)
	if err != nil {
		return sessionMeta{}, err
	}

	resolved := sessionMeta{
		Model:                options.Model,
		Env:                  options.Env,
		RawMessages:          rawMessageConfigFromMeta(meta),
		ProviderAuth:         options.ProviderAuth,
		ProviderAuthSupplied: options.ProviderAuthSupplied,
	}
	if options.ProviderAuthSupplied {
		resolved.injectionOutcome = new(string)
	}

	return resolved, nil
}

type hermesMetaOptions struct {
	Model                string
	Env                  map[string]string
	ProviderAuth         map[string]ProviderAuthBinding
	ProviderAuthSupplied bool
}

func hermesOptionsFromMeta(meta map[string]any) (hermesMetaOptions, error) {
	hermesMeta, _ := meta[hermesMetaKey].(map[string]any)

	optionsMap, _ := hermesMeta[metaOptionsKey].(map[string]any)
	if optionsMap == nil {
		return hermesMetaOptions{}, nil
	}

	options := hermesMetaOptions{}
	if model, _ := optionsMap[metaModelKey].(string); model != "" {
		options.Model = model
	}

	if rawEnv, ok := optionsMap[metaEnvKey]; ok {
		env, err := stringMapFromMeta(rawEnv)
		if err != nil {
			return hermesMetaOptions{}, err
		}

		options.Env = env
	}

	if raw, ok := optionsMap[metaProviderAuthKey]; ok {
		bindings, err := providerAuthBindingsFromMeta(raw)
		if err != nil {
			return hermesMetaOptions{}, err
		}

		options.ProviderAuth = bindings
		options.ProviderAuthSupplied = true
	}

	return options, nil
}

// providerAuthBindingsFromMeta decodes the injection map strictly. The
// credential union rejects unknown and duplicate fields, empty required
// strings, and every variant this adapter does not accept, so a binding that
// does not decode fails session establishment instead of being ignored.
func providerAuthBindingsFromMeta(value any) (map[string]ProviderAuthBinding, error) {
	encoded, err := agentJSONMarshal(value)
	if err != nil {
		return nil, unsupportedField(providerAuthOptionPath)
	}

	var bindings map[string]ProviderAuthBinding
	if err := agentJSONUnmarshal(encoded, &bindings); err != nil {
		return nil, unsupportedField(providerAuthOptionPath)
	}

	for providerID, binding := range bindings {
		if providerID == "" || binding.ConnectionID == "" || binding.Revision <= 0 || binding.BindingGeneration <= 0 {
			return nil, unsupportedField(providerAuthOptionPath)
		}
	}

	return bindings, nil
}

func validateLifecycleMeta(meta map[string]any, providerAuthEnabled bool) error {
	if len(meta) == 0 {
		return nil
	}

	hermesMeta, ok := meta[hermesMetaKey].(map[string]any)
	if !ok {
		if _, exists := meta[hermesMetaKey]; exists {
			return unsupportedField("_meta.hermes")
		}

		return nil
	}

	for key, value := range hermesMeta {
		switch key {
		case metaOptionsKey:
			optionsMap, ok := value.(map[string]any)
			if !ok {
				return unsupportedField("_meta.hermes.options")
			}

			for optionKey, optionValue := range optionsMap {
				switch optionKey {
				case metaModelKey:
					if _, ok := optionValue.(string); !ok {
						return unsupportedField("_meta.hermes.options.model")
					}
				case metaEnvKey:
				case metaProviderAuthKey:
					if !providerAuthEnabled {
						return unsupportedField(providerAuthOptionPath)
					}
				case metaOutputSchemaKey:
					return unsupportedField("_meta.hermes.options.outputSchema")
				default:
					return unsupportedField("_meta.hermes.options." + optionKey)
				}
			}
		case rawEventKey:
			rawEvent, ok := value.(map[string]any)
			if !ok {
				return unsupportedField("_meta.hermes.rawEvent")
			}

			for rawKey, rawValue := range rawEvent {
				switch rawKey {
				case rawEventEnabledKey:
					if _, ok := rawValue.(bool); !ok {
						return unsupportedField("_meta.hermes.rawEvent.enabled")
					}
				default:
					return unsupportedField("_meta.hermes.rawEvent." + rawKey)
				}
			}
		default:
			return unsupportedField("_meta.hermes." + key)
		}
	}

	return nil
}

func unsupportedField(path string) error {
	return acp.NewInvalidParams(map[string]any{
		jsonFieldError: valUnsupported,
		keyField:       path,
	})
}

func stringMapFromMeta(value any) (map[string]string, error) {
	switch typed := value.(type) {
	case map[string]string:
		return cloneStringMap(typed), nil
	case map[string]any:
		out := make(map[string]string, len(typed))
		for key, raw := range typed {
			str, ok := raw.(string)
			if !ok {
				return nil, unsupportedField("_meta.hermes.options.env")
			}

			out[key] = str
		}

		return out, nil
	default:
		return nil, unsupportedField("_meta.hermes.options.env")
	}
}

func cloneAnyMap(values map[string]any) map[string]any {
	if values == nil {
		return nil
	}

	cloned := make(map[string]any, len(values))
	for key, value := range values {
		cloned[key] = cloneAny(value)
	}

	return cloned
}

func cloneAnySlice(values []any) []any {
	if values == nil {
		return nil
	}

	cloned := make([]any, len(values))
	for i, value := range values {
		cloned[i] = cloneAny(value)
	}

	return cloned
}

func cloneAny(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneAnyMap(typed)
	case map[string]string:
		return cloneStringMap(typed)
	case []any:
		return cloneAnySlice(typed)
	default:
		return value
	}
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}

	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}

	return cloned
}

func sessionResponseMeta(snapshot sessionSnapshot) map[string]any {
	hermesMeta := map[string]any{
		hermesNativeIDMetaKey: snapshot.idmap.NativeSessionID,
	}
	if model := joinModelValue(snapshot.providerID, snapshot.modelID); model != "" {
		hermesMeta["model"] = model
		hermesMeta["modelId"] = model
	}

	if snapshot.mode != "" {
		hermesMeta[keyMode] = snapshot.mode
	}

	return map[string]any{hermesMetaKey: hermesMeta}
}

// lifecycleResponseMeta adds the values-free injection tri-state to a lifecycle
// response. The field is absent when the option key was not supplied at all:
// reporting noop there would claim an injection was evaluated and matched.
func lifecycleResponseMeta(snapshot sessionSnapshot) map[string]any {
	meta := sessionResponseMeta(snapshot)
	if snapshot.providerAuthInjection == "" {
		return meta
	}

	hermesMeta, _ := meta[hermesMetaKey].(map[string]any)
	hermesMeta[metaProviderAuthKey] = map[string]any{
		providerAuthInjectionName: snapshot.providerAuthInjection,
	}

	return meta
}

func sessionInfoMeta(snapshot sessionSnapshot) map[string]any {
	hermesMeta, _ := sessionResponseMeta(snapshot)[hermesMetaKey].(map[string]any)

	return map[string]any{
		hermesMetaKey: cloneAnyMap(hermesMeta),
	}
}
