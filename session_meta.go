package hermesacp

import (
	"encoding/json"
	"fmt"

	"github.com/coder/acp-go-sdk"
)

type sessionMeta struct {
	Model        string
	Env          map[string]string
	OutputSchema any
	RawMessages  rawMessageConfig
}

func sessionMetaFromLifecycle(meta map[string]any) (sessionMeta, error) {
	if err := validateLifecycleMeta(meta); err != nil {
		return sessionMeta{}, err
	}
	options, err := hermesOptionsFromMeta(meta)
	if err != nil {
		return sessionMeta{}, err
	}
	if options.OutputSchema != nil {
		return sessionMeta{}, unsupportedField("_meta.hermes.options.outputSchema")
	}

	return sessionMeta{
		Model:       options.Model,
		Env:         options.Env,
		RawMessages: rawMessageConfigFromMeta(meta),
	}, nil
}

type hermesMetaOptions struct {
	Model        string
	Env          map[string]string
	OutputSchema any
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
	if schema, ok := optionsMap[metaOutputSchemaKey]; ok {
		if err := validateSchemaObject(schema); err != nil {
			return hermesMetaOptions{}, err
		}
		options.OutputSchema = cloneAny(schema)
	}
	return options, nil
}

func validateLifecycleMeta(meta map[string]any) error {
	if len(meta) == 0 {
		return nil
	}
	if _, ok := meta["github.com/savid/acp-go-hermes"]; ok {
		return unsupportedField("_meta.github.com/savid/acp-go-hermes")
	}
	hermesMeta, ok := meta[hermesMetaKey].(map[string]any)
	if !ok {
		if _, exists := meta[hermesMetaKey]; exists {
			return fmt.Errorf("_meta.hermes must be an object")
		}
		return nil
	}
	for key, value := range hermesMeta {
		switch key {
		case metaOptionsKey:
			optionsMap, ok := value.(map[string]any)
			if !ok {
				return fmt.Errorf("_meta.hermes.options must be an object")
			}
			for optionKey := range optionsMap {
				switch optionKey {
				case "model", "env", "outputSchema":
				default:
					return unsupportedField("_meta.hermes.options." + optionKey)
				}
			}
		case rawEventKey:
			rawEvent, ok := value.(map[string]any)
			if !ok {
				return fmt.Errorf("_meta.hermes.rawEvent must be an object")
			}
			for rawKey, rawValue := range rawEvent {
				switch rawKey {
				case rawEventEnabledKey:
					if _, ok := rawValue.(bool); !ok {
						return fmt.Errorf("_meta.hermes.rawEvent.enabled must be a boolean")
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
		"error": "unsupported",
		"field": path,
	})
}

func validateSchemaObject(schema any) error {
	obj, ok := schema.(map[string]any)
	if !ok || len(obj) == 0 {
		return fmt.Errorf("output schema must be a non-empty JSON object")
	}
	if _, err := json.Marshal(obj); err != nil {
		return fmt.Errorf("output schema must be JSON serializable: %w", err)
	}

	return nil
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
				return nil, fmt.Errorf("_meta.hermes.options.env.%s must be a string", key)
			}
			out[key] = str
		}
		return out, nil
	default:
		return nil, fmt.Errorf("_meta.hermes.options.env must be an object")
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
		hermesMeta["mode"] = snapshot.mode
	}

	return map[string]any{hermesMetaKey: hermesMeta}
}

func sessionInfoMeta(snapshot sessionSnapshot) map[string]any {
	return map[string]any{
		hermesMetaKey: cloneAnyMap(sessionResponseMeta(snapshot)[hermesMetaKey].(map[string]any)),
	}
}
