package hermesacp

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/coder/acp-go-sdk"
)

const (
	hermesEnvOptionPath           = "_meta.hermes.options." + metaEnvKey
	hermesExtraPathDirsOptionPath = "_meta.hermes.options." + metaExtraPathDirsKey
	hermesModelOptionPath         = "_meta.hermes.options." + metaModelKey
	hermesEffortOptionPath        = "_meta.hermes.options." + metaEffortKey
)

type sessionMeta struct {
	Model            string
	Effort           string
	Env              map[string]string
	EnvSet           bool
	ExtraPathDirs    []string
	ExtraPathDirsSet bool
	OutputSchema     any
	RawMessages      rawMessageConfig
}

func (a *Agent) sessionMetaFromLifecycle(meta map[string]any) (sessionMeta, error) {
	if err := validateLifecycleMeta(meta); err != nil {
		return sessionMeta{}, err
	}

	options, err := hermesOptionsFromMeta(meta)
	if err != nil {
		return sessionMeta{}, err
	}

	return sessionMeta{
		Model:            options.Model,
		Effort:           options.Effort,
		Env:              cloneStringMap(options.Env),
		EnvSet:           options.EnvSet,
		ExtraPathDirs:    slices.Clone(options.ExtraPathDirs),
		ExtraPathDirsSet: options.ExtraPathDirsSet,
		RawMessages:      rawMessageConfigFromMeta(meta),
	}, nil
}

type hermesMetaOptions struct {
	Model            string
	Effort           string
	Env              map[string]string
	EnvSet           bool
	ExtraPathDirs    []string
	ExtraPathDirsSet bool
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

	if effort, _ := optionsMap[metaEffortKey].(string); effort != "" {
		options.Effort = effort
	}

	if rawEnv, ok := optionsMap[metaEnvKey]; ok {
		env, err := stringMapFromMeta(rawEnv)
		if err != nil {
			return hermesMetaOptions{}, err
		}

		options.Env = env
		options.EnvSet = true
	}

	if rawDirs, ok := optionsMap[metaExtraPathDirsKey]; ok {
		dirs, err := extraPathDirsFromMeta(rawDirs)
		if err != nil {
			return hermesMetaOptions{}, err
		}

		options.ExtraPathDirs = dirs
		options.ExtraPathDirsSet = true
	}

	return options, nil
}

func validateLifecycleMeta(meta map[string]any) error {
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
						return unsupportedField(hermesModelOptionPath)
					}
				case metaEffortKey:
					effort, ok := optionValue.(string)
					if !ok || (effort != "" && !hermesEffortLevel(effort)) {
						return unsupportedField(hermesEffortOptionPath)
					}
				case metaEnvKey, metaExtraPathDirsKey:
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
		jsonFieldField: path,
	})
}

func stringMapFromMeta(value any) (map[string]string, error) {
	var env map[string]string

	switch typed := value.(type) {
	case map[string]string:
		env = cloneStringMap(typed)
	case map[string]any:
		env = make(map[string]string, len(typed))
		for key, raw := range typed {
			str, ok := raw.(string)
			if !ok {
				return nil, unsupportedField(hermesEnvOptionPath + "." + key)
			}

			env[key] = str
		}
	default:
		return nil, unsupportedField(hermesEnvOptionPath)
	}

	if err := validateSessionEnv(env, hermesEnvOptionPath); err != nil {
		return nil, err
	}

	return env, nil
}

// extraPathDirsFromMeta accepts both the direct Go builder slice and the
// []any shape produced by JSON decoding. Every error names the exact element
// whose value could not be installed as one PATH component.
func extraPathDirsFromMeta(value any) ([]string, error) {
	var raw []any

	switch typed := value.(type) {
	case []string:
		raw = make([]any, len(typed))
		for index, dir := range typed {
			raw[index] = dir
		}
	case []any:
		raw = slices.Clone(typed)
	default:
		return nil, unsupportedField(hermesExtraPathDirsOptionPath)
	}

	dirs := make([]string, 0, len(raw))
	for index, entry := range raw {
		field := fmt.Sprintf("%s[%d]", hermesExtraPathDirsOptionPath, index)

		dir, ok := entry.(string)
		if !ok {
			return nil, unsupportedField(field)
		}

		if dir == "" {
			return nil, invalidExtraPathDir(field, "must not be empty")
		}

		if !filepath.IsAbs(dir) {
			return nil, invalidExtraPathDir(field, "must be an absolute path")
		}

		if strings.ContainsRune(dir, os.PathListSeparator) {
			return nil, invalidExtraPathDir(field, "must not contain the path list separator")
		}

		dirs = append(dirs, dir)
	}

	return dirs, nil
}

func invalidExtraPathDir(field string, reason string) error {
	return acp.NewInvalidParams(map[string]any{
		jsonFieldError: "session extra path dir " + reason,
		jsonFieldField: field,
	})
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
	case []string:
		return slices.Clone(typed)
	default:
		return value
	}
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}

	cloned := make(map[string]string, len(values))
	maps.Copy(cloned, values)

	return cloned
}

func sessionResponseMeta(snapshot sessionSnapshot) map[string]any {
	hermesMeta := map[string]any{
		hermesNativeIDMetaKey: snapshot.idmap.NativeSessionID,
	}
	if model := modelSelectionValue(snapshot.providerID, snapshot.modelID); model != "" {
		hermesMeta["model"] = model
		hermesMeta["modelId"] = model
	}

	return map[string]any{hermesMetaKey: hermesMeta}
}

func lifecycleResponseMeta(snapshot sessionSnapshot) map[string]any {
	return sessionResponseMeta(snapshot)
}

func sessionInfoMeta(snapshot sessionSnapshot) map[string]any {
	hermesMeta, _ := sessionResponseMeta(snapshot)[hermesMetaKey].(map[string]any)

	return map[string]any{
		hermesMetaKey: cloneAnyMap(hermesMeta),
	}
}
