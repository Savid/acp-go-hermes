package hermesacp

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	"github.com/coder/acp-go-sdk"
)

const (
	hermesEnvOptionPath           = "_meta.hermes.options." + metaEnvKey
	hermesExtraPathDirsOptionPath = "_meta.hermes.options." + metaExtraPathDirsKey
	hermesModelOptionPath         = "_meta.hermes.options." + metaModelKey
	sessionPathEnvironmentKey     = "PATH"
	sessionBashEnvironmentKey     = "BASH_ENV"
	sessionShellEnvironmentKey    = "ENV"
	sessionManagedPathEnvPrefix   = "ACP_GO_HERMES_PATH_DIR_"
	runtimePlatformWindows        = "windows"
)

type sessionMeta struct {
	Model         string
	Env           map[string]string
	ExtraPathDirs []string
	OutputSchema  any
	RawMessages   rawMessageConfig
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
		Model:         options.Model,
		Env:           cloneStringMap(options.Env),
		ExtraPathDirs: slices.Clone(options.ExtraPathDirs),
		RawMessages:   rawMessageConfigFromMeta(meta),
	}, nil
}

type hermesMetaOptions struct {
	Model         string
	Env           map[string]string
	ExtraPathDirs []string
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

	if rawDirs, ok := optionsMap[metaExtraPathDirsKey]; ok {
		dirs, err := extraPathDirsFromMeta(rawDirs)
		if err != nil {
			return hermesMetaOptions{}, err
		}

		options.ExtraPathDirs = dirs
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
		keyField:       path,
	})
}

func stringMapFromMeta(value any) (map[string]string, error) {
	switch typed := value.(type) {
	case map[string]string:
		for key := range typed {
			if sessionEnvironmentOwnsPath(key) {
				return nil, unsupportedField(hermesEnvOptionPath + "." + sessionPathEnvironmentKey)
			}

			if sessionEnvironmentOwnsBashEnv(key) {
				return nil, unsupportedField(hermesEnvOptionPath + "." + sessionBashEnvironmentKey)
			}

			if sessionEnvironmentOwnsShellEnv(key) {
				return nil, unsupportedField(hermesEnvOptionPath + "." + sessionShellEnvironmentKey)
			}

			if sessionEnvironmentOwnsManagedPath(key) {
				return nil, unsupportedField(hermesEnvOptionPath + "." + key)
			}
		}

		return cloneStringMap(typed), nil
	case map[string]any:
		out := make(map[string]string, len(typed))
		for key, raw := range typed {
			if sessionEnvironmentOwnsPath(key) {
				return nil, unsupportedField(hermesEnvOptionPath + "." + sessionPathEnvironmentKey)
			}

			if sessionEnvironmentOwnsBashEnv(key) {
				return nil, unsupportedField(hermesEnvOptionPath + "." + sessionBashEnvironmentKey)
			}

			if sessionEnvironmentOwnsShellEnv(key) {
				return nil, unsupportedField(hermesEnvOptionPath + "." + sessionShellEnvironmentKey)
			}

			if sessionEnvironmentOwnsManagedPath(key) {
				return nil, unsupportedField(hermesEnvOptionPath + "." + key)
			}

			str, ok := raw.(string)
			if !ok {
				return nil, unsupportedField(hermesEnvOptionPath)
			}

			out[key] = str
		}

		return out, nil
	default:
		return nil, unsupportedField(hermesEnvOptionPath)
	}
}

func sessionEnvironmentOwnsPath(key string) bool {
	return sessionEnvironmentOwnsPathForPlatform(key, runtime.GOOS)
}

func sessionEnvironmentOwnsPathForPlatform(key string, platform string) bool {
	if platform == runtimePlatformWindows {
		return strings.EqualFold(key, sessionPathEnvironmentKey)
	}

	return key == sessionPathEnvironmentKey
}

func sessionEnvironmentOwnsBashEnv(key string) bool {
	return sessionEnvironmentOwnsBashEnvForPlatform(key, runtime.GOOS)
}

func sessionEnvironmentOwnsBashEnvForPlatform(key string, platform string) bool {
	if platform == runtimePlatformWindows {
		return strings.EqualFold(key, sessionBashEnvironmentKey)
	}

	return key == sessionBashEnvironmentKey
}

func sessionEnvironmentOwnsShellEnv(key string) bool {
	return sessionEnvironmentOwnsShellEnvForPlatform(key, runtime.GOOS)
}

func sessionEnvironmentOwnsShellEnvForPlatform(key string, platform string) bool {
	if platform == runtimePlatformWindows {
		return strings.EqualFold(key, sessionShellEnvironmentKey)
	}

	return key == sessionShellEnvironmentKey
}

func sessionEnvironmentOwnsManagedPath(key string) bool {
	return strings.HasPrefix(strings.ToUpper(key), sessionManagedPathEnvPrefix)
}

func validatePathCarrierOptions(options Options) error {
	environments := []map[string]string{options.Env}

	for _, environment := range environments {
		for key := range environment {
			if sessionEnvironmentOwnsBashEnv(key) || sessionEnvironmentOwnsShellEnv(key) || sessionEnvironmentOwnsManagedPath(key) {
				return fmt.Errorf("environment variable %q is reserved for the session PATH carrier", key)
			}
		}
	}

	return nil
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
		keyField:       field,
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
	for key, value := range values {
		cloned[key] = value
	}

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
