package hermesacp

import (
	"errors"
	"fmt"
	"slices"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/process"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-hermes/internal/hermes"
)

const (
	metaOptionsKey       = "options"
	metaRawEventKey      = "rawEvent"
	metaModelKey         = "model"
	metaEnvKey           = "env"
	metaExtraPathDirsKey = "extraPathDirs"
	metaOutputSchemaKey  = "outputSchema"
	metaEffortKey        = "effort"
	metaEnabledKey       = "enabled"
)

// HermesOptions is the per-session options struct carried at _meta.hermes.options.
type HermesOptions struct {
	// Model selects the hermes model for this session as "provider/id".
	Model string `json:"model,omitempty"`
	// Env overlays the session's hermes process environment.
	Env map[string]string `json:"env,omitempty"`
	// ExtraPathDirs are absolute directories prepended, in order, to the PATH
	// of this session's hermes process.
	ExtraPathDirs []string `json:"extraPathDirs,omitempty"`
	// OutputSchema requests structured output. hermes has no native surface for
	// it, so a session carrying it fails at session start.
	OutputSchema map[string]any `json:"outputSchema,omitempty"`
	// Effort is a reasoning-level value passed unchanged to hermes.
	Effort string `json:"effort,omitempty"`
}

// HermesOption configures HermesOptions values.
type HermesOption func(*HermesOptions)

// NewHermesOptions constructs HermesOptions from functional options.
func NewHermesOptions(opts ...HermesOption) HermesOptions {
	options := HermesOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	return options.clone()
}

// WithHermesModel configures the session model as "provider/id".
func WithHermesModel(model string) HermesOption {
	return func(options *HermesOptions) { options.Model = model }
}

// WithHermesEnv configures the session environment overlay.
func WithHermesEnv(env map[string]string) HermesOption {
	cloned := cloneStringMap(env)

	return func(options *HermesOptions) { options.Env = cloneStringMap(cloned) }
}

// WithHermesExtraPathDirs configures the directories prepended to the session PATH.
func WithHermesExtraPathDirs(dirs ...string) HermesOption {
	cloned := slices.Clone(dirs)

	return func(options *HermesOptions) { options.ExtraPathDirs = slices.Clone(cloned) }
}

// WithHermesOutputSchema configures structured output, which hermes refuses at
// session start.
func WithHermesOutputSchema(schema map[string]any) HermesOption {
	cloned := cloneAnyMap(schema)

	return func(options *HermesOptions) { options.OutputSchema = cloneAnyMap(cloned) }
}

// WithHermesEffort configures the reasoning level passed to hermes.
func WithHermesEffort(level string) HermesOption {
	return func(options *HermesOptions) { options.Effort = level }
}

// Meta returns exactly {"hermes": {"options": {...}}} with the selected fields.
func (options HermesOptions) Meta() map[string]any {
	values := map[string]any{}

	if options.Model != "" {
		values[metaModelKey] = options.Model
	}

	if options.Env != nil {
		values[metaEnvKey] = cloneStringMap(options.Env)
	}

	if options.ExtraPathDirs != nil {
		values[metaExtraPathDirsKey] = slices.Clone(options.ExtraPathDirs)
	}

	if options.OutputSchema != nil {
		values[metaOutputSchemaKey] = cloneAnyMap(options.OutputSchema)
	}

	if options.Effort != "" {
		values[metaEffortKey] = options.Effort
	}

	return map[string]any{vendor: map[string]any{metaOptionsKey: values}}
}

func (options HermesOptions) clone() HermesOptions {
	cloned := options
	cloned.Env = cloneStringMap(options.Env)
	cloned.ExtraPathDirs = slices.Clone(options.ExtraPathDirs)
	cloned.OutputSchema = cloneAnyMap(options.OutputSchema)

	return cloned
}

// ValidateHermesSessionMeta runs the owned-namespace parsing of a session
// lifecycle request's _meta without an Agent and returns the same refusal.
func ValidateHermesSessionMeta(meta map[string]any) error {
	_, err := parseSessionMeta(meta)
	if err != nil {
		return err
	}

	return nil
}

// sessionMeta is what one session lifecycle request's _meta.hermes carried.
type sessionMeta struct {
	options   HermesOptions
	rawEvents bool
	// present records which carrier fields the request named, so a load or
	// resume inherits the stored value only for fields it left out.
	presentEnv           bool
	presentExtraPathDirs bool
}

// parseSessionMeta validates the owned _meta.hermes namespace of one session
// lifecycle request. Unknown own-namespace keys fail closed; foreign
// namespaces are ignored; the lifecycle literal is refused by name.
func parseSessionMeta(meta map[string]any) (sessionMeta, *acp.RequestError) {
	if refusal := lifecycle.RejectKey(meta); refusal != nil {
		return sessionMeta{}, invalidParam(refusal)
	}

	raw, exists := meta[vendor]
	if !exists {
		return sessionMeta{}, nil
	}

	vendorMeta, ok := raw.(map[string]any)
	if !ok {
		return sessionMeta{}, wire.Unsupported("_meta." + vendor)
	}

	parsed := sessionMeta{}

	for key := range vendorMeta {
		switch key {
		case metaOptionsKey, metaRawEventKey:
		default:
			return sessionMeta{}, wire.Unsupported("_meta." + vendor + "." + key)
		}
	}

	if rawEvent, ok := vendorMeta[metaRawEventKey]; ok {
		values, ok := rawEvent.(map[string]any)
		if !ok {
			return sessionMeta{}, wire.Unsupported("_meta." + vendor + "." + metaRawEventKey)
		}

		for key, item := range values {
			enabled, ok := item.(bool)
			if key != metaEnabledKey || !ok {
				return sessionMeta{}, wire.Unsupported("_meta." + vendor + "." + metaRawEventKey + "." + key)
			}

			parsed.rawEvents = enabled
		}
	}

	rawOptions, hasOptions := vendorMeta[metaOptionsKey]
	if !hasOptions {
		return parsed, nil
	}

	values, isObject := rawOptions.(map[string]any)
	if !isObject {
		return sessionMeta{}, wire.Unsupported(metaOptionPath(""))
	}

	options, err := parseHermesOptions(values)
	if err != nil {
		return sessionMeta{}, err
	}

	parsed.options = options
	_, parsed.presentEnv = values[metaEnvKey]
	_, parsed.presentExtraPathDirs = values[metaExtraPathDirsKey]

	return parsed, nil
}

func parseHermesOptions(values map[string]any) (HermesOptions, *acp.RequestError) {
	options := HermesOptions{}

	for key, item := range values {
		switch key {
		case metaModelKey:
			model, ok := item.(string)
			if !ok {
				return HermesOptions{}, wire.Unsupported(metaOptionPath(key))
			}

			options.Model = model
		case metaEnvKey:
			env, err := stringMapOption(item, metaOptionPath(key))
			if err != nil {
				return HermesOptions{}, err
			}

			options.Env = env
		case metaExtraPathDirsKey:
			dirs, err := stringSliceOption(item, metaOptionPath(key))
			if err != nil {
				return HermesOptions{}, err
			}

			options.ExtraPathDirs = dirs
		case metaOutputSchemaKey:
			schema, ok := item.(map[string]any)
			if !ok {
				return HermesOptions{}, wire.Unsupported(metaOptionPath(key))
			}

			options.OutputSchema = cloneAnyMap(schema)
		case metaEffortKey:
			level, ok := item.(string)
			if !ok || level == "" {
				return HermesOptions{}, wire.Unsupported(metaOptionPath(key))
			}

			options.Effort = level
		default:
			return HermesOptions{}, wire.Unsupported(metaOptionPath(key))
		}
	}

	return options, validateHermesOptions(options)
}

func validateHermesOptions(options HermesOptions) *acp.RequestError {
	if options.OutputSchema != nil {
		return wire.Unsupported(metaOptionPath(metaOutputSchemaKey))
	}

	if options.Model != "" {
		if err := hermes.ModelSelectionShapeError(options.Model); err != nil {
			return wire.Unsupported(metaOptionPath(metaModelKey))
		}
	}

	if options.Effort != "" && !slices.Contains(effortLevels(), options.Effort) {
		return wire.Unsupported(metaOptionPath(metaEffortKey))
	}

	if err := process.ValidateNames(options.Env); err != nil {
		var nameErr *process.NameError
		if errors.As(err, &nameErr) {
			return wire.Unsupported(metaOptionPath(metaEnvKey) + "." + nameErr.Key)
		}

		return wire.Unsupported(metaOptionPath(metaEnvKey))
	}

	if err := process.ValidateExtraPathDirs(options.ExtraPathDirs); err != nil {
		var dirErr *process.PathDirError
		if errors.As(err, &dirErr) {
			return wire.Unsupported(fmt.Sprintf("%s[%d]", metaOptionPath(metaExtraPathDirsKey), dirErr.Index))
		}

		return wire.Unsupported(metaOptionPath(metaExtraPathDirsKey))
	}

	return nil
}

func metaOptionPath(key string) string {
	path := "_meta." + vendor + "." + metaOptionsKey
	if key == "" {
		return path
	}

	return path + "." + key
}

func stringMapOption(value any, path string) (map[string]string, *acp.RequestError) {
	switch typed := value.(type) {
	case map[string]string:
		return cloneStringMap(typed), nil
	case map[string]any:
		result := make(map[string]string, len(typed))
		for key, item := range typed {
			text, ok := item.(string)
			if !ok {
				return nil, wire.Unsupported(path + "." + key)
			}

			result[key] = text
		}

		return result, nil
	default:
		return nil, wire.Unsupported(path)
	}
}

func stringSliceOption(value any, path string) ([]string, *acp.RequestError) {
	switch typed := value.(type) {
	case []string:
		return slices.Clone(typed), nil
	case []any:
		result := make([]string, 0, len(typed))
		for index, item := range typed {
			text, ok := item.(string)
			if !ok {
				return nil, wire.Unsupported(fmt.Sprintf("%s[%d]", path, index))
			}

			result = append(result, text)
		}

		return result, nil
	default:
		return nil, wire.Unsupported(path)
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

func cloneAny(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneAnyMap(typed)
	case []any:
		cloned := make([]any, len(typed))
		for index, item := range typed {
			cloned[index] = cloneAny(item)
		}

		return cloned
	case []string:
		return slices.Clone(typed)
	default:
		return typed
	}
}

func mergeAnyMap(base map[string]any, overlay map[string]any) map[string]any {
	result := cloneAnyMap(base)
	if result == nil {
		result = map[string]any{}
	}

	for key, value := range overlay {
		if valueMap, ok := value.(map[string]any); ok {
			if existing, ok := result[key].(map[string]any); ok {
				result[key] = mergeAnyMap(existing, valueMap)

				continue
			}
		}

		result[key] = cloneAny(value)
	}

	return result
}
