package hermesacp

import (
	"maps"
	"slices"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-hermes/internal/hermes"
)

const (
	metaOptionsKey       = "options"
	metaRawEventKey      = "rawEvent"
	metaModelKey         = "model"
	metaEnvKey           = "env"
	metaExtraPathDirsKey = "extraPathDirs"
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
	cloned := maps.Clone(env)

	return func(options *HermesOptions) { options.Env = maps.Clone(cloned) }
}

// WithHermesExtraPathDirs configures the directories prepended to the session PATH.
func WithHermesExtraPathDirs(dirs ...string) HermesOption {
	cloned := slices.Clone(dirs)

	return func(options *HermesOptions) { options.ExtraPathDirs = slices.Clone(cloned) }
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
		values[metaEnvKey] = maps.Clone(options.Env)
	}

	if options.ExtraPathDirs != nil {
		values[metaExtraPathDirsKey] = slices.Clone(options.ExtraPathDirs)
	}

	if options.Effort != "" {
		values[metaEffortKey] = options.Effort
	}

	return map[string]any{vendor: map[string]any{metaOptionsKey: values}}
}

func (options HermesOptions) clone() HermesOptions {
	cloned := options
	cloned.Env = maps.Clone(options.Env)
	cloned.ExtraPathDirs = slices.Clone(options.ExtraPathDirs)

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
		return sessionMeta{}, wire.ParamRefusal(refusal)
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
		return sessionMeta{}, wire.Unsupported(wire.MetaOptionPath(vendor, ""))
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
			if !ok || model == "" {
				return HermesOptions{}, wire.Unsupported(wire.MetaOptionPath(vendor, key))
			}

			options.Model = model
		case metaEnvKey:
			env, err := wire.StringMapOption(item, wire.MetaOptionPath(vendor, key))
			if err != nil {
				return HermesOptions{}, err
			}

			options.Env = env
		case metaExtraPathDirsKey:
			dirs, err := wire.StringSliceOption(item, wire.MetaOptionPath(vendor, key))
			if err != nil {
				return HermesOptions{}, err
			}

			options.ExtraPathDirs = dirs
		case metaEffortKey:
			level, ok := item.(string)
			if !ok || level == "" {
				return HermesOptions{}, wire.Unsupported(wire.MetaOptionPath(vendor, key))
			}

			options.Effort = level
		default:
			return HermesOptions{}, wire.Unsupported(wire.MetaOptionPath(vendor, key))
		}
	}

	return options, validateHermesOptions(options)
}

func validateHermesOptions(options HermesOptions) *acp.RequestError {
	if options.Model != "" {
		if err := hermes.ModelSelectionShapeError(options.Model); err != nil {
			return wire.Unsupported(wire.MetaOptionPath(vendor, metaModelKey))
		}
	}

	if options.Effort != "" && !slices.Contains(effortLevels(), options.Effort) {
		return wire.Unsupported(wire.MetaOptionPath(vendor, metaEffortKey))
	}

	return wire.ValidateSessionEnvironment(options.Env, options.ExtraPathDirs, wire.MetaOptionPath(vendor, ""))
}
