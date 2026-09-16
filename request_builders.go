package hermesacp

import (
	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/wire"
)

// WithSessionHermesOptions merges hermes-specific options into
// _meta.hermes.options of a session lifecycle request.
func WithSessionHermesOptions(options HermesOptions) wire.SessionRequestOption {
	return wire.WithSessionMetaValue(options.clone().Meta())
}

// WithSessionRawEvents toggles raw hermes event emission for the session.
func WithSessionRawEvents(enabled bool) wire.SessionRequestOption {
	return wire.WithSessionMetaValue(map[string]any{
		vendor: map[string]any{metaRawEventKey: map[string]any{metaEnabledKey: enabled}},
	})
}

// SetModelRequest constructs a model selector update as "provider/id".
func SetModelRequest(sessionID acp.SessionId, model string) acp.SetSessionConfigOptionRequest {
	return wire.SetConfigOptionRequest(sessionID, configModel, acp.SessionConfigValueId(model))
}
