package hermesacp

import (
	"slices"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/wire"
)

// SessionRequestOption configures embedded-Go session lifecycle requests.
type SessionRequestOption func(*sessionRequestConfig)

type sessionRequestConfig struct {
	additionalDirectories []string
	meta                  map[string]any
}

func newSessionRequestConfig(opts ...SessionRequestOption) sessionRequestConfig {
	config := sessionRequestConfig{}
	for _, opt := range opts {
		opt(&config)
	}

	return config
}

// NewSessionRequest constructs a session/new request. It always carries an
// empty mcpServers array: MCP is not supported.
func NewSessionRequest(cwd string, opts ...SessionRequestOption) acp.NewSessionRequest {
	config := newSessionRequestConfig(opts...)

	return acp.NewSessionRequest{
		Cwd:                   cwd,
		McpServers:            []acp.McpServer{},
		AdditionalDirectories: slices.Clone(config.additionalDirectories),
		Meta:                  cloneAnyMap(config.meta),
	}
}

// LoadSessionRequest constructs a session/load request.
func LoadSessionRequest(sessionID acp.SessionId, cwd string, opts ...SessionRequestOption) acp.LoadSessionRequest {
	config := newSessionRequestConfig(opts...)

	return acp.LoadSessionRequest{
		SessionId:             sessionID,
		Cwd:                   cwd,
		McpServers:            []acp.McpServer{},
		AdditionalDirectories: slices.Clone(config.additionalDirectories),
		Meta:                  cloneAnyMap(config.meta),
	}
}

// ResumeSessionRequest constructs a session/resume request.
func ResumeSessionRequest(sessionID acp.SessionId, cwd string, opts ...SessionRequestOption) acp.ResumeSessionRequest {
	config := newSessionRequestConfig(opts...)

	return acp.ResumeSessionRequest{
		SessionId:             sessionID,
		Cwd:                   cwd,
		McpServers:            []acp.McpServer{},
		AdditionalDirectories: slices.Clone(config.additionalDirectories),
		Meta:                  cloneAnyMap(config.meta),
	}
}

// DeleteSessionRequest constructs a session/delete request.
func DeleteSessionRequest(sessionID acp.SessionId) acp.UnstableDeleteSessionRequest {
	return acp.UnstableDeleteSessionRequest{SessionId: sessionID}
}

// WithSessionAdditionalDirectories sets additional workspace directories.
func WithSessionAdditionalDirectories(paths ...string) SessionRequestOption {
	cloned := slices.Clone(paths)

	return func(config *sessionRequestConfig) {
		config.additionalDirectories = slices.Clone(cloned)
	}
}

// WithSessionMeta merges host metadata into a session lifecycle request. A
// key matching a family-reserved literal panics rather than merging: those
// namespaces are stamped by the adapter alone.
func WithSessionMeta(meta map[string]any) SessionRequestOption {
	rejectReservedMeta("WithSessionMeta", meta)

	cloned := cloneAnyMap(meta)

	return func(config *sessionRequestConfig) {
		config.meta = mergeAnyMap(config.meta, cloned)
	}
}

// WithSessionHermesOptions merges hermes-specific options into _meta.hermes.options.
func WithSessionHermesOptions(options HermesOptions) SessionRequestOption {
	cloned := options.clone()

	return func(config *sessionRequestConfig) {
		config.meta = mergeAnyMap(config.meta, cloned.Meta())
	}
}

// WithSessionOutputSchema sets structured output, which hermes refuses at
// session start.
func WithSessionOutputSchema(schema map[string]any) SessionRequestOption {
	cloned := cloneAnyMap(schema)

	return func(config *sessionRequestConfig) {
		config.meta = mergeAnyMap(config.meta, HermesOptions{OutputSchema: cloned}.Meta())
	}
}

// WithSessionRawEvents toggles raw hermes event emission for the session.
func WithSessionRawEvents(enabled bool) SessionRequestOption {
	return func(config *sessionRequestConfig) {
		config.meta = mergeAnyMap(config.meta, map[string]any{
			vendor: map[string]any{metaRawEventKey: map[string]any{metaEnabledKey: enabled}},
		})
	}
}

// PromptRequest constructs a session/prompt request.
func PromptRequest(sessionID acp.SessionId, blocks ...acp.ContentBlock) acp.PromptRequest {
	return acp.PromptRequest{
		SessionId: sessionID,
		Prompt:    append([]acp.ContentBlock{}, blocks...),
	}
}

// TextPromptRequest constructs a session/prompt request with one text block.
func TextPromptRequest(sessionID acp.SessionId, text string) acp.PromptRequest {
	return PromptRequest(sessionID, acp.TextBlock(text))
}

// CancelRequest constructs a session/cancel notification.
func CancelRequest(sessionID acp.SessionId) acp.CancelNotification {
	return acp.CancelNotification{SessionId: sessionID}
}

// SetConfigOptionRequest constructs a value-id session/set_config_option request.
func SetConfigOptionRequest(
	sessionID acp.SessionId,
	configID acp.SessionConfigId,
	value acp.SessionConfigValueId,
) acp.SetSessionConfigOptionRequest {
	return acp.SetSessionConfigOptionRequest{
		ValueId: &acp.SetSessionConfigOptionValueId{
			SessionId: sessionID,
			ConfigId:  configID,
			Value:     value,
		},
	}
}

// SetModelRequest constructs a model selector update as "provider/id".
func SetModelRequest(sessionID acp.SessionId, model string) acp.SetSessionConfigOptionRequest {
	return SetConfigOptionRequest(sessionID, configModel, acp.SessionConfigValueId(model))
}

// ListSessionsRequestOption configures embedded-Go session/list requests.
type ListSessionsRequestOption func(*acp.ListSessionsRequest)

// ListSessionsRequest constructs a session/list request.
func ListSessionsRequest(opts ...ListSessionsRequestOption) acp.ListSessionsRequest {
	var req acp.ListSessionsRequest
	for _, opt := range opts {
		opt(&req)
	}

	return req
}

// WithListSessionsCwd filters session/list by cwd.
func WithListSessionsCwd(cwd string) ListSessionsRequestOption {
	return func(req *acp.ListSessionsRequest) { req.Cwd = &cwd }
}

// WithListSessionsCursor sets the pagination cursor.
func WithListSessionsCursor(cursor string) ListSessionsRequestOption {
	return func(req *acp.ListSessionsRequest) { req.Cursor = &cursor }
}

// WithListSessionsMeta sets host metadata on a session/list request, refusing
// reserved literals like WithSessionMeta.
func WithListSessionsMeta(meta map[string]any) ListSessionsRequestOption {
	rejectReservedMeta("WithListSessionsMeta", meta)

	cloned := cloneAnyMap(meta)

	return func(req *acp.ListSessionsRequest) { req.Meta = mergeAnyMap(req.Meta, cloned) }
}

func rejectReservedMeta(builder string, meta map[string]any) {
	if err := wire.CheckReservedMeta(meta); err != nil {
		panic(builder + ": " + err.Error())
	}
}
