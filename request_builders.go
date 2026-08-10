package hermesacp

import (
	"context"
	"encoding/json"
	"slices"

	"github.com/coder/acp-go-sdk"
)

const (
	metaOptionsKey       = "options"
	metaModelKey         = "model"
	metaEnvKey           = "env"
	metaExtraPathDirsKey = "extraPathDirs"
	metaOutputSchemaKey  = "outputSchema"
)

// HermesOptions is the stable Hermes-specific subset accepted at
// _meta.hermes.options.
type HermesOptions struct {
	Model         string            `json:"model,omitempty"`
	Env           map[string]string `json:"env,omitempty"`
	ExtraPathDirs []string          `json:"extraPathDirs,omitempty"`
	OutputSchema  map[string]any    `json:"outputSchema,omitempty"`
}

// Meta returns an ACP _meta object for the supported Hermes-specific options.
func (options HermesOptions) Meta() map[string]any {
	values := map[string]any{}
	if options.Model != "" {
		values[metaModelKey] = options.Model
	}

	if len(options.Env) > 0 {
		values[metaEnvKey] = cloneStringMap(options.Env)
	}

	if len(options.ExtraPathDirs) > 0 {
		values[metaExtraPathDirsKey] = slices.Clone(options.ExtraPathDirs)
	}

	if options.OutputSchema != nil {
		values[metaOutputSchemaKey] = cloneAnyMap(options.OutputSchema)
	}

	return map[string]any{
		hermesMetaKey: map[string]any{
			metaOptionsKey: values,
		},
	}
}

type SessionRequestOption func(*sessionRequestConfig)

type sessionRequestConfig struct {
	additionalDirectories []string
	mcpServers            []acp.McpServer
	meta                  map[string]any
}

func NewSessionRequest(cwd string, opts ...SessionRequestOption) acp.NewSessionRequest {
	config := newSessionRequestConfig(opts...)

	return acp.NewSessionRequest{
		Cwd:                   cwd,
		McpServers:            config.stableMCPServers(),
		AdditionalDirectories: config.additionalDirectoriesClone(),
		Meta:                  cloneAnyMap(config.meta),
	}
}

func LoadSessionRequest(sessionID acp.SessionId, cwd string, opts ...SessionRequestOption) acp.LoadSessionRequest {
	config := newSessionRequestConfig(opts...)

	return acp.LoadSessionRequest{
		SessionId:             sessionID,
		Cwd:                   cwd,
		McpServers:            config.stableMCPServers(),
		AdditionalDirectories: config.additionalDirectoriesClone(),
		Meta:                  cloneAnyMap(config.meta),
	}
}

func ResumeSessionRequest(sessionID acp.SessionId, cwd string, opts ...SessionRequestOption) acp.ResumeSessionRequest {
	config := newSessionRequestConfig(opts...)

	return acp.ResumeSessionRequest{
		SessionId:             sessionID,
		Cwd:                   cwd,
		McpServers:            config.stableMCPServers(),
		AdditionalDirectories: config.additionalDirectoriesClone(),
		Meta:                  cloneAnyMap(config.meta),
	}
}

func ForkSessionRequest(sessionID acp.SessionId, cwd string, opts ...SessionRequestOption) acp.UnstableForkSessionRequest {
	config := newSessionRequestConfig(opts...)

	return acp.UnstableForkSessionRequest{
		SessionId:             sessionID,
		Cwd:                   cwd,
		McpServers:            unstableMCPServersFromStable(config.stableMCPServers()),
		AdditionalDirectories: config.additionalDirectoriesClone(),
		Meta:                  cloneAnyMap(config.meta),
	}
}

func DeleteSessionRequest(sessionID acp.SessionId) acp.UnstableDeleteSessionRequest {
	return acp.UnstableDeleteSessionRequest{SessionId: sessionID}
}

func WithSessionMCPServers(servers ...acp.McpServer) SessionRequestOption {
	cloned := cloneMCPServers(servers)

	return func(config *sessionRequestConfig) {
		config.mcpServers = cloneMCPServers(cloned)
	}
}

func WithSessionAdditionalDirectories(paths ...string) SessionRequestOption {
	cloned := append([]string(nil), paths...)

	return func(config *sessionRequestConfig) {
		config.additionalDirectories = append([]string(nil), cloned...)
	}
}

func WithSessionMeta(meta map[string]any) SessionRequestOption {
	cloned := cloneAnyMap(meta)

	return func(config *sessionRequestConfig) {
		config.meta = mergeAnyMap(config.meta, cloned)
	}
}

func WithSessionHermesOptions(options HermesOptions) SessionRequestOption {
	cloned := cloneHermesOptions(options)

	return func(config *sessionRequestConfig) {
		config.meta = mergeAnyMap(config.meta, cloned.Meta())
	}
}

func WithSessionOutputSchema(schema map[string]any) SessionRequestOption {
	cloned := cloneAnyMap(schema)

	return func(config *sessionRequestConfig) {
		config.meta = mergeAnyMap(config.meta, HermesOptions{OutputSchema: cloned}.Meta())
	}
}

func WithSessionRawEvents(enabled bool) SessionRequestOption {
	return func(config *sessionRequestConfig) {
		if config.meta == nil {
			config.meta = map[string]any{}
		}

		hermesMeta := ensureMetaMap(config.meta, hermesMetaKey)
		hermesMeta[rawEventKey] = map[string]any{rawEventEnabledKey: enabled}
		config.meta[hermesMetaKey] = hermesMeta
	}
}

func StdioMCPServer(name string, command string, args []string, env map[string]string) acp.McpServer {
	variables := make([]acp.EnvVariable, 0, len(env))
	for key, value := range env {
		variables = append(variables, acp.EnvVariable{Name: key, Value: value})
	}

	return acp.McpServer{Stdio: &acp.McpServerStdio{
		Name:    name,
		Command: command,
		Args:    append([]string(nil), args...),
		Env:     variables,
	}}
}

func HTTPMCPServer(name string, url string, headers map[string]string) acp.McpServer {
	values := make([]acp.HttpHeader, 0, len(headers))
	for key, value := range headers {
		values = append(values, acp.HttpHeader{Name: key, Value: value})
	}

	return acp.McpServer{Http: &acp.McpServerHttpInline{
		Name:    name,
		Url:     url,
		Headers: values,
	}}
}

func PromptRequest(sessionID acp.SessionId, turnNonce string, blocks ...acp.ContentBlock) acp.PromptRequest {
	return acp.PromptRequest{
		SessionId: sessionID,
		Meta:      turnRouteMeta(turnNonce),
		Prompt:    append([]acp.ContentBlock{}, blocks...),
	}
}

func TextPromptRequest(sessionID acp.SessionId, turnNonce, text string) acp.PromptRequest {
	return PromptRequest(sessionID, turnNonce, acp.TextBlock(text))
}

// CancelRequest builds an active-turn cancellation carrying the mandatory route nonce.
func CancelRequest(sessionID acp.SessionId, turnNonce string) acp.CancelNotification {
	return acp.CancelNotification{SessionId: sessionID, Meta: turnRouteMeta(turnNonce)}
}

func SetConfigOptionRequest(sessionID acp.SessionId, configID acp.SessionConfigId, value acp.SessionConfigValueId) acp.SetSessionConfigOptionRequest {
	return acp.SetSessionConfigOptionRequest{
		ValueId: &acp.SetSessionConfigOptionValueId{
			SessionId: sessionID,
			ConfigId:  configID,
			Value:     value,
		},
	}
}

func SetModelRequest(sessionID acp.SessionId, model string) acp.SetSessionConfigOptionRequest {
	return SetConfigOptionRequest(sessionID, configModel, acp.SessionConfigValueId(model))
}

func CallForkSession(ctx context.Context, conn *acp.ClientSideConnection, params acp.UnstableForkSessionRequest) (acp.UnstableForkSessionResponse, error) {
	raw, err := conn.CallExtension(ctx, ForkSessionMethod, params)
	if err != nil {
		return acp.UnstableForkSessionResponse{}, err
	}

	var resp acp.UnstableForkSessionResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return acp.UnstableForkSessionResponse{}, err
	}

	return resp, nil
}

type ListSessionsRequestOption func(*acp.ListSessionsRequest)

func ListSessionsRequest(opts ...ListSessionsRequestOption) acp.ListSessionsRequest {
	var req acp.ListSessionsRequest
	for _, opt := range opts {
		opt(&req)
	}

	return req
}

func WithListSessionsCwd(cwd string) ListSessionsRequestOption {
	return func(req *acp.ListSessionsRequest) {
		value := cwd
		req.Cwd = &value
	}
}

func WithListSessionsCursor(cursor string) ListSessionsRequestOption {
	return func(req *acp.ListSessionsRequest) {
		value := cursor
		req.Cursor = &value
	}
}

func WithListSessionsMeta(meta map[string]any) ListSessionsRequestOption {
	cloned := cloneAnyMap(meta)

	return func(req *acp.ListSessionsRequest) {
		req.Meta = mergeAnyMap(req.Meta, cloned)
	}
}

type HermesOption func(*HermesOptions)

func NewHermesOptions(opts ...HermesOption) HermesOptions {
	options := HermesOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	return cloneHermesOptions(options)
}

func WithHermesModel(model string) HermesOption {
	return func(options *HermesOptions) {
		options.Model = model
	}
}

func WithHermesEnv(env map[string]string) HermesOption {
	cloned := cloneStringMap(env)

	return func(options *HermesOptions) {
		options.Env = cloneStringMap(cloned)
	}
}

// WithHermesExtraPathDirs configures absolute directories placed, in order,
// ahead of the native base PATH for this session's Hermes process. Session Env
// cannot carry PATH; this ordered option is the sole session PATH authority.
func WithHermesExtraPathDirs(dirs ...string) HermesOption {
	cloned := slices.Clone(dirs)

	return func(options *HermesOptions) {
		options.ExtraPathDirs = slices.Clone(cloned)
	}
}

func WithHermesOutputSchema(schema map[string]any) HermesOption {
	cloned := cloneAnyMap(schema)

	return func(options *HermesOptions) {
		options.OutputSchema = cloneAnyMap(cloned)
	}
}

func newSessionRequestConfig(opts ...SessionRequestOption) sessionRequestConfig {
	config := sessionRequestConfig{}
	for _, opt := range opts {
		opt(&config)
	}

	return config
}

func (config sessionRequestConfig) stableMCPServers() []acp.McpServer {
	if config.mcpServers == nil {
		return []acp.McpServer{}
	}

	return cloneMCPServers(config.mcpServers)
}

func (config sessionRequestConfig) additionalDirectoriesClone() []string {
	return append([]string(nil), config.additionalDirectories...)
}

func cloneHermesOptions(options HermesOptions) HermesOptions {
	return HermesOptions{
		Model:         options.Model,
		Env:           cloneStringMap(options.Env),
		ExtraPathDirs: slices.Clone(options.ExtraPathDirs),
		OutputSchema:  cloneAnyMap(options.OutputSchema),
	}
}

func mergeAnyMap(base map[string]any, overlay map[string]any) map[string]any {
	result := cloneAnyMap(base)
	if result == nil {
		result = map[string]any{}
	}

	for key, value := range overlay {
		if valueMap, ok := value.(map[string]any); ok {
			if existingMap, ok := result[key].(map[string]any); ok {
				result[key] = mergeAnyMap(existingMap, valueMap)

				continue
			}
		}

		result[key] = cloneAny(value)
	}

	return result
}

func ensureMetaMap(meta map[string]any, key string) map[string]any {
	current, _ := meta[key].(map[string]any)
	if current == nil {
		current = map[string]any{}
	} else {
		current = cloneAnyMap(current)
	}

	meta[key] = current

	return current
}

func cloneMCPServers(servers []acp.McpServer) []acp.McpServer {
	if servers == nil {
		return nil
	}

	cloned := make([]acp.McpServer, len(servers))
	for index, server := range servers {
		cloned[index] = cloneMCPServer(server)
	}

	return cloned
}

func cloneMCPServer(server acp.McpServer) acp.McpServer {
	switch {
	case server.Http != nil:
		value := *server.Http
		value.Meta = cloneAnyMap(value.Meta)
		value.Headers = cloneHTTPHeaders(value.Headers)

		return acp.McpServer{Http: &value}
	case server.Sse != nil:
		value := *server.Sse
		value.Meta = cloneAnyMap(value.Meta)
		value.Headers = cloneHTTPHeaders(value.Headers)

		return acp.McpServer{Sse: &value}
	case server.Acp != nil:
		value := *server.Acp
		value.Meta = cloneAnyMap(value.Meta)

		return acp.McpServer{Acp: &value}
	case server.Stdio != nil:
		return acp.McpServer{Stdio: cloneMCPServerStdio(server.Stdio)}
	default:
		return acp.McpServer{}
	}
}

func cloneMCPServerStdio(server *acp.McpServerStdio) *acp.McpServerStdio {
	if server == nil {
		return nil
	}

	value := *server
	value.Meta = cloneAnyMap(value.Meta)
	value.Args = append([]string(nil), value.Args...)
	value.Env = cloneEnvVariables(value.Env)

	return &value
}

func cloneHTTPHeaders(headers []acp.HttpHeader) []acp.HttpHeader {
	if headers == nil {
		return nil
	}

	cloned := make([]acp.HttpHeader, len(headers))
	for index, header := range headers {
		cloned[index] = header
		cloned[index].Meta = cloneAnyMap(header.Meta)
	}

	return cloned
}

func cloneEnvVariables(env []acp.EnvVariable) []acp.EnvVariable {
	if env == nil {
		return nil
	}

	cloned := make([]acp.EnvVariable, len(env))
	for index, variable := range env {
		cloned[index] = variable
		cloned[index].Meta = cloneAnyMap(variable.Meta)
	}

	return cloned
}

func unstableMCPServersFromStable(servers []acp.McpServer) []acp.UnstableMcpServer {
	if servers == nil {
		return nil
	}

	cloned := make([]acp.UnstableMcpServer, len(servers))
	for index, server := range servers {
		cloned[index] = unstableMCPServerFromStable(server)
	}

	return cloned
}

func unstableMCPServerFromStable(server acp.McpServer) acp.UnstableMcpServer {
	switch {
	case server.Http != nil:
		value := acp.UnstableMcpServerHttp{
			Meta:    cloneAnyMap(server.Http.Meta),
			Headers: cloneHTTPHeaders(server.Http.Headers),
			Name:    server.Http.Name,
			Type:    server.Http.Type,
			Url:     server.Http.Url,
		}

		return acp.UnstableMcpServer{Http: &value}
	case server.Sse != nil:
		value := acp.UnstableMcpServerSse{
			Meta:    cloneAnyMap(server.Sse.Meta),
			Headers: cloneHTTPHeaders(server.Sse.Headers),
			Name:    server.Sse.Name,
			Type:    server.Sse.Type,
			Url:     server.Sse.Url,
		}

		return acp.UnstableMcpServer{Sse: &value}
	case server.Acp != nil:
		value := acp.UnstableMcpServerAcpInline{
			Meta: cloneAnyMap(server.Acp.Meta),
			Id:   acp.UnstableMcpServerAcpId(server.Acp.Id),
			Name: server.Acp.Name,
			Type: server.Acp.Type,
		}

		return acp.UnstableMcpServer{Acp: &value}
	case server.Stdio != nil:
		return acp.UnstableMcpServer{Stdio: cloneMCPServerStdio(server.Stdio)}
	default:
		return acp.UnstableMcpServer{}
	}
}

func stableMCPServersFromUnstable(servers []acp.UnstableMcpServer) []acp.McpServer {
	if servers == nil {
		return nil
	}

	out := make([]acp.McpServer, len(servers))
	for index, server := range servers {
		switch {
		case server.Http != nil:
			out[index] = acp.McpServer{Http: &acp.McpServerHttpInline{
				Meta:    cloneAnyMap(server.Http.Meta),
				Headers: cloneHTTPHeaders(server.Http.Headers),
				Name:    server.Http.Name,
				Type:    server.Http.Type,
				Url:     server.Http.Url,
			}}
		case server.Sse != nil:
			out[index] = acp.McpServer{Sse: &acp.McpServerSseInline{
				Meta:    cloneAnyMap(server.Sse.Meta),
				Headers: cloneHTTPHeaders(server.Sse.Headers),
				Name:    server.Sse.Name,
				Type:    server.Sse.Type,
				Url:     server.Sse.Url,
			}}
		case server.Acp != nil:
			out[index] = acp.McpServer{Acp: &acp.McpServerAcpInline{
				Meta: cloneAnyMap(server.Acp.Meta),
				Id:   acp.McpServerAcpId(server.Acp.Id),
				Name: server.Acp.Name,
				Type: server.Acp.Type,
			}}
		case server.Stdio != nil:
			out[index] = acp.McpServer{Stdio: cloneMCPServerStdio(server.Stdio)}
		}
	}

	return out
}
