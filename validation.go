package hermesacp

import (
	"fmt"
	"path/filepath"

	"github.com/coder/acp-go-sdk"
)

func validateSessionStartPaths(cwd string, additionalDirectories []string) error {
	if err := validateRequiredAbsolutePath(jsonFieldCwd, cwd); err != nil {
		return err
	}

	for index, path := range additionalDirectories {
		if err := validateRequiredAbsolutePath(fmt.Sprintf("additionalDirectories[%d]", index), path); err != nil {
			return err
		}
	}

	return nil
}

func validateRequiredAbsolutePath(field string, value string) error {
	if value == "" {
		return acp.NewInvalidParams(map[string]any{field: validationRequired})
	}

	if !filepath.IsAbs(value) {
		return acp.NewInvalidParams(map[string]any{jsonFieldError: "absolute_path_required", keyField: field})
	}

	return nil
}

func validateOptionalAbsolutePath(field string, value *string) error {
	if value == nil {
		return nil
	}

	return validateRequiredAbsolutePath(field, *value)
}

func validateMCPServers(servers []acp.McpServer) error {
	seen := make(map[string]struct{}, len(servers))

	for index, server := range servers {
		if server.Sse != nil {
			return acp.NewInvalidParams(map[string]any{
				jsonFieldError: valUnsupported,
				keyField:       fmt.Sprintf("mcpServers[%d]", index),
				valServer:      server.Sse.Name,
			})
		}

		if server.Acp != nil {
			return acp.NewInvalidParams(map[string]any{
				jsonFieldError: valUnsupported,
				keyField:       fmt.Sprintf("mcpServers[%d]", index),
				valServer:      server.Acp.Name,
			})
		}

		name, err := mcpServerName(server, index)
		if err != nil {
			return err
		}

		if _, exists := seen[name]; exists {
			return acp.NewInvalidParams(map[string]any{
				fmt.Sprintf("mcpServers[%d].name", index): validationDuplicate,
			})
		}

		seen[name] = struct{}{}
	}

	return nil
}

// mcpServerName returns the declared name of a supported (stdio or http) MCP
// server. Names are host-supplied identity: every accepted declaration MUST
// carry a non-empty name, so an empty name is rejected as invalid params. The
// wrapper never fabricates, rewrites, or deduplicates names.
func mcpServerName(server acp.McpServer, index int) (string, error) {
	var name string

	switch {
	case server.Stdio != nil:
		name = server.Stdio.Name
	case server.Http != nil:
		name = server.Http.Name
	default:
		return "", acp.NewInvalidParams(map[string]any{keyField: fmt.Sprintf("mcpServers[%d]", index)})
	}

	if name == "" {
		return "", acp.NewInvalidParams(map[string]any{
			fmt.Sprintf("mcpServers[%d].name", index): validationRequired,
		})
	}

	return name, nil
}

func normalizeConcurrencyLimits(limits ConcurrencyLimits) (ConcurrencyLimits, error) {
	if limits.MaxActiveSessions < 0 || limits.MaxConcurrentClientCalls < 0 {
		return limits, fmt.Errorf("concurrency limits must be non-negative")
	}

	if limits.MaxActiveSessions == 0 {
		limits.MaxActiveSessions = defaultMaxActiveSessions
	}

	if limits.MaxConcurrentClientCalls == 0 {
		limits.MaxConcurrentClientCalls = defaultMaxConcurrentClientCalls
	}

	return limits, nil
}
