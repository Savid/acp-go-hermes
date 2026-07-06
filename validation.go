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
	}

	return nil
}

func normalizeConcurrencyLimits(limits ConcurrencyLimits) (ConcurrencyLimits, error) {
	if limits.MaxActiveSessions < 0 || limits.MaxConcurrentPrompts < 0 || limits.MaxConcurrentClientCalls < 0 {
		return limits, fmt.Errorf("concurrency limits must be non-negative")
	}

	if limits.MaxActiveSessions == 0 {
		limits.MaxActiveSessions = defaultMaxActiveSessions
	}

	if limits.MaxConcurrentPrompts == 0 {
		limits.MaxConcurrentPrompts = defaultMaxConcurrentPrompts
	}

	if limits.MaxConcurrentClientCalls == 0 {
		limits.MaxConcurrentClientCalls = defaultMaxConcurrentClientCalls
	}

	return limits, nil
}
