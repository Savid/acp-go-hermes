package hermes

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"

	"go.yaml.in/yaml/v3"
)

// PluginName is the adapter's Hermes plugin: its directory under the home's
// plugins and its key in config.yaml plugins.enabled.
const PluginName = "acp-go-hermes"

// CallEvent is the gateway event the plugin broadcasts for each model
// response of a session's own conversation.
const CallEvent = "plugin." + PluginName + ".call"

//go:embed plugin/plugin.yaml plugin/__init__.py
var pluginFiles embed.FS

// PublishPlugin writes the plugin into home's plugins directory. A file
// already holding the right bytes is left alone; any other is replaced
// atomically, so a Hermes process loading plugins never reads a partial file.
func PublishPlugin(home string) error {
	dir := filepath.Join(home, "plugins", PluginName)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create plugin directory: %w", err)
	}

	entries, err := pluginFiles.ReadDir("plugin")
	if err != nil {
		return err
	}

	for _, entry := range entries {
		contents, err := pluginFiles.ReadFile("plugin/" + entry.Name())
		if err != nil {
			return err
		}

		if err := publishFile(filepath.Join(dir, entry.Name()), contents); err != nil {
			return err
		}
	}

	return nil
}

func publishFile(path string, contents []byte) error {
	current, err := os.ReadFile(path)
	if err == nil && bytes.Equal(current, contents) {
		return nil
	}

	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("read plugin file: %w", err)
	}

	staging, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return fmt.Errorf("stage plugin file: %w", err)
	}

	_, writeErr := staging.Write(contents)
	if closeErr := staging.Close(); writeErr == nil {
		writeErr = closeErr
	}

	if writeErr == nil {
		writeErr = os.Rename(staging.Name(), path)
	}

	if writeErr != nil {
		_ = os.Remove(staging.Name())

		return fmt.Errorf("write plugin file: %w", writeErr)
	}

	return nil
}

// PluginState is how config.yaml under a home lists the plugin.
type PluginState int

const (
	// PluginUnlisted is a plugin config.yaml neither enables nor disables.
	PluginUnlisted PluginState = iota
	// PluginEnabled is a plugin plugins.enabled lists and plugins.disabled does not.
	PluginEnabled
	// PluginDisabled is a plugin plugins.disabled lists, which Hermes honours
	// over plugins.enabled.
	PluginDisabled
)

// ReadPluginState reports how config.yaml under home lists the plugin. A home
// without config.yaml lists nothing.
func ReadPluginState(home string) (PluginState, error) {
	data, err := os.ReadFile(filepath.Join(home, "config.yaml"))
	if errors.Is(err, fs.ErrNotExist) {
		return PluginUnlisted, nil
	}

	if err != nil {
		return PluginUnlisted, fmt.Errorf("native config: %w", err)
	}

	var config struct {
		Plugins struct {
			Enabled  []any `yaml:"enabled"`
			Disabled []any `yaml:"disabled"`
		} `yaml:"plugins"`
	}
	if err := yaml.Unmarshal(data, &config); err != nil {
		return PluginUnlisted, fmt.Errorf("native config: %w", err)
	}

	switch {
	case slices.Contains(config.Plugins.Disabled, any(PluginName)):
		return PluginDisabled, nil
	case slices.Contains(config.Plugins.Enabled, any(PluginName)):
		return PluginEnabled, nil
	default:
		return PluginUnlisted, nil
	}
}

// EnablePlugin asks the gateway to enable the plugin through Hermes's own
// plugin manager, which records it in config.yaml and loads it into the
// running process before any session exists there.
func (c *Client) EnablePlugin(ctx context.Context) error {
	var result struct {
		OK   bool   `json:"ok"`
		Name string `json:"name"`
	}
	if err := c.Call(ctx, "plugins.manage", map[string]any{"action": "toggle", fieldKey: PluginName, "enable": true}, &result); err != nil {
		return err
	}

	if !result.OK || result.Name != PluginName {
		return errors.New("hermes refused to enable the adapter plugin")
	}

	return nil
}

// Call is one model response the plugin reported: the native session whose
// conversation received it, the model the agent sent the request to, the id
// its gateway returned when the gateway sent one Hermes did not replace, and
// the Chat Completions usage members the gateway sent.
//
//nolint:tagliatelle // the plugin writes Hermes's snake_case member names.
type Call struct {
	SessionID  string    `json:"session_id"`
	Model      string    `json:"model"`
	ResponseID string    `json:"response_id"`
	Usage      ChatUsage `json:"usage"`
}

// ChatUsage is a Chat Completions usage block. A nil member is one the
// gateway did not send, or sent as null.
//
//nolint:tagliatelle // Chat Completions uses snake_case member names.
type ChatUsage struct {
	PromptTokens        *int64              `json:"prompt_tokens"`
	CompletionTokens    *int64              `json:"completion_tokens"`
	PromptTokensDetails *PromptTokenDetails `json:"prompt_tokens_details"`
}

// PromptTokenDetails splits the prompt tokens by prompt-cache use.
//
//nolint:tagliatelle // Chat Completions uses snake_case member names.
type PromptTokenDetails struct {
	CachedTokens     *int64 `json:"cached_tokens"`
	CacheWriteTokens *int64 `json:"cache_write_tokens"`
}

// DecodeCall reads a CallEvent payload. A payload that is no call report
// reports false.
func DecodeCall(payload json.RawMessage) (Call, bool) {
	var call Call
	if json.Unmarshal(payload, &call) != nil || call.SessionID == "" {
		return Call{}, false
	}

	return call, true
}
