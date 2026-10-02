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
	"os/exec"
	"path/filepath"
	"slices"
	"strings"

	"go.yaml.in/yaml/v3"
)

// PluginName is the adapter's Hermes plugin: its directory under the home's
// plugins and its key in config.yaml plugins.enabled.
const PluginName = "acp-go-hermes"

// CallEvent is the gateway event the plugin broadcasts for each model
// response of a session's own conversation.
const CallEvent = "plugin." + PluginName + ".call"

// EnvCallReports, set to 1, makes the plugin register in the process: only
// the hermes serve the adapter starts sets it, so every other Hermes process
// sharing the home loads the plugin without reporting.
const EnvCallReports = "ACP_GO_HERMES_CALL_REPORTS"

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

// PluginListed reports whether config.yaml under home names the plugin in
// plugins.enabled or plugins.disabled. A home without config.yaml lists
// nothing.
func PluginListed(home string) (bool, error) {
	data, err := os.ReadFile(filepath.Join(home, "config.yaml"))
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("native config: %w", err)
	}

	var config struct {
		Plugins struct {
			Enabled  []any `yaml:"enabled"`
			Disabled []any `yaml:"disabled"`
		} `yaml:"plugins"`
	}
	if err := yaml.Unmarshal(data, &config); err != nil {
		return false, fmt.Errorf("native config: %w", err)
	}

	return slices.Contains(config.Plugins.Enabled, any(PluginName)) || slices.Contains(config.Plugins.Disabled, any(PluginName)), nil
}

// NativeHome is the home a hermes launched with env in dir uses: the
// directory of the config file `hermes config path` names, which follows the
// active profile.
func NativeHome(ctx context.Context, executable string, env []string, dir string) (string, error) {
	command := exec.CommandContext(ctx, executable, "config", "path")
	command.Env, command.Dir = env, dir

	output, err := command.Output()
	if err != nil {
		return "", fmt.Errorf("native config path: %w", err)
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")

	path := strings.TrimSpace(lines[len(lines)-1])
	if !filepath.IsAbs(path) || filepath.Base(path) != "config.yaml" {
		return "", errors.New("native config path: unexpected output")
	}

	return filepath.Dir(path), nil
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

// ChatUsage is a Chat Completions usage block as the gateway sent it.
type ChatUsage map[string]any

// CallTokens are the token buckets Hermes reads from a Chat Completions usage
// block. A nil bucket is one the gateway sent no figure for.
type CallTokens struct {
	Prompt     *int64
	Output     *int64
	CacheRead  *int64
	CacheWrite *int64
}

// Tokens reads the buckets from the members Hermes's usage normalization
// reads on the Chat Completions wire, in its order: each bucket is the first
// non-zero figure among its members, else 0 when one of them carried a
// number.
func (u ChatUsage) Tokens() CallTokens {
	const details = "prompt_tokens_details"

	return CallTokens{
		Prompt:    u.first([]string{"prompt_tokens"}, []string{"input_tokens"}),
		Output:    u.first([]string{"completion_tokens"}, []string{"output_tokens"}),
		CacheRead: u.first([]string{details, "cached_tokens"}, []string{"cache_read_input_tokens"}, []string{"prompt_cache_hit_tokens"}, []string{"cached_tokens"}),
		CacheWrite: u.first([]string{details, "cache_write_tokens"}, []string{details, "cache_creation_input_tokens"},
			[]string{"cache_creation_input_tokens"}, []string{"cache_write_tokens"}),
	}
}

func (u ChatUsage) first(paths ...[]string) *int64 {
	var sent bool

	for _, path := range paths {
		value, ok := u.figure(path)
		if ok && value != 0 {
			return &value
		}

		sent = sent || ok
	}

	if sent {
		return new(int64(0))
	}

	return nil
}

// figure is the count at path, clamped at zero as Hermes clamps it, and
// whether the gateway sent a number there.
func (u ChatUsage) figure(path []string) (int64, bool) {
	var value any = map[string]any(u)

	for _, key := range path {
		object, ok := value.(map[string]any)
		if !ok {
			return 0, false
		}

		value = object[key]
	}

	number, ok := value.(float64)
	if !ok {
		return 0, false
	}

	return max(0, int64(number)), true
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
