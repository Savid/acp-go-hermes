package hermes

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPluginChecks runs the plugin against stand-ins for the Hermes and
// OpenAI modules it imports.
func TestPluginChecks(t *testing.T) {
	t.Parallel()

	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is not installed")
	}

	output, err := exec.CommandContext(t.Context(), python, "-B", filepath.Join("testdata", "plugin_check.py"), "plugin").CombinedOutput()
	require.NoError(t, err, string(output))
}

func TestPublishPlugin(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	stale := filepath.Join(home, "plugins", PluginName, "__init__.py")
	require.NoError(t, os.MkdirAll(filepath.Dir(stale), 0o700))
	require.NoError(t, os.WriteFile(stale, []byte("stale"), 0o600))

	for range 2 {
		require.NoError(t, PublishPlugin(home))
	}

	entries, err := os.ReadDir(filepath.Dir(stale))
	require.NoError(t, err)
	require.Len(t, entries, 2, "no staging file is left behind")

	for _, name := range []string{"plugin.yaml", "__init__.py"} {
		want, err := pluginFiles.ReadFile("plugin/" + name)
		require.NoError(t, err)

		got, err := os.ReadFile(filepath.Join(home, "plugins", PluginName, name))
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

func TestReadPluginState(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		config string
		want   PluginState
		fails  bool
	}{
		"absent":           {"", PluginUnlisted, false},
		"no plugins":       {"model:\n  default: x\n", PluginUnlisted, false},
		"other enabled":    {"plugins:\n  enabled: [other]\n", PluginUnlisted, false},
		"enabled":          {"plugins:\n  enabled: [other, acp-go-hermes]\n  disabled: []\n", PluginEnabled, false},
		"disabled":         {"plugins:\n  enabled: [acp-go-hermes]\n  disabled: [acp-go-hermes]\n", PluginDisabled, false},
		"unreadable lists": {"plugins: [", PluginUnlisted, true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			home := t.TempDir()
			if tc.config != "" {
				require.NoError(t, os.WriteFile(filepath.Join(home, "config.yaml"), []byte(tc.config), 0o600))
			}

			state, err := ReadPluginState(home)
			require.Equal(t, tc.fails, err != nil, err)
			require.Equal(t, tc.want, state)
		})
	}
}

func TestDecodeCall(t *testing.T) {
	t.Parallel()

	call, ok := DecodeCall([]byte(`{"session_id":"s1","model":"m1","response_id":"gen-1","usage":{"prompt_tokens":10,"completion_tokens":5,"prompt_tokens_details":{"cached_tokens":0,"cache_write_tokens":null},"cost":0.1}}`))
	require.True(t, ok)
	require.Equal(t, "s1", call.SessionID)
	require.Equal(t, "m1", call.Model)
	require.Equal(t, "gen-1", call.ResponseID)
	require.Equal(t, int64(10), *call.Usage.PromptTokens)
	require.Equal(t, int64(5), *call.Usage.CompletionTokens)
	require.Equal(t, int64(0), *call.Usage.PromptTokensDetails.CachedTokens)
	require.Nil(t, call.Usage.PromptTokensDetails.CacheWriteTokens, "a null member is one the gateway did not state")

	for _, payload := range []string{`{"usage":{}}`, `not json`, `{"session_id":"s1","usage":[]}`} {
		_, ok := DecodeCall([]byte(payload))
		require.False(t, ok, payload)
	}
}
