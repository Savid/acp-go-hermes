package hermes

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPluginChecks runs the plugin against stand-ins for the Hermes and
// OpenAI modules it imports. CI always runs it.
func TestPluginChecks(t *testing.T) {
	t.Parallel()

	python, err := exec.LookPath("python3")
	if err != nil {
		require.Empty(t, os.Getenv("CI"), "CI needs python3 for the plugin checks")
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

func TestPluginListed(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		config string
		want   bool
		fails  bool
	}{
		"absent":           {"", false, false},
		"no plugins":       {"model:\n  default: x\n", false, false},
		"other enabled":    {"plugins:\n  enabled: [other]\n", false, false},
		"enabled":          {"plugins:\n  enabled: [other, acp-go-hermes]\n  disabled: []\n", true, false},
		"disabled":         {"plugins:\n  disabled: [acp-go-hermes]\n", true, false},
		"unreadable lists": {"plugins: [", false, true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			home := t.TempDir()
			if tc.config != "" {
				require.NoError(t, os.WriteFile(filepath.Join(home, "config.yaml"), []byte(tc.config), 0o600))
			}

			listed, err := PluginListed(home)
			require.Equal(t, tc.fails, err != nil, err)
			require.Equal(t, tc.want, listed)
		})
	}
}

func TestNativeHome(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		output string
		want   string
	}{
		"profile":        {"notice\n/homes/work/profiles/a/config.yaml\n", "/homes/work/profiles/a"},
		"relative":       {"config.yaml\n", ""},
		"not the config": {"/homes/work/settings.yaml\n", ""},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			script := filepath.Join(t.TempDir(), "hermes")
			body := "#!/bin/sh\n[ \"$1 $2\" = \"config path\" ] || exit 2\nprintf '" + strings.ReplaceAll(tc.output, "\n", "\\n") + "'\n"
			require.NoError(t, os.WriteFile(script, []byte(body), 0o700))

			home, err := NativeHome(t.Context(), script, nil, t.TempDir())
			require.Equal(t, tc.want == "", err != nil, err)
			require.Equal(t, tc.want, home)
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
	require.Equal(t, CallTokens{Prompt: new(int64(10)), Output: new(int64(5)), CacheRead: new(int64(0))}, call.Usage.Tokens(),
		"a null member is one the gateway did not state")

	for _, payload := range []string{`{"usage":{}}`, `not json`, `{"session_id":"s1","usage":[]}`} {
		_, ok := DecodeCall([]byte(payload))
		require.False(t, ok, payload)
	}
}

// TestCallTokensReadHermessMembers proves each bucket is read from the
// members Hermes's usage normalization reads, in its order: the first
// non-zero figure wins, a reported zero stays 0, and a bucket no member
// states is absent.
func TestCallTokensReadHermessMembers(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		usage string
		want  CallTokens
	}{
		"chat completions": {`{"prompt_tokens":100,"completion_tokens":5,"prompt_tokens_details":{"cached_tokens":60,"cache_write_tokens":10}}`,
			CallTokens{Prompt: new(int64(100)), Output: new(int64(5)), CacheRead: new(int64(60)), CacheWrite: new(int64(10))}},
		"anthropic-style members beside a zero detail": {`{"prompt_tokens":100,"completion_tokens":5,"prompt_tokens_details":{"cached_tokens":0},"cache_read_input_tokens":70,"cache_creation_input_tokens":20}`,
			CallTokens{Prompt: new(int64(100)), Output: new(int64(5)), CacheRead: new(int64(70)), CacheWrite: new(int64(20))}},
		"detail cache creation": {`{"prompt_tokens":100,"prompt_tokens_details":{"cache_creation_input_tokens":30}}`,
			CallTokens{Prompt: new(int64(100)), CacheWrite: new(int64(30))}},
		"deepseek hits": {`{"prompt_tokens":100,"completion_tokens":5,"prompt_cache_hit_tokens":40}`,
			CallTokens{Prompt: new(int64(100)), Output: new(int64(5)), CacheRead: new(int64(40))}},
		"kimi cached tokens": {`{"prompt_tokens":100,"completion_tokens":5,"cached_tokens":50,"cache_write_tokens":0}`,
			CallTokens{Prompt: new(int64(100)), Output: new(int64(5)), CacheRead: new(int64(50)), CacheWrite: new(int64(0))}},
		"input and output names": {`{"input_tokens":80,"output_tokens":4}`,
			CallTokens{Prompt: new(int64(80)), Output: new(int64(4))}},
		"reported zeros": {`{"prompt_tokens":0,"completion_tokens":0,"prompt_tokens_details":{"cached_tokens":0}}`,
			CallTokens{Prompt: new(int64(0)), Output: new(int64(0)), CacheRead: new(int64(0))}},
		"negative clamps": {`{"prompt_tokens":-3}`, CallTokens{Prompt: new(int64(0))}},
		"nothing stated":  {`{"cost":0.1,"prompt_tokens_details":null}`, CallTokens{}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			call, ok := DecodeCall([]byte(`{"session_id":"s1","usage":` + tc.usage + `}`))
			require.True(t, ok)
			require.Equal(t, tc.want, call.Usage.Tokens())
		})
	}
}
