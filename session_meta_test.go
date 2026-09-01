package hermesacp

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
)

func TestLifecycleMetaStrictAllowlist(t *testing.T) {
	tests := []struct {
		name string
		meta map[string]any
		err  bool
	}{
		{name: "foreign ignored", meta: map[string]any{"codex": map[string]any{"deleted": true}}},
		{name: "trace ignored", meta: map[string]any{"traceparent": "00-abc"}},
		{name: "own unknown rejected", meta: map[string]any{hermesMetaKey: map[string]any{"goals": []any{}}}, err: true},
		{name: "own option unknown rejected", meta: map[string]any{hermesMetaKey: map[string]any{"options": map[string]any{"foo": "bar"}}}, err: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := sessionMetaFromLifecycle(tt.meta)
			if tt.err && err == nil {
				t.Fatal("expected error")
			}
			if !tt.err && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestOutputSchemaUnsupported(t *testing.T) {
	_, err := sessionMetaFromLifecycle(HermesOptions{OutputSchema: map[string]any{"type": "object"}}.Meta())
	if err == nil {
		t.Fatal("outputSchema unexpectedly accepted")
	}
	var reqErr *acp.RequestError
	if !errors.As(err, &reqErr) {
		t.Fatalf("error type = %T", err)
	}
	if reqErr.Data == nil {
		t.Fatalf("missing error data: %#v", reqErr)
	}
}

func TestLifecycleMetaValidatesExtraPathDirs(t *testing.T) {
	first := filepath.Join(t.TempDir(), "first")
	second := filepath.Join(t.TempDir(), "second")
	want := []string{first, second, first}

	for _, value := range []any{
		[]string{first, second, first},
		[]any{first, second, first},
	} {
		parsed, err := sessionMetaFromLifecycle(map[string]any{
			hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{metaExtraPathDirsKey: value}},
		})
		if err != nil {
			t.Fatalf("decode %#v: %v", value, err)
		}
		if !reflect.DeepEqual(parsed.ExtraPathDirs, want) {
			t.Fatalf("decoded dirs = %#v, want %#v", parsed.ExtraPathDirs, want)
		}
	}

	tests := []struct {
		name  string
		value any
		field string
	}{
		{name: "wrong list type", value: "bad", field: hermesExtraPathDirsOptionPath},
		{name: "wrong element type", value: []any{first, 1}, field: hermesExtraPathDirsOptionPath + "[1]"},
		{name: "empty", value: []string{first, ""}, field: hermesExtraPathDirsOptionPath + "[1]"},
		{name: "relative", value: []string{"relative"}, field: hermesExtraPathDirsOptionPath + "[0]"},
		{name: "separator", value: []string{first + string(os.PathListSeparator) + second}, field: hermesExtraPathDirsOptionPath + "[0]"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := sessionMetaFromLifecycle(map[string]any{
				hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{metaExtraPathDirsKey: test.value}},
			})
			requireLifecycleMetaField(t, err, test.field)
		})
	}

	input := []string{first, second}
	parsed, err := sessionMetaFromLifecycle(HermesOptions{ExtraPathDirs: input}.Meta())
	if err != nil {
		t.Fatal(err)
	}
	input[0] = filepath.Join(t.TempDir(), "mutated")
	if parsed.ExtraPathDirs[0] != first {
		t.Fatalf("caller mutation reached session meta: %#v", parsed.ExtraPathDirs)
	}
}

func TestLifecycleMetaTracksExplicitEmptyCarriers(t *testing.T) {
	omitted, err := sessionMetaFromLifecycle(HermesOptions{}.Meta())
	if err != nil {
		t.Fatal(err)
	}
	if omitted.EnvSet || omitted.ExtraPathDirsSet {
		t.Fatalf("omitted carrier presence = env %t dirs %t", omitted.EnvSet, omitted.ExtraPathDirsSet)
	}

	explicit, err := sessionMetaFromLifecycle(HermesOptions{
		Env:           map[string]string{},
		ExtraPathDirs: []string{},
	}.Meta())
	if err != nil {
		t.Fatal(err)
	}
	if !explicit.EnvSet || !explicit.ExtraPathDirsSet || explicit.Env == nil || explicit.ExtraPathDirs == nil {
		t.Fatalf("explicit carrier = %#v", explicit)
	}
}

func TestLifecycleMetaRejectsRawPATH(t *testing.T) {
	_, err := sessionMetaFromLifecycle(HermesOptions{Env: map[string]string{"PATH": "/operation/bin"}}.Meta())
	requireLifecycleMetaField(t, err, hermesEnvOptionPath+".PATH")
	_, err = sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{
		metaEnvKey: map[string]any{"PATH": 42},
	}}})
	requireLifecycleMetaField(t, err, hermesEnvOptionPath+".PATH")

	if !sessionEnvironmentOwnsPathForPlatform("Path", "windows") {
		t.Fatal("Windows PATH comparison was case-sensitive")
	}
	if sessionEnvironmentOwnsPathForPlatform("Path", "linux") {
		t.Fatal("Unix PATH comparison was case-insensitive")
	}
	_, err = sessionMetaFromLifecycle(HermesOptions{Env: map[string]string{"BASH_ENV": "/untrusted/init"}}.Meta())
	requireLifecycleMetaField(t, err, hermesEnvOptionPath+".BASH_ENV")
	_, err = sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{
		metaEnvKey: map[string]any{"BASH_ENV": "/untrusted/init"},
	}}})
	requireLifecycleMetaField(t, err, hermesEnvOptionPath+".BASH_ENV")
	_, err = sessionMetaFromLifecycle(HermesOptions{Env: map[string]string{"ENV": "/untrusted/init"}}.Meta())
	requireLifecycleMetaField(t, err, hermesEnvOptionPath+".ENV")
	_, err = sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{
		metaEnvKey: map[string]any{"ENV": "/untrusted/init"},
	}}})
	requireLifecycleMetaField(t, err, hermesEnvOptionPath+".ENV")
	if !sessionEnvironmentOwnsShellEnvForPlatform("env", "windows") || sessionEnvironmentOwnsShellEnvForPlatform("env", "linux") {
		t.Fatal("ENV platform comparison did not match native environment semantics")
	}
	if !sessionEnvironmentOwnsBashEnvForPlatform("bash_env", "windows") || sessionEnvironmentOwnsBashEnvForPlatform("bash_env", "linux") {
		t.Fatal("BASH_ENV platform comparison did not match native environment semantics")
	}

	managed := "ACP_GO_HERMES_PATH_DIR_COUNT"
	_, err = sessionMetaFromLifecycle(HermesOptions{Env: map[string]string{managed: "1"}}.Meta())
	requireLifecycleMetaField(t, err, hermesEnvOptionPath+"."+managed)
	_, err = sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{
		metaEnvKey: map[string]any{managed: "1"},
	}}})
	requireLifecycleMetaField(t, err, hermesEnvOptionPath+"."+managed)
	if !sessionEnvironmentOwnsManagedPath(strings.ToLower(managed)) {
		t.Fatal("adapter-managed PATH namespace comparison was case-sensitive")
	}
}

func TestLifecycleEntryPointsApplyExtraPathDirValidation(t *testing.T) {
	cwd := t.TempDir()
	badMeta := map[string]any{
		hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{
			metaExtraPathDirsKey: []any{cwd, 42},
		}},
	}
	agent := newTestAgent()

	checks := map[string]func() error{
		"new": func() error {
			_, err := agent.NewSession(t.Context(), NewSessionRequest(cwd, WithSessionMeta(badMeta)))

			return err
		},
		"load": func() error {
			_, err := agent.LoadSession(t.Context(), LoadSessionRequest("missing", cwd, WithSessionMeta(badMeta)))

			return err
		},
		"resume": func() error {
			_, err := agent.ResumeSession(t.Context(), ResumeSessionRequest("missing", cwd, WithSessionMeta(badMeta)))

			return err
		},
		"fork": func() error {
			_, err := agent.forkSession(t.Context(), ForkSessionRequest("missing", cwd, WithSessionMeta(badMeta)))

			return err
		},
	}
	for name, check := range checks {
		t.Run(name, func(t *testing.T) {
			requireLifecycleMetaField(t, check(), hermesExtraPathDirsOptionPath+"[1]")
		})
	}
}

func requireLifecycleMetaField(t *testing.T, err error, field string) {
	t.Helper()

	var requestErr *acp.RequestError
	if !errors.As(err, &requestErr) {
		t.Fatalf("error type = %T, want *acp.RequestError", err)
	}
	data, ok := requestErr.Data.(map[string]any)
	if !ok || data[keyField] != field {
		t.Fatalf("error data = %#v, want field %q", requestErr.Data, field)
	}
}
