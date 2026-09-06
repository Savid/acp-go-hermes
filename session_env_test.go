package hermesacp

import (
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

func simulateSessionEnvPlatform(t *testing.T, platform string) {
	t.Helper()

	previous := nativehermes.Platform
	t.Cleanup(func() { nativehermes.Platform = previous })

	nativehermes.Platform = platform
}

func envMeta(env map[string]any) map[string]any {
	return map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{metaEnvKey: env}}}
}

func requireAmbiguousField(t *testing.T, err error, field string) {
	t.Helper()

	var requestErr *acp.RequestError
	require.ErrorAs(t, err, &requestErr)
	require.Equal(t, map[string]any{jsonFieldError: valAmbiguous, jsonFieldField: field}, requestErr.Data)
}

func TestSessionEnvAcceptsEveryStructurallyValidName(t *testing.T) {
	simulateSessionEnvPlatform(t, "linux")

	env := map[string]string{
		"https_proxy":   "",
		"no_proxy":      "",
		"WAGIE_API_URL": "http://127.0.0.1:1",
		"BASH_FUNC_x%%": "() { :; }",
		"path":          "/not/the/search/path",
		"env":           "/not/the/shell/init",
		"bash_env":      "/not/the/shell/init",
		"ld_preload":    "/not/the/loader",
		"hermes_home":   "/not/the/managed/root",
	}

	parsed, err := sessionMetaFromLifecycle(HermesOptions{Env: env}.Meta())
	require.NoError(t, err)
	require.Equal(t, env, parsed.Env)
	require.NoError(t, ValidateHermesSessionMeta(HermesOptions{Env: env}.Meta()))
}

func TestSessionEnvRefusesStructurallyInvalidEntries(t *testing.T) {
	tests := []struct {
		name  string
		value any
		field string
	}{
		{"not an object", "A=B", hermesEnvOptionPath},
		{"value is not a string", map[string]any{"A": 1}, hermesEnvOptionPath + ".A"},
		{"empty name", map[string]any{"": "x"}, hermesEnvOptionPath + "."},
		{"name carries an equals sign", map[string]any{"A=B": "x"}, hermesEnvOptionPath + ".A=B"},
		{"name carries a NUL", map[string]any{"A\x00B": "x"}, hermesEnvOptionPath + ".A\x00B"},
		{"value carries a NUL", map[string]any{"A": "x\x00y"}, hermesEnvOptionPath + ".A"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := sessionMetaFromLifecycle(envMeta(nil))
			require.NoError(t, err)

			_, err = sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{metaEnvKey: test.value}}})
			requireLifecycleMetaField(t, err, test.field)
		})
	}
}

func TestSessionEnvRefusesBlockedNamesUnderThePlatformIdentity(t *testing.T) {
	blocked := []string{
		"PATH", "NODE_OPTIONS", "BASH_ENV", "ENV",
		"LD_PRELOAD", "DYLD_INSERT_LIBRARIES",
		"HERMES_HOME", "HERMES_DASHBOARD_SESSION_TOKEN",
	}

	for _, platform := range []string{"linux", "windows"} {
		simulateSessionEnvPlatform(t, platform)

		for _, key := range blocked {
			_, err := sessionMetaFromLifecycle(HermesOptions{Env: map[string]string{key: "x"}}.Meta())
			requireLifecycleMetaField(t, err, hermesEnvOptionPath+"."+key)
		}

		for _, key := range []string{sessionManagedPathEnvPrefix + "COUNT", "acp_go_hermes_path_dir_count"} {
			_, err := sessionMetaFromLifecycle(HermesOptions{Env: map[string]string{key: "x"}}.Meta())
			requireLifecycleMetaField(t, err, hermesEnvOptionPath+"."+key)
		}
	}

	simulateSessionEnvPlatform(t, "windows")

	for _, key := range []string{"path", "Node_Options", "bash_env", "env", "ld_preload", "hermes_home"} {
		_, err := sessionMetaFromLifecycle(HermesOptions{Env: map[string]string{key: "x"}}.Meta())
		requireLifecycleMetaField(t, err, hermesEnvOptionPath+"."+key)
	}
}

func TestSessionEnvReportsTheFirstKeyInSortedOrder(t *testing.T) {
	simulateSessionEnvPlatform(t, "linux")

	_, err := sessionMetaFromLifecycle(HermesOptions{Env: map[string]string{
		"ZZ_LAST":   "x\x00y",
		"AA_FIRST=": "x",
		"MM_MID":    "x",
	}}.Meta())
	requireLifecycleMetaField(t, err, hermesEnvOptionPath+".AA_FIRST=")
}

func TestSessionEnvRefusesTwoSpellingsOfOneWindowsVariable(t *testing.T) {
	env := map[string]string{"Https_Proxy": "a", "https_proxy": "b"}

	simulateSessionEnvPlatform(t, "linux")

	parsed, err := sessionMetaFromLifecycle(HermesOptions{Env: env}.Meta())
	require.NoError(t, err)
	require.Len(t, parsed.Env, 2)

	simulateSessionEnvPlatform(t, "windows")

	_, err = sessionMetaFromLifecycle(HermesOptions{Env: env}.Meta())
	requireAmbiguousField(t, err, hermesEnvOptionPath+".https_proxy")
	requireAmbiguousField(t, ValidateHermesSessionMeta(HermesOptions{Env: env}.Meta()), hermesEnvOptionPath+".https_proxy")
}

func TestValidateHermesSessionMetaMirrorsTheSessionParser(t *testing.T) {
	require.NoError(t, ValidateHermesSessionMeta(nil))
	require.NoError(t, ValidateHermesSessionMeta(HermesOptions{
		Env:           map[string]string{"https_proxy": "", "WAGIE_API_TOKEN": "bearer"},
		ExtraPathDirs: []string{absTestPath("session", "bin")},
	}.Meta()))
	requireLifecycleMetaField(t, ValidateHermesSessionMeta(HermesOptions{Env: map[string]string{"PATH": "/bin"}}.Meta()), hermesEnvOptionPath+".PATH")
	requireLifecycleMetaField(t, ValidateHermesSessionMeta(map[string]any{hermesMetaKey: map[string]any{"extra": true}}), "_meta.hermes.extra")
	requireLifecycleMetaField(t, ValidateHermesSessionMeta(HermesOptions{ExtraPathDirs: []string{"relative"}}.Meta()), hermesExtraPathDirsOptionPath+"[0]")
}
