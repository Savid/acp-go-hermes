package hermesacp

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

func TestValidationMetaAndHelperBranches(t *testing.T) {
	if err := validateSessionStartPaths("relative", nil); err == nil {
		t.Fatal("relative cwd accepted")
	}
	if err := validateRequiredAbsolutePath("cwd", ""); err == nil {
		t.Fatal("empty required absolute path accepted")
	}
	if err := validateSessionStartPaths("/tmp/project", []string{"relative"}); err == nil {
		t.Fatal("relative additional directory accepted")
	}
	value := "/tmp/project"
	if err := validateOptionalAbsolutePath("cwd", &value); err != nil {
		t.Fatalf("validateOptionalAbsolutePath: %v", err)
	}
	if err := validateMCPServers([]acp.McpServer{{Sse: &acp.McpServerSseInline{Name: "sse"}}}); err == nil {
		t.Fatal("unsupported MCP servers accepted")
	}
	if err := validateMCPServers([]acp.McpServer{{Acp: &acp.McpServerAcpInline{Name: "acp"}}}); err == nil {
		t.Fatal("unsupported ACP MCP server accepted")
	}
	if _, err := normalizeConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: -1}); err == nil {
		t.Fatal("negative active-session limit accepted")
	}
	if _, err := normalizeConcurrencyLimits(ConcurrencyLimits{MaxConcurrentClientCalls: -1}); err == nil {
		t.Fatal("negative client-call limit accepted")
	}
	if limits, err := normalizeConcurrencyLimits(ConcurrencyLimits{}); err != nil ||
		limits.MaxActiveSessions != defaultMaxActiveSessions ||
		limits.MaxConcurrentClientCalls != defaultMaxConcurrentClientCalls {
		t.Fatalf("default concurrency limits = %#v err=%v", limits, err)
	}
	if _, err := stringMapFromMeta(map[string]any{"A": 1}); err == nil {
		t.Fatal("non-string env accepted")
	}
	if env, err := stringMapFromMeta(map[string]string{"A": "1"}); err != nil || env["A"] != "1" {
		t.Fatalf("stringMapFromMeta map[string]string = %#v err=%v", env, err)
	}
	if err := validateLifecycleMeta(map[string]any{hermesMetaKey: "bad"}); err == nil {
		t.Fatal("bad hermes meta accepted")
	}
	if err := validateLifecycleMeta(map[string]any{"github.com/savid/acp-go-hermes": map[string]any{}}); err != nil {
		t.Fatalf("foreign module-path meta must be ignored, got %v", err)
	}
	if err := validateLifecycleMeta(map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: "bad"}}); err == nil {
		t.Fatal("bad options meta accepted")
	}
	if err := validateLifecycleMeta(map[string]any{hermesMetaKey: map[string]any{rawEventKey: "bad"}}); err == nil {
		t.Fatal("bad raw event object accepted")
	}
	if err := validateLifecycleMeta(map[string]any{hermesMetaKey: map[string]any{rawEventKey: map[string]any{"unknown": true}}}); err == nil {
		t.Fatal("unknown raw event key accepted")
	}
	if _, err := sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{rawEventKey: map[string]any{rawEventEnabledKey: "bad"}}}); err == nil {
		t.Fatal("bad raw event meta accepted")
	}
	meta, err := sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{
		metaModelKey: "p/m",
		metaEnvKey:   map[string]any{"A": "1"},
	}}})
	if err != nil || meta.Model != "p/m" || meta.Env["A"] != "1" {
		t.Fatalf("session meta = %#v err=%v", meta, err)
	}
	meta, err = sessionMetaFromLifecycle(map[string]any{})
	if err != nil {
		t.Fatalf("empty meta = %#v err=%v", meta, err)
	}
	if _, err := hermesOptionsFromMeta(map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{metaEnvKey: "bad"}}}); err == nil {
		t.Fatal("bad env meta accepted")
	}
	if _, err := sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{metaEnvKey: "bad"}}}); err == nil {
		t.Fatal("bad env lifecycle meta accepted")
	}
	testValidationSchemaAndCloneHelpers(t)
}

func TestValidateImageLimits(t *testing.T) {
	if err := validateImageLimits(ImageLimits{}); err != nil {
		t.Fatalf("zero image limits rejected: %v", err)
	}
	for name, limits := range map[string]ImageLimits{
		"input image":  {MaxInputBytesPerImage: -1},
		"input prompt": {MaxInputBytesPerPrompt: -1},
		"output image": {MaxOutputBytesPerImage: -1},
		"output tool":  {MaxOutputBytesPerToolCall: -1},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateImageLimits(limits); err == nil {
				t.Fatal("negative image limit accepted")
			}
		})
	}
}
func testValidationSchemaAndCloneHelpers(t *testing.T) {
	t.Helper()

	for name, schema := range map[string]any{
		"object":       map[string]any{"type": "object"},
		"empty-object": map[string]any{},
		"array":        []any{"bad"},
		"string":       "bad",
	} {
		_, err := sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{metaOutputSchemaKey: schema}}})
		requireUnsupportedField(t, err, "_meta.hermes.options.outputSchema", "outputSchema "+name)
	}
	requireUnsupportedField(t, mustErr(sessionMetaFromLifecycle(map[string]any{hermesMetaKey: "bad"})), "_meta.hermes", "hermes non-object")
	requireUnsupportedField(t, mustErr(sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: "bad"}})), "_meta.hermes.options", "options non-object")
	requireUnsupportedField(t, mustErr(sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{metaModelKey: 7}}})), "_meta.hermes.options.model", "non-string model")
	requireUnsupportedField(t, mustErr(sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{metaEnvKey: "bad"}}})), "_meta.hermes.options.env", "env non-object")
	requireUnsupportedField(t, mustErr(sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{metaEnvKey: map[string]any{"A": 1}}}})), "_meta.hermes.options.env", "env non-string value")
	requireUnsupportedField(t, mustErr(sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{rawEventKey: "bad"}})), "_meta.hermes.rawEvent", "rawEvent non-object")
	requireUnsupportedField(t, mustErr(sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{rawEventKey: map[string]any{rawEventEnabledKey: "bad"}}})), "_meta.hermes.rawEvent.enabled", "rawEvent enabled non-bool")
	if got := cloneAny([]any{map[string]any{"a": "b"}}); !reflect.DeepEqual(got, []any{map[string]any{"a": "b"}}) {
		t.Fatalf("cloneAny slice = %#v", got)
	}
	if cloneAnySlice(nil) != nil {
		t.Fatal("nil cloneAnySlice returned non-nil")
	}
	if splitProvider, splitModel := splitModelValue("model-only", "p", "m"); splitProvider != "p" || splitModel != "model-only" {
		t.Fatalf("split fallback = %q %q", splitProvider, splitModel)
	}
	if joinModelValue("", "m") != "m" || joinModelValue("p", "") != "p" {
		t.Fatal("joinModelValue fallback mismatch")
	}
}
func mustErr(_ sessionMeta, err error) error {
	return err
}
func TestValidateMCPServerNames(t *testing.T) {
	t.Run("rejects empty name", func(t *testing.T) {
		cases := map[string][]acp.McpServer{
			"stdio":            {StdioMCPServer("", "cmd", nil, nil)},
			"http":             {HTTPMCPServer("", "https://example.test/mcp", nil)},
			"stdio-whitespace": {StdioMCPServer("   ", "cmd", nil, nil)},
			"http-whitespace":  {HTTPMCPServer("   ", "https://example.test/mcp", nil)},
		}
		for name, servers := range cases {
			t.Run(name, func(t *testing.T) {
				data := requireInvalidParamsData(t, validateMCPServers(servers))
				want := map[string]any{"mcpServers[0].name": "required"}
				if !reflect.DeepEqual(data, want) {
					t.Fatalf("error data = %#v, want %#v", data, want)
				}
			})
		}
	})

	t.Run("rejects duplicate name at later index", func(t *testing.T) {
		servers := []acp.McpServer{
			StdioMCPServer("dup", "cmd", nil, nil),
			HTTPMCPServer("keep", "https://example.test/mcp", nil),
			HTTPMCPServer("dup", "https://collision.test/mcp", nil),
		}
		data := requireInvalidParamsData(t, validateMCPServers(servers))
		want := map[string]any{"mcpServers[2].name": "duplicate"}
		if !reflect.DeepEqual(data, want) {
			t.Fatalf("error data = %#v, want %#v", data, want)
		}
	})

	t.Run("rejects server with no transport", func(t *testing.T) {
		data := requireInvalidParamsData(t, validateMCPServers([]acp.McpServer{{}}))
		want := map[string]any{"error": "no_transport", "field": "mcpServers[0]"}
		if !reflect.DeepEqual(data, want) {
			t.Fatalf("error data = %#v, want %#v", data, want)
		}
	})

	t.Run("accepts unique named servers", func(t *testing.T) {
		servers := []acp.McpServer{
			StdioMCPServer("stdio", "cmd", nil, nil),
			HTTPMCPServer("http", "https://example.test/mcp", nil),
		}
		if err := validateMCPServers(servers); err != nil {
			t.Fatalf("unique named servers rejected: %v", err)
		}
	})

	t.Run("renders names verbatim into config", func(t *testing.T) {
		servers := []acp.McpServer{
			StdioMCPServer("stdio", "cmd", []string{"arg"}, map[string]string{"E": "V"}),
			HTTPMCPServer("http", "https://example.test/mcp", map[string]string{"Authorization": "token"}),
		}
		block := nativehermes.MCPServersConfig(servers)
		mcp, ok := block["mcp_servers"].(map[string]any)
		if !ok {
			t.Fatalf("mcp_servers block missing: %#v", block)
		}
		if len(mcp) != 2 {
			t.Fatalf("mcp_servers = %#v, want two entries keyed by name", mcp)
		}
		stdio, ok := mcp["stdio"].(map[string]any)
		if !ok || stdio["command"] != "cmd" {
			t.Fatalf("stdio entry = %#v", mcp["stdio"])
		}
		httpEntry, ok := mcp["http"].(map[string]any)
		if !ok || httpEntry[valURL] != "https://example.test/mcp" {
			t.Fatalf("http entry = %#v", mcp["http"])
		}
	})
}
func requireInvalidParamsData(t *testing.T, err error) map[string]any {
	t.Helper()
	if err == nil {
		t.Fatal("expected invalid-params error, got nil")
	}
	var reqErr *acp.RequestError
	if !errors.As(err, &reqErr) {
		t.Fatalf("error type = %T, want *acp.RequestError", err)
	}
	if reqErr.Code != -32602 {
		t.Fatalf("code = %d, want -32602", reqErr.Code)
	}
	data, ok := reqErr.Data.(map[string]any)
	if !ok {
		t.Fatalf("error data = %#v, want map", reqErr.Data)
	}

	return data
}
func requireUnsupportedField(t *testing.T, err error, field string, name string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected unsupported-field error, got nil", name)
	}
	var reqErr *acp.RequestError
	if !errors.As(err, &reqErr) {
		t.Fatalf("%s: error type = %T", name, err)
	}
	data, ok := reqErr.Data.(map[string]any)
	if !ok {
		t.Fatalf("%s: error data = %#v", name, reqErr.Data)
	}
	if data["error"] != "unsupported" || data["field"] != field {
		t.Fatalf("%s: error data = %#v want field %q", name, data, field)
	}
}

func TestValidateSharedHermesHomeOptionsRequiresACleanAbsolutePath(t *testing.T) {
	if err := validateSharedHermesHomeOptions(Options{SharedHermesHome: "relative"}); err == nil {
		t.Fatal("relative shared home accepted")
	}
	dirty := t.TempDir() + string(filepath.Separator) + "directory" + string(filepath.Separator) + ".."
	if err := validateSharedHermesHomeOptions(Options{SharedHermesHome: dirty}); err == nil {
		t.Fatal("unclean shared home accepted")
	}
}
