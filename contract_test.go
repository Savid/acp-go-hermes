package hermesacp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
)

// Contract pins: capability surface, stable-route rejection, and the uniform
// unknown-session error shape.

func TestInitializeCapabilitiesHardCutover(t *testing.T) {
	agent := NewAgent()
	resp, err := agent.Initialize(context.Background(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if resp.AgentInfo == nil || resp.AgentInfo.Name != "acp-go-hermes" {
		t.Fatalf("AgentInfo = %#v", resp.AgentInfo)
	}
	if resp.AgentCapabilities.SessionCapabilities.Fork != nil {
		t.Fatalf("stable fork capability advertised: %#v", resp.AgentCapabilities.SessionCapabilities.Fork)
	}
	if resp.AgentCapabilities.McpCapabilities.Acp {
		t.Fatal("ACP MCP capability advertised")
	}
	if resp.AgentCapabilities.McpCapabilities.Sse {
		t.Fatal("SSE MCP capability advertised")
	}
	if !resp.AgentCapabilities.PromptCapabilities.Image {
		t.Fatal("image prompt capability missing")
	}
	if !resp.AgentCapabilities.PromptCapabilities.EmbeddedContext {
		t.Fatal("embedded context capability missing")
	}
	encodedMeta, err := json.Marshal(resp.AgentCapabilities.Meta)
	if err != nil {
		t.Fatalf("marshal capability metadata: %v", err)
	}
	if strings.Contains(string(encodedMeta), `"image`) {
		t.Fatalf("image metadata advertised outside the standard prompt capability: %s", encodedMeta)
	}
	meta, _ := resp.AgentCapabilities.Meta[hermesMetaKey].(map[string]any)
	if _, ok := meta["structuredOutput"]; ok {
		t.Fatal("Hermes structured output advertised")
	}
	if store, _ := meta["sessionStore"].(map[string]any); store["format"] != SessionStoreFormat {
		t.Fatalf("sessionStore meta = %#v", store)
	}
}

func TestStableForkRouteMethodNotFound(t *testing.T) {
	agent := NewAgent()
	conn := &localAgentConnection{agent: agent}
	conn.initialized.Store(true)
	_, reqErr := conn.handle(context.Background(), acp.AgentMethodSessionFork, json.RawMessage(`{}`))
	if reqErr == nil {
		t.Fatal("session/fork unexpectedly succeeded")
	}
	if reqErr.Code != -32601 {
		t.Fatalf("code = %d, want -32601", reqErr.Code)
	}
}

func TestUnknownSessionErrorShape(t *testing.T) {
	ctx := context.Background()
	cwd := t.TempDir()

	t.Run("load not in store", func(t *testing.T) {
		_, err := NewAgent().LoadSession(ctx, LoadSessionRequest("missing", cwd))
		requireUnknownSession(t, err)
	})
	t.Run("resume not in store", func(t *testing.T) {
		_, err := NewAgent().ResumeSession(ctx, ResumeSessionRequest("missing", cwd))
		requireUnknownSession(t, err)
	})
	t.Run("load tombstoned", func(t *testing.T) {
		agent := NewAgent()
		agent.deleted["gone"] = struct{}{}
		_, err := agent.LoadSession(ctx, LoadSessionRequest("gone", cwd))
		requireUnknownSession(t, err)
	})
	t.Run("resume tombstoned", func(t *testing.T) {
		agent := NewAgent()
		agent.deleted["gone"] = struct{}{}
		_, err := agent.ResumeSession(ctx, ResumeSessionRequest("gone", cwd))
		requireUnknownSession(t, err)
	})
	t.Run("close unknown", func(t *testing.T) {
		_, err := NewAgent().CloseSession(ctx, acp.CloseSessionRequest{SessionId: "missing"})
		requireUnknownSession(t, err)
	})
}
