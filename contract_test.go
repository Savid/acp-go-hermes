package hermesacp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
)

// Contract pins: capability surface, stable-route rejection, and the uniform
// unknown-session error shape.

func TestInitializeCapabilitiesCurrentShape(t *testing.T) {
	agent := newTestAgent()
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
	meta, _ := resp.AgentCapabilities.Meta[hermesMetaKey].(map[string]any)
	encodedVendorMeta, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal vendor capability metadata: %v", err)
	}
	if strings.Contains(string(encodedVendorMeta), `"image`) {
		t.Fatalf("image metadata advertised under the vendor namespace: %s", encodedVendorMeta)
	}
	if _, ok := meta["structuredOutput"]; ok {
		t.Fatal("Hermes structured output advertised")
	}
	elicitationMeta, ok := meta[valElicitation].(map[string]any)
	if !ok || elicitationMeta["unstable"] != true || elicitationMeta["tracks"] != "ACP v1 elicitation" {
		t.Fatalf("unexpected elicitation capability: %#v", meta[valElicitation])
	}
	if store, _ := meta["sessionStore"].(map[string]any); store["format"] != SessionStoreFormat {
		t.Fatalf("sessionStore meta = %#v", store)
	}
}

func TestInitializeAdvertisesMediaEnvelope(t *testing.T) {
	resp, err := newTestAgent().Initialize(context.Background(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	envelope, ok := resp.AgentCapabilities.Meta[mediaEnvelopeMetaKey].(map[string]any)
	if !ok {
		t.Fatalf("media envelope missing: %#v", resp.AgentCapabilities.Meta)
	}
	if !reflect.DeepEqual(envelope, map[string]any{
		keyMaxBytes:                       defaultImageLimitBytes,
		mediaEnvelopeFieldMaxPromptBytes:  defaultImageLimitBytes,
		mediaEnvelopeFieldMaxDimension:    0,
		mediaEnvelopeFieldImageFormats:    []string{mimePNG, mimeJPEG, mimeGIF, mimeWebP},
		mediaEnvelopeFieldDocumentFormats: []string{},
	}) {
		t.Fatalf("media envelope = %#v", envelope)
	}

	encoded, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal media envelope: %v", err)
	}
	if want := `{"documentFormats":[],"imageFormats":["image/png","image/jpeg","image/gif","image/webp"],"maxBytes":6291456,"maxDimension":0,"maxPromptBytes":6291456}`; string(encoded) != want {
		t.Fatalf("encoded media envelope = %s, want %s", encoded, want)
	}
}

// TestMediaEnvelopeMatchesEnforcedGates pins the advertisement to the gate: a
// host that pre-checks against the advertised values sees the same numbers the
// rejection reports.
func TestMediaEnvelopeMatchesEnforcedGates(t *testing.T) {
	png := fixtureBytes(t, "valid.png")
	limits := ImageLimits{
		MaxInputBytesPerImage:  int64(len(png)) - 1,
		MaxInputBytesPerPrompt: int64(len(png)) * 3,
	}

	resp, err := newTestAgent(WithImageLimits(limits)).Initialize(context.Background(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	envelope, _ := resp.AgentCapabilities.Meta[mediaEnvelopeMetaKey].(map[string]any)
	if envelope[keyMaxBytes] != limits.MaxInputBytesPerImage || envelope[mediaEnvelopeFieldMaxPromptBytes] != limits.MaxInputBytesPerPrompt {
		t.Fatalf("advertised bounds = %#v, want the configured limits %#v", envelope, limits)
	}

	block := acp.ContentBlock{Image: &acp.ContentBlockImage{
		Data: base64.StdEncoding.EncodeToString(png), MimeType: mimePNG,
	}}

	_, imageErr := promptToHermesParts(t.Context(), []acp.ContentBlock{block}, limits, "")
	requireImageInputError(t, imageErr, map[string]any{
		keyField:       acpFieldPromptImage,
		jsonFieldError: imageErrTooLarge,
		keyIndex:       0,
		keySizeBytes:   int64(len(png)),
		keyMaxBytes:    envelope[keyMaxBytes],
	})

	_, promptErr := promptToHermesParts(t.Context(), []acp.ContentBlock{block, block, block, block}, ImageLimits{
		MaxInputBytesPerPrompt: limits.MaxInputBytesPerPrompt,
	}, "")
	requireImageInputError(t, promptErr, map[string]any{
		keyField:       acpFieldPromptImage,
		jsonFieldError: imageErrTooLarge,
		keyIndex:       3,
		keySizeBytes:   int64(len(png)) * 4,
		keyMaxBytes:    envelope[mediaEnvelopeFieldMaxPromptBytes],
	})
}

// TestInitializeAdvertisesHandoffOnlyWhenRootConfigured pins the conditional
// advertisement both ways: its absence is how a host learns its handoff option
// never reached this adapter.
func TestInitializeAdvertisesHandoffOnlyWhenRootConfigured(t *testing.T) {
	ctx := context.Background()
	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}

	withoutRoot, err := newTestAgent().Initialize(ctx, request)
	if err != nil {
		t.Fatalf("Initialize without handoff root: %v", err)
	}
	if _, ok := withoutRoot.AgentCapabilities.Meta[handoffMetaKey]; ok {
		t.Fatalf("handoff advertised without a configured root: %#v", withoutRoot.AgentCapabilities.Meta)
	}

	withRoot, err := newTestAgent(WithInputHandoffRoot(t.TempDir())).Initialize(ctx, request)
	if err != nil {
		t.Fatalf("Initialize with handoff root: %v", err)
	}
	if !reflect.DeepEqual(withRoot.AgentCapabilities.Meta[handoffMetaKey], map[string]any{keyVersion: handoffVersion}) {
		t.Fatalf("handoff advertisement = %#v", withRoot.AgentCapabilities.Meta[handoffMetaKey])
	}
	if _, ok := withRoot.AgentCapabilities.Meta[mediaEnvelopeMetaKey]; !ok {
		t.Fatal("media envelope missing when a handoff root is configured")
	}
}

func TestInputHandoffRootMustBeAbsolute(t *testing.T) {
	agent := newTestAgent(WithInputHandoffRoot("relative/handoff"))

	_, err := agent.Initialize(context.Background(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})

	var requestErr *acp.RequestError
	if !errors.As(err, &requestErr) || requestErr.Code != -32603 {
		t.Fatalf("Initialize error = %#v, want internal error", err)
	}
	data, _ := requestErr.Data.(map[string]any)
	message, _ := data[jsonFieldError].(string)

	// The wire carries the closed token; the path the operator misconfigured
	// stays out of it and on the agent's own option error instead.
	if message != valHermesInvalidOptions {
		t.Fatalf("error data = %#v", requestErr.Data)
	}

	if agent.optionsErr == nil || !strings.Contains(agent.optionsErr.Error(), "input handoff root must be an absolute path") {
		t.Fatalf("option validation prose lost: %v", agent.optionsErr)
	}
}

func TestStableForkRouteMethodNotFound(t *testing.T) {
	agent := newTestAgent()
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
		_, err := newTestAgent().LoadSession(ctx, LoadSessionRequest("missing", cwd))
		requireUnknownSession(t, err)
	})
	t.Run("resume not in store", func(t *testing.T) {
		_, err := newTestAgent().ResumeSession(ctx, ResumeSessionRequest("missing", cwd))
		requireUnknownSession(t, err)
	})
	t.Run("load tombstoned", func(t *testing.T) {
		agent := newTestAgent()
		agent.deleted["gone"] = struct{}{}
		_, err := agent.LoadSession(ctx, LoadSessionRequest("gone", cwd))
		requireUnknownSession(t, err)
	})
	t.Run("resume tombstoned", func(t *testing.T) {
		agent := newTestAgent()
		agent.deleted["gone"] = struct{}{}
		_, err := agent.ResumeSession(ctx, ResumeSessionRequest("gone", cwd))
		requireUnknownSession(t, err)
	})
	t.Run("close unknown", func(t *testing.T) {
		_, err := newTestAgent().CloseSession(ctx, acp.CloseSessionRequest{SessionId: "missing"})
		requireUnknownSession(t, err)
	})
}
