package hermesacp

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

const tinyPNG = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="

func TestImageNativeHandoffAndGates(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	data, err := base64.StdEncoding.DecodeString(tinyPNG)
	require.NoError(t, err)
	path := filepath.Join(root, "input.png")
	require.NoError(t, os.WriteFile(path, data, 0600))
	digest := sha256.Sum256(data)
	agent := NewAgent(testOptions(t, WithInputHandoffRoot(root))...)
	rec := newRecorder()
	agent.attach(rec, nil)
	t.Cleanup(func() { require.NoError(t, agent.Close()) })
	_, err = agent.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	session, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	uri := "file://" + path
	handoff := acp.ContentBlock{Image: &acp.ContentBlockImage{MimeType: "image/png", Uri: &uri, Meta: map[string]any{wire.HandoffKey: map[string]any{"version": 1, "digest": hex.EncodeToString(digest[:]), "sizeBytes": len(data)}}}}
	for _, block := range []acp.ContentBlock{acp.ImageBlock(tinyPNG, "image/png"), handoff} {
		before := len(rec.snapshot())
		_, err = agent.Prompt(t.Context(), PromptRequest(session.SessionId, acp.TextBlock("IMAGE"), block))
		require.NoError(t, err)
		require.Equal(t, tinyPNG, agentText(rec.snapshot()[before:]))
	}
	_, err = agent.Prompt(t.Context(), PromptRequest(session.SessionId, acp.ImageBlock("not base64!", "image/png")))
	require.Equal(t, "invalid_base64", requestErrorData(t, err)[stopReasonError])
	_, err = agent.Prompt(t.Context(), PromptRequest(session.SessionId, acp.ImageBlock(tinyPNG, "image/jpeg")))
	require.Error(t, err)
}

func TestNativeEnvironmentRemainsSessionScoped(t *testing.T) {
	t.Parallel()
	h := newHarness(t, WithEnv(map[string]string{fakeHermesEnv: "1", "ACP_MARKER": "agent"}))
	h.initialize()
	for _, marker := range []string{"first", "second"} {
		t.Run(marker, func(t *testing.T) {
			t.Parallel()
			first, second := t.TempDir(), t.TempDir()
			session, err := h.conn.NewSession(h.ctx(), NewSessionRequest(t.TempDir(), WithSessionHermesOptions(NewHermesOptions(WithHermesEnv(map[string]string{"ACP_MARKER": marker, "PATH": "/usr/bin:/bin"}), WithHermesExtraPathDirs(first, second)))))
			require.NoError(t, err)
			_, err = h.prompt(session.SessionId, "ENV", nil)
			require.NoError(t, err)
			var updates []acp.SessionNotification
			for _, update := range h.rec.snapshot() {
				if update.SessionId == session.SessionId {
					updates = append(updates, update)
				}
			}
			require.Equal(t, marker+"|"+first+string(os.PathListSeparator)+second+string(os.PathListSeparator)+"/usr/bin:/bin", agentText(updates))
		})
	}
}
