package hermesacp

import (
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-core/wire"
)

func TestSessionRequestBuilders(t *testing.T) {
	t.Parallel()

	request := NewSessionRequest("/w", WithSessionAdditionalDirectories("/a"), WithSessionMeta(map[string]any{"host": 1}), WithSessionRawEvents(true), WithSessionHermesOptions(NewHermesOptions(WithHermesModel("p/m"))))
	require.Equal(t, "/w", request.Cwd)
	require.Equal(t, []acp.McpServer{}, request.McpServers)
	require.Equal(t, []string{"/a"}, request.AdditionalDirectories)
	require.Equal(t, 1, request.Meta["host"])
	require.Equal(t, map[string]any{"options": map[string]any{"model": "p/m"}, "rawEvent": map[string]any{"enabled": true}}, request.Meta["hermes"])

	load := LoadSessionRequest(fieldID, "/w")
	require.Equal(t, acp.SessionId(fieldID), load.SessionId)
	require.Equal(t, []acp.McpServer{}, load.McpServers)

	resume := ResumeSessionRequest(fieldID, "/w", WithSessionOutputSchema(map[string]any{fieldType: "object"}))
	require.Equal(t, []acp.McpServer{}, resume.McpServers)
	require.NotNil(t, resume.Meta["hermes"])

	require.Equal(t, acp.SessionId(fieldID), DeleteSessionRequest(fieldID).SessionId)
	require.Equal(t, acp.SessionId(fieldID), CancelRequest(fieldID).SessionId)
	require.Len(t, TextPromptRequest(fieldID, "hi").Prompt, 1)
	require.NotNil(t, PromptRequest(fieldID).Prompt)
	require.Equal(t, configModel, SetModelRequest(fieldID, "p/m").ValueId.ConfigId)

	list := ListSessionsRequest(WithListSessionsCwd("/w"), WithListSessionsCursor("c"), WithListSessionsMeta(map[string]any{"k": "v"}))
	require.Equal(t, "/w", *list.Cwd)
	require.Equal(t, "c", *list.Cursor)
	require.Equal(t, "v", list.Meta["k"])
}

func TestBuildersRejectReservedMeta(t *testing.T) {
	t.Parallel()

	for _, literal := range wire.ReservedLiterals {
		require.Panics(t, func() { WithSessionMeta(map[string]any{literal: 1}) })
		require.Panics(t, func() { WithListSessionsMeta(map[string]any{literal: 1}) })
	}
}
