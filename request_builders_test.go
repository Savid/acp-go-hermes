package hermesacp

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/wire"
)

func TestSessionRequestBuilders(t *testing.T) {
	t.Parallel()

	request := wire.NewSessionRequest("/w", wire.WithSessionAdditionalDirectories("/a"), wire.WithSessionMeta(map[string]any{"host": 1}), WithSessionRawEvents(true), WithSessionHermesOptions(NewHermesOptions(WithHermesModel("p/m"))))
	require.Equal(t, "/w", request.Cwd)
	require.Equal(t, []acp.McpServer{}, request.McpServers)
	require.Equal(t, []string{"/a"}, request.AdditionalDirectories)
	require.Equal(t, 1, request.Meta["host"])
	require.Equal(t, map[string]any{"options": map[string]any{"model": "p/m"}, "rawEvent": map[string]any{"enabled": true}}, request.Meta["hermes"])

	load := wire.LoadSessionRequest(fieldID, "/w")
	require.Equal(t, acp.SessionId(fieldID), load.SessionId)
	require.Equal(t, []acp.McpServer{}, load.McpServers)

	resume := wire.ResumeSessionRequest(fieldID, "/w", WithSessionHermesOptions(NewHermesOptions(WithHermesEffort("high"))))
	require.Equal(t, []acp.McpServer{}, resume.McpServers)
	require.Equal(t, map[string]any{"options": map[string]any{"effort": "high"}}, resume.Meta["hermes"])

	require.Equal(t, acp.SessionId(fieldID), wire.DeleteSessionRequest(fieldID).SessionId)
	require.Equal(t, acp.SessionId(fieldID), wire.CancelRequest(fieldID).SessionId)
	require.Len(t, wire.TextPromptRequest(fieldID, "hi").Prompt, 1)
	require.NotNil(t, wire.PromptRequest(fieldID).Prompt)
	require.Equal(t, configModel, SetModelRequest(fieldID, "p/m").ValueId.ConfigId)

	list := wire.ListSessionsRequest(wire.WithListSessionsCwd("/w"), wire.WithListSessionsCursor("c"), wire.WithListSessionsMeta(map[string]any{"k": "v"}))
	require.Equal(t, "/w", *list.Cwd)
	require.Equal(t, "c", *list.Cursor)
	require.Equal(t, "v", list.Meta["k"])
}

func TestBuildersRejectReservedMeta(t *testing.T) {
	t.Parallel()

	for _, literal := range wire.ReservedLiterals {
		require.Panics(t, func() { wire.WithSessionMeta(map[string]any{literal: 1}) })
		require.Panics(t, func() { wire.WithListSessionsMeta(map[string]any{literal: 1}) })
	}
}

func TestMetadataClonesTypedEnvironment(t *testing.T) {
	t.Parallel()

	env := map[string]string{"SESSION_KEY": "original"}
	meta := map[string]any{vendor: map[string]any{"options": map[string]any{"env": env}}}
	option := wire.WithSessionMeta(meta)
	first := wire.NewSessionRequest(t.TempDir(), option)
	env["SESSION_KEY"] = "caller changed"
	second := wire.NewSessionRequest(t.TempDir(), option)
	for _, request := range []map[string]any{first.Meta, second.Meta} {
		parsed, err := parseSessionMeta(request)
		require.Nil(t, err)
		require.Equal(t, "original", parsed.options.Env["SESSION_KEY"], "a caller mutation reached the session environment the builder captured")
	}
}
