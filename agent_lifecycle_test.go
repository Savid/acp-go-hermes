package hermesacp

import (
	"encoding/json"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-hermes/internal/lifecycle"
	"github.com/stretchr/testify/require"
)

func lifecycleOffer(versions ...any) map[string]any {
	return map[string]any{lifecycle.MetaKey: map[string]any{"versions": versions}}
}

func TestLifecycleNegotiationAndReservedMetadata(t *testing.T) {
	weak := newTestAgent()
	response, err := weak.Initialize(t.Context(), acp.InitializeRequest{})
	require.NoError(t, err)
	require.Nil(t, response.Meta)
	require.False(t, weak.negotiatedLifecycle().Present())

	response, err = weak.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(2)})
	require.NoError(t, err)
	require.Nil(t, response.Meta)
	require.False(t, weak.negotiatedLifecycle().Present())

	response, err = weak.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(1)})
	require.NoError(t, err)
	advertisement, ok := response.Meta[lifecycle.MetaKey].(map[string]any)
	require.True(t, ok)
	require.Equal(t, []int{1}, advertisement["versions"])
	require.Equal(t, true, advertisement["updatesOutsidePrompt"])
	require.Equal(t, false, advertisement["authoritativeQuiescence"])
	require.Equal(t, []string{}, advertisement["activityKinds"])

	authoritative := newTestAgent()
	authoritative.containmentMode = RuntimeContainmentAuthoritative
	response, err = authoritative.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(1)})
	require.NoError(t, err)
	advertisement, ok = response.Meta[lifecycle.MetaKey].(map[string]any)
	require.True(t, ok)
	require.Equal(t, true, advertisement["authoritativeQuiescence"])
	require.Equal(t, string(lifecycle.ProofClassProcessContainment), advertisement["quiescenceSource"])

	_, err = weak.Initialize(t.Context(), acp.InitializeRequest{
		Meta: map[string]any{lifecycle.MetaKey: "v1"},
	})
	require.Error(t, err)

	reserved := map[string]any{lifecycle.MetaKey: map[string]any{}}
	_, err = weak.Authenticate(t.Context(), acp.AuthenticateRequest{Meta: reserved})
	require.Error(t, err)
	_, err = weak.Logout(t.Context(), acp.LogoutRequest{Meta: reserved})
	require.Error(t, err)
	_, err = weak.SetSessionMode(t.Context(), acp.SetSessionModeRequest{Meta: reserved})
	require.Error(t, err)

	_, err = weak.HandleExtensionMethod(t.Context(), "unknown", json.RawMessage(`{"_meta":{"acp-go.dev/lifecycle":{}}}`))
	require.Error(t, err)
	_, err = weak.HandleExtensionMethod(t.Context(), "unknown", json.RawMessage(`{`))
	require.Error(t, err)

	_, err = weak.NewSession(t.Context(), acp.NewSessionRequest{Meta: reserved})
	require.Error(t, err)
	_, err = weak.loadOrResumeSession(t.Context(), "session", "", nil, nil, reserved)
	require.Error(t, err)
	_, err = weak.ListSessions(t.Context(), acp.ListSessionsRequest{Meta: reserved})
	require.Error(t, err)
	_, err = weak.CloseSession(t.Context(), acp.CloseSessionRequest{Meta: reserved})
	require.Error(t, err)
	_, err = weak.UnstableDeleteSession(t.Context(), acp.UnstableDeleteSessionRequest{Meta: reserved})
	require.Error(t, err)
}
