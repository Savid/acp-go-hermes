package hermesacp

import (
	"encoding/json"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-hermes/internal/lifecycle"
	"github.com/stretchr/testify/require"
)

func lifecycleOffer(version any) map[string]any {
	return map[string]any{lifecycle.MetaKey: map[string]any{"version": version}}
}

// requireLifecycleKeyRefusal asserts the surface refused with invalid params
// naming the reserved literal itself, rather than with whatever the surface
// would have answered had it handled the request.
func requireLifecycleKeyRefusal(t *testing.T, err error) {
	t.Helper()

	var reqErr *acp.RequestError

	require.ErrorAs(t, err, &reqErr)
	require.Equal(t, acp.NewInvalidParams(map[string]any{
		jsonFieldError: valUnsupported,
		keyField:       lifecycle.MetaPath,
	}), reqErr)
}

// TestCancelReportsTheRouteVerdictWhenBothKeysFailClosed pins refusal
// precedence on the one inbound surface carrying both the route envelope and the
// lifecycle key. The authenticator precedes the placement rule: a cancel whose
// route is invalid and which also names the reserved literal reports the route
// verdict, one verdict rather than an implementation-defined choice of two, and
// nothing native runs on either refusal.
func TestCancelReportsTheRouteVerdictWhenBothKeysFailClosed(t *testing.T) {
	client := newFakeHermesClient()
	session := testSession(newTestAgent(), client)
	session.beginTurn(t.Context(), "turn")
	session.mu.Lock()
	session.turnInFlight = true
	session.mu.Unlock()

	both := map[string]any{
		routeMetaKey:      map[string]any{routeFieldVer: 2, routeFieldTurn: "turn"},
		lifecycle.MetaKey: map[string]any{},
	}

	var reqErr *acp.RequestError

	require.ErrorAs(t, session.cancelRouted(both), &reqErr)
	require.Equal(t, acp.NewInvalidParams(map[string]any{
		jsonFieldError: "unsupported route metadata version",
		keyField:       routeMetaKey,
	}), reqErr, "route validation runs before the reserved-key refusal")

	// The same cancel with a route that authenticates the turn reports the
	// lifecycle verdict instead, which is what makes the ordering above a choice
	// this surface actually makes rather than an accident of the route being the
	// only thing wrong.
	routed := turnRouteMeta("turn")
	routed[lifecycle.MetaKey] = map[string]any{}
	requireLifecycleKeyRefusal(t, session.cancelRouted(routed))

	require.Zero(t, client.abortCount(), "a refused cancel never reaches the native interrupt")
}

func TestLifecycleNegotiationAndReservedMetadata(t *testing.T) {
	weak := newTestAgent()
	response, err := weak.Initialize(t.Context(), acp.InitializeRequest{})
	require.NoError(t, err)
	require.Nil(t, response.Meta)
	require.False(t, weak.negotiatedLifecycle().Present())

	response, err = weak.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(2)})
	require.Error(t, err)
	require.False(t, weak.negotiatedLifecycle().Present())

	response, err = weak.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(1)})
	require.NoError(t, err)
	advertisement, ok := response.Meta[lifecycle.MetaKey].(map[string]any)
	require.True(t, ok)
	require.Equal(t, 1, advertisement["version"])
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

	// Either union variant of the config surface can carry `_meta`, and the
	// refusal precedes the surface's own variant handling: the boolean variant
	// is one this agent does not implement, so an unrefused key would answer
	// "unsupported variant" instead of naming the reserved literal.
	_, err = weak.SetSessionConfigOption(t.Context(), acp.SetSessionConfigOptionRequest{
		Boolean: &acp.SetSessionConfigOptionBoolean{Meta: reserved, Type: "boolean"},
	})
	requireLifecycleKeyRefusal(t, err)
	_, err = weak.SetSessionConfigOption(t.Context(), acp.SetSessionConfigOptionRequest{
		ValueId: &acp.SetSessionConfigOptionValueId{Meta: reserved, ConfigId: configModel, Value: "provider/model"},
	})
	requireLifecycleKeyRefusal(t, err)

	_, err = weak.HandleExtensionMethod(t.Context(), "unknown", json.RawMessage(`{"_meta":{"acp-go.dev/lifecycle":{}}}`))
	require.Error(t, err)
	_, err = weak.HandleExtensionMethod(t.Context(), "unknown", json.RawMessage(`{`))
	require.Error(t, err)

	_, err = weak.NewSession(t.Context(), acp.NewSessionRequest{Meta: reserved})
	require.Error(t, err)
	_, err = weak.loadOrResumeSession(t.Context(), "session", "", nil, nil, reserved, false)
	require.Error(t, err)
	_, err = weak.ListSessions(t.Context(), acp.ListSessionsRequest{Meta: reserved})
	require.Error(t, err)
	_, err = weak.CloseSession(t.Context(), acp.CloseSessionRequest{Meta: reserved})
	require.Error(t, err)
	_, err = weak.UnstableDeleteSession(t.Context(), acp.UnstableDeleteSessionRequest{Meta: reserved})
	require.Error(t, err)
}
