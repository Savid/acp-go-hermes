package hermesacp

import (
	"context"
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
		jsonFieldField: lifecycle.MetaPath,
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
		jsonFieldError: valUnsupported,
		jsonFieldField: routeMetaPath + "." + routeFieldVer,
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
	// A refused offer never binds, so the no-offer answer and the two decode
	// refusals below all belong to one connection. The accepted version-1
	// negotiation needs its own: the first answer binds the connection, so a
	// connection that answered "absent" may not later answer version 1.
	weak := newTestAgent()
	response, err := weak.Initialize(t.Context(), acp.InitializeRequest{})
	require.NoError(t, err)
	require.Nil(t, response.Meta)
	require.False(t, weak.negotiatedLifecycle().Present())

	response, err = weak.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(2)})
	require.Error(t, err)
	require.False(t, weak.negotiatedLifecycle().Present())

	// A connection that answered "absent" stays absent: a repeat of the same
	// no-offer negotiation asserts the same contract and is admitted.
	response, err = weak.Initialize(t.Context(), acp.InitializeRequest{})
	require.NoError(t, err)
	require.Nil(t, response.Meta)
	require.False(t, weak.negotiatedLifecycle().Present())

	enabled := newTestAgent()
	response, err = enabled.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(1)})
	require.NoError(t, err)
	advertisement, ok := response.Meta[lifecycle.MetaKey].(map[string]any)
	require.True(t, ok)
	require.Equal(t, 1, advertisement["version"])
	require.Equal(t, true, advertisement["updatesOutsidePrompt"])
	require.Equal(t, false, advertisement["authoritativeQuiescence"])
	require.Equal(t, []string{}, advertisement["activityKinds"])

	authoritative := NewAgent(WithHostAuthority(newTestHostAuthority()))
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

func TestApplyAdmittedActiveLifecycleRequest(t *testing.T) {
	session := testSession(newTestAgent(), newFakeHermesClient())
	meta := sessionMeta{}
	if rebind, err := applyAdmittedActiveLifecycleRequest(
		t.Context(), session, session.cwd, nil, nil, &meta,
	); err != nil || rebind {
		t.Fatalf("live reuse application = rebind:%v err:%v", rebind, err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := applyAdmittedActiveLifecycleRequest(
		canceled, session, session.cwd, nil, nil, &meta,
	); err == nil {
		t.Fatalf("cancelled reuse rejection = %v", err)
	}
}

// TestLifecycleAnswerBindsTheWholeConnection pins that the answer a connection
// gave is not rewritten by a later initialize. Enabling version 1 obligates the
// foreground stream for every session on the connection, so a second
// negotiation that would withdraw or alter the answer is refused on the
// lifecycle key while the sessions it was published for are still live.
func TestLifecycleAnswerBindsTheWholeConnection(t *testing.T) {
	for _, test := range []struct {
		name   string
		first  map[string]any
		second map[string]any
	}{
		{"withdrawn", lifecycleOffer(1), nil},
		{"introduced", nil, lifecycleOffer(1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			agent := newTestAgent()

			first, err := agent.Initialize(t.Context(), acp.InitializeRequest{Meta: test.first})
			require.NoError(t, err)

			bound := agent.negotiatedLifecycle()

			_, err = agent.Initialize(t.Context(), acp.InitializeRequest{Meta: test.second})
			requireLifecycleKeyRefusal(t, err)

			// The refused second negotiation changed nothing: the connection is
			// still bound by exactly the answer it gave.
			require.Equal(t, first.Meta[lifecycle.MetaKey] != nil, agent.negotiatedLifecycle().Present())
			require.Equal(t, bound, agent.negotiatedLifecycle())
		})
	}
}

// TestLifecycleRepeatedIdenticalNegotiationIsAdmitted pins the other half: a
// second initialize asserting the same answer states the same contract, so it
// changes nothing and is not an error.
func TestLifecycleRepeatedIdenticalNegotiationIsAdmitted(t *testing.T) {
	agent := newTestAgent()

	first, err := agent.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(1)})
	require.NoError(t, err)

	second, err := agent.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(1)})
	require.NoError(t, err)
	require.Equal(t, first.Meta[lifecycle.MetaKey], second.Meta[lifecycle.MetaKey])
	require.True(t, agent.negotiatedLifecycle().Present())
}

// TestLifecycleAnswerBindsAcrossAProvenFactChange pins that the comparison is
// against the answer the connection actually gave, not against the offer the
// host repeated. An agent whose proven facts differ answers differently, and
// that difference is what a re-negotiation may not introduce.
func TestLifecycleAnswerBindsAcrossAProvenFactChange(t *testing.T) {
	authoritative := NewAgent(WithHostAuthority(newTestHostAuthority()))

	_, err := authoritative.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(1)})
	require.NoError(t, err)
	require.True(t, authoritative.negotiatedLifecycle().AuthoritativeQuiescence)

	// The same offer against the same agent proves the same facts, so it is the
	// identical answer and is admitted.
	_, err = authoritative.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(1)})
	require.NoError(t, err)
	require.Equal(t, lifecycle.ProofClassProcessContainment, authoritative.negotiatedLifecycle().QuiescenceSource)
}

// TestLifecycleRefusedRenegotiationKeepsLiveSessionStreams pins the reason the
// rule exists. A session opened under a present answer holds an obligated
// foreground stream; a withdrawing re-negotiation is refused, so that stream
// keeps its identity and the host reducing it never sees the obligation
// cancelled underneath a live session.
func TestLifecycleRefusedRenegotiationKeepsLiveSessionStreams(t *testing.T) {
	agent := newTestAgent()

	_, err := agent.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(1)})
	require.NoError(t, err)

	session := testSession(agent, newFakeHermesClient())
	agent.mu.Lock()
	agent.sessions[session.id] = session
	agent.mu.Unlock()

	streamBefore := session.lifecycleStream()

	_, err = agent.Initialize(t.Context(), acp.InitializeRequest{})
	requireLifecycleKeyRefusal(t, err)

	require.True(t, agent.negotiatedLifecycle().Present())
	require.Same(t, streamBefore, session.lifecycleStream())

	agent.mu.Lock()
	_, stillOpen := agent.sessions[session.id]
	agent.mu.Unlock()
	require.True(t, stillOpen)
}
