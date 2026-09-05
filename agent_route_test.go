package hermesacp

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-hermes/internal/lifecycle"
	"github.com/stretchr/testify/require"
)

func TestRouteEnvelopeCurrentShape(t *testing.T) {
	ctx := withTurnRoute(context.Background(), "turn-old")
	require.Equal(t, turnRouteMeta("turn-old"), turnRouteMetaFromContext(ctx))
	require.Nil(t, turnRouteMetaFromContext(context.Background()))

	boundaryNonce := strings.Repeat("n", routeTurnNonceMaxBytes)
	route, err := parseInboundTurnRoute(turnRouteMeta(boundaryNonce))
	require.NoError(t, err)
	require.Equal(t, boundaryNonce, route.turnNonce)

	for _, meta := range []map[string]any{
		nil,
		{routeMetaKey: "bad"},
		{routeMetaKey: map[string]any{routeFieldVer: 2, routeFieldTurn: "turn"}},
		{routeMetaKey: map[string]any{routeFieldVer: float64(1), routeFieldTurn: "turn", "extra": true}},
		{routeMetaKey: map[string]any{routeFieldVer: 1, routeFieldTurn: ""}},
		{routeMetaKey: map[string]any{routeFieldVer: "1", routeFieldTurn: "turn"}},
		{routeMetaKey: map[string]any{routeFieldVer: 1.5, routeFieldTurn: "turn"}},
		{routeMetaKey: map[string]any{routeFieldVer: 1, routeFieldTurn: strings.Repeat("n", routeTurnNonceMaxBytes+1)}},
	} {
		_, routeErr := parseInboundTurnRoute(meta)
		require.Error(t, routeErr)
	}

	decoded, err := parseInboundTurnRoute(map[string]any{routeMetaKey: map[string]any{
		routeFieldVer: float64(1), routeFieldTurn: "decoded-turn",
	}})
	require.NoError(t, err)
	require.Equal(t, "decoded-turn", decoded.turnNonce)

	meta, err := stampRouteMeta(map[string]any{"hermes": map[string]any{"native": true}}, elicitationScope{
		SessionID: "session-1", TurnNonce: "turn-1", ToolCallID: "tool-1",
	})
	require.NoError(t, err)
	require.Contains(t, meta, "hermes")
	require.Equal(t, map[string]any{
		routeFieldVer: 1, routeFieldID: acp.SessionId("session-1"), routeFieldTurn: "turn-1", "toolCallId": acp.ToolCallId("tool-1"),
	}, meta[routeMetaKey])

	_, err = stampRouteMeta(map[string]any{routeMetaKey: map[string]any{}}, elicitationScope{SessionID: "s", TurnNonce: "t"})
	require.ErrorContains(t, err, "collision")
	requestID := "request-1"
	_, err = stampRouteMeta(nil, elicitationScope{SessionID: "s", TurnNonce: "t", ToolCallID: "tool", RequestID: &requestID})
	require.ErrorContains(t, err, "exactly one")
	_, err = stampRouteMeta(nil, elicitationScope{})
	require.Error(t, err)
	_, err = stampRouteMeta(nil, elicitationScope{SessionID: "s", TurnNonce: strings.Repeat("n", routeTurnNonceMaxBytes+1)})
	require.ErrorContains(t, err, "maximum size")
	require.Nil(t, turnRouteMeta(strings.Repeat("n", routeTurnNonceMaxBytes+1)))
	require.Nil(t, turnRouteMetaFromContext(withTurnRoute(context.Background(), strings.Repeat("n", routeTurnNonceMaxBytes+1))))

	previous := routeRandRead
	routeRandRead = func([]byte) (int, error) { return 0, errors.New("entropy") }
	t.Cleanup(func() { routeRandRead = previous })
	_, err = stampRouteMeta(nil, elicitationScope{SessionID: "s", TurnNonce: "t"})
	require.ErrorContains(t, err, "entropy")
}

func TestInitializeAdvertisesRouteV1(t *testing.T) {
	resp, err := newTestAgent().Initialize(context.Background(), acp.InitializeRequest{})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"version": 1}, resp.AgentCapabilities.Meta[routeMetaKey])
}

func TestRouteCapabilityScalar(t *testing.T) {
	response, err := newTestAgent().Initialize(t.Context(), acp.InitializeRequest{})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"version": 1}, response.AgentCapabilities.Meta["acp-go.dev/route"])
}

func TestPromptAndActiveCancelRequireCurrentRoute(t *testing.T) {
	session := &session{id: "session-1", turnInFlight: true, turnNonce: "active-turn"}
	agent := newTestAgent()
	agent.sessions[session.id] = session

	_, err := agent.Prompt(t.Context(), acp.PromptRequest{SessionId: session.id})
	require.Error(t, err)
	_, err = session.Prompt(t.Context(), acp.PromptRequest{SessionId: session.id})
	require.Error(t, err)
	require.Error(t, agent.Cancel(t.Context(), acp.CancelNotification{SessionId: session.id}))
	require.Error(t, agent.Cancel(t.Context(), CancelRequest(session.id, "stale-turn")))
}

// TestReservedEnvelopeRefusalTable pins the complete refusal vocabulary for the
// two reserved literals a prompt carries. The two verdicts are distinct facts a
// host acts on differently and are never collapsed: `missing` names a required
// key the caller left out, `unsupported` names a value that is present and
// refused, on the bare key path when the value is not an object and on the
// member path when one member is at fault.
func TestReservedEnvelopeRefusalTable(t *testing.T) {
	overBound := strings.Repeat("n", routeTurnNonceMaxBytes+1)

	correlation := func(value any) map[string]any {
		return map[string]any{lifecycle.MetaKey: value}
	}

	validSubmission := map[string]any{
		"submissionId": "submission",
		"clientNonce":  "nonce",
	}

	for _, test := range []struct {
		name  string
		meta  map[string]any
		error string
		field string
	}{
		{"route absent", map[string]any{}, valMissing, routeMetaPath},
		{"route non-object", map[string]any{routeMetaKey: "not-an-object"}, valUnsupported, routeMetaPath},
		{
			"route wrong version",
			map[string]any{routeMetaKey: map[string]any{routeFieldVer: 2, routeFieldTurn: "turn"}},
			valUnsupported, routeMetaPath + "." + routeFieldVer,
		},
		{
			"route empty nonce",
			map[string]any{routeMetaKey: map[string]any{routeFieldVer: 1, routeFieldTurn: ""}},
			valUnsupported, routeMetaPath + "." + routeFieldTurn,
		},
		{
			"route over-bound nonce",
			map[string]any{routeMetaKey: map[string]any{routeFieldVer: 1, routeFieldTurn: overBound}},
			valUnsupported, routeMetaPath + "." + routeFieldTurn,
		},
		{
			"route unknown member",
			map[string]any{routeMetaKey: map[string]any{routeFieldVer: 1, routeFieldTurn: "turn", "nope": true}},
			valUnsupported, routeMetaPath + ".nope",
		},
		{"lifecycle absent", turnRouteMeta("turn"), valMissing, lifecycle.MetaPath},
		{
			"lifecycle non-object",
			routedMetaWith(t, "turn", correlation("not-an-object")),
			valUnsupported, lifecycle.MetaPath,
		},
		{
			"lifecycle wrong version",
			routedMetaWith(t, "turn", correlation(map[string]any{"version": 2, "submission": validSubmission})),
			valUnsupported, lifecycle.MetaPath + ".version",
		},
		{
			"lifecycle empty identifier",
			routedMetaWith(t, "turn", correlation(map[string]any{
				"version":    1,
				"submission": map[string]any{"submissionId": "", "clientNonce": "nonce"},
			})),
			valUnsupported, lifecycle.MetaPath + ".submission.submissionId",
		},
		{
			"lifecycle unknown member",
			routedMetaWith(t, "turn", correlation(map[string]any{
				"version": 1, "submission": validSubmission, "nope": true,
			})),
			valUnsupported, lifecycle.MetaPath + ".nope",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			session, client := negotiatedPromptSession(t)

			_, err := session.Prompt(t.Context(), acp.PromptRequest{
				Meta:      test.meta,
				SessionId: session.id,
				Prompt:    []acp.ContentBlock{acp.TextBlock("hello")},
			})

			var reqErr *acp.RequestError

			require.ErrorAs(t, err, &reqErr)
			require.Equal(t, acp.NewInvalidParams(map[string]any{
				jsonFieldError: test.error,
				keyField:       test.field,
			}), reqErr)
			require.Zero(t, client.promptDispatchCount(), "a refused envelope never reaches the harness")
		})
	}

	t.Run("route refusal wins when both keys fail", func(t *testing.T) {
		session, client := negotiatedPromptSession(t)

		_, err := session.Prompt(t.Context(), acp.PromptRequest{
			Meta: map[string]any{
				routeMetaKey:      map[string]any{routeFieldVer: 2, routeFieldTurn: "turn"},
				lifecycle.MetaKey: "not-an-object",
			},
			SessionId: session.id,
			Prompt:    []acp.ContentBlock{acp.TextBlock("hello")},
		})

		var reqErr *acp.RequestError

		require.ErrorAs(t, err, &reqErr)
		require.Equal(t, acp.NewInvalidParams(map[string]any{
			jsonFieldError: valUnsupported,
			keyField:       routeMetaPath + "." + routeFieldVer,
		}), reqErr, "route validation runs before the lifecycle correlation is read")
		require.Zero(t, client.promptDispatchCount())
	})
}

// routedMetaWith builds a prompt `_meta` carrying a valid route envelope plus
// the supplied additional keys, so a lifecycle case is refused for its own
// reason rather than for a missing route.
func routedMetaWith(t *testing.T, turnNonce string, extra map[string]any) map[string]any {
	t.Helper()

	meta := turnRouteMeta(turnNonce)
	for key, value := range extra {
		meta[key] = value
	}

	return meta
}

// negotiatedPromptSession builds a session on a connection whose one lifecycle
// answer is version 1, which is what makes the prompt correlation required.
func negotiatedPromptSession(t *testing.T) (*session, *fakeHermesClient) {
	t.Helper()

	agent := newTestAgent()
	require.NoError(t, agent.retainNegotiatedLifecycle(lifecycle.Negotiated{
		Version:              lifecycle.Version,
		UpdatesOutsidePrompt: true,
		ActivityKinds:        []lifecycle.ActivityKind{},
	}))
	agent.setAgentClient(newRecordingAgentClient())
	client := newFakeHermesClient()

	return testSession(agent, client), client
}

// TestRouteCorrelationFailureIsAnInternalFailure pins the wire shape of an
// adapter-internal turn-correlation invariant failing closed. The reason names
// adapter state the caller cannot restate, so it stays on the Go error while the
// peer reads the closed unclassified token and its documented class.
func TestRouteCorrelationFailureIsAnInternalFailure(t *testing.T) {
	t.Parallel()

	err := routeInvalid("stale route turnNonce")
	require.EqualError(t, err, "stale route turnNonce")

	mapped := requestError(context.Background(), err)
	require.NotNil(t, mapped)
	require.Equal(t, -32603, mapped.Code)
	require.Equal(t, map[string]any{
		jsonFieldError: valHermesInternalFailure,
		keyClass:       classRouteCorrelation,
	}, mapped.Data)
}
