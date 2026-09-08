package hermesacp

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

func newRateLimitsFixtureAgent(t *testing.T) *Agent {
	t.Helper()
	agent := NewAgent(WithDefaultModel("openai/test"))
	for _, id := range []acp.SessionId{"session-1", "😀: session "} {
		agent.sessions[id] = &session{agent: agent, id: id, providerID: "openai", modelID: "test"}
	}
	t.Cleanup(func() {
		agent.mu.Lock()
		clear(agent.sessions)
		agent.mu.Unlock()
		require.NoError(t, agent.Close())
	})

	return agent
}

func TestRateLimitsProviderAndSessionSelection(t *testing.T) {
	t.Parallel()
	agent := newRateLimitsFixtureAgent(t)
	session := agent.sessions["session-1"]
	session.providerID = "openrouter"
	result, err := agent.HandleExtensionMethod(t.Context(), RateLimitsMethod, json.RawMessage(`{"sessionId":"session-1"}`))
	require.NoError(t, err)
	require.Equal(t, rateLimitsUnsupported("openrouter"), result)

	result, err = agent.HandleExtensionMethod(t.Context(), RateLimitsMethod, json.RawMessage(`{"providerId":"unimplemented","sessionId":"session-1"}`))
	require.NoError(t, err)
	require.Equal(t, rateLimitsUnsupported("unimplemented"), result)

	_, err = agent.HandleExtensionMethod(t.Context(), RateLimitsMethod, json.RawMessage(`{"providerId":"unimplemented","sessionId":"absent"}`))
	requireRateLimitsRequestError(t, err, "unknown session", "sessionId")
	agent.deleted["session-1"] = struct{}{}
	_, err = agent.HandleExtensionMethod(t.Context(), RateLimitsMethod, json.RawMessage(`{"sessionId":"session-1"}`))
	requireRateLimitsRequestError(t, err, "unknown session", "sessionId")
	delete(agent.deleted, "session-1")
	session.closed = true
	_, err = agent.HandleExtensionMethod(t.Context(), RateLimitsMethod, json.RawMessage(`{"sessionId":"session-1"}`))
	requireRateLimitsRequestError(t, err, "unknown session", "sessionId")
	session.closed = false
	session.poisonCause = "native_invariant"
	_, err = agent.HandleExtensionMethod(t.Context(), RateLimitsMethod, json.RawMessage(`{"sessionId":"session-1"}`))
	var requestErr *acp.RequestError
	require.ErrorAs(t, err, &requestErr)
	require.Equal(t, -32603, requestErr.Code)
}

func TestRateLimitsMissingProviderAndCancellation(t *testing.T) {
	t.Parallel()
	agent := NewAgent()
	t.Cleanup(func() { require.NoError(t, agent.Close()) })
	_, err := agent.HandleExtensionMethod(t.Context(), RateLimitsMethod, nil)
	requireRateLimitsRequestError(t, err, "missing", "providerId")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = agent.HandleExtensionMethod(ctx, RateLimitsMethod, json.RawMessage(`{"providerId":"openai"}`))
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, agent.sessions)
	require.NoError(t, agent.Close())
	_, err = agent.HandleExtensionMethod(t.Context(), RateLimitsMethod, json.RawMessage(`{"providerId":"openai"}`))
	require.Error(t, err)
}
