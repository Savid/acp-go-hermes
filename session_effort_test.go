package hermesacp

import (
	"context"
	"testing"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

func effortOption(t *testing.T, options []acp.SessionConfigOption) *acp.SessionConfigOptionSelect {
	t.Helper()

	for _, option := range options {
		if option.Select != nil && option.Select.Id == configEffort {
			return option.Select
		}
	}

	return nil
}

func effortValues(t *testing.T, sel *acp.SessionConfigOptionSelect) []string {
	t.Helper()
	require.NotNil(t, sel)
	require.NotNil(t, sel.Options.Ungrouped)

	values := make([]string, 0, len(*sel.Options.Ungrouped))
	for _, option := range *sel.Options.Ungrouped {
		values = append(values, string(option.Value))
	}

	return values
}

// TestEffortOptionReadsTheLevelHermesResolved pins the option's current value
// when the session selected nothing: the level Hermes reports for it, read
// back once and then held, with the full vocabulary offered around it.
func TestEffortOptionReadsTheLevelHermesResolved(t *testing.T) {
	ctx := context.Background()
	client := newFakeHermesClient()
	client.effort = "high"
	agent := newTestAgent()
	sess := testSession(t, agent, client)

	sel := effortOption(t, sess.configOptions(ctx))
	require.NotNil(t, sel)
	require.Equal(t, acp.SessionConfigValueId("high"), sel.CurrentValue)
	require.Equal(t, acp.SessionConfigOptionCategoryThoughtLevel, *sel.Category)
	require.Equal(t, hermesEffortLevels, effortValues(t, sel))
	require.Equal(t, "high", sess.currentEffort(), "the read-back is held on the session")

	// A level Hermes reports outside the vocabulary stays selectable.
	client.effort = "custom"
	other := testSession(t, agent, client)
	require.Contains(t, effortValues(t, effortOption(t, other.configOptions(ctx))), "custom")

	// A session Hermes will not answer for publishes no effort option.
	client.effort = ""
	silent := testSession(t, agent, client)
	require.Nil(t, effortOption(t, silent.configOptions(ctx)))
}

// TestEffortSelectionBindsNativelyAndRefusesOutsideTheVocabulary pins the set
// door: a level reaches Hermes as a session-scoped reasoning selection and the
// acknowledged level becomes the current value; a display word or any other
// value never reaches Hermes; a native refusal maps to the value error.
func TestEffortSelectionBindsNativelyAndRefusesOutsideTheVocabulary(t *testing.T) {
	ctx := context.Background()
	client := newFakeHermesClient()
	client.effort = "medium"
	agent := newTestAgent()
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	sess := testSession(t, agent, client)
	agent.mu.Lock()
	agent.sessions[sess.id] = sess
	agent.mu.Unlock()

	resp, err := agent.SetSessionConfigOption(ctx, SetEffortRequest(sess.id, "xhigh"))
	require.NoError(t, err)
	require.Equal(t, []fakeModelSelection{{sessionID: "native-1", value: "xhigh"}}, client.setEffortCalls)
	require.Equal(t, acp.SessionConfigValueId("xhigh"), effortOption(t, resp.ConfigOptions).CurrentValue)
	require.Equal(t, "xhigh", sess.currentEffort())
	require.NotZero(t, conn.updateCount(), "a selection emits a config update")

	for _, value := range []string{"show", "hide", "full", "clamp", "HIGH", "maximum"} {
		_, refused := agent.SetSessionConfigOption(ctx, SetEffortRequest(sess.id, value))
		requireUnsupportedField(t, refused, keyValue, value)
	}
	require.Len(t, client.setEffortCalls, 1, "nothing outside the vocabulary reaches Hermes")

	client.setEffortErr = &nativehermes.RPCError{Code: 4002, Message: "unknown reasoning value"}
	_, err = agent.SetSessionConfigOption(ctx, SetEffortRequest(sess.id, "ultra"))
	var reqErr *acp.RequestError
	require.ErrorAs(t, err, &reqErr)
	data, ok := reqErr.Data.(map[string]any)
	require.True(t, ok)
	require.Equal(t, valHermesEffortSelectionRefused, data[jsonFieldError])
	require.Equal(t, "xhigh", sess.currentEffort(), "a refused selection leaves the bound level")
}

// TestSessionMetaEffortBindsAtCreation pins the lifecycle door: an effort in
// _meta.hermes.options is bound on the native session before it is published,
// and a value outside the vocabulary fails naming the option path.
func TestSessionMetaEffortBindsAtCreation(t *testing.T) {
	meta, err := sessionMetaFromLifecycle(NewHermesOptions(WithHermesEffort("low")).Meta())
	require.NoError(t, err)
	require.Equal(t, "low", meta.Effort)

	_, err = sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{metaEffortKey: "show"}}})
	requireUnsupportedField(t, err, hermesEffortOptionPath, "display word")
	_, err = sessionMetaFromLifecycle(map[string]any{hermesMetaKey: map[string]any{metaOptionsKey: map[string]any{metaEffortKey: 3}}})
	requireUnsupportedField(t, err, hermesEffortOptionPath, "non-string")

	client := newFakeHermesClient()
	sess := testSession(t, newTestAgent(), client)
	sess.setEffort("max")
	require.Equal(t, "max", sess.snapshot().effort)
}
