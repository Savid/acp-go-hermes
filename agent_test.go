package hermesacp

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCancelledVersionProbeDoesNotPoisonAgent(t *testing.T) {
	agent := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = agent.Close() })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := agent.ensureExecutable(ctx)
	require.Error(t, err)
	_, err = agent.ensureExecutable(t.Context())
	require.NoError(t, err, "a request-local cancelled probe permanently poisoned the agent")
}
