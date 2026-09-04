package hermesacp

import (
	"context"
	"encoding/json"
	"testing"
)

func disconnectParams(connectionID string, generation int64) map[string]any {
	return map[string]any{
		authFieldSessionID:         string(testSessionID),
		authFieldProviderID:        testProviderID,
		authFieldConnectionID:      connectionID,
		authFieldBindingGeneration: generation,
	}
}

func disconnectWithContext(
	t *testing.T,
	agent *Agent,
	ctx context.Context,
	params map[string]any,
) (any, error) {
	t.Helper()

	encoded, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal disconnect params: %v", err)
	}

	return agent.providerAuth.disconnect(ctx, encoded)
}
