package hermes

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeUsage(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		payload string
		want    Usage
		ok      bool
	}{
		"tick":           {`{"usage":{"calls":2,"prompt":30,"completion":4,"reasoning":1,"total":34,"context_used":18,"context_max":1000}}`, Usage{Prompt: 30, Completion: 4, Reasoning: 1, Total: 34, ContextUsed: 18, ContextMax: 1000}, true},
		"compacted":      {`{"text":"done","status":"complete","usage":{"prompt":30,"completion":4,"total":34,"compressions":1}}`, Usage{Prompt: 30, Completion: 4, Total: 34}, true},
		"no agent":       {`{"text":"","status":"error","usage":{}}`, Usage{}, true},
		"without usage":  {`{"text":"summary"}`, Usage{}, false},
		"malformed body": {`[]`, Usage{}, false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, ok := DecodeUsage(json.RawMessage(tc.payload))
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestUsageSince(t *testing.T) {
	t.Parallel()

	earlier := Usage{Prompt: 100, Completion: 10, Reasoning: 2, Total: 110, ContextUsed: 60}

	require.Equal(t, Usage{Prompt: 40, Completion: 5, Reasoning: 1, Total: 45}, Usage{Prompt: 140, Completion: 15, Reasoning: 3, Total: 155, ContextUsed: 40}.Since(earlier))
	require.Equal(t, Usage{Prompt: 30, Completion: 4, Total: 34}, Usage{Prompt: 30, Completion: 4, Total: 34}.Since(earlier), "a rebuilt agent restarts its counters")
	require.Equal(t, Usage{Prompt: 30, Completion: 20, Reasoning: 3, Total: 50}, Usage{Prompt: 30, Completion: 20, Reasoning: 3, Total: 50}.Since(earlier), "a rebuilt agent can exceed some earlier counters before its first reading")
	require.Equal(t, Usage{Prompt: 70, Completion: 14, Reasoning: 3, Total: 84}, Usage{Prompt: 40, Completion: 4, Reasoning: 1, Total: 44}.Add(Usage{Prompt: 30, Completion: 10, Reasoning: 2, Total: 40}))
}
