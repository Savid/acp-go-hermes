package lifecycle

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLifecycleCapabilityStrictScalar(t *testing.T) {
	t.Parallel()

	var decoded Negotiated
	require.NoError(t, json.Unmarshal([]byte(`{"version":1}`), &decoded))
	require.Equal(t, Version, decoded.Version)

	for _, test := range []struct {
		name string
		data string
	}{
		{"empty", `{}`},
		{"not json", ``},
		{"truncated member", `{"`},
		{"truncated close", `{"version":1`},
		{"missing", `{"updatesOutsidePrompt":true}`},
		{"other integer", `{"version":2}`},
		{"fractional", `{"version":1.0}`},
		{"string", `{"version":"1"}`},
		{"boolean", `{"version":true}`},
		{"duplicate", `{"version":1,"version":1}`},
		{"unknown", `{"version":1,"unknown":true}`},
		{"invalid updates outside prompt", `{"version":1,"updatesOutsidePrompt":"true"}`},
		{"invalid authoritative quiescence", `{"version":1,"authoritativeQuiescence":"true"}`},
		{"invalid quiescence source", `{"version":1,"quiescenceSource":1}`},
		{"invalid activity kinds", `{"version":1,"activityKinds":true}`},
		{"trailing", `{"version":1} {}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var value Negotiated
			require.Error(t, json.Unmarshal([]byte(test.data), &value))
		})
	}
}
