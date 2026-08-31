package lifecycle

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNegotiatedJSONExactVersion(t *testing.T) {
	t.Parallel()

	var decoded Negotiated
	require.NoError(t, json.Unmarshal([]byte(`{"version":1}`), &decoded))
	require.Equal(t, Version, decoded.Version)
	require.NoError(t, json.Unmarshal([]byte(`{}`), &decoded))
	require.False(t, decoded.Present())

	for _, test := range []struct {
		name string
		data string
	}{
		{"missing", `{"updatesOutsidePrompt":true}`},
		{"other integer", `{"version":2}`},
		{"fractional", `{"version":1.0}`},
		{"string", `{"version":"1"}`},
		{"boolean", `{"version":true}`},
		{"duplicate", `{"version":1,"version":1}`},
		{"unknown", `{"version":1,"unknown":true}`},
		{"trailing", `{"version":1} {}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var value Negotiated
			require.Error(t, json.Unmarshal([]byte(test.data), &value))
		})
	}
}
