package hermes

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEndpointArgs(t *testing.T) {
	t.Parallel()

	endpoint := Endpoint{URL: "http://127.0.0.1:43117", Token: "secret"}
	require.Equal(t, []string{"serve", "--isolated", "--host", "127.0.0.1", "--port", "43117"}, endpoint.Args(),
		"the session's own backend, never the user's host backend; the token stays out of argv")
}
