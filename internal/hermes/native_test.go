package hermes

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServeArgs(t *testing.T) {
	t.Parallel()

	require.Equal(t, []string{"serve", "--isolated", "--host", "127.0.0.1", "--port", "0"}, ServeArgs(),
		"the session's own backend on a port the OS assigns at bind, never the user's host backend; the token stays out of argv")
}

func TestEndpointBound(t *testing.T) {
	t.Parallel()

	endpoint := Endpoint{Token: "secret"}.Bound(43117)
	require.Equal(t, Endpoint{URL: "http://127.0.0.1:43117", Token: "secret"}, endpoint)
}

func TestScanStdout(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name, stdout string
		port         int
	}{
		{"announced", "HERMES_BACKEND_READY port=43117\nHERMES_DASHBOARD_READY port=43117\n", 43117},
		// The long line's tail opens like an announcement but is not a line.
		{"after noise", "starting\r\n" + strings.Repeat("x", 4096) + "HERMES_BACKEND_READY port=1\nHERMES_BACKEND_READY port=43117\r\n", 43117},
		{"unterminated", "HERMES_BACKEND_READY port=43117", 43117},
		{"first only", "HERMES_BACKEND_READY port=43117\nHERMES_BACKEND_READY port=43118\n", 43117},
		{"legacy only", "HERMES_DASHBOARD_READY port=43117\n", 0},
		{"invalid", "HERMES_BACKEND_READY port=0\nHERMES_BACKEND_READY port=70000\nHERMES_BACKEND_READY port=x\n", 0},
		{"silent", "", 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			bound := make(chan int, 1)
			// Returning means stdout was read to its end.
			ScanStdout(strings.NewReader(tc.stdout), bound)

			select {
			case port := <-bound:
				require.Equal(t, tc.port, port)
			default:
				require.Zero(t, tc.port, "no port was announced")
			}
		})
	}
}
