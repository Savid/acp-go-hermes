package hermes

import (
	"net/http"
	"net/http/httptest"
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

func TestDecodeConversation(t *testing.T) {
	t.Parallel()

	export := `{"id":"s1","messages":[{"id":1,"role":"user","content":"hi"}, {"id":2,"role":"assistant","content":{"a":[1,2]}}],"title":"t","cwd":"/w"}`

	conversation, err := DecodeConversation(strings.NewReader(export))
	require.NoError(t, err)
	require.JSONEq(t, `{"id":"s1","title":"t","cwd":"/w"}`, string(conversation.Session))
	require.Equal(t, []string{`{"id":1,"role":"user","content":"hi"}`, `{"id":2,"role":"assistant","content":{"a":[1,2]}}`},
		[]string{string(conversation.Messages[0]), string(conversation.Messages[1])}, "messages keep their raw bytes")
	require.Equal(t, "s1", SessionID(conversation.Session))
	require.NoError(t, ValidSession(conversation.Session, "s1"))

	encoded, err := conversation.Encode()
	require.NoError(t, err)
	require.JSONEq(t, export, string(encoded))

	empty, err := DecodeConversation(strings.NewReader(`{"id":"s2","messages":[]}`))
	require.NoError(t, err)
	require.NotNil(t, empty.Messages)
	require.Empty(t, empty.Messages)

	for _, invalid := range []string{
		`[]`, `{"id":"s1"}`, `{"id":"s1","messages":[1]}`, `{"messages":[],"messages":[]}`,
		`{"id":"s1","messages":[]} {}`, `{"id":"s1","messages":[{}]`,
	} {
		_, err := DecodeConversation(strings.NewReader(invalid))
		require.Error(t, err, invalid)
	}
}

func TestValidSession(t *testing.T) {
	t.Parallel()

	for session, valid := range map[string]bool{
		`{"id":"s1","cwd":"/w"}`:    true,
		`{"id":"s2"}`:               false,
		`{"id":"s1","messages":[]}`: false,
		`null`:                      false,
		`[]`:                        false,
	} {
		require.Equal(t, valid, ValidSession([]byte(session), "s1") == nil, session)
	}
}

func TestSameMessage(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		a, b string
		same bool
	}{
		"identical":        {`{"id":1,"role":"user","content":"hi"}`, `{"id":1,"role":"user","content":"hi"}`, true},
		"renumbered":       {`{"id":1,"session_id":"a","role":"user"}`, `{"id":9,"session_id":"b","role":"user"}`, true},
		"reordered":        {`{"role":"user","content":{"a":1,"b":2}}`, `{"content":{"b":2,"a":1},"role":"user"}`, true},
		"different":        {`{"role":"user","content":"hi"}`, `{"role":"user","content":"bye"}`, false},
		"missing member":   {`{"role":"user","content":"hi"}`, `{"role":"user"}`, false},
		"different member": {`{"role":"user","a":1}`, `{"role":"user","b":1}`, false},
	} {
		same, err := SameMessage([]byte(tc.a), []byte(tc.b))
		require.NoError(t, err, name)
		require.Equal(t, tc.same, same, name)
	}

	_, err := SameMessage([]byte(`not json`), []byte(`{}`))
	require.Error(t, err)
}

func TestExportBoundsTheConversation(t *testing.T) {
	t.Parallel()

	body := `{"id":"s1","messages":[{"role":"user","content":"` + strings.Repeat("x", 64) + `"}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/sessions/s1/export":
			_, _ = w.Write([]byte(body))
		case "/api/sessions/other/export":
			_, _ = w.Write([]byte(`{"id":"s1","messages":[]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	endpoint := Endpoint{URL: server.URL, Token: "token"}

	conversation, found, err := endpoint.Export(t.Context(), "s1")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, conversation.Messages, 1)

	_, found, err = endpoint.Export(t.Context(), "missing")
	require.NoError(t, err)
	require.False(t, found)

	_, _, err = endpoint.Export(t.Context(), "other")
	require.ErrorContains(t, err, "identity differs")

	reader := &limitedReader{reader: strings.NewReader(body), remaining: int64(len(body) - 1)}
	_, err = DecodeConversation(reader)
	require.Error(t, err)
	require.True(t, reader.exceeded, "an export past its bound is refused")
}
