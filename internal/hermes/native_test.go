package hermes

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
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
		"archived":         {`{"role":"user","active":1,"compacted":0}`, `{"role":"user","active":0,"compacted":1}`, true},
	} {
		same, err := SameMessage([]byte(tc.a), []byte(tc.b))
		require.NoError(t, err, name)
		require.Equal(t, tc.same, same, name)
	}

	_, err := SameMessage([]byte(`not json`), []byte(`{}`))
	require.Error(t, err)
}

func TestExportReadsTheNamedConversation(t *testing.T) {
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

	conversation, found, err := endpoint.Export(t.Context(), "s1", Prefix{})
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, conversation.Messages, 1)

	_, found, err = endpoint.Export(t.Context(), "missing", Prefix{})
	require.NoError(t, err)
	require.False(t, found)

	_, _, err = endpoint.Export(t.Context(), "other", Prefix{})
	require.ErrorContains(t, err, "identity differs")
}

// TestExportBoundsKeptBytes proves the export bounds each message and the new
// messages a continuing read keeps, never the messages it only digests.
func TestExportBoundsKeptBytes(t *testing.T) {
	t.Parallel()

	bounds := exportBounds{value: 64, kept: 64}
	large := `{"content":"` + strings.Repeat("x", 40) + `"}`
	small := `{"id":1}`
	export := func(messages ...string) io.Reader {
		return strings.NewReader(`{"id":"s1","messages":[` + strings.Join(messages, ",") + `]}`)
	}

	prefix := PrefixOf([][]byte{[]byte(large), []byte(large)})

	read, err := decodeExport(export(large, large, small), prefix, bounds)
	require.NoError(t, err, "digested messages past the kept bound do not count")
	require.True(t, read.Continued)
	require.Equal(t, [][]byte{[]byte(small)}, read.Messages)
	require.Equal(t, int64(2*len(large)+len(small)), read.Next.Bytes)

	full, err := decodeExport(export(large, large, small), Prefix{}, bounds)
	require.NoError(t, err, "a full read keeps the whole conversation")
	require.Len(t, full.Messages, 3)

	_, err = decodeExport(export(small, large, large), PrefixOf([][]byte{[]byte(small)}), bounds)
	require.ErrorContains(t, err, "new messages exceed", "a continuing read bounds the new bytes it keeps")

	oversized := `{"content":"` + strings.Repeat("x", 200) + `"}`
	_, err = decodeExport(export(oversized), Prefix{}, bounds)
	require.ErrorIs(t, err, errValueTooLarge, "one message past the value bound is refused")
}

func TestImportLimits(t *testing.T) {
	t.Parallel()

	session := []byte(`{"id":"s1"}`)
	within := Export{Session: session, Next: Prefix{Count: 2, Bytes: 100}}
	require.False(t, within.ImportRefused())
	require.EqualValues(t, len(session)+len(`,"messages":[]`)+1+100, within.ImportSize())

	encoded, err := Conversation{Session: session, Messages: [][]byte{[]byte(`{"a":1}`), []byte(`{"b":2}`)}}.Encode()
	require.NoError(t, err)
	require.EqualValues(t, len(encoded), Export{Session: session, Next: PrefixOf([][]byte{[]byte(`{"a":1}`), []byte(`{"b":2}`)})}.ImportSize(),
		"the size is the encoding's")

	require.True(t, Export{Session: session, Next: Prefix{Count: ImportMessageLimit + 1}}.ImportRefused())
	require.True(t, Export{Session: session, Next: Prefix{Count: 1, Bytes: ImportByteLimit}}.ImportRefused())
}

func TestImportFencesTheMutation(t *testing.T) {
	t.Parallel()

	bodies := make(chan []byte, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		bodies <- data
		_, _ = w.Write([]byte(`{"ok":true,"imported":1}`))
	}))
	t.Cleanup(server.Close)

	endpoint := Endpoint{URL: server.URL, Token: "token"}
	conversation := Conversation{Session: []byte(`{"id":"s1"}`), Messages: [][]byte{[]byte(`{"role":"user"}`)}}
	require.NoError(t, endpoint.Import(t.Context(), conversation))
	require.NoError(t, endpoint.Import(t.Context(), conversation))
	close(bodies)

	requests := make([]string, 0, 2)
	for data := range bodies {
		//nolint:tagliatelle // Hermes uses request_id and expected_revision on the wire.
		var body struct {
			RequestID        string            `json:"request_id"`
			ExpectedRevision *int              `json:"expected_revision"`
			Sessions         []json.RawMessage `json:"sessions"`
		}
		require.NoError(t, json.Unmarshal(data, &body))
		require.NotEmpty(t, body.RequestID)
		require.NotNil(t, body.ExpectedRevision)
		require.Zero(t, *body.ExpectedRevision, "a session the store does not hold is at revision 0")
		require.JSONEq(t, `[{"id":"s1","messages":[{"role":"user"}]}]`, mustJSON(t, body.Sessions))
		requests = append(requests, body.RequestID)
	}
	require.Len(t, requests, 2)
	require.NotEqual(t, requests[0], requests[1], "each import is its own request, never a retry of the last")
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()

	data, err := json.Marshal(value)
	require.NoError(t, err)

	return string(data)
}

func TestPersistenceErrorsCarryHermesText(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/sessions/import" {
			http.Error(w, `{"detail":"Session import payload is too large"}`, http.StatusRequestEntityTooLarge)

			return
		}

		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("database\nlocked"))
	}))
	t.Cleanup(server.Close)

	endpoint := Endpoint{URL: server.URL, Token: "token"}

	err := endpoint.Import(t.Context(), Conversation{Session: []byte(`{"id":"s1"}`), Messages: [][]byte{}})
	require.ErrorContains(t, err, "status 413")
	require.ErrorContains(t, err, "Session import payload is too large")

	_, _, err = endpoint.Export(t.Context(), "s1", Prefix{})
	require.ErrorContains(t, err, "status 500: database locked")
}

func TestDecodeExportAfterPrefix(t *testing.T) {
	t.Parallel()

	messages := []string{`{"id":1,"active":1}`, `{"id":2,"active":1}`, `{"id":3,"active":1}`}
	export := func(messages ...string) string {
		return `{"id":"s1","messages":[` + strings.Join(messages, ",") + `]}`
	}
	prefix := PrefixOf([][]byte{[]byte(messages[0]), []byte(messages[1])})

	continued, err := DecodeExport(strings.NewReader(export(messages...)), prefix)
	require.NoError(t, err)
	require.True(t, continued.Continued)
	require.Equal(t, [][]byte{[]byte(messages[2])}, continued.Messages, "only the messages after the prefix are kept")
	require.Equal(t, PrefixOf([][]byte{[]byte(messages[0]), []byte(messages[1]), []byte(messages[2])}), continued.Next)

	unchanged, err := DecodeExport(strings.NewReader(export(messages[:2]...)), prefix)
	require.NoError(t, err)
	require.True(t, unchanged.Continued)
	require.Empty(t, unchanged.Messages)
	require.Equal(t, prefix, unchanged.Next)

	for name, body := range map[string]string{
		"archived in place": export(`{"id":1,"active":0}`, messages[1], messages[2]),
		"shorter":           export(messages[0]),
	} {
		changed, err := DecodeExport(strings.NewReader(body), prefix)
		require.NoError(t, err, name)
		require.False(t, changed.Continued, name)
		require.Nil(t, changed.Messages, name)
	}
}

func TestExportRereadsAChangedConversation(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32

	body := `{"id":"s1","messages":[{"id":1,"active":0,"compacted":1},{"id":2,"active":1}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)

	endpoint := Endpoint{URL: server.URL, Token: "token"}

	export, found, err := endpoint.Export(t.Context(), "s1", PrefixOf([][]byte{[]byte(`{"id":1,"active":1,"compacted":0}`)}))
	require.NoError(t, err)
	require.True(t, found)
	require.False(t, export.Continued)
	require.Len(t, export.Messages, 2, "a conversation changed in place is read again in full")
	require.EqualValues(t, 2, requests.Load())

	export, _, err = endpoint.Export(t.Context(), "s1", export.Next)
	require.NoError(t, err)
	require.True(t, export.Continued)
	require.Empty(t, export.Messages)
	require.EqualValues(t, 3, requests.Load(), "an unchanged conversation is read once")
}
