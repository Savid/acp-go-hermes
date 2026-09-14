package hermes

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

func TestOrderedResponsesAndNotifications(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.CloseNow() }()
		_, frame, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		var request rpcRequest
		if json.Unmarshal(frame, &request) != nil {
			return
		}
		for _, value := range []any{
			map[string]any{"jsonrpc": "2.0", "method": "event", "params": map[string]any{"type": "message.start", "session_id": "live"}},
			map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": map[string]any{"status": "streaming"}},
			map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": map[string]any{"status": "duplicate"}},
			map[string]any{"jsonrpc": "2.0", "method": "event", "params": map[string]any{"type": "message.complete", "session_id": "live"}},
		} {
			data, _ := json.Marshal(value)
			if conn.Write(r.Context(), websocket.MessageText, data) != nil {
				return
			}
		}
		_, _, _ = conn.Read(r.Context())
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	client, err := Dial(ctx, strings.Replace(server.URL, "http://", "ws://", 1), nil)
	require.NoError(t, err)
	defer func() { _ = client.Close(websocket.StatusNormalClosure, "") }()
	result, watermark, uncertain, err := client.SubmitPromptWatermark(ctx, "live", "hello")
	require.NoError(t, err)
	require.False(t, uncertain)
	require.Equal(t, "streaming", result.Status)
	require.Equal(t, uint64(2), watermark)
	first, second := <-client.Deliveries(), <-client.Deliveries()
	require.Equal(t, uint64(1), first.Event.InboundSequence)
	require.Equal(t, uint64(4), second.Event.InboundSequence)
	require.Equal(t, "message.complete", second.Event.Type)
}

func TestMalformedResponseCannotCompleteCall(t *testing.T) {
	t.Parallel()
	for _, frame := range []string{`{"jsonrpc":"2.0","id":1,"result":{},"error":null}`, `{"jsonrpc":"1.0","id":1,"result":{}}`, `{"jsonrpc":"2.0","id":1,"error":null}`, `[]`} {
		t.Run(frame, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer func() { _ = conn.CloseNow() }()
				if _, _, err := conn.Read(r.Context()); err != nil {
					return
				}
				_ = conn.Write(r.Context(), websocket.MessageText, []byte(frame))
				_, _, _ = conn.Read(r.Context())
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			client, err := Dial(ctx, strings.Replace(server.URL, "http://", "ws://", 1), nil)
			require.NoError(t, err)
			defer func() { _ = client.Close(websocket.StatusNormalClosure, "") }()
			require.Error(t, client.Call(ctx, "prompt.submit", nil, nil))
			require.Error(t, client.Err())
		})
	}
}
