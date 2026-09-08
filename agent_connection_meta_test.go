package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
	"github.com/savid/acp-go-hermes/internal/lifecycle"
	"github.com/stretchr/testify/require"
)

func TestServePreservesOwnedLifecycleMetadata(t *testing.T) {
	const offer = `"acp-go.dev/lifecycle":{"version":1}`
	const route = `"acp-go.dev/route":{"version":1,"turnNonce":"turn"}`
	const submission = `"submission":{"submissionId":"s","clientNonce":"n"}`
	for _, tc := range []struct {
		name, method, meta, field, verdict string
	}{
		{"SDK metadata alias", "initialize", `"_META":{"acp-go.dev/lifecycle":{"version":2,"version":1}}`, lifecycle.MetaPath + ".version", "unsupported"},
		{"mixed metadata aliases", "initialize", `"_META":{` + offer + `},"_meta":{}`, lifecycle.MetaPath, "unsupported"},
		{"owned overflow", "initialize", `"_meta":{"acp-go.dev/lifecycle":{"version":1e400}}`, lifecycle.MetaPath + ".version", "unsupported"},
		{"tagged session erasure", "session/new", `"_meta":{` + offer + `},"_meta":{}`, lifecycle.MetaPath, "unsupported"},
		{"route fraction", "session/prompt", `"_meta":{"acp-go.dev/route":{"version":1.0000000000000001,"turnNonce":"turn"},"acp-go.dev/lifecycle":{"version":1,` + submission + `}}`, routeMetaPath + ".version", "unsupported"},
		{"route integral exponent", "session/prompt", `"_meta":{"acp-go.dev/route":{"version":1e0,"turnNonce":"turn"},"acp-go.dev/lifecycle":{"version":1,` + submission + `}}`, "", ""},
		{"overflow route precedence", "session/prompt", `"_meta":{"acp-go.dev/lifecycle":{"version":1e400}}`, routeMetaPath, "missing"},
		{"valid offer", "initialize", `"_meta":{` + offer + `}`, "", ""},
		{"foreign envelope duplicates", "initialize", `"_meta":{"foreign":1},"_meta":{"foreign":2}`, "", ""},
		{"duplicate namespace", "initialize", `"_meta":{` + offer + `,` + offer + `}`, lifecycle.MetaPath, "unsupported"},
		{"erased offer", "initialize", `"_meta":{` + offer + `},"_meta":{}`, lifecycle.MetaPath, "unsupported"},
		{"erased by null", "initialize", `"_meta":{` + offer + `},"_meta":null`, lifecycle.MetaPath, "unsupported"},
		{"repeated envelope", "initialize", `"_meta":{},"_meta":{` + offer + `}`, lifecycle.MetaPath, "unsupported"},
		{"valid prompt", "session/prompt", `"_meta":{` + route + `,"acp-go.dev/lifecycle":{"version":1,` + submission + `}}`, "", ""},
		{"duplicate prompt version", "session/prompt", `"_meta":{` + route + `,"acp-go.dev/lifecycle":{"version":2,"version":1,` + submission + `}}`, lifecycle.MetaPath + ".version", "unsupported"},
		{"rounded prompt version", "session/prompt", `"_meta":{` + route + `,"acp-go.dev/lifecycle":{"version":1.0000000000000001,` + submission + `}}`, lifecycle.MetaPath + ".version", "unsupported"},
		{"duplicate submission", "session/prompt", `"_meta":{` + route + `,"acp-go.dev/lifecycle":{"version":1,` + submission + `,` + submission + `}}`, lifecycle.MetaPath + ".submission", "unsupported"},
		{"duplicate identifier", "session/prompt", `"_meta":{` + route + `,"acp-go.dev/lifecycle":{"version":1,"submission":{"submissionId":"first","submissionId":"s","clientNonce":"n"}}}`, lifecycle.MetaPath + ".submission.submissionId", "unsupported"},
		{"erased prompt", "session/prompt", `"_meta":{` + offer + `},"_meta":{` + route + `}`, lifecycle.MetaPath, "unsupported"},
		{"missing correlation", "session/prompt", `"_meta":{` + route + `}`, lifecycle.MetaPath, "missing"},
		{"route precedence", "session/prompt", `"_meta":{"acp-go.dev/lifecycle":{"version":2,"version":1}}`, `_meta["acp-go.dev/route"]`, "missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			agent := newTestAgent()
			agent.options.clientFactory = func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
				t.Error("malformed metadata reached native startup")

				return nil, errors.New("native startup forbidden")
			}
			client := newFakeHermesClient()
			session := testSession(t, agent, client)
			agent.sessions[session.id] = session
			previous := newAgentForServe
			newAgentForServe = func(...Option) *Agent { return agent }
			t.Cleanup(func() { newAgentForServe = previous })
			input, writer := io.Pipe()
			reader, output := io.Pipe()
			done := make(chan error, 1)
			go func() { done <- Serve(ctx, input, output) }()
			t.Cleanup(func() {
				_ = writer.Close()
				_ = reader.Close()
				_ = input.Close()
				_ = output.Close()
				cancel()
				<-done
			})
			decoder := json.NewDecoder(reader)
			call := func(method, params string) *acp.RequestError {
				t.Helper()
				_, err := io.WriteString(writer, `{"jsonrpc":"2.0","id":1,"method":"`+method+`","params":`+params+"}\n")
				require.NoError(t, err)
				var response struct {
					ID    *int              `json:"id"`
					Error *acp.RequestError `json:"error"`
				}
				for response.ID == nil {
					require.NoError(t, decoder.Decode(&response))
				}

				return response.Error
			}
			params := `{"protocolVersion":1,` + tc.meta + `}`
			if tc.method != "initialize" {
				require.Nil(t, call("initialize", `{"protocolVersion":1,"_meta":{`+offer+`}}`))
				params = `{"sessionId":"session-1","prompt":[{"type":"text","text":"reply"}],` + tc.meta + `}`
				if tc.method == "session/new" {
					params = fmt.Sprintf(`{"cwd":%q,"mcpServers":[],%s}`, session.cwd, tc.meta)
				}
			}
			err := call(tc.method, params)
			if tc.field == "" {
				require.Nil(t, err)
			} else {
				require.NotNil(t, err)
				require.EqualValues(t, -32602, err.Code)
				require.Equal(t, "Invalid params", err.Message)
				require.Equal(t, map[string]any{"error": tc.verdict, "field": tc.field}, err.Data)
				require.Zero(t, client.promptDispatchCount())
			}
		})
	}
}

func TestServeHandoffNumbersPreserveExactValuesAndPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name, version, size, data, want string
	}{
		{"fractional version", "1.0000000000000001", "1", "", imageErrInvalidHandoff},
		{"fractional size", "1", "1.0000000000000001", "", imageErrInvalidHandoff},
		{"tiny negative size", "1", "-1e-400", "", imageErrInvalidHandoff},
		{"huge exponent", "1e400", "0", "", imageErrInvalidHandoff},
		{"mathematical integers", "1e0", "0.0", "", imageErrInvalidMediaType},
		{"embedded dominance", "1e400", "-1e-400", "embedded", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := durableTempDir(t)
			agent := newTestAgent(WithInputHandoffRoot(root))
			client := newFakeHermesClient()
			session := testSession(t, agent, client)
			agent.sessions[session.id] = session
			call := serveMetadataCaller(t, agent)
			require.Nil(t, call("initialize", `{"protocolVersion":1}`))
			var reads atomic.Int32
			previous := openHandoffRoot
			openHandoffRoot = func(string) (*os.Root, error) {
				reads.Add(1)

				return nil, os.ErrPermission
			}
			t.Cleanup(func() { openHandoffRoot = previous })
			data, mime := "", "image/unsupported"
			if tc.data != "" {
				data, mime = fixtureBase64(t, "valid.png"), mimePNG
			}
			params := fmt.Sprintf(`{"sessionId":"session-1","_meta":{"acp-go.dev/route":{"version":1,"turnNonce":"turn"}},"prompt":[{"type":"image","data":%q,"mimeType":%q,"uri":%q,"_META":{"acp-go.dev/handoff":{"version":%s,"sizeBytes":%s,"digest":%q}}}]}`,
				data, mime, handoffFileURI(root+string(os.PathSeparator)+"image.png"), tc.version, tc.size, handoffDigest(nil))
			err := call("session/prompt", params)
			if tc.want == "" {
				require.Nil(t, err)
				require.Equal(t, uint64(1), client.promptDispatchCount())
			} else {
				require.NotNil(t, err)
				require.EqualValues(t, -32602, err.Code)
				fields, ok := err.Data.(map[string]any)
				require.True(t, ok)
				require.Equal(t, tc.want, fields[jsonFieldError])
				require.Equal(t, acpFieldPromptImage, fields[jsonFieldField])
				require.Zero(t, client.promptDispatchCount())
			}
			require.Zero(t, reads.Load())
		})
	}
}

func serveMetadataCaller(t *testing.T, agent *Agent) func(string, string) *acp.RequestError {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	previous := newAgentForServe
	newAgentForServe = func(...Option) *Agent { return agent }
	t.Cleanup(func() { newAgentForServe = previous })
	input, writer := io.Pipe()
	reader, output := io.Pipe()
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, input, output) }()
	t.Cleanup(func() {
		_ = writer.Close()
		_ = reader.Close()
		_ = input.Close()
		_ = output.Close()
		cancel()
		<-done
	})
	decoder := json.NewDecoder(reader)

	return func(method, params string) *acp.RequestError {
		t.Helper()
		_, err := io.WriteString(writer, `{"jsonrpc":"2.0","id":1,"method":"`+method+`","params":`+params+"}\n")
		require.NoError(t, err)
		var response struct {
			ID    *int              `json:"id"`
			Error *acp.RequestError `json:"error"`
		}
		for response.ID == nil {
			require.NoError(t, decoder.Decode(&response))
		}

		return response.Error
	}
}
