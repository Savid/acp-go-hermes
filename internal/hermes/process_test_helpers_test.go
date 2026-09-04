package hermes

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

const (
	fakeProcessModeOK             = "ok"
	fakeProcessModeStatusOnly     = "status-only"
	fakeProcessModeNoGatewayReady = "no-ready"
	fakeProcessModeBadStatus      = "bad-status"
	fakeProcessModeOldVersion     = "old-version"
	fakeProcessModeBadVersion     = "bad-version"
	fakeProcessModeMissingMethod  = "missing-method"
	fakeProcessModeProbeSentinel  = "probe-sentinel"
	fakeProcessModeSessionCLI     = "session-cli"
)

type fakeSessionCLICapture struct {
	Path        string `json:"path"`
	Resolved    string `json:"resolved"`
	Token       string `json:"token"`
	OperationID string `json:"operationId"`
	Output      string `json:"output"`
}

func TestFakeHermesProcessHelper(t *testing.T) {
	if os.Getenv("ACP_GO_HERMES_INTERNAL_HELPER") != "1" {
		return
	}
	if err := runFakeHermesProcess(os.Args, os.Getenv("ACP_GO_HERMES_INTERNAL_MODE")); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

func fakeHermesExecutable(t *testing.T, mode string) string {
	t.Helper()
	testBinary, err := os.Executable()
	if err != nil {
		t.Fatalf("test executable: %v", err)
	}

	return writeTestBinaryLauncher(t, t.TempDir(), "hermes", testBinary,
		map[string]string{
			"ACP_GO_HERMES_INTERNAL_HELPER": "1",
			"ACP_GO_HERMES_INTERNAL_MODE":   mode,
		},
		[]string{"-test.run=TestFakeHermesProcessHelper", "--"})
}

//nolint:gocyclo // The fake keeps the gateway's complete deterministic method matrix in one server.
func runFakeHermesProcess(args []string, mode string) error {
	for _, arg := range args {
		if arg == "--version" {
			switch mode {
			case fakeProcessModeOldVersion:
				_, _ = fmt.Fprintln(os.Stdout, "Hermes Agent v0.17.9 (test)")

				return nil
			case fakeProcessModeBadVersion:
				_, _ = fmt.Fprintln(os.Stdout, "Hermes Agent test build")

				return nil
			}
			_, _ = fmt.Fprintln(os.Stdout, "Hermes Agent v0.20.0 (test)")

			return nil
		}
	}
	port := ""
	for i, arg := range args {
		if arg == "--port" && i+1 < len(args) {
			port = args[i+1]

			break
		}
	}
	if port == "" {
		return fmt.Errorf("missing --port in %q", strings.Join(args, " "))
	}
	if mode == fakeProcessModeSessionCLI {
		if err := captureFakeSessionCLI(); err != nil {
			return err
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, _ *http.Request) {
		if mode == fakeProcessModeBadStatus {
			w.WriteHeader(http.StatusServiceUnavailable)

			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	if mode != fakeProcessModeStatusOnly {
		mux.HandleFunc("/api/ws", func(w http.ResponseWriter, r *http.Request) {
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close(websocket.StatusNormalClosure, "done")
			if mode != fakeProcessModeNoGatewayReady {
				data, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": "event", "params": Event{Type: "gateway.ready"}})
				_ = conn.Write(r.Context(), websocket.MessageText, data)
			}
			for {
				typ, data, err := conn.Read(r.Context())
				if err != nil {
					return
				}
				if typ != websocket.MessageText {
					continue
				}
				var req struct {
					ID     int64           `json:"id"`
					Method string          `json:"method"`
					Params json.RawMessage `json:"params"`
				}
				if err := json.Unmarshal(data, &req); err != nil {
					return
				}
				params := map[string]any{}
				_ = json.Unmarshal(req.Params, &params)
				var response []byte
				if target, ok := strings.CutPrefix(mode, "probe-error:"); ok && req.Method == target {
					response, _ = json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "error": map[string]any{"code": -32601, "message": "method not found"}})
				} else if target, ok := strings.CutPrefix(mode, "probe-domain:"); ok && req.Method == target {
					response, _ = json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "error": map[string]any{"code": 4007, "message": "session not found"}})
				} else if target, ok := strings.CutPrefix(mode, "probe-empty:"); ok && req.Method == target {
					response, _ = json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": map[string]any{}})
				} else if mode == fakeProcessModeProbeSentinel &&
					(req.Method == "approval.respond" || req.Method == "clarify.respond") &&
					params["session_id"] != missingProbeSessionID {
					response, _ = json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "error": map[string]any{"code": -32601, "message": "presence probe activated a real session"}})
				} else if mode == fakeProcessModeProbeSentinel &&
					(req.Method == "approval.respond" || req.Method == "clarify.respond") {
					response, _ = json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "error": map[string]any{"code": 4007, "message": "session not found"}})
				} else if mode == fakeProcessModeMissingMethod && req.Method == "model.options" {
					response, _ = json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "error": map[string]any{"code": -32601, "message": "method not found"}})
				} else {
					response, _ = json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": resultForMethod(req.Method, params)})
				}
				_ = conn.Write(r.Context(), websocket.MessageText, response)
			}
		})
	}
	// The fake serves until it is killed, and a launcher that cannot exec leaves
	// it running when its own process is killed instead. Reap it against the
	// test process it belongs to so no platform can strand it.
	exitWhenParentTestExits()

	server := &http.Server{Addr: "127.0.0.1:" + port, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	return server.ListenAndServe()
}

func captureFakeSessionCLI() error {
	capturePath := os.Getenv("ACP_GO_HERMES_TEST_ROOT")
	if capturePath == "" {
		return errors.New("session CLI capture path is empty")
	}

	resolved, err := exec.LookPath("wagie")
	if err != nil {
		return fmt.Errorf("resolve wagie: %w", err)
	}
	output, err := exec.Command(resolved).Output()
	if err != nil {
		return fmt.Errorf("execute wagie: %w", err)
	}
	capture := fakeSessionCLICapture{
		Path:        os.Getenv("PATH"),
		Resolved:    resolved,
		Token:       os.Getenv("WAGIE_API_TOKEN"),
		OperationID: os.Getenv("WAGIE_OPERATION_ID"),
		Output:      string(output),
	}
	encoded, err := json.Marshal(capture)
	if err != nil {
		return err
	}

	return os.WriteFile(capturePath, encoded, 0o600)
}

func darwinTestProcessOptions(t *testing.T, options ProcessOptions) ProcessOptions {
	t.Helper()
	options.AmbientEnvironment = testAmbientEnvironment()
	if options.ScratchParent == "" {
		options.ScratchParent = t.TempDir()
	}

	return options
}

func testAmbientEnvironment() map[string]string {
	return map[string]string{"PATH": os.Getenv("PATH"), "HOME": os.Getenv("HOME")}
}
