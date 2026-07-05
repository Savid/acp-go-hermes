package hermes

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"
)

const MinimumVersion = "0.18.0"

var (
	commandContext      = exec.CommandContext
	listenTCP           = net.Listen
	randReader          = rand.Reader
	mkdirTemp           = os.MkdirTemp
	mkdirAll            = os.MkdirAll
	userHomeDir         = os.UserHomeDir
	statPath            = os.Stat
	after               = time.After
	newStatusHTTPClient = func() *http.Client { return &http.Client{Timeout: 2 * time.Second} }
	waitProcessCommand  = func(cmd *exec.Cmd) error { return cmd.Wait() }
	executableProbeMu   sync.Mutex
	executableProbed    = map[string]struct{}{}
	versionPattern      = regexp.MustCompile(`v?(\d+)\.(\d+)\.(\d+)`)
)

type ProcessOptions struct {
	ExecutablePath string
	Home           string
	Cwd            string
	Env            map[string]string
	Timeout        time.Duration
	Configure      func(*exec.Cmd)
	LogWriter      io.Writer
}

type Process struct {
	Cmd       *exec.Cmd
	Client    *Client
	Home      string
	Port      int
	Token     string
	StatusURL string

	cancel context.CancelFunc
}

func Start(ctx context.Context, opts ProcessOptions) (*Process, error) {
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	executable := opts.ExecutablePath
	if executable == "" {
		executable = "hermes"
	}
	home := opts.Home
	if home == "" {
		var err error
		home, err = mkdirTemp("", "acp-go-hermes-*")
		if err != nil {
			return nil, err
		}
	}
	if err := mkdirAll(home, 0o700); err != nil {
		return nil, err
	}
	versionCtx, versionCancel := context.WithTimeout(ctx, timeout)
	probeNeeded, err := ensureExecutableVersion(versionCtx, executable)
	versionCancel()
	if err != nil {
		return nil, err
	}
	port, err := freePort()
	if err != nil {
		return nil, err
	}
	token, err := randomToken()
	if err != nil {
		return nil, err
	}

	processCtx, cancel := context.WithCancel(context.Background())
	args := []string{"serve", "--host", "127.0.0.1", "--port", strconv.Itoa(port)}
	if opts.Env["HERMES_WEB_DIST"] != "" || defaultWebDistExists() {
		args = append(args, "--skip-build")
	}
	cmd := commandContext(processCtx, executable, args...)
	if opts.Cwd != "" {
		cmd.Dir = opts.Cwd
	}
	env := os.Environ()
	for key, value := range opts.Env {
		env = append(env, key+"="+value)
	}
	env = append(env,
		"HERMES_HOME="+home,
		"HERMES_DASHBOARD_SESSION_TOKEN="+token,
	)
	cmd.Env = env
	if opts.LogWriter != nil {
		cmd.Stdout = opts.LogWriter
		cmd.Stderr = opts.LogWriter
	}
	if opts.Configure != nil {
		opts.Configure(cmd)
	}
	if err := cmd.Start(); err != nil {
		cancel()
		return nil, err
	}
	process := &Process{
		Cmd:       cmd,
		Home:      home,
		Port:      port,
		Token:     token,
		StatusURL: "http://127.0.0.1:" + strconv.Itoa(port) + "/api/status",
		cancel:    cancel,
	}
	readyCtx, readyCancel := context.WithTimeout(ctx, timeout)
	defer readyCancel()
	if err := process.waitReady(readyCtx); err != nil {
		_ = process.Close(context.Background())
		return nil, err
	}
	client, err := Dial(readyCtx, "ws://127.0.0.1:"+strconv.Itoa(port)+"/api/ws?token="+token, http.Header{
		"X-Hermes-Session-Token": []string{token},
	})
	if err != nil {
		_ = process.Close(context.Background())
		return nil, err
	}
	process.Client = client
	if err := process.waitGatewayReady(readyCtx); err != nil {
		_ = process.Close(context.Background())
		return nil, err
	}
	if probeNeeded {
		if err := process.probeGatewayMethods(readyCtx); err != nil {
			_ = process.Close(context.Background())
			return nil, err
		}
		markExecutableProbed(executable)
	}
	return process, nil
}

func ensureExecutableVersion(ctx context.Context, executable string) (bool, error) {
	executableProbeMu.Lock()
	_, ok := executableProbed[executable]
	executableProbeMu.Unlock()
	if ok {
		return false, nil
	}
	cmd := commandContext(ctx, executable, "--version")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return false, fmt.Errorf("hermes --version probe failed: %w: %s", err, string(out))
	}
	version, ok := parseVersion(string(out))
	if !ok {
		return false, fmt.Errorf("hermes --version output missing semantic version: %s", string(out))
	}
	if compareVersions(version, MinimumVersion) < 0 {
		return false, fmt.Errorf("hermes version %s is below minimum %s", version, MinimumVersion)
	}
	return true, nil
}

func markExecutableProbed(executable string) {
	executableProbeMu.Lock()
	executableProbed[executable] = struct{}{}
	executableProbeMu.Unlock()
}

func parseVersion(output string) (string, bool) {
	match := versionPattern.FindStringSubmatch(output)
	if len(match) != 4 {
		return "", false
	}
	return match[1] + "." + match[2] + "." + match[3], true
}

func compareVersions(left string, right string) int {
	l := versionParts(left)
	r := versionParts(right)
	for i := range l {
		switch {
		case l[i] < r[i]:
			return -1
		case l[i] > r[i]:
			return 1
		}
	}
	return 0
}

func versionParts(value string) [3]int {
	var out [3]int
	parts := regexp.MustCompile(`\.`).Split(value, 3)
	for i := 0; i < len(parts) && i < len(out); i++ {
		n, _ := strconv.Atoi(parts[i])
		out[i] = n
	}
	return out
}

func (p *Process) probeGatewayMethods(ctx context.Context) error {
	created, err := p.Client.CreateSession(ctx, map[string]any{"cwd": p.Home, "title": "acp-go-hermes startup probe"})
	if err != nil {
		return fmt.Errorf("hermes startup probe session.create failed: %w", err)
	}
	if created.SessionID == "" || created.StoredSessionID == "" {
		return fmt.Errorf("hermes startup probe session.create schema drift")
	}
	if resumed, err := p.Client.ResumeSession(ctx, created.StoredSessionID, map[string]any{}); err != nil {
		if err := methodPresent("session.resume", err); err != nil {
			return err
		}
	} else if resumed.SessionID == "" || resumed.StoredSessionID == "" {
		return fmt.Errorf("hermes startup probe session.resume schema drift")
	}
	if active, err := p.Client.ActiveList(ctx); err != nil {
		return fmt.Errorf("hermes startup probe session.active_list failed: %w", err)
	} else if active.Sessions == nil {
		return fmt.Errorf("hermes startup probe session.active_list schema drift")
	}
	live := created.SessionID
	if models, err := p.Client.ModelOptions(ctx, live); err != nil {
		return fmt.Errorf("hermes startup probe model.options failed: %w", err)
	} else if models.Providers == nil {
		return fmt.Errorf("hermes startup probe model.options schema drift")
	}
	if err := methodPresent("prompt.submit", p.Client.SubmitPrompt(ctx, "__acp_go_hermes_missing_probe__", "")); err != nil {
		return err
	}
	if err := methodPresent("approval.respond", p.Client.ApprovalRespond(ctx, live, "deny", false)); err != nil {
		return err
	}
	if err := methodPresent("clarify.respond", p.Client.ClarifyRespond(ctx, live, "")); err != nil {
		return err
	}
	if err := p.Client.CloseSession(ctx, live); err != nil {
		return fmt.Errorf("hermes startup probe session.close failed: %w", err)
	}
	if err := p.Client.DeleteSession(ctx, created.StoredSessionID); err != nil {
		if err := methodPresent("session.delete", err); err != nil {
			return err
		}
	}
	return nil
}

func methodPresent(method string, err error) error {
	if err == nil {
		return nil
	}
	var rpcErr *RPCError
	if errors.As(err, &rpcErr) && rpcErr.Code != -32601 && rpcErr.Code >= 4000 {
		return nil
	}
	return fmt.Errorf("hermes startup probe %s failed: %w", method, err)
}

func (p *Process) Close(ctx context.Context) error {
	if p.Client != nil {
		_ = p.Client.Close(websocket.StatusNormalClosure, "closing")
	}
	defer func() {
		if p.cancel != nil {
			p.cancel()
		}
	}()
	if p.Cmd == nil || p.Cmd.Process == nil {
		return nil
	}
	afterFn := after
	waitFn := waitProcessCommand
	done := make(chan error, 1)
	go func() { done <- waitFn(p.Cmd) }()
	_ = terminateProcess(p.Cmd)
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		_ = killProcess(p.Cmd)
		return ctx.Err()
	case <-afterFn(5 * time.Second):
		_ = killProcess(p.Cmd)
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		case <-afterFn(time.Second):
		}
		return fmt.Errorf("hermes serve did not exit")
	}
}

// Redial opens a fresh WebSocket gateway connection to the still-running
// `hermes serve` process, used to recover from an idle disconnect. It does not
// mutate p.Client; the caller owns the returned client's lifetime.
func (p *Process) Redial(ctx context.Context) (*Client, error) {
	url := "ws://127.0.0.1:" + strconv.Itoa(p.Port) + "/api/ws?token=" + p.Token
	return Dial(ctx, url, http.Header{"X-Hermes-Session-Token": []string{p.Token}})
}

func (p *Process) waitReady(ctx context.Context) error {
	client := newStatusHTTPClient()
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.StatusURL, http.NoBody)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			if err != nil {
				return fmt.Errorf("hermes status check failed: %w", err)
			}
			return ctx.Err()
		case <-after(100 * time.Millisecond):
		}
	}
}

func (p *Process) waitGatewayReady(ctx context.Context) error {
	for {
		select {
		case event, ok := <-p.Client.Events():
			if !ok {
				return fmt.Errorf("hermes websocket closed before gateway.ready")
			}
			if event.Type == "gateway.ready" {
				return nil
			}
		case err, ok := <-p.Client.Errors():
			if ok && err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func freePort() (int, error) {
	ln, err := listenTCP("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port, nil
}

func randomToken() (string, error) {
	var buf [32]byte
	if _, err := randReader.Read(buf[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf[:]), nil
}

func defaultWebDistExists() bool {
	home, err := userHomeDir()
	if err != nil {
		return false
	}
	info, err := statPath(filepath.Join(home, ".hermes", "hermes-agent", "hermes_cli", "web_dist"))
	return err == nil && info.IsDir()
}

func IsStateDB(path string) bool {
	name := filepath.Base(path)
	return name == "state.db" || name == "state.db-wal" || name == "state.db-shm"
}
