package hermes

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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
	return process, nil
}

func (p *Process) Close(ctx context.Context) error {
	if p.Client != nil {
		_ = p.Client.Close(websocket.StatusNormalClosure, "closing")
	}
	if p.cancel != nil {
		p.cancel()
	}
	if p.Cmd == nil || p.Cmd.Process == nil {
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- waitProcessCommand(p.Cmd) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		_ = p.Cmd.Process.Kill()
		return ctx.Err()
	case <-after(5 * time.Second):
		_ = p.Cmd.Process.Kill()
		return fmt.Errorf("hermes serve did not exit")
	}
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
