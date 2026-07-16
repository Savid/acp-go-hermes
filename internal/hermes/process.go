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

const (
	MinimumVersion    = "0.18.2"
	fieldCwd          = "cwd"
	fieldTitle        = "title"
	eventGatewayReady = "gateway.ready"
)

// ErrProcessTreeUnproven means a launched Hermes process tree could not be
// proven quiescent. Callers that hold native-root admission must retain it.
var ErrProcessTreeUnproven = errors.New("hermes process tree quiescence is unproven")

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
	processTreeClose    = func(tree *processContainment) error { return tree.close() }
	executableProbeMu   sync.Mutex
	executableProbed    = map[string]struct{}{}
	versionPattern      = regexp.MustCompile(`v?(\d+)\.(\d+)\.(\d+)`)
)

type ProcessOptions struct {
	ExecutablePath string
	Home           string
	// ScratchParent is the resolved parent directory used to materialize an
	// isolated home when Home is empty. The internal package never consults the
	// system temp directory itself.
	ScratchParent       string
	Cwd                 string
	Env                 map[string]string
	Timeout             time.Duration
	Configure           func(*exec.Cmd)
	LogWriter           io.Writer
	ObserveStartupStage func(context.Context, string, string, time.Duration, error)
}

type Process struct {
	Cmd       *exec.Cmd
	Client    *Client
	Home      string
	Port      int
	Token     string
	StatusURL string

	cancel context.CancelFunc
	tree   *processContainment

	waitOnce sync.Once
	waitDone chan struct{}
}

// ProviderDescendantCount returns the absolute number of processes in the
// native containment boundary when that boundary provides authoritative
// inventory. A false result means no observation may be inferred.
func (p *Process) ProviderDescendantCount() (int, bool) {
	if p == nil || p.tree == nil {
		return 0, false
	}

	return p.tree.descendantCount()
}

func Start(ctx context.Context, opts ProcessOptions) (*Process, error) {
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}

	executable := opts.ExecutablePath
	if executable == "" {
		executable = valHermes
	}

	home := opts.Home
	if home == "" {
		var err error

		home, err = mkdirTemp(opts.ScratchParent, "acp-go-hermes-*")
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

	args := []string{valServe, "--host", "127.0.0.1", argPort, strconv.Itoa(port)}
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

	configureHermesProcess(cmd)

	spawnStarted := time.Now()

	tree, startErr := startContainedProcess(cmd)
	if startErr != nil {
		observeHermesStartupStage(ctx, opts.ObserveStartupStage, "session", "spawn", spawnStarted, startErr)
		cancel()

		return nil, startErr
	}

	observeHermesStartupStage(ctx, opts.ObserveStartupStage, "session", "spawn", spawnStarted, nil)

	process := &Process{
		Cmd:       cmd,
		Home:      home,
		Port:      port,
		Token:     token,
		StatusURL: "http://127.0.0.1:" + strconv.Itoa(port) + "/api/status",
		cancel:    cancel,
		tree:      tree,
	}
	process.beginWait()

	readyCtx, readyCancel := context.WithTimeout(ctx, timeout)
	defer readyCancel()

	readinessStarted := time.Now()
	if readyErr := process.waitReady(readyCtx); readyErr != nil {
		observeHermesStartupStage(ctx, opts.ObserveStartupStage, "session", "readiness", readinessStarted, readyErr)

		return nil, errors.Join(readyErr, process.Close(context.Background()))
	}

	client, err := Dial(readyCtx, "ws://127.0.0.1:"+strconv.Itoa(port)+"/api/ws?token="+token, http.Header{
		"X-Hermes-Session-Token": []string{token},
	})
	if err != nil {
		return nil, errors.Join(err, process.Close(context.Background()))
	}

	process.Client = client
	if err := process.waitGatewayReady(readyCtx); err != nil {
		observeHermesStartupStage(ctx, opts.ObserveStartupStage, "session", "readiness", readinessStarted, err)

		return nil, errors.Join(err, process.Close(context.Background()))
	}

	if probeNeeded {
		if err := process.probeGatewayMethods(readyCtx); err != nil {
			observeHermesStartupStage(ctx, opts.ObserveStartupStage, "session", "readiness", readinessStarted, err)

			return nil, errors.Join(err, process.Close(context.Background()))
		}

		markExecutableProbed(executable)
	}

	observeHermesStartupStage(ctx, opts.ObserveStartupStage, "session", "readiness", readinessStarted, nil)

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
	created, err := p.Client.CreateSession(ctx, map[string]any{fieldCwd: p.Home, fieldTitle: "acp-go-hermes startup probe"})
	if err != nil {
		return fmt.Errorf("hermes startup probe session.create failed: %w", err)
	}

	if created.SessionID == "" || created.StoredSessionID == "" {
		return fmt.Errorf("hermes startup probe session.create schema drift")
	}

	if resumed, err := p.Client.ResumeSession(ctx, created.StoredSessionID, map[string]any{}); err != nil {
		if presentErr := methodPresent("session.resume", err); presentErr != nil {
			return presentErr
		}
	} else if resumed.SessionID == "" || resumed.SessionKey == "" {
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

	if err := methodPresent("image.attach_bytes", p.Client.AttachImageBytes(ctx, "__acp_go_hermes_missing_probe__", "AA==", "probe.png")); err != nil {
		return err
	}

	if err := methodPresent("approval.respond", p.Client.ApprovalRespond(ctx, live, "deny", false)); err != nil {
		return err
	}

	if err := methodPresent("clarify.respond", p.Client.ClarifyRespond(ctx, live, "acp-go-hermes-probe", "")); err != nil {
		return err
	}

	if err := p.Client.CloseSession(ctx, live); err != nil {
		return fmt.Errorf("hermes startup probe session.close failed: %w", err)
	}

	if err := p.Client.DeleteSession(ctx, created.StoredSessionID); err != nil {
		if presentErr := methodPresent("session.delete", err); presentErr != nil {
			return presentErr
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
	done := p.beginWait()

	if p.tree != nil {
		_ = p.tree.terminate(p.Cmd)
	} else {
		_ = terminateProcess(p.Cmd)
	}

	var err error

	select {
	case <-done:
		return p.quiesceProcessTree()
	case <-ctx.Done():
		err = ctx.Err()
	case <-afterFn(5 * time.Second):
		err = fmt.Errorf("hermes serve did not exit")
	}

	if p.tree != nil {
		_ = p.tree.kill(p.Cmd)
	} else {
		_ = killProcess(p.Cmd)
	}

	select {
	case <-done:
	case <-afterFn(time.Second):
	}

	return errors.Join(err, p.quiesceProcessTree())
}

// beginWait installs the process's sole waiter as soon as the child starts.
// A Hermes server may exit independently after a provider failure while its
// ACP session remains resident; waiting only from Close would leave that root
// as a zombie until the session was eventually released.
func (p *Process) beginWait() <-chan struct{} {
	p.waitOnce.Do(func() {
		p.waitDone = make(chan struct{})
		waitFn := waitProcessCommand

		go func() {
			_ = waitFn(p.Cmd)
			close(p.waitDone)
		}()
	})

	return p.waitDone
}

func (p *Process) quiesceProcessTree() error {
	if p.tree == nil {
		// Start always installs containment. A nil tree is only possible for
		// package-internal tests that wrap an already-started command.
		return nil
	}

	if err := p.tree.quiesce(5 * time.Second); err != nil {
		return fmt.Errorf("%w: %v", ErrProcessTreeUnproven, err)
	}

	if err := processTreeClose(p.tree); err != nil {
		return fmt.Errorf("close Hermes process containment: %w", err)
	}

	p.tree = nil

	return nil
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

			if event.Type == eventGatewayReady {
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

	addr, _ := ln.Addr().(*net.TCPAddr)

	return addr.Port, nil
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
