//nolint:wsl_v5 // Startup and probe cleanup are linear fail-closed sequences.
package hermes

import (
	"bytes"
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
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

const (
	MinimumVersion = "0.19.0"
	// A cold Hermes gateway may spend more than 15 seconds loading its model
	// catalog before the compatibility sweep reaches model.options.
	defaultProcessTimeout = 60 * time.Second
	fieldCwd              = "cwd"
	fieldTitle            = "title"
	eventGatewayReady     = "gateway.ready"
)

// ErrProcessContainmentIncomplete means the selected native containment
// boundary did not complete. Callers retaining native resources must keep them.
var ErrProcessContainmentIncomplete = errors.New("hermes process containment incomplete")

var (
	commandContext              = exec.CommandContext
	command                     = exec.Command
	listenTCP                   = net.Listen
	randReader                  = rand.Reader
	mkdirTemp                   = os.MkdirTemp
	mkdirAll                    = os.MkdirAll
	removeAll                   = os.RemoveAll
	userHomeDir                 = os.UserHomeDir
	statPath                    = os.Stat
	after                       = time.After
	newStatusHTTPClient         = func() *http.Client { return &http.Client{Timeout: 2 * time.Second} }
	waitProcessCommand          = func(cmd *exec.Cmd) error { return cmd.Wait() }
	processTreeClose            = func(tree *processContainment) error { return tree.close() }
	startHermesContainedProcess = startContainedProcess
	newProcessBrowserShim       = newBrowserShim
	processNativeTreeHandoff    = handoffGeneratedNativeTree
	executableProbeMu           sync.Mutex
	executableProbed            = map[string]bool{}
	versionPattern              = regexp.MustCompile(`v?(\d+)\.(\d+)\.(\d+)`)
)

type ProcessOptions struct {
	ExecutablePath string
	Home           string
	// ScratchParent is the resolved parent directory used to materialize an
	// isolated home when Home is empty. The internal package never consults the
	// system temp directory itself.
	ScratchParent    string
	Cwd              string
	ProviderAuthHome string
	Env              map[string]string
	Isolation        *ProcessIsolation
	// AmbientEnvironment is the adapter's own environment, captured once by the
	// host-facing Agent. Ordinary same-identity execution sanitizes it into the
	// native environment; an explicit policy ignores it entirely, because that
	// policy's BaseEnvironment is a replacement rather than an overlay.
	AmbientEnvironment          map[string]string
	Timeout                     time.Duration
	Configure                   func(*exec.Cmd)
	LogWriter                   io.Writer
	ObserveStartupStage         func(context.Context, string, string, time.Duration, error)
	AcquireDiscoveryResources   func(context.Context) (func(), func(), error)
	RetainDiscoveryRoot         func(string, error)
	DarwinBestEffortContainment bool
}

// ContainmentSpec identifies the wrapper-owned scratch generation used by one
// native launch. Darwin records this identity before spawning.
type ContainmentSpec struct {
	DarwinBestEffort bool
	ScratchParent    string
	GenerationRoot   string
	RuntimeID        string
	LifecycleKind    string
	Isolation        *ProcessIsolation
}

// synchronizedBuffer is used where exec may still be retiring its pipe-copy
// goroutine while containment proof is being collected. Keep bytes.Buffer's
// ReaderFrom method hidden so every copy write takes the same lock as String.
type synchronizedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *synchronizedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buffer.Write(data)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buffer.String()
}

type Process struct {
	Cmd        *exec.Cmd
	Client     *Client
	Home       string
	Port       int
	Token      string
	StatusURL  string
	APIBaseURL string

	cancel context.CancelFunc
	tree   *processContainment
	shim   *browserShim

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

// BrowserLaunchContained reports whether this process runs with the launcher
// shim installed. A login leg must not start a native flow without it: hermes
// opens a browser for the login, and only the shim keeps that launch off the
// operator's desktop.
func (p *Process) BrowserLaunchContained() bool {
	return p != nil && p.shim != nil
}

func Start(ctx context.Context, opts ProcessOptions) (*Process, error) {
	if err := validateProcessContainment(opts.DarwinBestEffortContainment); err != nil {
		return nil, err
	}
	// A supplied policy carries its own Linux-only platform gate, so this is the
	// single place an explicit request is refused for the platform. An omitted
	// policy has nothing to validate: ordinary same-identity execution is what
	// it selects, and every supported platform can perform it.
	if opts.Isolation != nil {
		if err := validateProcessIsolation(opts.Isolation); err != nil {
			return nil, fmt.Errorf("validate Hermes process isolation: %w", err)
		}
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = defaultProcessTimeout
	}

	executable := opts.ExecutablePath
	if executable == "" {
		executable = valHermes
	}

	// The version probe below rebuilds this environment authoritatively and
	// reports an unusable one after it has taken, and can release, the
	// admissions it needs. Resolution here only needs a PATH to find the
	// executable with, so an environment failure is deliberately not fatal yet.
	baseEnvironment, _ := processLaunchEnvironment(opts)

	executable, err := resolveHarnessExecutable(opts.Isolation, executable, baseEnvironment)
	if err != nil {
		return nil, err
	}

	home := opts.Home
	if home == "" {
		home, err = mkdirTemp(opts.ScratchParent, "acp-go-hermes-runtime-")
		if err != nil {
			return nil, err
		}
	}

	if mkdirErr := mkdirAll(home, 0o700); mkdirErr != nil {
		return nil, mkdirErr
	}

	versionCtx, versionCancel := context.WithTimeout(ctx, timeout)
	probeNeeded, err := ensureExecutableVersion(versionCtx, executable, opts)

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

	env := append([]string(nil), baseEnvironment...)

	// PYTHONUNBUFFERED is a launch precondition rather than a preference: off a
	// TTY hermes block-buffers stdout and emits nothing while working normally.
	env = upsertProcessEnv(env, envHermesHome, home)
	env = upsertProcessEnv(env, envHermesSessionToken, token)
	env = upsertProcessEnv(env, "PYTHONUNBUFFERED", "1")
	if opts.ProviderAuthHome != "" {
		env = upsertProcessEnv(env, envHermesAuthHome, opts.ProviderAuthHome)
	}

	// A login runs inside this process, and hermes opens a browser for it even
	// when told not to: --no-browser is accepted and then ignored. The shim
	// shadows every launcher it could exec and points BROWSER at one of those
	// no-ops, because python's webbrowser reads BROWSER but still execs
	// xdg-open on its own. Where no shim can exist the session still starts and
	// the login leg refuses instead; a prompt turn opens nothing.
	shim, err := newProcessBrowserShim(opts.ScratchParent)
	if err != nil {
		cancel()

		return nil, err
	}
	if shim != nil {
		if handoffErr := processNativeTreeHandoff(shim.dir, opts.Isolation); handoffErr != nil {
			cancel()

			return nil, errors.Join(handoffErr, shim.remove())
		}
	}

	cmd.Env = shim.environ(env)
	if opts.LogWriter != nil {
		cmd.Stdout = opts.LogWriter
		cmd.Stderr = opts.LogWriter
	}

	if opts.Configure != nil {
		opts.Configure(cmd)
	}

	configureHermesProcess(cmd)

	spawnStarted := time.Now()

	tree, startErr := startHermesContainedProcess(cmd, ContainmentSpec{
		DarwinBestEffort: opts.DarwinBestEffortContainment,
		ScratchParent:    opts.ScratchParent,
		GenerationRoot:   home,
		LifecycleKind:    containmentSessionKind,
		Isolation:        opts.Isolation,
	})
	if startErr != nil {
		observeHermesStartupStage(ctx, opts.ObserveStartupStage, "session", "spawn", spawnStarted, startErr)
		cancel()

		return nil, errors.Join(startErr, shim.remove())
	}

	observeHermesStartupStage(ctx, opts.ObserveStartupStage, "session", "spawn", spawnStarted, nil)

	process := &Process{
		Cmd:        cmd,
		Home:       home,
		Port:       port,
		Token:      token,
		StatusURL:  "http://127.0.0.1:" + strconv.Itoa(port) + "/api/status",
		APIBaseURL: "http://127.0.0.1:" + strconv.Itoa(port) + "/api",
		cancel:     cancel,
		tree:       tree,
		shim:       shim,
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

// processLaunchEnvironment builds the environment the native harness receives.
// The two modes differ in kind rather than degree: an explicit policy supplies
// the complete replacement environment, and an omitted one sanitizes the
// adapter's captured ambient environment.
func processLaunchEnvironment(opts ProcessOptions) ([]string, error) {
	if opts.Isolation != nil {
		return isolationEnvironment(opts.Isolation, opts.Env)
	}

	return ordinaryEnvironment(opts.AmbientEnvironment, opts.Env)
}

// resolveHarnessExecutable resolves the harness executable against the launch
// environment. Resolution follows the same split as the environment itself: a
// closed policy requires absolute PATH entries because its author wrote the
// whole environment, while an ordinary launch resolves the way a shell would.
func resolveHarnessExecutable(isolation *ProcessIsolation, executable string, environment []string) (string, error) {
	var (
		resolved string
		err      error
	)

	if isolation != nil {
		resolved, err = lookPathInEnvironment(executable, environment)
	} else {
		resolved, err = lookOrdinaryPathInEnvironment(executable, environment)
	}

	if err != nil {
		return "", fmt.Errorf("resolve Hermes executable: %w", err)
	}

	return resolved, nil
}

func upsertProcessEnv(env []string, key string, value string) []string {
	prefix := key + "="
	filtered := env[:0]
	for _, entry := range env {
		if strings.HasPrefix(entry, prefix) {
			continue
		}

		filtered = append(filtered, entry)
	}

	return append(filtered, prefix+value)
}

func ensureExecutableVersion(ctx context.Context, executable string, opts ProcessOptions) (bool, error) {
	executableProbeMu.Lock()
	probed := executableProbed[executable]
	executableProbeMu.Unlock()

	if probed {
		return false, nil
	}
	if opts.AcquireDiscoveryResources == nil || opts.RetainDiscoveryRoot == nil {
		return false, errors.New("hermes version discovery resource callbacks are required")
	}

	nativeRelease, scratchRelease, err := opts.AcquireDiscoveryResources(ctx)
	if err != nil {
		return false, fmt.Errorf("admit Hermes version probe: %w", err)
	}
	if nativeRelease == nil || scratchRelease == nil {
		if nativeRelease != nil {
			nativeRelease()
		}
		if scratchRelease != nil {
			scratchRelease()
		}

		return false, errors.New("hermes version discovery resource callback returned a nil release")
	}

	probeRoot, err := mkdirTemp(opts.ScratchParent, "acp-go-hermes-runtime-")
	if err != nil {
		nativeRelease()
		scratchRelease()

		return false, fmt.Errorf("create Hermes version-probe generation: %w", err)
	}

	var output synchronizedBuffer
	cmd := command(executable, "--version")
	probeEnvironment, envErr := processLaunchEnvironment(opts)
	if envErr != nil {
		nativeRelease()
		removeErr := removeAll(probeRoot)
		if removeErr == nil {
			scratchRelease()
		}

		return false, errors.Join(envErr, removeErr)
	}
	probeEnvironment = upsertProcessEnv(probeEnvironment, envHermesHome, probeRoot)
	if handoffErr := processNativeTreeHandoff(probeRoot, opts.Isolation); handoffErr != nil {
		nativeRelease()
		removeErr := removeAll(probeRoot)
		if removeErr == nil {
			scratchRelease()
		}

		return false, fmt.Errorf("handoff Hermes version-probe generation: %w", errors.Join(handoffErr, removeErr))
	}
	cmd.Env = probeEnvironment
	cmd.Stdout = &output
	cmd.Stderr = &output
	configureHermesProcess(cmd)

	tree, err := startHermesContainedProcess(cmd, ContainmentSpec{
		DarwinBestEffort: opts.DarwinBestEffortContainment,
		ScratchParent:    opts.ScratchParent,
		GenerationRoot:   probeRoot,
		LifecycleKind:    "discovery",
		Isolation:        opts.Isolation,
	})
	if err == nil {
		wait := tree.directChild(cmd)
		select {
		case <-wait.done:
			err = errors.Join(wait.err, tree.complete(5*time.Second))
		case <-ctx.Done():
			err = errors.Join(ctx.Err(), tree.complete(5*time.Second))
		}
	}
	if errors.Is(err, ErrProcessContainmentIncomplete) {
		opts.RetainDiscoveryRoot(probeRoot, err)
	} else {
		nativeRelease()
		removeErr := removeAll(probeRoot)
		err = errors.Join(err, removeErr)
		if removeErr == nil {
			scratchRelease()
		}
	}
	if err != nil {
		return false, fmt.Errorf("hermes --version probe failed: %w: %s", err, output.String())
	}

	version, ok := parseVersion(output.String())
	if !ok {
		return false, fmt.Errorf("hermes --version output missing semantic version: %s", output.String())
	}

	if compareVersions(version, MinimumVersion) < 0 {
		return false, fmt.Errorf("hermes version %s is below minimum %s", version, MinimumVersion)
	}

	return true, nil
}

func markExecutableProbed(executable string) {
	executableProbeMu.Lock()
	executableProbed[executable] = true
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

	if err := methodPresent("image.attach_bytes", p.Client.AttachImageBytes(ctx, "__acp_go_hermes_missing_probe__", []byte{0})); err != nil {
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
		return p.shim.remove()
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
		return errors.Join(p.completeProcessContainment(), p.shim.remove())
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

	fallbackReaped := false
	select {
	case <-done:
		fallbackReaped = true
	case <-afterFn(time.Second):
	}

	hadContainment := p.tree != nil
	containmentErr := p.completeProcessContainment()
	cleanupErr := p.shim.remove()
	if fallbackReaped && hadContainment && containmentErr == nil {
		return cleanupErr
	}

	return errors.Join(err, containmentErr, cleanupErr)
}

// beginWait installs the process's sole waiter as soon as the child starts.
// A Hermes server may exit independently after a provider failure while its
// ACP session remains resident; waiting only from Close would leave that root
// as a zombie until the session was eventually released.
func (p *Process) beginWait() <-chan struct{} {
	p.waitOnce.Do(func() {
		if p.tree != nil {
			p.waitDone = p.tree.directChild(p.Cmd).done
		} else {
			wait := installDirectChildWait(p.Cmd, false)
			p.waitDone = wait.done
		}
	})

	return p.waitDone
}

func (p *Process) completeProcessContainment() error {
	if p.tree == nil {
		// Start always installs containment. A nil tree is only possible for
		// package-internal tests that wrap an already-started command.
		return nil
	}

	if err := p.tree.complete(5 * time.Second); err != nil {
		return fmt.Errorf("%w: %v", ErrProcessContainmentIncomplete, err)
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
		case <-p.waitDone:
			if p.Cmd != nil && p.Cmd.ProcessState != nil {
				return fmt.Errorf("hermes process exited before readiness: %s", p.Cmd.ProcessState)
			}

			return errors.New("hermes process exited before readiness")
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
