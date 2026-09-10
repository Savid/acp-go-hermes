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
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

const (
	MinimumVersion = "0.21.1"
	argVersion     = "--version"
	// A cold Hermes gateway may spend more than 15 seconds loading its model
	// catalog before the required-method sweep reaches model.options.
	defaultProcessTimeout = 60 * time.Second
	fieldCwd              = "cwd"
	fieldTitle            = "title"
	eventGatewayReady     = "gateway.ready"
	missingProbeSessionID = "__acp_go_hermes_missing_probe__"
)

var (
	listenTCP             = net.Listen
	randReader            = rand.Reader
	mkdirTemp             = os.MkdirTemp
	mkdirAll              = os.MkdirAll
	removeAll             = os.RemoveAll
	after                 = time.After
	newStatusHTTPClient   = func() *http.Client { return &http.Client{Timeout: 2 * time.Second} }
	newProcessBrowserShim = newBrowserShim
	versionPattern        = regexp.MustCompile(`v?(\d+)\.(\d+)\.(\d+)`)
)

var (
	executableProbeMu sync.Mutex
	// sharedExecutableVersionProbeMu serializes the deliberately uncached
	// shared-home probes. Every shared process start re-runs --version so a
	// binary replaced at the same path cannot inherit an earlier verdict.
	sharedExecutableVersionProbeMu sync.Mutex
	// executableProbed records the executables whose --version output has been
	// read and accepted. gatewayProbed records the executables whose gateway
	// answered the startup method sweep. They are separate facts: the version
	// probe spawns its own process and proves the binary, while the sweep proves
	// one live gateway, so a start that never reached readiness must not cost a
	// second --version process next time.
	executableProbed = map[string]bool{}
	gatewayProbed    = map[string]bool{}
	// executableProbes holds the version probe currently in flight per
	// executable, so concurrent starts share one native probe process instead of
	// each spawning their own. The channel is closed when that probe settles.
	executableProbes = map[string]chan struct{}{}
)

type ProcessOptions struct {
	ExecutablePath string
	Home           string
	// SharedHome binds the exact probed Hermes version to Home before launch.
	SharedHome bool
	// PrepareSharedHome runs after a fresh executable version probe and exact
	// home-version binding, but before any serve process is spawned. It is used
	// for the serialized shared config transaction so an unproven or mismatched
	// executable can never mutate the durable residence.
	PrepareSharedHome func(context.Context, string) error
	// ScratchParent is the resolved parent directory used to materialize an
	// isolated home when Home is empty. The internal package never consults the
	// system temp directory itself.
	ScratchParent string
	Cwd           string
	// Env is the static Agent-scoped overlay used for executable lookup,
	// version probing, and as the native base environment.
	Env map[string]string
	// SessionEnv is applied only after executable lookup and version probing.
	SessionEnv            map[string]string
	ExtraPathDirs         []string
	NativeEnvironment     map[string]string
	StartNative           NativeStarter
	PrepareNativeTree     func(context.Context, string) error
	ReclaimNativeTree     func(context.Context, string) error
	RetainNativeTree      func(string, error) bool
	NativeTreeSettled     func()
	ContainmentIncomplete error
	NativeTreeBusy        error
	// AmbientEnvironment is the adapter's own environment, captured once by the
	// host-facing Agent. Ordinary same-identity execution sanitizes it into the
	// native environment; managed execution uses NativeEnvironment instead. The
	// final PATH carrier boundary deliberately scrubs an inherited BASH_ENV.
	AmbientEnvironment  map[string]string
	Timeout             time.Duration
	LogWriter           io.Writer
	ObserveStartupStage func(context.Context, string, string, time.Duration, error)
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

type synchronizedWriter struct {
	mu     sync.Mutex
	writer io.Writer
}

func (w *synchronizedWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.writer.Write(data)
}

type Process struct {
	Client                *Client
	Home                  string
	Port                  int
	Token                 string
	StatusURL             string
	APIBaseURL            string
	native                NativeProcess
	managed               bool
	shim                  *browserShim
	preparedHome          bool
	preparedShim          bool
	shimCleanupPending    bool
	reclaimNativeTree     func(context.Context, string) error
	retainNativeTree      func(string, error) bool
	nativeTreeSettled     func()
	containmentIncomplete error
	nativeTreeBusy        error

	waitMu        sync.Mutex
	waitActive    bool
	waitDone      chan struct{}
	waitCancel    context.CancelFunc
	waitResult    NativeResult
	waitErr       error
	outputMu      sync.Mutex
	outputActive  bool
	outputWorkers int
	outputDone    chan struct{}
	outputStdout  io.ReadCloser
	outputStderr  io.ReadCloser
	outputErr     error
	closeMu       sync.Mutex
}

// BrowserLaunchContained reports whether this process runs with the launcher
// shim installed. A login leg must not start a native flow without it: hermes
// opens a browser for the login, and only the shim keeps that launch off the
// operator's desktop.
func (p *Process) BrowserLaunchContained() bool {
	return p != nil && p.shim != nil
}

// prepareNativeTree relinquishes adapter access before invoking the host. Every
// unsuccessful attempt stays opaque, including busy, cancellation and panic;
// only a successful prepare gives the caller ownership of a later reclaim.
func (opts ProcessOptions) prepareNativeTree(ctx context.Context, root string) (err error) {
	opaque := true
	defer func() {
		if recover() != nil {
			err = errors.New("native tree preparation panicked")
		}
		if opaque {
			err = errors.Join(err, opts.ContainmentIncomplete)
			if opts.RetainNativeTree != nil {
				_ = opts.RetainNativeTree(root, err)
			}
		}
	}()

	err = opts.PrepareNativeTree(ctx, root)
	opaque = err != nil

	return err
}

//nolint:gocyclo // Startup is one ordered native-tree and process transaction.
func Start(ctx context.Context, opts ProcessOptions) (*Process, error) {
	extraPathDirs, err := validatedProcessCarrier(opts)
	if err != nil {
		return nil, err
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = defaultProcessTimeout
	}

	executable := opts.ExecutablePath
	if executable == "" {
		executable = valHermes
	}
	baseEnvironment, err := processLaunchEnvironment(opts)
	if err != nil {
		return nil, err
	}
	if opts.StartNative == nil {
		executable, err = resolveHarnessExecutable(executable, baseEnvironment)
		if err != nil {
			return nil, err
		}
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
	err = ensureExecutableVersion(versionCtx, executable, opts)
	versionCancel()
	if err != nil {
		return nil, err
	}
	if opts.SharedHome && opts.PrepareSharedHome != nil {
		if prepareErr := opts.PrepareSharedHome(ctx, home); prepareErr != nil {
			return nil, prepareErr
		}
	}
	probeNeeded := gatewayMethodProbeNeeded(opts, executable)

	env, err := processSessionLaunchEnvironment(opts)
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
	args := processServeArgs(port)
	env = upsertProcessEnv(env, envHermesHome, home)
	env = upsertProcessEnv(env, envHermesSessionToken, token)
	env = upsertProcessEnv(env, "PYTHONUNBUFFERED", "1")
	if opts.StartNative == nil {
		env = installHermesPathCarrier(env, home, extraPathDirs)
	}

	shim, err := newProcessBrowserShim(opts.ScratchParent)
	if err != nil {
		return nil, err
	}
	process := &Process{
		Home: home, Port: port, Token: token,
		StatusURL:  "http://127.0.0.1:" + strconv.Itoa(port) + "/api/status",
		APIBaseURL: "http://127.0.0.1:" + strconv.Itoa(port) + "/api",
		managed:    opts.StartNative != nil, shim: shim, reclaimNativeTree: opts.ReclaimNativeTree,
		retainNativeTree: opts.RetainNativeTree, nativeTreeSettled: opts.NativeTreeSettled,
		containmentIncomplete: opts.ContainmentIncomplete, nativeTreeBusy: opts.NativeTreeBusy,
	}
	if process.managed {
		if shim != nil {
			if prepareErr := opts.prepareNativeTree(ctx, shim.dir); prepareErr != nil {
				return nil, prepareErr
			}
			process.preparedShim = true
		}
		if prepareErr := opts.prepareNativeTree(ctx, home); prepareErr != nil {
			rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), closeTimeout)
			rollbackErr := process.reclaimAndRemove(rollbackCtx)
			rollbackCancel()
			if process.preparedShim && process.retainNativeTree != nil &&
				process.retainNativeTree(process.shim.dir, rollbackErr) {
				process.preparedShim = false
			}
			if process.shimCleanupPending && process.retainNativeTree != nil &&
				process.retainNativeTree(process.shim.dir, nil) {
				process.shimCleanupPending = false
			}

			return nil, errors.Join(prepareErr, rollbackErr)
		}
		process.preparedHome = true
	}

	starter := opts.StartNative
	if starter == nil {
		starter = startOrdinaryNative
	}
	spawnStarted := time.Now()
	process.native, err = starter(ctx, NativeRequest{
		Executable: executable, Arguments: args,
		Environment: prependPathDirs(shim.environ(env), extraPathDirs), WorkingDirectory: opts.Cwd,
	})
	if err != nil {
		observeHermesStartupStage(ctx, opts.ObserveStartupStage, "session", "spawn", spawnStarted, err)
		if process.managed {
			if errors.Is(err, process.containmentIncomplete) {
				process.retainPreparedTrees(err)

				return nil, err
			}

			rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), closeTimeout)
			rollbackErr := process.rollbackPreparedTrees(rollbackCtx)
			rollbackCancel()
			process.retainPreparedTrees(rollbackErr)
			if rollbackErr != nil {
				return nil, errors.Join(err, rollbackErr)
			}

			return nil, err
		}

		return nil, errors.Join(err, process.reclaimAndRemove(context.Background()))
	}
	if process.native == nil || process.native.Stdin() == nil || process.native.Stdout() == nil || process.native.Stderr() == nil {
		return nil, process.startupFailure(errors.New("native process returned unusable host stdio"))
	}
	process.beginWait()
	process.drainOutput(opts.LogWriter)
	observeHermesStartupStage(ctx, opts.ObserveStartupStage, "session", "spawn", spawnStarted, nil)

	readyCtx, readyCancel := context.WithTimeout(ctx, timeout)
	defer readyCancel()
	readinessStarted := time.Now()
	if readyErr := process.waitReady(readyCtx); readyErr != nil {
		observeHermesStartupStage(ctx, opts.ObserveStartupStage, "session", "readiness", readinessStarted, readyErr)

		return nil, process.startupFailure(readyErr)
	}
	client, err := Dial(readyCtx, "ws://127.0.0.1:"+strconv.Itoa(port)+"/api/ws?token="+token, http.Header{
		"X-Hermes-Session-Token": []string{token},
	})
	if err != nil {
		return nil, process.startupFailure(err)
	}
	process.Client = client
	if err := process.waitGatewayReady(readyCtx); err != nil {
		observeHermesStartupStage(ctx, opts.ObserveStartupStage, "session", "readiness", readinessStarted, err)

		return nil, process.startupFailure(err)
	}
	if probeNeeded {
		if err := process.probeGatewayMethods(readyCtx); err != nil {
			observeHermesStartupStage(ctx, opts.ObserveStartupStage, "session", "readiness", readinessStarted, err)

			return nil, process.startupFailure(err)
		}
		if opts.StartNative == nil {
			markGatewayMethodsProbed(executable)
		}
	}
	observeHermesStartupStage(ctx, opts.ObserveStartupStage, "session", "readiness", readinessStarted, nil)

	return process, nil
}

func gatewayMethodProbeNeeded(opts ProcessOptions, executable string) bool {
	return !opts.SharedHome && (opts.StartNative != nil || !gatewayMethodsProbed(executable))
}

// processLaunchEnvironment builds the environment the native harness receives.
// Managed execution receives a complete authority environment. Ordinary
// execution sanitizes the adapter's captured ambient environment.
func processLaunchEnvironment(opts ProcessOptions) ([]string, error) {
	if opts.StartNative != nil {
		return managedEnvironment(opts.NativeEnvironment, opts.Env)
	}

	return ordinaryEnvironment(opts.AmbientEnvironment, opts.Env)
}

// processSessionLaunchEnvironment applies the session-owned overlay only after
// executable resolution and version probing have consumed the static base
// environment. Both paths rebuild from the captured maps and never consult the
// ambient process environment.
func processSessionLaunchEnvironment(opts ProcessOptions) ([]string, error) {
	if opts.StartNative != nil {
		return managedEnvironment(opts.NativeEnvironment, opts.Env, opts.SessionEnv)
	}

	return ordinaryEnvironment(opts.AmbientEnvironment, opts.Env, opts.SessionEnv)
}

func validatedProcessCarrier(opts ProcessOptions) ([]string, error) {
	dirs, err := cloneAndValidateExtraPathDirs(opts.ExtraPathDirs)
	if err != nil {
		return nil, err
	}
	if sessionEnvErr := validateSessionEnvironmentNoPath(opts.SessionEnv); sessionEnvErr != nil {
		return nil, sessionEnvErr
	}
	if envErr := validatePathCarrierEnvironment(opts.Env); envErr != nil {
		return nil, envErr
	}
	if opts.StartNative != nil {
		if environmentErr := validatePathCarrierEnvironment(opts.NativeEnvironment); environmentErr != nil {
			return nil, environmentErr
		}
	}

	return dirs, nil
}

func processServeArgs(port int) []string {
	args := []string{valServe, "--host", "127.0.0.1", argPort, strconv.Itoa(port)}

	return args
}

func cloneAndValidateExtraPathDirs(dirs []string) ([]string, error) {
	cloned := append([]string(nil), dirs...)
	for index, dir := range cloned {
		switch {
		case dir == "":
			return nil, fmt.Errorf("extra path directory %d is empty", index)
		case !filepath.IsAbs(dir):
			return nil, fmt.Errorf("extra path directory %d is not absolute: %q", index, dir)
		case strings.ContainsRune(dir, os.PathListSeparator):
			return nil, fmt.Errorf("extra path directory %d contains path-list separator %q", index, string(os.PathListSeparator))
		}
	}

	return cloned, nil
}

// prependPathDirs rewrites env with dirs ahead of its existing PATH. Caller
// order and duplicates are preserved, inherited empty components are dropped,
// and an absent base PATH stays absent unless at least one directory is added.
//
// Where names fold, the last spelling in the block supplies the base and every
// spelling is replaced by the single rewritten entry. Splicing the spellings
// together would invent a search order no phase asked for, and keeping one
// alongside the rewrite would leave the child's own deduplication to decide.
func prependPathDirs(env []string, dirs []string) []string {
	kept := make([]string, 0, len(env)+1)
	base := ""

	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok || !processEnvironmentKeyMatches(key, envPath) {
			kept = append(kept, entry)

			continue
		}

		base = value
	}

	parts := append([]string(nil), dirs...)
	for component := range strings.SplitSeq(base, string(os.PathListSeparator)) {
		if component != "" {
			parts = append(parts, component)
		}
	}

	if len(parts) == 0 {
		return kept
	}

	return append(kept, "PATH="+strings.Join(parts, string(os.PathListSeparator)))
}

func processEnvironmentKeyMatches(left string, right string) bool {
	return processEnvironmentKeyMatchesForPlatform(left, right, Platform)
}

func processEnvironmentKeyMatchesForPlatform(left string, right string, platform string) bool {
	if platform == processPlatformWindows {
		return strings.EqualFold(left, right)
	}

	return left == right
}

// processEnvironmentKeysFold reports whether this platform's environment block
// names variables case-insensitively. Windows does, which is why an inherited
// block spells the search path "Path" and a "PATH" written beside it is the
// same variable rather than a second one.
func processEnvironmentKeysFold() bool {
	return processEnvironmentKeyMatchesForPlatform("path", "PATH", Platform)
}

// EnvironmentKey is the name the target platform resolves an environment key
// by: the exact bytes where names are case-sensitive, the upper-cased spelling
// on Windows.
func EnvironmentKey(key string) string {
	if processEnvironmentKeysFold() {
		return strings.ToUpper(key)
	}

	return key
}

// mergeProcessEnvironmentPhases folds the ordered phases a launch environment is
// assembled from — the ambient base, then each overlay — into one block.
//
// Where names fold, a later phase's "PATH" replaces an earlier phase's "Path"
// instead of joining it, so the phase order the caller wrote is the order that
// decides. Leaving both spellings live would hand the decision to whichever one
// the child's environment block happened to keep, and this adapter's own
// executable resolution reads the block before that happens. Two spellings
// inside one phase have no order at all, so they are refused rather than
// resolved by map iteration.
func mergeProcessEnvironmentPhases(phases ...map[string]string) (map[string]string, error) {
	folds := processEnvironmentKeysFold()
	env := make(map[string]string)

	for _, phase := range phases {
		if folds {
			spellings := make(map[string]string, len(phase))
			for key := range phase {
				canonical := strings.ToUpper(key)
				if other, duplicated := spellings[canonical]; duplicated {
					return nil, fmt.Errorf("process environment names %s twice, as %q and %q",
						canonical, min(key, other), max(key, other))
				}

				spellings[canonical] = key
			}
		}

		for key, value := range phase {
			if folds {
				for existing := range env {
					if existing != key && processEnvironmentKeyMatches(existing, key) {
						delete(env, existing)
					}
				}
			}

			env[key] = value
		}
	}

	return env, nil
}

// sortedProcessEnvironment renders an accumulated block as the sorted KEY=VALUE
// slice a launch environment is. Sorting is what makes one built environment
// byte-identical to the next, which several process assertions depend on.
func sortedProcessEnvironment(env map[string]string) []string {
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, key+"="+env[key])
	}

	return out
}

// resolveHarnessExecutable resolves the harness executable against the launch
// environment for ordinary execution. Managed selectors remain logical and
// are resolved by the host.

func resolveHarnessExecutable(executable string, environment []string) (string, error) {
	resolved, err := lookOrdinaryPathInEnvironment(executable, environment)
	if err != nil {
		return "", fmt.Errorf("resolve Hermes executable: %w", err)
	}

	return resolved, nil
}

func managedEnvironment(base map[string]string, overlays ...map[string]string) ([]string, error) {
	if base == nil {
		return nil, errors.New("host authority native environment is unavailable")
	}

	phases := make([]map[string]string, 0, len(overlays)+1)
	for _, phase := range append([]map[string]string{base}, overlays...) {
		sanitized := make(map[string]string, len(phase))
		for key, value := range phase {
			if key == "" || strings.ContainsRune(key, '=') || strings.IndexByte(key, 0) >= 0 || strings.IndexByte(value, 0) >= 0 {
				return nil, fmt.Errorf("process environment contains invalid key %q", key)
			}
			if !scrubOrdinaryEnvironmentKey(key) {
				sanitized[key] = value
			}
		}
		phases = append(phases, sanitized)
	}
	env, err := mergeProcessEnvironmentPhases(phases...)
	if err != nil {
		return nil, err
	}

	return sortedProcessEnvironment(env), nil
}

func upsertProcessEnv(env []string, key string, value string) []string {
	filtered := env[:0]
	for _, entry := range env {
		entryKey, _, ok := strings.Cut(entry, "=")
		if ok && processEnvironmentKeyMatches(entryKey, key) {
			continue
		}

		filtered = append(filtered, entry)
	}

	return append(filtered, key+"="+value)
}

// ensureExecutableVersion proves the harness executable satisfies the minimum.
// Ordinary absolute executables are cached and concurrent starts share their
// probe. Managed logical selectors are authority-scoped, so every managed start
// runs a fresh authority-routed probe.
//
// A waiter answers to its own context throughout. It leaves the moment that
// context ends, without disturbing the probe; and if the probe it joined
// failed, it takes a turn at running one itself rather than inheriting a
// verdict that is routinely about the other caller's abandonment and not about
// the executable.
//
// The executable is marked the moment the version parses and passes, because
// that is precisely what this probe proves. Whether a gateway later answered
// its startup method sweep is a separate fact with its own marker, so a start
// that failed at readiness no longer costs a second --version process.
func ensureExecutableVersion(ctx context.Context, executable string, opts ProcessOptions) error {
	if opts.StartNative != nil {
		return probeExecutableVersion(ctx, executable, opts)
	}

	if opts.SharedHome {
		sharedExecutableVersionProbeMu.Lock()
		defer sharedExecutableVersionProbeMu.Unlock()

		return probeExecutableVersion(ctx, executable, opts)
	}

	for {
		settled, run := beginExecutableVersionProbe(executable)
		if settled == nil {
			return nil
		}

		if run {
			err := probeExecutableVersion(ctx, executable, opts)
			settleExecutableVersionProbe(executable, settled, err)

			return err
		}

		select {
		case <-settled:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// beginExecutableVersionProbe reports the probe this caller must wait on, or a
// nil channel when the executable is already proven. run is true for the single
// caller that must perform it.
func beginExecutableVersionProbe(executable string) (chan struct{}, bool) {
	executableProbeMu.Lock()
	defer executableProbeMu.Unlock()

	if executableProbed[executable] {
		return nil, false
	}

	if settled, inFlight := executableProbes[executable]; inFlight {
		return settled, false
	}

	settled := make(chan struct{})
	executableProbes[executable] = settled

	return settled, true
}

// settleExecutableVersionProbe records what the probe proved and releases every
// waiter. A pass is what marks the executable; a failure leaves it unproven, so
// the next caller through takes its own turn.
func settleExecutableVersionProbe(executable string, settled chan struct{}, err error) {
	executableProbeMu.Lock()

	if err == nil {
		executableProbed[executable] = true
	}

	delete(executableProbes, executable)
	executableProbeMu.Unlock()

	close(settled)
}

func gatewayMethodsProbed(executable string) bool {
	executableProbeMu.Lock()
	defer executableProbeMu.Unlock()

	return gatewayProbed[executable]
}

func markGatewayMethodsProbed(executable string) {
	executableProbeMu.Lock()
	gatewayProbed[executable] = true
	executableProbeMu.Unlock()
}

//nolint:gocyclo // Version probing is one fail-closed process and tree transaction.
func probeExecutableVersion(ctx context.Context, executable string, opts ProcessOptions) error {
	probeRoot, err := mkdirTemp(opts.ScratchParent, "acp-go-hermes-probe-")
	if err != nil {
		return fmt.Errorf("create Hermes version-probe generation: %w", err)
	}
	probeEnvironment, envErr := processLaunchEnvironment(opts)
	if envErr != nil {
		return errors.Join(envErr, removeAll(probeRoot))
	}
	probeEnvironment = upsertProcessEnv(probeEnvironment, envHermesHome, probeRoot)
	managed := opts.StartNative != nil
	if managed {
		if opts.PrepareNativeTree == nil || opts.ReclaimNativeTree == nil {
			return errors.Join(errors.New("host authority tree operations are unavailable"), removeAll(probeRoot))
		}
		if prepareErr := opts.prepareNativeTree(ctx, probeRoot); prepareErr != nil {
			return prepareErr
		}
	}
	starter := opts.StartNative
	if starter == nil {
		starter = startOrdinaryNative
	}
	process, err := starter(ctx, NativeRequest{
		Executable: executable, Arguments: []string{argVersion}, Environment: probeEnvironment,
	})
	if err != nil {
		if managed {
			if errors.Is(err, opts.ContainmentIncomplete) {
				if opts.RetainNativeTree != nil {
					_ = opts.RetainNativeTree(probeRoot, err)
				}

				return fmt.Errorf("hermes --version probe failed: %w", err)
			}

			cleanupErr := reclaimProbeTree(opts, probeRoot, true)
			if cleanupErr != nil {
				err = errors.Join(err, cleanupErr)
			}

			return fmt.Errorf("hermes --version probe failed: %w", err)
		}

		return fmt.Errorf("hermes --version probe failed: %w", errors.Join(err, reclaimProbeTree(opts, probeRoot, managed)))
	}
	var stdin io.WriteCloser

	var stdoutPipe, stderrPipe io.ReadCloser

	if process != nil {
		stdin, stdoutPipe, stderrPipe = process.Stdin(), process.Stdout(), process.Stderr()
	}

	if process == nil || stdin == nil || stdoutPipe == nil || stderrPipe == nil {
		settled, settleErr := settleProbeProcess(process)
		if !settled {
			uncertainErr := errors.Join(
				errors.New("native process returned unusable host stdio"), settleErr,
				opts.ContainmentIncomplete,
			)
			if opts.RetainNativeTree != nil {
				_ = opts.RetainNativeTree(probeRoot, uncertainErr)
			}

			return fmt.Errorf("hermes --version probe failed: %w", uncertainErr)
		}

		return fmt.Errorf("hermes --version probe failed: %w", errors.Join(
			errors.New("native process returned unusable host stdio"), settleErr, reclaimProbeTree(opts, probeRoot, managed),
		))
	}
	_ = stdin.Close()
	var output synchronizedBuffer
	drained := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(&output, stdoutPipe); drained <- struct{}{} }()
	go func() { _, _ = io.Copy(&output, stderrPipe); drained <- struct{}{} }()
	result, waitErr := process.Wait(ctx)
	settled := waitErr == nil || (!managed && ctx.Err() == nil)
	if waitErr != nil {
		revokeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		revokeErr := process.Revoke(revokeCtx)
		cancel()
		waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
		var finalWaitErr error
		result, finalWaitErr = process.Wait(waitCtx)
		waitCancel()
		settled = finalWaitErr == nil || (!managed && !errors.Is(finalWaitErr, context.Canceled) && !errors.Is(finalWaitErr, context.DeadlineExceeded))
		waitErr = errors.Join(waitErr, revokeErr, finalWaitErr)
	}
	var outputErr error
	if settled {
		<-drained
		<-drained
		outputErr = errors.Join(closeOutputStream(stdoutPipe), closeOutputStream(stderrPipe))
	} else {
		outputErr = errors.Join(closeOutputStream(stdoutPipe), closeOutputStream(stderrPipe))
		<-drained
		<-drained
	}
	waitErr = errors.Join(waitErr, outputErr)
	if !settled {
		uncertainErr := errors.Join(waitErr, opts.ContainmentIncomplete)
		if opts.RetainNativeTree != nil {
			_ = opts.RetainNativeTree(probeRoot, uncertainErr)
		}

		return fmt.Errorf("hermes --version probe failed: %w", uncertainErr)
	}

	err = waitErr
	err = errors.Join(err, reclaimProbeTree(opts, probeRoot, managed))
	if result.ExitCode != 0 || result.Signal != 0 || result.Revoked {
		err = errors.Join(err, fmt.Errorf("exit code %d signal %d", result.ExitCode, result.Signal))
	}
	if err != nil {
		return fmt.Errorf("hermes --version probe failed: %w: %s", err, output.String())
	}

	version, ok := parseVersion(output.String())
	if !ok {
		return fmt.Errorf("hermes --version output missing semantic version: %s", output.String())
	}

	if compareVersions(version, MinimumVersion) < 0 {
		return fmt.Errorf("hermes version %s is below minimum %s", version, MinimumVersion)
	}

	return nil
}

func settleProbeProcess(process NativeProcess) (bool, error) {
	if process == nil {
		return false, errors.New("native process is unavailable for settlement")
	}
	revokeCtx, revokeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	revokeErr := process.Revoke(revokeCtx)
	revokeCancel()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, waitErr := process.Wait(waitCtx)
	waitCancel()

	return waitErr == nil, errors.Join(revokeErr, waitErr)
}

func reclaimProbeTree(opts ProcessOptions, root string, managed bool) error {
	if managed {
		reclaimCtx, reclaimCancel := context.WithTimeout(context.Background(), closeTimeout)
		err := opts.ReclaimNativeTree(reclaimCtx, root)
		reclaimCancel()
		if err != nil {
			if opts.RetainNativeTree != nil {
				_ = opts.RetainNativeTree(root, err)
			}

			return err
		}
	}

	removeErr := removeAll(root)
	if removeErr != nil && opts.RetainNativeTree != nil {
		_ = opts.RetainNativeTree(root, nil)
	}

	return removeErr
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

func (p *Process) probeGatewayMethods(ctx context.Context) (returnErr error) {
	created, err := p.Client.CreateSession(ctx, map[string]any{fieldCwd: p.Home, fieldTitle: "acp-go-hermes startup probe"})
	if err != nil {
		return fmt.Errorf("hermes startup probe session.create failed: %w", err)
	}

	if created.SessionID == "" || created.StoredSessionID == "" {
		return fmt.Errorf("hermes startup probe session.create schema drift")
	}
	liveSessions := map[string]struct{}{created.SessionID: {}}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		defer cancel()
		var cleanupErr error
		for liveID := range liveSessions {
			if closeErr := p.Client.CloseSession(cleanupCtx, liveID); closeErr != nil {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("hermes startup probe session.close failed: %w", closeErr))
			}
		}
		if deleteErr := p.Client.DeleteSession(cleanupCtx, created.StoredSessionID); deleteErr != nil {
			if presentErr := methodPresent("session.delete", deleteErr); presentErr != nil {
				cleanupErr = errors.Join(cleanupErr, presentErr)
			}
		}
		returnErr = errors.Join(returnErr, cleanupErr)
	}()

	if resumed, err := p.Client.ResumeSession(ctx, created.StoredSessionID, map[string]any{}); err != nil {
		if presentErr := methodPresent("session.resume", err); presentErr != nil {
			return presentErr
		}
	} else if resumed.SessionID == "" || resumed.StoredKey() == "" {
		return fmt.Errorf("hermes startup probe session.resume schema drift")
	} else {
		liveSessions[resumed.SessionID] = struct{}{}
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

	if err := methodPresent("prompt.submit", p.Client.SubmitPrompt(ctx, missingProbeSessionID, "")); err != nil {
		return err
	}

	if err := methodPresent("image.attach_bytes", p.Client.AttachImageBytes(ctx, missingProbeSessionID, []byte{0})); err != nil {
		return err
	}

	// Presence-only response probes use a missing sentinel. Official Hermes
	// resolves a real session through _sess, which can trigger its deferred full
	// agent build and runtime dependency discovery. Startup must not activate a
	// session merely to prove that these methods exist.
	if err := methodPresent("approval.respond", p.Client.ApprovalRespond(ctx, missingProbeSessionID, "deny")); err != nil {
		return err
	}

	if err := methodPresent("clarify.respond", p.Client.ClarifyRespond(ctx, missingProbeSessionID, "acp-go-hermes-probe", "")); err != nil {
		return err
	}

	// The cold-resume build barrier rides this method, so its absence would turn
	// every session/load into an unbarriered resume rather than a loud failure.
	if err := methodPresent("process.list", p.Client.AwaitSessionBuild(ctx, missingProbeSessionID)); err != nil {
		return err
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
	p.closeMu.Lock()
	defer p.closeMu.Unlock()

	if p.Client != nil {
		_ = p.Client.Close(websocket.StatusNormalClosure, "closing")
	}
	if p.native == nil {
		if p.managed {
			return errors.New("native process is unavailable for settlement")
		}

		return p.reclaimAndRemove(ctx)
	}

	revokeErr := p.native.Revoke(ctx)
	callerErr := ctx.Err()

	waitCtx, waitCancel := context.WithTimeout(context.Background(), closeTimeout)
	if callerErr != nil {
		waitCancel()
	}
	defer waitCancel()

	waitDone := p.beginWait()
	result, waitErr := p.awaitCloseWait(waitDone, waitCtx)
	if p.managed && waitErr != nil {
		p.clearWaitFlight(waitDone)
	}
	outputErr := p.closeOutputDrains()
	if callerErr != nil {
		return errors.Join(revokeErr, callerErr, waitErr, outputErr)
	}
	if result.Revoked && !p.managed {
		var exitErr interface{ ExitCode() int }
		if errors.As(waitErr, &exitErr) {
			waitErr = nil
		}
	}
	if p.managed && waitErr != nil {
		return errors.Join(waitErr, revokeErr, outputErr)
	}
	if waitErr != nil && (errors.Is(waitErr, context.Canceled) || errors.Is(waitErr, context.DeadlineExceeded)) {
		return errors.Join(waitErr, revokeErr, outputErr)
	}

	reclaimCtx, reclaimCancel := context.WithTimeout(context.Background(), closeTimeout)
	defer reclaimCancel()

	reclaimErr := p.reclaimAndRemove(reclaimCtx)
	if reclaimErr != nil {
		return errors.Join(revokeErr, waitErr, outputErr, reclaimErr)
	}

	return errors.Join(waitErr, outputErr)
}

func (p *Process) awaitCloseWait(waitDone <-chan struct{}, waitCtx context.Context) (NativeResult, error) {
	select {
	case <-waitDone:
		return p.waitOutcome()
	case <-waitCtx.Done():
		p.waitMu.Lock()
		cancel := p.waitCancel
		p.waitMu.Unlock()
		if cancel != nil {
			cancel()
		}

		// NativeProcess.Wait owns the context contract. Rejoin the exact flight
		// unconditionally so Close cannot return with this adapter waiter live.
		<-waitDone
		result, waitErr := p.waitOutcome()
		if errors.Is(waitErr, context.Canceled) && !errors.Is(waitErr, p.containmentIncomplete) {
			p.clearWaitFlight(waitDone)

			return NativeResult{}, errors.Join(
				p.containmentFailure("wait for Hermes process", waitCtx.Err()), waitErr,
			)
		}

		return result, waitErr
	}
}

func (p *Process) waitOutcome() (NativeResult, error) {
	p.waitMu.Lock()
	defer p.waitMu.Unlock()

	return p.waitResult, p.waitErr
}

func (p *Process) clearWaitFlight(waitDone <-chan struct{}) {
	p.waitMu.Lock()
	defer p.waitMu.Unlock()
	if p.waitDone != waitDone {
		return
	}

	p.waitActive = false
	p.waitDone = nil
	p.waitCancel = nil
	p.waitResult = NativeResult{}
	p.waitErr = nil
}

// beginWait installs the process's sole waiter as soon as the child starts.
// A Hermes server may exit independently after a provider failure while its
// ACP session remains resident; waiting only from Close would leave that root
// as a zombie until the session was eventually released.
func (p *Process) beginWait() <-chan struct{} {
	p.waitMu.Lock()
	if !p.waitActive {
		waitCtx, waitCancel := context.WithCancel(context.Background())
		p.waitActive = true
		p.waitDone = make(chan struct{})
		p.waitCancel = waitCancel
		done := p.waitDone
		go func() {
			result, err := p.native.Wait(waitCtx)
			p.waitMu.Lock()
			p.waitResult, p.waitErr = result, err
			if p.waitDone == done {
				p.waitCancel = nil
			}
			p.waitMu.Unlock()
			close(done)
		}()
	}
	done := p.waitDone
	p.waitMu.Unlock()

	return done
}

func (p *Process) drainOutput(writer io.Writer) {
	if writer == nil {
		writer = io.Discard
	}
	writer = &synchronizedWriter{writer: writer}

	stdout := p.native.Stdout()
	stderr := p.native.Stderr()
	done := make(chan struct{})
	p.outputMu.Lock()
	p.outputActive = true
	p.outputWorkers = 2
	p.outputDone = done
	p.outputStdout = stdout
	p.outputStderr = stderr
	p.outputMu.Unlock()
	copyOutput := func(stream io.Reader) {
		_, _ = io.Copy(writer, stream)
		p.outputMu.Lock()
		p.outputWorkers--
		if p.outputWorkers == 0 {
			close(done)
		}
		p.outputMu.Unlock()
	}
	go copyOutput(stdout)
	go copyOutput(stderr)
}

func (p *Process) closeOutputDrains() error {
	p.outputMu.Lock()
	if !p.outputActive {
		err := p.outputErr
		p.outputMu.Unlock()

		return err
	}
	stdout, stderr, done := p.outputStdout, p.outputStderr, p.outputDone
	p.outputMu.Unlock()

	closeErr := errors.Join(closeOutputStream(stdout), closeOutputStream(stderr))
	<-done

	p.outputMu.Lock()
	p.outputActive = false
	p.outputStdout = nil
	p.outputStderr = nil
	p.outputErr = errors.Join(p.outputErr, closeErr)
	err := p.outputErr
	p.outputMu.Unlock()

	return err
}

// closeOutputStream releases a parent pipe end once its drain has finished.
// The backend owns both ends of every pipe, so nothing else closes one first
// and a refusal here is a real refusal.
func closeOutputStream(stream io.Closer) error {
	return stream.Close()
}

func (p *Process) startupFailure(cause error) error {
	ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	closeErr := p.Close(ctx)
	cancel()
	if p.managed && (p.preparedShim || p.preparedHome) && closeErr != nil &&
		!p.treeBusy(closeErr) && !errors.Is(closeErr, p.containmentIncomplete) {
		closeErr = p.containmentFailure("settle failed Hermes startup", closeErr)
	}

	p.retainPreparedTrees(closeErr)

	return errors.Join(cause, closeErr)
}

func (p *Process) retainPreparedTrees(err error) {
	if p.retainNativeTree == nil {
		return
	}

	if p.preparedShim && p.shim != nil && p.retainNativeTree(p.shim.dir, err) {
		p.preparedShim = false
	}

	if p.preparedHome && p.retainNativeTree(p.Home, err) {
		p.preparedHome = false
	}

	if p.shimCleanupPending && p.shim != nil && p.retainNativeTree(p.shim.dir, nil) {
		p.shimCleanupPending = false
	}
}

func (p *Process) containmentFailure(operation string, cause error) error {
	if p.containmentIncomplete == nil {
		return fmt.Errorf("native containment incomplete: %s: %w", operation, cause)
	}

	return fmt.Errorf("%w: %s: %w", p.containmentIncomplete, operation, cause)
}

func (p *Process) treeBusy(err error) bool {
	return p.nativeTreeBusy != nil && errors.Is(err, p.nativeTreeBusy)
}

func (p *Process) removeUnpreparedTrees() error {
	var err error
	if p.shim != nil && !p.preparedShim {
		err = p.shim.remove()
	}
	if p.Home != "" && !p.preparedHome {
		err = errors.Join(err, removeAll(p.Home))
	}

	return err
}

// rollbackPreparedTrees unwinds the successfully prepared server trees after
// StartNative refuses admission. A refusal proves that no process handle or
// ambiguous identity remains, so the adapter can reclaim and remove in reverse
// preparation order.
func (p *Process) rollbackPreparedTrees(ctx context.Context) error {
	var err error
	if p.preparedHome {
		if reclaimErr := p.reclaimNativeTree(ctx, p.Home); reclaimErr != nil {
			err = errors.Join(err, reclaimErr)
		} else {
			p.preparedHome = false
			removeErr := removeAll(p.Home)
			if removeErr != nil && p.retainNativeTree != nil {
				_ = p.retainNativeTree(p.Home, nil)
			}
			err = errors.Join(err, removeErr)
		}
	}

	if p.preparedShim && p.shim != nil {
		if reclaimErr := p.reclaimNativeTree(ctx, p.shim.dir); reclaimErr != nil {
			err = errors.Join(err, reclaimErr)
		} else {
			p.preparedShim = false
			removeErr := p.shim.remove()
			if removeErr != nil && p.retainNativeTree != nil {
				_ = p.retainNativeTree(p.shim.dir, nil)
			}
			err = errors.Join(err, removeErr)
		}
	}

	if err == nil && p.nativeTreeSettled != nil {
		p.nativeTreeSettled()
	}

	return err
}

func (p *Process) reclaimAndRemove(ctx context.Context) error {
	var err error
	if p.managed && p.reclaimNativeTree != nil {
		if p.preparedShim && p.shim != nil {
			if reclaimErr := p.reclaimNativeTree(ctx, p.shim.dir); reclaimErr != nil {
				err = errors.Join(err, reclaimErr)
			} else {
				p.preparedShim = false
			}
		}

		if !p.preparedShim && p.shim != nil {
			removeErr := p.shim.remove()
			p.shimCleanupPending = removeErr != nil
			err = errors.Join(err, removeErr)
		}

		if p.preparedHome {
			if reclaimErr := p.reclaimNativeTree(ctx, p.Home); reclaimErr != nil {
				err = errors.Join(err, reclaimErr)
			} else {
				p.preparedHome = false
			}
		}
	} else {
		err = errors.Join(err, p.shim.remove())
	}
	if err == nil && p.managed && p.nativeTreeSettled != nil {
		p.nativeTreeSettled()
	}

	return err
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
			return fmt.Errorf("hermes process exited before readiness: exit code %d signal %d: %w",
				p.waitResult.ExitCode, p.waitResult.Signal, p.waitErr)
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
		case delivery, ok := <-p.Client.Deliveries():
			if !ok {
				return fmt.Errorf("hermes websocket closed before gateway.ready")
			}

			if delivery.Err != nil {
				return delivery.Err
			}

			if delivery.Event != nil && delivery.Event.Type == eventGatewayReady {
				return nil
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
