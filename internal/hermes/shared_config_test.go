//nolint:govet // Ownership fault tests intentionally use repeated scoped error probes.
package hermes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/coder/acp-go-sdk"
)

func TestSharedHermesConfigSerializesIdenticalWritersAndRejectsMismatch(t *testing.T) {
	home := t.TempDir()
	rawServers := []acp.McpServer{stdioMCPServer("shared", "runner", []string{"serve"}, map[string]string{"TOKEN": "fixture-secret"})}
	servers, secretEnv, err := mcpServersWithSecretEnv(rawServers, nil)
	if err != nil {
		t.Fatal(err)
	}
	if secretEnv["ACP_GO_HERMES_MCP_ENV_1_1"] != "fixture-secret" {
		t.Fatalf("stdio secret environment = %#v", secretEnv)
	}
	files := map[string]string{"instructions.md": "stable"}

	const writers = 8
	errs := make(chan error, writers)
	var group sync.WaitGroup
	for range writers {
		group.Add(1)
		go func() {
			defer group.Done()
			errs <- materializeSharedHermesConfig(t.Context(), home, servers, files)
		}()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("identical concurrent writer: %v", err)
		}
	}

	fingerprint, err := os.ReadFile(filepath.Join(home, sharedConfigFingerprintName))
	if err != nil {
		t.Fatalf("read fingerprint: %v", err)
	}
	if len(fingerprint) != 64 || strings.Contains(string(fingerprint), "fixture-secret") {
		t.Fatalf("fingerprint leaked values or had wrong shape: %q", fingerprint)
	}
	config, err := os.ReadFile(filepath.Join(home, hermesConfigFileName))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(config), "fixture-secret") || !strings.Contains(string(config), "${ACP_GO_HERMES_MCP_ENV_1_1}") {
		t.Fatalf("shared config contains literal stdio secret or lacks reference: %s", config)
	}

	different := []acp.McpServer{stdioMCPServer("shared", "different", nil, nil)}
	if err := materializeSharedHermesConfig(t.Context(), home, different, files); err == nil ||
		!strings.Contains(err.Error(), "different managed configuration") {
		t.Fatalf("different shared config error = %v", err)
	}
}

func TestSharedHermesPathInitDoesNotMutateOperatorConfig(t *testing.T) {
	home := t.TempDir()
	config := []byte("terminal:\n  shell_init_files:\n    - /operator/init\nmodel:\n  provider: custom\n")
	if err := os.WriteFile(filepath.Join(home, hermesConfigFileName), config, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := materializeSharedHermesConfig(t.Context(), home, nil, nil); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(home, hermesConfigFileName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, config) {
		t.Fatalf("shared operator config mutated: got %q, want %q", got, config)
	}
	if script, err := os.ReadFile(filepath.Join(home, hermesPathInitFileName)); err != nil || !bytes.Equal(script, hermesPathInitScript) {
		t.Fatalf("shared managed PATH init = %q err=%v", script, err)
	}
}

func TestSharedHomeMCPSecretsRemainPerProcessWithStablePlaceholderConfig(t *testing.T) {
	home := t.TempDir()
	executable := fakeHermesGatewayExecutable(t, fakeGatewayModeOK)
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	type running struct {
		server  Server
		capture string
		stdio   string
		header  string
	}
	started := make([]running, 0, 2)
	for index, secrets := range []struct{ stdio, header string }{{"stdio-one", "header-one"}, {"stdio-two", "header-two"}} {
		capture := filepath.Join(t.TempDir(), "capture.json")
		servers := []acp.McpServer{
			stdioMCPServer("stdio", "runner", []string{"serve"}, map[string]string{"TOKEN": secrets.stdio}),
			httpMCPServer("http", "https://example.test/mcp", map[string]string{"Authorization": secrets.header}),
		}
		server, err := StartServer(t.Context(), darwinTestStartOptions(t, StartOptions{
			ACPSessionID:     ACPSessionIDString(fmt.Sprintf("mcp-secret-%d", index)),
			Cwd:              t.TempDir(),
			ExecutablePath:   executable,
			SharedHermesHome: home,
			ExistingXDG:      testXDGDirs(t),
			MCPServers:       servers,
			SessionEnv:       map[string]string{"ACP_GO_HERMES_TEST_CAPTURE_MCP_ENV": capture},
			Logger:           logger,
		}))
		if err != nil {
			t.Fatalf("start process %d: %v", index, err)
		}
		started = append(started, running{server: server, capture: capture, stdio: secrets.stdio, header: secrets.header})
	}
	defer func() {
		for _, item := range started {
			_ = item.server.Close(context.Background())
		}
	}()
	for index, item := range started {
		data, err := os.ReadFile(item.capture)
		if err != nil {
			t.Fatal(err)
		}
		var captured map[string]string
		if err := json.Unmarshal(data, &captured); err != nil {
			t.Fatal(err)
		}
		want := map[string]string{
			"ACP_GO_HERMES_MCP_ENV_1_1":    item.stdio,
			"ACP_GO_HERMES_MCP_HEADER_2_1": item.header,
		}
		if !maps.Equal(captured, want) {
			t.Fatalf("process %d MCP env = %#v, want %#v", index, captured, want)
		}
	}
	for _, secret := range []string{"stdio-one", "header-one", "stdio-two", "header-two"} {
		if strings.Contains(logs.String(), secret) {
			t.Fatalf("adapter logs contain MCP secret %q", secret)
		}
		err := filepath.WalkDir(home, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil || entry.IsDir() {
				return walkErr
			}
			data, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			if bytes.Contains(data, []byte(secret)) {
				return fmt.Errorf("shared artifact %s contains MCP secret", filepath.Base(path))
			}

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestSharedHermesConfigLockHonorsCancellation(t *testing.T) {
	home := t.TempDir()
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- withHermesConfigLock(context.Background(), home, func() error {
			close(entered)
			<-release

			return nil
		})
	}()
	<-entered

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := withHermesConfigLock(ctx, home, func() error { return nil }); err == nil {
		t.Fatal("canceled config lock acquisition succeeded")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("release config lock: %v", err)
	}
}

func TestSharedHermesConfigRejectsCrossProcessGlobalMutationModes(t *testing.T) {
	for name, config := range map[string]string{
		"turn isolation true":   "dashboard:\n  turn_isolation: true\n",
		"turn isolation string": "dashboard:\n  turn_isolation: yes\n",
		"turn isolation number": "dashboard:\n  turn_isolation: 1\n",
		"turn isolation null":   "dashboard:\n  turn_isolation: null\n",
		"turn isolation object": "dashboard:\n  turn_isolation: {}\n",
		"turn isolation list":   "dashboard:\n  turn_isolation: []\n",
		"persist switch true":   "model:\n  persist_switch_by_default: true\n",
		"persist switch string": "model:\n  persist_switch_by_default: 'false'\n",
		"persist switch number": "model:\n  persist_switch_by_default: 0\n",
		"both false safe":       "dashboard:\n  turn_isolation: false\nmodel:\n  persist_switch_by_default: false\n",
	} {
		t.Run(name, func(t *testing.T) {
			home := t.TempDir()
			err := materializeSharedHermesConfig(t.Context(), home, nil, map[string]string{hermesConfigFileName: config})
			if strings.Contains(name, "safe") {
				if err != nil {
					t.Fatalf("safe config: %v", err)
				}

				return
			}
			if err == nil || !strings.Contains(err.Error(), "to be exactly false") {
				t.Fatalf("unsafe config error = %v", err)
			}
		})
	}
}

func TestSharedHermesConfigRejectsDotEnvMCPSecretOverrides(t *testing.T) {
	for name, seed := range map[string]string{
		"stdio":  "ACP_GO_HERMES_MCP_ENV_1_1=host-override\n",
		"header": "export acp_go_hermes_mcp_header_1_1=host-override\n",
	} {
		t.Run("existing "+name, func(t *testing.T) {
			home := t.TempDir()
			if err := os.WriteFile(filepath.Join(home, ".env"), []byte(seed), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := materializeSharedHermesConfig(t.Context(), home, nil, nil); err == nil || !strings.Contains(err.Error(), "reserved MCP") {
				t.Fatalf("reserved .env error = %v", err)
			}
		})
		t.Run("seeded "+name, func(t *testing.T) {
			if err := materializeSharedHermesConfig(t.Context(), t.TempDir(), nil, map[string]string{".ENV": seed}); err == nil || !strings.Contains(err.Error(), "reserved MCP") {
				t.Fatalf("reserved seeded .env error = %v", err)
			}
		})
	}
}

func TestSharedHermesHomeBindsExactProbedVersion(t *testing.T) {
	home := t.TempDir()
	if err := bindSharedHermesVersion(t.Context(), home, "0.20.0"); err != nil {
		t.Fatalf("bind version: %v", err)
	}
	if err := bindSharedHermesVersion(t.Context(), home, "0.20.0"); err != nil {
		t.Fatalf("repeat version: %v", err)
	}
	if err := bindSharedHermesVersion(t.Context(), home, "0.20.1"); err == nil || !strings.Contains(err.Error(), "bound to Hermes 0.20.0") {
		t.Fatalf("mixed version error = %v", err)
	}
}

func TestAtomicSharedHermesWriteKeepsCompleteOldBytesOnRenameFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("old-complete\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	previous := sharedAtomicRename
	sharedAtomicRename = func(string, string) error { return errors.New("rename failed") }
	t.Cleanup(func() { sharedAtomicRename = previous })

	if err := atomicSharedHermesWriteFile(path, []byte("new-complete\n"), 0o600); err == nil {
		t.Fatal("atomic write accepted rename failure")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "old-complete\n" {
		t.Fatalf("old bytes changed after failed atomic commit: %q", data)
	}
}

func TestSharedConfigPublishesFingerprintLastAndRetries(t *testing.T) {
	home := t.TempDir()
	previous := sharedAtomicWriteFile
	sharedAtomicWriteFile = func(path string, data []byte, mode os.FileMode) error {
		if filepath.Base(path) == hermesConfigFileName {
			return errors.New("config commit failed")
		}

		return previous(path, data, mode)
	}

	files := map[string]string{
		"instructions.md":    "managed instructions\n",
		hermesConfigFileName: "model:\n  default: fixture\n",
	}
	if err := materializeSharedHermesConfig(t.Context(), home, nil, files); err == nil {
		t.Fatal("materialization accepted config commit failure")
	}
	if _, err := os.Stat(filepath.Join(home, sharedConfigFingerprintName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fingerprint published before config commit: %v", err)
	}
	if data, err := os.ReadFile(filepath.Join(home, "instructions.md")); err != nil || string(data) != "managed instructions\n" {
		t.Fatalf("first managed file was not committed before injected failure: %q err=%v", data, err)
	}
	if _, err := os.Stat(filepath.Join(home, hermesSeedPendingName)); err != nil {
		t.Fatalf("pending recovery journal missing after partial commit: %v", err)
	}

	sharedAtomicWriteFile = previous
	t.Cleanup(func() { sharedAtomicWriteFile = previous })
	if err := materializeSharedHermesConfig(t.Context(), home, nil, files); err != nil {
		t.Fatalf("retry complete materialization: %v", err)
	}
	if _, err := os.Stat(filepath.Join(home, hermesSeedPendingName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pending recovery journal survived completed retry: %v", err)
	}
}

func TestSharedConfigRecoversAfterManifestCommitFailure(t *testing.T) {
	home := t.TempDir()
	previous := sharedAtomicWriteFile
	failManifest := true
	sharedAtomicWriteFile = func(path string, data []byte, mode os.FileMode) error {
		if failManifest && filepath.Base(path) == hermesSeedManifestName {
			return errors.New("manifest commit failed")
		}

		return previous(path, data, mode)
	}
	t.Cleanup(func() { sharedAtomicWriteFile = previous })
	files := map[string]string{"a.md": "a", "b.md": "b"}
	if err := materializeSharedHermesConfig(t.Context(), home, nil, files); err == nil {
		t.Fatal("materialization accepted manifest commit failure")
	}
	for name, want := range files {
		data, err := os.ReadFile(filepath.Join(home, name))
		if err != nil || string(data) != want {
			t.Fatalf("partially committed %s = %q err=%v", name, data, err)
		}
	}
	failManifest = false
	if err := materializeSharedHermesConfig(t.Context(), home, nil, files); err != nil {
		t.Fatalf("retry after manifest failure: %v", err)
	}
	if _, err := os.Stat(filepath.Join(home, hermesSeedPendingName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pending recovery journal survived retry: %v", err)
	}
}

func TestSharedSessionOwnershipIsPerHashedACPIdentity(t *testing.T) {
	home := t.TempDir()
	first, err := acquireSharedACPSessionOwner(home, "same/acp/id")
	if err != nil {
		t.Fatalf("acquire first owner: %v", err)
	}
	defer func() { _ = first.Release() }()

	if _, err := acquireSharedACPSessionOwner(home, "same/acp/id"); err == nil || !strings.Contains(err.Error(), "already active") {
		t.Fatalf("duplicate owner error = %v", err)
	}
	other, err := acquireSharedACPSessionOwner(home, "different/acp/id")
	if err != nil {
		t.Fatalf("different owner: %v", err)
	}
	if err := other.Release(); err != nil {
		t.Fatalf("release different owner: %v", err)
	}

	control, err := SharedHermesAdapterControlDir(home)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(control, sharedSessionOwnersDir))
	if err != nil {
		t.Fatalf("read owner directory: %v", err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), "same") || strings.Contains(entry.Name(), "acp") || strings.Contains(entry.Name(), "id") {
			t.Fatalf("owner filename contains raw ACP identity: %q", entry.Name())
		}
	}

	if err := first.Release(); err != nil {
		t.Fatalf("release first owner: %v", err)
	}
	reacquired, err := acquireSharedACPSessionOwner(home, "same/acp/id")
	if err != nil {
		t.Fatalf("reacquire owner: %v", err)
	}
	if err := reacquired.Release(); err != nil {
		t.Fatalf("release reacquired owner: %v", err)
	}
}

func TestSharedNativeOwnershipConflictsAcrossDifferentACPRecords(t *testing.T) {
	home := t.TempDir()
	first, err := AcquireSharedNativeSessionOwner(home, "same-native-id")
	if err != nil {
		t.Fatalf("acquire native owner: %v", err)
	}
	defer func() { _ = first.Release() }()

	if _, err := AcquireSharedNativeSessionOwner(home, "same-native-id"); err == nil || !strings.Contains(err.Error(), "already active") {
		t.Fatalf("duplicate native owner error = %v", err)
	}
	other, err := AcquireSharedNativeSessionOwner(home, "different-native-id")
	if err != nil {
		t.Fatalf("different native owner: %v", err)
	}
	if err := other.Release(); err != nil {
		t.Fatalf("release different native owner: %v", err)
	}
}

func TestSharedSessionOwnerClaimRefusesExactLiveProcessAndAdmitsStartMismatch(t *testing.T) {
	home := t.TempDir()
	owner, err := AcquireSharedNativeSessionOwner(home, "orphan-native")
	if err != nil {
		t.Fatal(err)
	}
	identity, err := InspectProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.BindProcessIdentity(os.Getpid(), identity.StartTime); err != nil {
		t.Fatal(err)
	}
	// Simulate an adapter crash: its lock description closes without clearing
	// the atomically-published native claimant sidecar.
	if err := errors.Join(owner.unlock(), owner.file.Close()); err != nil {
		t.Fatalf("abandon adapter lock: %v", err)
	}

	if _, err := AcquireSharedNativeSessionOwner(home, "orphan-native"); err == nil || !strings.Contains(err.Error(), "claimant process is still live") {
		t.Fatalf("exact live claimant error = %v", err)
	}
	if err := atomicSharedHermesWriteFile(owner.claimPath, []byte(`{"pid":`+fmt.Sprint(os.Getpid())+`,"kernelStartTime":"reused-pid-start"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	recovered, err := AcquireSharedNativeSessionOwner(home, "orphan-native")
	if err != nil {
		t.Fatalf("start-time mismatch was not recoverable: %v", err)
	}
	if err := recovered.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestSharedSessionOwnerClaimReplacementIsAtomic(t *testing.T) {
	home := t.TempDir()
	owner, err := AcquireSharedNativeSessionOwner(home, "atomic-native")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = owner.Release() }()
	if err := owner.BindProcessIdentity(101, "old-complete"); err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(owner.claimPath)
	if err != nil {
		t.Fatal(err)
	}

	previousRename := sharedAtomicRename
	sharedAtomicRename = func(string, string) error { return errors.New("rename failed") }
	t.Cleanup(func() { sharedAtomicRename = previousRename })
	if err := owner.BindProcessIdentity(202, "new-incomplete"); err == nil {
		t.Fatal("claim replacement accepted failed atomic rename")
	}
	got, err := os.ReadFile(owner.claimPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("claim changed after failed atomic replacement: got %q want %q", got, want)
	}
	sharedAtomicRename = previousRename
}

func TestSharedSessionOwnerIsRetainedWhenStartContainmentIsUnproven(t *testing.T) {
	restoreProcessSeams(t)
	home := t.TempDir()
	t.Cleanup(func() {
		retainedSharedSessionOwners.Lock()
		for key, owner := range retainedSharedSessionOwners.owners {
			if strings.HasPrefix(key, home+string(os.PathSeparator)) {
				_ = owner.Release()
				delete(retainedSharedSessionOwners.owners, key)
			}
		}
		retainedSharedSessionOwners.Unlock()
	})

	startHermesContainedProcess = func(*exec.Cmd, ...ContainmentSpec) (*processContainment, error) {
		return nil, ErrProcessContainmentIncomplete
	}
	options := darwinTestStartOptions(t, StartOptions{
		ACPSessionID:     "failed-start",
		Cwd:              t.TempDir(),
		ExecutablePath:   fakeHermesExecutable(t, fakeProcessModeOK),
		SharedHermesHome: home,
		ExistingXDG:      testXDGDirs(t),
	})
	if _, err := StartServer(t.Context(), options); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("failed start error = %v", err)
	}

	options.ExistingXDG = testXDGDirs(t)
	if _, err := StartServer(t.Context(), options); err == nil || !strings.Contains(err.Error(), "already active") {
		t.Fatalf("second owner after unproven start error = %v", err)
	}
}
