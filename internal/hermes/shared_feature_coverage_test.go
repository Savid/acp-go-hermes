//go:build unix

package hermes

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	acp "github.com/coder/acp-go-sdk"
	"golang.org/x/sys/unix"
)

func TestSharedGatewayProtocolFaultCoverage(t *testing.T) { //nolint:gocyclo // One stateful fake covers the wire failure matrix.
	t.Run("model rpc admission and failure", func(t *testing.T) {
		fake := newFakeGatewayServer(t)
		client := fake.dialClient(t)
		if err := client.SetModel(t.Context(), "live", "missing-separator"); err == nil {
			t.Fatal("invalid model selection reached config.set")
		}
		fake.setFail("config.set")
		if err := client.SetModel(t.Context(), "live", "provider/model"); err == nil {
			t.Fatal("config.set failure was ignored")
		}
		_ = client.Close(1000, "done")
	})

	t.Run("draft bind cleanup", func(t *testing.T) {
		for _, notFound := range []bool{false, true} {
			fake := newFakeGatewayServer(t)
			if notFound {
				fake.setNotFound("session.close", 1)
			} else {
				fake.setFail("session.close")
			}
			server := newGatewayBackedHermesServer(t, fake, "")
			_, err := server.CreateSessionWithDraft(t.Context(), "draft", func(SessionDraft) error {
				return errors.New("bind failed")
			})
			if err == nil || !strings.Contains(err.Error(), "bind Hermes session draft") {
				t.Fatalf("draft bind cleanup error = %v", err)
			}
		}
	})

	t.Run("persisted inventory faults", func(t *testing.T) {
		fake := newFakeGatewayServer(t)
		fake.setFail("session.list")
		server := newGatewayBackedHermesServer(t, fake, "")
		if _, err := server.PersistedSessions(t.Context()); err == nil {
			t.Fatal("session.list failure was ignored")
		}

		fake = newFakeGatewayServer(t)
		fake.persistedMissingID = true
		server = newGatewayBackedHermesServer(t, fake, "")
		if _, err := server.PersistedSessions(t.Context()); err == nil || !strings.Contains(err.Error(), "missing id") {
			t.Fatalf("missing persisted id error = %v", err)
		}

		fake = newFakeGatewayServer(t)
		fake.setFail("session.list")
		server = newGatewayBackedHermesServer(t, fake, "")
		if _, err := server.CreateSession(t.Context(), "draft"); err == nil || !strings.Contains(err.Error(), "verify persisted") {
			t.Fatalf("persisted draft verification error = %v", err)
		}

		fake = newFakeGatewayServer(t)
		fake.titleDoesNotPersist = true
		server = newGatewayBackedHermesServer(t, fake, "")
		if _, err := server.CreateSession(t.Context(), "draft"); err == nil || !strings.Contains(err.Error(), "durable row") {
			t.Fatalf("missing durable draft error = %v", err)
		}
	})

	t.Run("delete reconciles active runtime faults", func(t *testing.T) {
		fake := newFakeGatewayServer(t)
		fake.setFail("session.active_list")
		server := newGatewayBackedHermesServer(t, fake, "")
		if err := server.DeleteSession(t.Context(), "stored-1"); err == nil || !strings.Contains(err.Error(), "list live") {
			t.Fatalf("active list delete error = %v", err)
		}

		fake = newFakeGatewayServer(t)
		fake.activeNoID = true
		server = newGatewayBackedHermesServer(t, fake, "")
		if err := server.DeleteSession(t.Context(), "stored-1"); err == nil || !strings.Contains(err.Error(), "missing id") {
			t.Fatalf("active id delete error = %v", err)
		}

		fake = newFakeGatewayServer(t)
		fake.setFail("session.close")
		server = newGatewayBackedHermesServer(t, fake, "")
		bindTestGatewaySession(t, server, "stored-1", "live-1")
		if err := server.DeleteSession(t.Context(), "stored-1"); err == nil || !strings.Contains(err.Error(), "close Hermes") {
			t.Fatalf("close-before-delete error = %v", err)
		}

		fake = newFakeGatewayServer(t)
		fake.setNotFound("session.close", 1)
		fake.setNotFound("session.delete", 1)
		server = newGatewayBackedHermesServer(t, fake, "")
		bindTestGatewaySession(t, server, "stored-1", "live-1")
		if err := server.DeleteSession(t.Context(), "stored-1"); err != nil {
			t.Fatalf("not-found delete was not idempotent: %v", err)
		}
	})

	t.Run("branch response cleanup faults", func(t *testing.T) {
		fake := newFakeGatewayServer(t)
		fake.setFail("session.list")
		server := newGatewayBackedHermesServer(t, fake, "")
		if _, err := server.Fork(t.Context(), "stored-1", "marker"); err == nil || !strings.Contains(err.Error(), "list Hermes") {
			t.Fatalf("pre-branch list error = %v", err)
		}

		fake = newFakeGatewayServer(t)
		fake.branchNoSession = true
		server = newGatewayBackedHermesServer(t, fake, "")
		bindTestGatewaySession(t, server, "stored-1", "live-1")
		if _, err := server.ForkWithBaseline(t.Context(), "stored-1", "marker", nil); err == nil || !strings.Contains(err.Error(), "missing session_id") {
			t.Fatalf("missing branch live id error = %v", err)
		}

		fake = newFakeGatewayServer(t)
		fake.branchNoKey = true
		fake.setNotFound("session.close", 1)
		server = newGatewayBackedHermesServer(t, fake, "")
		bindTestGatewaySession(t, server, "stored-1", "live-1")
		if _, err := server.ForkWithBaseline(t.Context(), "stored-1", "marker", nil); err == nil || !strings.Contains(err.Error(), "missing stored_session_id") {
			t.Fatalf("missing branch key error = %v", err)
		}

		fake = newFakeGatewayServer(t)
		fake.setFail("session.close")
		fake.setNotFound("session.delete", 1)
		server = newGatewayBackedHermesServer(t, fake, "")
		bindTestGatewaySession(t, server, "stored-1", "live-1")
		if _, err := server.ForkWithBaseline(t.Context(), "stored-1", "marker", nil); err == nil || !strings.Contains(err.Error(), "close Hermes branch") {
			t.Fatalf("branch detach error = %v", err)
		}

		fake = newFakeGatewayServer(t)
		fake.setNotFound("session.close", 1)
		server = newGatewayBackedHermesServer(t, fake, "")
		bindTestGatewaySession(t, server, "stored-1", "live-1")
		if branch, err := server.ForkWithBaseline(t.Context(), "stored-1", "marker", nil); err != nil || branch.ID != "stored-branch" {
			t.Fatalf("not-found branch detach = %#v, %v", branch, err)
		}
	})

	t.Run("failed branch recovery faults", func(t *testing.T) {
		fake := newFakeGatewayServer(t)
		fake.branchFailAfterSave = true
		fake.setFail("session.list")
		server := newGatewayBackedHermesServer(t, fake, "")
		bindTestGatewaySession(t, server, "stored-1", "live-1")
		if _, err := server.ForkWithBaseline(t.Context(), "stored-1", "marker", nil); err == nil || !strings.Contains(err.Error(), "list durable") {
			t.Fatalf("failed branch inventory error = %v", err)
		}

		fake = newFakeGatewayServer(t)
		fake.branchFailAfterSave = true
		fake.branchWrongTitle = true
		server = newGatewayBackedHermesServer(t, fake, "")
		bindTestGatewaySession(t, server, "stored-1", "live-1")
		if _, err := server.ForkWithBaseline(t.Context(), "stored-1", "marker", nil); !errors.Is(err, ErrBranchRecoveryAmbiguous) {
			t.Fatalf("ambiguous branch error = %v", err)
		}

		fake = newFakeGatewayServer(t)
		fake.branchFailAfterSave = true
		fake.setFail("session.delete")
		server = newGatewayBackedHermesServer(t, fake, "")
		bindTestGatewaySession(t, server, "stored-1", "live-1")
		if _, err := server.ForkWithBaseline(t.Context(), "stored-1", "marker", nil); err == nil || !strings.Contains(err.Error(), "delete failed") {
			t.Fatalf("branch cleanup delete error = %v", err)
		}

		fake = newFakeGatewayServer(t)
		fake.branchFailAfterSave = true
		fake.setFailAfter("session.list", 1)
		server = newGatewayBackedHermesServer(t, fake, "")
		bindTestGatewaySession(t, server, "stored-1", "live-1")
		if _, err := server.ForkWithBaseline(t.Context(), "stored-1", "marker", nil); err == nil || !strings.Contains(err.Error(), "verify failed") {
			t.Fatalf("branch cleanup verify error = %v", err)
		}

		fake = newFakeGatewayServer(t)
		fake.branchFailAfterSave = true
		fake.deleteKeepsBranch = true
		server = newGatewayBackedHermesServer(t, fake, "")
		bindTestGatewaySession(t, server, "stored-1", "live-1")
		if _, err := server.ForkWithBaseline(t.Context(), "stored-1", "marker", nil); err == nil || !strings.Contains(err.Error(), "durable row remains") {
			t.Fatalf("branch cleanup retained-row error = %v", err)
		}
	})

	t.Run("message and server model faults", func(t *testing.T) {
		fake := newFakeGatewayServer(t)
		fake.setFail("config.set")
		server := newGatewayBackedHermesServer(t, fake, "")
		bindTestGatewaySession(t, server, "stored-1", "live-1")
		_, err := server.SendMessage(t.Context(), "stored-1", MessageRequest{
			Model: &ModelSelector{ProviderID: "provider", ModelID: "model"},
		})
		if err == nil || !strings.Contains(err.Error(), "config.set") {
			t.Fatalf("message model error = %v", err)
		}

		fake = newFakeGatewayServer(t)
		fake.setFail("session.resume")
		server = newGatewayBackedHermesServer(t, fake, "")
		if err := server.SetModel(t.Context(), "stored-1", "provider/model"); err == nil || !strings.Contains(err.Error(), "session.resume") {
			t.Fatalf("model resume error = %v", err)
		}

		fake = newFakeGatewayServer(t)
		fake.setNotFound("config.set", 1)
		server = newGatewayBackedHermesServer(t, fake, "")
		bindTestGatewaySession(t, server, "stored-1", "stale-live")
		if err := server.SetModel(t.Context(), "stored-1", "provider/model"); err != nil {
			t.Fatalf("model stale-live retry: %v", err)
		}

		fake = newFakeGatewayServer(t)
		server = newGatewayBackedHermesServer(t, fake, "")
		bindTestGatewaySession(t, server, "stored-1", "live-1")
		if err := server.SetModel(t.Context(), "stored-1", "provider/model"); err != nil {
			t.Fatalf("model success: %v", err)
		}
	})

	t.Run("model helpers and server identity", func(t *testing.T) {
		if ModelSelectionValue("provider", "provider/model") != "provider/model" ||
			ModelSelectionValue("provider", "model") != "provider/model" ||
			ModelSelectionValue("", "model") != "model" || ModelSelectionValue("provider", "") != "provider" {
			t.Fatal("model selection helper branches drifted")
		}
		var nilServer *hermesServer
		if nilServer.ProviderAuthSupported() {
			t.Fatal("nil server advertised provider auth")
		}
	})
}

func TestSharedFilesystemHelperFaultCoverage(t *testing.T) { //nolint:gocyclo // Filesystem faults are kept together to restore global seams safely.
	t.Run("xdg creation faults", func(t *testing.T) {
		originalMkdir := xdgMkdirAll
		t.Cleanup(func() { xdgMkdirAll = originalMkdir })
		xdgMkdirAll = func(string, os.FileMode) error { return errors.New("mkdir fault") }
		if _, err := CreateGenerationXDGDirs(durableTempDir(t)); err == nil {
			t.Fatal("CreateGenerationXDGDirs ignored mkdir fault")
		}
		xdgMkdirAll = originalMkdir
		if _, err := SharedHomeXDGDirs(""); err == nil {
			t.Fatal("SharedHomeXDGDirs accepted empty home")
		}
	})

	t.Run("seed pending and managed-file faults", func(t *testing.T) {
		home := durableTempDir(t)
		if err := applyHermesSeedGuard(home, nil); err != nil {
			t.Fatalf("empty seed guard: %v", err)
		}
		pendingPath := filepath.Join(home, hermesSeedPendingName)
		if err := os.WriteFile(pendingPath, []byte("null\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := loadHermesSeedPending(home); err == nil || !strings.Contains(err.Error(), "empty") {
			t.Fatalf("null pending error = %v", err)
		}
		if err := os.WriteFile(pendingPath, []byte("{bad"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := loadHermesSeedPending(home); err == nil {
			t.Fatal("malformed pending journal was accepted")
		}

		target := filepath.Join(home, "managed")
		if err := os.WriteFile(target, []byte("old"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := writeManagedSeedFileWithWriter(target, []byte("new"), func(path string, _ []byte, _ os.FileMode) error {
			if strings.HasSuffix(path, hermesSeedBackupSuffix) {
				return errors.New("backup fault")
			}

			return nil
		}); err == nil {
			t.Fatal("managed seed ignored backup fault")
		}
		if err := writeManagedSeedFile(target, []byte("old")); err != nil {
			t.Fatalf("identical managed seed: %v", err)
		}
		if err := os.Mkdir(filepath.Join(home, "unreadable"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := writeManagedSeedFile(filepath.Join(home, "unreadable"), []byte("data")); err == nil {
			t.Fatal("directory target was accepted as a managed file")
		}
		parentFile := filepath.Join(home, "parent-file")
		if err := os.WriteFile(parentFile, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := writeManagedSeedFile(filepath.Join(parentFile, "child"), []byte("data")); err == nil {
			t.Fatal("managed seed ignored parent-directory creation fault")
		}
	})

	t.Run("seed transaction faults", func(t *testing.T) {
		badHome := filepath.Join(durableTempDir(t), "parent-file")
		if err := os.WriteFile(badHome, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := applyHermesSeedGuard(badHome, []seedWrite{{relative: "x", target: filepath.Join(badHome, "x"), bytes: []byte("x")}}); err == nil {
			t.Fatal("seed guard ignored invalid home")
		}

		home := durableTempDir(t)
		write := seedWrite{relative: "x", target: filepath.Join(home, "x"), bytes: []byte("x")}
		if err := os.WriteFile(filepath.Join(home, hermesSeedPendingName), []byte(`{"other":"digest"}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := applyHermesSeedGuard(home, []seedWrite{write}); err == nil || !strings.Contains(err.Error(), "does not match") {
			t.Fatalf("pending mismatch error = %v", err)
		}

		home = durableTempDir(t)
		if err := applyHermesSeedGuardWithWriter(home, []seedWrite{{relative: "x", target: filepath.Join(home, "x"), bytes: []byte("x")}}, func(path string, _ []byte, _ os.FileMode) error {
			if strings.HasSuffix(path, hermesSeedPendingName) {
				return errors.New("pending write fault")
			}

			return nil
		}); err == nil || !strings.Contains(err.Error(), "pending write fault") {
			t.Fatalf("pending write error = %v", err)
		}

		home = durableTempDir(t)
		if err := os.Mkdir(filepath.Join(home, hermesSeedPendingName), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := applyHermesSeedGuard(home, []seedWrite{{relative: "x", target: filepath.Join(home, "x"), bytes: []byte("x")}}); err == nil {
			t.Fatal("pending journal directory was accepted")
		}

		home = durableTempDir(t)
		if err := os.WriteFile(filepath.Join(home, hermesSeedPendingName), []byte("{\"x\":\"2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881\"}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(filepath.Join(home, "x"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := applyHermesSeedGuard(home, []seedWrite{{relative: "x", target: filepath.Join(home, "x"), bytes: []byte("x")}}); err == nil {
			t.Fatal("unreadable pending target was accepted")
		}

		home = durableTempDir(t)
		if err := os.Mkdir(filepath.Join(home, hermesSeedPendingName), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(home, hermesSeedPendingName, "entry"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := clearHermesSeedPending(home); err == nil {
			t.Fatal("non-empty pending directory was cleared")
		}
	})

	t.Run("seed primitive wrappers", func(t *testing.T) {
		home := durableTempDir(t)
		if err := saveHermesSeedManifest(home, map[string]bool{"x": true}); err != nil {
			t.Fatal(err)
		}
		if err := clearHermesSeedPending(home); err != nil {
			t.Fatal(err)
		}
		if equalStringMaps(map[string]string{"x": "1"}, map[string]string{}) ||
			equalStringMaps(map[string]string{"x": "1"}, map[string]string{"x": "2"}) {
			t.Fatal("unequal seed maps compared equal")
		}
		if !equalStringMaps(map[string]string{"x": "1"}, map[string]string{"x": "1"}) {
			t.Fatal("equal seed maps compared unequal")
		}
		if redacted, err := RedactedMCPServers(nil); err != nil || len(redacted) != 0 {
			t.Fatalf("redact empty MCP servers = %#v, %v", redacted, err)
		}
		servers := []acp.McpServer{{Stdio: &acp.McpServerStdio{
			Name: "stdio",
			Env:  []acp.EnvVariable{{Name: "TOKEN", Value: "secret"}},
		}}}
		if _, _, err := mcpServersWithSecretEnv(servers, map[string]string{"ACP_GO_HERMES_MCP_ENV_1_1": "occupied"}); err == nil {
			t.Fatal("reserved stdio MCP environment collision was accepted")
		}

		originalMarshal := hermesMarshalIndent
		t.Cleanup(func() { hermesMarshalIndent = originalMarshal })
		hermesMarshalIndent = func(any, string, string) ([]byte, error) { return nil, errors.New("marshal fault") }
		if err := saveHermesSeedManifest(durableTempDir(t), map[string]bool{"x": true}); err == nil {
			t.Fatal("seed manifest marshal fault was ignored")
		}
		if err := saveHermesSeedPendingWithWriter(durableTempDir(t), map[string]string{"x": "digest"}, os.WriteFile); err == nil {
			t.Fatal("seed pending marshal fault was ignored")
		}
		hermesMarshalIndent = originalMarshal
	})
}

func TestSharedStartServerEarlyFaultCoverage(t *testing.T) {
	badParent := filepath.Join(durableTempDir(t), "file")
	if err := os.WriteFile(badParent, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	options := darwinTestStartOptions(t, StartOptions{
		ACPSessionID:     "shared-xdg-fault",
		Cwd:              durableTempDir(t),
		ExecutablePath:   fakeHermesExecutable(t, fakeProcessModeOK),
		SharedHermesHome: filepath.Join(badParent, "home"),
		ExistingXDG:      testXDGDirs(t),
	})
	if server, err := StartServer(t.Context(), options); err == nil || server != nil {
		t.Fatalf("invalid shared XDG = %#v, %v", server, err)
	}

	homeForLocality := durableTempDir(t)
	options = darwinTestStartOptions(t, StartOptions{
		ACPSessionID:     "shared-locality-fault",
		Cwd:              durableTempDir(t),
		ExecutablePath:   fakeHermesExecutable(t, fakeProcessModeOK),
		SharedHermesHome: homeForLocality,
		ExistingXDG:      testXDGDirs(t),
	})
	originalLocalValidator := sharedHomeLocalValidator
	t.Cleanup(func() { sharedHomeLocalValidator = originalLocalValidator })
	sharedHomeLocalValidator = func(string) error { return errors.New("locality fault") }
	if server, err := StartServer(t.Context(), options); err == nil || server != nil || !strings.Contains(err.Error(), "locality fault") {
		t.Fatalf("shared locality fault = %#v, %v", server, err)
	}
	sharedHomeLocalValidator = originalLocalValidator

	home := durableTempDir(t)
	options = darwinTestStartOptions(t, StartOptions{
		ACPSessionID:     "shared-release-fault",
		Cwd:              durableTempDir(t),
		ExecutablePath:   fakeHermesExecutable(t, fakeProcessModeOK),
		SharedHermesHome: home,
		ExistingXDG:      testXDGDirs(t),
	})
	originalMkdir := hermesControlMkdir
	t.Cleanup(func() { hermesControlMkdir = originalMkdir })
	hermesControlMkdir = func(string, os.FileMode) error { return errors.New("control fault") }
	_, err := StartServer(t.Context(), options)
	hermesControlMkdir = originalMkdir
	if err == nil || !strings.Contains(err.Error(), "control fault") {
		t.Fatalf("shared control failure = %v", err)
	}
	owner, ownerErr := AcquireSharedACPSessionOwner(home, "shared-release-fault")
	if ownerErr != nil {
		t.Fatalf("early StartServer failure retained owner: %v", ownerErr)
	}
	if err := owner.Release(); err != nil {
		t.Fatal(err)
	}
	homeOwner, homeErr := AcquireSharedHomeOwner(home)
	if homeErr != nil {
		t.Fatalf("early StartServer failure retained the home root: %v", homeErr)
	}
	if err := homeOwner.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestSharedOwnerFaultCoverage(t *testing.T) {
	originalChmod := sharedOwnerChmod
	originalFileChmod := sharedOwnerFileChmod
	originalTryLock := sharedOwnerTryLock
	t.Cleanup(func() {
		sharedOwnerChmod = originalChmod
		sharedOwnerFileChmod = originalFileChmod
		sharedOwnerTryLock = originalTryLock
	})

	t.Run("input and owner directory", func(t *testing.T) {
		if _, err := AcquireSharedNativeSessionOwner(durableTempDir(t), ""); err == nil {
			t.Fatal("empty native owner id was accepted")
		}
		if _, err := AcquireSharedNativeSessionOwner("relative", "native"); err == nil {
			t.Fatal("relative owner home was accepted")
		}

		home := durableTempDir(t)
		control, err := EnsureSharedHermesAdapterControlDir(home)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(control, sharedSessionOwnersDir), []byte("blocked"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := AcquireSharedNativeSessionOwner(home, "native"); err == nil || !strings.Contains(err.Error(), "directory") {
			t.Fatalf("blocked owner directory error = %v", err)
		}
	})

	t.Run("lock open and low-level flock faults", func(t *testing.T) {
		home := durableTempDir(t)
		owner, acquireErr := AcquireSharedNativeSessionOwner(home, "native")
		if acquireErr != nil {
			t.Fatal(acquireErr)
		}
		lockPath := owner.lockPath
		if err := owner.Release(); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(lockPath, 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := AcquireSharedNativeSessionOwner(home, "native"); err == nil || !strings.Contains(err.Error(), "open") {
			t.Fatalf("owner lock directory error = %v", err)
		}

		file, createErr := os.CreateTemp(durableTempDir(t), "closed-owner-lock")
		if createErr != nil {
			t.Fatal(createErr)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := tryLockHermesFile(file); err == nil {
			t.Fatal("closed owner file acquired flock")
		}
		file, createErr = os.CreateTemp(durableTempDir(t), "owner-unlock")
		if createErr != nil {
			t.Fatal(createErr)
		}
		unlock, acquired, lockErr := tryLockHermesFile(file)
		if lockErr != nil || !acquired {
			t.Fatalf("acquire low-level owner lock: %v", lockErr)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		if err := unlock(); err == nil {
			t.Fatal("closed owner file unlock succeeded")
		}
	})

	t.Run("typed owner operation faults", func(t *testing.T) {
		sharedOwnerChmod = func(string, os.FileMode) error { return errors.New("directory chmod fault") }
		if _, err := AcquireSharedNativeSessionOwner(durableTempDir(t), "native"); err == nil || !strings.Contains(err.Error(), "protect") {
			t.Fatalf("owner directory chmod fault = %v", err)
		}
		sharedOwnerChmod = originalChmod

		sharedOwnerFileChmod = func(*os.File, os.FileMode) error { return errors.New("file chmod fault") }
		if _, err := AcquireSharedNativeSessionOwner(durableTempDir(t), "native"); err == nil || !strings.Contains(err.Error(), "protect") {
			t.Fatalf("owner file chmod fault = %v", err)
		}
		sharedOwnerFileChmod = originalFileChmod

		sharedOwnerTryLock = func(*os.File) (func() error, bool, error) {
			return nil, false, errors.New("flock fault")
		}
		if _, err := AcquireSharedNativeSessionOwner(durableTempDir(t), "native"); err == nil || !strings.Contains(err.Error(), "flock fault") {
			t.Fatalf("owner lock fault = %v", err)
		}
		sharedOwnerTryLock = originalTryLock
	})
}

func TestSharedSessionSetLockFaultCoverage(t *testing.T) { //nolint:gocyclo // Lock-path faults share one validated residence.
	originalLocalValidator := sharedHomeLocalValidator
	originalFileChmod := sharedSessionSetFileChmod
	originalTryLock := sharedSessionSetTryLock
	originalLstat := sharedSessionSetLstat
	originalChmod := sharedSessionSetChmod
	originalSyncDir := sharedSessionSetSyncDir
	t.Cleanup(func() {
		sharedHomeLocalValidator = originalLocalValidator
		sharedSessionSetFileChmod = originalFileChmod
		sharedSessionSetTryLock = originalTryLock
		sharedSessionSetLstat = originalLstat
		sharedSessionSetChmod = originalChmod
		sharedSessionSetSyncDir = originalSyncDir
	})

	if _, err := SharedHermesAdapterControlDir(filepath.Join(durableTempDir(t), "missing")); err == nil {
		t.Fatal("missing shared home resolved")
	}
	if _, err := EnsureSharedHermesAdapterControlDir("relative"); err == nil {
		t.Fatal("relative control home was accepted")
	}
	if err := EnsureLocalSharedHermesHome(filepath.Join(durableTempDir(t), "missing")); err == nil {
		t.Fatal("missing local home passed filesystem validation")
	}
	badParent := filepath.Join(durableTempDir(t), "file")
	if err := os.WriteFile(badParent, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ensureSharedHermesAdapterControlDir(filepath.Join(badParent, "child")); err == nil || !strings.Contains(err.Error(), "create") {
		t.Fatalf("control directory invalid-parent error = %v", err)
	}

	home := durableTempDir(t)
	control, controlErr := EnsureSharedHermesAdapterControlDir(home)
	if controlErr != nil {
		t.Fatal(controlErr)
	}
	lockPath := filepath.Join(control, sharedSessionSetLockName)
	if err := os.Mkdir(lockPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireSharedSessionSetLock(t.Context(), home, SharedSessionSetLockExclusive); err == nil || !strings.Contains(err.Error(), "open") {
		t.Fatalf("session-set lock directory error = %v", err)
	}

	file, createErr := os.CreateTemp(durableTempDir(t), "closed-session-set")
	if createErr != nil {
		t.Fatal(createErr)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := tryLockSharedSessionSetFile(file, SharedSessionSetLockShared); err == nil {
		t.Fatal("closed session-set file acquired lock")
	}
	file, createErr = os.CreateTemp(durableTempDir(t), "session-set-unlock")
	if createErr != nil {
		t.Fatal(createErr)
	}
	unlock, acquired, lockErr := tryLockSharedSessionSetFile(file, SharedSessionSetLockExclusive)
	if lockErr != nil || !acquired {
		t.Fatalf("acquire low-level session-set lock: %v", lockErr)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := unlock(); err == nil {
		t.Fatal("closed session-set file unlock succeeded")
	}

	if err := (*SharedSessionSetLock)(nil).Release(); err != nil {
		t.Fatal(err)
	}
	closed, closedCreateErr := os.CreateTemp(durableTempDir(t), "release")
	if closedCreateErr != nil {
		t.Fatal(closedCreateErr)
	}
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	lock := &SharedSessionSetLock{file: closed, unlock: func() error { return errors.New("unlock fault") }}
	if err := lock.Release(); err == nil || !strings.Contains(err.Error(), "unlock fault") {
		t.Fatalf("release fault = %v", err)
	}

	t.Run("typed control and lock faults", func(t *testing.T) {
		sharedHomeLocalValidator = func(string) error { return errors.New("locality fault") }
		if _, err := EnsureSharedHermesAdapterControlDir(durableTempDir(t)); err == nil || !strings.Contains(err.Error(), "locality fault") {
			t.Fatalf("control locality fault = %v", err)
		}
		sharedHomeLocalValidator = originalLocalValidator

		sharedSessionSetFileChmod = func(*os.File, os.FileMode) error { return errors.New("file chmod fault") }
		if _, err := AcquireSharedSessionSetLock(t.Context(), durableTempDir(t), SharedSessionSetLockExclusive); err == nil || !strings.Contains(err.Error(), "protect") {
			t.Fatalf("session-set file chmod fault = %v", err)
		}
		sharedSessionSetFileChmod = originalFileChmod

		sharedSessionSetTryLock = func(*os.File, SharedSessionSetLockMode) (func() error, bool, error) {
			return nil, false, errors.New("flock fault")
		}
		if _, err := AcquireSharedSessionSetLock(t.Context(), durableTempDir(t), SharedSessionSetLockExclusive); err == nil || !strings.Contains(err.Error(), "flock fault") {
			t.Fatalf("session-set flock fault = %v", err)
		}
		sharedSessionSetTryLock = originalTryLock

		sharedSessionSetLstat = func(string) (os.FileInfo, error) { return nil, errors.New("lstat fault") }
		if err := ensureSharedHermesAdapterControlDir(filepath.Join(durableTempDir(t), "control")); err == nil || !strings.Contains(err.Error(), "inspect") {
			t.Fatalf("control lstat fault = %v", err)
		}
		sharedSessionSetLstat = originalLstat

		sharedSessionSetChmod = func(string, os.FileMode) error { return errors.New("chmod fault") }
		if err := ensureSharedHermesAdapterControlDir(filepath.Join(durableTempDir(t), "control")); err == nil || !strings.Contains(err.Error(), "protect") {
			t.Fatalf("control chmod fault = %v", err)
		}
		sharedSessionSetChmod = originalChmod

		sharedSessionSetSyncDir = func(string) error { return errors.New("sync fault") }
		if err := ensureSharedHermesAdapterControlDir(filepath.Join(durableTempDir(t), "control")); err == nil || !strings.Contains(err.Error(), "sync") {
			t.Fatalf("control sync fault = %v", err)
		}
		sharedSessionSetSyncDir = originalSyncDir
	})
}

func TestSharedAtomicWriteFaultCoverage(t *testing.T) {
	badParent := filepath.Join(durableTempDir(t), "file")
	if err := os.WriteFile(badParent, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := atomicSharedHermesWriteFile(filepath.Join(badParent, "child"), []byte("x"), 0o600); err == nil {
		t.Fatal("atomic write ignored invalid parent")
	}

	originalCreate := sharedAtomicCreateTemp
	originalRename := sharedAtomicRename
	originalSync := sharedAtomicFileSync
	originalClose := sharedAtomicFileClose
	t.Cleanup(func() {
		sharedAtomicCreateTemp = originalCreate
		sharedAtomicRename = originalRename
		sharedAtomicFileSync = originalSync
		sharedAtomicFileClose = originalClose
	})
	sharedAtomicCreateTemp = func(string, string) (*os.File, error) { return nil, errors.New("create fault") }
	if err := atomicSharedHermesWriteFile(filepath.Join(durableTempDir(t), "x"), nil, 0o600); err == nil || !strings.Contains(err.Error(), "create") {
		t.Fatalf("atomic create fault = %v", err)
	}

	closed, closedCreateErr := os.CreateTemp(durableTempDir(t), "closed-atomic")
	if closedCreateErr != nil {
		t.Fatal(closedCreateErr)
	}
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	sharedAtomicCreateTemp = func(string, string) (*os.File, error) { return closed, nil }
	if err := atomicSharedHermesWriteFile(filepath.Join(durableTempDir(t), "x"), nil, 0o600); err == nil {
		t.Fatal("atomic write ignored closed temporary")
	}

	directory, openErr := os.Open(durableTempDir(t))
	if openErr != nil {
		t.Fatal(openErr)
	}
	sharedAtomicCreateTemp = func(string, string) (*os.File, error) { return directory, nil }
	if err := atomicSharedHermesWriteFile(filepath.Join(durableTempDir(t), "x"), []byte("x"), 0o600); err == nil {
		t.Fatal("atomic write ignored directory temporary")
	}

	reader, writer, pipeErr := os.Pipe()
	if pipeErr != nil {
		t.Fatal(pipeErr)
	}
	t.Cleanup(func() { _ = reader.Close(); _ = writer.Close() })
	pipeFD, dupErr := unix.Dup(int(writer.Fd()))
	if dupErr != nil {
		t.Fatal(dupErr)
	}
	pipeAlias := os.NewFile(uintptr(pipeFD), filepath.Join(durableTempDir(t), "pipe-temp"))
	sharedAtomicCreateTemp = func(string, string) (*os.File, error) { return pipeAlias, nil }
	if err := atomicSharedHermesWriteFile(filepath.Join(durableTempDir(t), "x"), []byte("x"), 0o600); err == nil {
		t.Fatal("atomic write ignored pipe sync fault")
	}

	sharedAtomicCreateTemp = originalCreate
	sharedAtomicRename = func(oldPath, _ string) error {
		_ = os.Remove(oldPath)

		return errors.New("rename fault")
	}
	if err := atomicSharedHermesWriteFile(filepath.Join(durableTempDir(t), "x"), []byte("x"), 0o600); err == nil || !strings.Contains(err.Error(), "rename") {
		t.Fatalf("atomic rename fault = %v", err)
	}
	sharedAtomicRename = originalRename
	sharedAtomicFileSync = func(*os.File) error { return errors.New("sync fault") }
	if err := atomicSharedHermesWriteFile(filepath.Join(durableTempDir(t), "x"), []byte("x"), 0o600); err == nil || !strings.Contains(err.Error(), "sync fault") {
		t.Fatalf("atomic sync fault = %v", err)
	}
	sharedAtomicFileSync = originalSync
	sharedAtomicFileClose = func(file *os.File) error {
		_ = file.Close()

		return errors.New("close fault")
	}
	if err := atomicSharedHermesWriteFile(filepath.Join(durableTempDir(t), "x"), []byte("x"), 0o600); err == nil || !strings.Contains(err.Error(), "close fault") {
		t.Fatalf("atomic close fault = %v", err)
	}
	sharedAtomicFileClose = originalClose
	if err := syncSharedHermesDirectory(filepath.Join(durableTempDir(t), "missing")); err == nil {
		t.Fatal("directory sync ignored missing directory")
	}

	source, sourceErr := os.CreateTemp(durableTempDir(t), "atomic-source")
	if sourceErr != nil {
		t.Fatal(sourceErr)
	}
	t.Cleanup(func() { _ = source.Close() })
	duplicateFD, duplicateErr := unix.Dup(int(source.Fd()))
	if duplicateErr != nil {
		t.Fatal(duplicateErr)
	}
	nonEmptyDir := filepath.Join(durableTempDir(t), "non-empty")
	if err := os.Mkdir(nonEmptyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nonEmptyDir, "entry"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	alias := os.NewFile(uintptr(duplicateFD), nonEmptyDir)
	sharedAtomicCreateTemp = func(string, string) (*os.File, error) { return alias, nil }
	sharedAtomicRename = func(string, string) error { return nil }
	if err := atomicSharedHermesWriteFile(filepath.Join(durableTempDir(t), "target"), []byte("x"), 0o600); err == nil {
		t.Fatal("atomic write ignored temporary cleanup failure")
	}
}

func TestSharedConfigFaultCoverage(t *testing.T) { //nolint:gocyclo // Serialized config faults share global writer seams.
	t.Run("existing config and dotenv validation", func(t *testing.T) {
		home := durableTempDir(t)
		if err := os.WriteFile(filepath.Join(home, hermesConfigFileName), []byte("dashboard:\n  turn_isolation: true\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := materializeSharedHermesConfig(t.Context(), home, nil, nil); err == nil {
			t.Fatal("unsafe existing config was accepted")
		}
		if err := os.WriteFile(filepath.Join(home, hermesConfigFileName), []byte("[malformed"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := validateSharedHermesConfigFile(home); err == nil || !strings.Contains(err.Error(), "parse") {
			t.Fatalf("malformed config error = %v", err)
		}
		if err := os.Remove(filepath.Join(home, hermesConfigFileName)); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(filepath.Join(home, hermesConfigFileName), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := validateSharedHermesConfigFile(home); err == nil || !strings.Contains(err.Error(), "read") {
			t.Fatalf("config directory read error = %v", err)
		}

		dotEnvHome := durableTempDir(t)
		if err := validateSharedHermesDotEnv(dotEnvHome, map[string]string{".env": "SAFE=value\n"}); err != nil {
			t.Fatalf("safe dotenv: %v", err)
		}
		if err := os.Mkdir(filepath.Join(dotEnvHome, ".env"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := validateSharedHermesDotEnv(dotEnvHome, nil); err == nil || !strings.Contains(err.Error(), "read") {
			t.Fatalf("dotenv directory read error = %v", err)
		}
	})

	t.Run("fingerprint read and publication faults", func(t *testing.T) {
		home := durableTempDir(t)
		if err := os.Mkdir(filepath.Join(sharedTestControlDir(t, home), sharedConfigFingerprintName), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := materializeSharedHermesConfig(t.Context(), home, nil, nil); err == nil || !strings.Contains(err.Error(), "read shared Hermes config fingerprint") {
			t.Fatalf("fingerprint read error = %v", err)
		}

		originalWriter := sharedAtomicWriteFile
		t.Cleanup(func() { sharedAtomicWriteFile = originalWriter })
		home = durableTempDir(t)
		sharedAtomicWriteFile = func(path string, data []byte, mode os.FileMode) error {
			if filepath.Base(path) == sharedConfigFingerprintName {
				return errors.New("fingerprint fault")
			}

			return originalWriter(path, data, mode)
		}
		if err := materializeSharedHermesConfig(t.Context(), home, nil, map[string]string{"x": "y"}); err == nil || !strings.Contains(err.Error(), "commit shared Hermes config fingerprint") {
			t.Fatalf("fingerprint publication error = %v", err)
		}
		sharedAtomicWriteFile = originalWriter
	})

	t.Run("fingerprint encoding and config lock faults", func(t *testing.T) {
		originalMarshal := sharedConfigJSONMarshal
		originalTryLock := sharedConfigTryLock
		t.Cleanup(func() {
			sharedConfigJSONMarshal = originalMarshal
			sharedConfigTryLock = originalTryLock
		})

		sharedConfigJSONMarshal = func(any) ([]byte, error) { return nil, errors.New("JSON fault") }
		if err := materializeSharedHermesConfig(t.Context(), durableTempDir(t), nil, nil); err == nil || !strings.Contains(err.Error(), "encode") {
			t.Fatalf("shared fingerprint encoding fault = %v", err)
		}
		sharedConfigJSONMarshal = originalMarshal

		sharedConfigTryLock = func(*os.File) (func() error, bool, error) {
			return nil, false, errors.New("lock fault")
		}
		if err := withHermesConfigLock(t.Context(), durableTempDir(t), func(string) error { return nil }); err == nil || !strings.Contains(err.Error(), "lock fault") {
			t.Fatalf("shared config lock fault = %v", err)
		}
		sharedConfigTryLock = originalTryLock
	})

	t.Run("post-materialization validation", func(t *testing.T) {
		originalWriter := sharedAtomicWriteFile
		t.Cleanup(func() { sharedAtomicWriteFile = originalWriter })
		home := durableTempDir(t)
		sharedAtomicWriteFile = func(path string, data []byte, mode os.FileMode) error {
			if err := originalWriter(path, data, mode); err != nil {
				return err
			}
			if filepath.Base(path) == hermesConfigFileName {
				return os.WriteFile(path, []byte("dashboard:\n  turn_isolation: true\n"), mode)
			}

			return nil
		}
		if err := materializeSharedHermesConfig(t.Context(), home, nil, map[string]string{hermesConfigFileName: "safe: true\n"}); err == nil || !strings.Contains(err.Error(), "exactly false") {
			t.Fatalf("post-materialization validation error = %v", err)
		}
	})

	t.Run("config lock control, open and run errors", func(t *testing.T) {
		homeFile := filepath.Join(durableTempDir(t), "home-file")
		if err := os.WriteFile(homeFile, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(homeFile+sharedHermesControlSuffix, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := withHermesConfigLock(t.Context(), homeFile, func(string) error { return nil }); err == nil || !strings.Contains(err.Error(), "control directory") {
			t.Fatalf("config lock control directory error = %v", err)
		}

		blocked := durableTempDir(t)
		if err := os.Mkdir(filepath.Join(sharedTestControlDir(t, blocked), sharedConfigLockName), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := withHermesConfigLock(t.Context(), blocked, func(string) error { return nil }); err == nil || !strings.Contains(err.Error(), "open") {
			t.Fatalf("config lock open error = %v", err)
		}
		want := errors.New("run fault")
		if err := withHermesConfigLock(t.Context(), durableTempDir(t), func(string) error { return want }); !errors.Is(err, want) {
			t.Fatalf("config lock run error = %v", err)
		}
	})

	if err := EnsureLocalSharedHermesHome("/dev"); err == nil {
		t.Fatal("devfs was accepted as a durable shared home")
	}
}
