package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

func TestSharedHomeHostAuthorityValidationResidualBranch(t *testing.T) {
	options := Options{}
	options.SharedHermesHome = filepath.Clean(durableTempDir(t))
	options.HostAuthority = newTestHostAuthority()
	if err := validateSharedHermesHomeOptions(options); err == nil {
		t.Fatal("shared home accepted host authority")
	}
}

func TestHostAuthorityValidationAndEnvironmentResidualBranches(t *testing.T) {
	if hostAuthorityNil(residualValueAuthority{}) {
		t.Fatal("value authority was classified as nil")
	}
	if _, err := readHostEnvironment(nil); !errors.Is(err, ErrHostAuthorityUnavailable) {
		t.Fatalf("nil authority environment = %v", err)
	}
	if _, err := readHostEnvironment(&residualAuthority{environment: func() map[string]string {
		panic("environment unavailable")
	}}); !errors.Is(err, ErrHostAuthorityUnavailable) {
		t.Fatalf("panicked authority environment = %v", err)
	}
	if _, err := readHostEnvironment(&residualAuthority{environment: func() map[string]string { return nil }}); !errors.Is(err, ErrHostAuthorityUnavailable) {
		t.Fatalf("nil authority environment result = %v", err)
	}
	if err := validateHostAuthority(Options{
		hostAuthoritySupplied: true, HostAuthority: residualValueAuthority{}, SharedHermesHome: durableTempDir(t),
	}); err == nil {
		t.Fatal("shared residence accepted a value host authority")
	}
}

func TestConfiguredHostAuthorityResidualBranches(t *testing.T) {
	newConfigured := func(t *testing.T) (*Agent, *residualAuthority, nativehermes.StartOptions) {
		t.Helper()
		authority := &residualAuthority{}
		agent := NewAgent(WithHostAuthority(authority), WithScratchDir(durableTempDir(t)))
		if agent.optionsErr != nil {
			t.Fatal(agent.optionsErr)
		}
		start := nativehermes.StartOptions{}
		agent.configureHostAuthority(&start)

		return agent, authority, start
	}

	t.Run("prepare admission", func(t *testing.T) {
		agent, _, start := newConfigured(t)
		agent.authorityErr = ErrHostAuthorityUnavailable
		if err := start.PrepareNativeTree(t.Context(), durableTempDir(t)); !errors.Is(err, ErrHostAuthorityUnavailable) {
			t.Fatalf("prepare admission = %v", err)
		}
	})
	t.Run("prepare panic", func(t *testing.T) {
		_, authority, start := newConfigured(t)
		authority.prepare = func(context.Context, string) error { panic("prepare unavailable") }
		if err := start.PrepareNativeTree(t.Context(), durableTempDir(t)); !errors.Is(err, ErrHostAuthorityUnavailable) {
			t.Fatalf("prepare panic = %v", err)
		}
	})
	t.Run("prepare busy", func(t *testing.T) {
		_, authority, start := newConfigured(t)
		authority.prepare = func(context.Context, string) error { return ErrNativeTreeBusy }
		if err := start.PrepareNativeTree(t.Context(), durableTempDir(t)); !errors.Is(err, ErrNativeTreeBusy) {
			t.Fatalf("prepare busy = %v", err)
		}
	})
	t.Run("start admission", func(t *testing.T) {
		agent, _, start := newConfigured(t)
		agent.authorityErr = ErrHostAuthorityUnavailable
		if _, err := start.StartNative(t.Context(), nativehermes.NativeRequest{}); !errors.Is(err, ErrHostAuthorityUnavailable) {
			t.Fatalf("start admission = %v", err)
		}
	})
	t.Run("start panic", func(t *testing.T) {
		_, authority, start := newConfigured(t)
		authority.start = func(context.Context, NativeRequest) (NativeProcess, error) { panic("start unavailable") }
		if _, err := start.StartNative(t.Context(), nativehermes.NativeRequest{}); !errors.Is(err, ErrHostAuthorityUnavailable) {
			t.Fatalf("start panic = %v", err)
		}
	})
	t.Run("start nil process", func(t *testing.T) {
		_, authority, start := newConfigured(t)
		authority.start = func(context.Context, NativeRequest) (NativeProcess, error) {
			return nilResidualNativeProcess(), nil
		}
		if _, err := start.StartNative(t.Context(), nativehermes.NativeRequest{}); !errors.Is(err, ErrHostAuthorityUnavailable) {
			t.Fatalf("nil start process = %v", err)
		}
	})
	t.Run("reclaim panic", func(t *testing.T) {
		agent, authority, _ := newConfigured(t)
		authority.reclaim = func(context.Context, string) error { panic("reclaim unavailable") }
		if err := agent.reclaimNativeTree(t.Context(), durableTempDir(t)); !errors.Is(err, ErrHostAuthorityUnavailable) {
			t.Fatalf("reclaim panic = %v", err)
		}
	})
	t.Run("reclaim failure", func(t *testing.T) {
		agent, authority, _ := newConfigured(t)
		want := errors.New("reclaim refused")
		authority.reclaim = func(context.Context, string) error { return want }
		if err := agent.reclaimNativeTree(t.Context(), durableTempDir(t)); !errors.Is(err, want) || !errors.Is(err, ErrContainmentIncomplete) {
			t.Fatalf("reclaim failure = %v", err)
		}
	})
}

func TestRetiredNativeRootRetryResidualBranches(t *testing.T) {
	authority := &residualAuthority{}
	agent := NewAgent(WithHostAuthority(authority), WithScratchDir(durableTempDir(t)))
	root := durableTempDir(t)
	agent.retiredNativeRoots[root] = true
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := agent.retryRetiredNativeRoots(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled retry = %v", err)
	}

	want := errors.New("reclaim refused")
	authority.reclaim = func(context.Context, string) error { return want }
	if err := agent.retryRetiredNativeRoots(t.Context()); !errors.Is(err, want) || !errors.Is(err, ErrContainmentIncomplete) {
		t.Fatalf("failed retry = %v", err)
	}
}

func TestNativeProcessBridgeResidualBranches(t *testing.T) {
	if !nativeProcessNil(nil) {
		t.Fatal("nil process was not classified as nil")
	}
	if nativeProcessNil(residualValueNativeProcess{}) {
		t.Fatal("value process was classified as nil")
	}
	want := errors.New("native failure")
	assertRecorded := func(t *testing.T, invoke func(*residualNativeProcess, nativeProcessBridge) error) {
		t.Helper()
		var recorded error
		process := &residualNativeProcess{}
		bridge := nativeProcessBridge{
			process: process,
			record: func(err error) error {
				recorded = err

				return err
			},
		}
		if err := invoke(process, bridge); err == nil || recorded == nil {
			t.Fatalf("bridge result = %v, recorded=%v", err, recorded)
		}
	}

	if err := (nativeProcessBridge{}).recordError(want); !errors.Is(err, want) {
		t.Fatalf("nil recorder = %v", err)
	}
	assertRecorded(t, func(process *residualNativeProcess, bridge nativeProcessBridge) error {
		process.stdout = func() io.ReadCloser { panic("stdout unavailable") }
		if stream := bridge.Stdout(); stream != nil {
			t.Fatal("panicked stdout returned a stream")
		}

		return ErrHostAuthorityUnavailable
	})
	assertRecorded(t, func(process *residualNativeProcess, bridge nativeProcessBridge) error {
		process.stdout = func() io.ReadCloser { return nil }
		if stream := bridge.Stdout(); stream != nil {
			t.Fatal("nil stdout returned a stream")
		}

		return ErrHostAuthorityUnavailable
	})
	assertRecorded(t, func(process *residualNativeProcess, bridge nativeProcessBridge) error {
		process.stderr = func() io.ReadCloser { panic("stderr unavailable") }
		if stream := bridge.Stderr(); stream != nil {
			t.Fatal("panicked stderr returned a stream")
		}

		return ErrHostAuthorityUnavailable
	})
	assertRecorded(t, func(process *residualNativeProcess, bridge nativeProcessBridge) error {
		process.stderr = func() io.ReadCloser { return nil }
		if stream := bridge.Stderr(); stream != nil {
			t.Fatal("nil stderr returned a stream")
		}

		return ErrHostAuthorityUnavailable
	})
	assertRecorded(t, func(process *residualNativeProcess, bridge nativeProcessBridge) error {
		process.wait = func(context.Context) (NativeResult, error) { panic("wait unavailable") }
		_, err := bridge.Wait(t.Context())

		return err
	})
	assertRecorded(t, func(process *residualNativeProcess, bridge nativeProcessBridge) error {
		process.wait = func(context.Context) (NativeResult, error) { return NativeResult{}, want }
		_, err := bridge.Wait(t.Context())

		return err
	})
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	cancelBridge := nativeProcessBridge{process: &residualNativeProcess{
		wait: func(context.Context) (NativeResult, error) { return NativeResult{ExitCode: 7}, context.Canceled },
	}}
	if result, err := cancelBridge.Wait(canceled); !errors.Is(err, context.Canceled) || result.ExitCode != 7 {
		t.Fatalf("canceled wait = %#v, %v", result, err)
	}
	assertRecorded(t, func(process *residualNativeProcess, bridge nativeProcessBridge) error {
		process.revoke = func(context.Context) error { panic("revoke unavailable") }

		return bridge.Revoke(t.Context())
	})
	cancelBridge.process = &residualNativeProcess{revoke: func(context.Context) error { return context.Canceled }}
	if err := cancelBridge.Revoke(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled revoke = %v", err)
	}
	assertRecorded(t, func(process *residualNativeProcess, bridge nativeProcessBridge) error {
		process.revoke = func(context.Context) error { return ErrContainmentIncomplete }

		return bridge.Revoke(t.Context())
	})
	genericBridge := nativeProcessBridge{process: &residualNativeProcess{revoke: func(context.Context) error { return want }}}
	if err := genericBridge.Revoke(t.Context()); !errors.Is(err, want) {
		t.Fatalf("generic revoke = %v", err)
	}
}

func TestManagedSnapshotStateResidualBranches(t *testing.T) {
	session := testSession(t, newTestAgent(), newFakeHermesClient())
	if err := session.withReclaimedManagedState(t.Context(), session.client, func(string) error { return nil }); err == nil {
		t.Fatal("ordinary client state reclaim succeeded")
	}
	if _, _, err := session.beginManagedSnapshotStateHeld(t.Context(), session.client); err == nil {
		t.Fatal("ordinary client snapshot begin succeeded")
	}

	wantClose := errors.New("managed close refused")
	failing := &managedHermesServer{
		Server: &fakeHermesClient{closeErr: wantClose}, managed: true, root: durableTempDir(t),
	}
	if _, _, err := session.beginManagedSnapshotStateHeld(t.Context(), failing); !errors.Is(err, wantClose) {
		t.Fatalf("managed snapshot close failure = %v", err)
	}
	fake := newFakeHermesClient()
	fake.closeErr = wantClose
	failing.Server = fake
	session.client = failing
	if _, err := session.captureSnapshotLocked(t.Context(), nil); !errors.Is(err, wantClose) {
		t.Fatalf("managed snapshot capture close failure = %v", err)
	}
}

func TestCompleteManagedSnapshotCommitResidualBranches(t *testing.T) {
	newCommit := func(t *testing.T) (*session, *sessionStoreCommit) {
		t.Helper()
		root := durableTempDir(t)
		managed := &managedHermesServer{
			Server: newFakeHermesClient(), managed: true, settled: true, root: root,
		}
		session := testSession(t, newTestAgent(), newFakeHermesClient())
		commit := &sessionStoreCommit{
			managed: managed, managedRoot: root,
			managedMain:  &stateSnapshot{Archives: map[string]archiveInfo{}},
			managedState: SessionKey{SessionID: "session", Subpath: stateDBSubpath},
			mainKey:      SessionKey{SessionID: "session", Subpath: SessionStoreMainSubpath},
		}

		return session, commit
	}

	t.Run("archive entry encoding", func(t *testing.T) {
		restoreStateStoreSeams(t)
		session, commit := newCommit(t)
		if err := os.WriteFile(filepath.Join(commit.managedRoot, fileStateDB), []byte("sqlite placeholder"), 0o600); err != nil {
			t.Fatal(err)
		}
		stateSQLiteArchiveContent = func(string, string) ([]byte, bool, error) {
			return []byte("scrubbed sqlite"), true, nil
		}
		want := errors.New("archive entry marshal refused")
		stateJSONMarshal = func(any) ([]byte, error) { return nil, want }
		if err := session.completeManagedSnapshotCommit(commit); !errors.Is(err, want) {
			t.Fatalf("archive entry encoding = %v", err)
		}
	})

	t.Run("main encoding", func(t *testing.T) {
		restoreStateStoreSeams(t)
		session, commit := newCommit(t)
		want := errors.New("main marshal refused")
		stateJSONMarshal = func(any) ([]byte, error) { return nil, want }
		if err := session.completeManagedSnapshotCommit(commit); !errors.Is(err, want) || commit.managedReady != nil {
			t.Fatalf("main encoding = %v, ready=%#v", err, commit.managedReady)
		}
	})

	t.Run("finish reclaim", func(t *testing.T) {
		session, commit := newCommit(t)
		want := errors.New("finish refused")
		commit.managed.closed = true
		commit.managed.closeErr = want
		if err := session.completeManagedSnapshotCommit(commit); !errors.Is(err, want) {
			t.Fatalf("finish reclaim = %v", err)
		}
	})
}

func TestSnapshotCaptureCancellationResidualBranches(t *testing.T) {
	t.Run("after id map", func(t *testing.T) {
		restoreStateStoreSeams(t)
		session := snapshotFaultSession(t)
		ctx, cancel := context.WithCancel(t.Context())
		stateJSONMarshal = func(value any) ([]byte, error) {
			encoded, err := json.Marshal(value)
			cancel()

			return encoded, err
		}
		if _, err := session.captureSnapshotLocked(ctx, nil); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation after id map = %v", err)
		}
	})

	t.Run("after managed completion", func(t *testing.T) {
		restoreStateStoreSeams(t)
		fake := newFakeHermesClient()
		root := durableTempDir(t)
		xdg, err := testGenerationXDG(root)
		if err != nil {
			t.Fatal(err)
		}
		fake.xdg = xdg
		managed := &managedHermesServer{Server: fake, managed: true, root: root}
		session := testSession(t, newTestAgent(), fake)
		session.client = managed
		ctx, cancel := context.WithCancel(t.Context())
		calls := 0
		stateJSONMarshal = func(value any) ([]byte, error) {
			calls++
			encoded, marshalErr := json.Marshal(value)
			if calls == 2 {
				cancel()
			}

			return encoded, marshalErr
		}
		commit, err := session.captureSnapshotLocked(ctx, nil)
		if !errors.Is(err, context.Canceled) || commit == nil || calls != 2 {
			t.Fatalf("managed completion cancellation = commit:%v calls:%d err:%v", commit != nil, calls, err)
		}
	})
}
