package hermesacp

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

func TestManagedHermesServerOptionalSurfaces(t *testing.T) {
	const storedSessionID = "stored"

	unsupported := &managedHermesServer{Server: struct{ nativehermes.Server }{}}
	if _, err := unsupported.CreateSessionWithDraft(t.Context(), "title", func(nativehermes.SessionDraft) error { return nil }); err == nil {
		t.Fatal("unsupported draft creation succeeded")
	}
	if _, err := unsupported.PersistedSessions(t.Context()); err == nil {
		t.Fatal("unsupported persisted inventory succeeded")
	}
	if _, err := unsupported.ForkWithBaseline(t.Context(), "id", "marker", nil); err == nil {
		t.Fatal("unsupported recoverable fork succeeded")
	}
	if err := unsupported.SetModel(t.Context(), "id", "provider/model"); err == nil {
		t.Fatal("unsupported model selection succeeded")
	}
	if unsupported.ProviderAuthSupported() {
		t.Fatal("unsupported provider auth was advertised")
	}

	client := newFakeHermesClient()
	client.createSession = nativehermes.Session{ID: storedSessionID}
	client.persistedSessions = []nativehermes.Session{{ID: storedSessionID}}
	client.forkSession = nativehermes.Session{ID: "forked"}
	supported := &managedHermesServer{Server: client}
	created, err := supported.CreateSessionWithDraft(t.Context(), "title", func(draft nativehermes.SessionDraft) error {
		if draft.StoredSessionID != storedSessionID {
			t.Fatalf("draft = %#v", draft)
		}

		return nil
	})
	if err != nil || created.ID != storedSessionID {
		t.Fatalf("draft creation = %#v, %v", created, err)
	}
	listed, err := supported.PersistedSessions(t.Context())
	if err != nil || len(listed) != 1 {
		t.Fatalf("persisted sessions = %#v, %v", listed, err)
	}
	forked, err := supported.ForkWithBaseline(t.Context(), storedSessionID, "marker", nil)
	if err != nil || forked.ID != "forked" {
		t.Fatalf("recoverable fork = %#v, %v", forked, err)
	}
	if err := supported.SetModel(t.Context(), storedSessionID, "provider/model"); err != nil {
		t.Fatal(err)
	}
	if !supported.ProviderAuthSupported() {
		t.Fatal("supported provider auth was hidden")
	}
}

func TestManagedHermesServerSnapshotResidualBranches(t *testing.T) {
	if _, err := (&managedHermesServer{}).reclaimForSnapshot(t.Context()); err == nil {
		t.Fatal("unmanaged snapshot reclaim succeeded")
	}
	if _, err := (&managedHermesServer{managed: true, closed: true}).reclaimForSnapshot(t.Context()); err == nil {
		t.Fatal("closed snapshot reclaim succeeded")
	}
	settled := &managedHermesServer{managed: true, settled: true, root: "/settled"}
	if root, err := settled.reclaimForSnapshot(t.Context()); err != nil || root != "/settled" {
		t.Fatalf("settled snapshot reclaim = %q, %v", root, err)
	}

	retained := 0
	client := newFakeHermesClient()
	client.closeErr = ErrContainmentIncomplete
	server := &managedHermesServer{
		Server: client, managed: true, root: durableTempDir(t), sessionID: "session",
		retainIncomplete: func(error, acp.SessionId, string) { retained++ },
	}
	if _, err := server.reclaimForSnapshot(t.Context()); !errors.Is(err, ErrContainmentIncomplete) || retained != 1 {
		t.Fatalf("failed snapshot reclaim = %v, retained=%d", err, retained)
	}

	if err := (&managedHermesServer{}).finishReclaimedSnapshot(); err == nil {
		t.Fatal("unreclaimed snapshot finished")
	}
	wantClosed := errors.New("closed snapshot")
	if err := (&managedHermesServer{closed: true, closeErr: wantClosed}).finishReclaimedSnapshot(); !errors.Is(err, wantClosed) {
		t.Fatalf("closed snapshot finish = %v", err)
	}

	originalRemoveAll := managedRemoveAll
	t.Cleanup(func() { managedRemoveAll = originalRemoveAll })
	managedRemoveAll = func(string) error { return errors.New("scratch remove refused") }
	if err := (&managedHermesServer{settled: true, root: durableTempDir(t)}).finishReclaimedSnapshot(); err == nil {
		t.Fatal("snapshot cleanup failure was ignored")
	}
	if err := (&managedHermesServer{Server: newFakeHermesClient(), settled: true, root: durableTempDir(t)}).Close(t.Context()); err == nil {
		t.Fatal("close cleanup failure was ignored")
	}
	managedRemoveAll = originalRemoveAll
}

type residualValueAuthority struct{}

func (residualValueAuthority) NativeEnvironment() map[string]string {
	return map[string]string{"PATH": "/bin"}
}

func (residualValueAuthority) PrepareNativeTree(context.Context, string) error { return nil }

func (residualValueAuthority) WriteNativeAppendLog(context.Context, string, [][]byte) error {
	return ErrHostAuthorityUnavailable
}

func (residualValueAuthority) ReadNativeAppendLog(context.Context, string, uint64) ([][]byte, error) {
	return nil, nil
}

func (residualValueAuthority) ReclaimNativeTree(context.Context, string) error { return nil }

func (residualValueAuthority) StartNative(context.Context, NativeRequest) (NativeProcess, error) {
	return residualValueNativeProcess{}, nil
}

type residualValueNativeProcess struct{}

func (residualValueNativeProcess) Stdin() io.WriteCloser { return nopWriteCloser{Writer: io.Discard} }

func (residualValueNativeProcess) Stdout() io.ReadCloser { return io.NopCloser(strings.NewReader("")) }

func (residualValueNativeProcess) Stderr() io.ReadCloser { return io.NopCloser(strings.NewReader("")) }

func (residualValueNativeProcess) Wait(context.Context) (NativeResult, error) {
	return NativeResult{}, nil
}

func (residualValueNativeProcess) Revoke(context.Context) error { return nil }

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

func nilResidualNativeProcess() NativeProcess {
	var process *residualNativeProcess

	return process
}

type residualAuthority struct {
	environment func() map[string]string
	prepare     func(context.Context, string) error
	reclaim     func(context.Context, string) error
	start       func(context.Context, NativeRequest) (NativeProcess, error)
}

func (a *residualAuthority) NativeEnvironment() map[string]string {
	if a.environment != nil {
		return a.environment()
	}

	return map[string]string{"PATH": "/bin"}
}

func (a *residualAuthority) PrepareNativeTree(ctx context.Context, root string) error {
	if a.prepare != nil {
		return a.prepare(ctx, root)
	}

	return nil
}

func (*residualAuthority) WriteNativeAppendLog(context.Context, string, [][]byte) error {
	return ErrHostAuthorityUnavailable
}

func (*residualAuthority) ReadNativeAppendLog(context.Context, string, uint64) ([][]byte, error) {
	return nil, nil
}

func (a *residualAuthority) ReclaimNativeTree(ctx context.Context, root string) error {
	if a.reclaim != nil {
		return a.reclaim(ctx, root)
	}

	return nil
}

func (a *residualAuthority) StartNative(ctx context.Context, request NativeRequest) (NativeProcess, error) {
	if a.start != nil {
		return a.start(ctx, request)
	}

	return residualValueNativeProcess{}, nil
}

type residualNativeProcess struct {
	stdin  func() io.WriteCloser
	stdout func() io.ReadCloser
	stderr func() io.ReadCloser
	wait   func(context.Context) (NativeResult, error)
	revoke func(context.Context) error
}

func (p *residualNativeProcess) Stdin() io.WriteCloser {
	if p.stdin != nil {
		return p.stdin()
	}

	return nopWriteCloser{Writer: io.Discard}
}

func (p *residualNativeProcess) Stdout() io.ReadCloser {
	if p.stdout != nil {
		return p.stdout()
	}

	return io.NopCloser(strings.NewReader(""))
}

func (p *residualNativeProcess) Stderr() io.ReadCloser {
	if p.stderr != nil {
		return p.stderr()
	}

	return io.NopCloser(strings.NewReader(""))
}

func (p *residualNativeProcess) Wait(ctx context.Context) (NativeResult, error) {
	if p.wait != nil {
		return p.wait(ctx)
	}

	return NativeResult{}, nil
}

func (p *residualNativeProcess) Revoke(ctx context.Context) error {
	if p.revoke != nil {
		return p.revoke(ctx)
	}

	return nil
}
