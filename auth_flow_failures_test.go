package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

func authRawParams(t *testing.T, params map[string]any) json.RawMessage {
	t.Helper()

	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal auth params: %v", err)
	}

	return raw
}

func endedAuthContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	return ctx
}

func TestAuthGateQueuedWaiterAcquiresAfterRelease(t *testing.T) {
	t.Parallel()

	gates := map[string]*authGate{}
	var mu sync.Mutex

	first, ok := authAcquireGate(context.Background(), &mu, gates, "provider")
	if !ok {
		t.Fatal("first acquisition failed")
	}

	acquired := make(chan func(), 1)
	go func() {
		release, queued := authAcquireGate(context.Background(), &mu, gates, "provider")
		if queued {
			acquired <- release
		}
	}()

	deadline := time.Now().Add(time.Second)
	for {
		mu.Lock()
		waiters := gates["provider"].waiters
		mu.Unlock()
		if waiters == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("waiter did not join the gate")
		}
	}

	first()

	select {
	case release := <-acquired:
		release()
	case <-time.After(time.Second):
		t.Fatal("queued waiter did not acquire after release")
	}
}

func TestProviderAuthResidenceValidationAndPreparationFailures(t *testing.T) {
	if err := validateProviderAuthRoots(Options{
		ProviderAuthRoot: t.TempDir(),
		SharedHermesHome: "relative",
	}); err == nil {
		t.Fatal("relative provider auth residence accepted")
	}

	if _, err := prepareProviderAuthResidence("relative"); err == nil {
		t.Fatal("relative provider auth residence prepared")
	}

	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if _, err := prepareProviderAuthResidence(filepath.Join(file, "child")); err == nil {
		t.Fatal("provider auth residence beneath a file prepared")
	}

	agent := newTestAgent(
		WithProviderAuthRoot(t.TempDir()),
		WithSharedHermesHome(filepath.Join(file, "child")),
	)
	if agent.providerAuth != nil {
		t.Fatal("unusable provider auth residence advertised")
	}

	if _, err := newAuthLedger(Options{
		ProviderAuthRoot: t.TempDir(),
		SharedHermesHome: "relative",
	}); err == nil {
		t.Fatal("ledger accepted relative provider auth residence")
	}
}

func TestAuthMintRequiresLiveNativeClientAndCompleterDisarmsOnce(t *testing.T) {
	flow := &authFlow{
		id:            "flow",
		expiresAt:     time.Now().Add(time.Minute),
		probeInterval: time.Second,
		method:        authCatalogMethod{Label: "Login"},
		disarm:        make(chan struct{}),
	}

	_, cause := (&providerAuth{}).buildMint(t.Context(), &session{}, flow)
	if cause != authCauseTransport {
		t.Fatalf("mint cause = %q", cause)
	}

	flow.stopCompleter()
	flow.stopCompleter()
}

func TestAuthFailureRetriabilityIsClosed(t *testing.T) {
	t.Parallel()

	for cause, want := range map[string]bool{
		authCauseTransport:       true,
		authCauseProcess:         true,
		authCauseTimeout:         true,
		authCauseProviderRefused: false,
	} {
		if got := authCauseRetryable(cause); got != want {
			t.Fatalf("%s retryable = %v", cause, got)
		}
	}

	var requestErr *acp.RequestError
	if !errors.As(authFailed(authCauseTransport, "", "", ""), &requestErr) {
		t.Fatal("auth failure did not produce request error")
	}
}

func TestNativePresentationValidationBranches(t *testing.T) {
	t.Parallel()

	mint := authMint{expiresAt: time.Now().Add(time.Minute)}
	method := authCatalogMethod{Flow: nativehermes.AuthFlowDeviceCode}

	if _, cause := applyNativeStart(mint, nativehermes.AuthStart{
		Flow: nativehermes.AuthFlowDeviceCode,
		URL:  "not-a-url",
	}, method); cause != authCauseNativeVeto {
		t.Fatalf("invalid URL cause = %q", cause)
	}

	if _, cause := applyNativeStart(mint, nativehermes.AuthStart{
		Flow:     nativehermes.AuthFlowDeviceCode,
		URL:      testDeviceURL,
		UserCode: "bad code",
	}, method); cause != authCauseNativeVeto {
		t.Fatalf("invalid user code cause = %q", cause)
	}
}

func mustSession(t *testing.T, agent *Agent) *session {
	t.Helper()

	session, err := agent.session(testSessionID)
	if err != nil {
		t.Fatalf("session: %v", err)
	}

	return session
}

func TestProviderAuthResidenceHookFailures(t *testing.T) {
	restoreLedgerHooks(t)

	home := filepath.Join(t.TempDir(), "auth")
	ledgerChmod = func(string, os.FileMode) error { return errors.New("chmod") }
	if _, err := prepareProviderAuthResidence(home); err == nil {
		t.Fatal("provider auth residence chmod failure accepted")
	}

	restoreLedgerHooks(t)
	ledgerEvalPath = func(string) (string, error) { return "", errors.New("resolve") }
	if _, err := prepareProviderAuthResidence(home); err == nil {
		t.Fatal("provider auth residence resolution failure accepted")
	}
}
