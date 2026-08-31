//go:build integration

package hermes

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestOfficialSharedHomeConcurrentProcessesRestartModelCatalogAndAuthResidence
// is the adversarial proof for official shared-home mode. Two official
// gateways use the exact same HERMES_HOME while retaining distinct wrapper
// XDG/control generations, create and persist different native sessions, read
// the catalog, select models, observe one credential residence, close, and
// resume both durable IDs through fresh processes.
func TestOfficialSharedHomeConcurrentProcessesRestartModelCatalogAndAuthResidence(t *testing.T) {
	if os.Getenv("ACP_GO_HERMES_RUN_INTEGRATION") == "" {
		t.Skip("set ACP_GO_HERMES_RUN_INTEGRATION=1")
	}

	executable := os.Getenv("ACP_GO_HERMES_HARNESS_PATH")
	if executable == "" {
		var err error
		executable, err = exec.LookPath("hermes")
		if err != nil {
			t.Fatalf("find hermes: %v", err)
		}
	}

	sharedHome := t.TempDir()
	authData, err := json.Marshal(map[string]any{
		"version": 1,
		"providers": map[string]any{
			"xai-oauth": map[string]any{
				"tokens": map[string]any{
					"access_token":  "shared-home-access",
					"refresh_token": "shared-home-refresh",
					"token_type":    "Bearer",
				},
				"auth_mode": "oauth_device_code",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sharedHome, "auth.json"), authData, 0o600); err != nil {
		t.Fatalf("seed shared auth residence: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	type lane struct {
		id     ACPSessionIDString
		cwd    string
		xdg    XDGDirs
		server Server
		native Session
	}
	lanes := []*lane{
		{id: "shared-a", cwd: t.TempDir(), xdg: testXDGDirs(t)},
		{id: "shared-b", cwd: t.TempDir(), xdg: testXDGDirs(t)},
	}

	start := func(l *lane) error {
		server, startErr := StartServer(ctx, darwinTestStartOptions(t, StartOptions{
			ACPSessionID:     l.id,
			Cwd:              l.cwd,
			ExecutablePath:   executable,
			SharedHermesHome: sharedHome,
			ExistingXDG:      l.xdg,
			HealthTimeout:    2 * time.Minute,
			Logger:           slog.New(slog.DiscardHandler),
			Env: map[string]string{
				"OPENAI_API_KEY": "shared-home-test-key",
				"XAI_API_KEY":    "",
			},
		}))
		l.server = server

		return startErr
	}

	runLanesConcurrently(t, lanes, start)
	defer func() {
		for _, l := range lanes {
			if l.server != nil {
				_ = l.server.Close(context.Background())
			}
		}
	}()

	for _, l := range lanes {
		if l.server.XDGDirs().Root != l.xdg.Root {
			t.Fatalf("%s wrapper home = %q, want %q", l.id, l.server.XDGDirs().Root, l.xdg.Root)
		}
		controlLock := filepath.Join(ControlDirForXDG(l.xdg.Root), "server.lock")
		if _, err := os.Stat(controlLock); err != nil {
			t.Fatalf("%s control lock: %v", l.id, err)
		}
	}
	firstRuntime := lanes[0].server.(*hermesServer)
	secondRuntime := lanes[1].server.(*hermesServer)
	if firstRuntime.process.Home != sharedHome || secondRuntime.process.Home != sharedHome {
		t.Fatalf("native processes do not share exact HERMES_HOME: %q / %q", firstRuntime.process.Home, secondRuntime.process.Home)
	}
	if firstRuntime.process.Port == secondRuntime.process.Port || firstRuntime.process.Token == secondRuntime.process.Token ||
		firstRuntime.process.shim.dir == secondRuntime.process.shim.dir || firstRuntime.process.native == secondRuntime.process.native {
		t.Fatalf("same-home process runtime carriers collided")
	}

	runLanesConcurrently(t, lanes, func(l *lane) error {
		native, createErr := l.server.CreateSession(ctx, string(l.id))
		l.native = native

		return createErr
	})

	for _, l := range lanes {
		if l.native.ID == "" || l.native.Directory != l.cwd {
			t.Fatalf("%s native session = %#v", l.id, l.native)
		}
		catalog, catalogErr := l.server.ConfigProviders(ctx)
		if catalogErr != nil {
			t.Fatalf("%s model catalog: %v", l.id, catalogErr)
		}
		providerID, modelID := catalogModelForProvider(catalog, "xai-oauth")
		if providerID == "" || modelID == "" {
			t.Fatalf("%s model catalog is empty: %#v", l.id, catalog)
		}
		assertXAIModelResidence(t, catalog, true)
		setter, ok := l.server.(interface {
			SetModel(context.Context, string, string) error
		})
		if !ok {
			t.Fatalf("%s server has no session model setter", l.id)
		}
		selection := ModelSelectionValue(providerID, modelID)
		if setErr := setter.SetModel(ctx, l.native.ID, selection); setErr != nil {
			t.Fatalf("%s set model: %v", l.id, setErr)
		}
		selected, selectedErr := sessionModelOptions(ctx, l.server, l.native.ID)
		if selectedErr != nil {
			t.Fatalf("%s selected model state: %v", l.id, selectedErr)
		}
		assertSelectedModel(t, selected, providerID, modelID)
		providers, providersErr := l.server.AuthProviders(ctx)
		if providersErr != nil {
			t.Fatalf("%s auth catalog: %v", l.id, providersErr)
		}
		if !authProviderPresent(providers, "xai-oauth") {
			t.Fatalf("%s did not observe xai-oauth in shared auth residence: %#v", l.id, providers)
		}
		if !authProviderLoggedIn(providers, "xai-oauth") {
			t.Fatalf("%s did not observe xai-oauth as logged in: %#v", l.id, providers)
		}
	}

	// Mutate only the disposable fixture. Process A disconnects the provider;
	// process B must immediately observe that the shared residence is no longer
	// usable. Restoring the fixture must become visible to both live processes.
	if err := lanes[0].server.AuthDisconnect(ctx, "xai-oauth"); err != nil {
		t.Fatalf("disconnect xai-oauth through process A: %v", err)
	}
	providers, err := lanes[1].server.AuthProviders(ctx)
	if err != nil {
		t.Fatalf("read auth catalog through process B after disconnect: %v", err)
	}
	if authProviderLoggedIn(providers, "xai-oauth") {
		t.Fatalf("process B retained stale xai-oauth login after process A disconnected it: %#v", providers)
	}
	disconnectedCatalog, err := lanes[1].server.ConfigProviders(ctx)
	if err != nil {
		t.Fatalf("process B model catalog after disconnect: %v", err)
	}
	assertXAIModelResidence(t, disconnectedCatalog, false)
	if err := os.WriteFile(filepath.Join(sharedHome, "auth.json"), authData, 0o600); err != nil {
		t.Fatalf("restore disposable shared auth fixture: %v", err)
	}
	for _, l := range lanes {
		providers, providersErr := l.server.AuthProviders(ctx)
		if providersErr != nil {
			t.Fatalf("%s auth catalog after restore: %v", l.id, providersErr)
		}
		if !authProviderLoggedIn(providers, "xai-oauth") {
			t.Fatalf("%s did not observe restored xai-oauth fixture: %#v", l.id, providers)
		}
		catalog, catalogErr := l.server.ConfigProviders(ctx)
		if catalogErr != nil {
			t.Fatalf("%s model catalog after restore: %v", l.id, catalogErr)
		}
		assertXAIModelResidence(t, catalog, true)
	}

	for _, l := range lanes {
		if closeErr := l.server.Close(ctx); closeErr != nil {
			t.Fatalf("close %s: %v", l.id, closeErr)
		}
		l.server = nil
	}
	if _, err := os.Stat(filepath.Join(sharedHome, "auth.json")); err != nil {
		t.Fatalf("shared auth residence after close: %v", err)
	}

	runLanesConcurrently(t, lanes, func(l *lane) error {
		if err := start(l); err != nil {
			return err
		}
		resumed, resumeErr := l.server.GetSession(ctx, l.native.ID)
		if resumeErr == nil && resumed.ID != l.native.ID {
			return errors.New("resumed the wrong durable native session")
		}

		return resumeErr
	})
}

func sessionModelOptions(ctx context.Context, server Server, storedSessionID string) (ProvidersResponse, error) {
	runtime := server.(*hermesServer)
	transport := runtime.beginGatewayTurn()
	defer runtime.endGatewayTurn()
	if transport == nil {
		return ProvidersResponse{}, ErrGatewayDisconnected
	}
	liveSessionID := runtime.liveSessionIDOn(transport, storedSessionID)
	if liveSessionID == "" {
		return ProvidersResponse{}, MissingLiveSessionMappingError{StoredSessionID: storedSessionID}
	}
	models, err := transport.client.ModelOptions(ctx, liveSessionID)
	if err != nil {
		return ProvidersResponse{}, err
	}

	return providersFromGateway(models), nil
}

func runLanesConcurrently[T any](t *testing.T, lanes []*T, run func(*T) error) {
	t.Helper()

	errs := make(chan error, len(lanes))
	var group sync.WaitGroup
	for _, l := range lanes {
		group.Add(1)
		go func() {
			defer group.Done()
			errs <- run(l)
		}()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func catalogModelForProvider(catalog ProvidersResponse, providerID string) (string, string) {
	for _, provider := range catalog.Providers {
		if provider.ID != providerID {
			continue
		}
		for modelID := range provider.Models {
			return provider.ID, modelID
		}
	}
	return "", ""
}

func assertSelectedModel(t *testing.T, catalog ProvidersResponse, providerID string, modelID string) {
	t.Helper()
	var payload struct {
		Provider string `json:"provider"`
		Model    string `json:"model"`
	}
	if err := json.Unmarshal(catalog.Raw, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Provider != providerID || payload.Model != modelID {
		t.Fatalf("selected provider/model = %q/%q, want %q/%q", payload.Provider, payload.Model, providerID, modelID)
	}
}

func authProviderPresent(providers []AuthProvider, id string) bool {
	for _, provider := range providers {
		if provider.ID == id {
			return true
		}
	}

	return false
}

func authProviderLoggedIn(providers []AuthProvider, id string) bool {
	for _, provider := range providers {
		if provider.ID == id {
			return provider.LoggedIn
		}
	}

	return false
}

func assertXAIModelResidence(t *testing.T, catalog ProvidersResponse, wantAuthenticated bool) {
	t.Helper()

	var payload struct {
		Providers []struct {
			Slug          string   `json:"slug"`
			Authenticated bool     `json:"authenticated"`
			Models        []string `json:"models"`
		} `json:"providers"`
	}
	if err := json.Unmarshal(catalog.Raw, &payload); err != nil {
		t.Fatalf("decode model.options residence: %v", err)
	}

	foundOAuth := false
	for _, provider := range payload.Providers {
		switch provider.Slug {
		case "xai":
			if len(provider.Models) > 0 {
				t.Fatalf("model.options rewrote xai-oauth models under xai: %#v", provider.Models)
			}
		case "xai-oauth":
			foundOAuth = true
			if provider.Authenticated != wantAuthenticated {
				t.Fatalf("xai-oauth authenticated = %t, want %t", provider.Authenticated, wantAuthenticated)
			}
			if wantAuthenticated && len(provider.Models) == 0 {
				t.Fatal("xai-oauth has no model IDs while authenticated")
			}
		}
	}
	if wantAuthenticated && !foundOAuth {
		t.Fatal("model.options omitted exact xai-oauth provider")
	}
}
