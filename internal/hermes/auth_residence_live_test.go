//go:build integration

package hermes

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestLiveProviderAuthResidenceSurvivesSequentialRestarts(t *testing.T) {
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

	authHome := t.TempDir()
	authData, err := json.Marshal(map[string]any{
		"version": 1,
		"providers": map[string]any{
			"xai-oauth": map[string]any{
				"tokens": map[string]any{
					"access_token":  "restart-test-access",
					"refresh_token": "restart-test-refresh",
					"token_type":    "Bearer",
				},
				"auth_mode": "oauth_device_code",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(authHome, "auth.json"), authData, 0o600); err != nil {
		t.Fatalf("seed provider auth residence: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	var runtimeHomes []string
	for restart := range 2 {
		scratch := t.TempDir()
		runtimeHome, err := os.MkdirTemp(scratch, "acp-go-hermes-runtime-")
		if err != nil {
			t.Fatalf("create runtime home %d: %v", restart+1, err)
		}
		runtimeHomes = append(runtimeHomes, runtimeHome)
		options := ProcessOptions{
			ExecutablePath:   executable,
			Home:             runtimeHome,
			Cwd:              t.TempDir(),
			ScratchParent:    scratch,
			ProviderAuthHome: authHome,
			Timeout:          2 * time.Minute,
			Env: map[string]string{
				"NO_COLOR": "1",
				"PATH":     os.Getenv("PATH"),
			},
			AcquireDiscoveryResources: testDiscoveryResourceAdmission,
			RetainDiscoveryRoot:       func(string, error) {},
		}
		if runtime.GOOS == "darwin" {
			options.DarwinBestEffortContainment = true
		}

		process, err := Start(ctx, options)
		if err != nil {
			t.Fatalf("start %d: %v", restart+1, err)
		}

		providers, providerErr := (&hermesServer{process: process}).AuthProviders(ctx)
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		closeErr := process.Close(closeCtx)
		closeCancel()
		if providerErr != nil {
			t.Fatalf("providers after start %d: %v", restart+1, providerErr)
		}
		if closeErr != nil {
			t.Fatalf("close start %d: %v", restart+1, closeErr)
		}

		found := false
		for _, provider := range providers {
			if provider.ID == "xai-oauth" {
				found = provider.LoggedIn
				break
			}
		}
		if !found {
			t.Fatalf("xai-oauth was not resident after start %d", restart+1)
		}
	}

	if runtimeHomes[0] == runtimeHomes[1] {
		t.Fatal("sequential starts reused a runtime home")
	}
	for _, runtimeHome := range runtimeHomes {
		if _, err := os.Stat(filepath.Join(runtimeHome, "auth.json")); !os.IsNotExist(err) {
			t.Fatalf("runtime home contains provider credentials: %s", runtimeHome)
		}
	}
	if _, err := os.Stat(filepath.Join(authHome, "auth.json")); err != nil {
		t.Fatalf("durable provider auth residence was lost: %v", err)
	}
}
