package hermes

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestAuthStartRefusesAnUncontainedBrowserLaunch pins the platform-independent
// half of the containment rule: a session whose process carries no shim still
// runs, and only its login leg refuses — before the native call that would open
// the tab.
func TestAuthStartRefusesAnUncontainedBrowserLaunch(t *testing.T) {
	t.Parallel()

	stub := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		t.Errorf("a login leg reached hermes at %s with no contained browser launch", r.URL.Path)
	}))
	t.Cleanup(stub.Close)

	server := &hermesServer{process: &Process{APIBaseURL: stub.URL + "/api", Token: "session-token"}}

	if _, err := server.AuthStart(context.Background(), "anthropic"); !errors.Is(err, ErrBrowserLaunchUncontained) {
		t.Fatalf("AuthStart without a shim = %v", err)
	}
}

func TestBrowserLaunchContainedFollowsTheShim(t *testing.T) {
	t.Parallel()

	var absent *Process
	if absent.BrowserLaunchContained() {
		t.Fatal("a process that does not exist reported a contained browser launch")
	}

	if (&Process{}).BrowserLaunchContained() {
		t.Fatal("a process without a shim reported a contained browser launch")
	}

	if !(&Process{shim: &browserShim{dir: durableTempDir(t)}}).BrowserLaunchContained() {
		t.Fatal("a process with a shim reported an uncontained browser launch")
	}
}

func TestBrowserShimEnvironKeepsEnvWithoutAShim(t *testing.T) {
	t.Parallel()

	var shim *browserShim

	env := []string{"PATH=/usr/bin", "BROWSER=/usr/bin/launcher"}
	kept := shim.environ(env)

	if len(kept) != len(env) || kept[0] != env[0] || kept[1] != env[1] {
		t.Fatalf("environ without a shim = %q", kept)
	}
}

func TestBrowserShimEnvironOverridesPathAndBrowser(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(durableTempDir(t), "shim")
	env := browserShimEnviron([]string{
		"MALFORMED",
		"HERMES_HOME=/home",
		"PATH=/usr/bin",
		"BROWSER=/usr/bin/open",
	}, dir)

	want := []string{
		"MALFORMED",
		"HERMES_HOME=/home",
		"PATH=" + dir + string(os.PathListSeparator) + "/usr/bin",
		"BROWSER=" + filepath.Join(dir, "open"),
	}
	if len(env) != len(want) {
		t.Fatalf("environ = %q", env)
	}

	for i, entry := range want {
		if env[i] != entry {
			t.Fatalf("environ[%d] = %q, want %q", i, env[i], entry)
		}
	}
}

func TestBrowserShimEnvironAddsPathWhenAbsent(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(durableTempDir(t), "shim")

	env := browserShimEnviron(nil, dir)
	if len(env) != 2 || env[0] != "PATH="+dir {
		t.Fatalf("environ = %q", env)
	}

	if strings.Contains(env[0], string(os.PathListSeparator)) {
		t.Fatalf("empty inherited PATH kept a separator: %q", env[0])
	}
}

func TestUpsertProcessEnvMakesProtectedValuesAuthoritative(t *testing.T) {
	t.Parallel()

	env := []string{
		"HERMES_DASHBOARD_SESSION_TOKEN=attacker",
		"A=1",
		"HERMES_DASHBOARD_SESSION_TOKEN=stale",
	}
	env = upsertProcessEnv(env, "HERMES_DASHBOARD_SESSION_TOKEN", "durable")

	var values []string
	for _, entry := range env {
		if strings.HasPrefix(entry, "HERMES_DASHBOARD_SESSION_TOKEN=") {
			values = append(values, entry)
		}
	}

	if len(values) != 1 || values[0] != "HERMES_DASHBOARD_SESSION_TOKEN=durable" {
		t.Fatalf("protected environment = %#v", env)
	}
}

func TestBrowserShimRemoveToleratesANilShim(t *testing.T) {
	t.Parallel()

	var shim *browserShim
	if err := shim.remove(); err != nil {
		t.Fatalf("remove on a nil shim = %v", err)
	}
}
