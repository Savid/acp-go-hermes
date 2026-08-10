//go:build unix

package hermes

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// runOrdinaryIdentityProbe executes a real native child through the ordinary
// launch path and returns what that child observed about itself. Nothing is
// stubbed between the call and the process: the point of these tests is that an
// omitted policy really does start a process as the identity this test already
// holds, which a fake lifecycle seam could not show.
func runOrdinaryIdentityProbe(t *testing.T) map[string]string {
	t.Helper()

	status := filepath.Join(t.TempDir(), "ordinary-identity")
	// $PPID is POSIX, so the no-interposed-supervisor assertion below works on
	// every Unix rather than only where /proc exists.
	script := `echo uid=$(id -u) > "$1"
echo gid=$(id -g) >> "$1"
echo ppid=$PPID >> "$1"`

	command := exec.Command("/bin/sh", "-c", script, "probe", status)
	command.Dir = "/"
	command.Env = []string{"PATH=/usr/bin:/bin"}
	configureHermesProcess(command)

	tree, err := startContainedProcess(command)
	if err != nil {
		t.Fatalf("ordinary start: %v", err)
	}

	// No credential is requested. The child is the identity we already are, so
	// there is nothing to drop and nothing to assert privilege for.
	if command.SysProcAttr != nil && command.SysProcAttr.Credential != nil {
		t.Fatalf("ordinary launch requested credential %#v", command.SysProcAttr.Credential)
	}

	// Ordinary mode publishes no provider-descendant inventory, not even zero.
	if count, available := tree.descendantCount(); available {
		t.Fatalf("ordinary execution published a descendant inventory of %d", count)
	}

	// Let the child finish on its own before completing the boundary; the
	// teardown ladder would otherwise signal it before it wrote its probe.
	wait := tree.directChild(command)
	select {
	case <-wait.done:
	case <-time.After(10 * time.Second):
		t.Fatal("ordinary child did not exit")
	}

	if err := tree.complete(10 * time.Second); err != nil {
		t.Fatalf("ordinary completion: %v", err)
	}

	if err := tree.close(); err != nil {
		t.Fatalf("ordinary close: %v", err)
	}

	contents, err := os.ReadFile(status)
	if err != nil {
		t.Fatalf("read ordinary identity probe: %v", err)
	}

	observed := map[string]string{}

	for _, line := range strings.Split(strings.TrimSpace(string(contents)), "\n") {
		key, value, ok := strings.Cut(line, "=")
		if ok {
			observed[key] = value
		}
	}

	// The direct child is parented by this process. An interposed supervisor
	// would show a different PPid, which is what proves no guardian was started.
	if observed["ppid"] != strconv.Itoa(os.Getpid()) {
		t.Fatalf("ordinary child PPid = %q, want %d", observed["ppid"], os.Getpid())
	}

	return observed
}

// TestProcessIsolationOmissionAllowsOrdinaryUser proves an omitted policy runs
// native work as the caller's ordinary non-root identity, with no credential
// change, no authority claim, no interposed supervisor, and no descendant
// inventory.
func TestProcessIsolationOmissionAllowsOrdinaryUser(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("this case names the non-root identity; the root case is covered separately")
	}

	observed := runOrdinaryIdentityProbe(t)

	if observed["uid"] != strconv.Itoa(os.Geteuid()) || observed["gid"] != strconv.Itoa(os.Getegid()) {
		t.Fatalf("ordinary child identity = uid %q gid %q, want uid %d gid %d",
			observed["uid"], observed["gid"], os.Geteuid(), os.Getegid())
	}
}

// TestProcessIsolationOmissionAllowsRoot proves the same for a root caller.
// Root is an ordinary identity here rather than a trusted supervisor: omission
// requests no descent, so nothing about the launch changes because the caller
// happens to be uid 0.
func TestProcessIsolationOmissionAllowsRoot(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("this case names the root identity")
	}

	observed := runOrdinaryIdentityProbe(t)

	if observed["uid"] != "0" || observed["gid"] != strconv.Itoa(os.Getegid()) {
		t.Fatalf("ordinary root child identity = uid %q gid %q", observed["uid"], observed["gid"])
	}
}
