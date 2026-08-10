package hermesacp

import (
	"reflect"
	"testing"
)

func TestAmbientEnvironmentSnapshotsTheAdapterEnvironment(t *testing.T) {
	original := captureAmbientEnvironment
	t.Cleanup(func() { captureAmbientEnvironment = original })

	captureAmbientEnvironment = func() []string {
		return []string{
			"PATH=/usr/bin",
			"EMPTY=",
			"WITH=EQUALS=SIGNS",
			// A bare entry names no variable and is dropped.
			"NOEQUALS",
			// Windows spells per-drive working directories with a leading '=';
			// the harness never reads one, so it is dropped too.
			"=C:=C:\\work",
		}
	}

	want := map[string]string{
		"PATH":  "/usr/bin",
		"EMPTY": "",
		"WITH":  "EQUALS=SIGNS",
	}

	if got := ambientEnvironment(); !reflect.DeepEqual(got, want) {
		t.Fatalf("ambient environment = %#v, want %#v", got, want)
	}
}

// TestAgentCapturesAmbientEnvironmentOnce proves the snapshot is taken at
// construction rather than per session, so a mutation between two sessions
// cannot change what the second one inherits.
func TestAgentCapturesAmbientEnvironmentOnce(t *testing.T) {
	original := captureAmbientEnvironment
	t.Cleanup(func() { captureAmbientEnvironment = original })

	captures := 0
	captureAmbientEnvironment = func() []string {
		captures++

		return []string{"PATH=/usr/bin"}
	}

	agent := newTestAgent()

	if captures != 1 {
		t.Fatalf("ambient captures = %d", captures)
	}

	if got := agent.ambientEnv["PATH"]; got != "/usr/bin" {
		t.Fatalf("captured PATH = %q", got)
	}

	captureAmbientEnvironment = func() []string {
		t.Error("the ambient environment was re-read after construction")

		return nil
	}

	if got := agent.ambientEnv["PATH"]; got != "/usr/bin" {
		t.Fatalf("PATH after later drift = %q", got)
	}
}
