package hermesacp

import (
	"reflect"
	"strings"
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

	if got := ambientEnvironment(Options{}); !reflect.DeepEqual(got, want) {
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

// TestWithAmbientEnvironmentReplacesTheAdapterEnvironment proves a supplied
// block stands in for the adapter's own environment: it is the snapshot every
// ordinary launch inherits from, and the adapter's environment is never read.
func TestWithAmbientEnvironmentReplacesTheAdapterEnvironment(t *testing.T) {
	original := captureAmbientEnvironment
	t.Cleanup(func() { captureAmbientEnvironment = original })

	captureAmbientEnvironment = func() []string {
		t.Error("the adapter environment was read despite a supplied ambient block")

		return []string{"HOME=/adapter/home"}
	}

	supplied := map[string]string{"HOME": "/host/home", "PATH": "/host/bin", "OPENAI_API_KEY": "ambient-key"}
	agent := newTestAgent(WithAmbientEnvironment(supplied))

	if err := agent.optionsErr; err != nil {
		t.Fatalf("options error = %v", err)
	}

	if !reflect.DeepEqual(agent.ambientEnv, supplied) {
		t.Fatalf("ambient environment = %#v, want %#v", agent.ambientEnv, supplied)
	}
}

// TestWithAmbientEnvironmentEmptyBlockInheritsNothing proves an empty supplied
// block is a block, not an omission.
func TestWithAmbientEnvironmentEmptyBlockInheritsNothing(t *testing.T) {
	original := captureAmbientEnvironment
	t.Cleanup(func() { captureAmbientEnvironment = original })

	captureAmbientEnvironment = func() []string { return []string{"PATH=/adapter/bin"} }

	agent := newTestAgent(WithAmbientEnvironment(map[string]string{}))
	if len(agent.ambientEnv) != 0 {
		t.Fatalf("ambient environment = %#v, want empty", agent.ambientEnv)
	}
}

func TestWithAmbientEnvironmentRefusesMalformedEntries(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
		want string
	}{
		{name: "empty key", env: map[string]string{"": "x"}, want: `key "" is not a variable name`},
		{name: "equals in key", env: map[string]string{"A=B": "x"}, want: `key "A=B" is not a variable name`},
		{name: "nul in key", env: map[string]string{"A\x00B": "x"}, want: "is not a variable name"},
		{name: "nul in value", env: map[string]string{"A": "x\x00y"}, want: `value for "A" contains NUL`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := newTestAgent(WithAmbientEnvironment(tc.env)).optionsErr
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("options error = %v, want %q", err, tc.want)
			}
		})
	}
}
