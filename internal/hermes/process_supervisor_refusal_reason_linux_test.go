//go:build linux

package hermes

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

// TestHermesSupervisorRefusalReasonReachesTheGuardian proves the whole point of
// the terminal readiness frame. The liveness supervisor's refusal reason used to
// exist only as a stderr line nobody correlated, so the guardian — the reader on
// the other end of the status pipe — saw the pipe close and reported the same
// wordless verdict whatever the cause. Publishing the frame and reading it back
// is what turns that into a named refusal.
func TestHermesSupervisorRefusalReasonReachesTheGuardian(t *testing.T) {
	const reason = "fork/exec /usr/bin/hermes: no such file or directory"

	status := &bytes.Buffer{}
	refuseHermesSupervisorReadiness(status, true, "start native target", errors.New(reason))

	frame := status.String()
	if !strings.HasPrefix(frame, hermesSupervisorRefusal) {
		t.Fatalf("refusal frame = %q", frame)
	}

	report := hermesLivenessRefusal(frame, nil)
	if !strings.Contains(report, "liveness refused to start") {
		t.Fatalf("guardian report = %q", report)
	}
	if !strings.Contains(report, "start native target") || !strings.Contains(report, reason) {
		t.Fatalf("guardian report dropped the reason: %q", report)
	}
}

// TestHermesSupervisorRefusalIsGuardianOnly pins that the frame belongs to the
// liveness protocol alone. The guardian publishes no readiness of its own, so a
// non-liveness refusal must leave the status channel untouched rather than
// inventing a frame nothing is reading.
func TestHermesSupervisorRefusalIsGuardianOnly(t *testing.T) {
	status := &bytes.Buffer{}
	refuseHermesSupervisorReadiness(status, false, "acquire native authority", errors.New("operation not permitted"))

	if status.Len() != 0 {
		t.Fatalf("non-liveness refusal published %q", status.String())
	}
}

// TestHermesWordlessLivenessDeathIsNotAClaimedRefusal pins the other half of the
// contract. The frame names a refusal that had something to say; a supervisor
// that dies without writing one has not said anything, and the guardian must
// report that read failure rather than dress it up as a named refusal.
func TestHermesWordlessLivenessDeathIsNotAClaimedRefusal(t *testing.T) {
	report := hermesLivenessRefusal("", io.EOF)
	if strings.Contains(report, "refused to start") {
		t.Fatalf("wordless death claimed a named refusal: %q", report)
	}
	if !strings.Contains(report, "await liveness readiness") || !strings.Contains(report, "EOF") {
		t.Fatalf("wordless death report = %q", report)
	}

	report = hermesLivenessRefusal("garbage\n", nil)
	if strings.Contains(report, "refused to start") {
		t.Fatalf("unparsable readiness claimed a named refusal: %q", report)
	}
	if !strings.Contains(report, `invalid liveness readiness "garbage"`) {
		t.Fatalf("unparsable readiness report = %q", report)
	}
}
