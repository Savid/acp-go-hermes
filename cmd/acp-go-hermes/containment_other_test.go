//go:build !darwin

package main

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

type containmentOtherErrorWriter struct{}

func (containmentOtherErrorWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

func TestContainmentCommandsFailOperationallyOffDarwin(t *testing.T) {
	runtimeID := strings.Repeat("a", 32)
	for _, test := range []struct {
		args []string
		want string
	}{
		{args: []string{"diagnose", "-scratch-dir", "."}, want: "containment diagnostics are available only on darwin"},
		{args: []string{"cleanup", "-scratch-dir", ".", "-runtime-id", runtimeID, "-force"}, want: "containment cleanup is available only on darwin"},
	} {
		var stdout, stderr bytes.Buffer
		if code := runContainmentCommand(test.args, &stdout, &stderr); code != 1 {
			t.Fatalf("runContainmentCommand(%v) = %d, stderr=%q", test.args, code, stderr.String())
		}
		if stdout.Len() != 0 {
			t.Fatalf("runContainmentCommand(%v) stdout = %q", test.args, stdout.String())
		}
		if got := stderr.String(); got != "acp-go-hermes: "+test.want+"\n" {
			t.Fatalf("runContainmentCommand(%v) stderr = %q", test.args, got)
		}
	}
}

func TestContainmentCommandUsageOffDarwin(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := runContainmentCommand([]string{"diagnose"}, &stdout, &stderr); code != 2 {
		t.Fatalf("diagnose usage code = %d, stderr=%q", code, stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("diagnose usage stdout = %q", stdout.String())
	}
	if got := stderr.String(); got != "acp-go-hermes: containment diagnose requires -scratch-dir\n" {
		t.Fatalf("diagnose usage stderr = %q", got)
	}
}

func TestContainmentCommandValidationAndSuccessOffDarwin(t *testing.T) {
	runtimeID := strings.Repeat("a", 32)
	for _, args := range [][]string{
		{"other"},
		{"diagnose", "-bad"},
		{"diagnose", "-scratch-dir", ".", "extra"},
		{"cleanup", "-bad"},
		{"cleanup", "-scratch-dir", ".", "-runtime-id", runtimeID, "-force", "extra"},
		{"cleanup"},
		{"cleanup", "-runtime-id", runtimeID},
		{"cleanup", "-scratch-dir", ".", "-runtime-id", strings.Repeat("A", 32), "-force"},
		{"cleanup", "-scratch-dir", ".", "-runtime-id", runtimeID},
	} {
		if code := runContainmentCommand(args, io.Discard, io.Discard); code != 2 {
			t.Fatalf("runContainmentCommand(%v) = %d", args, code)
		}
	}
	if validContainmentRuntimeID(strings.Repeat("g", 32)) {
		t.Fatal("runtime ID accepted non-hex text")
	}

	originalDiagnose := containmentDiagnoseCommand
	originalCleanup := containmentCleanupCommand
	t.Cleanup(func() {
		containmentDiagnoseCommand = originalDiagnose
		containmentCleanupCommand = originalCleanup
	})
	containmentDiagnoseCommand = func(string) (containmentDiagnoseOutput, error) {
		return containmentDiagnoseOutput{Vendor: containmentVendor}, nil
	}
	if code := runContainmentCommand([]string{"diagnose", "-scratch-dir", "."}, io.Discard, io.Discard); code != 0 {
		t.Fatalf("diagnose success = %d", code)
	}
	containmentCleanupCommand = func(string, string, bool) (containmentCleanupOutput, error) {
		return containmentCleanupOutput{Vendor: containmentVendor}, nil
	}
	cleanupArgs := []string{"cleanup", "-scratch-dir", ".", "-runtime-id", runtimeID, "-force"}
	if code := runContainmentCommand(cleanupArgs, io.Discard, io.Discard); code != 0 {
		t.Fatalf("cleanup success = %d", code)
	}
	if code := runContainmentCommand(cleanupArgs, containmentOtherErrorWriter{}, io.Discard); code != 1 {
		t.Fatalf("cleanup encode failure = %d", code)
	}
	containmentCleanupCommand = func(string, string, bool) (containmentCleanupOutput, error) {
		return containmentCleanupOutput{Reportable: true}, errors.New("partial")
	}
	if code := runContainmentCommand(cleanupArgs, containmentOtherErrorWriter{}, io.Discard); code != 1 {
		t.Fatalf("partial cleanup encode failure = %d", code)
	}
}
