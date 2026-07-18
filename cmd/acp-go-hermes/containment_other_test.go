//go:build !darwin

package main

import (
	"bytes"
	"strings"
	"testing"
)

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
