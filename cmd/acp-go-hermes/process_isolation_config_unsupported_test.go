//go:build !linux

package main

import (
	"strings"
	"testing"
)

func TestProcessIsolationConfigRefusesUnsupportedPlatform(t *testing.T) {
	if _, err := loadProcessIsolationConfig("/etc/acp-go/policy.json"); err == nil || !strings.Contains(err.Error(), "supported only on linux") {
		t.Fatalf("unsupported-platform error = %v", err)
	}
}
