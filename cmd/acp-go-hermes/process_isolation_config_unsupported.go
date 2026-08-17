//go:build !linux

package main

import "fmt"

func loadProcessIsolationConfig(string) (processIsolationConfig, error) {
	// Ordinary standalone native mode is supported here; only the explicitly
	// requested isolation policy is Linux-only, and this loader runs solely
	// because a policy path was supplied.
	return processIsolationConfig{}, fmt.Errorf("explicit process isolation is supported only on linux")
}
