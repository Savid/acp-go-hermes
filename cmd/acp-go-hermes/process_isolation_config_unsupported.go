//go:build !linux

package main

import "fmt"

func loadProcessIsolationConfig(string) (processIsolationConfig, error) {
	return processIsolationConfig{}, fmt.Errorf("standalone native mode is supported only on linux")
}
