//go:build unix

package main

import (
	"os"
	"syscall"
)

func forwardedSignals() []os.Signal {
	return []os.Signal{os.Interrupt, syscall.SIGTERM, syscall.SIGHUP}
}

func signalCode(sig os.Signal) int {
	if sys, ok := sig.(syscall.Signal); ok {
		return 128 + int(sys)
	}

	return 1
}
