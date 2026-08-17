//go:build !linux && !darwin && !windows

package hermes

import "fmt"

func ensureLocalSharedHermesHome(string) error {
	return fmt.Errorf("shared Hermes home filesystem locality is unsupported on this platform")
}
