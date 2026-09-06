//go:build windows

package hermes

import (
	"errors"
)

func EnsureLocalSharedHermesHome(string) error {
	return errors.New("shared Hermes home is unsupported on windows: inherited session-owner lock handles are unavailable")
}
