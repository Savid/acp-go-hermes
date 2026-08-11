package hermes

import (
	"errors"
	"fmt"
	"os"
)

var currentDurableProcessStartTime = inspectHermesProcessStartTime

// DurableProcessIdentity is the PID plus kernel-assigned process start
// identity needed to distinguish a live process from PID reuse.
type DurableProcessIdentity struct {
	PID             int    `json:"pid"`
	KernelStartTime string `json:"kernelStartTime"`
}

// CurrentDurableProcessIdentity captures the current process identity.
func CurrentDurableProcessIdentity() (DurableProcessIdentity, error) {
	pid := os.Getpid()

	start, err := currentDurableProcessStartTime(pid)
	if err != nil {
		return DurableProcessIdentity{}, fmt.Errorf("inspect current process identity: %w", err)
	}

	return DurableProcessIdentity{PID: pid, KernelStartTime: start}, nil
}

// DurableProcessIdentityGone proves that an exact PID/start-time pair no
// longer names a live process. Inspection uncertainty is returned as an error,
// never treated as process death.
func DurableProcessIdentityGone(identity DurableProcessIdentity) (bool, error) {
	if identity.PID <= 0 || identity.KernelStartTime == "" {
		return false, errors.New("durable process identity is incomplete")
	}

	return sharedSessionOwnerClaimGone(sharedSessionOwnerClaim(identity))
}
