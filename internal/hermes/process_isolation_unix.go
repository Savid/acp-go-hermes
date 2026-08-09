//go:build unix

package hermes

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

var (
	processIsolationGeteuid   = os.Geteuid
	processIsolationGetegid   = os.Getegid
	processIsolationGetgroups = os.Getgroups
)

func validateProcessIsolationPlatform() error { return nil }

// sharedProcessIdentity reports whether the native identity is the identity the
// supervisor already runs as. Nothing separates the two ends of the launch in
// that shape, so every step that exists to cross the boundary has nothing to
// cross. A zero effective uid never qualifies: the supervisor holds the trusted
// identity there, and a nonzero native uid is required everywhere, so the two
// can never name the same identity. Only the Linux backend recognises the
// shape; the Darwin backend states its own boundary and is left as it is.
func sharedProcessIdentity(isolation *ProcessIsolation) bool {
	if isolation == nil || processIsolationPlatform != processPlatformLinux {
		return false
	}

	effectiveUID := processIsolationGeteuid()

	return effectiveUID > 0 && uint64(isolation.UID) == uint64(effectiveUID)
}

func applyProcessIsolation(cmd *exec.Cmd, isolation *ProcessIsolation) error {
	if err := validateProcessIsolation(isolation); err != nil {
		return err
	}

	if cmd == nil {
		return fmt.Errorf("process isolation command is nil")
	}

	if isolation.TestOnlyNoCredential {
		return nil
	}

	uid, gid := int64(processIsolationGeteuid()), int64(processIsolationGetegid())

	// Requesting no credential change at all is the only honest instruction
	// when the native identity is already the running one. The supplementary
	// groups belong to the account the supervisor was started under, and an
	// unprivileged process can neither shed them nor re-enter them. A native
	// group it could not enter is still refused, because emitting nothing would
	// otherwise run the agent in a group the policy never named.
	if sharedProcessIdentity(isolation) {
		if gid != int64(isolation.GID) {
			return fmt.Errorf(
				"native group %d cannot be entered from group %d; %s",
				isolation.GID, gid, sharedIdentitySupervisorRemedy,
			)
		}

		return nil
	}

	if uid == int64(isolation.UID) && gid == int64(isolation.GID) {
		return verifyProcessIsolation(isolation)
	}

	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}

	cmd.SysProcAttr.Credential = &syscall.Credential{Uid: isolation.UID, Gid: isolation.GID, Groups: []uint32{}}

	return nil
}

func verifyProcessIsolation(isolation *ProcessIsolation) error {
	if err := validateProcessIsolation(isolation); err != nil {
		return err
	}

	uid, gid := int64(processIsolationGeteuid()), int64(processIsolationGetegid())
	if uid != int64(isolation.UID) || gid != int64(isolation.GID) {
		return fmt.Errorf("process identity is uid=%d gid=%d, want uid=%d gid=%d", uid, gid, isolation.UID, isolation.GID)
	}

	groups, err := processIsolationGetgroups()
	if err != nil {
		return fmt.Errorf("read supplementary groups: %w", err)
	}

	if len(groups) != 0 {
		return fmt.Errorf("unexpected supplementary groups %v", groups)
	}

	return nil
}
