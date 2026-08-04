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
