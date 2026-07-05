//go:build unix

package hermesacp

import (
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

var (
	hermesSyscallGetpgid = syscall.Getpgid
	hermesSyscallKill    = syscall.Kill
	procReadFile         = os.ReadFile
)

func configureHermesProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
}

func terminateHermesProcess(cmd *exec.Cmd) error {
	return signalHermesProcessGroup(cmd, syscall.SIGTERM)
}

func killHermesProcess(cmd *exec.Cmd) error {
	return signalHermesProcessGroup(cmd, syscall.SIGKILL)
}

func signalHermesProcessGroup(cmd *exec.Cmd, signal syscall.Signal) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	pgid, err := hermesSyscallGetpgid(cmd.Process.Pid)
	if err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return err
	}
	if err := hermesSyscallKill(-pgid, signal); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return err
	}

	return nil
}

func terminateProcessGroupID(pid int) error {
	return signalLeaseGroup(pid, syscall.SIGTERM)
}

func killProcessGroupID(pid int) error {
	return signalLeaseGroup(pid, syscall.SIGKILL)
}

func signalLeaseGroup(pid int, signal syscall.Signal) error {
	if pid <= 0 {
		return nil
	}
	target := pid
	if pgid, err := hermesSyscallGetpgid(pid); err == nil {
		target = -pgid
	}
	if err := hermesSyscallKill(target, signal); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return err
	}
	return nil
}

func inspectHermesProcess(pid int) (processIdentity, error) {
	if pid <= 0 {
		return processIdentity{}, syscall.ESRCH
	}
	stat, err := procReadFile(procPath(pid, "stat"))
	if err != nil {
		return processIdentity{}, err
	}
	startTime, err := procStartTime(string(stat))
	if err != nil {
		return processIdentity{}, err
	}
	cmdlineData, err := procReadFile(procPath(pid, "cmdline"))
	if err != nil {
		return processIdentity{}, err
	}
	envData, err := procReadFile(procPath(pid, "environ"))
	if err != nil {
		return processIdentity{}, err
	}

	return processIdentity{
		StartTime: startTime,
		Cmdline:   splitProcNUL(cmdlineData),
		Env:       splitProcEnv(envData),
	}, nil
}

func procPath(pid int, name string) string {
	return "/proc/" + strconv.Itoa(pid) + "/" + name
}

func procStartTime(stat string) (string, error) {
	closeParen := strings.LastIndex(stat, ")")
	if closeParen < 0 || closeParen+2 >= len(stat) {
		return "", errors.New("malformed proc stat")
	}
	fields := strings.Fields(stat[closeParen+2:])
	if len(fields) < 20 {
		return "", errors.New("proc stat missing start time")
	}
	return fields[19], nil
}

func splitProcNUL(data []byte) []string {
	raw := strings.Split(strings.TrimRight(string(data), "\x00"), "\x00")
	out := make([]string, 0, len(raw))
	for _, value := range raw {
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func splitProcEnv(data []byte) map[string]string {
	env := map[string]string{}
	for _, entry := range splitProcNUL(data) {
		key, value, ok := strings.Cut(entry, "=")
		if ok && key != "" {
			env[key] = value
		}
	}
	return env
}
