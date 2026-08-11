//go:build linux

package hermes

import (
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

var procReadFile = os.ReadFile

const linuxProcessFlagExiting = 0x00000004

func configureHermesProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
}

func inspectHermesProcess(pid int) (ProcessIdentity, error) {
	startTime, err := inspectHermesProcessStartTime(pid)
	if err != nil {
		return ProcessIdentity{}, err
	}

	cmdlineData, err := procReadFile(procPath(pid, "cmdline"))
	if err != nil {
		return ProcessIdentity{}, err
	}

	envData, err := procReadFile(procPath(pid, "environ"))
	if err != nil {
		return ProcessIdentity{}, err
	}

	return ProcessIdentity{
		StartTime: startTime,
		Cmdline:   splitProcNUL(cmdlineData),
		Env:       splitProcEnv(envData),
	}, nil
}

func inspectHermesProcessStartTime(pid int) (string, error) {
	if pid <= 0 {
		return "", syscall.ESRCH
	}

	stat, err := procReadFile(procPath(pid, "stat"))
	if err != nil {
		return "", err
	}

	startTime, err := procStartTime(string(stat))
	if err != nil {
		return "", err
	}

	return startTime, nil
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

	flags, err := strconv.ParseUint(fields[6], 10, 64)
	if err != nil {
		return "", errors.New("proc stat contains malformed process flags")
	}
	// A killed child can remain visible in procfs as a zombie until PID 1
	// reaps it. It owns no executable state or open descriptors, so it cannot
	// drive a Hermes session and must not make a durable owner claim appear
	// live. X is the corresponding transient dead state. PF_EXITING closes the
	// same race just before procfs publishes Z: the task has irreversibly entered
	// kernel exit and cannot return to userspace even if its old state is still
	// visible for a few instructions.
	if fields[0] == "Z" || fields[0] == "X" || flags&linuxProcessFlagExiting != 0 {
		return "", syscall.ESRCH
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
