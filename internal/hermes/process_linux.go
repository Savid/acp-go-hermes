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

func configureHermesProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
}

func inspectHermesProcess(pid int) (ProcessIdentity, error) {
	if pid <= 0 {
		return ProcessIdentity{}, syscall.ESRCH
	}

	stat, err := procReadFile(procPath(pid, "stat"))
	if err != nil {
		return ProcessIdentity{}, err
	}

	startTime, err := procStartTime(string(stat))
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
