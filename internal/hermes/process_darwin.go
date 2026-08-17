//go:build darwin

//nolint:wsl_v5,nlreturn // Strict procargs parsing stages intentionally remain contiguous.
package hermes

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os/exec"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

var (
	darwinSysctlKinfoProc = unix.SysctlKinfoProc
	darwinSysctlProcArgs  = func(pid int) ([]byte, error) {
		return unix.SysctlRaw("kern.procargs2", pid)
	}
)

func configureHermesProcess(cmd *exec.Cmd) {
	// Darwin has no Pdeathsig equivalent; parent-death cleanup is best-effort
	// via process-group signalling and stale-lease reaping.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func inspectHermesProcess(pid int) (ProcessIdentity, error) {
	startTime, err := inspectHermesProcessStartTime(pid)
	if err != nil {
		return ProcessIdentity{}, err
	}

	raw, err := darwinSysctlProcArgs(pid)
	if err != nil {
		return ProcessIdentity{}, err
	}

	cmdline, env, err := parseProcArgs2(raw)
	if err != nil {
		return ProcessIdentity{}, err
	}

	return ProcessIdentity{
		StartTime: startTime,
		Cmdline:   cmdline,
		Env:       env,
	}, nil
}

func inspectHermesProcessStartTime(pid int) (string, error) {
	if pid <= 0 {
		return "", syscall.ESRCH
	}

	kinfo, err := darwinSysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return "", err
	}

	start := kinfo.Proc.P_starttime

	return strconv.FormatInt(start.Sec, 10) + "." + strconv.FormatInt(int64(start.Usec), 10), nil
}

// parseProcArgs2 decodes a kern.procargs2 sysctl buffer: an int32 argc, the
// executable path, NUL padding, argc NUL-terminated argv entries, then
// NUL-terminated environment entries terminated by an empty entry.
func parseProcArgs2(data []byte) ([]string, map[string]string, error) {
	cmdline, entries, err := parseDarwinProcArgs(data)
	if err != nil {
		return nil, nil, err
	}
	env := make(map[string]string, len(entries))
	for _, entry := range entries {
		key, value, ok := strings.Cut(entry, "=")
		if ok && key != "" {
			env[key] = value
		}
	}
	return cmdline, env, nil
}

func parseDarwinProcArgs(data []byte) ([]string, []string, error) {
	if len(data) < 4 {
		return nil, nil, errors.New("procargs2 buffer too short")
	}

	argc := int(binary.NativeEndian.Uint32(data[:4]))
	if argc <= 0 || argc > len(data)-4 {
		return nil, nil, errors.New("procargs2 argument count is invalid")
	}
	rest := data[4:]

	execEnd := bytes.IndexByte(rest, 0)
	if execEnd < 0 {
		return nil, nil, errors.New("procargs2 missing executable path terminator")
	}

	rest = rest[execEnd:]
	for len(rest) > 0 && rest[0] == 0 {
		rest = rest[1:]
	}

	cmdline := make([]string, 0, argc)
	for len(cmdline) < argc {
		end := bytes.IndexByte(rest, 0)
		if end <= 0 {
			return nil, nil, errors.New("procargs2 truncated argv")
		}

		cmdline = append(cmdline, string(rest[:end]))
		rest = rest[end+1:]
	}

	env := make([]string, 0)
	terminated := false
	for len(rest) > 0 {
		end := bytes.IndexByte(rest, 0)
		if end < 0 {
			return nil, nil, errors.New("procargs2 environment is incomplete")
		}
		entry := string(rest[:end])
		rest = rest[end+1:]
		if entry == "" {
			if len(env) == 0 {
				return nil, nil, errors.New("procargs2 environment boundary is ambiguous")
			}
			terminated = true
			break
		}
		env = append(env, entry)
	}
	if !terminated {
		return nil, nil, errors.New("procargs2 environment is incomplete")
	}
	return cmdline, env, nil
}
