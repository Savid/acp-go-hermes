//go:build darwin

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
	if pid <= 0 {
		return ProcessIdentity{}, syscall.ESRCH
	}

	kinfo, err := darwinSysctlKinfoProc("kern.proc.pid", pid)
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

	start := kinfo.Proc.P_starttime

	return ProcessIdentity{
		StartTime: strconv.FormatInt(start.Sec, 10) + "." + strconv.FormatInt(int64(start.Usec), 10),
		Cmdline:   cmdline,
		Env:       env,
	}, nil
}

// parseProcArgs2 decodes a kern.procargs2 sysctl buffer: an int32 argc, the
// executable path, NUL padding, argc NUL-terminated argv entries, then
// NUL-terminated environment entries terminated by an empty entry.
func parseProcArgs2(data []byte) ([]string, map[string]string, error) {
	if len(data) < 4 {
		return nil, nil, errors.New("procargs2 buffer too short")
	}

	argc := int(binary.NativeEndian.Uint32(data[:4]))
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
		if end < 0 {
			return nil, nil, errors.New("procargs2 truncated argv")
		}

		cmdline = append(cmdline, string(rest[:end]))
		rest = rest[end+1:]
	}

	env := map[string]string{}

	for len(rest) > 0 {
		end := bytes.IndexByte(rest, 0)

		var entry string
		if end < 0 {
			entry, rest = string(rest), nil
		} else {
			entry, rest = string(rest[:end]), rest[end+1:]
		}

		if entry == "" {
			break
		}

		key, value, ok := strings.Cut(entry, "=")
		if ok && key != "" {
			env[key] = value
		}
	}

	return cmdline, env, nil
}
