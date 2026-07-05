//go:build darwin

package hermesacp

import (
	"errors"
	"os"
	"os/exec"
	"testing"

	"golang.org/x/sys/unix"
)

func TestConfigureHermesProcessDarwin(t *testing.T) {
	cmd := exec.Command("true")
	configureHermesProcess(cmd)
	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setpgid {
		t.Fatalf("SysProcAttr = %#v, want Setpgid", cmd.SysProcAttr)
	}
}

func TestInspectHermesProcessSelf(t *testing.T) {
	identity, err := inspectHermesProcess(os.Getpid())
	if err != nil {
		t.Fatalf("inspect self: %v", err)
	}
	if identity.StartTime == "" {
		t.Fatal("empty start time")
	}
	if len(identity.Cmdline) == 0 {
		t.Fatal("empty cmdline")
	}
	if len(identity.Env) == 0 {
		t.Fatal("empty environment")
	}
}

func TestInspectHermesProcessDarwinBranches(t *testing.T) {
	oldKinfo := darwinSysctlKinfoProc
	oldProcArgs := darwinSysctlProcArgs
	t.Cleanup(func() {
		darwinSysctlKinfoProc = oldKinfo
		darwinSysctlProcArgs = oldProcArgs
	})

	if _, err := inspectHermesProcess(0); err == nil {
		t.Fatal("zero pid inspected successfully")
	}

	darwinSysctlKinfoProc = func(string, ...int) (*unix.KinfoProc, error) {
		return nil, errors.New("kinfo failed")
	}
	if _, err := inspectHermesProcess(123); err == nil {
		t.Fatal("kinfo error ignored")
	}

	kinfo := &unix.KinfoProc{}
	kinfo.Proc.P_starttime = unix.Timeval{Sec: 12, Usec: 345}
	darwinSysctlKinfoProc = func(string, ...int) (*unix.KinfoProc, error) {
		return kinfo, nil
	}
	darwinSysctlProcArgs = func(int) ([]byte, error) {
		return nil, errors.New("procargs failed")
	}
	if _, err := inspectHermesProcess(123); err == nil {
		t.Fatal("procargs error ignored")
	}

	darwinSysctlProcArgs = func(int) ([]byte, error) {
		return []byte{0, 0}, nil
	}
	if _, err := inspectHermesProcess(123); err == nil {
		t.Fatal("malformed procargs accepted")
	}

	darwinSysctlProcArgs = func(int) ([]byte, error) {
		return procArgs2Buffer(2, "/usr/bin/hermes", []string{"hermes", "serve"}, []string{"HERMES_HOME=/tmp/home", "HERMES_DASHBOARD_SESSION_TOKEN=secret"}), nil
	}
	identity, err := inspectHermesProcess(123)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if identity.StartTime != "12.345" {
		t.Fatalf("start time = %q", identity.StartTime)
	}
	if len(identity.Cmdline) != 2 || identity.Cmdline[0] != "hermes" || identity.Cmdline[1] != "serve" {
		t.Fatalf("cmdline = %#v", identity.Cmdline)
	}
	if identity.Env["HERMES_HOME"] != "/tmp/home" || identity.Env["HERMES_DASHBOARD_SESSION_TOKEN"] != "secret" {
		t.Fatalf("env = %#v", identity.Env)
	}
}

func TestParseProcArgs2Branches(t *testing.T) {
	if _, _, err := parseProcArgs2([]byte{1, 0}); err == nil {
		t.Fatal("short buffer accepted")
	}
	if _, _, err := parseProcArgs2([]byte{1, 0, 0, 0, 'a', 'b'}); err == nil {
		t.Fatal("missing exec path terminator accepted")
	}
	if _, _, err := parseProcArgs2(append([]byte{2, 0, 0, 0}, "/bin/x\x00\x00hermes\x00"...)); err == nil {
		t.Fatal("truncated argv accepted")
	}

	data := append([]byte{1, 0, 0, 0}, "/bin/x\x00\x00\x00hermes\x00A=1\x00NOEQUALS\x00=skipped\x00\x00B=2\x00"...)
	cmdline, env, err := parseProcArgs2(data)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(cmdline) != 1 || cmdline[0] != "hermes" {
		t.Fatalf("cmdline = %#v", cmdline)
	}
	if len(env) != 1 || env["A"] != "1" {
		t.Fatalf("env = %#v, want stop at empty entry", env)
	}

	data = append([]byte{0, 0, 0, 0}, "/bin/x\x00C=3"...)
	cmdline, env, err = parseProcArgs2(data)
	if err != nil || len(cmdline) != 0 {
		t.Fatalf("cmdline = %#v err = %v", cmdline, err)
	}
	if env["C"] != "3" {
		t.Fatalf("env = %#v, want unterminated trailing entry", env)
	}
}

func procArgs2Buffer(argc byte, execPath string, args, env []string) []byte {
	data := []byte{argc, 0, 0, 0}
	data = append(data, execPath...)
	data = append(data, 0, 0, 0)
	for _, arg := range args {
		data = append(data, arg...)
		data = append(data, 0)
	}
	for _, entry := range env {
		data = append(data, entry...)
		data = append(data, 0)
	}
	data = append(data, 0)
	return data
}
