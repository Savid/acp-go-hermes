//go:build windows

package hermes

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

type processContainment struct {
	job windows.Handle
}

type jobBasicAccounting struct {
	TotalUserTime             int64
	TotalKernelTime           int64
	ThisPeriodTotalUserTime   int64
	ThisPeriodTotalKernelTime int64
	TotalPageFaultCount       uint32
	TotalProcesses            uint32
	ActiveProcesses           uint32
	TotalTerminatedProcesses  uint32
}

func configureHermesProcess(*exec.Cmd) {}

func startContainedProcess(cmd *exec.Cmd) (*processContainment, error) {
	job, err := createProcessJob()
	if err != nil {
		return nil, err
	}

	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}

	cmd.SysProcAttr.CreationFlags |= windows.CREATE_NEW_PROCESS_GROUP | windows.CREATE_SUSPENDED
	if err := cmd.Start(); err != nil {
		_ = windows.CloseHandle(job)

		return nil, err
	}

	containment := &processContainment{job: job}
	process, err := windows.OpenProcess(
		windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE|windows.PROCESS_QUERY_INFORMATION,
		false,
		uint32(cmd.Process.Pid),
	)
	if err == nil {
		err = windows.AssignProcessToJobObject(job, process)
		_ = windows.CloseHandle(process)
	}

	if err == nil {
		err = resumePrimaryThread(uint32(cmd.Process.Pid))
	}

	if err != nil {
		cleanupErr := cleanupSuspendedProcess(cmd, containment)
		if cleanupErr != nil {
			return nil, fmt.Errorf("%w: assign suspended Hermes root to Windows Job Object: %v; cleanup: %v", ErrProcessTreeUnproven, err, cleanupErr)
		}

		return nil, fmt.Errorf("assign suspended Hermes root to Windows Job Object: %w", err)
	}

	return containment, nil
}

func createProcessJob() (windows.Handle, error) {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil && job == 0 {
		return 0, fmt.Errorf("create Windows Job Object: %w", err)
	}

	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	info.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err := windows.SetInformationJobObject(
		job,
		windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&info)),
		uint32(unsafe.Sizeof(info)),
	); err != nil {
		_ = windows.CloseHandle(job)

		return 0, fmt.Errorf("set Windows Job Object kill-on-close: %w", err)
	}

	return job, nil
}

func resumePrimaryThread(pid uint32) error {
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPTHREAD, 0)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(snapshot) //nolint:errcheck // Best-effort handle cleanup.

	entry := windows.ThreadEntry32{Size: uint32(unsafe.Sizeof(windows.ThreadEntry32{}))}
	if err := windows.Thread32First(snapshot, &entry); err != nil {
		return err
	}

	for {
		if entry.OwnerProcessID == pid {
			thread, openErr := windows.OpenThread(windows.THREAD_SUSPEND_RESUME, false, entry.ThreadID)
			if openErr != nil {
				return openErr
			}

			_, resumeErr := windows.ResumeThread(thread)
			_ = windows.CloseHandle(thread)

			return resumeErr
		}

		entry.Size = uint32(unsafe.Sizeof(windows.ThreadEntry32{}))
		if err := windows.Thread32Next(snapshot, &entry); err != nil {
			if errors.Is(err, syscall.ERROR_NO_MORE_FILES) {
				return fmt.Errorf("primary thread for process %d was not found", pid)
			}

			return err
		}
	}
}

func cleanupSuspendedProcess(cmd *exec.Cmd, containment *processContainment) error {
	var cleanupErr error
	_ = windows.TerminateJobObject(containment.job, 1)

	if killErr := cmd.Process.Kill(); killErr != nil && !errors.Is(killErr, os.ErrProcessDone) {
		cleanupErr = errors.Join(cleanupErr, killErr)
	}

	if waitErr := cmd.Wait(); waitErr != nil {
		var exitErr *exec.ExitError
		if !errors.As(waitErr, &exitErr) {
			cleanupErr = errors.Join(cleanupErr, waitErr)
		}
	}
	cleanupErr = errors.Join(cleanupErr, containment.quiesce(time.Second))
	cleanupErr = errors.Join(cleanupErr, containment.close())

	return cleanupErr
}

func terminateProcess(cmd *exec.Cmd) error { return killProcess(cmd) }

func killProcess(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}

	if err := cmd.Process.Kill(); err != nil {
		if errors.Is(err, os.ErrProcessDone) {
			return nil
		}

		return err
	}

	return nil
}

func (c *processContainment) quiesce(timeout time.Duration) error {
	active, err := c.activeProcesses()
	if err != nil {
		return err
	}

	if active > 0 {
		if err := windows.TerminateJobObject(c.job, 1); err != nil {
			return fmt.Errorf("terminate Windows Job Object: %w", err)
		}
	}

	if timeout <= 0 {
		timeout = time.Second
	}

	deadline := time.Now().Add(timeout)
	for {
		active, err = c.activeProcesses()
		if err != nil {
			return err
		}

		if active == 0 {
			return nil
		}

		if !time.Now().Before(deadline) {
			return fmt.Errorf("Windows Job Object retained %d active processes", active)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (c *processContainment) activeProcesses() (uint32, error) {
	info := jobBasicAccounting{}
	if err := windows.QueryInformationJobObject(
		c.job,
		windows.JobObjectBasicAccountingInformation,
		uintptr(unsafe.Pointer(&info)),
		uint32(unsafe.Sizeof(info)),
		nil,
	); err != nil {
		return 0, fmt.Errorf("query Windows Job Object process count: %w", err)
	}

	return info.ActiveProcesses, nil
}

func (c *processContainment) descendantCount() (int, bool) {
	active, err := c.activeProcesses()
	if err != nil {
		return 0, false
	}

	return int(active), true
}

func (c *processContainment) close() error { return windows.CloseHandle(c.job) }
