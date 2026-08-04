//go:build linux

package hermes

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"golang.org/x/sys/unix"
)

const agentIdentityLockRetry = 10 * time.Millisecond

var (
	agentIdentityLockRunRoot    = "/run"
	agentIdentityLockTrustedUID = uint32(0)
	agentIdentityLockTrustedGID = uint32(0)
)

type agentIdentityLock struct {
	file *os.File
}

func acquireAgentIdentityLock(uid uint32, testOnly bool, canceled <-chan struct{}, signals <-chan os.Signal) (*agentIdentityLock, error) {
	if testOnly {
		root := filepath.Join(os.TempDir(), "acp-go-hermes-agent-identities-"+strconv.Itoa(os.Getppid()))
		if err := os.Mkdir(root, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("create test agent identity runtime root: %w", err)
		}
		oldRoot, oldUID, oldGID := agentIdentityLockRunRoot, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID
		agentIdentityLockRunRoot = root
		agentIdentityLockTrustedUID = uint32(os.Geteuid())
		agentIdentityLockTrustedGID = uint32(os.Getegid())
		defer func() {
			agentIdentityLockRunRoot, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID = oldRoot, oldUID, oldGID
		}()
	}
	directory, err := openAgentIdentityLockDirectory()
	if err != nil {
		return nil, err
	}
	defer directory.Close()

	name := strconv.FormatUint(uint64(uid), 10) + ".lock"
	fd, err := unix.Openat(int(directory.Fd()), name, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open agent identity lock %s: %w", name, err)
	}

	file := os.NewFile(uintptr(fd), name)
	if err := validateAgentIdentityLockFile(file); err != nil {
		_ = file.Close()
		return nil, err
	}

	for {
		select {
		case <-canceled:
			_ = file.Close()
			return nil, errors.New("agent identity lock canceled")
		case signal := <-signals:
			_ = file.Close()
			return nil, fmt.Errorf("agent identity lock interrupted by %s", signal)
		default:
		}

		err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return &agentIdentityLock{file: file}, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			_ = file.Close()
			return nil, fmt.Errorf("lock agent identity %d: %w", uid, err)
		}

		timer := time.NewTimer(agentIdentityLockRetry)
		select {
		case <-canceled:
			if !timer.Stop() {
				<-timer.C
			}
			_ = file.Close()
			return nil, errors.New("agent identity lock canceled")
		case signal := <-signals:
			if !timer.Stop() {
				<-timer.C
			}
			_ = file.Close()
			return nil, fmt.Errorf("agent identity lock interrupted by %s", signal)
		case <-timer.C:
		}
	}
}

func openAgentIdentityLockDirectory() (*os.File, error) {
	fd, err := unix.Open(agentIdentityLockRunRoot, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open agent identity runtime root: %w", err)
	}
	run := os.NewFile(uintptr(fd), agentIdentityLockRunRoot)
	if err := validateAgentIdentityDirectory(run, false); err != nil {
		_ = run.Close()
		return nil, fmt.Errorf("validate agent identity runtime root: %w", err)
	}

	wagie, err := openOrCreateAgentIdentityDirectory(int(run.Fd()), "wagie", 0o700)
	_ = run.Close()
	if err != nil {
		return nil, fmt.Errorf("open agent identity owner directory: %w", err)
	}

	directory, err := openOrCreateAgentIdentityDirectory(int(wagie.Fd()), "agent-identities", 0o700)
	_ = wagie.Close()
	if err != nil {
		return nil, fmt.Errorf("open agent identity lock directory: %w", err)
	}

	return directory, nil
}

func openOrCreateAgentIdentityDirectory(parentFD int, name string, mode uint32) (*os.File, error) {
	err := unix.Mkdirat(parentFD, name, mode)
	if err != nil && !errors.Is(err, unix.EEXIST) {
		return nil, err
	}

	fd, err := unix.Openat(parentFD, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}

	file := os.NewFile(uintptr(fd), name)
	if err := validateAgentIdentityDirectory(file, true); err != nil {
		_ = file.Close()
		return nil, err
	}

	return file, nil
}

func validateAgentIdentityDirectory(file *os.File, exactMode bool) error {
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Uid != agentIdentityLockTrustedUID || stat.Gid != agentIdentityLockTrustedGID {
		return errors.New("agent identity directory must be trusted-owned")
	}
	permissions := stat.Mode & 0o777
	if exactMode && permissions != 0o700 {
		return fmt.Errorf("agent identity directory mode is %#o, want 0700", permissions)
	}
	if !exactMode && permissions&0o022 != 0 {
		return errors.New("agent identity runtime root must not be group- or world-writable")
	}

	return nil
}

func validateAgentIdentityLockFile(file *os.File) error {
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Uid != agentIdentityLockTrustedUID || stat.Gid != agentIdentityLockTrustedGID || stat.Nlink != 1 {
		return errors.New("agent identity lock must be a trusted-owned regular file with one link")
	}
	if permissions := stat.Mode & 0o777; permissions != 0o600 {
		return fmt.Errorf("agent identity lock mode is %#o, want 0600", permissions)
	}

	return nil
}

func (lock *agentIdentityLock) Close() error {
	if lock == nil || lock.file == nil {
		return nil
	}
	fd := int(lock.file.Fd())
	unlockErr := unix.Flock(fd, unix.LOCK_UN)
	closeErr := lock.file.Close()
	lock.file = nil

	return errors.Join(unlockErr, closeErr)
}
