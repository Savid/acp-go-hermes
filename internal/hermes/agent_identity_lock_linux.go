//go:build linux

package hermes

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

var (
	agentIdentityLockRunRoot    = "/var/lib"
	agentIdentityLockTrustedUID = uint32(0)
	agentIdentityLockTrustedGID = uint32(0)
)

type agentIdentityLock struct {
	file *os.File
}

var agentIdentityLockFcntl = unix.FcntlInt
var agentIdentityDirectoryMkdirat = unix.Mkdirat
var agentIdentityDirectoryOpenat = unix.Openat
var agentIdentityDirectoryFchown = unix.Fchown
var agentIdentityDirectoryFchmod = unix.Fchmod
var agentIdentityDirectoryFsync = unix.Fsync
var agentIdentityDirectoryFstatat = unix.Fstatat
var agentIdentityDirectoryClose = func(file *os.File) error { return file.Close() }
var agentIdentityLockReadFile = os.ReadFile
var agentIdentityLockClose = func(file *os.File) error { return file.Close() }
var agentIdentityLockFstat = unix.Fstat
var agentIdentityLockOpenat = unix.Openat
var agentIdentityLockCloseFD = unix.Close

func bootstrapAgentIdentityLockDirectory(runRoot string, trustedUID, trustedGID uint32) (*os.File, error) {
	run, err := openAgentIdentityRuntimeRoot(runRoot, trustedUID, trustedGID)
	if err != nil {
		return nil, err
	}

	acpGo, err := bootstrapAgentIdentityDirectory(run, "acp-go", trustedUID, trustedGID)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("bootstrap agent identity owner directory: %w", err), run.Close())
	}

	if err = agentIdentityDirectoryClose(run); err != nil {
		return nil, errors.Join(err, acpGo.Close())
	}

	directory, err := bootstrapAgentIdentityDirectory(acpGo, "agent-identities", trustedUID, trustedGID)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("bootstrap agent identity lock directory: %w", err), acpGo.Close())
	}

	if err = agentIdentityDirectoryClose(acpGo); err != nil {
		return nil, errors.Join(err, directory.Close())
	}

	return directory, nil
}

func openAgentIdentityLockDirectory(runRoot string, trustedUID, trustedGID uint32) (*os.File, error) {
	run, err := openAgentIdentityRuntimeRoot(runRoot, trustedUID, trustedGID)
	if err != nil {
		return nil, err
	}

	acpGo, err := openExistingAgentIdentityDirectory(run, "acp-go", trustedUID, trustedGID)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("open existing agent identity owner directory: %w", err), run.Close())
	}

	if err = agentIdentityDirectoryClose(run); err != nil {
		return nil, errors.Join(err, acpGo.Close())
	}

	directory, err := openExistingAgentIdentityDirectory(acpGo, "agent-identities", trustedUID, trustedGID)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("open existing agent identity lock directory: %w", err), acpGo.Close())
	}

	if err = agentIdentityDirectoryClose(acpGo); err != nil {
		return nil, errors.Join(err, directory.Close())
	}

	return directory, nil
}

func openAgentIdentityRuntimeRoot(runRoot string, trustedUID, trustedGID uint32) (*os.File, error) {
	fd, err := unix.Open(runRoot, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open agent identity runtime root: %w", err)
	}

	run := os.NewFile(uintptr(fd), runRoot)
	if err := validateAgentIdentityDirectory(run, trustedUID, trustedGID, false); err != nil {
		_ = run.Close()

		return nil, fmt.Errorf("validate agent identity runtime root: %w", err)
	}

	return run, nil
}

func bootstrapAgentIdentityDirectory(
	parent *os.File,
	name string,
	trustedUID uint32,
	trustedGID uint32,
) (*os.File, error) {
	err := agentIdentityDirectoryMkdirat(int(parent.Fd()), name, 0o700)
	if errors.Is(err, unix.EEXIST) {
		return openExistingAgentIdentityDirectory(parent, name, trustedUID, trustedGID)
	}

	if err != nil {
		return nil, err
	}

	fd, err := agentIdentityDirectoryOpenat(
		int(parent.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0,
	)
	if err != nil {
		return nil, err
	}

	file := os.NewFile(uintptr(fd), name)

	fail := func(cause error) (*os.File, error) {
		return nil, errors.Join(cause, agentIdentityDirectoryClose(file))
	}
	if err = agentIdentityDirectoryFchown(fd, int(trustedUID), int(trustedGID)); err != nil {
		return fail(fmt.Errorf("set agent identity directory owner: %w", err))
	}

	if err = agentIdentityDirectoryFchmod(fd, 0o700); err != nil {
		return fail(fmt.Errorf("set agent identity directory mode: %w", err))
	}

	if err = agentIdentityDirectoryFsync(fd); err != nil {
		return fail(fmt.Errorf("sync new agent identity directory: %w", err))
	}

	if err = agentIdentityDirectoryFsync(int(parent.Fd())); err != nil {
		return fail(fmt.Errorf("sync agent identity parent directory: %w", err))
	}

	if err = agentIdentityDirectoryClose(file); err != nil {
		return nil, fmt.Errorf("close new agent identity directory before reopen: %w", err)
	}

	return openExistingAgentIdentityDirectory(parent, name, trustedUID, trustedGID)
}

func openExistingAgentIdentityDirectory(
	parent *os.File,
	name string,
	trustedUID uint32,
	trustedGID uint32,
) (*os.File, error) {
	fd, err := agentIdentityDirectoryOpenat(
		int(parent.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0,
	)
	if err != nil {
		return nil, err
	}

	file := os.NewFile(uintptr(fd), name)

	fail := func(cause error) (*os.File, error) {
		return nil, errors.Join(cause, file.Close())
	}
	if err = validateAgentIdentityDirectory(file, trustedUID, trustedGID, true); err != nil {
		return fail(err)
	}

	var descriptor, named unix.Stat_t
	if err = agentIdentityLockFstat(fd, &descriptor); err != nil {
		return fail(err)
	}

	if err = agentIdentityDirectoryFstatat(
		int(parent.Fd()), name, &named, unix.AT_SYMLINK_NOFOLLOW,
	); err != nil || descriptor.Dev != named.Dev || descriptor.Ino != named.Ino {
		return fail(errors.Join(errors.New("agent identity directory is not its permanent named inode"), err))
	}

	return file, nil
}

func validateAgentIdentityDirectory(file *os.File, trustedUID, trustedGID uint32, exactMode bool) error {
	var stat unix.Stat_t
	if err := agentIdentityLockFstat(int(file.Fd()), &stat); err != nil {
		return err
	}

	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Uid != trustedUID || stat.Gid != trustedGID {
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

func validateAgentIdentityLockFile(file *os.File, trustedUID, trustedGID uint32) error {
	var stat unix.Stat_t
	if err := agentIdentityLockFstat(int(file.Fd()), &stat); err != nil {
		return err
	}

	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Uid != trustedUID || stat.Gid != trustedGID || stat.Nlink != 1 {
		return errors.New("agent identity lock must be a trusted-owned regular file with one link")
	}

	if permissions := stat.Mode & 0o777; permissions != 0o600 {
		return fmt.Errorf("agent identity lock mode is %#o, want 0600", permissions)
	}

	return nil
}

func duplicateAgentIdentityLock(file *os.File) (*os.File, error) {
	if file == nil {
		return nil, errors.New("agent identity lock descriptor is required")
	}

	fd, err := unix.FcntlInt(file.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}

	return os.NewFile(uintptr(fd), "amp-agent-identity-lock"), nil
}

func adoptAgentIdentityLock(file *os.File, uid uint32, testOnly bool, testRoot string) (*agentIdentityLock, error) {
	if file == nil {
		return nil, errors.New("inherited agent identity lock descriptor is unavailable")
	}

	fail := func(err error) (*agentIdentityLock, error) {
		return nil, errors.Join(err, file.Close())
	}
	runRoot := agentIdentityLockRunRoot
	trustedUID := agentIdentityLockTrustedUID
	trustedGID := agentIdentityLockTrustedGID

	if testOnly {
		if testRoot == "" {
			return fail(errors.New("test agent identity lock root is required"))
		}

		runRoot = testRoot
		trustedUID = uint32(os.Geteuid())
		trustedGID = uint32(os.Getegid())
	} else if testRoot != "" {
		return fail(errors.New("test agent identity lock root is forbidden"))
	}

	directory, err := openAgentIdentityLockDirectory(runRoot, trustedUID, trustedGID)
	if err != nil {
		return fail(err)
	}
	defer directory.Close()

	if err = validateAgentIdentityLockFile(file, trustedUID, trustedGID); err != nil {
		return fail(err)
	}

	var descriptor, named unix.Stat_t
	if err = agentIdentityLockFstat(int(file.Fd()), &descriptor); err != nil {
		return fail(err)
	}

	name := strconv.FormatUint(uint64(uid), 10) + ".lock"
	if err = agentIdentityDirectoryFstatat(int(directory.Fd()), name, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return fail(fmt.Errorf("inspect named agent identity lock %s: %w", name, err))
	}

	if descriptor.Dev != named.Dev || descriptor.Ino != named.Ino {
		return fail(fmt.Errorf("inherited agent identity lock is not the trusted named lock %s", name))
	}

	if err = validateInheritedAgentIdentityFlock(file, descriptor, "WRITE"); err != nil {
		return fail(err)
	}

	if err = proveInheritedAgentIdentityLock(file, directory, name, descriptor, trustedUID, trustedGID); err != nil {
		return fail(err)
	}

	if err = setAgentIdentityLockCloseOnExec(file); err != nil {
		return fail(err)
	}

	return &agentIdentityLock{file: file}, nil
}

func adoptAgentAuthorityDomain(file *os.File, testOnly bool, testRoot string) (*agentIdentityLock, error) {
	if file == nil {
		return nil, errors.New("inherited agent authority domain descriptor is unavailable")
	}

	fail := func(err error) (*agentIdentityLock, error) {
		return nil, errors.Join(err, file.Close())
	}
	runRoot := agentIdentityLockRunRoot
	trustedUID := agentIdentityLockTrustedUID
	trustedGID := agentIdentityLockTrustedGID

	if testOnly {
		if testRoot == "" {
			return fail(errors.New("test agent identity lock root is required"))
		}

		runRoot = testRoot
		trustedUID = uint32(os.Geteuid())
		trustedGID = uint32(os.Getegid())
	} else if testRoot != "" {
		return fail(errors.New("test agent identity lock root is forbidden"))
	}

	directory, err := openAgentIdentityLockDirectory(runRoot, trustedUID, trustedGID)
	if err != nil {
		return fail(err)
	}
	defer directory.Close()

	if err = validateAgentIdentityLockFile(file, trustedUID, trustedGID); err != nil {
		return fail(err)
	}

	var descriptor, named unix.Stat_t
	if err = agentIdentityLockFstat(int(file.Fd()), &descriptor); err != nil {
		return fail(err)
	}

	const name = "domain.lock"
	if err = agentIdentityDirectoryFstatat(int(directory.Fd()), name, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return fail(fmt.Errorf("inspect named agent authority domain %s: %w", name, err))
	}

	if descriptor.Dev != named.Dev || descriptor.Ino != named.Ino {
		return fail(errors.New("inherited agent authority domain is not the trusted named domain.lock"))
	}

	if err = validateInheritedAgentIdentityFlock(file, descriptor, "READ"); err != nil {
		return fail(err)
	}

	contenderFD, err := agentIdentityLockOpenat(int(directory.Fd()), name, unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return fail(err)
	}

	contenderErr := unix.Flock(contenderFD, unix.LOCK_EX|unix.LOCK_NB)

	closeErr := agentIdentityLockCloseFD(contenderFD)
	if contenderErr == nil {
		return fail(errors.Join(errors.New("inherited agent authority domain was not locked before handoff"), closeErr))
	}

	if !errors.Is(contenderErr, unix.EWOULDBLOCK) && !errors.Is(contenderErr, unix.EAGAIN) {
		return fail(errors.Join(fmt.Errorf("contend for inherited agent authority domain: %w", contenderErr), closeErr))
	}

	if closeErr != nil {
		return fail(closeErr)
	}

	if err = validateAgentAuthorityDomainRecord(directory, trustedUID, trustedGID); err != nil {
		return fail(err)
	}

	if err = setAgentIdentityLockCloseOnExec(file); err != nil {
		return fail(err)
	}

	return &agentIdentityLock{file: file}, nil
}

func validateBorrowedAgentIdentityDisposition(uid, gid uint32, testOnly bool, testRoot string) error {
	runRoot := agentIdentityLockRunRoot
	trustedUID := agentIdentityLockTrustedUID
	trustedGID := agentIdentityLockTrustedGID

	if testOnly {
		if testRoot == "" {
			return errors.New("test agent identity lock root is required")
		}

		runRoot = testRoot
		trustedUID = uint32(os.Geteuid())
		trustedGID = uint32(os.Getegid())
	} else if testRoot != "" {
		return errors.New("test agent identity lock root is forbidden")
	}

	directory, err := openAgentIdentityLockDirectory(runRoot, trustedUID, trustedGID)
	if err != nil {
		return err
	}
	defer directory.Close()

	if err = rejectBorrowedAgentIdentityTemporaries(directory); err != nil {
		return err
	}

	deadline := time.Now().Add(agentStandaloneClaimMax)
	if err = auditAgentStandaloneAuthorityRoot(
		directory,
		trustedUID,
		trustedGID,
		false,
		false,
		true,
		deadline,
		nil,
		nil,
	); err != nil {
		return fmt.Errorf("audit borrowed agent identity authority: %w", err)
	}

	ownerName := strconv.FormatUint(uint64(uid), 10) + ".owner"

	var owner unix.Stat_t

	ownerErr := agentIdentityDirectoryFstatat(int(directory.Fd()), ownerName, &owner, unix.AT_SYMLINK_NOFOLLOW)
	if ownerErr == nil {
		return fmt.Errorf("borrowed agent identity uid %d has a permanent owner binding", uid)
	}

	if !errors.Is(ownerErr, unix.ENOENT) {
		return fmt.Errorf("inspect borrowed agent identity owner %s: %w", ownerName, ownerErr)
	}

	marker, err := loadAgentStandaloneMarker(directory, uid, trustedUID, trustedGID)
	if err != nil {
		return fmt.Errorf("load borrowed agent identity disposition: %w", err)
	}

	if marker.State != "active" || marker.GID != gid {
		return fmt.Errorf("borrowed agent identity uid %d does not have its matching ownerless ACTIVE disposition", uid)
	}

	return nil
}

func validateInheritedStandaloneAgentIdentityDisposition(
	uid uint32,
	gid uint32,
	ownerID string,
	stateRoot string,
	testOnly bool,
	testRoot string,
) error {
	boundRoot, err := bindAgentStandaloneStateRoot(stateRoot, uid, gid)
	if err != nil {
		return fmt.Errorf("bind inherited standalone state root: %w", err)
	}

	runRoot := agentIdentityLockRunRoot
	trustedUID := agentIdentityLockTrustedUID
	trustedGID := agentIdentityLockTrustedGID

	if testOnly {
		if testRoot == "" {
			return errors.New("test agent identity lock root is required")
		}

		runRoot = testRoot
		trustedUID = uint32(os.Geteuid())
		trustedGID = uint32(os.Getegid())
	} else if testRoot != "" {
		return errors.New("test agent identity lock root is forbidden")
	}

	directory, err := openAgentIdentityLockDirectory(runRoot, trustedUID, trustedGID)
	if err != nil {
		return err
	}
	defer directory.Close()

	if err = rejectBorrowedAgentIdentityTemporaries(directory); err != nil {
		return err
	}

	deadline := time.Now().Add(agentStandaloneClaimMax)
	if err = auditAgentStandaloneAuthorityRoot(
		directory,
		trustedUID,
		trustedGID,
		false,
		false,
		true,
		deadline,
		nil,
		nil,
	); err != nil {
		return fmt.Errorf("audit inherited standalone agent identity authority: %w", err)
	}

	want := agentStandaloneOwner{
		Version:   1,
		UID:       uid,
		GID:       gid,
		Kind:      agentStandaloneOwnerKind,
		Provider:  agentStandaloneOwnerID,
		OwnerID:   ownerID,
		StateRoot: boundRoot,
	}

	owner, err := loadAgentStandaloneOwner(directory, uid, trustedUID, trustedGID)
	if err != nil {
		return fmt.Errorf("load inherited standalone owner: %w", err)
	}

	if owner != want {
		return fmt.Errorf("inherited standalone agent identity uid %d does not match its immutable owner tuple", uid)
	}

	if err = validateAgentStandaloneRetainedActiveDisposition(directory, owner, trustedUID, trustedGID); err != nil {
		return fmt.Errorf("validate inherited standalone disposition: %w", err)
	}

	return nil
}

func rejectBorrowedAgentIdentityTemporaries(directory *os.File) error {
	entries, err := agentStandaloneAuthorityEntries(directory)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, "domain.json.next-") || strings.HasPrefix(name, ".authority-probe-") ||
			strings.Contains(name, ".owner.next-") || strings.Contains(name, ".quarantine.next-") {
			return fmt.Errorf("borrowed agent identity authority contains unresolved temporary %q", name)
		}
	}

	return nil
}

func setAgentIdentityLockCloseOnExec(file *os.File) error {
	flags, err := agentIdentityLockFcntl(file.Fd(), unix.F_GETFD, 0)
	if err != nil {
		return fmt.Errorf("read inherited agent identity lock descriptor flags: %w", err)
	}

	if _, err = agentIdentityLockFcntl(file.Fd(), unix.F_SETFD, flags|unix.FD_CLOEXEC); err != nil {
		return fmt.Errorf("protect inherited agent identity lock from native exec: %w", err)
	}

	return nil
}

func proveInheritedAgentIdentityLock(
	file *os.File,
	directory *os.File,
	name string,
	descriptor unix.Stat_t,
	trustedUID uint32,
	trustedGID uint32,
) (proofErr error) {
	contenderFD, err := agentIdentityLockOpenat(
		int(directory.Fd()), name, unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0,
	)
	if err != nil {
		return fmt.Errorf("open inherited agent identity lock %s ownership contender: %w", name, err)
	}

	contender := os.NewFile(uintptr(contenderFD), name+"-contender")
	defer func() {
		if closeErr := contender.Close(); closeErr != nil {
			proofErr = errors.Join(proofErr, fmt.Errorf("close inherited agent identity lock %s ownership contender: %w", name, closeErr))
		}
	}()

	if err = validateAgentIdentityLockFile(contender, trustedUID, trustedGID); err != nil {
		return err
	}

	var contenderStat unix.Stat_t
	if err = agentIdentityLockFstat(contenderFD, &contenderStat); err != nil {
		return err
	}

	if contenderStat.Dev != descriptor.Dev || contenderStat.Ino != descriptor.Ino {
		return fmt.Errorf("inherited agent identity lock ownership contender is not the trusted named lock %s", name)
	}

	contenderErr := unix.Flock(contenderFD, unix.LOCK_EX|unix.LOCK_NB)
	if contenderErr == nil {
		return fmt.Errorf("inherited agent identity lock %s was not locked before handoff", name)
	}

	if !errors.Is(contenderErr, unix.EWOULDBLOCK) && !errors.Is(contenderErr, unix.EAGAIN) {
		return fmt.Errorf("contend for inherited agent identity lock %s: %w", name, contenderErr)
	}

	return nil
}

func validateInheritedAgentIdentityFlock(file *os.File, descriptor unix.Stat_t, wantMode string) error {
	path := fmt.Sprintf("/proc/self/fdinfo/%d", file.Fd())

	payload, err := agentIdentityLockReadFile(path)
	if err != nil {
		return fmt.Errorf("read inherited agent identity flock state: %w", err)
	}

	lockLines := 0

	for _, line := range strings.Split(string(payload), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "lock:" {
			continue
		}

		lockLines++

		if err = validateInheritedAgentIdentityFlockLine(fields, descriptor, wantMode); err != nil {
			return err
		}
	}

	if lockLines != 1 {
		return fmt.Errorf("inherited agent identity descriptor has %d flock entries, want exactly one", lockLines)
	}

	return nil
}

func validateInheritedAgentIdentityFlockLine(fields []string, descriptor unix.Stat_t, wantMode string) error {
	if len(fields) != 9 || !strings.HasSuffix(fields[1], ":") || fields[2] != "FLOCK" ||
		fields[3] != "ADVISORY" || fields[4] != wantMode || fields[7] != "0" || fields[8] != "EOF" {
		return errors.New("inherited agent identity descriptor has malformed or wrong-mode flock state")
	}

	if _, err := strconv.ParseUint(strings.TrimSuffix(fields[1], ":"), 10, 64); err != nil {
		return errors.New("inherited agent identity descriptor has malformed flock sequence")
	}

	if pid, err := strconv.ParseInt(fields[5], 10, 64); err != nil || pid < 0 {
		return errors.New("inherited agent identity descriptor has malformed flock owner")
	}

	identity := strings.Split(fields[6], ":")
	if len(identity) != 3 {
		return errors.New("inherited agent identity descriptor has malformed flock inode")
	}

	_, majorErr := strconv.ParseUint(identity[0], 16, 32)
	_, minorErr := strconv.ParseUint(identity[1], 16, 32)

	inode, inodeErr := strconv.ParseUint(identity[2], 10, 64)
	if majorErr != nil || minorErr != nil || inodeErr != nil {
		return errors.New("inherited agent identity descriptor has malformed flock inode")
	}
	// The fdinfo flock entry carries the backing superblock device, while
	// fstat reports the mount's device. Those device numbers can differ when
	// the descriptor crosses a mount namespace, such as for a Docker volume.
	// The adoption path separately matches descriptor.Dev and descriptor.Ino
	// against the exact trusted named authority. Identity-lock adoption also
	// proves that a fresh contender is blocked.
	if inode != descriptor.Ino {
		return errors.New("inherited agent identity descriptor flock does not cover its exact inode")
	}

	return nil
}

func (lock *agentIdentityLock) Close() error {
	if lock == nil || lock.file == nil {
		return nil
	}

	closeErr := agentIdentityLockClose(lock.file)
	lock.file = nil

	return closeErr
}

func (lock *agentIdentityLock) Duplicate() (*os.File, error) {
	if lock == nil || lock.file == nil {
		return nil, errors.New("agent identity lock is unavailable")
	}

	return duplicateAgentIdentityLock(lock.file)
}
