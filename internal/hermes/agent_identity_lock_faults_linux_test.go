//go:build linux

package hermes

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// identityLockCovSeams restores the agent identity lock seam group, including
// the descriptor seams this file drives, when the test ends.
func identityLockCovSeams(t *testing.T) {
	t.Helper()
	restoreAgentIdentityLockTestSeams(t)
	fstat := agentIdentityLockFstat
	openat := agentIdentityLockOpenat
	closeFD := agentIdentityLockCloseFD
	fcntl := agentIdentityLockFcntl
	t.Cleanup(func() {
		agentIdentityLockFstat = fstat
		agentIdentityLockOpenat = openat
		agentIdentityLockCloseFD = closeFD
		agentIdentityLockFcntl = fcntl
	})
}

// identityLockCovAuthority bootstraps a trusted authority root and returns its
// open directory descriptor together with the runtime root it lives under.
func identityLockCovAuthority(t *testing.T) (*os.File, string) {
	t.Helper()
	identityLockCovSeams(t)
	root := configureAgentIdentityLockTestRoot(t)
	directory, err := bootstrapAgentIdentityLockDirectory(
		root, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = directory.Close() })

	return directory, root
}

func identityLockCovAuthorityPath(root string, name string) string {
	return filepath.Join(root, "acp-go", "agent-identities", name)
}

func identityLockCovNamedLock(t *testing.T, directory *os.File, name string) *os.File {
	t.Helper()
	file, err := openAgentStandaloneNamedLock(
		directory, name, true, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })

	return file
}

func identityLockCovLocked(t *testing.T, directory *os.File, name string, operation int) *os.File {
	t.Helper()
	file := identityLockCovNamedLock(t, directory, name)
	if err := unix.Flock(int(file.Fd()), operation|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}

	return file
}

func identityLockCovDuplicate(t *testing.T, source *os.File) *os.File {
	t.Helper()
	duplicate, err := duplicateAgentIdentityLock(source)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = duplicate.Close() })

	return duplicate
}

func identityLockCovClosed(t *testing.T) *os.File {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "identity-lock-cov")
	if err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}

	return file
}

// identityLockCovFaultFstat makes the identity lock's descriptor metadata
// probe fail on its at-th call, standing in for a kernel that stops answering
// for a descriptor the adoption has already accepted. The caller restores the
// seam through identityLockCovSeams.
func identityLockCovFaultFstat(at int, failure error) {
	original := agentIdentityLockFstat
	calls := 0
	agentIdentityLockFstat = func(fd int, stat *unix.Stat_t) error {
		calls++
		if calls == at {
			return failure
		}

		return original(fd, stat)
	}
}

// identityLockCovFaultFstatat makes the named-entry probe fail on its at-th
// call. The first two calls always belong to the authority directory chain.
func identityLockCovFaultFstatat(at int, failure error) {
	original := agentIdentityDirectoryFstatat
	calls := 0
	agentIdentityDirectoryFstatat = func(dirfd int, path string, stat *unix.Stat_t, flags int) error {
		calls++
		if calls == at {
			return failure
		}

		return original(dirfd, path, stat, flags)
	}
}

// identityLockCovFaultClose makes the authority directory handoff close fail
// on its at-th call while still releasing the descriptor.
func identityLockCovFaultClose(at int, failure error) {
	calls := 0
	agentIdentityDirectoryClose = func(file *os.File) error {
		calls++
		if err := file.Close(); err != nil {
			return err
		}
		if calls == at {
			return failure
		}

		return nil
	}
}

// identityLockCovFdinfo renders an fdinfo payload claiming an flock of the
// given mode over the file's real inode, whether or not one is actually held.
func identityLockCovFdinfo(t *testing.T, file *os.File, mode string) []byte {
	t.Helper()
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		t.Fatal(err)
	}

	return []byte(fmt.Sprintf(
		"pos:\t0\nlock:\t1: FLOCK ADVISORY %s %d 00:26:%d 0 EOF\n", mode, os.Getpid(), stat.Ino,
	))
}

// identityLockCovBorrowed establishes a complete ownerless ACTIVE borrowed
// disposition for uid/gid and returns the runtime root it lives under, so a
// test can break exactly one property of an otherwise acceptable authority.
func identityLockCovBorrowed(t *testing.T, uid uint32, gid uint32) string {
	t.Helper()
	identityLockCovSeams(t)
	root := configureAgentIdentityLockTestRoot(t)
	directory, err := bootstrapAgentIdentityLockDirectory(
		root, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = directory.Close() })

	deadline := time.Now().Add(agentStandaloneClaimMax)
	domain, err := acquireAgentStandaloneDomain(
		directory, agentStandaloneOwner{},
		agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
		true, deadline, nil, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = domain.Close() })

	owners := identityLockCovNamedLock(t, directory, "owners.lock")
	if err = owners.Close(); err != nil {
		t.Fatal(err)
	}
	identityLockCovLocked(t, directory, strconv.FormatUint(uint64(uid), 10)+".lock", unix.LOCK_EX)

	const ownerDigest = "identity-lock-cov-session"
	affinity := identityLockCovNamedLock(t, directory, agentStandaloneAffinityLockName(ownerDigest))
	if err = affinity.Close(); err != nil {
		t.Fatal(err)
	}
	if err = publishAgentStandaloneActive(
		directory, uid, gid,
		agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
		ownerDigest, deadline, nil, nil,
	); err != nil {
		t.Fatal(err)
	}
	if err = validateBorrowedAgentIdentityDisposition(uid, gid, true, root); err != nil {
		t.Fatalf("borrowed fixture is not itself acceptable: %v", err)
	}

	return root
}

// TestAgentIdentityAuthorityBootstrapHandoffFaultsFailClosed proves the
// two-step creation of the agent identity authority root never hands back a
// directory it could not fully establish: a failed creation, reopen, metadata
// probe or handoff close all abort with no directory returned, and a failed
// creation leaves the authority path absent rather than half made.
func TestAgentIdentityAuthorityBootstrapHandoffFaultsFailClosed(t *testing.T) {
	for name, testCase := range map[string]struct {
		fault  func(error)
		unmade string
	}{
		"owner directory handoff close": {
			fault: func(failure error) { identityLockCovFaultClose(2, failure) },
		},
		"authority directory handoff close": {
			fault: func(failure error) { identityLockCovFaultClose(4, failure) },
		},
		"authority directory creation": {
			fault: func(failure error) {
				original := agentIdentityDirectoryMkdirat
				calls := 0
				agentIdentityDirectoryMkdirat = func(dirfd int, path string, mode uint32) error {
					calls++
					if calls == 2 {
						return failure
					}

					return original(dirfd, path, mode)
				}
			},
			unmade: "agent-identities",
		},
		"owner directory open": {
			fault: func(failure error) {
				agentIdentityDirectoryOpenat = func(int, string, int, uint32) (int, error) {
					return -1, failure
				}
			},
			unmade: "agent-identities",
		},
		"runtime root metadata": {
			fault:  func(failure error) { identityLockCovFaultFstat(1, failure) },
			unmade: "acp-go",
		},
		"named owner directory metadata": {
			fault: func(failure error) { identityLockCovFaultFstat(3, failure) },
		},
	} {
		t.Run(name, func(t *testing.T) {
			identityLockCovSeams(t)
			root := configureAgentIdentityLockTestRoot(t)
			failure := errors.New("injected " + name + " failure")
			testCase.fault(failure)
			directory, err := bootstrapAgentIdentityLockDirectory(
				root, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
			)
			if directory != nil {
				_ = directory.Close()
				t.Fatal("authority bootstrap returned a directory despite an injected fault")
			}
			if !errors.Is(err, failure) {
				t.Fatalf("authority bootstrap error = %v, want %v", err, failure)
			}
			if testCase.unmade == "" {
				return
			}
			path := filepath.Join(root, "acp-go", testCase.unmade)
			if testCase.unmade == "acp-go" {
				path = filepath.Join(root, "acp-go")
			}
			if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("failed bootstrap left %s behind: %v", path, statErr)
			}
		})
	}

	t.Run("absent runtime root", func(t *testing.T) {
		identityLockCovSeams(t)
		root := filepath.Join(configureAgentIdentityLockTestRoot(t), "absent")
		_, err := bootstrapAgentIdentityLockDirectory(
			root, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
		)
		if !errors.Is(err, unix.ENOENT) ||
			!strings.Contains(err.Error(), "open agent identity runtime root") {
			t.Fatalf("absent runtime root error = %v", err)
		}
	})
}

// TestAgentIdentityAuthorityOpenHandoffFaultsFailClosed proves that reopening
// an already-established authority root refuses whenever any step of the
// descriptor chain — the runtime root, either handoff close, or the authority
// directory itself — cannot be completed, so no caller ever operates on a
// partially proven authority.
func TestAgentIdentityAuthorityOpenHandoffFaultsFailClosed(t *testing.T) {
	const (
		uid = uint32(62451)
		gid = uint32(62452)
	)
	for name, testCase := range map[string]struct {
		prepare   func(root string, failure error) error
		absent    bool
		wantError string
	}{
		"absent runtime root": {
			absent: true,
		},
		"owner directory handoff close": {
			prepare: func(_ string, failure error) error {
				identityLockCovFaultClose(1, failure)

				return nil
			},
		},
		"authority directory handoff close": {
			prepare: func(_ string, failure error) error {
				identityLockCovFaultClose(2, failure)

				return nil
			},
		},
		"authority directory is gone": {
			prepare: func(root string, _ error) error {
				authority := filepath.Join(root, "acp-go", "agent-identities")

				return os.Rename(authority, authority+"-moved")
			},
			wantError: "open existing agent identity lock directory",
		},
	} {
		t.Run(name, func(t *testing.T) {
			fixtureRoot := identityLockCovBorrowed(t, uid, gid)
			identityLockCovSeams(t)
			failure := errors.New("injected " + name + " failure")
			root := fixtureRoot
			if testCase.absent {
				root = filepath.Join(fixtureRoot, "absent")
			}
			if testCase.prepare != nil {
				if err := testCase.prepare(fixtureRoot, failure); err != nil {
					t.Fatal(err)
				}
			}
			err := validateBorrowedAgentIdentityDisposition(uid, gid, true, root)
			switch {
			case testCase.absent:
				if !errors.Is(err, unix.ENOENT) {
					t.Fatalf("authority open error = %v, want ENOENT", err)
				}
			case testCase.wantError != "":
				if err == nil || !strings.Contains(err.Error(), testCase.wantError) {
					t.Fatalf("authority open error = %v, want one containing %q", err, testCase.wantError)
				}
			default:
				if !errors.Is(err, failure) {
					t.Fatalf("authority open error = %v, want %v", err, failure)
				}
			}
		})
	}
}

// TestAgentIdentityLockDuplicationRefusesUnusableDescriptors proves the
// duplication path never fabricates a descriptor: a missing lock, a released
// lock and a closed descriptor are all refused, while a live lock duplicates
// onto the same inode as an independent descriptor.
func TestAgentIdentityLockDuplicationRefusesUnusableDescriptors(t *testing.T) {
	directory, _ := identityLockCovAuthority(t)
	if _, err := duplicateAgentIdentityLock(nil); err == nil ||
		!strings.Contains(err.Error(), "agent identity lock descriptor is required") {
		t.Fatalf("absent descriptor error = %v", err)
	}
	if _, err := duplicateAgentIdentityLock(identityLockCovClosed(t)); !errors.Is(err, unix.EBADF) {
		t.Fatalf("closed descriptor error = %v, want EBADF", err)
	}
	for name, lock := range map[string]*agentIdentityLock{
		"released lock": {},
		"absent lock":   nil,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := lock.Duplicate(); err == nil ||
				!strings.Contains(err.Error(), "agent identity lock is unavailable") {
				t.Fatalf("%s duplicate error = %v", name, err)
			}
		})
	}

	source := identityLockCovLocked(t, directory, "1260.lock", unix.LOCK_EX)
	lock := &agentIdentityLock{file: source}
	duplicate, err := lock.Duplicate()
	if err != nil {
		t.Fatalf("duplicate a live identity lock: %v", err)
	}
	defer duplicate.Close()
	var original, copied unix.Stat_t
	if err = unix.Fstat(int(source.Fd()), &original); err != nil {
		t.Fatal(err)
	}
	if err = unix.Fstat(int(duplicate.Fd()), &copied); err != nil {
		t.Fatal(err)
	}
	if original.Dev != copied.Dev || original.Ino != copied.Ino || duplicate.Fd() == source.Fd() {
		t.Fatalf("duplicate descriptor %d does not independently name the source inode", duplicate.Fd())
	}
}

// TestAdoptAgentIdentityLockRefusesEveryUnprovenHandoff proves the adoption of
// an inherited uid lock re-proves every claim about the descriptor it is
// handed, and closes that descriptor on refusal instead of leaving a lease the
// caller believes was rejected.
func TestAdoptAgentIdentityLockRefusesEveryUnprovenHandoff(t *testing.T) {
	directory, root := identityLockCovAuthority(t)
	source := identityLockCovLocked(t, directory, "1261.lock", unix.LOCK_EX)
	identityLockCovNamedLock(t, directory, "1262.lock")

	if _, err := adoptAgentIdentityLock(nil, 1261, false, ""); err == nil ||
		!strings.Contains(err.Error(), "inherited agent identity lock descriptor is unavailable") {
		t.Fatalf("absent descriptor error = %v", err)
	}

	handed := identityLockCovDuplicate(t, source)
	if _, err := adoptAgentIdentityLock(handed, 1261, true, ""); err == nil ||
		!strings.Contains(err.Error(), "test agent identity lock root is required") {
		t.Fatalf("empty test root error = %v", err)
	}
	if err := handed.Close(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("refused adoption left the handed descriptor open: %v", err)
	}

	handed = identityLockCovDuplicate(t, source)
	if _, err := adoptAgentIdentityLock(handed, 1261, false, root); err == nil ||
		!strings.Contains(err.Error(), "test agent identity lock root is forbidden") {
		t.Fatalf("forbidden test root error = %v", err)
	}
	if err := handed.Close(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("refused adoption left the handed descriptor open: %v", err)
	}

	adopted, err := adoptAgentIdentityLock(identityLockCovDuplicate(t, source), 1261, true, root)
	if err != nil {
		t.Fatalf("adopt through the test authority root: %v", err)
	}
	if err = adopted.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err = adoptAgentIdentityLock(identityLockCovClosed(t), 1261, false, ""); !errors.Is(err, unix.EBADF) {
		t.Fatalf("closed descriptor error = %v, want EBADF", err)
	}

	if _, err = adoptAgentIdentityLock(identityLockCovDuplicate(t, source), 1262, false, ""); err == nil ||
		!strings.Contains(err.Error(), "is not the trusted named lock 1262.lock") {
		t.Fatalf("mismatched named lock error = %v", err)
	}

	for name, fault := range map[string]func(error){
		"lock inode": func(failure error) { identityLockCovFaultFstat(7, failure) },
		"named lock resolution": func(failure error) {
			identityLockCovFaultFstatat(3, failure)
		},
		"flock state": func(failure error) {
			agentIdentityLockReadFile = func(string) ([]byte, error) { return nil, failure }
		},
		"close on exec flags": func(failure error) {
			agentIdentityLockFcntl = func(uintptr, int, int) (int, error) { return 0, failure }
		},
		"close on exec protection": func(failure error) {
			original := agentIdentityLockFcntl
			calls := 0
			agentIdentityLockFcntl = func(fd uintptr, request, argument int) (int, error) {
				calls++
				if calls == 2 {
					return 0, failure
				}

				return original(fd, request, argument)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			identityLockCovSeams(t)
			failure := errors.New("injected " + name + " failure")
			handed := identityLockCovDuplicate(t, source)
			fault(failure)
			if _, adoptErr := adoptAgentIdentityLock(handed, 1261, false, ""); !errors.Is(adoptErr, failure) {
				t.Fatalf("adoption error = %v, want %v", adoptErr, failure)
			}
			if closeErr := handed.Close(); !errors.Is(closeErr, os.ErrClosed) {
				t.Fatalf("refused adoption left the handed descriptor open: %v", closeErr)
			}
		})
	}

	var stat unix.Stat_t
	if err = unix.Fstat(int(source.Fd()), &stat); err != nil {
		t.Fatal(err)
	}
	if err = validateInheritedAgentIdentityFlock(source, stat, "WRITE"); err != nil {
		t.Fatalf("refused adoptions mutated the host's exclusive lease: %v", err)
	}
}

// TestInheritedAgentIdentityLockOwnershipProofFailsClosed proves adoption does
// not believe the descriptor's own flock claim: it independently opens the
// trusted named lock and proves a fresh contender is blocked by it. Every way
// that proof can fail to complete — the contender cannot be opened, described,
// matched, contended, or is not blocked at all — refuses the handoff.
func TestInheritedAgentIdentityLockOwnershipProofFailsClosed(t *testing.T) {
	directory, root := identityLockCovAuthority(t)
	source := identityLockCovLocked(t, directory, "1270.lock", unix.LOCK_EX)
	identityLockCovNamedLock(t, directory, "1271.lock")
	unlocked := identityLockCovNamedLock(t, directory, "1272.lock")
	unlockedPath := identityLockCovAuthorityPath(root, "1272.lock")
	unlockedFdinfo := identityLockCovFdinfo(t, unlocked, "WRITE")

	for name, testCase := range map[string]struct {
		uid       uint32
		handed    string
		fault     func(error)
		wantError string
	}{
		"contender cannot be opened": {
			uid: 1270,
			fault: func(failure error) {
				agentIdentityLockOpenat = func(int, string, int, uint32) (int, error) {
					return -1, failure
				}
			},
		},
		"contender descriptor is unusable": {
			uid: 1270,
			fault: func(error) {
				agentIdentityLockOpenat = func(int, string, int, uint32) (int, error) {
					return 999999, nil
				}
			},
			wantError: "close inherited agent identity lock 1270.lock ownership contender",
		},
		"contender inode is not described": {
			uid:   1270,
			fault: func(failure error) { identityLockCovFaultFstat(9, failure) },
		},
		"contender is a different lock": {
			uid: 1270,
			fault: func(error) {
				original := agentIdentityLockOpenat
				agentIdentityLockOpenat = func(dirfd int, _ string, flags int, mode uint32) (int, error) {
					return original(dirfd, "1271.lock", flags, mode)
				}
			},
			wantError: "ownership contender is not the trusted named lock 1270.lock",
		},
		"contender is not blocked": {
			uid:    1272,
			handed: unlockedPath,
			fault: func(error) {
				agentIdentityLockReadFile = func(string) ([]byte, error) { return unlockedFdinfo, nil }
			},
			wantError: "inherited agent identity lock 1272.lock was not locked before handoff",
		},
		"contender cannot contend": {
			uid: 1270,
			fault: func(error) {
				agentIdentityLockOpenat = func(dirfd int, path string, _ int, _ uint32) (int, error) {
					return unix.Openat(dirfd, path, unix.O_PATH|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
				}
			},
			wantError: "contend for inherited agent identity lock 1270.lock",
		},
	} {
		t.Run(name, func(t *testing.T) {
			identityLockCovSeams(t)
			failure := errors.New("injected " + name + " failure")
			handed := identityLockCovDuplicate(t, source)
			if testCase.handed != "" {
				opened, openErr := os.OpenFile(testCase.handed, os.O_RDWR, 0)
				if openErr != nil {
					t.Fatal(openErr)
				}
				handed = opened
				t.Cleanup(func() { _ = handed.Close() })
			}
			testCase.fault(failure)
			_, adoptErr := adoptAgentIdentityLock(handed, testCase.uid, false, "")
			if testCase.wantError == "" {
				if !errors.Is(adoptErr, failure) {
					t.Fatalf("ownership proof error = %v, want %v", adoptErr, failure)
				}

				return
			}
			if adoptErr == nil || !strings.Contains(adoptErr.Error(), testCase.wantError) {
				t.Fatalf("ownership proof error = %v, want one containing %q", adoptErr, testCase.wantError)
			}
		})
	}

	var stat unix.Stat_t
	if err := unix.Fstat(int(source.Fd()), &stat); err != nil {
		t.Fatal(err)
	}
	if err := validateInheritedAgentIdentityFlock(source, stat, "WRITE"); err != nil {
		t.Fatalf("refused ownership proofs mutated the host's exclusive lease: %v", err)
	}
}

// identityLockCovDomainRecord publishes the running authority domain so a
// domain lock handoff can be adopted end to end.
func identityLockCovDomainRecord(t *testing.T, directory *os.File, root string) {
	t.Helper()
	record, err := currentAgentAuthorityDomain(directory)
	if err != nil {
		t.Fatal(err)
	}
	record.AuthorityID = authorityDomainCovID
	payload, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	path := identityLockCovAuthorityPath(root, agentAuthorityDomainRecordName)
	if err = os.WriteFile(path, append(payload, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestAdoptAgentAuthorityDomainRefusesEveryUnprovenHandoff proves the shared
// authority domain lease is adopted only when the descriptor is the trusted
// named domain.lock and is really holding a lease that blocks an exclusive
// contender, and that any step the kernel cannot complete refuses the handoff
// and closes the descriptor.
func TestAdoptAgentAuthorityDomainRefusesEveryUnprovenHandoff(t *testing.T) {
	directory, root := identityLockCovAuthority(t)
	identityLockCovDomainRecord(t, directory, root)
	source := identityLockCovLocked(t, directory, "domain.lock", unix.LOCK_SH)
	foreign := identityLockCovLocked(t, directory, "1280.lock", unix.LOCK_SH)

	if _, err := adoptAgentAuthorityDomain(nil, false, ""); err == nil ||
		!strings.Contains(err.Error(), "inherited agent authority domain descriptor is unavailable") {
		t.Fatalf("absent descriptor error = %v", err)
	}

	handed := identityLockCovDuplicate(t, source)
	if _, err := adoptAgentAuthorityDomain(handed, true, ""); err == nil ||
		!strings.Contains(err.Error(), "test agent identity lock root is required") {
		t.Fatalf("empty test root error = %v", err)
	}
	if err := handed.Close(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("refused adoption left the handed descriptor open: %v", err)
	}

	handed = identityLockCovDuplicate(t, source)
	if _, err := adoptAgentAuthorityDomain(handed, false, root); err == nil ||
		!strings.Contains(err.Error(), "test agent identity lock root is forbidden") {
		t.Fatalf("forbidden test root error = %v", err)
	}

	adopted, err := adoptAgentAuthorityDomain(identityLockCovDuplicate(t, source), true, root)
	if err != nil {
		t.Fatalf("adopt through the test authority root: %v", err)
	}
	if err = adopted.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err = adoptAgentAuthorityDomain(identityLockCovClosed(t), false, ""); !errors.Is(err, unix.EBADF) {
		t.Fatalf("closed descriptor error = %v, want EBADF", err)
	}

	if _, err = adoptAgentAuthorityDomain(identityLockCovDuplicate(t, foreign), false, ""); err == nil ||
		!strings.Contains(err.Error(), "is not the trusted named domain.lock") {
		t.Fatalf("foreign named lock error = %v", err)
	}

	for name, testCase := range map[string]struct {
		fault     func(error)
		wantError string
	}{
		"domain inode": {
			fault: func(failure error) { identityLockCovFaultFstat(7, failure) },
		},
		"named domain resolution": {
			fault: func(failure error) { identityLockCovFaultFstatat(3, failure) },
		},
		"contender cannot be opened": {
			fault: func(failure error) {
				agentIdentityLockOpenat = func(int, string, int, uint32) (int, error) {
					return -1, failure
				}
			},
		},
		"contender cannot contend": {
			fault: func(error) {
				agentIdentityLockOpenat = func(dirfd int, path string, _ int, _ uint32) (int, error) {
					return unix.Openat(dirfd, path, unix.O_PATH|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
				}
			},
			wantError: "contend for inherited agent authority domain",
		},
		"contender cannot be released": {
			fault: func(failure error) {
				original := agentIdentityLockCloseFD
				agentIdentityLockCloseFD = func(fd int) error {
					_ = original(fd)

					return failure
				}
			},
		},
		"close on exec flags": {
			fault: func(failure error) {
				agentIdentityLockFcntl = func(uintptr, int, int) (int, error) { return 0, failure }
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			identityLockCovSeams(t)
			failure := errors.New("injected " + name + " failure")
			handed := identityLockCovDuplicate(t, source)
			testCase.fault(failure)
			_, adoptErr := adoptAgentAuthorityDomain(handed, false, "")
			switch {
			case testCase.wantError != "":
				if adoptErr == nil || !strings.Contains(adoptErr.Error(), testCase.wantError) {
					t.Fatalf("domain adoption error = %v, want one containing %q", adoptErr, testCase.wantError)
				}
			default:
				if !errors.Is(adoptErr, failure) {
					t.Fatalf("domain adoption error = %v, want %v", adoptErr, failure)
				}
			}
			if closeErr := handed.Close(); !errors.Is(closeErr, os.ErrClosed) {
				t.Fatalf("refused adoption left the handed descriptor open: %v", closeErr)
			}
		})
	}

	t.Run("domain was not locked", func(t *testing.T) {
		identityLockCovSeams(t)
		if err := source.Close(); err != nil {
			t.Fatal(err)
		}
		handed, openErr := os.OpenFile(identityLockCovAuthorityPath(root, "domain.lock"), os.O_RDWR, 0)
		if openErr != nil {
			t.Fatal(openErr)
		}
		t.Cleanup(func() { _ = handed.Close() })
		payload := identityLockCovFdinfo(t, handed, "READ")
		agentIdentityLockReadFile = func(string) ([]byte, error) { return payload, nil }
		if _, adoptErr := adoptAgentAuthorityDomain(handed, false, ""); adoptErr == nil ||
			!strings.Contains(adoptErr.Error(), "was not locked before handoff") {
			t.Fatalf("unlocked domain error = %v", adoptErr)
		}
	})
}

// TestBorrowedAgentIdentityDispositionRefusesUnprovenModes proves the borrowed
// disposition check only runs against the authority root it was told to use,
// and refuses when the authority root cannot be enumerated or the owner
// binding cannot be resolved, rather than reading either as "nothing found".
func TestBorrowedAgentIdentityDispositionRefusesUnprovenModes(t *testing.T) {
	const (
		uid = uint32(62461)
		gid = uint32(62462)
	)
	t.Run("test root is required", func(t *testing.T) {
		identityLockCovBorrowed(t, uid, gid)
		if err := validateBorrowedAgentIdentityDisposition(uid, gid, true, ""); err == nil ||
			!strings.Contains(err.Error(), "test agent identity lock root is required") {
			t.Fatalf("empty test root error = %v", err)
		}
	})

	t.Run("test root is forbidden", func(t *testing.T) {
		root := identityLockCovBorrowed(t, uid, gid)
		if err := validateBorrowedAgentIdentityDisposition(uid, gid, false, root); err == nil ||
			!strings.Contains(err.Error(), "test agent identity lock root is forbidden") {
			t.Fatalf("forbidden test root error = %v", err)
		}
	})

	t.Run("owner binding cannot be resolved", func(t *testing.T) {
		root := identityLockCovBorrowed(t, uid, gid)
		identityLockCovSeams(t)
		failure := errors.New("injected owner resolution failure")
		identityLockCovFaultFstatat(3, failure)
		err := validateBorrowedAgentIdentityDisposition(uid, gid, true, root)
		if !errors.Is(err, failure) ||
			!strings.Contains(err.Error(), "inspect borrowed agent identity owner "+
				strconv.FormatUint(uint64(uid), 10)+".owner") {
			t.Fatalf("owner resolution error = %v, want %v", err, failure)
		}
	})

	t.Run("authority root cannot be enumerated", func(t *testing.T) {
		identityLockCovSeams(t)
		root := configureAgentIdentityLockTestRoot(t)
		directory, err := bootstrapAgentIdentityLockDirectory(
			root, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
		)
		if err != nil {
			t.Fatal(err)
		}
		if err = directory.Close(); err != nil {
			t.Fatal(err)
		}
		if err = rejectBorrowedAgentIdentityTemporaries(directory, uid); !errors.Is(err, unix.EBADF) {
			t.Fatalf("unenumerable authority error = %v, want EBADF", err)
		}
	})
}

// TestRejectBorrowedAgentIdentityTemporariesScopesByUID proves the borrowed
// disposition scan refuses only the temporaries that are the borrower's own
// fault. A uid-scoped temporary named for another participant is that
// participant's in-flight atomic write and must be tolerated; the borrower's
// own unresolved temporary of each class still refuses; a malformed name is
// fatal; and a domain-global temporary is transient by construction, so it is
// absorbed by a bounded re-read and refused only once it persists.
func TestRejectBorrowedAgentIdentityTemporariesScopesByUID(t *testing.T) {
	const (
		borrower  = uint32(63700)
		bystander = uint32(995)
		suffix    = "0123456789abcdef01234567"
	)
	identityLockCovSeams(t)
	root := configureAgentIdentityLockTestRoot(t)
	directory, err := bootstrapAgentIdentityLockDirectory(
		root, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := directory.Close(); closeErr != nil {
			t.Errorf("close borrowed authority directory: %v", closeErr)
		}
	})
	authority := filepath.Join(root, "acp-go", "agent-identities")

	stage := func(t *testing.T, name string) string {
		t.Helper()
		path := filepath.Join(authority, name)
		if writeErr := os.WriteFile(path, []byte("{}\n"), 0o600); writeErr != nil {
			t.Fatal(writeErr)
		}
		t.Cleanup(func() {
			if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, unix.ENOENT) {
				t.Errorf("remove staged temporary %q: %v", name, removeErr)
			}
		})

		return name
	}

	for _, name := range []string{
		strconv.FormatUint(uint64(bystander), 10) + ".quarantine.next-" + suffix,
		strconv.FormatUint(uint64(bystander), 10) + ".owner.next-" + suffix,
	} {
		t.Run("tolerates bystander "+name, func(t *testing.T) {
			staged := stage(t, name)
			if rejectErr := rejectBorrowedAgentIdentityTemporaries(directory, borrower); rejectErr != nil {
				t.Fatalf("bystander temporary %q refused = %v, want tolerated", staged, rejectErr)
			}
		})
	}

	for _, name := range []string{
		strconv.FormatUint(uint64(borrower), 10) + ".quarantine.next-" + suffix,
		strconv.FormatUint(uint64(borrower), 10) + ".owner.next-" + suffix,
	} {
		t.Run("refuses own "+name, func(t *testing.T) {
			staged := stage(t, name)
			rejectErr := rejectBorrowedAgentIdentityTemporaries(directory, borrower)
			if rejectErr == nil || !strings.Contains(rejectErr.Error(), "unresolved temporary") ||
				!strings.Contains(rejectErr.Error(), staged) {
				t.Fatalf("own temporary %q refusal = %v", staged, rejectErr)
			}
		})
	}

	for _, name := range []string{
		"bad.quarantine.next-" + suffix,
		"bad.owner.next-" + suffix,
	} {
		t.Run("refuses malformed "+name, func(t *testing.T) {
			staged := stage(t, name)
			if rejectErr := rejectBorrowedAgentIdentityTemporaries(directory, borrower); rejectErr == nil {
				t.Fatalf("malformed temporary %q was tolerated", staged)
			}
		})
	}

	for _, name := range []string{
		"domain.json.next-" + suffix,
		".authority-probe-" + suffix,
	} {
		t.Run("refuses persistent domain-global "+name, func(t *testing.T) {
			staged := stage(t, name)
			started := time.Now()
			rejectErr := rejectBorrowedAgentIdentityTemporaries(directory, borrower)
			if rejectErr == nil || !strings.Contains(rejectErr.Error(), "unresolved temporary") ||
				!strings.Contains(rejectErr.Error(), staged) {
				t.Fatalf("persistent domain-global %q refusal = %v", staged, rejectErr)
			}
			if elapsed := time.Since(started); elapsed < borrowedAgentIdentityTemporaryReadDelay {
				t.Fatalf("domain-global refusal skipped the bounded re-read: elapsed %s", elapsed)
			}
		})
	}

	t.Run("absorbs a domain-global rename in flight", func(t *testing.T) {
		staged := stage(t, "domain.json.next-"+suffix)
		go func() {
			time.Sleep(borrowedAgentIdentityTemporaryReadDelay)
			_ = os.Remove(filepath.Join(authority, staged))
		}()
		if rejectErr := rejectBorrowedAgentIdentityTemporaries(directory, borrower); rejectErr != nil {
			t.Fatalf("in-flight domain rename refused = %v, want tolerated after re-read", rejectErr)
		}
	})
}

// TestInheritedAgentIdentityFlockStateRejectsMalformedEntries proves the flock
// entry a descriptor reports must be readable and fully parseable: an
// unreadable fdinfo aborts the proof, and a non-numeric sequence, a negative or
// non-numeric owner, and an inode identity that is not exactly
// major:minor:inode are all refused.
func TestInheritedAgentIdentityFlockStateRejectsMalformedEntries(t *testing.T) {
	descriptor := unix.Stat_t{Dev: unix.Mkdev(0, 0x26), Ino: 52599113}
	valid := strings.Fields("lock: 1: FLOCK ADVISORY WRITE 0 00:26:52599113 0 EOF")
	if err := validateInheritedAgentIdentityFlockLine(valid, descriptor, "WRITE"); err != nil {
		t.Fatalf("validate a well formed flock entry: %v", err)
	}
	for name, testCase := range map[string]struct {
		index     int
		value     string
		wantError string
	}{
		"sequence is not numeric":     {index: 1, value: "zz:", wantError: "malformed flock sequence"},
		"owner is not numeric":        {index: 5, value: "owner", wantError: "malformed flock owner"},
		"owner is negative":           {index: 5, value: "-1", wantError: "malformed flock owner"},
		"inode identity is truncated": {index: 6, value: "00:26", wantError: "malformed flock inode"},
	} {
		t.Run(name, func(t *testing.T) {
			fields := append([]string(nil), valid...)
			fields[testCase.index] = testCase.value
			err := validateInheritedAgentIdentityFlockLine(fields, descriptor, "WRITE")
			if err == nil || !strings.Contains(err.Error(), testCase.wantError) {
				t.Fatalf("flock entry error = %v, want one containing %q", err, testCase.wantError)
			}
		})
	}

	t.Run("fdinfo is unreadable", func(t *testing.T) {
		identityLockCovSeams(t)
		failure := errors.New("injected fdinfo read failure")
		agentIdentityLockReadFile = func(string) ([]byte, error) { return nil, failure }
		err := validateInheritedAgentIdentityFlock(os.Stdin, descriptor, "WRITE")
		if !errors.Is(err, failure) ||
			!strings.Contains(err.Error(), "read inherited agent identity flock state") {
			t.Fatalf("unreadable fdinfo error = %v, want %v", err, failure)
		}
	})
}

// TestInheritedStandaloneDispositionRefusesEveryDrift proves an inherited
// standalone identity is only re-accepted when its state root binding, mode
// selection, authority root, audit and immutable owner tuple are all exactly
// as they were sealed, and that a uid holding a permanent standalone owner
// binding can never be validated as a borrowed ownerless identity.
func TestInheritedStandaloneDispositionRefusesEveryDrift(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("inherited standalone disposition drift requires a root-owned authority")
	}
	const (
		uid     = uint32(62471)
		gid     = uint32(62472)
		ownerID = "identity-lock-cov-owner"
	)
	acquire := func(t *testing.T) (string, string) {
		t.Helper()
		identityLockCovSeams(t)
		root := configureAgentIdentityLockTestRoot(t)
		stateRoot := createAgentStandaloneProtectedStateRoot(t, uid, gid)
		identity, err := acquireAgentStandaloneIdentity(
			uid, gid, ownerID, stateRoot, true, root, make(chan struct{}), make(chan os.Signal),
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = identity.Close() })
		if err = validateInheritedStandaloneAgentIdentityDisposition(
			uid, gid, ownerID, stateRoot, true, root,
		); err != nil {
			t.Fatalf("standalone fixture is not itself acceptable: %v", err)
		}

		return root, stateRoot
	}

	t.Run("state root cannot be bound", func(t *testing.T) {
		root, _ := acquire(t)
		err := validateInheritedStandaloneAgentIdentityDisposition(
			uid, gid, ownerID, "relative/state", true, root,
		)
		if err == nil || !strings.Contains(err.Error(), "bind inherited standalone state root") {
			t.Fatalf("unbindable state root error = %v", err)
		}
	})

	t.Run("test root is required", func(t *testing.T) {
		_, stateRoot := acquire(t)
		err := validateInheritedStandaloneAgentIdentityDisposition(
			uid, gid, ownerID, stateRoot, true, "",
		)
		if err == nil || !strings.Contains(err.Error(), "test agent identity lock root is required") {
			t.Fatalf("empty test root error = %v", err)
		}
	})

	t.Run("test root is forbidden", func(t *testing.T) {
		root, stateRoot := acquire(t)
		err := validateInheritedStandaloneAgentIdentityDisposition(
			uid, gid, ownerID, stateRoot, false, root,
		)
		if err == nil || !strings.Contains(err.Error(), "test agent identity lock root is forbidden") {
			t.Fatalf("forbidden test root error = %v", err)
		}
	})

	t.Run("authority root is absent", func(t *testing.T) {
		root, stateRoot := acquire(t)
		err := validateInheritedStandaloneAgentIdentityDisposition(
			uid, gid, ownerID, stateRoot, true, filepath.Join(root, "absent"),
		)
		if !errors.Is(err, unix.ENOENT) {
			t.Fatalf("absent authority root error = %v, want ENOENT", err)
		}
	})

	t.Run("authority root holds an unresolved temporary", func(t *testing.T) {
		root, stateRoot := acquire(t)
		temporary := identityLockCovAuthorityPath(root, "domain.json.next-0123456789abcdef01234567")
		if err := os.WriteFile(temporary, []byte("{}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		err := validateInheritedStandaloneAgentIdentityDisposition(
			uid, gid, ownerID, stateRoot, true, root,
		)
		if err == nil || !strings.Contains(err.Error(), "unresolved temporary") {
			t.Fatalf("unresolved temporary error = %v", err)
		}
	})

	t.Run("authority root fails its audit", func(t *testing.T) {
		root, stateRoot := acquire(t)
		if err := os.WriteFile(identityLockCovAuthorityPath(root, "not-a-uid.owner"), []byte("{}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		err := validateInheritedStandaloneAgentIdentityDisposition(
			uid, gid, ownerID, stateRoot, true, root,
		)
		if err == nil ||
			!strings.Contains(err.Error(), "audit inherited standalone agent identity authority") ||
			!strings.Contains(err.Error(), `invalid standalone owner name "not-a-uid.owner"`) {
			t.Fatalf("unauditable authority error = %v", err)
		}
	})

	t.Run("owner binding is gone", func(t *testing.T) {
		root, stateRoot := acquire(t)
		// The retained ACTIVE marker outlives the owner binding, so the
		// authority still audits cleanly as an ownerless borrowed identity
		// once its affinity lock is present. Only the owner load may refuse.
		directory, err := openAgentIdentityLockDirectory(
			root, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
		)
		if err != nil {
			t.Fatal(err)
		}
		defer directory.Close()
		owner, err := loadAgentStandaloneOwner(
			directory, uid, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
		)
		if err != nil {
			t.Fatal(err)
		}
		ownerDigest := agentStandaloneOwnerDigest(owner)
		affinity, err := openAgentStandaloneNamedLock(
			directory, agentStandaloneAffinityLockName(ownerDigest), true,
			agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
		)
		if err != nil {
			t.Fatal(err)
		}
		if err = affinity.Close(); err != nil {
			t.Fatal(err)
		}
		if err = os.Remove(
			identityLockCovAuthorityPath(root, strconv.FormatUint(uint64(uid), 10)+".owner"),
		); err != nil {
			t.Fatal(err)
		}
		err = validateInheritedStandaloneAgentIdentityDisposition(
			uid, gid, ownerID, stateRoot, true, root,
		)
		if err == nil || !strings.Contains(err.Error(), "load inherited standalone owner") {
			t.Fatalf("absent owner binding error = %v", err)
		}
	})

	t.Run("permanently bound identity cannot be borrowed", func(t *testing.T) {
		root, _ := acquire(t)
		err := validateBorrowedAgentIdentityDisposition(uid, gid, true, root)
		if err == nil || !strings.Contains(err.Error(), "has a permanent owner binding") {
			t.Fatalf("permanently bound identity error = %v", err)
		}
	})
}
