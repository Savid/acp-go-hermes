//go:build linux

package hermes

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestBorrowedAuthorityDomainValidatesStrictCurrentRecord(t *testing.T) {
	restoreAgentIdentityLockTestSeams(t)
	root := configureAgentIdentityLockTestRoot(t)
	directory, err := bootstrapAgentIdentityLockDirectory(root, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID)
	if err != nil {
		t.Fatal(err)
	}
	defer directory.Close()
	record, err := currentAgentAuthorityDomain(directory)
	if err != nil {
		t.Fatal(err)
	}
	record.AuthorityID = "0123456789abcdef0123456789abcdef"
	payload, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	recordPath := filepath.Join(root, "acp-go", "agent-identities", "domain.json")
	if err = os.WriteFile(recordPath, append(payload, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	source, err := openAgentStandaloneNamedLock(
		directory, "domain.lock", true, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if err = unix.Flock(int(source.Fd()), unix.LOCK_SH|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	duplicate, err := duplicateAgentIdentityLock(source)
	if err != nil {
		t.Fatal(err)
	}
	adopted, err := adoptAgentAuthorityDomain(duplicate, false, "")
	if err != nil {
		t.Fatalf("adopt strict authority domain: %v", err)
	}
	if err = adopted.Close(); err != nil {
		t.Fatal(err)
	}
	var sourceStat unix.Stat_t
	if err = unix.Fstat(int(source.Fd()), &sourceStat); err != nil {
		t.Fatal(err)
	}
	if err = validateInheritedAgentIdentityFlock(source, sourceStat, "READ"); err != nil {
		t.Fatalf("authority adoption mutated the host's shared lease: %v", err)
	}

	var fields map[string]any
	if err = json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}
	fields["unexpected"] = true
	malformed, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(recordPath, append(malformed, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	duplicate, err = duplicateAgentIdentityLock(source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = adoptAgentAuthorityDomain(duplicate, false, ""); err == nil {
		t.Fatal("borrowed authority domain accepted an unknown record field")
	}
}

func TestAdoptedAgentIdentityLockRetainsOFDAndRejectsWrongName(t *testing.T) {
	restoreAgentIdentityLockTestSeams(t)
	root := configureAgentIdentityLockTestRoot(t)
	directory, err := bootstrapAgentIdentityLockDirectory(
		root, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer directory.Close()
	sourceFile, err := openAgentStandaloneNamedLock(
		directory, "1210.lock", true, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = unix.Flock(int(sourceFile.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	source := &agentIdentityLock{file: sourceFile}
	duplicate, err := duplicateAgentIdentityLock(source.file)
	if err != nil {
		t.Fatal(err)
	}
	adopted, err := adoptAgentIdentityLock(duplicate, 1210, false, "")
	if err != nil {
		t.Fatal(err)
	}
	var sourceStat unix.Stat_t
	if err = unix.Fstat(int(source.file.Fd()), &sourceStat); err != nil {
		t.Fatal(err)
	}
	if err = validateInheritedAgentIdentityFlock(source.file, sourceStat, "WRITE"); err != nil {
		t.Fatalf("identity adoption mutated the host's exclusive lease: %v", err)
	}
	if err = source.Close(); err != nil {
		t.Fatal(err)
	}
	contender, err := os.OpenFile(filepath.Join(root, "acp-go", "agent-identities", "1210.lock"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer contender.Close()
	if err = unix.Flock(int(contender.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != unix.EWOULDBLOCK {
		t.Fatalf("contender lock after source close = %v, want EWOULDBLOCK", err)
	}
	if err = adopted.Close(); err != nil {
		t.Fatal(err)
	}
	if err = unix.Flock(int(contender.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatalf("contender did not acquire after adopted descriptor close: %v", err)
	}

	wrongFile, err := openAgentStandaloneNamedLock(
		directory, "1211.lock", true, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = unix.Flock(int(wrongFile.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	wrong := &agentIdentityLock{file: wrongFile}
	defer wrong.Close()
	wrongDuplicate, err := duplicateAgentIdentityLock(wrong.file)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = adoptAgentIdentityLock(wrongDuplicate, 1212, false, ""); err == nil {
		t.Fatal("descriptor for a different UID lock was accepted")
	}

	unlockedSource, err := openAgentStandaloneNamedLock(
		directory, "1213.lock", true, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = unlockedSource.Close(); err != nil {
		t.Fatal(err)
	}
	unlocked, err := os.OpenFile(filepath.Join(root, "acp-go", "agent-identities", "1213.lock"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = adoptAgentIdentityLock(unlocked, 1213, false, ""); err == nil {
		t.Fatal("unlocked descriptor for the exact named lock was accepted")
	}
}

func TestAgentIdentityLockRejectsUnsafePaths(t *testing.T) {
	t.Run("runtime root mode", func(t *testing.T) {
		restoreAgentIdentityLockTestSeams(t)
		root := configureAgentIdentityLockTestRoot(t)
		if err := os.Chmod(root, 0o777); err != nil {
			t.Fatal(err)
		}
		if _, err := bootstrapAgentIdentityLockDirectory(
			root, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
		); err == nil {
			t.Fatal("world-writable runtime root accepted")
		}
	})

	t.Run("owner directory mode", func(t *testing.T) {
		restoreAgentIdentityLockTestSeams(t)
		root := configureAgentIdentityLockTestRoot(t)
		if err := os.Mkdir(filepath.Join(root, "acp-go"), 0o755); err != nil {
			t.Fatal(err)
		}
		if _, err := bootstrapAgentIdentityLockDirectory(
			root, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
		); err == nil {
			t.Fatal("unsafe owner directory accepted")
		}
	})

	t.Run("owner directory symlink", func(t *testing.T) {
		restoreAgentIdentityLockTestSeams(t)
		root := configureAgentIdentityLockTestRoot(t)
		if err := os.Symlink(t.TempDir(), filepath.Join(root, "acp-go")); err != nil {
			t.Fatal(err)
		}
		if _, err := bootstrapAgentIdentityLockDirectory(
			root, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
		); err == nil {
			t.Fatal("symlink owner directory accepted")
		}
	})

	t.Run("lock mode", func(t *testing.T) {
		restoreAgentIdentityLockTestSeams(t)
		root := configureAgentIdentityLockTestRoot(t)
		directory, err := bootstrapAgentIdentityLockDirectory(
			root, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
		)
		if err != nil {
			t.Fatal(err)
		}
		defer directory.Close()
		lock, err := openAgentStandaloneNamedLock(
			directory, "1205.lock", true, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
		)
		if err != nil {
			t.Fatal(err)
		}
		if err = lock.Close(); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(root, "acp-go", "agent-identities", "1205.lock")
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err = openAgentStandaloneNamedLock(
			directory, "1205.lock", false, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
		); err == nil {
			t.Fatal("unsafe lock mode accepted")
		}
	})

	t.Run("lock link count", func(t *testing.T) {
		restoreAgentIdentityLockTestSeams(t)
		root := configureAgentIdentityLockTestRoot(t)
		directory, err := bootstrapAgentIdentityLockDirectory(
			root, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
		)
		if err != nil {
			t.Fatal(err)
		}
		defer directory.Close()
		lock, err := openAgentStandaloneNamedLock(
			directory, "1206.lock", true, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
		)
		if err != nil {
			t.Fatal(err)
		}
		if err = lock.Close(); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(root, "acp-go", "agent-identities", "1206.lock")
		if err := os.Link(path, filepath.Join(root, "linked.lock")); err != nil {
			t.Fatal(err)
		}
		if _, err = openAgentStandaloneNamedLock(
			directory, "1206.lock", false, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
		); err == nil {
			t.Fatal("multiply-linked identity lock accepted")
		}
	})

	t.Run("untrusted owner", func(t *testing.T) {
		restoreAgentIdentityLockTestSeams(t)
		root := configureAgentIdentityLockTestRoot(t)
		agentIdentityLockTrustedUID++
		if _, err := bootstrapAgentIdentityLockDirectory(
			root, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
		); err == nil {
			t.Fatal("untrusted runtime root accepted")
		}
	})
}

func TestAgentIdentityAuthorityDirectoryCreationIsDurableAndUmaskIndependent(t *testing.T) {
	restoreAgentIdentityLockTestSeams(t)
	root := configureAgentIdentityLockTestRoot(t)
	previousUmask := unix.Umask(0o0777)
	t.Cleanup(func() { unix.Umask(previousUmask) })
	fchowns := 0
	fchmods := 0
	fsyncs := 0
	reopens := 0
	namedChecks := 0
	originalFchown := agentIdentityDirectoryFchown
	originalFchmod := agentIdentityDirectoryFchmod
	originalFsync := agentIdentityDirectoryFsync
	originalOpenat := agentIdentityDirectoryOpenat
	originalFstatat := agentIdentityDirectoryFstatat
	agentIdentityDirectoryFchown = func(fd, uid, gid int) error {
		fchowns++

		return originalFchown(fd, uid, gid)
	}
	agentIdentityDirectoryFchmod = func(fd int, mode uint32) error {
		fchmods++

		return originalFchmod(fd, mode)
	}
	agentIdentityDirectoryFsync = func(fd int) error {
		fsyncs++

		return originalFsync(fd)
	}
	agentIdentityDirectoryOpenat = func(dirfd int, path string, flags int, mode uint32) (int, error) {
		reopens++

		return originalOpenat(dirfd, path, flags, mode)
	}
	agentIdentityDirectoryFstatat = func(dirfd int, path string, stat *unix.Stat_t, flags int) error {
		namedChecks++

		return originalFstatat(dirfd, path, stat, flags)
	}

	directory, err := bootstrapAgentIdentityLockDirectory(
		root, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = directory.Close(); err != nil {
		t.Fatal(err)
	}
	if fchowns != 2 || fchmods != 2 || fsyncs != 4 || reopens != 4 || namedChecks != 2 {
		t.Fatalf(
			"directory durability calls chown=%d chmod=%d fsync=%d open=%d named=%d",
			fchowns, fchmods, fsyncs, reopens, namedChecks,
		)
	}
	for _, path := range []string{
		filepath.Join(root, "acp-go"),
		filepath.Join(root, "acp-go", "agent-identities"),
	} {
		info, statErr := os.Stat(path)
		if statErr != nil {
			t.Fatal(statErr)
		}
		if info.Mode().Perm() != 0o700 {
			t.Fatalf("%s mode = %o, want 0700", path, info.Mode().Perm())
		}
	}
}

func TestAgentIdentityAuthorityDirectoryCreationFaultsFailClosed(t *testing.T) {
	for _, fault := range []string{"chown", "chmod", "child fsync", "parent fsync", "close", "reopen", "named inode"} {
		t.Run(fault, func(t *testing.T) {
			restoreAgentIdentityLockTestSeams(t)
			root := configureAgentIdentityLockTestRoot(t)
			wantErr := errors.New("injected " + fault + " failure")
			switch fault {
			case "chown":
				agentIdentityDirectoryFchown = func(int, int, int) error { return wantErr }
			case "chmod":
				agentIdentityDirectoryFchmod = func(int, uint32) error { return wantErr }
			case "child fsync", "parent fsync":
				calls := 0
				failAt := 1
				if fault == "parent fsync" {
					failAt = 2
				}
				original := agentIdentityDirectoryFsync
				agentIdentityDirectoryFsync = func(fd int) error {
					calls++
					if calls == failAt {
						return wantErr
					}

					return original(fd)
				}
			case "close":
				agentIdentityDirectoryClose = func(file *os.File) error {
					if err := file.Close(); err != nil {
						return err
					}

					return wantErr
				}
			case "reopen":
				calls := 0
				original := agentIdentityDirectoryOpenat
				agentIdentityDirectoryOpenat = func(dirfd int, path string, flags int, mode uint32) (int, error) {
					calls++
					if calls == 2 {
						return -1, wantErr
					}

					return original(dirfd, path, flags, mode)
				}
			case "named inode":
				agentIdentityDirectoryFstatat = func(int, string, *unix.Stat_t, int) error { return wantErr }
			}

			directory, err := bootstrapAgentIdentityLockDirectory(
				root, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
			)
			if directory != nil {
				_ = directory.Close()
				t.Fatal("directory bootstrap succeeded despite injected durability fault")
			}
			if !errors.Is(err, wantErr) {
				t.Fatalf("directory bootstrap error = %v, want %v", err, wantErr)
			}
		})
	}
}

func TestAgentIdentityAuthorityDirectoryExistingWrongMetadataIsNeverRepaired(t *testing.T) {
	restoreAgentIdentityLockTestSeams(t)
	root := configureAgentIdentityLockTestRoot(t)
	path := filepath.Join(root, "acp-go")
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}
	fchmods := 0
	original := agentIdentityDirectoryFchmod
	agentIdentityDirectoryFchmod = func(fd int, mode uint32) error {
		fchmods++

		return original(fd, mode)
	}

	if _, err := bootstrapAgentIdentityLockDirectory(
		root, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
	); err == nil {
		t.Fatal("existing wrong-mode authority directory was accepted")
	}
	if fchmods != 0 {
		t.Fatalf("existing authority directory was repaired with %d chmod calls", fchmods)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o755 {
		t.Fatalf("existing authority directory mode mutated to %o", info.Mode().Perm())
	}
}

func TestAgentIdentityAdoptionDoesNotRecreateUnlinkedAuthorityDirectories(t *testing.T) {
	for _, kind := range []string{"uid", "domain"} {
		t.Run(kind, func(t *testing.T) {
			restoreAgentIdentityLockTestSeams(t)
			root := configureAgentIdentityLockTestRoot(t)
			directory, err := bootstrapAgentIdentityLockDirectory(
				root, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
			)
			if err != nil {
				t.Fatal(err)
			}
			name := "1220.lock"
			operation := unix.LOCK_EX | unix.LOCK_NB
			if kind == "domain" {
				name = "domain.lock"
				operation = unix.LOCK_SH | unix.LOCK_NB
			}
			source, err := openAgentStandaloneNamedLock(
				directory, name, true, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			if err = unix.Flock(int(source.Fd()), operation); err != nil {
				t.Fatal(err)
			}
			duplicate, err := duplicateAgentIdentityLock(source)
			if err != nil {
				t.Fatal(err)
			}
			if err = directory.Close(); err != nil {
				t.Fatal(err)
			}
			oldPath := filepath.Join(root, "acp-go")
			if err = os.Rename(oldPath, filepath.Join(root, "acp-go-unlinked")); err != nil {
				t.Fatal(err)
			}
			if kind == "uid" {
				_, err = adoptAgentIdentityLock(duplicate, 1220, false, "")
			} else {
				_, err = adoptAgentAuthorityDomain(duplicate, false, "")
			}
			if err == nil {
				t.Fatal("adoption accepted an unlinked authority directory")
			}
			if _, statErr := os.Stat(oldPath); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("adoption recreated authority path: %v", statErr)
			}
		})
	}
}

func TestBorrowedIdentityAdoptionRequiresFlockOnSuppliedOFD(t *testing.T) {
	for _, mode := range []string{"unlocked behind holder", "wrong shared mode", "malformed fdinfo"} {
		t.Run(mode, func(t *testing.T) {
			restoreAgentIdentityLockTestSeams(t)
			root := configureAgentIdentityLockTestRoot(t)
			directory, err := bootstrapAgentIdentityLockDirectory(
				root, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer directory.Close()
			source, err := openAgentStandaloneNamedLock(
				directory, "1230.lock", true, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			if mode == "unlocked behind holder" {
				holder, openErr := openAgentStandaloneNamedLock(
					directory, "1230.lock", false, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
				)
				if openErr != nil {
					t.Fatal(openErr)
				}
				defer holder.Close()
				if err = unix.Flock(int(holder.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
					t.Fatal(err)
				}
			} else if err = unix.Flock(int(source.Fd()), unix.LOCK_SH|unix.LOCK_NB); err != nil {
				t.Fatal(err)
			}
			if mode == "malformed fdinfo" {
				agentIdentityLockReadFile = func(string) ([]byte, error) {
					return []byte("lock:\tmalformed\n"), nil
				}
			}
			duplicate, err := duplicateAgentIdentityLock(source)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = adoptAgentIdentityLock(duplicate, 1230, false, ""); err == nil {
				t.Fatal("identity adoption accepted supplied OFD without exact exclusive flock")
			}
			if mode == "wrong shared mode" {
				var stat unix.Stat_t
				if err = unix.Fstat(int(source.Fd()), &stat); err != nil {
					t.Fatal(err)
				}
				if err = validateInheritedAgentIdentityFlock(source, stat, "READ"); err != nil {
					t.Fatalf("failed adoption mutated host shared lock: %v", err)
				}
			}
		})
	}
}

func TestBorrowedDomainAdoptionRejectsExclusiveOFDWithoutMutatingIt(t *testing.T) {
	restoreAgentIdentityLockTestSeams(t)
	root := configureAgentIdentityLockTestRoot(t)
	directory, err := bootstrapAgentIdentityLockDirectory(
		root, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer directory.Close()
	source, err := openAgentStandaloneNamedLock(
		directory, "domain.lock", true, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if err = unix.Flock(int(source.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	duplicate, err := duplicateAgentIdentityLock(source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = adoptAgentAuthorityDomain(duplicate, false, ""); err == nil {
		t.Fatal("domain adoption accepted an exclusive supplied OFD")
	}
	var stat unix.Stat_t
	if err = unix.Fstat(int(source.Fd()), &stat); err != nil {
		t.Fatal(err)
	}
	if err = validateInheritedAgentIdentityFlock(source, stat, "WRITE"); err != nil {
		t.Fatalf("failed domain adoption mutated host exclusive lock: %v", err)
	}
}

func configureAgentIdentityLockTestRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0o755); err != nil {
		t.Fatal(err)
	}
	agentIdentityLockRunRoot = root
	agentIdentityLockTrustedUID = uint32(os.Geteuid())
	agentIdentityLockTrustedGID = uint32(os.Getegid())
	return root
}

func restoreAgentIdentityLockTestSeams(t *testing.T) {
	t.Helper()
	root := agentIdentityLockRunRoot
	uid := agentIdentityLockTrustedUID
	gid := agentIdentityLockTrustedGID
	mkdirat := agentIdentityDirectoryMkdirat
	openat := agentIdentityDirectoryOpenat
	fchown := agentIdentityDirectoryFchown
	fchmod := agentIdentityDirectoryFchmod
	fsync := agentIdentityDirectoryFsync
	fstatat := agentIdentityDirectoryFstatat
	closeDirectory := agentIdentityDirectoryClose
	readFile := agentIdentityLockReadFile
	t.Cleanup(func() {
		agentIdentityLockRunRoot = root
		agentIdentityLockTrustedUID = uid
		agentIdentityLockTrustedGID = gid
		agentIdentityDirectoryMkdirat = mkdirat
		agentIdentityDirectoryOpenat = openat
		agentIdentityDirectoryFchown = fchown
		agentIdentityDirectoryFchmod = fchmod
		agentIdentityDirectoryFsync = fsync
		agentIdentityDirectoryFstatat = fstatat
		agentIdentityDirectoryClose = closeDirectory
		agentIdentityLockReadFile = readFile
	})
}
