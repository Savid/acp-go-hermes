//go:build linux

package hermes

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"golang.org/x/sys/unix"
)

func bindAgentStandaloneStateRoot(path string, uid, gid uint32) (agentStandaloneStateRoot, error) {
	if !validAgentStandaloneStateRootPath(path) {
		return agentStandaloneStateRoot{}, errors.New("standalone state root must be a clean absolute path")
	}
	fd, err := agentStandaloneStateRootOpen("/", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return agentStandaloneStateRoot{}, fmt.Errorf("open filesystem root for standalone state root: %w", err)
	}
	defer func() { _ = unix.Close(fd) }()
	how := &unix.OpenHow{
		Flags:   unix.O_PATH | unix.O_DIRECTORY | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS,
	}

	for _, component := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		var parent unix.Stat_t
		if err = agentStandaloneStateRootFstat(fd, &parent); err != nil {
			return agentStandaloneStateRoot{}, err
		}
		if parent.Mode&unix.S_IFMT != unix.S_IFDIR || parent.Uid != 0 || parent.Mode&0o022 != 0 {
			return agentStandaloneStateRoot{}, fmt.Errorf("standalone state root ancestor before %q is not protected root-owned storage", component)
		}
		next, openErr := unix.Openat2(fd, component, how)
		if openErr != nil {
			return agentStandaloneStateRoot{}, fmt.Errorf("open standalone state root component %q: %w", component, openErr)
		}
		_ = unix.Close(fd)
		fd = next
	}
	var final unix.Stat_t
	if err = agentStandaloneStateRootFstat(fd, &final); err != nil {
		return agentStandaloneStateRoot{}, err
	}
	if final.Mode&unix.S_IFMT != unix.S_IFDIR || final.Uid != uid || final.Gid != gid ||
		final.Mode&0o777 != 0o700 || final.Dev == 0 || final.Ino == 0 {
		return agentStandaloneStateRoot{}, errors.New("standalone state root must be the claimed UID:GID-owned mode-0700 directory")
	}

	return agentStandaloneStateRoot{Path: path, Dev: uint64(final.Dev), Ino: final.Ino}, nil
}

func validAgentStandaloneStateRootPath(path string) bool {
	if len(path) == 0 || len(path) > 4096 || !utf8.ValidString(path) || !filepath.IsAbs(path) ||
		filepath.Clean(path) != path || path == "/" || strings.IndexByte(path, 0) >= 0 {
		return false
	}
	for _, character := range path {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func revalidateAgentStandaloneStateRoot(want agentStandaloneStateRoot, uid, gid uint32) error {
	current, err := bindAgentStandaloneStateRoot(want.Path, uid, gid)
	if err != nil {
		return err
	}
	if current != want {
		return errors.New("standalone state root changed during identity claim")
	}

	return nil
}

// agentStandaloneSessionKey derives the durable session key for a standalone
// owner. The owner record carries only integers and strings, so encoding it
// cannot fail and there is no error to propagate.
func agentStandaloneSessionKey(owner agentStandaloneOwner) string {
	payload, _ := json.Marshal(owner)
	digest := sha256.Sum256(payload)

	return "standalone:" + hex.EncodeToString(digest[:])
}

// agentStandaloneRandomHex returns size bytes of hex-encoded cryptographic
// randomness. crypto/rand.Read is documented never to report failure — it
// panics instead — so there is no error to propagate.
func agentStandaloneRandomHex(size int) string {
	value := make([]byte, size)
	_, _ = rand.Read(value)

	return hex.EncodeToString(value)
}

func knownAgentStandaloneProvider(value string) bool {
	return slices.Contains([]string{
		"github.com/savid/acp-go-amp",
		"github.com/savid/acp-go-claude",
		"github.com/savid/acp-go-codex",
		"github.com/savid/acp-go-hermes",
		"github.com/savid/acp-go-opencode",
		"github.com/savid/acp-go-pi",
	}, value)
}

const (
	agentStandaloneOwnerKind = "standalone-provider"
	agentStandaloneOwnerID   = "github.com/savid/acp-go-hermes"
	agentStandaloneOwnerMax  = 8 << 10
	agentStandaloneMarkerMax = 1 << 20
	agentStandaloneClaimMax  = 30 * time.Second
	agentStandaloneRetry     = 10 * time.Millisecond
)

var (
	errAgentStandaloneCanceled       = errors.New("standalone agent identity acquisition canceled")
	errAgentStandaloneOwnerTemporary = errors.New("standalone owner temporary requires registry cleanup")
	errAgentStandaloneMarkerTempBusy = errors.New("standalone marker temporary has a live UID holder")
	errAgentStandaloneProbeLive      = errors.New("standalone authority probe has a live holder")
)

var agentStandaloneStateRootOpen = unix.Open
var agentStandaloneStateRootFstat = unix.Fstat
var agentStandaloneEntriesOpenat = unix.Openat
var agentStandaloneProbeCloseFD = unix.Close
var agentStandaloneCloseTemporary = func(file *os.File) error { return file.Close() }
var agentStandaloneVacancyScan = proveAgentStandaloneIdentityVacant
var agentStandaloneReadDir = os.ReadDir
var agentStandaloneReadFile = os.ReadFile
var agentStandaloneReplaceDomain = replaceAgentStandaloneDomainRecord
var agentStandaloneLockOpenat = unix.Openat
var agentStandaloneLockFchown = unix.Fchown
var agentStandaloneLockFchmod = unix.Fchmod
var agentStandaloneLockFileSync = func(file *os.File) error { return file.Sync() }
var agentStandaloneLockDirectorySync = unix.Fsync
var agentStandaloneLockClose = func(file *os.File) error { return file.Close() }
var agentStandaloneReleaseLock = func(file *os.File) error { return file.Close() }
var agentStandaloneLockFstatat = unix.Fstatat
var agentStandaloneFilesystemProbe = probeAgentStandaloneFilesystem
var agentStandaloneProbeFstatfs = unix.Fstatfs
var agentStandaloneReadlink = os.Readlink
var agentStandaloneProcessID = os.Getpid
var agentStandaloneNamespaceIdentity = agentAuthorityNamespaceIdentity
var agentStandaloneProbeFcntl = unix.FcntlInt
var agentStandaloneProbeUnlinkat = unix.Unlinkat
var agentStandaloneProbeDirectorySync = unix.Fsync
var agentStandaloneDurableFchown = unix.Fchown
var agentStandaloneDurableFchmod = unix.Fchmod
var agentStandaloneDurableFlock = unix.Flock
var agentStandaloneDurableWrite = func(file *os.File, payload []byte) (int, error) { return file.Write(payload) }
var agentStandaloneDurableSync = func(file *os.File) error { return file.Sync() }
var agentStandaloneDurableFstat = unix.Fstat
var agentStandaloneDurableFstatat = unix.Fstatat
var agentStandaloneDurableUnlinkat = unix.Unlinkat

type agentStandaloneOwner struct {
	Version   int                      `json:"version"`
	UID       uint32                   `json:"uid"`
	GID       uint32                   `json:"gid"`
	Kind      string                   `json:"kind"`
	Provider  string                   `json:"provider"`
	OwnerID   string                   `json:"ownerId"`
	StateRoot agentStandaloneStateRoot `json:"stateRoot"`
}

type agentStandaloneStateRoot struct {
	Path string `json:"path"`
	Dev  uint64 `json:"dev"`
	Ino  uint64 `json:"ino"`
}

type agentStandaloneMarker struct {
	Version    int                           `json:"version"`
	UID        uint32                        `json:"uid"`
	GID        uint32                        `json:"gid"`
	SessionKey string                        `json:"sessionKey"`
	State      string                        `json:"state"`
	LeaseID    string                        `json:"leaseId,omitempty"`
	Paths      []agentStandaloneManifestPath `json:"paths"`
}

type agentStandaloneManifestPath struct {
	Base     string   `json:"base"`
	Segments []string `json:"segments"`
	Action   string   `json:"action"`
	RootDev  uint64   `json:"rootDev"`
	RootIno  uint64   `json:"rootIno"`
}

type agentStandaloneIdentity struct {
	identity  *agentIdentityLock
	authority *agentIdentityLock
}

func acquireAgentStandaloneIdentity(
	uid uint32,
	gid uint32,
	ownerID string,
	stateRoot string,
	testOnly bool,
	testRoot string,
	canceled <-chan struct{},
	signals <-chan os.Signal,
) (*agentStandaloneIdentity, error) {
	deadline := time.Now().Add(agentStandaloneClaimMax)
	boundRoot, err := bindAgentStandaloneStateRoot(stateRoot, uid, gid)
	if err != nil {
		return nil, err
	}
	runRoot := agentIdentityLockRunRoot
	ownerUID := agentIdentityLockTrustedUID
	ownerGID := agentIdentityLockTrustedGID
	authorityTest := testRoot != ""
	if authorityTest {
		runRoot = testRoot
		ownerUID = uint32(os.Geteuid())
		ownerGID = uint32(os.Getegid())
	} else if testOnly {
		return nil, errors.New("test agent identity lock root is required")
	}
	authorityRoot := filepath.Join(runRoot, "acp-go", "agent-identities")
	if boundRoot.Path == authorityRoot || strings.HasPrefix(boundRoot.Path, authorityRoot+string(filepath.Separator)) {
		return nil, errors.New("standalone state root must be separate from the agent identity authority root")
	}
	directory, err := bootstrapAgentIdentityLockDirectory(runRoot, ownerUID, ownerGID)
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	wantOwner := agentStandaloneOwner{
		Version: 1, UID: uid, GID: gid, Kind: agentStandaloneOwnerKind,
		Provider: agentStandaloneOwnerID, OwnerID: ownerID, StateRoot: boundRoot,
	}
	authority, err := acquireAgentStandaloneDomain(
		directory, wantOwner, ownerUID, ownerGID, authorityTest, deadline, canceled, signals,
	)
	if err != nil {
		return nil, err
	}
	failAuthority := func(cause error) (*agentStandaloneIdentity, error) {
		return nil, errors.Join(cause, authority.Close())
	}
	identityFile, err := acquireAgentStandaloneOwnerIdentity(
		directory, wantOwner, ownerUID, ownerGID, deadline, canceled, signals,
	)
	if err != nil {
		return failAuthority(err)
	}

	return &agentStandaloneIdentity{
		identity:  &agentIdentityLock{file: identityFile},
		authority: &agentIdentityLock{file: authority},
	}, nil
}

func acquireAgentStandaloneOwnerIdentity(
	directory *os.File,
	want agentStandaloneOwner,
	ownerUID uint32,
	ownerGID uint32,
	deadline time.Time,
	canceled <-chan struct{},
	signals <-chan os.Signal,
) (*os.File, error) {
	for {
		if err := checkAgentStandaloneAcquisition(deadline, canceled, signals); err != nil {
			return nil, err
		}
		temporaries, err := agentStandaloneOwnerTemporariesPresent(directory)
		if err != nil {
			return nil, err
		}
		if temporaries {
			cleaned, busy, cleanupErr := drainAgentStandaloneOwnerTemporaries(
				directory, ownerUID, ownerGID, deadline, canceled, signals,
			)
			if cleanupErr != nil {
				return nil, cleanupErr
			}
			if cleaned || busy {
				if busy {
					if err = waitAgentStandaloneRetry(deadline, canceled, signals); err != nil {
						return nil, err
					}
				}
				continue
			}
		}
		existing, err := loadAgentStandaloneOwner(directory, want.UID, ownerUID, ownerGID)
		switch {
		case err == nil:
			if existing != want {
				return nil, fmt.Errorf("agent identity uid %d is permanently bound to another standalone owner", want.UID)
			}
			identityFile, acquireErr := acquireAgentStandaloneExistingOwner(
				directory, want, ownerUID, ownerGID, deadline, canceled, signals,
			)
			if errors.Is(acquireErr, errAgentStandaloneOwnerTemporary) {
				continue
			}
			return identityFile, acquireErr
		case !errors.Is(err, unix.ENOENT):
			return nil, err
		}

		ownersLock, err := acquireAgentStandaloneOwnersExclusive(
			directory, ownerUID, ownerGID, deadline, canceled, signals,
		)
		if err != nil {
			return nil, err
		}
		cleaned, busy, cleanupErr := drainAgentStandaloneOwnerTemporariesUnderLock(
			directory, ownerUID, ownerGID, deadline, canceled, signals,
		)
		if cleanupErr != nil {
			return nil, errors.Join(cleanupErr, ownersLock.Close())
		}
		if cleaned || busy {
			if err = agentStandaloneReleaseLock(ownersLock); err != nil {
				return nil, err
			}
			if busy {
				if err = waitAgentStandaloneRetry(deadline, canceled, signals); err != nil {
					return nil, err
				}
			}
			continue
		}
		existing, err = loadAgentStandaloneOwner(directory, want.UID, ownerUID, ownerGID)
		if err == nil {
			closeErr := agentStandaloneReleaseLock(ownersLock)
			if existing != want {
				return nil, errors.Join(
					fmt.Errorf("agent identity uid %d is permanently bound to another standalone owner", want.UID), closeErr,
				)
			}
			if closeErr != nil {
				return nil, closeErr
			}
			continue
		}
		if !errors.Is(err, unix.ENOENT) {
			return nil, errors.Join(err, ownersLock.Close())
		}
		if err = validateAgentStandaloneUIDLockMayBeCreated(directory, want.UID); err != nil {
			return nil, errors.Join(err, ownersLock.Close())
		}
		identityFile, acquired, err := tryAgentStandaloneNamedLock(
			directory, strconv.FormatUint(uint64(want.UID), 10)+".lock", true, ownerUID, ownerGID,
		)
		if err != nil {
			return nil, errors.Join(err, ownersLock.Close())
		}
		if !acquired {
			if err = agentStandaloneReleaseLock(ownersLock); err != nil {
				return nil, err
			}
			if err = waitAgentStandaloneRetry(deadline, canceled, signals); err != nil {
				return nil, err
			}
			continue
		}
		fail := func(cause error) (*os.File, error) {
			return nil, errors.Join(cause, identityFile.Close(), ownersLock.Close())
		}
		if err = cleanupAgentStandaloneTargetMarkerTemporaries(
			directory, want.UID, identityFile, ownerUID, ownerGID, deadline, canceled, signals,
		); err != nil {
			return fail(err)
		}
		if err = auditAgentStandaloneAuthorityRoot(
			directory, ownerUID, ownerGID, false, false, true, deadline, canceled, signals,
		); err != nil {
			return fail(err)
		}
		if err = completeAgentStandaloneOwnerClaim(
			directory, want, ownerUID, ownerGID, false, deadline, canceled, signals,
		); err != nil {
			return fail(err)
		}
		if err = agentStandaloneReleaseLock(ownersLock); err != nil {
			return nil, errors.Join(err, identityFile.Close())
		}
		return identityFile, nil
	}
}

func acquireAgentStandaloneExistingOwner(
	directory *os.File,
	want agentStandaloneOwner,
	ownerUID uint32,
	ownerGID uint32,
	deadline time.Time,
	canceled <-chan struct{},
	signals <-chan os.Signal,
) (*os.File, error) {
	identityFile, err := acquireAgentStandaloneNamedLock(
		directory, strconv.FormatUint(uint64(want.UID), 10)+".lock", unix.LOCK_EX,
		false, ownerUID, ownerGID, deadline, canceled, signals,
	)
	if err != nil {
		return nil, err
	}
	ownersLock, err := openAgentStandaloneNamedLock(directory, "owners.lock", false, ownerUID, ownerGID)
	if err != nil {
		return nil, errors.Join(err, identityFile.Close())
	}
	if err = agentStandaloneReleaseLock(ownersLock); err != nil {
		return nil, errors.Join(err, identityFile.Close())
	}
	if err = cleanupAgentStandaloneTargetMarkerTemporaries(
		directory, want.UID, identityFile, ownerUID, ownerGID, deadline, canceled, signals,
	); err != nil {
		return nil, errors.Join(err, identityFile.Close())
	}
	if err = auditAgentStandaloneAuthorityRoot(
		directory, ownerUID, ownerGID, false, false, true, deadline, canceled, signals,
	); err != nil {
		return nil, errors.Join(err, identityFile.Close())
	}
	if err = completeAgentStandaloneOwnerClaim(
		directory, want, ownerUID, ownerGID, true, deadline, canceled, signals,
	); err != nil {
		return nil, errors.Join(err, identityFile.Close())
	}
	return identityFile, nil
}

func completeAgentStandaloneOwnerClaim(
	directory *os.File,
	want agentStandaloneOwner,
	ownerUID uint32,
	ownerGID uint32,
	wasPresent bool,
	deadline time.Time,
	canceled <-chan struct{},
	signals <-chan os.Signal,
) error {
	if err := checkAgentStandaloneAcquisition(deadline, canceled, signals); err != nil {
		return err
	}
	if err := revalidateAgentStandaloneStateRoot(want.StateRoot, want.UID, want.GID); err != nil {
		return err
	}
	if err := claimAgentStandaloneOwner(
		directory, want, ownerUID, ownerGID, wasPresent, deadline, canceled, signals,
	); err != nil {
		return err
	}
	sessionKey := agentStandaloneSessionKey(want)
	if wasPresent {
		if err := proveAgentStandaloneIdentityVacantTwice(want.UID, want.GID, deadline, canceled, signals); err != nil {
			return err
		}
	} else if err := agentStandaloneVacancyScan(want.UID, want.GID, deadline, canceled, signals); err != nil {
		return fmt.Errorf("post-owner standalone task vacancy proof: %w", err)
	}
	if err := checkAgentStandaloneAcquisition(deadline, canceled, signals); err != nil {
		return err
	}
	if err := revalidateAgentStandaloneStateRoot(want.StateRoot, want.UID, want.GID); err != nil {
		return err
	}
	if err := checkAgentStandaloneAcquisition(deadline, canceled, signals); err != nil {
		return err
	}
	return publishAgentStandaloneActive(
		directory, want.UID, want.GID, ownerUID, ownerGID, sessionKey, deadline, canceled, signals,
	)
}

func (identity *agentStandaloneIdentity) Close() error {
	if identity == nil {
		return nil
	}
	var err error
	if identity.identity != nil {
		err = errors.Join(err, identity.identity.Close())
		identity.identity = nil
	}
	if identity.authority != nil {
		err = errors.Join(err, identity.authority.Close())
		identity.authority = nil
	}

	return err
}

func acquireAgentStandaloneDomain(
	directory *os.File,
	wantOwner agentStandaloneOwner,
	ownerUID uint32,
	ownerGID uint32,
	testOnly bool,
	deadline time.Time,
	canceled <-chan struct{},
	signals <-chan os.Signal,
) (*os.File, error) {
	for {
		shared, err := acquireAgentStandaloneNamedLock(
			directory, "domain.lock", unix.LOCK_SH, false,
			ownerUID, ownerGID, deadline, canceled, signals,
		)
		if errors.Is(err, unix.ENOENT) {
			shared, err = acquireAgentStandaloneMissingDomainLock(
				directory, ownerUID, ownerGID, deadline, canceled, signals,
			)
		}
		if err != nil {
			return nil, err
		}
		record, loadErr := loadAgentAuthorityDomainRecord(directory, ownerUID, ownerGID)
		if loadErr == nil {
			current, currentErr := currentAgentAuthorityDomain(directory)
			if currentErr != nil {
				_ = shared.Close()
				return nil, currentErr
			}
			if record.sameDomain(current) {
				current.AuthorityID = record.AuthorityID
				requiresExclusive, cleanupErr := adjudicateAgentStandaloneMatchingDomainTemporaries(
					directory, ownerUID, ownerGID, false,
				)
				if cleanupErr != nil {
					_ = shared.Close()
					return nil, cleanupErr
				}
				if !requiresExclusive {
					if err = normalizeAgentStandaloneSharedDomainLease(
						directory, shared, ownerUID, ownerGID, current,
					); err != nil {
						_ = shared.Close()
						return nil, err
					}

					return shared, nil
				}
				if err = agentStandaloneReleaseLock(shared); err != nil {
					return nil, err
				}
				exclusive, exclusiveErr := acquireAgentStandaloneNamedLock(
					directory, "domain.lock", unix.LOCK_EX, false,
					ownerUID, ownerGID, deadline, canceled, signals,
				)
				if exclusiveErr != nil {
					return nil, exclusiveErr
				}
				record, loadErr = loadAgentAuthorityDomainRecord(directory, ownerUID, ownerGID)
				current, currentErr = currentAgentAuthorityDomain(directory)
				if loadErr != nil || currentErr != nil || !record.sameDomain(current) {
					_ = exclusive.Close()
					if loadErr != nil && !errors.Is(loadErr, unix.ENOENT) {
						return nil, loadErr
					}
					if currentErr != nil {
						return nil, currentErr
					}
					continue
				}
				current.AuthorityID = record.AuthorityID
				if _, err = adjudicateAgentStandaloneMatchingDomainTemporaries(
					directory, ownerUID, ownerGID, true,
				); err != nil {
					_ = exclusive.Close()
					return nil, err
				}
				if err = normalizeAgentStandaloneSharedDomainLease(
					directory, exclusive, ownerUID, ownerGID, current,
				); err != nil {
					_ = exclusive.Close()
					return nil, err
				}

				return exclusive, nil
			}
		}
		if loadErr != nil && !errors.Is(loadErr, unix.ENOENT) {
			_ = shared.Close()
			return nil, loadErr
		}
		if err = agentStandaloneReleaseLock(shared); err != nil {
			return nil, err
		}
		exclusive, err := acquireAgentStandaloneNamedLock(
			directory, "domain.lock", unix.LOCK_EX, false,
			ownerUID, ownerGID, deadline, canceled, signals,
		)
		if err != nil {
			return nil, err
		}
		record, loadErr = loadAgentAuthorityDomainRecord(directory, ownerUID, ownerGID)
		if loadErr == nil {
			current, currentErr := currentAgentAuthorityDomain(directory)
			if currentErr != nil {
				_ = exclusive.Close()
				return nil, currentErr
			}
			if record.sameDomain(current) {
				if err = agentStandaloneDurableFlock(int(exclusive.Fd()), unix.LOCK_SH); err != nil {
					_ = exclusive.Close()
					return nil, err
				}
				if err = revalidateAgentStandaloneDomain(directory, ownerUID, ownerGID, current); err != nil {
					_ = exclusive.Close()
					return nil, err
				}
				return exclusive, nil
			}
			if !testOnly {
				if err = validateAgentStandaloneBinder(); err != nil {
					_ = exclusive.Close()
					return nil, err
				}
			}
			ownerTempsBusy, drainErr := drainAgentStandaloneDomainOwnerTemporaries(
				directory, ownerUID, ownerGID, deadline, canceled, signals,
			)
			if drainErr != nil || ownerTempsBusy {
				_ = exclusive.Close()
				if drainErr != nil {
					return nil, drainErr
				}
				if waitErr := waitAgentStandaloneRetry(deadline, canceled, signals); waitErr != nil {
					return nil, waitErr
				}
				continue
			}
			if err = auditAgentStandaloneAuthorityRoot(
				directory, ownerUID, ownerGID, false, true, false, deadline, canceled, signals,
			); err != nil {
				_ = exclusive.Close()
				if errors.Is(err, errAgentStandaloneMarkerTempBusy) {
					if waitErr := waitAgentStandaloneRetry(deadline, canceled, signals); waitErr != nil {
						return nil, waitErr
					}
					continue
				}
				return nil, err
			}
			var rebindIdentity *os.File
			if record.BootID == current.BootID {
				rebindIdentity, err = validateAgentStandaloneSameBootRebind(
					directory, wantOwner, ownerUID, ownerGID, deadline, canceled, signals,
				)
				if err != nil {
					_ = exclusive.Close()
					return nil, err
				}
			}
			current.AuthorityID = record.AuthorityID
			if err = agentStandaloneFilesystemProbe(directory, testOnly); err != nil {
				if rebindIdentity != nil {
					err = errors.Join(err, rebindIdentity.Close())
				}
				return nil, errors.Join(err, exclusive.Close())
			}
			if err = agentStandaloneReplaceDomain(directory, ownerUID, ownerGID, current); err != nil {
				if rebindIdentity != nil {
					err = errors.Join(err, rebindIdentity.Close())
				}
				return nil, errors.Join(err, exclusive.Close())
			}
			if rebindIdentity != nil {
				if err = agentStandaloneReleaseLock(rebindIdentity); err != nil {
					_ = exclusive.Close()
					return nil, err
				}
			}
			if err = agentStandaloneDurableFlock(int(exclusive.Fd()), unix.LOCK_SH); err != nil {
				_ = exclusive.Close()
				return nil, err
			}
			if err = revalidateAgentStandaloneDomain(directory, ownerUID, ownerGID, current); err != nil {
				_ = exclusive.Close()
				return nil, err
			}
			return exclusive, nil
		}
		if !errors.Is(loadErr, unix.ENOENT) {
			_ = exclusive.Close()
			return nil, loadErr
		}
		if !testOnly {
			if err = validateAgentStandaloneBinder(); err != nil {
				_ = exclusive.Close()
				return nil, err
			}
		}
		ownerTempsBusy, drainErr := drainAgentStandaloneDomainOwnerTemporaries(
			directory, ownerUID, ownerGID, deadline, canceled, signals,
		)
		if drainErr != nil || ownerTempsBusy {
			_ = exclusive.Close()
			if drainErr != nil {
				return nil, drainErr
			}
			if waitErr := waitAgentStandaloneRetry(deadline, canceled, signals); waitErr != nil {
				return nil, waitErr
			}
			continue
		}
		if err = auditAgentStandaloneAuthorityRoot(
			directory, ownerUID, ownerGID, true, true, false, deadline, canceled, signals,
		); err != nil {
			_ = exclusive.Close()
			if errors.Is(err, errAgentStandaloneMarkerTempBusy) {
				if waitErr := waitAgentStandaloneRetry(deadline, canceled, signals); waitErr != nil {
					return nil, waitErr
				}
				continue
			}
			return nil, err
		}
		if err = agentStandaloneFilesystemProbe(directory, testOnly); err != nil {
			_ = exclusive.Close()
			return nil, err
		}
		current, err := currentAgentAuthorityDomain(directory)
		if err != nil {
			_ = exclusive.Close()
			return nil, err
		}
		current.AuthorityID = agentStandaloneRandomHex(16)
		if err = agentStandaloneReplaceDomain(directory, ownerUID, ownerGID, current); err != nil {
			_ = exclusive.Close()
			return nil, err
		}
		if err = agentStandaloneDurableFlock(int(exclusive.Fd()), unix.LOCK_SH); err != nil {
			_ = exclusive.Close()
			return nil, err
		}
		if err = revalidateAgentStandaloneDomain(directory, ownerUID, ownerGID, current); err != nil {
			_ = exclusive.Close()
			return nil, err
		}
		return exclusive, nil
	}
}

func revalidateAgentStandaloneDomain(
	directory *os.File,
	ownerUID uint32,
	ownerGID uint32,
	want agentAuthorityDomainRecord,
) error {
	published, err := loadAgentAuthorityDomainRecord(directory, ownerUID, ownerGID)
	if err != nil {
		return err
	}
	if !published.sameDomain(want) || published.AuthorityID != want.AuthorityID {
		return errors.New("agent authority record changed during shared-lease transition")
	}

	return nil
}

func normalizeAgentStandaloneSharedDomainLease(
	directory *os.File,
	lease *os.File,
	ownerUID uint32,
	ownerGID uint32,
	want agentAuthorityDomainRecord,
) error {
	if err := agentStandaloneDurableFlock(int(lease.Fd()), unix.LOCK_SH); err != nil {
		return fmt.Errorf("normalize agent authority domain shared lease: %w", err)
	}

	return revalidateAgentStandaloneDomain(directory, ownerUID, ownerGID, want)
}

func adjudicateAgentStandaloneMatchingDomainTemporaries(
	directory *os.File,
	ownerUID uint32,
	ownerGID uint32,
	domainExclusive bool,
) (bool, error) {
	entries, err := agentStandaloneAuthorityEntries(directory)
	if err != nil {
		return false, err
	}
	requiresExclusive := false
	for _, entry := range entries {
		name := entry.Name()
		switch {
		case strings.HasPrefix(name, "domain.json.next"):
			if err = parseAgentStandaloneTemporarySuffix(name, "domain.json.next-", false); err != nil {
				return false, err
			}
			if err = validateAgentStandaloneTemporary(
				directory, name, ownerUID, ownerGID, agentAuthorityDomainMaxSize,
			); err != nil {
				return false, err
			}
			if !domainExclusive {
				requiresExclusive = true
				continue
			}
			if err = cleanupAgentStandaloneDomainTemporary(directory, name, ownerUID, ownerGID); err != nil {
				return false, err
			}
		case strings.HasPrefix(name, ".authority-probe"):
			err = cleanupAgentStandaloneProbeTemporary(directory, name, ownerUID, ownerGID)
			if errors.Is(err, errAgentStandaloneProbeLive) {
				continue
			}
			if err != nil {
				return false, err
			}
		}
	}

	return requiresExclusive, nil
}

func acquireAgentStandaloneNamedLock(
	directory *os.File,
	name string,
	operation int,
	allowCreate bool,
	ownerUID uint32,
	ownerGID uint32,
	deadline time.Time,
	canceled <-chan struct{},
	signals <-chan os.Signal,
) (*os.File, error) {
	file, err := openAgentStandaloneNamedLock(directory, name, allowCreate, ownerUID, ownerGID)
	if err != nil {
		return nil, err
	}
	for {
		if err = checkAgentStandaloneAcquisition(deadline, canceled, signals); err != nil {
			_ = file.Close()
			return nil, err
		}
		if err = agentStandaloneDurableFlock(int(file.Fd()), operation|unix.LOCK_NB); err == nil {
			return file, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			_ = file.Close()
			return nil, err
		}
		if err = waitAgentStandaloneRetry(deadline, canceled, signals); err != nil {
			_ = file.Close()
			return nil, err
		}
	}
}

func openAgentStandaloneNamedLock(
	directory *os.File,
	name string,
	allowCreate bool,
	ownerUID uint32,
	ownerGID uint32,
) (*os.File, error) {
	created := false
	flags := unix.O_RDWR | unix.O_CLOEXEC | unix.O_NOFOLLOW
	if allowCreate {
		created = true
		flags |= unix.O_CREAT | unix.O_EXCL
	}
	fd, err := agentStandaloneLockOpenat(int(directory.Fd()), name, flags, 0o600)
	if allowCreate && errors.Is(err, unix.EEXIST) {
		created = false
		fd, err = agentStandaloneLockOpenat(
			int(directory.Fd()), name, unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0,
		)
	}
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), name)
	if created {
		if err = agentStandaloneLockFchown(fd, int(ownerUID), int(ownerGID)); err == nil {
			err = agentStandaloneLockFchmod(fd, 0o600)
		}
		if err == nil {
			err = agentStandaloneLockFileSync(file)
		}
		if err == nil {
			err = agentStandaloneLockDirectorySync(int(directory.Fd()))
		}
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		if err = agentStandaloneLockClose(file); err != nil {
			return nil, fmt.Errorf("close new permanent lock before reopen: %w", err)
		}
		fd, err = agentStandaloneLockOpenat(
			int(directory.Fd()), name, unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0,
		)
		if err != nil {
			return nil, fmt.Errorf("reopen new permanent lock: %w", err)
		}
		file = os.NewFile(uintptr(fd), name)
	}
	if err = validateAgentIdentityLockFile(file, ownerUID, ownerGID); err != nil {
		_ = file.Close()
		return nil, err
	}
	var descriptor, named unix.Stat_t
	if err = agentStandaloneDurableFstat(fd, &descriptor); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err = agentStandaloneLockFstatat(int(directory.Fd()), name, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil ||
		descriptor.Dev != named.Dev || descriptor.Ino != named.Ino {
		_ = file.Close()
		return nil, errors.Join(errors.New("agent identity lock is not its permanent named inode"), err)
	}

	return file, nil
}

func tryAgentStandaloneNamedLock(
	directory *os.File,
	name string,
	allowCreate bool,
	ownerUID uint32,
	ownerGID uint32,
) (*os.File, bool, error) {
	file, err := openAgentStandaloneNamedLock(directory, name, allowCreate, ownerUID, ownerGID)
	if err != nil {
		return nil, false, err
	}
	if err = agentStandaloneDurableFlock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err == nil {
		return file, true, nil
	}
	closeErr := file.Close()
	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
		return nil, false, closeErr
	}
	return nil, false, errors.Join(err, closeErr)
}

func checkAgentStandaloneAcquisition(deadline time.Time, canceled <-chan struct{}, signals <-chan os.Signal) error {
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		return errors.New("standalone agent identity acquisition exceeded 30 seconds")
	}
	select {
	case <-canceled:
		return errAgentStandaloneCanceled
	case signal := <-signals:
		return fmt.Errorf("standalone agent identity acquisition interrupted by %s", signal)
	default:
		return nil
	}
}

func waitAgentStandaloneRetry(deadline time.Time, canceled <-chan struct{}, signals <-chan os.Signal) error {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return errors.New("standalone agent identity acquisition exceeded 30 seconds")
	}
	delay := agentStandaloneRetry
	if remaining < delay {
		delay = remaining
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-canceled:
		return errAgentStandaloneCanceled
	case signal := <-signals:
		return fmt.Errorf("standalone agent identity acquisition interrupted by %s", signal)
	case <-timer.C:
		return checkAgentStandaloneAcquisition(deadline, canceled, signals)
	}
}

func acquireAgentStandaloneMissingDomainLock(
	directory *os.File,
	ownerUID uint32,
	ownerGID uint32,
	deadline time.Time,
	canceled <-chan struct{},
	signals <-chan os.Signal,
) (*os.File, error) {
	entries, err := agentStandaloneAuthorityEntries(directory)
	if err != nil {
		return nil, err
	}
	if len(entries) != 0 {
		if len(entries) == 1 && entries[0].Name() == "domain.lock" {
			return acquireAgentStandaloneNamedLock(
				directory, "domain.lock", unix.LOCK_EX, false,
				ownerUID, ownerGID, deadline, canceled, signals,
			)
		}
		return nil, errors.New("agent authority domain lock is missing from a non-empty authority root")
	}
	return acquireAgentStandaloneNamedLock(
		directory, "domain.lock", unix.LOCK_EX, true, ownerUID, ownerGID, deadline, canceled, signals,
	)
}

func acquireAgentStandaloneOwnersExclusive(
	directory *os.File,
	ownerUID uint32,
	ownerGID uint32,
	deadline time.Time,
	canceled <-chan struct{},
	signals <-chan os.Signal,
) (*os.File, error) {
	lock, err := acquireAgentStandaloneNamedLock(
		directory, "owners.lock", unix.LOCK_EX, false, ownerUID, ownerGID, deadline, canceled, signals,
	)
	if !errors.Is(err, unix.ENOENT) {
		return lock, err
	}
	entries, err := agentStandaloneAuthorityEntries(directory)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.Name() == "owners.lock" {
			return acquireAgentStandaloneNamedLock(
				directory, "owners.lock", unix.LOCK_EX, false,
				ownerUID, ownerGID, deadline, canceled, signals,
			)
		}
		if entry.Name() != "domain.lock" && entry.Name() != "domain.json" {
			return nil, fmt.Errorf("permanent owners.lock is missing from non-pristine registry containing %q", entry.Name())
		}
	}
	return acquireAgentStandaloneNamedLock(
		directory, "owners.lock", unix.LOCK_EX, true, ownerUID, ownerGID, deadline, canceled, signals,
	)
}

func agentStandaloneAuthorityEntries(directory *os.File) ([]os.DirEntry, error) {
	duplicate, err := agentStandaloneEntriesOpenat(
		int(directory.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0,
	)
	if err != nil {
		return nil, err
	}
	reader := os.NewFile(uintptr(duplicate), "agent-authority-entries")
	entries, readErr := reader.ReadDir(-1)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil {
		return nil, errors.Join(readErr, closeErr)
	}
	return entries, nil
}

func validateAgentStandaloneUIDLockMayBeCreated(directory *os.File, uid uint32) error {
	lockName := strconv.FormatUint(uint64(uid), 10) + ".lock"
	var stat unix.Stat_t
	if err := unix.Fstatat(int(directory.Fd()), lockName, &stat, unix.AT_SYMLINK_NOFOLLOW); err == nil {
		return nil
	} else if !errors.Is(err, unix.ENOENT) {
		return err
	}
	entries, err := agentStandaloneAuthorityEntries(directory)
	if err != nil {
		return err
	}
	ownerName := strconv.FormatUint(uint64(uid), 10) + ".owner"
	markerName := strconv.FormatUint(uint64(uid), 10) + ".quarantine"
	for _, entry := range entries {
		name := entry.Name()
		if name == ownerName || name == markerName || strings.HasPrefix(name, ownerName+".next-") ||
			strings.HasPrefix(name, markerName+".next-") {
			return fmt.Errorf("uid %d permanent lock is missing while its registry state %q exists", uid, name)
		}
	}
	return nil
}

func validateAgentStandaloneBinder() error {
	self, err := validateAgentAuthorityPIDVisibility()
	if err != nil {
		return err
	}
	if err = validateAgentStandalonePIDNamespaceAnchor(self); err != nil {
		return err
	}
	if _, err = agentStandaloneReadFile("/proc/1/status"); err != nil {
		return fmt.Errorf("prove unrestricted root procfs visibility: %w", err)
	}
	const initialPIDNamespaceInode = 0xeffffffc
	if self.Ino != initialPIDNamespaceInode && agentStandaloneProcessID() != 1 {
		return errors.New("non-initial PID namespace may establish agent authority only from namespace PID 1")
	}

	return nil
}

func validateAgentStandalonePIDNamespaceAnchor(self agentAuthorityDomainInode) error {
	processID := agentStandaloneProcessID()
	anchor, err := agentStandaloneReadlink("/proc/self")
	if err != nil {
		return fmt.Errorf("resolve procfs self anchor: %w", err)
	}
	wantAnchor := strconv.Itoa(processID)
	if anchor != wantAnchor {
		return fmt.Errorf("procfs self anchor = %q, want %q", anchor, wantAnchor)
	}
	anchorNamespace, err := agentStandaloneNamespaceIdentity(filepath.Join("/proc", anchor, "ns/pid"))
	if err != nil {
		return fmt.Errorf("inspect procfs self PID namespace anchor: %w", err)
	}
	if self != anchorNamespace {
		return errors.New("standalone agent authority binder requires self and procfs PID namespaces to match")
	}

	return nil
}

func probeAgentStandaloneFilesystem(directory *os.File, testOnly bool) (probeErr error) {
	var filesystem unix.Statfs_t
	if err := agentStandaloneProbeFstatfs(int(directory.Fd()), &filesystem); err != nil {
		return err
	}
	if filesystem.Flags&unix.ST_RDONLY != 0 {
		return errors.New("agent authority filesystem is read-only")
	}
	if !testOnly {
		switch int64(filesystem.Type) {
		case 0xef53, 0x58465342, 0x9123683e, 0xf2f52010, 0x2fc12fc1, 0xca451a4e:
		default:
			return fmt.Errorf("agent authority filesystem type %#x is not in the local durable allowlist", filesystem.Type)
		}
	}
	first := ".authority-probe-" + agentStandaloneRandomHex(12)
	second := first + ".renamed"
	fd, err := unix.Openat(int(directory.Fd()), first, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), first)
	defer func() {
		for _, name := range []string{first, second} {
			if unlinkErr := agentStandaloneProbeUnlinkat(
				int(directory.Fd()), name, 0,
			); unlinkErr != nil && !errors.Is(unlinkErr, unix.ENOENT) {
				probeErr = errors.Join(probeErr, unlinkErr)
			}
		}
		probeErr = errors.Join(probeErr, agentStandaloneProbeDirectorySync(int(directory.Fd())))
		probeErr = errors.Join(probeErr, file.Close())
	}()
	if err = setAgentStandaloneProbeCloseOnExec(file); err != nil {
		return err
	}
	if err = agentStandaloneDurableFchown(fd, int(os.Geteuid()), int(os.Getegid())); err != nil {
		return err
	}
	if err = agentStandaloneDurableFchmod(fd, 0o600); err != nil {
		return err
	}
	if err = agentStandaloneDurableFlock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return err
	}
	contender, err := unix.Openat(int(directory.Fd()), first, unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	contenderErr := agentStandaloneDurableFlock(contender, unix.LOCK_EX|unix.LOCK_NB)
	closeErr := agentStandaloneProbeCloseFD(contender)
	if contenderErr == nil || (!errors.Is(contenderErr, unix.EWOULDBLOCK) && !errors.Is(contenderErr, unix.EAGAIN)) {
		return errors.Join(errors.New("agent authority filesystem lacks separate-open flock exclusion"), closeErr)
	}
	if closeErr != nil {
		return closeErr
	}
	if _, err = agentStandaloneDurableWrite(file, []byte("acp-go-authority-probe\n")); err != nil {
		return err
	}
	if err = agentStandaloneDurableSync(file); err != nil {
		return err
	}
	var before, after unix.Stat_t
	if err = agentStandaloneDurableFstat(fd, &before); err != nil {
		return err
	}
	if err = unix.Renameat(int(directory.Fd()), first, int(directory.Fd()), second); err != nil {
		return err
	}
	if err = agentStandaloneDurableFstatat(int(directory.Fd()), second, &after, unix.AT_SYMLINK_NOFOLLOW); err != nil ||
		before.Dev != after.Dev || before.Ino != after.Ino {
		return errors.Join(errors.New("agent authority filesystem rename did not preserve inode identity"), err)
	}

	return unix.Fsync(int(directory.Fd()))
}

func setAgentStandaloneProbeCloseOnExec(file *os.File) error {
	flags, err := agentStandaloneProbeFcntl(file.Fd(), unix.F_GETFD, 0)
	if err != nil {
		return fmt.Errorf("read authority probe descriptor flags: %w", err)
	}
	if _, err = agentStandaloneProbeFcntl(file.Fd(), unix.F_SETFD, flags|unix.FD_CLOEXEC); err != nil {
		return fmt.Errorf("set authority probe close-on-exec: %w", err)
	}
	flags, err = agentStandaloneProbeFcntl(file.Fd(), unix.F_GETFD, 0)
	if err != nil {
		return fmt.Errorf("re-read authority probe descriptor flags: %w", err)
	}
	if flags&unix.FD_CLOEXEC == 0 {
		return errors.New("authority probe descriptor is not close-on-exec")
	}

	return nil
}

func validateAgentStandaloneSameBootRebind(
	directory *os.File,
	want agentStandaloneOwner,
	ownerUID uint32,
	ownerGID uint32,
	deadline time.Time,
	canceled <-chan struct{},
	signals <-chan os.Signal,
) (*os.File, error) {
	ownersLock, err := acquireAgentStandaloneNamedLock(
		directory, "owners.lock", unix.LOCK_SH, false,
		ownerUID, ownerGID, deadline, canceled, signals,
	)
	if err != nil {
		return nil, err
	}
	failOwners := func(cause error) (*os.File, error) {
		return nil, errors.Join(cause, ownersLock.Close())
	}
	entries, err := agentStandaloneAuthorityEntries(directory)
	if err != nil {
		return failOwners(err)
	}
	ownerCount := 0
	for _, entry := range entries {
		if err = checkAgentStandaloneAcquisition(deadline, canceled, signals); err != nil {
			return failOwners(err)
		}
		uidText, ownerEntry := strings.CutSuffix(entry.Name(), ".owner")
		if !ownerEntry {
			continue
		}
		uid, parseErr := parseAgentStandaloneUID(uidText)
		if parseErr != nil {
			return failOwners(parseErr)
		}
		ownerCount++
		if uid != want.UID {
			return failOwners(fmt.Errorf(
				"same-boot authority rebind is blocked by standalone owner uid %d", uid,
			))
		}
	}
	if ownerCount != 1 {
		return failOwners(errors.New("same-boot authority rebind requires exactly one standalone owner binding"))
	}

	uidLock, acquired, lockErr := tryAgentStandaloneNamedLock(
		directory, strconv.FormatUint(uint64(want.UID), 10)+".lock", false, ownerUID, ownerGID,
	)
	if lockErr != nil || !acquired {
		return failOwners(errors.Join(
			fmt.Errorf("same-boot standalone owner uid %d still has a live UID lock holder", want.UID),
			lockErr,
		))
	}
	failIdentity := func(cause error) (*os.File, error) {
		return nil, errors.Join(cause, uidLock.Close(), ownersLock.Close())
	}

	owner, err := loadAgentStandaloneOwner(directory, want.UID, ownerUID, ownerGID)
	if err != nil || owner != want {
		return failIdentity(errors.Join(
			errors.New("same-boot authority rebind requires the exact standalone owner binding"), err,
		))
	}
	marker, err := loadAgentStandaloneMarker(directory, want.UID, ownerUID, ownerGID)
	if err != nil {
		return failIdentity(errors.Join(
			errors.New("same-boot authority rebind requires the retained standalone ACTIVE marker"), err,
		))
	}
	sessionKey := agentStandaloneSessionKey(owner)
	if marker.State != "active" || marker.GID != owner.GID || marker.SessionKey != sessionKey || len(marker.Paths) != 0 {
		return failIdentity(errors.New("same-boot authority rebind requires the exact retained standalone ACTIVE marker"))
	}
	if err = proveAgentStandaloneIdentityVacantTwice(
		want.UID, want.GID, deadline, canceled, signals,
	); err != nil {
		return failIdentity(fmt.Errorf("same-boot standalone task vacancy proof: %w", err))
	}
	if err = agentStandaloneReleaseLock(ownersLock); err != nil {
		return nil, errors.Join(err, uidLock.Close())
	}

	return uidLock, nil
}

func auditAgentStandaloneAuthorityRoot(
	directory *os.File,
	ownerUID uint32,
	ownerGID uint32,
	requireEmpty bool,
	allowCleanup bool,
	allowOwnerlessActive bool,
	deadline time.Time,
	canceled <-chan struct{},
	signals <-chan os.Signal,
) error {
	entries, err := agentStandaloneAuthorityEntries(directory)
	if err != nil {
		return err
	}
	owners := make(map[uint32]agentStandaloneOwner)
	markers := make(map[uint32]agentStandaloneMarker)
	uidLocks := make(map[uint32]struct{})
	affinityLocks := make(map[string]struct{})
	ownersLockPresent := false
	registryStatePresent := false
	authorityPath, err := agentStandaloneAuthorityPath(directory)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err = checkAgentStandaloneAcquisition(deadline, canceled, signals); err != nil {
			return err
		}
		name := entry.Name()
		if uidText, ok := strings.CutSuffix(name, ".owner"); ok {
			uid, parseErr := parseAgentStandaloneUID(uidText)
			if parseErr != nil {
				return fmt.Errorf("invalid standalone owner name %q", name)
			}
			owner, loadErr := loadAgentStandaloneOwner(directory, uid, ownerUID, ownerGID)
			if loadErr != nil {
				return loadErr
			}
			owners[uid] = owner
		}
	}
	for _, entry := range entries {
		if err = checkAgentStandaloneAcquisition(deadline, canceled, signals); err != nil {
			return err
		}
		name := entry.Name()
		if name == "domain.lock" || name == "domain.json" {
			continue
		}
		if name == "owners.lock" {
			if requireEmpty {
				return errors.New("agent authority record is missing but permanent owners.lock exists")
			}
			lock, openErr := openAgentStandaloneNamedLock(directory, name, false, ownerUID, ownerGID)
			if openErr != nil {
				return openErr
			}
			if closeErr := agentStandaloneReleaseLock(lock); closeErr != nil {
				return closeErr
			}
			ownersLockPresent = true
			continue
		}
		if strings.Contains(name, ".owner.next-") {
			return fmt.Errorf("%w: %q", errAgentStandaloneOwnerTemporary, name)
		}
		if strings.HasPrefix(name, "domain.json.next-") {
			if !allowCleanup {
				return fmt.Errorf("domain record temporary %q requires domain-exclusive cleanup", name)
			}
			if err = cleanupAgentStandaloneDomainTemporary(directory, name, ownerUID, ownerGID); err != nil {
				return err
			}
			continue
		}
		if strings.HasPrefix(name, ".authority-probe-") {
			if !allowCleanup {
				return fmt.Errorf("authority probe temporary %q requires domain-exclusive cleanup", name)
			}
			if err = cleanupAgentStandaloneProbeTemporary(directory, name, ownerUID, ownerGID); err != nil {
				return err
			}
			continue
		}
		if strings.Contains(name, ".quarantine.next-") {
			uid, parseErr := parseAgentStandaloneMarkerTemporary(name)
			if parseErr != nil {
				return parseErr
			}
			if err = validateAgentStandaloneTemporary(directory, name, ownerUID, ownerGID, agentStandaloneMarkerMax); err != nil {
				return err
			}
			if !allowCleanup {
				if allowOwnerlessActive {
					continue
				}
				return fmt.Errorf("marker temporary %q requires domain-exclusive cleanup", name)
			}
			if err = cleanupAgentStandaloneMarkerTemporary(directory, uid, name, ownerUID, ownerGID); err != nil {
				return err
			}
			continue
		}
		// The first pass parsed and loaded every ".owner" entry, so any name
		// that still carries that suffix here is already known-good.
		if strings.HasSuffix(name, ".owner") {
			if requireEmpty {
				return errors.New("agent authority record is missing but a permanent owner binding exists")
			}
			registryStatePresent = true
			continue
		}
		if uidText, ok := strings.CutSuffix(name, ".quarantine"); ok {
			uid, parseErr := parseAgentStandaloneUID(uidText)
			if parseErr != nil {
				return fmt.Errorf("invalid standalone marker name %q", name)
			}
			marker, loadErr := loadAgentStandaloneMarker(directory, uid, ownerUID, ownerGID)
			if loadErr != nil {
				return loadErr
			}
			if requireEmpty {
				return errors.New("agent authority record is missing but a durable marker exists")
			}
			markers[uid] = marker
			registryStatePresent = true
			continue
		}
		if uidText, ok := strings.CutSuffix(name, ".lock"); ok {
			if requireEmpty {
				return fmt.Errorf("agent authority record is missing but root contains prior lock %q", name)
			}
			if strings.HasPrefix(uidText, "affinity-") {
				shardText := strings.TrimPrefix(uidText, "affinity-")
				shard, parseErr := strconv.ParseUint(shardText, 16, 16)
				if parseErr != nil || len(shardText) != 4 || shardText != strings.ToLower(shardText) || shard >= 4096 {
					return fmt.Errorf("agent authority root contains invalid affinity lock %q", name)
				}
				affinityLocks[name] = struct{}{}
			} else {
				uid, parseErr := parseAgentStandaloneUID(uidText)
				if parseErr != nil {
					return fmt.Errorf("agent authority root contains unknown lock %q", name)
				}
				uidLocks[uid] = struct{}{}
			}
			lock, openErr := openAgentStandaloneNamedLock(directory, name, false, ownerUID, ownerGID)
			if openErr != nil {
				return openErr
			}
			if closeErr := agentStandaloneReleaseLock(lock); closeErr != nil {
				return closeErr
			}
			registryStatePresent = true
			continue
		}
		return fmt.Errorf("agent authority root contains unknown entry %q", name)
	}
	if registryStatePresent && !ownersLockPresent {
		return errors.New("agent identity registry state exists without permanent owners.lock")
	}
	seenGIDs := make(map[uint32]uint32, len(owners))
	seenOwners := make(map[string]uint32, len(owners))
	seenStatePaths := make(map[string]uint32, len(owners))
	seenStateInodes := make(map[[2]uint64]uint32, len(owners))
	for uid, owner := range owners {
		if agentStandalonePathWithin(owner.StateRoot.Path, authorityPath) {
			return fmt.Errorf("standalone owner uid %d uses the authority registry as its state root", uid)
		}
		if _, present := uidLocks[uid]; !present {
			return fmt.Errorf("standalone owner uid %d exists without its permanent UID lock", uid)
		}
		if prior, duplicate := seenGIDs[owner.GID]; duplicate {
			return fmt.Errorf("standalone gid %d is duplicated by uids %d and %d", owner.GID, prior, uid)
		}
		seenGIDs[owner.GID] = uid
		ownerKey := owner.Provider + "\x00" + owner.OwnerID
		if prior, duplicate := seenOwners[ownerKey]; duplicate {
			return fmt.Errorf("standalone provider owner %q is duplicated by uids %d and %d", owner.OwnerID, prior, uid)
		}
		seenOwners[ownerKey] = uid
		if prior, duplicate := seenStatePaths[owner.StateRoot.Path]; duplicate {
			return fmt.Errorf("standalone state root path %q is duplicated by uids %d and %d", owner.StateRoot.Path, prior, uid)
		}
		seenStatePaths[owner.StateRoot.Path] = uid
		inodeKey := [2]uint64{owner.StateRoot.Dev, owner.StateRoot.Ino}
		if prior, duplicate := seenStateInodes[inodeKey]; duplicate {
			return fmt.Errorf("standalone state root inode is duplicated by uids %d and %d", prior, uid)
		}
		seenStateInodes[inodeKey] = uid
	}
	for uid, marker := range markers {
		if _, present := uidLocks[uid]; !present {
			return fmt.Errorf("durable marker uid %d exists without its permanent UID lock", uid)
		}
		if prior, duplicate := seenGIDs[marker.GID]; duplicate && prior != uid {
			return fmt.Errorf("agent identity gid %d is duplicated by uids %d and %d", marker.GID, prior, uid)
		}
		seenGIDs[marker.GID] = uid
		if owner, bound := owners[uid]; bound {
			sessionKey := agentStandaloneSessionKey(owner)
			if marker.State != "active" || marker.GID != owner.GID || marker.SessionKey != sessionKey || len(marker.Paths) != 0 {
				return fmt.Errorf("standalone owner uid %d has an incompatible retained marker", uid)
			}
			continue
		}
		affinityName := agentStandaloneAffinityLockName(marker.SessionKey)
		if _, present := affinityLocks[affinityName]; !present {
			return fmt.Errorf("ownerless durable marker uid %d exists without permanent affinity lock %q", uid, affinityName)
		}
		if marker.State == "active" && !allowOwnerlessActive {
			return fmt.Errorf("provider cannot recover ownerless ACTIVE uid %d; authoritative host recovery is required", uid)
		}
	}

	return nil
}

func agentStandaloneAffinityLockName(key string) string {
	digest := sha256.Sum256([]byte(key))
	shard := (uint16(digest[0])<<8 | uint16(digest[1])) & 4095
	return fmt.Sprintf("affinity-%04x.lock", shard)
}

func parseAgentStandaloneTemporarySuffix(name, prefix string, allowRenamed bool) error {
	suffix, ok := strings.CutPrefix(name, prefix)
	if !ok {
		return fmt.Errorf("temporary entry %q has an invalid name", name)
	}
	if allowRenamed {
		suffix, _ = strings.CutSuffix(suffix, ".renamed")
	}
	if len(suffix) != 24 || suffix != strings.ToLower(suffix) {
		return fmt.Errorf("temporary entry %q has an invalid name", name)
	}
	if _, err := hex.DecodeString(suffix); err != nil {
		return fmt.Errorf("temporary entry %q has an invalid name", name)
	}
	return nil
}

func agentStandaloneAuthorityPath(directory *os.File) (string, error) {
	path, err := agentStandaloneReadlink(fmt.Sprintf("/proc/self/fd/%d", directory.Fd()))
	if err != nil {
		return "", fmt.Errorf("resolve agent authority root path: %w", err)
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", errors.New("agent authority root did not resolve to a clean absolute path")
	}
	if strings.HasSuffix(path, " (deleted)") {
		return "", errors.New("agent authority root descriptor refers to a deleted directory")
	}
	return path, nil
}

func agentStandalonePathWithin(path, root string) bool {
	return path == root || strings.HasPrefix(path, root+string(filepath.Separator))
}

func cleanupAgentStandaloneOwnerTemporary(
	directory *os.File,
	name string,
	ownerUID uint32,
	ownerGID uint32,
) error {
	if _, err := parseAgentStandaloneOwnerTemporary(name); err != nil {
		return err
	}
	if err := validateAgentStandaloneTemporary(directory, name, ownerUID, ownerGID, agentStandaloneOwnerMax); err != nil {
		return err
	}
	if err := agentStandaloneDurableUnlinkat(int(directory.Fd()), name, 0); err != nil {
		return err
	}
	return unix.Fsync(int(directory.Fd()))
}

func parseAgentStandaloneOwnerTemporary(name string) (uint32, error) {
	uidText, suffix, ok := strings.Cut(name, ".owner.next-")
	uid, err := parseAgentStandaloneUID(uidText)
	if !ok || err != nil || len(suffix) != 24 || suffix != strings.ToLower(suffix) {
		return 0, fmt.Errorf("owner temporary %q has an invalid name", name)
	}
	if _, err = hex.DecodeString(suffix); err != nil {
		return 0, fmt.Errorf("owner temporary %q has an invalid name", name)
	}
	return uid, nil
}

func agentStandaloneOwnerTemporariesPresent(directory *os.File) (bool, error) {
	entries, err := agentStandaloneAuthorityEntries(directory)
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), ".owner.next-") {
			return true, nil
		}
	}
	return false, nil
}

func drainAgentStandaloneOwnerTemporaries(
	directory *os.File,
	ownerUID uint32,
	ownerGID uint32,
	deadline time.Time,
	canceled <-chan struct{},
	signals <-chan os.Signal,
) (cleaned bool, busy bool, cleanupErr error) {
	ownersLock, err := acquireAgentStandaloneOwnersExclusive(
		directory, ownerUID, ownerGID, deadline, canceled, signals,
	)
	if err != nil {
		return false, false, err
	}
	cleaned, busy, cleanupErr = drainAgentStandaloneOwnerTemporariesUnderLock(
		directory, ownerUID, ownerGID, deadline, canceled, signals,
	)
	return cleaned, busy, errors.Join(cleanupErr, ownersLock.Close())
}

func drainAgentStandaloneDomainOwnerTemporaries(
	directory *os.File,
	ownerUID uint32,
	ownerGID uint32,
	deadline time.Time,
	canceled <-chan struct{},
	signals <-chan os.Signal,
) (busy bool, drainErr error) {
	present, err := agentStandaloneOwnerTemporariesPresent(directory)
	if err != nil || !present {
		return false, err
	}
	_, busy, err = drainAgentStandaloneOwnerTemporaries(
		directory, ownerUID, ownerGID, deadline, canceled, signals,
	)
	return busy, err
}

func drainAgentStandaloneOwnerTemporariesUnderLock(
	directory *os.File,
	ownerUID uint32,
	ownerGID uint32,
	deadline time.Time,
	canceled <-chan struct{},
	signals <-chan os.Signal,
) (cleaned bool, busy bool, cleanupErr error) {
	entries, err := agentStandaloneAuthorityEntries(directory)
	if err != nil {
		return false, false, err
	}
	for _, entry := range entries {
		if err = checkAgentStandaloneAcquisition(deadline, canceled, signals); err != nil {
			return cleaned, false, err
		}
		if !strings.Contains(entry.Name(), ".owner.next-") {
			continue
		}
		uid, parseErr := parseAgentStandaloneOwnerTemporary(entry.Name())
		if parseErr != nil {
			return cleaned, false, parseErr
		}
		uidLock, acquired, lockErr := tryAgentStandaloneNamedLock(
			directory, strconv.FormatUint(uint64(uid), 10)+".lock", false, ownerUID, ownerGID,
		)
		if lockErr != nil {
			return cleaned, false, lockErr
		}
		if !acquired {
			return cleaned, true, nil
		}
		cleanupErr = cleanupAgentStandaloneOwnerTemporary(directory, entry.Name(), ownerUID, ownerGID)
		closeErr := uidLock.Close()
		if cleanupErr != nil || closeErr != nil {
			return cleaned, false, errors.Join(cleanupErr, closeErr)
		}
		cleaned = true
	}
	return cleaned, false, nil
}

func parseAgentStandaloneMarkerTemporary(name string) (uint32, error) {
	uidText, suffix, ok := strings.Cut(name, ".quarantine.next-")
	uid, err := parseAgentStandaloneUID(uidText)
	if !ok || err != nil || len(suffix) != 24 || suffix != strings.ToLower(suffix) {
		return 0, fmt.Errorf("marker temporary %q has an invalid name", name)
	}
	if _, err = hex.DecodeString(suffix); err != nil {
		return 0, fmt.Errorf("marker temporary %q has an invalid name", name)
	}
	return uid, nil
}

func validateAgentStandaloneTemporary(
	directory *os.File,
	name string,
	ownerUID uint32,
	ownerGID uint32,
	maxSize int64,
) error {
	var stat unix.Stat_t
	if err := unix.Fstatat(int(directory.Fd()), name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Uid != ownerUID || stat.Gid != ownerGID ||
		stat.Nlink != 1 || stat.Mode&0o777 != 0o600 || stat.Size < 0 || stat.Size > maxSize {
		return fmt.Errorf("temporary entry %q is not a trusted bounded regular file", name)
	}
	return nil
}

func cleanupAgentStandaloneDomainTemporary(
	directory *os.File,
	name string,
	ownerUID uint32,
	ownerGID uint32,
) error {
	if err := parseAgentStandaloneTemporarySuffix(name, "domain.json.next-", false); err != nil {
		return err
	}
	if err := validateAgentStandaloneTemporary(directory, name, ownerUID, ownerGID, agentAuthorityDomainMaxSize); err != nil {
		return err
	}
	if err := agentStandaloneDurableUnlinkat(int(directory.Fd()), name, 0); err != nil {
		return err
	}
	return unix.Fsync(int(directory.Fd()))
}

func cleanupAgentStandaloneProbeTemporary(
	directory *os.File,
	name string,
	ownerUID uint32,
	ownerGID uint32,
) error {
	if err := parseAgentStandaloneTemporarySuffix(name, ".authority-probe-", true); err != nil {
		return err
	}
	file, err := openAgentStandaloneNamedLock(directory, name, false, ownerUID, ownerGID)
	if err != nil {
		return err
	}
	defer file.Close()
	if err = agentStandaloneDurableFlock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return fmt.Errorf("%w: %q", errAgentStandaloneProbeLive, name)
		}
		return err
	}
	if err = agentStandaloneDurableUnlinkat(int(directory.Fd()), name, 0); err != nil {
		return err
	}
	return unix.Fsync(int(directory.Fd()))
}

func cleanupAgentStandaloneMarkerTemporary(
	directory *os.File,
	uid uint32,
	name string,
	ownerUID uint32,
	ownerGID uint32,
) error {
	uidLock, acquired, err := tryAgentStandaloneNamedLock(
		directory, strconv.FormatUint(uint64(uid), 10)+".lock", false, ownerUID, ownerGID,
	)
	if err != nil {
		return err
	}
	if !acquired {
		return fmt.Errorf("%w: %q", errAgentStandaloneMarkerTempBusy, name)
	}
	defer uidLock.Close()
	if err = validateAgentStandaloneTemporary(directory, name, ownerUID, ownerGID, agentStandaloneMarkerMax); err != nil {
		return err
	}
	if err = agentStandaloneDurableUnlinkat(int(directory.Fd()), name, 0); err != nil {
		return err
	}
	return unix.Fsync(int(directory.Fd()))
}

func cleanupAgentStandaloneTargetMarkerTemporaries(
	directory *os.File,
	uid uint32,
	heldUIDLock *os.File,
	ownerUID uint32,
	ownerGID uint32,
	deadline time.Time,
	canceled <-chan struct{},
	signals <-chan os.Signal,
) error {
	if heldUIDLock == nil {
		return errors.New("target marker temporary cleanup requires its held UID lock")
	}
	lockName := strconv.FormatUint(uint64(uid), 10) + ".lock"
	var descriptor, named unix.Stat_t
	if err := unix.Fstat(int(heldUIDLock.Fd()), &descriptor); err != nil {
		return err
	}
	if err := unix.Fstatat(int(directory.Fd()), lockName, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil ||
		descriptor.Dev != named.Dev || descriptor.Ino != named.Ino {
		return errors.Join(errors.New("held target UID lock is not its permanent named inode"), err)
	}
	entries, err := agentStandaloneAuthorityEntries(directory)
	if err != nil {
		return err
	}
	prefix := strconv.FormatUint(uint64(uid), 10) + ".quarantine.next-"
	cleaned := false
	for _, entry := range entries {
		if err = checkAgentStandaloneAcquisition(deadline, canceled, signals); err != nil {
			return err
		}
		if !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		parsedUID, parseErr := parseAgentStandaloneMarkerTemporary(entry.Name())
		if parseErr != nil || parsedUID != uid {
			return errors.Join(fmt.Errorf("target marker temporary %q is invalid", entry.Name()), parseErr)
		}
		if err = validateAgentStandaloneTemporary(
			directory, entry.Name(), ownerUID, ownerGID, agentStandaloneMarkerMax,
		); err != nil {
			return err
		}
		if err = agentStandaloneDurableUnlinkat(int(directory.Fd()), entry.Name(), 0); err != nil {
			return err
		}
		cleaned = true
	}
	if cleaned {
		return unix.Fsync(int(directory.Fd()))
	}
	return nil
}

func replaceAgentStandaloneDomainRecord(directory *os.File, ownerUID, ownerGID uint32, record agentAuthorityDomainRecord) (replaceErr error) {
	temporary := "domain.json.next-" + agentStandaloneRandomHex(12)
	payload, _ := json.Marshal(record)
	fd, err := unix.Openat(int(directory.Fd()), temporary, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), temporary)
	temporaryOpen := true
	defer func() {
		if temporaryOpen {
			replaceErr = errors.Join(replaceErr, file.Close())
		}
		if unlinkErr := agentStandaloneDurableUnlinkat(int(directory.Fd()), temporary, 0); unlinkErr != nil && !errors.Is(unlinkErr, unix.ENOENT) {
			replaceErr = errors.Join(replaceErr, unlinkErr)
		}
	}()
	if err = agentStandaloneDurableFchown(fd, int(ownerUID), int(ownerGID)); err != nil {
		return err
	}
	if err = agentStandaloneDurableFchmod(fd, 0o600); err != nil {
		return err
	}
	if _, err = agentStandaloneDurableWrite(file, append(payload, '\n')); err != nil {
		return err
	}
	if err = agentStandaloneDurableSync(file); err != nil {
		return err
	}
	var descriptor unix.Stat_t
	if err = agentStandaloneDurableFstat(fd, &descriptor); err != nil {
		return err
	}
	err = agentStandaloneCloseTemporary(file)
	temporaryOpen = false
	if err != nil {
		return fmt.Errorf("close agent authority record temporary before publication: %w", err)
	}
	if err = unix.Renameat(int(directory.Fd()), temporary, int(directory.Fd()), "domain.json"); err != nil {
		return err
	}
	published, err := loadAgentAuthorityDomainRecord(directory, ownerUID, ownerGID)
	if err != nil {
		return err
	}
	publishedPayload, err := json.Marshal(published)
	if err != nil || !bytes.Equal(publishedPayload, payload) {
		return errors.Join(errors.New("published agent authority record payload changed"), err)
	}
	var named unix.Stat_t
	if err = agentStandaloneDurableFstatat(int(directory.Fd()), "domain.json", &named, unix.AT_SYMLINK_NOFOLLOW); err != nil ||
		descriptor.Dev != named.Dev || descriptor.Ino != named.Ino || named.Mode&unix.S_IFMT != unix.S_IFREG ||
		named.Uid != ownerUID || named.Gid != ownerGID || named.Nlink != 1 || named.Mode&0o777 != 0o600 {
		return errors.Join(errors.New("published agent authority record is not the temporary inode"), err)
	}

	return unix.Fsync(int(directory.Fd()))
}

func claimAgentStandaloneOwner(
	directory *os.File,
	want agentStandaloneOwner,
	ownerUID uint32,
	ownerGID uint32,
	wasPresent bool,
	deadline time.Time,
	canceled <-chan struct{},
	signals <-chan os.Signal,
) error {
	existing, err := loadAgentStandaloneOwner(directory, want.UID, ownerUID, ownerGID)
	if err == nil {
		if existing != want {
			return fmt.Errorf("agent identity uid %d is permanently bound to another standalone owner", want.UID)
		}
		if err = validateAgentStandaloneOwnerUniqueness(
			directory, want, ownerUID, ownerGID, deadline, canceled, signals,
		); err != nil {
			return err
		}
		return validateAgentStandalonePriorDisposition(directory, want, ownerUID, ownerGID)
	}
	if !errors.Is(err, unix.ENOENT) {
		return err
	}
	if wasPresent {
		return errors.New("standalone owner disappeared while its immutable binding was locked")
	}
	if _, markerErr := loadAgentStandaloneMarker(directory, want.UID, ownerUID, ownerGID); markerErr == nil {
		return fmt.Errorf("ownerless agent identity uid %d has prior disposition state", want.UID)
	} else if !errors.Is(markerErr, unix.ENOENT) {
		return markerErr
	}
	if err = validateAgentStandaloneOwnerUniqueness(
		directory, want, ownerUID, ownerGID, deadline, canceled, signals,
	); err != nil {
		return err
	}
	if err = agentStandaloneVacancyScan(want.UID, want.GID, deadline, canceled, signals); err != nil {
		return err
	}
	if err = checkAgentStandaloneAcquisition(deadline, canceled, signals); err != nil {
		return err
	}
	return createAgentStandaloneOwner(directory, want, ownerUID, ownerGID)
}

func validateAgentStandaloneOwnerUniqueness(
	directory *os.File,
	want agentStandaloneOwner,
	ownerUID uint32,
	ownerGID uint32,
	deadline time.Time,
	canceled <-chan struct{},
	signals <-chan os.Signal,
) error {
	duplicate, err := unix.Openat(
		int(directory.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0,
	)
	if err != nil {
		return err
	}
	reader := os.NewFile(uintptr(duplicate), "standalone-owner-uniqueness")
	entries, readErr := reader.ReadDir(-1)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil {
		return errors.Join(readErr, closeErr)
	}
	for _, entry := range entries {
		if err = checkAgentStandaloneAcquisition(deadline, canceled, signals); err != nil {
			return err
		}
		if strings.Contains(entry.Name(), ".owner.next-") {
			if _, parseErr := parseAgentStandaloneOwnerTemporary(entry.Name()); parseErr != nil {
				return parseErr
			}
			return fmt.Errorf("%w: %q", errAgentStandaloneOwnerTemporary, entry.Name())
		}
		if strings.Contains(entry.Name(), ".quarantine.next-") {
			temporaryUID, parseErr := parseAgentStandaloneMarkerTemporary(entry.Name())
			if parseErr != nil {
				return parseErr
			}
			if err = validateAgentStandaloneTemporary(
				directory, entry.Name(), ownerUID, ownerGID, agentStandaloneMarkerMax,
			); err != nil {
				return err
			}
			if temporaryUID == want.UID {
				return fmt.Errorf("target uid %d marker temporary appeared after held-lock cleanup", want.UID)
			}
			continue
		}
		uidText, ok := strings.CutSuffix(entry.Name(), ".owner")
		if ok {
			uid, parseErr := parseAgentStandaloneUID(uidText)
			if parseErr != nil {
				return fmt.Errorf("invalid standalone owner name %q", entry.Name())
			}
			owner, loadErr := loadAgentStandaloneOwner(directory, uid, ownerUID, ownerGID)
			if loadErr != nil {
				return loadErr
			}
			if owner.UID == want.UID {
				if owner != want {
					return fmt.Errorf("standalone uid %d is already bound to another tuple", want.UID)
				}
				continue
			}
			if owner.GID == want.GID {
				return fmt.Errorf("standalone gid %d is already bound to uid %d", want.GID, owner.UID)
			}
			if owner.Provider == want.Provider && owner.OwnerID == want.OwnerID {
				return fmt.Errorf("standalone provider owner %q is already bound to uid %d", want.OwnerID, owner.UID)
			}
			if owner.StateRoot.Path == want.StateRoot.Path ||
				(owner.StateRoot.Dev == want.StateRoot.Dev && owner.StateRoot.Ino == want.StateRoot.Ino) {
				return fmt.Errorf("standalone state root is already bound to uid %d", owner.UID)
			}
			continue
		}
		uidText, ok = strings.CutSuffix(entry.Name(), ".quarantine")
		if ok {
			uid, parseErr := parseAgentStandaloneUID(uidText)
			if parseErr != nil {
				return fmt.Errorf("invalid agent identity marker name %q", entry.Name())
			}
			marker, loadErr := loadAgentStandaloneMarker(directory, uid, ownerUID, ownerGID)
			if loadErr != nil {
				return loadErr
			}
			if uid != want.UID && marker.GID == want.GID {
				return fmt.Errorf("standalone gid %d is reserved by durable uid %d marker", want.GID, uid)
			}
		}
	}

	return nil
}

func validateAgentStandalonePriorDisposition(directory *os.File, owner agentStandaloneOwner, ownerUID, ownerGID uint32) error {
	err := validateAgentStandaloneRetainedActiveDisposition(directory, owner, ownerUID, ownerGID)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}

	return err
}

func validateAgentStandaloneRetainedActiveDisposition(
	directory *os.File,
	owner agentStandaloneOwner,
	ownerUID uint32,
	ownerGID uint32,
) error {
	marker, err := loadAgentStandaloneMarker(directory, owner.UID, ownerUID, ownerGID)
	if err != nil {
		return err
	}
	sessionKey := agentStandaloneSessionKey(owner)
	if marker.State != "active" || marker.GID != owner.GID || marker.SessionKey != sessionKey || len(marker.Paths) != 0 {
		return errors.New("standalone owner has an incompatible retained ACTIVE marker")
	}

	return nil
}

func createAgentStandaloneOwner(directory *os.File, owner agentStandaloneOwner, ownerUID, ownerGID uint32) (createErr error) {
	name := strconv.FormatUint(uint64(owner.UID), 10) + ".owner"
	temporary := name + ".next-" + agentStandaloneRandomHex(12)
	payload, _ := json.Marshal(owner)
	payload = append(payload, '\n')
	fd, err := unix.Openat(int(directory.Fd()), temporary, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), temporary)
	temporaryOpen := true
	defer func() {
		if temporaryOpen {
			createErr = errors.Join(createErr, file.Close())
		}
		if unlinkErr := agentStandaloneDurableUnlinkat(int(directory.Fd()), temporary, 0); unlinkErr != nil && !errors.Is(unlinkErr, unix.ENOENT) {
			createErr = errors.Join(createErr, unlinkErr)
		}
	}()
	if err = agentStandaloneDurableFchown(fd, int(ownerUID), int(ownerGID)); err != nil {
		return err
	}
	if err = agentStandaloneDurableFchmod(fd, 0o600); err != nil {
		return err
	}
	if _, err = agentStandaloneDurableWrite(file, payload); err != nil {
		return err
	}
	if err = agentStandaloneDurableSync(file); err != nil {
		return err
	}
	var descriptor unix.Stat_t
	if err = agentStandaloneDurableFstat(fd, &descriptor); err != nil {
		return err
	}
	err = agentStandaloneCloseTemporary(file)
	temporaryOpen = false
	if err != nil {
		return fmt.Errorf("close standalone owner temporary before publication: %w", err)
	}
	if err = unix.Renameat2(int(directory.Fd()), temporary, int(directory.Fd()), name, unix.RENAME_NOREPLACE); err != nil {
		return fmt.Errorf("publish immutable standalone owner without replacement: %w", err)
	}
	published, err := loadAgentStandaloneOwner(directory, owner.UID, ownerUID, ownerGID)
	if err != nil || published != owner {
		return errors.Join(errors.New("published standalone owner payload changed"), err)
	}
	var named unix.Stat_t
	if err = agentStandaloneDurableFstatat(int(directory.Fd()), name, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil ||
		descriptor.Dev != named.Dev || descriptor.Ino != named.Ino || descriptor.Nlink != 1 || descriptor.Mode&0o777 != 0o600 {
		return errors.Join(errors.New("published standalone owner is not its trusted named inode"), err)
	}

	return unix.Fsync(int(directory.Fd()))
}

func loadAgentStandaloneOwner(directory *os.File, uid, ownerUID, ownerGID uint32) (agentStandaloneOwner, error) {
	name := strconv.FormatUint(uint64(uid), 10) + ".owner"
	payload, err := readAgentStandaloneFile(directory, name, ownerUID, ownerGID, agentStandaloneOwnerMax)
	if err != nil {
		return agentStandaloneOwner{}, err
	}
	if err = rejectAgentAuthorityDuplicateJSONKeys(payload); err != nil {
		return agentStandaloneOwner{}, err
	}
	fields, err := exactAgentAuthorityFields(payload, "version", "uid", "gid", "kind", "provider", "ownerId", "stateRoot")
	if err != nil {
		return agentStandaloneOwner{}, err
	}
	if _, err = exactAgentAuthorityFields(fields["stateRoot"], "path", "dev", "ino"); err != nil {
		return agentStandaloneOwner{}, fmt.Errorf("invalid standalone state root: %w", err)
	}
	var owner agentStandaloneOwner
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&owner); err != nil {
		return agentStandaloneOwner{}, err
	}
	if canonical, _ := json.Marshal(owner); !bytes.Equal(payload, append(canonical, '\n')) {
		return agentStandaloneOwner{}, errors.New("standalone owner is not canonical compact JSON with one newline")
	}
	if owner.Version != 1 || owner.UID != uid || owner.UID == 0 || owner.GID == 0 ||
		owner.Kind != agentStandaloneOwnerKind || !knownAgentStandaloneProvider(owner.Provider) ||
		!validStandaloneOwnerID(owner.OwnerID) || owner.StateRoot.Dev == 0 || owner.StateRoot.Ino == 0 ||
		!validAgentStandaloneStateRootPath(owner.StateRoot.Path) {
		return agentStandaloneOwner{}, errors.New("standalone owner record is invalid")
	}

	return owner, nil
}

func publishAgentStandaloneActive(
	directory *os.File,
	uid, gid, ownerUID, ownerGID uint32,
	key string,
	deadline time.Time,
	canceled <-chan struct{},
	signals <-chan os.Signal,
) error {
	marker := agentStandaloneMarker{
		Version: 2, UID: uid, GID: gid, SessionKey: key, State: "active",
		LeaseID: agentStandaloneRandomHex(16), Paths: make([]agentStandaloneManifestPath, 0),
	}
	payload, _ := json.Marshal(marker)
	return replaceAgentStandaloneFile(
		directory, strconv.FormatUint(uint64(uid), 10)+".quarantine", payload,
		ownerUID, ownerGID, deadline, canceled, signals,
	)
}

func loadAgentStandaloneMarker(directory *os.File, uid, ownerUID, ownerGID uint32) (agentStandaloneMarker, error) {
	name := strconv.FormatUint(uint64(uid), 10) + ".quarantine"
	payload, err := readAgentStandaloneFile(directory, name, ownerUID, ownerGID, agentStandaloneMarkerMax)
	if err != nil {
		return agentStandaloneMarker{}, err
	}
	if !utf8.Valid(payload) {
		return agentStandaloneMarker{}, errors.New("agent identity marker is not UTF-8")
	}
	if err = rejectAgentAuthorityDuplicateJSONKeys(payload); err != nil {
		return agentStandaloneMarker{}, err
	}
	var raw map[string]json.RawMessage
	if err = json.Unmarshal(payload, &raw); err != nil {
		return agentStandaloneMarker{}, err
	}
	var marker agentStandaloneMarker
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&marker); err != nil {
		return agentStandaloneMarker{}, err
	}
	if marker.Version != 2 || marker.UID != uid || marker.UID == 0 || marker.GID == 0 ||
		!validAgentStandaloneSessionKey(marker.SessionKey) {
		return agentStandaloneMarker{}, errors.New("agent identity marker is incomplete")
	}
	switch marker.State {
	case "active":
		if len(raw) != 7 || raw["leaseId"] == nil || raw["paths"] == nil || len(marker.LeaseID) != 32 || marker.Paths == nil {
			return agentStandaloneMarker{}, errors.New("ACTIVE marker lacks exact v2 fields")
		}
		if _, err = hex.DecodeString(marker.LeaseID); err != nil || marker.LeaseID != strings.ToLower(marker.LeaseID) {
			return agentStandaloneMarker{}, errors.New("ACTIVE marker lease id is invalid")
		}
	case "clean-ready":
		if len(raw) != 5 || raw["leaseId"] != nil || raw["paths"] != nil {
			return agentStandaloneMarker{}, errors.New("CLEAN marker has forbidden fields")
		}
	default:
		return agentStandaloneMarker{}, errors.New("agent identity marker state is invalid")
	}
	if len(marker.Paths) > 128 {
		return agentStandaloneMarker{}, errors.New("agent identity marker has too many paths")
	}
	// raw["paths"] is the very array marker.Paths was decoded from, so this
	// re-decode agrees element for element by construction. A CLEAN marker is
	// already proven to carry no "paths" member at all.
	var rawPaths []json.RawMessage
	_ = json.Unmarshal(raw["paths"], &rawPaths)

	seenPaths := make(map[string]string, len(marker.Paths))
	for index, path := range marker.Paths {
		if _, fieldsErr := exactAgentAuthorityFields(
			rawPaths[index], "base", "segments", "action", "rootDev", "rootIno",
		); fieldsErr != nil {
			return agentStandaloneMarker{}, fmt.Errorf("invalid marker path %d schema: %w", index, fieldsErr)
		}
		if err = validateAgentStandaloneManifestPath(path); err != nil {
			return agentStandaloneMarker{}, fmt.Errorf("invalid marker path %d: %w", index, err)
		}
		identity := filepath.Join(append([]string{path.Base}, path.Segments...)...)
		if prior, duplicate := seenPaths[identity]; duplicate {
			return agentStandaloneMarker{}, fmt.Errorf("marker path %q is duplicated with actions %q and %q", identity, prior, path.Action)
		}
		for priorPath, priorAction := range seenPaths {
			overlaps := strings.HasPrefix(identity, priorPath+string(filepath.Separator)) ||
				strings.HasPrefix(priorPath, identity+string(filepath.Separator))
			if overlaps && (path.Action == "remove-path" || priorAction == "remove-path") {
				return agentStandaloneMarker{}, fmt.Errorf("marker removal path %q conflicts with %q", identity, priorPath)
			}
		}
		seenPaths[identity] = path.Action
	}

	return marker, nil
}

func validAgentStandaloneSessionKey(key string) bool {
	if key == "" || len(key) > 1024 || !utf8.ValidString(key) || strings.TrimSpace(key) != key {
		return false
	}
	for _, character := range key {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func validateAgentStandaloneManifestPath(path agentStandaloneManifestPath) error {
	if !validAgentStandaloneStateRootPath(path.Base) && path.Base != "/" {
		return errors.New("manifest base is not a clean absolute path")
	}
	if len(path.Segments) == 0 || path.RootDev == 0 || path.RootIno == 0 {
		return errors.New("manifest path is incomplete")
	}
	for _, segment := range path.Segments {
		if segment == "" || segment == "." || segment == ".." || filepath.Base(segment) != segment ||
			!utf8.ValidString(segment) {
			return errors.New("manifest path segment is invalid")
		}
		for _, character := range segment {
			if unicode.IsControl(character) {
				return errors.New("manifest path segment contains a control character")
			}
		}
	}
	switch path.Action {
	case "revoke-path", "revoke-tree", "remove-path":
		return nil
	default:
		return errors.New("manifest path action is invalid")
	}
}

func readAgentStandaloneFile(directory *os.File, name string, ownerUID, ownerGID uint32, max int64) ([]byte, error) {
	fd, err := unix.Openat(int(directory.Fd()), name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), name)
	defer file.Close()
	var descriptor, named unix.Stat_t
	if err = agentStandaloneDurableFstat(fd, &descriptor); err != nil {
		return nil, err
	}
	if err = agentStandaloneDurableFstatat(int(directory.Fd()), name, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil ||
		descriptor.Dev != named.Dev || descriptor.Ino != named.Ino || descriptor.Mode&unix.S_IFMT != unix.S_IFREG ||
		descriptor.Uid != ownerUID || descriptor.Gid != ownerGID || descriptor.Nlink != 1 ||
		descriptor.Mode&0o777 != 0o600 || descriptor.Size <= 0 || descriptor.Size > max {
		return nil, errors.Join(fmt.Errorf("%s is not its trusted bounded named inode", name), err)
	}
	payload, err := io.ReadAll(io.LimitReader(file, max+1))
	if err != nil {
		return nil, err
	}
	var after unix.Stat_t
	if err = agentStandaloneDurableFstatat(int(directory.Fd()), name, &after, unix.AT_SYMLINK_NOFOLLOW); err != nil ||
		descriptor.Dev != after.Dev || descriptor.Ino != after.Ino || descriptor.Nlink != after.Nlink ||
		descriptor.Mode != after.Mode || descriptor.Uid != after.Uid || descriptor.Gid != after.Gid {
		return nil, errors.Join(fmt.Errorf("%s changed while its payload was read", name), err)
	}

	return payload, nil
}

func replaceAgentStandaloneFile(
	directory *os.File,
	name string,
	payload []byte,
	ownerUID uint32,
	ownerGID uint32,
	deadline time.Time,
	canceled <-chan struct{},
	signals <-chan os.Signal,
) (replaceErr error) {
	temporary := name + ".next-" + agentStandaloneRandomHex(12)
	fd, err := unix.Openat(int(directory.Fd()), temporary, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), temporary)
	temporaryOpen := true
	defer func() {
		if temporaryOpen {
			replaceErr = errors.Join(replaceErr, file.Close())
		}
		if unlinkErr := agentStandaloneDurableUnlinkat(int(directory.Fd()), temporary, 0); unlinkErr != nil && !errors.Is(unlinkErr, unix.ENOENT) {
			replaceErr = errors.Join(replaceErr, unlinkErr)
		}
	}()
	if err = agentStandaloneDurableFchown(fd, int(ownerUID), int(ownerGID)); err != nil {
		return err
	}
	if err = agentStandaloneDurableFchmod(fd, 0o600); err != nil {
		return err
	}
	if _, err = agentStandaloneDurableWrite(file, append(payload, '\n')); err != nil {
		return err
	}
	if err = agentStandaloneDurableSync(file); err != nil {
		return err
	}
	var descriptor unix.Stat_t
	if err = agentStandaloneDurableFstat(fd, &descriptor); err != nil {
		return err
	}
	err = agentStandaloneCloseTemporary(file)
	temporaryOpen = false
	if err != nil {
		return fmt.Errorf("close agent identity marker temporary before publication: %w", err)
	}
	if err = checkAgentStandaloneAcquisition(deadline, canceled, signals); err != nil {
		return err
	}
	if err = unix.Renameat(int(directory.Fd()), temporary, int(directory.Fd()), name); err != nil {
		return err
	}
	published, err := readAgentStandaloneFile(directory, name, ownerUID, ownerGID, agentStandaloneMarkerMax)
	if err != nil || !bytes.Equal(published, append(append([]byte(nil), payload...), '\n')) {
		return errors.Join(errors.New("published agent identity marker payload changed"), err)
	}
	var named unix.Stat_t
	if err = agentStandaloneDurableFstatat(int(directory.Fd()), name, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil ||
		descriptor.Dev != named.Dev || descriptor.Ino != named.Ino || named.Mode&unix.S_IFMT != unix.S_IFREG ||
		named.Uid != ownerUID || named.Gid != ownerGID || named.Nlink != 1 || named.Mode&0o777 != 0o600 {
		return errors.Join(errors.New("published agent identity marker is not the temporary inode"), err)
	}

	return unix.Fsync(int(directory.Fd()))
}

func parseAgentStandaloneUID(text string) (uint32, error) {
	value, err := strconv.ParseUint(text, 10, 32)
	if err != nil || value == 0 || strconv.FormatUint(value, 10) != text {
		return 0, errors.New("invalid uid")
	}

	return uint32(value), nil
}

func proveAgentStandaloneIdentityVacant(
	uid uint32,
	gid uint32,
	deadline time.Time,
	canceled <-chan struct{},
	signals <-chan os.Signal,
) error {
	if err := checkAgentStandaloneAcquisition(deadline, canceled, signals); err != nil {
		return err
	}
	entries, err := agentStandaloneReadDir("/proc")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err = checkAgentStandaloneAcquisition(deadline, canceled, signals); err != nil {
			return err
		}
		pid, parseErr := strconv.Atoi(entry.Name())
		if parseErr != nil || pid <= 0 {
			continue
		}
		if err = proveAgentStandaloneProcessTasksVacant(pid, uid, gid, deadline, canceled, signals); err != nil {
			return err
		}
	}

	return nil
}

func proveAgentStandaloneProcessTasksVacant(
	pid int,
	uid uint32,
	gid uint32,
	deadline time.Time,
	canceled <-chan struct{},
	signals <-chan os.Signal,
) error {
	const maxTaskSetAttempts = 64
	taskRoot := fmt.Sprintf("/proc/%d/task", pid)
	for attempt := 0; attempt < maxTaskSetAttempts; attempt++ {
		if err := checkAgentStandaloneAcquisition(deadline, canceled, signals); err != nil {
			return err
		}
		before, err := agentStandaloneReadDir(taskRoot)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("inspect process %d tasks: %w", pid, err)
		}
		unstable := false
		for _, task := range before {
			if err = checkAgentStandaloneAcquisition(deadline, canceled, signals); err != nil {
				return err
			}
			if _, parseErr := strconv.ParseUint(task.Name(), 10, 32); parseErr != nil {
				return fmt.Errorf("process %d task directory contains invalid entry %q", pid, task.Name())
			}
			statusPath := filepath.Join(taskRoot, task.Name(), "status")
			payload, statusErr := agentStandaloneReadFile(statusPath)
			if errors.Is(statusErr, os.ErrNotExist) {
				unstable = true
				break
			}
			if statusErr != nil {
				return fmt.Errorf("inspect task credentials %s: %w", statusPath, statusErr)
			}
			matched, parseErr := agentStandaloneStatusMatches(payload, uid, gid)
			if parseErr != nil {
				return fmt.Errorf("parse task credentials %s: %w", statusPath, parseErr)
			}
			if matched {
				return fmt.Errorf("agent identity %d:%d is still used by task %d/%s", uid, gid, pid, task.Name())
			}
		}
		if unstable {
			continue
		}
		after, err := agentStandaloneReadDir(taskRoot)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("reinspect process %d tasks: %w", pid, err)
		}
		if agentStandaloneTaskSetsEqual(before, after) {
			return nil
		}
	}
	return fmt.Errorf("process %d task set did not stabilize within %d attempts", pid, maxTaskSetAttempts)
}

func agentStandaloneTaskSetsEqual(left, right []os.DirEntry) bool {
	if len(left) != len(right) {
		return false
	}
	leftNames := make(map[string]struct{}, len(left))
	for _, entry := range left {
		leftNames[entry.Name()] = struct{}{}
	}
	if len(leftNames) != len(left) {
		return false
	}
	for _, entry := range right {
		if _, present := leftNames[entry.Name()]; !present {
			return false
		}
		delete(leftNames, entry.Name())
	}
	return len(leftNames) == 0
}

func proveAgentStandaloneIdentityVacantTwice(
	uid uint32,
	gid uint32,
	deadline time.Time,
	canceled <-chan struct{},
	signals <-chan os.Signal,
) error {
	if err := agentStandaloneVacancyScan(uid, gid, deadline, canceled, signals); err != nil {
		return err
	}
	if err := agentStandaloneVacancyScan(uid, gid, deadline, canceled, signals); err != nil {
		return fmt.Errorf("second standalone task vacancy proof: %w", err)
	}

	return nil
}

func agentStandaloneStatusMatches(payload []byte, uid, gid uint32) (bool, error) {
	var uidValues, gidValues, groups []uint32
	uidFields := 0
	gidFields := 0
	groupFields := 0
	for _, line := range strings.Split(string(payload), "\n") {
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		fields := strings.Fields(value)
		var target *[]uint32
		switch name {
		case "Uid":
			uidFields++
			target = &uidValues
		case "Gid":
			gidFields++
			target = &gidValues
		case "Groups":
			groupFields++
			target = &groups
		default:
			continue
		}
		for _, field := range fields {
			parsed, err := strconv.ParseUint(field, 10, 32)
			if err != nil {
				return false, err
			}
			*target = append(*target, uint32(parsed))
		}
	}
	if uidFields != 1 || gidFields != 1 || groupFields != 1 || len(uidValues) != 4 || len(gidValues) != 4 {
		return false, errors.New("task status lacks exactly one Uid, Gid, or Groups credential field")
	}

	return slices.Contains(uidValues, uid) || slices.Contains(gidValues, gid) || slices.Contains(groups, gid), nil
}
