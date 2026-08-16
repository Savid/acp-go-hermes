//nolint:gosec,govet // Paths are hash-only beneath the canonical validated adapter control root.
package hermes

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	sharedSessionOwnersDir = ".acp-go-hermes-session-owners"
	// sharedOwnerClaimAttempts bounds the reopen loop that runs when a
	// predecessor unlinks its lock file in the window between this caller's
	// open and its own successful flock. Locking that detached inode would
	// fence nothing, so the attempt is retired and retried against the path.
	sharedOwnerClaimAttempts = 3
)

// errSharedOwnerLockReplaced reports that the locked inode is no longer the one
// the lock path names. It never reaches a caller: acquisition retries.
var errSharedOwnerLockReplaced = errors.New("shared Hermes owner lock was replaced during acquisition")

// sharedOwnerClaimKind names one exclusion unit in adapter-facing text. label
// spells the artifact pair; active and live are the refusals a second claimant
// sees when the kernel lock is held and when the recorded claimant is proven
// live.
type sharedOwnerClaimKind struct {
	label  string
	active string
	live   string
}

var (
	sharedSessionOwnerKind = sharedOwnerClaimKind{
		label:  "session-owner",
		active: "shared Hermes session is already active",
		live:   "shared Hermes session claimant process is still live",
	}
	sharedHomeOwnerKind = sharedOwnerClaimKind{
		label:  "home-root",
		active: "shared Hermes home root is already claimed by a live writer",
		live:   "shared Hermes home-root claimant process is still live",
	}
)

type SharedSessionOwner struct {
	lockPath  string
	claimPath string
	file      *os.File
	unlock    func() error
	mu        sync.Mutex
	once      sync.Once
	err       error
}

type sharedSessionOwnerClaim struct {
	PID             int    `json:"pid"`
	KernelStartTime string `json:"kernelStartTime"`
}

var (
	sharedOwnerChmod            = os.Chmod
	sharedOwnerFileChmod        = (*os.File).Chmod
	sharedOwnerFileStat         = (*os.File).Stat
	sharedOwnerLstat            = os.Lstat
	sharedOwnerTryLock          = tryLockHermesFile
	sharedOwnerJSONMarshal      = json.Marshal
	sharedOwnerInspectStartTime = inspectHermesProcessStartTime
)

// acquireSharedSessionOwner prevents two official Hermes processes from
// resuming and mutating the same logical ACP session concurrently. The file
// name is a fixed-size hash; raw host session identifiers never become paths.
func acquireSharedSessionOwner(home string, kind string, id string) (*SharedSessionOwner, error) {
	if id == "" {
		return nil, fmt.Errorf("shared Hermes home requires a non-empty %s session id", kind)
	}

	control, err := EnsureSharedHermesAdapterControlDir(home)
	if err != nil {
		return nil, err
	}

	dir := filepath.Join(control, sharedSessionOwnersDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create shared Hermes session-owner directory: %w", err)
	}

	if err := sharedOwnerChmod(dir, 0o700); err != nil {
		return nil, fmt.Errorf("protect shared Hermes session-owner directory: %w", err)
	}

	digest := sha256.Sum256([]byte(kind + "\x00" + id))

	return acquireSharedOwnerClaim(filepath.Join(dir, hex.EncodeToString(digest[:])), sharedSessionOwnerKind)
}

// acquireSharedOwnerClaim takes the exclusive kernel lock at base+".lock" and
// admits the caller only once base+".claim" names no live process. The lock
// descriptor is inherited by the contained Hermes process/guardian, so an
// adapter crash does not admit a replacement until that exact native process
// has exited and released the kernel-held claim.
func acquireSharedOwnerClaim(base string, kind sharedOwnerClaimKind) (*SharedSessionOwner, error) {
	lockPath := base + ".lock"
	claimPath := base + ".claim"

	for range sharedOwnerClaimAttempts {
		owner, err := tryAcquireSharedOwnerClaim(lockPath, claimPath, kind)
		if !errors.Is(err, errSharedOwnerLockReplaced) {
			return owner, err
		}
	}

	return nil, fmt.Errorf("shared Hermes %s lock was replaced during every acquisition attempt", kind.label)
}

func tryAcquireSharedOwnerClaim(lockPath string, claimPath string, kind sharedOwnerClaimKind) (*SharedSessionOwner, error) {
	file, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open shared Hermes %s lock: %w", kind.label, err)
	}

	if err := sharedOwnerFileChmod(file, 0o600); err != nil {
		return nil, errors.Join(fmt.Errorf("protect shared Hermes %s lock: %w", kind.label, err), file.Close())
	}

	unlock, acquired, err := sharedOwnerTryLock(file)
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}

	if !acquired {
		return nil, errors.Join(errors.New(kind.active), file.Close())
	}

	if err := verifySharedOwnerLockPath(file, lockPath, kind); err != nil {
		return nil, errors.Join(err, unlock(), file.Close())
	}

	claim, err := readSharedOwnerClaim(claimPath, kind.label)
	if err != nil {
		return nil, errors.Join(err, unlock(), file.Close())
	}

	if claim.PID > 0 {
		gone, inspectErr := sharedSessionOwnerClaimGone(claim)
		if inspectErr != nil {
			return nil, errors.Join(inspectErr, unlock(), file.Close())
		}

		if !gone {
			return nil, errors.Join(errors.New(kind.live), unlock(), file.Close())
		}
	}

	return &SharedSessionOwner{lockPath: lockPath, claimPath: claimPath, file: file, unlock: unlock}, nil
}

// verifySharedOwnerLockPath proves the locked inode is still the one this path
// names. Release unlinks the lock file while the lock is still held, so a
// descriptor opened just before that unlink would otherwise fence a detached
// inode while a replacement claimed a brand-new one.
func verifySharedOwnerLockPath(file *os.File, lockPath string, kind sharedOwnerClaimKind) error {
	locked, err := sharedOwnerFileStat(file)
	if err != nil {
		return fmt.Errorf("inspect shared Hermes %s lock: %w", kind.label, err)
	}

	named, err := sharedOwnerLstat(lockPath)
	if err != nil || !os.SameFile(locked, named) {
		return errSharedOwnerLockReplaced
	}

	return nil
}

func readSharedOwnerClaim(path string, label string) (sharedSessionOwnerClaim, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return sharedSessionOwnerClaim{}, nil
	}

	if err != nil {
		return sharedSessionOwnerClaim{}, fmt.Errorf("open shared Hermes %s claim: %w", label, err)
	}

	defer file.Close()

	data, err := io.ReadAll(io.LimitReader(file, 4097))
	if err != nil {
		return sharedSessionOwnerClaim{}, fmt.Errorf("read shared Hermes %s claim: %w", label, err)
	}

	if len(data) == 0 {
		return sharedSessionOwnerClaim{}, fmt.Errorf("shared Hermes %s claim is empty", label)
	}

	if len(data) > 4096 {
		return sharedSessionOwnerClaim{}, fmt.Errorf("shared Hermes %s claim is oversized", label)
	}

	var claim sharedSessionOwnerClaim
	if err := json.Unmarshal(data, &claim); err != nil {
		return sharedSessionOwnerClaim{}, fmt.Errorf("parse shared Hermes %s claim: %w", label, err)
	}

	if claim.PID <= 0 || claim.KernelStartTime == "" {
		return sharedSessionOwnerClaim{}, fmt.Errorf("shared Hermes %s claim is incomplete", label)
	}

	return claim, nil
}

func sharedSessionOwnerClaimGone(claim sharedSessionOwnerClaim) (bool, error) {
	startTime, err := sharedOwnerInspectStartTime(claim.PID)
	if err != nil {
		if sharedOwnerInspectionProvesGone(err) {
			return true, nil
		}

		return false, fmt.Errorf("verify shared Hermes claimant: %w", err)
	}

	return startTime != claim.KernelStartTime, nil
}

func acquireSharedACPSessionOwner(home string, id ACPSessionIDString) (*SharedSessionOwner, error) {
	return acquireSharedSessionOwner(home, "ACP", string(id))
}

// AcquireSharedACPSessionOwner claims the adapter lifecycle process that may
// still own an uncommitted New operation after its originating adapter died.
func AcquireSharedACPSessionOwner(home string, id ACPSessionIDString) (*SharedSessionOwner, error) {
	return acquireSharedACPSessionOwner(home, id)
}

// AcquireSharedNativeSessionOwner claims one durable official Hermes session
// before it can be resumed or used by an ACP runtime.
func AcquireSharedNativeSessionOwner(home string, id string) (*SharedSessionOwner, error) {
	return acquireSharedSessionOwner(home, "native", id)
}

// BindProcess persists the exact native process claimant while the owner lock
// is held. A later adapter process refuses the session even after an adapter
// crash releases the OS lock, until this PID/start-time pair is proven dead.
func (o *SharedSessionOwner) BindProcess(pid int) error {
	if o == nil {
		return nil
	}

	startTime, err := sharedOwnerInspectStartTime(pid)
	if err != nil {
		return fmt.Errorf("inspect shared Hermes session-owner process: %w", err)
	}

	return o.BindProcessIdentity(pid, startTime)
}

// BindSharedSessionOwnerToServer binds a claim acquired after session.create
// to the already-running official Hermes process before the native ID is
// exposed or persisted by the adapter.
func BindSharedSessionOwnerToServer(owner *SharedSessionOwner, server Server) error {
	if owner == nil {
		return nil
	}

	identitySource, ok := server.(interface {
		SharedSessionOwnerProcessIdentity() (int, string, error)
	})
	if !ok {
		return errors.New("hermes server does not expose a session-owner process identity")
	}

	pid, kernelStartTime, err := identitySource.SharedSessionOwnerProcessIdentity()
	if err != nil {
		return err
	}

	return owner.BindProcessIdentity(pid, kernelStartTime)
}

// BindProcessIdentity persists an identity already inspected by the owning
// server. It is exported only so the adapter wrapper can bind a native-session
// claim to the exact process owned by an internal Server.
func (o *SharedSessionOwner) BindProcessIdentity(pid int, kernelStartTime string) error {
	if o == nil {
		return nil
	}

	if pid <= 0 || kernelStartTime == "" {
		return errors.New("shared Hermes session-owner process identity is incomplete")
	}

	data, err := sharedOwnerJSONMarshal(sharedSessionOwnerClaim{PID: pid, KernelStartTime: kernelStartTime})
	if err != nil {
		return err
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if err := atomicSharedHermesWriteFile(o.claimPath, append(data, '\n'), 0o600); err != nil {
		return fmt.Errorf("sync shared Hermes session-owner claim: %w", err)
	}

	return nil
}

// Release drops a proven-settled claim and removes both durable artifacts, so
// a long-lived residence does not accumulate one empty lock file per session
// identity it has ever hosted. The lock file is unlinked before the lock is
// dropped: a replacement then creates its own inode, and any descriptor opened
// against the abandoned one fails the acquisition path check.
func (o *SharedSessionOwner) Release() error {
	if o == nil {
		return nil
	}

	o.once.Do(func() {
		o.mu.Lock()

		clearErr := errors.Join(removeSharedOwnerArtifact(o.claimPath), removeSharedOwnerArtifact(o.lockPath))
		if clearErr == nil {
			clearErr = syncSharedHermesDirectory(filepath.Dir(o.claimPath))
		}
		o.mu.Unlock()
		o.err = errors.Join(clearErr, o.unlock(), o.file.Close())
	})

	return o.err
}

func removeSharedOwnerArtifact(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	return nil
}

func sharedSessionOwnerFiles(owners []*SharedSessionOwner) ([]*os.File, error) {
	files := make([]*os.File, 0, len(owners))
	for _, owner := range owners {
		if owner == nil {
			continue
		}

		owner.mu.Lock()
		file := owner.file
		owner.mu.Unlock()

		if file == nil {
			return nil, errors.New("shared Hermes session-owner lock is unavailable")
		}

		files = append(files, file)
	}

	return files, nil
}

var retainedSharedSessionOwners = struct {
	sync.Mutex
	owners map[*SharedSessionOwner]struct{}
}{owners: make(map[*SharedSessionOwner]struct{})}

// retainSharedSessionOwner deliberately holds an OS lock until adapter process
// exit when startup left native containment unproven. Releasing it would admit
// a second owner while descendants from the failed start may still mutate the
// same durable native session. The set is keyed by the owner itself: keying it
// by lock path would let one retention evict another, and an evicted owner is
// unreachable, so its *os.File finalizer would close the descriptor and
// silently drop the very lock this retention exists to hold.
func retainSharedSessionOwner(owner *SharedSessionOwner) {
	if owner == nil {
		return
	}

	retainedSharedSessionOwners.Lock()
	retainedSharedSessionOwners.owners[owner] = struct{}{}
	retainedSharedSessionOwners.Unlock()
}

// Retain deliberately holds the claim until adapter process exit after an
// unproven containment result.
func (o *SharedSessionOwner) Retain() {
	retainSharedSessionOwner(o)
}
