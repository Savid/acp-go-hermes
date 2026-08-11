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

const sharedSessionOwnersDir = ".acp-go-hermes-session-owners"

type SharedSessionOwner struct {
	key       string
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
	sharedOwnerChmod       = os.Chmod
	sharedOwnerFileChmod   = (*os.File).Chmod
	sharedOwnerTryLock     = tryLockHermesFile
	sharedOwnerJSONMarshal = json.Marshal
)

// acquireSharedSessionOwner prevents two official Hermes processes from
// resuming and mutating the same logical ACP session concurrently. The file
// name is a fixed-size hash; raw host session identifiers never become paths.
// The lock descriptor is inherited by the contained Hermes process/guardian,
// so an adapter crash does not admit a replacement until that exact native
// process has exited and released the kernel-held claim.
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
	base := filepath.Join(dir, hex.EncodeToString(digest[:]))
	path := base + ".lock"
	claimPath := base + ".claim"

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open shared Hermes session-owner lock: %w", err)
	}

	if err := sharedOwnerFileChmod(file, 0o600); err != nil {
		return nil, errors.Join(fmt.Errorf("protect shared Hermes session-owner lock: %w", err), file.Close())
	}

	unlock, acquired, err := sharedOwnerTryLock(file)
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}

	if !acquired {
		return nil, errors.Join(errors.New("shared Hermes session is already active"), file.Close())
	}

	claim, err := readSharedSessionOwnerClaim(claimPath)
	if err != nil {
		return nil, errors.Join(err, unlock(), file.Close())
	}

	if claim.PID > 0 {
		gone, inspectErr := sharedSessionOwnerClaimGone(claim)
		if inspectErr != nil {
			return nil, errors.Join(inspectErr, unlock(), file.Close())
		}

		if !gone {
			return nil, errors.Join(errors.New("shared Hermes session claimant process is still live"), unlock(), file.Close())
		}
	}

	return &SharedSessionOwner{key: path, claimPath: claimPath, file: file, unlock: unlock}, nil
}

func readSharedSessionOwnerClaim(path string) (sharedSessionOwnerClaim, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return sharedSessionOwnerClaim{}, nil
	}

	if err != nil {
		return sharedSessionOwnerClaim{}, fmt.Errorf("open shared Hermes session-owner claim: %w", err)
	}

	defer file.Close()

	data, err := io.ReadAll(io.LimitReader(file, 4097))
	if err != nil {
		return sharedSessionOwnerClaim{}, fmt.Errorf("read shared Hermes session-owner claim: %w", err)
	}

	if len(data) == 0 {
		return sharedSessionOwnerClaim{}, errors.New("shared Hermes session-owner claim is empty")
	}

	if len(data) > 4096 {
		return sharedSessionOwnerClaim{}, errors.New("shared Hermes session-owner claim is oversized")
	}

	var claim sharedSessionOwnerClaim
	if err := json.Unmarshal(data, &claim); err != nil {
		return sharedSessionOwnerClaim{}, fmt.Errorf("parse shared Hermes session-owner claim: %w", err)
	}

	if claim.PID <= 0 || claim.KernelStartTime == "" {
		return sharedSessionOwnerClaim{}, errors.New("shared Hermes session-owner claim is incomplete")
	}

	return claim, nil
}

func sharedSessionOwnerClaimGone(claim sharedSessionOwnerClaim) (bool, error) {
	startTime, err := inspectHermesProcessStartTime(claim.PID)
	if err != nil {
		if sharedOwnerInspectionProvesGone(err) {
			return true, nil
		}

		return false, fmt.Errorf("verify shared Hermes session-owner claimant: %w", err)
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

	startTime, err := inspectHermesProcessStartTime(pid)
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

// Release drops a proven-settled session claim.
func (o *SharedSessionOwner) Release() error {
	if o == nil {
		return nil
	}

	o.once.Do(func() {
		o.mu.Lock()

		clearErr := os.Remove(o.claimPath)
		if errors.Is(clearErr, os.ErrNotExist) {
			clearErr = nil
		}

		if clearErr == nil {
			clearErr = syncSharedHermesDirectory(filepath.Dir(o.claimPath))
		}
		o.mu.Unlock()
		o.err = errors.Join(clearErr, o.unlock(), o.file.Close())
	})

	return o.err
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
	owners map[string]*SharedSessionOwner
}{owners: make(map[string]*SharedSessionOwner)}

// retainSharedSessionOwner deliberately holds an OS lock until adapter process
// exit when startup left native containment unproven. Releasing it would admit
// a second owner while descendants from the failed start may still mutate the
// same durable native session.
func retainSharedSessionOwner(owner *SharedSessionOwner) {
	if owner == nil {
		return
	}

	retainedSharedSessionOwners.Lock()
	retainedSharedSessionOwners.owners[owner.key] = owner
	retainedSharedSessionOwners.Unlock()
}

// Retain deliberately holds the claim until adapter process exit after an
// unproven containment result.
func (o *SharedSessionOwner) Retain() {
	retainSharedSessionOwner(o)
}
