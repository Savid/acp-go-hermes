package hermes

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
)

const sharedHomeOwnerBaseName = "home-root"

// SharedHomeOwner is the exclusive claim on one durable HERMES_HOME root. It is
// held for the whole lifetime of every native writer this adapter launches
// against that root, not for the duration of an operation, so a second adapter
// is refused rather than admitted alongside. The claim records this adapter's
// own PID and kernel start time, and its lock descriptor is inherited by each
// contained Hermes process, so an adapter crash leaves the root fenced by the
// surviving native writer instead of releasing it.
type SharedHomeOwner struct {
	claim   *SharedSessionOwner
	control string
	refs    int
}

// sharedHomeOwners is the process-wide registry of live home-root claims. One
// adapter process may run many native writers against one root, and flock
// conflicts between separate open descriptions even inside a single process, so
// the claim is taken once per root and reference counted from there.
var sharedHomeOwners = struct {
	sync.Mutex
	owners map[string]*SharedHomeOwner
}{owners: make(map[string]*SharedHomeOwner)}

var sharedHomeOwnerIdentity = CurrentDurableProcessIdentity

// AcquireSharedHomeOwner claims one durable HERMES_HOME root for this adapter
// process. A concurrent adapter holding the same root is refused with a closed
// error, and a predecessor's abandoned claim is honoured until its recorded
// process is proven dead by PID and kernel start time.
func AcquireSharedHomeOwner(home string) (*SharedHomeOwner, error) {
	control, err := EnsureSharedHermesAdapterControlDir(home)
	if err != nil {
		return nil, err
	}

	sharedHomeOwners.Lock()
	defer sharedHomeOwners.Unlock()

	if owner, live := sharedHomeOwners.owners[control]; live {
		owner.refs++

		return owner, nil
	}

	claim, err := acquireSharedOwnerClaim(filepath.Join(control, sharedHomeOwnerBaseName), sharedHomeOwnerKind)
	if err != nil {
		return nil, err
	}

	identity, err := sharedHomeOwnerIdentity()
	if err != nil {
		return nil, errors.Join(err, claim.Release())
	}

	if err := claim.BindProcessIdentity(identity.PID, identity.KernelStartTime); err != nil {
		return nil, errors.Join(err, claim.Release())
	}

	owner := &SharedHomeOwner{claim: claim, control: control, refs: 1}
	sharedHomeOwners.owners[control] = owner

	return owner, nil
}

// Release drops one native writer's reference and gives the root up once the
// last of them has settled.
func (o *SharedHomeOwner) Release() error {
	if o == nil {
		return nil
	}

	sharedHomeOwners.Lock()
	defer sharedHomeOwners.Unlock()

	if o.refs == 0 {
		return nil
	}

	o.refs--
	if o.refs > 0 {
		return nil
	}

	delete(sharedHomeOwners.owners, o.control)

	return o.claim.Release()
}

// Retain deliberately holds the root until adapter process exit after an
// unproven containment result. Descendants of the failed start may still be
// writing this exact HERMES_HOME, so every remaining reference is dropped
// without releasing the claim and no later acquisition is admitted.
func (o *SharedHomeOwner) Retain() {
	if o == nil {
		return
	}

	sharedHomeOwners.Lock()
	defer sharedHomeOwners.Unlock()

	if o.refs == 0 {
		return
	}

	o.refs = 0

	delete(sharedHomeOwners.owners, o.control)
	o.claim.Retain()
}

// appendLockFile adds the home-root descriptor the native child must inherit
// atomically, so an adapter crash cannot release the exclusive claim while a
// native writer it launched is still running.
func (o *SharedHomeOwner) appendLockFile(files []*os.File) ([]*os.File, error) {
	if o == nil {
		return files, nil
	}

	claimed, err := sharedSessionOwnerFiles([]*SharedSessionOwner{o.claim})
	if err != nil {
		return nil, err
	}

	return append(files, claimed...), nil
}
