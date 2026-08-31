package hermes

import (
	"path/filepath"
	"sync"
)

const sharedHomeOwnerBaseName = "home-root"

// SharedHomeOwner is the exclusive process-local claim on one durable
// HERMES_HOME root.
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

// AcquireSharedHomeOwner claims one durable HERMES_HOME root for this adapter
// process. A concurrent adapter holding the same root is refused while its OS
// lock remains active.
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

	claim, err := acquireSharedOwnerLock(filepath.Join(control, sharedHomeOwnerBaseName+".lock"), sharedHomeOwnerKind)
	if err != nil {
		return nil, err
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
