package hermesacp

import (
	"os"
	"path/filepath"
	"sync"
)

// managedHandoffRoot pins the one configured read domain before any native tree
// is prepared. All managed residences and shims are allocated under the complete
// scratch parent. The host keeps those domains disjoint, including mount aliases
// and root/ancestor replacement, for the authority's lifetime.
type managedHandoffRoot struct {
	mu     sync.RWMutex
	frozen bool
	root   *os.Root
}

func (r *managedHandoffRoot) freeze(handoff, scratch string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.frozen {
		return
	}

	r.frozen = true

	if handoff == "" {
		return
	}

	domain, err := handoffDirectoryLineage(scratch)
	if err != nil {
		return
	}

	readDomain, err := handoffDirectoryLineage(handoff)
	if err != nil {
		return
	}

	for _, relation := range []struct {
		ancestors []os.FileInfo
		directory os.FileInfo
	}{{domain, readDomain[0]}, {readDomain, domain[0]}} {
		for _, ancestor := range relation.ancestors {
			if os.SameFile(ancestor, relation.directory) {
				return
			}
		}
	}

	root, err := openHandoffRoot(handoff)
	if err != nil {
		return
	}

	info, err := root.Stat(".")
	if err != nil || !os.SameFile(info, readDomain[0]) {
		_ = root.Close()

		return
	}

	r.root = root
}

// handoffDirectoryLineage is used only before preparation. Comparing directory
// identities includes symlink aliases and native path-case behavior.
func handoffDirectoryLineage(path string) ([]os.FileInfo, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, err
	}

	current, err := filepath.Abs(resolved)
	if err != nil {
		return nil, err
	}

	var lineage []os.FileInfo

	for {
		info, statErr := os.Stat(current)
		if statErr != nil {
			return nil, statErr
		}

		lineage = append(lineage, info)

		parent := filepath.Dir(current)
		if parent == current {
			return lineage, nil
		}

		current = parent
	}
}

func (r *managedHandoffRoot) borrow() (*os.Root, func()) {
	r.mu.RLock()

	if r.root == nil {
		r.mu.RUnlock()

		return nil, nil
	}

	return r.root, r.mu.RUnlock
}

func (r *managedHandoffRoot) close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.frozen = true
	if r.root == nil {
		return nil
	}

	err := r.root.Close()
	r.root = nil

	return err
}
