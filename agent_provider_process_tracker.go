package hermesacp

import (
	"context"
	"errors"
	"sync"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

type providerProcessInventory interface {
	ProviderDescendantCount() (int, bool)
}

// providerTreeInventory is the whole-tree vacancy proof a close-fenced
// quiescence fact reads. Only a boundary that enumerates its complete descendant
// tree implements it; every weaker boundary answers that it has no observation.
type providerTreeInventory interface {
	ProviderTreeVacant() (bool, bool)
}

type providerProcessTracker struct {
	mu         sync.Mutex
	hooks      RuntimeResourceHooks
	enabled    bool
	nextID     uint64
	entries    map[uint64]providerProcessEntry
	publishing bool
	dirty      bool
}

type providerProcessEntry struct {
	inventory providerProcessInventory
}

type providerProcessRoot struct {
	tracker *providerProcessTracker
	id      uint64
}

func newProviderProcessTracker(hooks RuntimeResourceHooks, enabled bool) *providerProcessTracker {
	return &providerProcessTracker{
		hooks:   hooks,
		enabled: enabled,
		entries: make(map[uint64]providerProcessEntry),
	}
}

func (t *providerProcessTracker) register() *providerProcessRoot {
	t.mu.Lock()
	t.nextID++
	t.entries[t.nextID] = providerProcessEntry{}
	root := &providerProcessRoot{tracker: t, id: t.nextID}
	startPublisher := t.markDirtyLocked()
	t.mu.Unlock()

	if startPublisher {
		t.publish(context.Background())
	}

	return root
}

func (r *providerProcessRoot) observe(ctx context.Context, process any) {
	inventory, ok := process.(providerProcessInventory)
	if !ok {
		return
	}

	r.tracker.update(ctx, r.id, providerProcessEntry{inventory: inventory})
}

func (r *providerProcessRoot) retire(ctx context.Context, proven bool) {
	if !proven {
		return
	}

	r.tracker.remove(ctx, r.id)
}

func (t *providerProcessTracker) update(ctx context.Context, id uint64, entry providerProcessEntry) {
	t.mu.Lock()
	if _, ok := t.entries[id]; !ok {
		t.mu.Unlock()

		return
	}

	t.entries[id] = entry
	startPublisher := t.markDirtyLocked()
	t.mu.Unlock()

	if startPublisher {
		t.publish(ctx)
	}
}

func (t *providerProcessTracker) remove(ctx context.Context, id uint64) {
	t.mu.Lock()
	if _, ok := t.entries[id]; !ok {
		t.mu.Unlock()

		return
	}

	delete(t.entries, id)
	startPublisher := t.markDirtyLocked()
	t.mu.Unlock()

	if startPublisher {
		t.publish(ctx)
	}
}

func (t *providerProcessTracker) markDirtyLocked() bool {
	t.dirty = true
	if t.publishing {
		return false
	}

	t.publishing = true

	return true
}

func (t *providerProcessTracker) publish(ctx context.Context) {
	if !t.enabled {
		t.mu.Lock()
		t.dirty = false
		t.publishing = false
		t.mu.Unlock()

		return
	}

	for {
		t.mu.Lock()
		if !t.dirty {
			t.publishing = false
			t.mu.Unlock()

			return
		}

		t.dirty = false
		total, available := t.snapshotLocked()
		t.mu.Unlock()

		if available {
			observeRuntimeProcessSnapshot(ctx, t.hooks, RuntimeProcessProviderDescendant, total)
		}
	}
}

func (t *providerProcessTracker) snapshotLocked() (int, bool) {
	total := 0

	for _, entry := range t.entries {
		if entry.inventory == nil {
			return 0, false
		}

		count, available := entry.inventory.ProviderDescendantCount()
		if !available || count < 0 {
			return 0, false
		}

		total += count
	}

	return total, true
}

func providerProcessTreeProven(err error) bool {
	return !errors.Is(err, nativehermes.ErrProcessContainmentIncomplete)
}
