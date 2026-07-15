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

type providerProcessTracker struct {
	mu      sync.Mutex
	hooks   RuntimeResourceHooks
	nextID  uint64
	entries map[uint64]providerProcessEntry
}

type providerProcessEntry struct {
	count int
	known bool
}

type providerProcessRoot struct {
	tracker *providerProcessTracker
	id      uint64
}

func newProviderProcessTracker(hooks RuntimeResourceHooks) *providerProcessTracker {
	return &providerProcessTracker{
		hooks:   hooks,
		entries: make(map[uint64]providerProcessEntry),
	}
}

func (t *providerProcessTracker) register() *providerProcessRoot {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.nextID++
	t.entries[t.nextID] = providerProcessEntry{}

	return &providerProcessRoot{tracker: t, id: t.nextID}
}

func (r *providerProcessRoot) observe(ctx context.Context, process any) {
	inventory, ok := process.(providerProcessInventory)
	if !ok {
		return
	}

	count, available := inventory.ProviderDescendantCount()
	if !available || count < 0 {
		return
	}

	r.tracker.update(ctx, r.id, providerProcessEntry{count: count, known: true})
}

func (r *providerProcessRoot) retire(ctx context.Context, proven bool) {
	if !proven {
		return
	}

	r.tracker.remove(ctx, r.id)
}

func (t *providerProcessTracker) update(ctx context.Context, id uint64, entry providerProcessEntry) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.entries[id]; !ok {
		return
	}

	t.entries[id] = entry
	t.publishLocked(ctx)
}

func (t *providerProcessTracker) remove(ctx context.Context, id uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.entries[id]; !ok {
		return
	}

	delete(t.entries, id)
	t.publishLocked(ctx)
}

func (t *providerProcessTracker) publishLocked(ctx context.Context) {
	total := 0

	for _, entry := range t.entries {
		if !entry.known {
			return
		}

		total += entry.count
	}

	observeRuntimeProcessSnapshot(ctx, t.hooks, RuntimeProcessProviderDescendant, total)
}

func providerProcessTreeProven(err error) bool {
	return !errors.Is(err, nativehermes.ErrProcessTreeUnproven)
}
