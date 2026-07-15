package hermesacp

import (
	"context"
	"errors"
	"sync"
	"testing"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
	"github.com/stretchr/testify/require"
)

type testProviderInventory struct {
	count     int
	available bool
}

func (i testProviderInventory) ProviderDescendantCount() (int, bool) {
	return i.count, i.available
}

type inventoryHermesServer struct {
	nativehermes.Server
	count int
}

func (s inventoryHermesServer) ProviderDescendantCount() (int, bool) {
	return s.count, true
}

func TestProviderProcessTrackerAggregatesOnlyCompleteInventories(t *testing.T) {
	var (
		mu        sync.Mutex
		snapshots []int
	)
	tracker := newProviderProcessTracker(RuntimeResourceHooks{
		ObserveProcessSnapshot: func(_ context.Context, kind RuntimeProcessKind, count int) {
			require.Equal(t, RuntimeProcessProviderDescendant, kind)
			mu.Lock()
			snapshots = append(snapshots, count)
			mu.Unlock()
		},
	})

	unknown := tracker.register()
	known := tracker.register()
	unknown.observe(t.Context(), struct{}{})
	unknown.observe(t.Context(), testProviderInventory{count: 9})
	unknown.observe(t.Context(), testProviderInventory{count: -1, available: true})
	known.observe(t.Context(), testProviderInventory{count: 2, available: true})
	require.Empty(t, snapshots)

	unknown.observe(t.Context(), testProviderInventory{count: 3, available: true})
	require.Equal(t, []int{5}, snapshots)

	unknown.retire(t.Context(), false)
	require.Equal(t, []int{5}, snapshots)
	require.False(t, providerProcessTreeProven(nativehermes.ErrProcessTreeUnproven))
	require.True(t, providerProcessTreeProven(errors.New("ordinary close error")))

	unknown.retire(t.Context(), true)
	known.retire(t.Context(), true)
	known.retire(t.Context(), true)
	known.observe(t.Context(), testProviderInventory{count: 7, available: true})
	require.Equal(t, []int{5, 2, 0}, snapshots)
}

func TestProviderProcessTrackerConcurrentLifecycle(t *testing.T) {
	const roots = 16

	var (
		mu        sync.Mutex
		snapshots []int
	)
	tracker := newProviderProcessTracker(RuntimeResourceHooks{
		ObserveProcessSnapshot: func(_ context.Context, _ RuntimeProcessKind, count int) {
			mu.Lock()
			snapshots = append(snapshots, count)
			mu.Unlock()
		},
	})
	registered := make([]*providerProcessRoot, roots)
	for index := range registered {
		registered[index] = tracker.register()
	}

	var wg sync.WaitGroup
	for _, root := range registered {
		wg.Add(1)
		go func() {
			defer wg.Done()
			root.observe(t.Context(), testProviderInventory{count: 1, available: true})
		}()
	}
	wg.Wait()

	for _, root := range registered {
		wg.Add(1)
		go func() {
			defer wg.Done()
			root.retire(t.Context(), true)
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, snapshots, roots+1)
	require.Equal(t, roots, snapshots[0])
	require.Equal(t, 0, snapshots[len(snapshots)-1])
}

func TestManagedHermesServerProviderInventory(t *testing.T) {
	without := &managedHermesServer{Server: newFakeHermesClient()}
	count, available := without.ProviderDescendantCount()
	require.Zero(t, count)
	require.False(t, available)

	with := &managedHermesServer{Server: inventoryHermesServer{
		Server: newFakeHermesClient(),
		count:  4,
	}}
	count, available = with.ProviderDescendantCount()
	require.Equal(t, 4, count)
	require.True(t, available)
}

func TestHermesProductionProcessSnapshotLifecycle(t *testing.T) {
	tests := []struct {
		name          string
		closeErr      error
		wantSnapshots []int
	}{
		{name: "proven close resets zero", wantSnapshots: []int{4, 4, 0}},
		{
			name:          "unproven close preserves nonzero",
			closeErr:      nativehermes.ErrProcessTreeUnproven,
			wantSnapshots: []int{4, 4},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var snapshots []int
			root := t.TempDir()
			fake := newFakeHermesClient()
			fake.xdg.Root = root
			fake.closeErr = test.closeErr
			agent := NewAgent(
				WithScratchDir(t.TempDir()),
				WithRuntimeResourceHooks(RuntimeResourceHooks{
					ObserveProcessSnapshot: func(_ context.Context, _ RuntimeProcessKind, count int) {
						snapshots = append(snapshots, count)
					},
				}),
				func(options *Options) {
					options.clientFactory = func(context.Context, nativehermes.StartOptions) (nativehermes.Server, error) {
						return inventoryHermesServer{Server: fake, count: 4}, nil
					}
				},
			)

			server, err := agent.newHermesClient(
				t.Context(), "snapshot-session", t.TempDir(), sessionMeta{}, nativehermes.XDGDirs{Root: root},
			)
			require.NoError(t, err)
			require.ErrorIs(t, server.Close(t.Context()), test.closeErr)
			require.Equal(t, test.wantSnapshots, snapshots)
		})
	}
}
