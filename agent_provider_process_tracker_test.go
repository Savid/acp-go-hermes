package hermesacp

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
	"github.com/stretchr/testify/require"
)

type testProviderInventory struct {
	count     int
	available bool
}

type mutableProviderInventory struct {
	mu        sync.Mutex
	count     int
	available bool
}

func (i *mutableProviderInventory) ProviderDescendantCount() (int, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()

	return i.count, i.available
}

func (i *mutableProviderInventory) set(count int, available bool) {
	i.mu.Lock()
	i.count = count
	i.available = available
	i.mu.Unlock()
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
	}, true)

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
	require.False(t, providerProcessTreeProven(nativehermes.ErrProcessContainmentIncomplete))
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
	}, true)
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
	require.GreaterOrEqual(t, len(snapshots), 2)
	require.LessOrEqual(t, len(snapshots), roots+1)
	require.Equal(t, roots, snapshots[0])
	require.Equal(t, 0, snapshots[len(snapshots)-1])
}

func TestProviderProcessTrackerRequeriesEveryRoot(t *testing.T) {
	var snapshots []int
	tracker := newProviderProcessTracker(RuntimeResourceHooks{
		ObserveProcessSnapshot: func(_ context.Context, _ RuntimeProcessKind, count int) {
			snapshots = append(snapshots, count)
		},
	}, true)
	rootA := tracker.register()
	rootB := tracker.register()
	inventoryA := &mutableProviderInventory{count: 1, available: true}
	inventoryB := &mutableProviderInventory{count: 2, available: true}

	rootA.observe(t.Context(), inventoryA)
	rootB.observe(t.Context(), inventoryB)
	require.Equal(t, []int{3}, snapshots)

	inventoryA.set(5, true)
	inventoryB.set(4, true)
	rootB.observe(t.Context(), inventoryB)
	require.Equal(t, []int{3, 9}, snapshots)

	inventoryA.set(5, false)
	rootB.observe(t.Context(), inventoryB)
	require.Equal(t, []int{3, 9}, snapshots)

	inventoryA.set(6, true)
	rootB.observe(t.Context(), inventoryB)
	require.Equal(t, []int{3, 9, 10}, snapshots)
}

func TestProviderProcessTrackerHookCanReenter(t *testing.T) {
	var (
		root      *providerProcessRoot
		snapshots []int
	)
	tracker := newProviderProcessTracker(RuntimeResourceHooks{
		ObserveProcessSnapshot: func(ctx context.Context, _ RuntimeProcessKind, count int) {
			snapshots = append(snapshots, count)
			if count == 1 {
				root.retire(ctx, true)
			}
		},
	}, true)
	root = tracker.register()
	root.observe(t.Context(), testProviderInventory{count: 1, available: true})

	require.Equal(t, []int{1, 0}, snapshots)
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
			closeErr:      nativehermes.ErrProcessContainmentIncomplete,
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
			options := []Option{
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
			}
			if runtime.GOOS == "darwin" {
				options = append(options, WithDarwinBestEffortContainment())
				test.wantSnapshots = nil
			}
			agent := NewAgent(options...)

			server, err := agent.newHermesClient(
				t.Context(), "snapshot-session", t.TempDir(), sessionMeta{}, nativehermes.XDGDirs{Root: root},
			)
			require.NoError(t, err)
			require.ErrorIs(t, server.Close(t.Context()), test.closeErr)
			require.Equal(t, test.wantSnapshots, snapshots)
		})
	}
}
