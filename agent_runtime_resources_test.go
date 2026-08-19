package hermesacp

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
	"github.com/stretchr/testify/require"
)

func TestRuntimeResourceHooks(t *testing.T) {
	options := Options{}
	WithRuntimeResourceHooks(RuntimeResourceHooks{AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
		return func() {}, nil
	}})(&options)
	require.NotNil(t, options.RuntimeResourceHooks.AcquireNativeRoot)

	release, err := acquireNativeRoot(t.Context(), RuntimeResourceHooks{}, RuntimeResourceSession)
	require.NoError(t, err)
	release()

	wantErr := errors.New("full")
	_, err = reserveScratchRoot(t.Context(), RuntimeResourceHooks{ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
		return nil, wantErr
	}}, RuntimeResourceSession)
	require.ErrorIs(t, err, wantErr)

	_, err = acquireNativeRoot(t.Context(), RuntimeResourceHooks{AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
		return nil, nil //nolint:nilnil // A nil release is the invalid hook result under test.
	}}, RuntimeResourcePrompt)
	require.ErrorContains(t, err, "nil release")

	releases := 0
	release, err = acquireNativeRoot(t.Context(), RuntimeResourceHooks{AcquireNativeRoot: func(_ context.Context, kind RuntimeResourceKind) (func(), error) {
		require.Equal(t, RuntimeResourceDiscovery, kind)

		return func() { releases++ }, nil
	}}, RuntimeResourceDiscovery)
	require.NoError(t, err)
	release()
	release()
	require.Equal(t, 1, releases)
}

func TestManagedHermesServerResourceRelease(t *testing.T) {
	previousRemove := runtimeRemoveAll
	t.Cleanup(func() { runtimeRemoveAll = previousRemove })

	t.Run("success is idempotent", func(t *testing.T) {
		runtimeRemoveAll = previousRemove
		nativeReleases, scratchReleases := 0, 0
		server := &managedHermesServer{
			Server:         newFakeHermesClient(),
			root:           t.TempDir(),
			nativeRelease:  func() { nativeReleases++ },
			scratchRelease: func() { scratchReleases++ },
		}

		require.NoError(t, server.Close(t.Context()))
		require.NoError(t, server.Close(t.Context()))
		require.Equal(t, 1, nativeReleases)
		require.Equal(t, 1, scratchReleases)
	})

	t.Run("ordinary close error still unwinds", func(t *testing.T) {
		runtimeRemoveAll = previousRemove
		nativeReleases, scratchReleases := 0, 0
		closeErr := errors.New("close")
		client := newFakeHermesClient()
		client.closeErr = closeErr
		server := &managedHermesServer{
			Server:         client,
			root:           t.TempDir(),
			nativeRelease:  func() { nativeReleases++ },
			scratchRelease: func() { scratchReleases++ },
		}

		require.ErrorIs(t, server.Close(t.Context()), closeErr)
		require.Equal(t, 1, nativeReleases)
		require.Equal(t, 1, scratchReleases)
	})

	t.Run("unproven tree retains all ownership", func(t *testing.T) {
		removeCalls := 0
		runtimeRemoveAll = func(string) error {
			removeCalls++

			return nil
		}
		nativeReleases, scratchReleases := 0, 0
		client := newFakeHermesClient()
		client.closeErr = errors.Join(errors.New("shutdown failed"), nativehermes.ErrProcessContainmentIncomplete)
		server := &managedHermesServer{
			Server:         client,
			root:           t.TempDir(),
			nativeRelease:  func() { nativeReleases++ },
			scratchRelease: func() { scratchReleases++ },
		}

		require.ErrorIs(t, server.Close(t.Context()), nativehermes.ErrProcessContainmentIncomplete)
		require.Zero(t, removeCalls)
		require.Zero(t, nativeReleases)
		require.Zero(t, scratchReleases)
	})

	t.Run("deletion failure retains scratch and joins errors", func(t *testing.T) {
		closeErr := errors.New("close")
		removeErr := errors.New("remove")
		runtimeRemoveAll = func(string) error { return removeErr }
		nativeReleases, scratchReleases := 0, 0
		client := newFakeHermesClient()
		client.closeErr = closeErr
		server := &managedHermesServer{
			Server:         client,
			root:           t.TempDir(),
			nativeRelease:  func() { nativeReleases++ },
			scratchRelease: func() { scratchReleases++ },
		}

		err := server.Close(t.Context())
		require.ErrorIs(t, err, closeErr)
		require.ErrorIs(t, err, removeErr)
		require.Equal(t, 1, nativeReleases)
		require.Zero(t, scratchReleases)
	})
}

func TestHermesSessionResourceAdmission(t *testing.T) {
	wantErr := errors.New("resource exhausted")
	agent := newTestAgent(WithScratchDir(t.TempDir()), WithRuntimeResourceHooks(RuntimeResourceHooks{
		ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) { return nil, wantErr },
	}))
	_, err := agent.newHermesClient(t.Context(), "session-1", t.TempDir(), sessionMeta{}, nativehermes.XDGDirs{})
	require.ErrorIs(t, err, wantErr)
	_, err = agent.LoadSession(t.Context(), LoadSessionRequest("session-2", t.TempDir()))
	require.ErrorIs(t, err, wantErr)

	scratchReleases := 0
	agent = newTestAgent(WithScratchDir(t.TempDir()), WithRuntimeResourceHooks(RuntimeResourceHooks{
		ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
			return func() { scratchReleases++ }, nil
		},
		AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) { return nil, wantErr },
	}))
	_, err = agent.newHermesClient(t.Context(), "session-1", t.TempDir(), sessionMeta{}, nativehermes.XDGDirs{})
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, 1, scratchReleases)

	forkBlocked := newTestAgent(WithScratchDir(t.TempDir()), WithRuntimeResourceHooks(RuntimeResourceHooks{
		ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) { return nil, wantErr },
	}))
	client := newFakeHermesClient()
	client.forkSession = testNativeSession("native-child")
	parent := testSession(forkBlocked, client)
	forkBlocked.sessions[parent.id] = parent
	_, err = forkBlocked.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir()))
	require.ErrorIs(t, err, wantErr)

	previousReap := reapHermesLeaseFile
	reapHermesLeaseFile = func(string, *slog.Logger) bool { return true }
	t.Cleanup(func() { reapHermesLeaseFile = previousReap })
	err = newTestAgent().cleanupDeletedSession(deleteCleanupRecord{SessionID: "session-1", XDGRoot: t.TempDir()})
	require.ErrorContains(t, err, "live lease")
}

func TestHermesVersionDiscoveryHasIndependentAdmissions(t *testing.T) {
	var nativeKinds, scratchKinds []RuntimeResourceKind
	var startOptions nativehermes.StartOptions
	agent := newTestAgent(WithScratchDir(t.TempDir()), WithRuntimeResourceHooks(RuntimeResourceHooks{
		ReserveScratchRoot: func(_ context.Context, kind RuntimeResourceKind) (func(), error) {
			scratchKinds = append(scratchKinds, kind)

			return func() {}, nil
		},
		AcquireNativeRoot: func(_ context.Context, kind RuntimeResourceKind) (func(), error) {
			nativeKinds = append(nativeKinds, kind)

			return func() {}, nil
		},
	}))
	agent.options.clientFactory = func(ctx context.Context, options nativehermes.StartOptions) (nativehermes.Server, error) {
		startOptions = options
		require.NotNil(t, options.AcquireDiscoveryResources)
		require.NotNil(t, options.RetainDiscoveryRoot)
		nativeRelease, scratchRelease, err := options.AcquireDiscoveryResources(ctx)
		require.NoError(t, err)
		nativeRelease()
		scratchRelease()
		client := newFakeHermesClient()
		client.xdg = options.ExistingXDG

		return client, nil
	}

	client, err := agent.newHermesClient(t.Context(), "session-1", t.TempDir(), sessionMeta{}, nativehermes.XDGDirs{})
	require.NoError(t, err)
	require.Equal(t, []RuntimeResourceKind{RuntimeResourceSession, RuntimeResourceDiscovery}, nativeKinds)
	require.Equal(t, []RuntimeResourceKind{RuntimeResourceSession, RuntimeResourceDiscovery}, scratchKinds)
	require.NoError(t, client.Close(t.Context()))
	require.Empty(t, hermesServerRoot(nil))

	wantErr := errors.New("discovery rejected")
	agent.options.RuntimeResourceHooks.ReserveScratchRoot = func(context.Context, RuntimeResourceKind) (func(), error) {
		return nil, wantErr
	}
	_, _, err = startOptions.AcquireDiscoveryResources(t.Context())
	require.ErrorIs(t, err, wantErr)

	discoveryScratchReleases := 0
	agent.options.RuntimeResourceHooks.ReserveScratchRoot = func(context.Context, RuntimeResourceKind) (func(), error) {
		return func() { discoveryScratchReleases++ }, nil
	}
	agent.options.RuntimeResourceHooks.AcquireNativeRoot = func(context.Context, RuntimeResourceKind) (func(), error) {
		return nil, wantErr
	}
	_, _, err = startOptions.AcquireDiscoveryResources(t.Context())
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, 1, discoveryScratchReleases)

	startOptions.RetainDiscoveryRoot("/scratch/acp-go-hermes-runtime-retained", nativehermes.ErrProcessContainmentIncomplete)
	require.ErrorIs(t, agent.Close(), nativehermes.ErrProcessContainmentIncomplete)
}

func TestHermesGenerationAndScratchPreparationFailures(t *testing.T) {
	wantErr := errors.New("generation failed")
	previousCreate := createHermesGeneration
	createHermesGeneration = func(string) (nativehermes.XDGDirs, error) {
		return nativehermes.XDGDirs{}, wantErr
	}
	t.Cleanup(func() { createHermesGeneration = previousCreate })

	t.Run("new client", func(t *testing.T) {
		agent := newTestAgent(WithScratchDir(t.TempDir()))
		_, err := agent.newHermesClient(t.Context(), "session-1", t.TempDir(), sessionMeta{}, nativehermes.XDGDirs{})
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("load", func(t *testing.T) {
		agent := newTestAgent(
			WithScratchDir(t.TempDir()),
			WithSessionStore(validHydrateStore(t, t.Context())),
		)
		_, err := agent.LoadSession(t.Context(), LoadSessionRequest("s", t.TempDir()))
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("resume runtime", func(t *testing.T) {
		session, _, _ := newResumeRuntimeTestSession(t)
		require.ErrorIs(t, session.resumeRuntimeForTurnLocked(t.Context()), wantErr)
	})

	t.Run("fork", func(t *testing.T) {
		agent := newTestAgent(WithScratchDir(t.TempDir()))
		parentClient := newFakeHermesClient()
		parentClient.forkSession = testNativeSession("native-child")
		parent := testSession(agent, parentClient)
		agent.sessions[parent.id] = parent

		_, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir()))
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("new client with scratch", func(t *testing.T) {
		blockedRoot := filepath.Join(t.TempDir(), "file")
		require.NoError(t, os.WriteFile(blockedRoot, []byte("blocked"), 0o600))
		agent := newTestAgent(WithScratchDir(blockedRoot))
		_, err := agent.newHermesClientWithScratch(
			t.Context(),
			"session-1",
			t.TempDir(),
			sessionMeta{},
			nativehermes.XDGDirs{Root: t.TempDir()},
			func() {},
		)
		require.Error(t, err)
	})

	t.Run("shared Hermes home ownership", func(t *testing.T) {
		agent := newTestAgent(
			WithSharedHermesHome(t.TempDir()),
			WithProcessIsolation(ProcessIsolation{
				UID: uint32(os.Geteuid() + 1), GID: uint32(os.Getegid() + 1),
			}),
		)
		_, err := agent.newHermesClientWithScratch(
			t.Context(), "session-1", t.TempDir(), sessionMeta{}, nativehermes.XDGDirs{}, func() {},
		)
		require.Error(t, err)
	})
}

func TestHermesSessionRetainsNativeAdmissionWhenQuiescenceIsUnproven(t *testing.T) {
	previousRemove := runtimeRemoveAll
	t.Cleanup(func() { runtimeRemoveAll = previousRemove })

	t.Run("factory sentinel retains native scratch and root", func(t *testing.T) {
		runtimeRemoveAll = previousRemove
		nativeReleases, scratchReleases := 0, 0
		agent := newTestAgent(WithScratchDir(t.TempDir()), WithRuntimeResourceHooks(RuntimeResourceHooks{
			ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
				return func() { scratchReleases++ }, nil
			},
			AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
				return func() { nativeReleases++ }, nil
			},
		}))

		var xdg nativehermes.XDGDirs
		factoryCalls := 0
		agent.options.clientFactory = func(_ context.Context, options nativehermes.StartOptions) (nativehermes.Server, error) {
			factoryCalls++
			xdg = options.ExistingXDG
			require.NotEmpty(t, xdg.Root)

			return nil, nativehermes.ErrProcessContainmentIncomplete
		}

		_, err := agent.newHermesClient(t.Context(), "session-1", t.TempDir(), sessionMeta{}, nativehermes.XDGDirs{})
		require.ErrorIs(t, err, nativehermes.ErrProcessContainmentIncomplete)
		require.DirExists(t, xdg.Root)
		require.Zero(t, nativeReleases)
		require.Zero(t, scratchReleases)
		_, retryErr := agent.newHermesClient(t.Context(), "session-1", t.TempDir(), sessionMeta{}, nativehermes.XDGDirs{})
		require.ErrorIs(t, retryErr, nativehermes.ErrProcessContainmentIncomplete)
		require.Equal(t, 1, factoryCalls)
		require.NoError(t, previousRemove(xdg.Root))
	})

	t.Run("ordinary factory error deletes and releases", func(t *testing.T) {
		runtimeRemoveAll = previousRemove
		startupErr := errors.New("startup")
		nativeReleases, scratchReleases := 0, 0
		agent := newTestAgent(WithScratchDir(t.TempDir()), WithRuntimeResourceHooks(RuntimeResourceHooks{
			ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
				return func() { scratchReleases++ }, nil
			},
			AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
				return func() { nativeReleases++ }, nil
			},
		}))

		var xdg nativehermes.XDGDirs
		agent.options.clientFactory = func(_ context.Context, options nativehermes.StartOptions) (nativehermes.Server, error) {
			xdg = options.ExistingXDG
			require.NotEmpty(t, xdg.Root)

			return nil, startupErr
		}

		_, err := agent.newHermesClient(t.Context(), "session-1", t.TempDir(), sessionMeta{}, nativehermes.XDGDirs{})
		require.ErrorIs(t, err, startupErr)
		require.NoDirExists(t, xdg.Root)
		require.Equal(t, 1, nativeReleases)
		require.Equal(t, 1, scratchReleases)
	})

	t.Run("delete failure joins and retains scratch", func(t *testing.T) {
		startupErr := errors.New("startup")
		deleteErr := errors.New("delete")
		runtimeRemoveAll = func(string) error { return deleteErr }
		nativeReleases, scratchReleases := 0, 0
		agent := newTestAgent(WithScratchDir(t.TempDir()), WithRuntimeResourceHooks(RuntimeResourceHooks{
			ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
				return func() { scratchReleases++ }, nil
			},
			AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
				return func() { nativeReleases++ }, nil
			},
		}))

		var xdg nativehermes.XDGDirs
		agent.options.clientFactory = func(_ context.Context, options nativehermes.StartOptions) (nativehermes.Server, error) {
			xdg = options.ExistingXDG
			require.NotEmpty(t, xdg.Root)

			return nil, startupErr
		}

		_, err := agent.newHermesClient(t.Context(), "session-1", t.TempDir(), sessionMeta{}, nativehermes.XDGDirs{})
		require.ErrorIs(t, err, startupErr)
		require.ErrorIs(t, err, deleteErr)
		require.DirExists(t, xdg.Root)
		require.Equal(t, 1, nativeReleases)
		require.Zero(t, scratchReleases)
		require.NoError(t, previousRemove(xdg.Root))
	})
}

func TestDifferentACPRecordsCannotClaimTheSameSharedNativeSession(t *testing.T) {
	home := t.TempDir()
	firstAgent := newTestAgent(WithSharedHermesHome(home))
	secondAgent := newTestAgent(WithSharedHermesHome(home))
	first := &managedHermesServer{Server: newFakeHermesClient()}
	second := &managedHermesServer{Server: newFakeHermesClient()}

	if err := firstAgent.claimSharedNativeSession(first, "same-native"); err != nil {
		t.Fatalf("first native claim: %v", err)
	}
	t.Cleanup(func() { _ = first.nativeSessionOwner.Release() })
	if err := secondAgent.claimSharedNativeSession(second, "same-native"); err == nil {
		t.Fatal("different ACP record claimed an already-owned native session")
	}
	if err := secondAgent.claimSharedNativeSession(second, "different-native"); err != nil {
		t.Fatalf("different native session claim: %v", err)
	}
	t.Cleanup(func() { _ = second.nativeSessionOwner.Release() })
}

func TestHermesLoadAndForkFactorySentinelRetainsOwnership(t *testing.T) {
	t.Run("load", func(t *testing.T) {
		nativeReleases, scratchReleases := 0, 0
		agent := newTestAgent(
			WithScratchDir(t.TempDir()),
			WithSessionStore(validHydrateStore(t, t.Context())),
			WithRuntimeResourceHooks(RuntimeResourceHooks{
				ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
					return func() { scratchReleases++ }, nil
				},
				AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
					return func() { nativeReleases++ }, nil
				},
			}),
		)

		var root string
		agent.options.clientFactory = func(_ context.Context, options nativehermes.StartOptions) (nativehermes.Server, error) {
			root = options.ExistingXDG.Root

			return nil, nativehermes.ErrProcessContainmentIncomplete
		}

		_, err := agent.LoadSession(t.Context(), LoadSessionRequest("s", t.TempDir()))
		require.ErrorIs(t, err, nativehermes.ErrProcessContainmentIncomplete)
		require.DirExists(t, root)
		require.Zero(t, nativeReleases)
		require.Zero(t, scratchReleases)
		require.NoError(t, os.RemoveAll(root))
	})

	t.Run("fork", func(t *testing.T) {
		nativeReleases, scratchReleases := 0, 0
		agent := newTestAgent(WithScratchDir(t.TempDir()), WithRuntimeResourceHooks(RuntimeResourceHooks{
			ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
				return func() { scratchReleases++ }, nil
			},
			AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
				return func() { nativeReleases++ }, nil
			},
		}))
		parentClient := newFakeHermesClient()
		parentClient.forkSession = testNativeSession("native-child")
		parent := testSession(agent, parentClient)
		agent.sessions[parent.id] = parent

		var root string
		agent.options.clientFactory = func(_ context.Context, options nativehermes.StartOptions) (nativehermes.Server, error) {
			root = options.ExistingXDG.Root

			return nil, nativehermes.ErrProcessContainmentIncomplete
		}

		_, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir()))
		require.ErrorIs(t, err, nativehermes.ErrProcessContainmentIncomplete)
		require.DirExists(t, root)
		require.Zero(t, nativeReleases)
		require.Zero(t, scratchReleases)
		require.NoError(t, os.RemoveAll(root))
	})

	t.Run("fork rejects a retained generated root before admission", func(t *testing.T) {
		previousReader := sessionIDRandReader
		sessionIDRandReader = bytes.NewReader(make([]byte, 16))
		t.Cleanup(func() { sessionIDRandReader = previousReader })

		scratchAcquires := 0
		agent := newTestAgent(WithScratchDir(t.TempDir()), WithRuntimeResourceHooks(RuntimeResourceHooks{
			ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
				scratchAcquires++

				return func() {}, nil
			},
		}))
		parentClient := newFakeHermesClient()
		parentClient.forkSession = testNativeSession("native-child")
		parent := testSession(agent, parentClient)
		agent.sessions[parent.id] = parent
		agent.retainIncompleteHermesRoot("00000000-0000-4000-8000-000000000000", filepath.Join(agent.homeRoot(), "retained"))

		_, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir()))
		require.ErrorIs(t, err, nativehermes.ErrProcessContainmentIncomplete)
		require.Zero(t, scratchAcquires)
	})
}

func TestHermesLoadGetSessionFailureUnwind(t *testing.T) {
	previousRemove := runtimeRemoveAll
	t.Cleanup(func() { runtimeRemoveAll = previousRemove })

	tests := []struct {
		name                string
		closeErr            error
		deleteErr           error
		wantSentinel        bool
		wantNativeReleases  int
		wantScratchReleases int
		wantRoot            bool
	}{
		{name: "ordinary close error unwinds", closeErr: errors.New("close"), wantNativeReleases: 1, wantScratchReleases: 1},
		{name: "proof sentinel retains all ownership", closeErr: errors.Join(errors.New("close"), nativehermes.ErrProcessContainmentIncomplete), wantSentinel: true, wantRoot: true},
		{name: "delete failure retains scratch", closeErr: errors.New("close"), deleteErr: errors.New("delete"), wantNativeReleases: 1, wantRoot: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtimeRemoveAll = previousRemove
			if test.deleteErr != nil {
				runtimeRemoveAll = func(string) error { return test.deleteErr }
			}

			getErr := errors.New("get")
			nativeReleases, scratchReleases := 0, 0
			client := newFakeHermesClient()
			client.getErr = getErr
			client.closeErr = test.closeErr
			agent := newTestAgent(
				WithScratchDir(t.TempDir()),
				WithSessionStore(validHydrateStore(t, t.Context())),
				WithRuntimeResourceHooks(RuntimeResourceHooks{
					ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
						return func() { scratchReleases++ }, nil
					},
					AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
						return func() { nativeReleases++ }, nil
					},
				}),
			)

			var root string
			factoryCalls := 0
			agent.options.clientFactory = func(_ context.Context, options nativehermes.StartOptions) (nativehermes.Server, error) {
				factoryCalls++
				root = options.ExistingXDG.Root
				client.xdg = options.ExistingXDG

				return client, nil
			}

			_, err := agent.LoadSession(t.Context(), LoadSessionRequest("s", t.TempDir()))
			require.ErrorIs(t, err, getErr)
			require.ErrorIs(t, err, test.closeErr)
			if test.wantSentinel {
				require.ErrorIs(t, err, nativehermes.ErrProcessContainmentIncomplete)
				_, retryErr := agent.LoadSession(t.Context(), LoadSessionRequest("s", t.TempDir()))
				require.ErrorIs(t, retryErr, nativehermes.ErrProcessContainmentIncomplete)
				require.Equal(t, 1, factoryCalls)
			}
			if test.deleteErr != nil {
				require.ErrorIs(t, err, test.deleteErr)
			}

			require.Equal(t, test.wantNativeReleases, nativeReleases)
			require.Equal(t, test.wantScratchReleases, scratchReleases)
			if test.wantRoot {
				require.DirExists(t, root)
				require.NoError(t, previousRemove(root))
			} else {
				require.NoDirExists(t, root)
			}
		})
	}
}

func TestHermesForkGetSessionProofFailureRetainsOwnership(t *testing.T) {
	nativeReleases, scratchReleases := 0, 0
	agent := newTestAgent(WithScratchDir(t.TempDir()), WithRuntimeResourceHooks(RuntimeResourceHooks{
		ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
			return func() { scratchReleases++ }, nil
		},
		AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
			return func() { nativeReleases++ }, nil
		},
	}))
	parentClient := newFakeHermesClient()
	parentClient.forkSession = testNativeSession("native-child")
	parent := testSession(agent, parentClient)
	agent.sessions[parent.id] = parent

	getErr := errors.New("get")
	child := newFakeHermesClient()
	child.getErr = getErr
	child.closeErr = errors.Join(errors.New("close"), nativehermes.ErrProcessContainmentIncomplete)
	var root string
	agent.options.clientFactory = func(_ context.Context, options nativehermes.StartOptions) (nativehermes.Server, error) {
		root = options.ExistingXDG.Root
		child.xdg = options.ExistingXDG

		return child, nil
	}

	_, err := agent.forkSession(t.Context(), ForkSessionRequest(parent.id, t.TempDir()))
	require.ErrorIs(t, err, getErr)
	require.ErrorIs(t, err, nativehermes.ErrProcessContainmentIncomplete)
	require.DirExists(t, root)
	require.Zero(t, nativeReleases)
	require.Zero(t, scratchReleases)
	require.ErrorIs(t, agent.Close(), nativehermes.ErrProcessContainmentIncomplete)
	require.NoError(t, os.RemoveAll(root))
}

func TestHermesFailedStartedSessionProofFailureRetainsOwnership(t *testing.T) {
	nativeReleases, scratchReleases := 0, 0
	rootParent := t.TempDir()
	xdg, err := testGenerationXDG(rootParent)
	require.NoError(t, err)
	client := newFakeHermesClient()
	client.xdg = xdg
	client.closeErr = nativehermes.ErrProcessContainmentIncomplete
	managed := &managedHermesServer{
		Server:         client,
		root:           xdg.Root,
		nativeRelease:  func() { nativeReleases++ },
		scratchRelease: func() { scratchReleases++ },
	}
	agent := newTestAgent()
	session := newSession(agent, "failed-start", t.TempDir(), nil, nil, testNativeSession("native-failed"), managed, sessionMeta{}, idmapRecord{})
	agent.sessions[session.id] = session

	err = agent.cleanupFailedStartedSession(t.Context(), session)
	require.ErrorIs(t, err, nativehermes.ErrProcessContainmentIncomplete)
	require.Nil(t, agent.activeSession(session.id))
	require.DirExists(t, xdg.Root)
	require.Zero(t, nativeReleases)
	require.Zero(t, scratchReleases)
	require.ErrorIs(t, agent.Close(), nativehermes.ErrProcessContainmentIncomplete)
	require.NoError(t, os.RemoveAll(xdg.Root))
}

func TestHermesDeleteProofFailureRetainsOwnership(t *testing.T) {
	nativeReleases, scratchReleases := 0, 0
	rootParent := t.TempDir()
	xdg, err := testGenerationXDG(rootParent)
	require.NoError(t, err)
	client := newFakeHermesClient()
	client.xdg = xdg
	client.closeErr = nativehermes.ErrProcessContainmentIncomplete
	managed := &managedHermesServer{
		Server:         client,
		root:           xdg.Root,
		nativeRelease:  func() { nativeReleases++ },
		scratchRelease: func() { scratchReleases++ },
	}
	agent := newTestAgent()
	session := newSession(agent, "deleted", t.TempDir(), nil, nil, testNativeSession("native-deleted"), managed, sessionMeta{}, idmapRecord{})
	agent.sessions[session.id] = session

	_, err = agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(session.id))
	require.ErrorIs(t, err, nativehermes.ErrProcessContainmentIncomplete)
	require.Nil(t, agent.activeSession(session.id))
	require.Contains(t, agent.deleted, session.id)
	require.NotContains(t, agent.deleteCleanup, session.id)
	require.DirExists(t, xdg.Root)
	require.Zero(t, nativeReleases)
	require.Zero(t, scratchReleases)
	require.ErrorIs(t, agent.Close(), nativehermes.ErrProcessContainmentIncomplete)
	require.NoError(t, os.RemoveAll(xdg.Root))
}

func TestManagedHermesServerCapabilityEdges(t *testing.T) {
	base := newFakeHermesClient()
	serverOnly := sessionOperationServerOnly{Server: base}
	managed := &managedHermesServer{Server: serverOnly}
	if _, err := managed.CreateSessionWithDraft(t.Context(), "title", func(nativehermes.SessionDraft) error { return nil }); err == nil {
		t.Fatal("missing draft capability accepted")
	}
	if _, err := managed.PersistedSessions(t.Context()); err == nil {
		t.Fatal("missing persisted inventory accepted")
	}
	if _, err := managed.ForkWithBaseline(t.Context(), "parent", "marker", nil); err == nil {
		t.Fatal("missing recoverable fork accepted")
	}
	if err := managed.SetModel(t.Context(), "native", "provider/model"); err == nil {
		t.Fatal("missing model selection capability accepted")
	}
	wantModelErr := errors.New("set model")
	managed.Server = modelSetterTestServer{Server: base, err: wantModelErr}
	if err := managed.SetModel(t.Context(), "native", "provider/model"); !errors.Is(err, wantModelErr) {
		t.Fatalf("model selection error = %v", err)
	}

	base.forkSession = testNativeSession("child")
	managed.Server = base
	if child, err := managed.ForkWithBaseline(t.Context(), "parent", "marker", nil); err != nil || child.ID != "child" {
		t.Fatalf("recoverable fork=%+v err=%v", child, err)
	}

	managed.providerAuthSupported = true
	if !managed.ProviderAuthSupported() {
		t.Fatal("forced provider auth was not advertised")
	}
	managed.providerAuthSupported = false
	supported := false
	base.providerAuthSupported = &supported
	if managed.ProviderAuthSupported() {
		t.Fatal("native provider-auth result ignored")
	}
	managed.Server = serverOnly
	if managed.ProviderAuthSupported() {
		t.Fatal("missing provider-auth capability advertised")
	}
}

func TestClaimSharedNativeSessionFailureEdges(t *testing.T) {
	isolated := newTestAgent()
	if err := isolated.claimSharedNativeSession(newFakeHermesClient(), "native"); err != nil {
		t.Fatal(err)
	}

	home := t.TempDir()
	agent := newTestAgent(WithSharedHermesHome(home))
	if err := agent.claimSharedNativeSession(newFakeHermesClient(), "unmanaged"); err == nil {
		t.Fatal("unmanaged server accepted a shared native claim")
	}

	managed := &managedHermesServer{Server: newFakeHermesClient()}
	if err := agent.claimSharedNativeSession(managed, "first"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = managed.nativeSessionOwner.Release() })
	if err := agent.claimSharedNativeSession(managed, "second"); err == nil {
		t.Fatal("managed server accepted a second shared native claim")
	}

	unbound := &managedHermesServer{Server: sessionOperationServerOnly{Server: newFakeHermesClient()}}
	if err := agent.claimSharedNativeSession(unbound, "third"); err == nil {
		t.Fatal("server without process identity accepted a shared native claim")
	}
}

func TestManagedProviderTreeVacantReportsServerInventory(t *testing.T) {
	managed := &managedHermesServer{Server: treeInventoryServer{fakeHermesClient: newFakeHermesClient(), vacant: true}}
	vacant, proved := managed.ProviderTreeVacant()
	require.True(t, vacant)
	require.True(t, proved)
}
