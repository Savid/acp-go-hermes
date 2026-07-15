package hermesacp

import (
	"context"
	"errors"
	"testing"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
	"github.com/savid/acp-go-hermes/internal/observer"
	"github.com/stretchr/testify/require"
)

func TestRuntimeObservationHooksComposeExactLifetimes(t *testing.T) {
	var releases int
	var processDelta int64
	var snapshot int
	var stage RuntimeStartupStage
	hooks := instrumentRuntimeResourceHooks(RuntimeResourceHooks{
		AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
			return func() { releases++ }, nil
		},
		ObserveProcess: func(_ context.Context, _ RuntimeProcessKind, delta int64) {
			processDelta += delta
		},
		ObserveProcessSnapshot: func(_ context.Context, _ RuntimeProcessKind, count int) {
			snapshot = count
		},
		ObserveStartupStage: func(_ context.Context, _ RuntimeResourceKind, got RuntimeStartupStage, _ time.Duration, _ error) {
			stage = got
		},
	}, observer.New(observer.Config{}))

	release, err := hooks.AcquireNativeRoot(t.Context(), RuntimeResourceSession)
	require.NoError(t, err)
	release()
	release()
	require.Equal(t, 1, releases)

	observeRuntimeProcess(t.Context(), hooks, RuntimeProcessHomeLockSupervisor, 2)
	observeRuntimeProcessSnapshot(t.Context(), hooks, RuntimeProcessProviderDescendant, 3)
	observeRuntimeStartupStage(t.Context(), hooks, RuntimeResourceRuntime, RuntimeStartupReadiness, time.Now(), nil)
	require.Equal(t, int64(2), processDelta)
	require.Equal(t, 3, snapshot)
	require.Equal(t, RuntimeStartupReadiness, stage)

	wantErr := errors.New("full")
	rejected := instrumentRuntimeResourceHooks(RuntimeResourceHooks{
		ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
			return nil, wantErr
		},
	}, observer.New(observer.Config{}))
	_, err = rejected.ReserveScratchRoot(t.Context(), RuntimeResourcePrompt)
	require.ErrorIs(t, err, wantErr)
}

func TestHermesClientForwardsNativeStartupStages(t *testing.T) {
	var gotKind RuntimeResourceKind
	var gotStage RuntimeStartupStage
	agent := NewAgent(
		WithScratchDir(t.TempDir()),
		WithRuntimeResourceHooks(RuntimeResourceHooks{
			ObserveStartupStage: func(_ context.Context, kind RuntimeResourceKind, stage RuntimeStartupStage, _ time.Duration, err error) {
				require.NoError(t, err)
				gotKind = kind
				gotStage = stage
			},
		}),
		func(options *Options) {
			options.clientFactory = func(ctx context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
				start.ObserveStartupStage(ctx, string(RuntimeResourceSession), string(RuntimeStartupReadiness), time.Second, nil)

				return newFakeHermesClient(), nil
			}
		},
	)

	client, err := agent.newHermesClient(t.Context(), "session", t.TempDir(), sessionMeta{}, nativehermes.XDGDirs{})
	require.NoError(t, err)
	require.Equal(t, RuntimeResourceSession, gotKind)
	require.Equal(t, RuntimeStartupReadiness, gotStage)
	require.NoError(t, client.Close(t.Context()))
}
