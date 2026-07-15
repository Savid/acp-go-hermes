package hermesacp

import (
	"context"
	"sync"
	"time"

	"github.com/savid/acp-go-hermes/internal/observer"
)

func instrumentRuntimeResourceHooks(hooks RuntimeResourceHooks, observe *observer.Observer) RuntimeResourceHooks {
	wrapAcquire := func(resource string, acquire func(context.Context, RuntimeResourceKind) (func(), error)) func(context.Context, RuntimeResourceKind) (func(), error) {
		return func(ctx context.Context, lifecycle RuntimeResourceKind) (func(), error) {
			var (
				release func()
				err     error
			)

			if acquire == nil {
				release = func() {}
			} else {
				release, err = acquire(ctx, lifecycle)
			}

			if err != nil || release == nil {
				observe.RecordRuntimeResourceAdmission(ctx, resource, string(lifecycle), "rejected")

				return release, err
			}

			observe.RecordRuntimeResourceAdmission(ctx, resource, string(lifecycle), "admitted")
			observe.AddRuntimeResource(ctx, resource, 1)

			var once sync.Once

			return func() {
				once.Do(func() {
					release()
					observe.AddRuntimeResource(context.Background(), resource, -1)
				})
			}, nil
		}
	}
	hooks.AcquireNativeRoot = wrapAcquire("managed_native_root", hooks.AcquireNativeRoot)
	hooks.ReserveScratchRoot = wrapAcquire("adapter_scratch_root", hooks.ReserveScratchRoot)

	externalProcess := hooks.ObserveProcess
	hooks.ObserveProcess = func(ctx context.Context, kind RuntimeProcessKind, delta int64) {
		observe.AddRuntimeProcess(ctx, string(kind), delta)

		if externalProcess != nil {
			externalProcess(ctx, kind, delta)
		}
	}
	externalSnapshot := hooks.ObserveProcessSnapshot
	hooks.ObserveProcessSnapshot = func(ctx context.Context, kind RuntimeProcessKind, count int) {
		observe.SetRuntimeProcess(ctx, string(kind), count)

		if externalSnapshot != nil {
			externalSnapshot(ctx, kind, count)
		}
	}
	externalStage := hooks.ObserveStartupStage
	hooks.ObserveStartupStage = func(ctx context.Context, lifecycle RuntimeResourceKind, stage RuntimeStartupStage, elapsed time.Duration, err error) {
		observe.ObserveRuntimeStartupStage(ctx, string(lifecycle), string(stage), elapsed, err)

		if externalStage != nil {
			externalStage(ctx, lifecycle, stage, elapsed, err)
		}
	}

	return hooks
}

func observeRuntimeProcess(ctx context.Context, hooks RuntimeResourceHooks, kind RuntimeProcessKind, delta int64) {
	if hooks.ObserveProcess != nil && delta != 0 {
		hooks.ObserveProcess(ctx, kind, delta)
	}
}

func observeRuntimeProcessSnapshot(ctx context.Context, hooks RuntimeResourceHooks, kind RuntimeProcessKind, count int) {
	if hooks.ObserveProcessSnapshot != nil && count >= 0 {
		hooks.ObserveProcessSnapshot(ctx, kind, count)
	}
}

func observeRuntimeStartupStage(
	ctx context.Context,
	hooks RuntimeResourceHooks,
	lifecycle RuntimeResourceKind,
	stage RuntimeStartupStage,
	started time.Time,
	err error,
) {
	if hooks.ObserveStartupStage != nil {
		hooks.ObserveStartupStage(ctx, lifecycle, stage, time.Since(started), err)
	}
}
