package observer

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRuntimeObserver(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	var nilObserver *Observer
	nilObserver.RecordRuntimeResourceAdmission(ctx, "native", "session", "admitted")
	nilObserver.AddRuntimeResource(ctx, "native", 1)
	nilObserver.AddRuntimeProcess(ctx, "supervisor", 1)
	nilObserver.SetRuntimeProcess(ctx, "descendant", 1)
	nilObserver.ObserveRuntimeStartupStage(ctx, "session", "spawn", time.Second, nil)

	empty := &Observer{}
	empty.RecordRuntimeResourceAdmission(ctx, "native", "session", "admitted")
	empty.AddRuntimeResource(ctx, "native", 1)
	empty.AddRuntimeProcess(ctx, "supervisor", 1)
	empty.SetRuntimeProcess(ctx, "descendant", 1)
	empty.ObserveRuntimeStartupStage(ctx, "session", "spawn", time.Second, nil)

	observe := New(Config{})
	observe.RecordRuntimeResourceAdmission(ctx, "native", "session", "admitted")
	observe.AddRuntimeResource(ctx, "native", 0)
	observe.AddRuntimeResource(ctx, "native", 1)
	observe.AddRuntimeProcess(ctx, "supervisor", 0)
	observe.AddRuntimeProcess(ctx, "supervisor", 2)
	observe.SetRuntimeProcess(ctx, "descendant", -1)
	observe.SetRuntimeProcess(ctx, "descendant", 3)
	observe.SetRuntimeProcess(ctx, "descendant", 1)
	observe.ObserveRuntimeStartupStage(ctx, "session", "spawn", time.Second, nil)
	observe.ObserveRuntimeStartupStage(ctx, "session", "readiness", time.Second, errors.New("not ready"))
}
