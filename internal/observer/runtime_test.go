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
	nilObserver.ObserveNativeStartup(ctx, "session", "spawn", time.Second, nil)

	empty := &Observer{}
	empty.ObserveNativeStartup(ctx, "session", "spawn", time.Second, nil)

	observe := New(Config{})
	observe.ObserveNativeStartup(ctx, "session", "spawn", time.Second, nil)
	observe.ObserveNativeStartup(ctx, "session", "readiness", time.Second, errors.New("not ready"))
}
