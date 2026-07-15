package hermes

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestObserveHermesStartupStage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	observeHermesStartupStage(ctx, nil, "session", "spawn", time.Now(), nil)

	wantErr := errors.New("spawn failed")
	called := false
	observeHermesStartupStage(ctx, func(gotCtx context.Context, lifecycle, stage string, elapsed time.Duration, err error) {
		called = true
		if gotCtx != ctx || lifecycle != "session" || stage != "spawn" || elapsed < 0 || !errors.Is(err, wantErr) {
			t.Fatalf("observation = (%v, %q, %q, %v, %v)", gotCtx, lifecycle, stage, elapsed, err)
		}
	}, "session", "spawn", time.Now(), wantErr)
	if !called {
		t.Fatal("startup-stage callback was not called")
	}
}
