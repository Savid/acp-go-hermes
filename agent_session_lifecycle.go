package hermesacp

import (
	"context"
	"sync"

	"github.com/coder/acp-go-sdk"
)

type sessionLifecycleLease struct {
	token chan struct{}
	refs  int
}

func (a *Agent) acquireSessionLifecycle(ctx context.Context, id acp.SessionId) (context.Context, func(), error) {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()

		return nil, nil, acp.NewInvalidRequest(map[string]any{jsonFieldError: agentClosedMessage})
	}

	lease := a.lifecycleLeases[id]
	if lease == nil {
		lease = &sessionLifecycleLease{token: make(chan struct{}, 1)}
		lease.token <- struct{}{}

		a.lifecycleLeases[id] = lease
	}

	lease.refs++

	a.lifecycleSeq++
	operationID := a.lifecycleSeq
	operationCtx, cancel := context.WithCancelCause(ctx)
	a.lifecycleCancel[operationID] = cancel
	a.lifecycleOps.Add(1)
	a.mu.Unlock()

	var once sync.Once

	finish := func(holding bool) {
		once.Do(func() {
			if holding {
				lease.token <- struct{}{}
			}

			a.mu.Lock()

			lease.refs--
			if lease.refs == 0 {
				delete(a.lifecycleLeases, id)
			}

			// A response-scoped active reuse can still inherit operationCtx after
			// the keyed lease is released; its request remains the context owner.
			delete(a.lifecycleCancel, operationID)
			a.mu.Unlock()
			a.lifecycleOps.Done()
		})
	}

	select {
	case <-lease.token:
		if cause := context.Cause(operationCtx); cause != nil {
			finish(true)

			return nil, nil, cause
		}

		return operationCtx, func() { finish(true) }, nil
	case <-operationCtx.Done():
		finish(false)

		return nil, nil, context.Cause(operationCtx)
	}
}
