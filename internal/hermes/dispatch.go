package hermes

import (
	"context"
	"errors"
)

type promptDispatchKey struct{}

type PromptDispatchInfo struct {
	CycleID             string
	TransportGeneration uint64
}

var ErrPromptDispatchIdentity = errors.New("hermes prompt dispatch requires exact cycle and transport identity")

// PromptDispatchHook is invoked at the dispatch linearization point: the gateway
// has acknowledged the submitted frame and owns it durably. It runs before any
// event that frame causes, and a refusal from it fails the turn rather than
// letting work proceed under a boundary the caller could not record.
type PromptDispatchHook func(context.Context, PromptDispatchInfo) error

// WithPromptDispatch installs the hook one submission's dispatch point invokes.
func WithPromptDispatch(ctx context.Context, accepted PromptDispatchHook) context.Context {
	return context.WithValue(ctx, promptDispatchKey{}, accepted)
}

// NotifyPromptDispatch runs the installed hook with the exact native route. It
// is called by the gateway once the native side has taken ownership of the
// frame, and is exported so deterministic harnesses can report the same point.
func NotifyPromptDispatch(ctx context.Context, info PromptDispatchInfo) error {
	if info.CycleID == "" || info.TransportGeneration == 0 {
		return ErrPromptDispatchIdentity
	}

	accepted, _ := ctx.Value(promptDispatchKey{}).(PromptDispatchHook)
	if accepted == nil {
		return ErrPromptDispatchIdentity
	}

	return accepted(ctx, info)
}
