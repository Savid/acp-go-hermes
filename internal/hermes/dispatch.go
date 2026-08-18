package hermes

import "context"

type promptDispatchKey struct{}

// PromptDispatchHook is invoked at the dispatch linearization point: the gateway
// has acknowledged the submitted frame and owns it durably. It runs before any
// event that frame causes, and a refusal from it fails the turn rather than
// letting work proceed under a boundary the caller could not record.
type PromptDispatchHook func(context.Context) error

// WithPromptDispatch installs the hook one submission's dispatch point invokes.
// A context without one dispatches exactly as before.
func WithPromptDispatch(ctx context.Context, accepted PromptDispatchHook) context.Context {
	return context.WithValue(ctx, promptDispatchKey{}, accepted)
}

// NotifyPromptDispatch runs the installed hook. It is called by the gateway once
// the native side has taken ownership of the frame, and it is exported so a
// deterministic harness can report the same point the real gateway does.
func NotifyPromptDispatch(ctx context.Context) error {
	accepted, _ := ctx.Value(promptDispatchKey{}).(PromptDispatchHook)
	if accepted == nil {
		return nil
	}

	return accepted(ctx)
}
