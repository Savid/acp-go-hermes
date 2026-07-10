package hermesacp

import (
	"context"
	"log/slog"
)

// recoverAgentGoroutine recovers a panic in an agent-owned goroutine and logs
// it instead of crashing the host process.
func recoverAgentGoroutine(ctx context.Context, log *slog.Logger, name string) {
	handleAgentGoroutinePanic(ctx, log, name, nil, recover())
}

func handleAgentGoroutinePanic(
	ctx context.Context,
	log *slog.Logger,
	name string,
	shutdown func(any),
	recovered any,
) {
	if recovered == nil {
		return
	}

	if log == nil {
		log = slog.Default()
	}

	log.ErrorContext(ctx, "agent goroutine panic", slog.String("goroutine", name), slog.Any("panic", recovered))

	if shutdown != nil {
		shutdown(recovered)
	}
}

func agentLogger(agent *Agent) *slog.Logger {
	if agent == nil {
		return nil
	}

	return agent.log
}
