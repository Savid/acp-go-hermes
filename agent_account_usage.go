package hermesacp

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/savid/acp-go-core/process"
	"github.com/savid/acp-go-core/usage"
	"github.com/savid/acp-go-core/usage/anthropic"
	"github.com/savid/acp-go-core/usage/gateway"
	"github.com/savid/acp-go-core/usage/openaicodex"
	"github.com/savid/acp-go-core/usage/opencodego"
	"github.com/savid/acp-go-core/usage/openrouter"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-hermes/internal/hermes"
)

// accountUsage answers _hermes/accountUsage. Hermes exposes no provider
// credentials natively, so a provider is read only through the gateways its
// config routes to. A sessionId, when given, must name a live session but
// does not scope the read.
func (a *Agent) accountUsage(ctx context.Context, params json.RawMessage) (response wire.AccountUsageResponse, err error) {
	request, refusal := wire.DecodeAccountUsageRequest(params, wire.AccountUsageScopeAgent)

	ctx, finish := a.observe.StartACP(ctx, request.Meta, AccountUsageMethod)
	defer func() { finish(err) }()

	if refusal != nil {
		return wire.AccountUsageResponse{}, refusal
	}

	switch request.ProviderID {
	case "":
		return wire.AccountUsageResponse{}, wire.Missing("providerId")
	case anthropic.ProviderID, openaicodex.ProviderID, opencodego.ProviderID, openrouter.ProviderID:
	default:
		return wire.AccountUsageResponse{}, wire.Unsupported("providerId")
	}

	if request.SessionID != "" {
		if _, lookupErr := a.session(ctx, request.SessionID); lookupErr != nil {
			return wire.AccountUsageResponse{}, lookupErr
		}
	}

	readCtx, cancel := context.WithTimeout(ctx, wire.AccountUsageReadTimeout)
	defer cancel()

	response, err = gateway.ReadRoutes(readCtx, a.usageTransport, func(context.Context) ([]gateway.Route, error) {
		env, envErr := a.environment(nil, nil).Build()
		if envErr != nil {
			return nil, envErr
		}

		lookup := func(key string) (string, bool) { return process.Lookup(env, key) }

		return hermes.GatewayRoutes(hermes.AgentDir(a.options.Home, lookup), lookup)
	}, request.ProviderID, wire.AccountUsageUnavailable(wire.AccountUsageNotAuthenticated))
	if err != nil {
		a.log.ErrorContext(ctx, "hermes account usage read failed", slog.String("reason", err.Error()))

		return wire.AccountUsageResponse{}, usage.RequestError(vendor, err)
	}

	return response, nil
}
