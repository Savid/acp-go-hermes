package hermesacp

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/coder/acp-go-sdk"
)

func (a *Agent) handleRateLimits(ctx context.Context, raw json.RawMessage) (RateLimitsResponse, error) {
	request, err := decodeRateLimitsRequest(raw)
	if err != nil {
		return RateLimitsResponse{}, err
	}

	if err := ctx.Err(); err != nil {
		return RateLimitsResponse{}, err
	}

	model := a.options.DefaultModel
	if request.SessionID != "" {
		session, err := a.session(request.SessionID)
		if err != nil {
			return RateLimitsResponse{}, err
		}

		model, err = rateLimitsSessionModel(session)
		if err != nil {
			return RateLimitsResponse{}, err
		}
	}

	provider := request.ProviderID
	if provider == "" {
		selected, modelID, found := strings.Cut(model, "/")
		if !found || strings.TrimSpace(selected) == "" || strings.TrimSpace(modelID) == "" {
			return RateLimitsResponse{}, acp.NewInvalidParams(map[string]any{rateLimitsFieldError: valMissing, rateLimitsFieldField: rateLimitsFieldProviderID})
		}

		provider = selected
	}

	return rateLimitsUnsupported(provider), nil
}

func rateLimitsSessionModel(session *session) (string, error) {
	session.mu.Lock()
	defer session.mu.Unlock()

	if session.closed || session.lifecycleClosing {
		return "", unknownSessionError()
	}

	if session.poisonCause != "" {
		return "", poisonWireError(session.poisonCause)
	}

	return joinModelValue(session.providerID, session.modelID), nil
}
