package hermes

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"

	"github.com/savid/acp-go-core/wire"
)

// UsageAccess keeps credential material inside the adapter.
type UsageAccess struct {
	APIKey      string
	AccountID   string
	Reason      string
	Fingerprint [32]byte
}

//nolint:tagliatelle // Native gateway members use snake_case.
type providerAccess struct {
	Configured *bool             `json:"configured"`
	Provider   string            `json:"provider"`
	API        string            `json:"api_mode"`
	BaseURL    string            `json:"base_url"`
	APIKey     string            `json:"api_key"`
	AccountID  string            `json:"account_id"`
	Headers    map[string]string `json:"headers"`
}

func (c *Client) ProviderAccess(ctx context.Context, sessionID, providerID string) (UsageAccess, error) {
	var raw json.RawMessage
	if err := c.Call(ctx, "session.provider_access", map[string]any{fieldSessionID: sessionID, "provider": providerID}, &raw); err != nil {
		return UsageAccess{}, errors.New("native provider access failed")
	}

	var access providerAccess
	if json.Unmarshal(raw, &access) != nil || access.Configured == nil {
		return UsageAccess{}, errors.New("native provider access invalid")
	}

	if !*access.Configured || strings.TrimSpace(access.APIKey) == "" {
		return UsageAccess{Reason: wire.AccountUsageNotAuthenticated}, nil
	}

	if !access.official(providerID) {
		return UsageAccess{Reason: wire.AccountUsageNotReported}, nil
	}

	return UsageAccess{APIKey: access.APIKey, AccountID: access.AccountID, Fingerprint: sha256.Sum256(raw)}, nil
}

func (a providerAccess) official(providerID string) bool {
	if a.Provider != providerID {
		return false
	}

	base := strings.TrimSuffix(a.BaseURL, "/")

	switch providerID {
	case "openai-codex":
		if a.API != "codex_responses" || base != "https://chatgpt.com/backend-api/codex" || a.AccountID == "" {
			return false
		}
	case "anthropic":
		if a.API != "anthropic_messages" || strings.TrimSuffix(base, "/v1") != "https://api.anthropic.com" {
			return false
		}
	case "openrouter", "opencode-go":
		want := "https://openrouter.ai/api"
		if providerID == "opencode-go" {
			want = "https://opencode.ai/zen/go"
		}

		switch a.API {
		case "anthropic_messages":
		case "chat_completions", "responses":
			want += "/v1"
		default:
			return false
		}

		if base != want {
			return false
		}
	default:
		return false
	}

	for name, value := range a.Headers {
		switch strings.ToLower(name) {
		case "authorization":
			if value != "Bearer "+a.APIKey {
				return false
			}
		case "x-api-key":
			if value != a.APIKey {
				return false
			}
		case "chatgpt-account-id":
			if providerID != "openai-codex" || value != a.AccountID {
				return false
			}
		case "http-referer", "x-title", "x-openrouter-categories", "user-agent", "originator", "openai-beta":
		default:
			return false
		}
	}

	return true
}
