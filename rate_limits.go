package hermesacp

import (
	"bytes"
	"encoding/json"
	"maps"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/coder/acp-go-sdk"
)

// RateLimitsMethod reads quota observations for an effective provider context.
const RateLimitsMethod = "_hermes/rateLimits"

const (
	rateLimitsFieldParams             = "params"
	rateLimitsFieldProviderID         = "providerId"
	rateLimitsFieldSessionID          = "sessionId"
	rateLimitsFieldError              = "error"
	rateLimitsFieldField              = "field"
	rateLimitsAvailabilityUnsupported = "unsupported"
	rateLimitsAvailabilityUnavailable = "unavailable"
)

// RateLimitsRequest selects the provider and optional live-session configuration for a quota read.
type RateLimitsRequest struct {
	ProviderID string        `json:"providerId,omitempty"`
	SessionID  acp.SessionId `json:"sessionId,omitempty"`
}

// RateLimitsResponse reports provider quota observations and their availability.
type RateLimitsResponse struct {
	ProviderID   string          `json:"providerId"`
	Availability string          `json:"availability"`
	Reason       string          `json:"reason,omitempty"`
	Pools        []RateLimitPool `json:"pools"`
}

// RateLimitPool groups the windows and monetary balances of one native allowance.
type RateLimitPool struct {
	ID       string             `json:"id"`
	Label    string             `json:"label,omitempty"`
	PlanType string             `json:"planType,omitempty"`
	Windows  []RateLimitWindow  `json:"windows"`
	Balances []RateLimitBalance `json:"balances,omitempty"`
}

// RateLimitWindow reports source-observed utilization or status for one quota window.
type RateLimitWindow struct {
	ID              string   `json:"id"`
	UsedPercent     *float64 `json:"usedPercent,omitempty"`
	DurationSeconds *int64   `json:"durationSeconds,omitempty"`
	Status          string   `json:"status,omitempty"`
	ObservedAt      string   `json:"observedAt"`
	ResetsAt        string   `json:"resetsAt,omitempty"`
}

// RateLimitBalance reports monetary observations for one allowance or account balance.
type RateLimitBalance struct {
	ID            string          `json:"id"`
	Used          *RateLimitMoney `json:"used,omitempty"`
	Limit         *RateLimitMoney `json:"limit,omitempty"`
	Remaining     *RateLimitMoney `json:"remaining,omitempty"`
	Uncapped      bool            `json:"uncapped,omitempty"`
	ResetInterval string          `json:"resetInterval,omitempty"`
	ObservedAt    string          `json:"observedAt"`
	ResetsAt      string          `json:"resetsAt,omitempty"`
}

// RateLimitMoney expresses an amount in major units of its uppercase currency code.
type RateLimitMoney struct {
	Amount   float64 `json:"amount"`
	Currency string  `json:"currency"`
}

func rateLimitsUnsupported(providerID string) RateLimitsResponse {
	return RateLimitsResponse{ProviderID: providerID, Availability: rateLimitsAvailabilityUnsupported, Pools: []RateLimitPool{}}
}

func rateLimitsUnavailable(providerID, reason string) RateLimitsResponse {
	return RateLimitsResponse{ProviderID: providerID, Availability: rateLimitsAvailabilityUnavailable, Reason: reason, Pools: []RateLimitPool{}}
}

func rateLimitsInvalid(field string) error {
	return acp.NewInvalidParams(map[string]any{rateLimitsFieldError: rateLimitsAvailabilityUnsupported, rateLimitsFieldField: field})
}

func decodeRateLimitsRequest(raw json.RawMessage) (RateLimitsRequest, error) {
	if len(raw) == 0 {
		return RateLimitsRequest{}, nil
	}

	if !json.Valid(raw) || !rateLimitsValidUnicode(raw) {
		return RateLimitsRequest{}, rateLimitsInvalid(rateLimitsFieldParams)
	}

	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return RateLimitsRequest{}, nil
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))

	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return RateLimitsRequest{}, rateLimitsInvalid(rateLimitsFieldParams)
	}

	fields, err := decodeRateLimitsFields(decoder)
	if err != nil {
		return RateLimitsRequest{}, err
	}

	keys := slices.Sorted(maps.Keys(fields))
	for _, key := range keys {
		if key != rateLimitsFieldProviderID && key != rateLimitsFieldSessionID {
			return RateLimitsRequest{}, rateLimitsInvalid(key)
		}
	}

	values := make(map[string]string, len(fields))

	for _, key := range keys {
		var value string
		if err := json.Unmarshal(fields[key], &value); err != nil || strings.TrimSpace(value) == "" {
			return RateLimitsRequest{}, rateLimitsInvalid(key)
		}

		values[key] = value
	}

	return RateLimitsRequest{ProviderID: values[rateLimitsFieldProviderID], SessionID: acp.SessionId(values[rateLimitsFieldSessionID])}, nil
}

func decodeRateLimitsFields(decoder *json.Decoder) (map[string]json.RawMessage, error) {
	fields := make(map[string]json.RawMessage, 2)

	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, rateLimitsInvalid(rateLimitsFieldParams)
		}

		key, ok := token.(string)
		if !ok {
			return nil, rateLimitsInvalid(rateLimitsFieldParams)
		}

		if _, duplicate := fields[key]; duplicate {
			return nil, rateLimitsInvalid(key)
		}

		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, rateLimitsInvalid(rateLimitsFieldParams)
		}

		fields[key] = value
	}

	return fields, nil
}

// encoding/json repairs malformed Unicode; quota selectors must retain exact
// identity instead. JSON syntax is checked before this scalar-value scan.
func rateLimitsValidUnicode(raw []byte) bool {
	if !utf8.Valid(raw) {
		return false
	}

	for index := 0; index < len(raw); index++ {
		if raw[index] != '\\' {
			continue
		}

		index++
		if index >= len(raw) {
			return false
		}

		if raw[index] != 'u' {
			continue
		}

		following, valid := rateLimitsUnicodeEscape(raw, index)
		if !valid {
			return false
		}

		index = following
	}

	return true
}

func rateLimitsUnicodeEscape(raw []byte, index int) (int, bool) {
	if len(raw)-index < 5 {
		return index, false
	}

	value, err := strconv.ParseUint(string(raw[index+1:index+5]), 16, 16)
	if err != nil || value >= 0xdc00 && value <= 0xdfff {
		return index, false
	}

	if value < 0xd800 || value > 0xdbff {
		return index + 4, true
	}

	if len(raw)-index < 11 || raw[index+5] != '\\' || raw[index+6] != 'u' {
		return index, false
	}

	low, err := strconv.ParseUint(string(raw[index+7:index+11]), 16, 16)

	return index + 10, err == nil && low >= 0xdc00 && low <= 0xdfff
}
