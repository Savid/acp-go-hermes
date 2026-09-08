package hermesacp

import (
	"bytes"
	"encoding/json"
	"slices"
	"strconv"
	"strings"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-hermes/internal/lifecycle"
)

// prepareWireMetadata masks only owned metadata before SDK decoding. This keeps
// even an out-of-float-range number at its semantic validation stage. Foreign
// metadata and non-metadata request fields keep the SDK's existing behavior.
func prepareWireMetadata(params json.RawMessage) json.RawMessage {
	if !json.Valid(params) {
		return params
	}

	return rewriteWireObject(params, func(name string, raw json.RawMessage) json.RawMessage {
		switch {
		case strings.EqualFold(name, "_meta"):
			return maskWireMetadata(raw, lifecycle.MetaKey, routeMetaKey)
		case strings.EqualFold(name, "prompt"):
			var blocks []json.RawMessage
			if json.Unmarshal(raw, &blocks) != nil || blocks == nil {
				return raw
			}

			for i, block := range blocks {
				blocks[i] = rewriteWireObject(block, func(field string, value json.RawMessage) json.RawMessage {
					if strings.EqualFold(field, "_meta") {
						return maskWireMetadata(value, handoffMetaKey)
					}

					return value
				})
			}

			encoded, _ := json.Marshal(blocks)

			return encoded
		default:
			return raw
		}
	})
}

func maskWireMetadata(raw json.RawMessage, keys ...string) json.RawMessage {
	return rewriteWireObject(raw, func(name string, value json.RawMessage) json.RawMessage {
		if slices.Contains(keys, name) {
			return json.RawMessage("null")
		}

		return value
	})
}

// rewriteWireObject preserves ordered occurrences instead of passing through a
// map that discards duplicates. Its callers supply validated JSON; non-objects
// retain their original shape for the SDK or the owned validator to refuse.
func rewriteWireObject(raw json.RawMessage, rewrite func(string, json.RawMessage) json.RawMessage) json.RawMessage {
	decoder := json.NewDecoder(bytes.NewReader(raw))

	opening, _ := decoder.Token()
	if opening != json.Delim('{') {
		return raw
	}

	var output bytes.Buffer
	output.WriteByte('{')

	for decoder.More() {
		token, _ := decoder.Token()
		name, _ := token.(string)

		var value json.RawMessage

		_ = decoder.Decode(&value)

		value = rewrite(name, value)
		if value == nil {
			continue
		}

		if output.Len() > 1 {
			output.WriteByte(',')
		}

		key, _ := json.Marshal(name)
		output.Write(key)
		output.WriteByte(':')
		output.Write(value)
	}

	output.WriteByte('}')

	return output.Bytes()
}

func appendWireMember(raw json.RawMessage, key string, value json.RawMessage) json.RawMessage {
	object := rewriteWireObject(raw, func(name string, member json.RawMessage) json.RawMessage {
		if name == key {
			return nil
		}

		return member
	})

	object = object[:len(object)-1]
	if len(object) > 1 {
		object = append(object, ',')
	}

	encodedKey, _ := json.Marshal(key)
	object = append(object, encodedKey...)
	object = append(object, ':')
	object = append(object, value...)

	return append(object, '}')
}

func restoreWireMetadata(params json.RawMessage, request any) {
	var raw struct {
		Meta   map[string]json.RawMessage `json:"_meta"` //nolint:tagliatelle // ACP wire spelling.
		Prompt []struct {
			Meta map[string]json.RawMessage `json:"_meta"` //nolint:tagliatelle // ACP wire spelling.
		} `json:"prompt"`
	}

	_ = json.Unmarshal(params, &raw) // Owned numbers remain raw here.
	if meta := requestMeta(request); meta != nil {
		preserveRequestLifecycle(params, meta)
		restoreWireNumberValue(*meta, routeMetaKey, raw.Meta[routeMetaKey])
	}

	if prompt, ok := request.(*acp.PromptRequest); ok {
		for i, block := range prompt.Prompt {
			if block.Image != nil && i < len(raw.Prompt) {
				restoreWireNumberValue(block.Image.Meta, handoffMetaKey, raw.Prompt[i].Meta[handoffMetaKey])
			}
		}
	}
}

func restoreWireNumberValue(meta map[string]any, key string, raw json.RawMessage) {
	if _, present := meta[key]; !present || raw == nil {
		return
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()

	var value any

	_ = decoder.Decode(&value)
	meta[key] = value
}

// exactWireInteger reads a numeric value without rounding its decimal token.
// Route and handoff permit integral decimal/exponent spellings. The bound is
// checked before allocating zero padding, even for a very large exponent.
func exactWireInteger(number json.Number) (int64, bool) {
	raw := string(number)
	if !json.Valid([]byte(raw)) {
		return 0, false
	}

	negative := strings.HasPrefix(raw, "-")
	raw = strings.TrimPrefix(raw, "-")
	mantissa, exponent, hasExponent := strings.Cut(strings.ToLower(raw), "e")
	whole, fraction, _ := strings.Cut(mantissa, ".")

	digits := whole + fraction
	for _, digit := range digits {
		if digit < '0' || digit > '9' {
			return 0, false
		}
	}

	digits = strings.TrimLeft(digits, "0")
	if digits == "" {
		return 0, true
	}

	var power int64

	if hasExponent {
		var err error

		power, err = strconv.ParseInt(exponent, 10, 32)
		if err != nil {
			return 0, false
		}
	}

	power -= int64(len(fraction))
	if power < 0 {
		cut := int64(len(digits)) + power
		if cut <= 0 || strings.Trim(digits[cut:], "0") != "" {
			return 0, false
		}

		digits = digits[:cut]
	} else {
		if int64(len(digits))+power > 19 {
			return 0, false
		}

		digits += strings.Repeat("0", int(power))
	}

	if negative {
		digits = "-" + digits
	}

	value, err := strconv.ParseInt(digits, 10, 64)

	return value, err == nil
}

// preserveRequestLifecycle retains only owned wire metadata after typed request
// validation. Semantic handlers still decide construction, route and lifecycle
// precedence; foreign metadata retains the SDK's decoding behavior.
//
//nolint:gocritic // Replaces an SDK map erased by a later duplicate envelope.
func preserveRequestLifecycle(params json.RawMessage, meta *map[string]any) {
	if meta == nil {
		return
	}

	decoder := json.NewDecoder(bytes.NewReader(params))

	opening, _ := decoder.Token() // The typed request decode proved valid JSON.
	if opening != json.Delim('{') {
		return
	}

	var offer json.RawMessage

	metaCount, ownedCount := 0, 0

	for decoder.More() {
		key, _ := decoder.Token()

		var value json.RawMessage

		_ = decoder.Decode(&value)

		if name, ok := key.(string); !ok || !strings.EqualFold(name, "_meta") {
			continue
		}

		metaCount++
		metadata := json.NewDecoder(bytes.NewReader(value))

		start, _ := metadata.Token()
		if start != json.Delim('{') {
			continue
		}

		for metadata.More() {
			name, _ := metadata.Token()

			var member json.RawMessage

			_ = metadata.Decode(&member)

			if name == lifecycle.MetaKey {
				ownedCount++
				offer = member
			}
		}
	}

	if ownedCount == 0 {
		return
	}

	if *meta == nil {
		*meta = make(map[string]any)
	}

	if metaCount > 1 || ownedCount > 1 {
		(*meta)[lifecycle.MetaKey] = &lifecycle.ParamError{
			Field: lifecycle.MetaPath, Verdict: lifecycle.VerdictUnsupported,
		}

		return
	}

	(*meta)[lifecycle.MetaKey] = offer
}

//nolint:gocritic // Returns the exact SDK field so an erased map can be replaced.
func requestMeta(value any) *map[string]any {
	switch request := value.(type) {
	case *acp.InitializeRequest:
		return &request.Meta
	case *acp.AuthenticateRequest:
		return &request.Meta
	case *acp.LogoutRequest:
		return &request.Meta
	case *acp.CancelNotification:
		return &request.Meta
	case *acp.CloseSessionRequest:
		return &request.Meta
	case *acp.UnstableDeleteSessionRequest:
		return &request.Meta
	case *acp.ListSessionsRequest:
		return &request.Meta
	case *acp.LoadSessionRequest:
		return &request.Meta
	case *acp.NewSessionRequest:
		return &request.Meta
	case *acp.PromptRequest:
		return &request.Meta
	case *acp.ResumeSessionRequest:
		return &request.Meta
	case *acp.SetSessionConfigOptionRequest:
		if request.Boolean != nil {
			return &request.Boolean.Meta
		}

		if request.ValueId != nil {
			return &request.ValueId.Meta
		}

		return nil
	case *acp.SetSessionModeRequest:
		return &request.Meta
	default:
		return nil
	}
}
