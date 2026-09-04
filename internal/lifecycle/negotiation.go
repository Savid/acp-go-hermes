package lifecycle

import (
	"encoding/json"
	"math"
)

// MetaPath is the request path a rejection names. Negotiation and correlation
// values are rejected as invalid params rather than as stream violations,
// because they are read before any stream exists.
const MetaPath = `_meta["` + MetaKey + `"]`

// ParamError refuses a negotiation or correlation value. It names the exact
// member path so a host can tell which value it got wrong, and it is the one
// family literal this adapter validates on `initialize` itself.
type ParamError struct {
	// Field is the full request path, from MetaPath down to the offending
	// member.
	Field string
}

// Error implements error.
func (e *ParamError) Error() string { return "unsupported " + e.Field }

func paramError(members ...string) *ParamError {
	field := MetaPath
	for _, member := range members {
		field += "." + member
	}

	return &ParamError{Field: field}
}

// DecodeCapability reads the capability from `InitializeRequest._meta`. An absent value is
// reported as not present rather than as a refusal: the host asked for nothing,
// and the answer, every envelope, and every correlation read are then omitted for
// the whole connection.
func DecodeCapability(meta map[string]any) (bool, *ParamError) {
	raw, present := meta[MetaKey]
	if !present {
		return false, nil
	}

	fields, ok := raw.(map[string]any)
	if !ok {
		return false, paramError()
	}

	for key := range fields {
		if key != fieldVersion {
			return false, paramError(key)
		}
	}

	version, ok := integerValue(fields[fieldVersion])
	if !ok || version != Version {
		return false, paramError(fieldVersion)
	}

	return true, nil
}

// Submission names one accepted client prompt. The client nonce is the host's own
// input identity: it is distinct from every JSON-RPC message id, and neither is
// ever substituted for the other.
type Submission struct {
	SubmissionID string
	ClientNonce  string
	RunID        string
}

// DecodePromptCorrelation reads the value a `session/prompt` carries while
// version 1 is negotiated. The key is required when negotiated and forbidden when
// not, and either way the verdict is reached before the prompt is dispatched, so
// no frame is written to the harness.
func DecodePromptCorrelation(meta map[string]any, negotiated Negotiated) (Submission, *ParamError) {
	raw, present := meta[MetaKey]

	switch {
	case !negotiated.Present() && present:
		return Submission{}, paramError()
	case !negotiated.Present():
		return Submission{}, nil
	case !present:
		return Submission{}, paramError()
	}

	fields, ok := raw.(map[string]any)
	if !ok {
		return Submission{}, paramError()
	}

	for key := range fields {
		if key != fieldVersion && key != fieldSubmission {
			return Submission{}, paramError(key)
		}
	}

	if refusal := checkCorrelationVersion(fields, negotiated); refusal != nil {
		return Submission{}, refusal
	}

	return decodeSubmission(fields[fieldSubmission])
}

func checkCorrelationVersion(fields map[string]any, negotiated Negotiated) *ParamError {
	version, ok := integerValue(fields[fieldVersion])
	if !ok || version != Version || !negotiated.Present() {
		return paramError(fieldVersion)
	}

	return nil
}

// integerValue reads one JSON integer. A decoded wire value arrives as a float64
// and an embedding Go host writes an int, so both are the same integer; a
// fractional value is neither, and neither is one no int can hold.
func integerValue(raw any) (int, bool) {
	switch value := raw.(type) {
	case float64:
		return integerFromFloat(value)
	case int:
		return value, true
	case json.Number:
		number, err := value.Int64()

		return int(number), err == nil
	default:
		return 0, false
	}
}

// integerFromFloat accepts a float64 only where it names exactly one int.
// Integrality is not enough: a magnitude like 1e300 has no fractional part and
// still names no int, and the conversion an out-of-range value would take is
// implementation-defined, so the range is judged before the conversion rather
// than after it. Inside 2^63 an integral float64 is exact as an int64, and the
// round trip back to float64 judges int itself, which is the narrower type on a
// 32-bit platform.
func integerFromFloat(value float64) (int, bool) {
	const intBound = float64(1<<62) * 2

	if value != math.Trunc(value) || value >= intBound || value < -intBound {
		return 0, false
	}

	converted := int(int64(value))

	return converted, float64(converted) == value
}

func decodeSubmission(raw any) (Submission, *ParamError) {
	fields, ok := raw.(map[string]any)
	if !ok {
		return Submission{}, paramError(fieldSubmission)
	}

	for key := range fields {
		if key != fieldSubmissionID && key != fieldClientNonce && key != fieldRunID {
			return Submission{}, paramError(fieldSubmission, key)
		}
	}

	submission := Submission{}

	for _, member := range []struct {
		key      string
		target   *string
		required bool
	}{
		{fieldSubmissionID, &submission.SubmissionID, true},
		{fieldClientNonce, &submission.ClientNonce, true},
		{fieldRunID, &submission.RunID, false},
	} {
		value, refusal := correlationIdentifier(fields, member.key, member.required)
		if refusal != nil {
			return Submission{}, refusal
		}

		*member.target = value
	}

	return submission, nil
}

// correlationIdentifier reads one opaque handle. An identifier is a correlation
// handle, not a payload: it is bounded, and it is never empty — an optional one
// is omitted rather than emptied, so a member present carrying the empty string
// is malformed rather than absent.
func correlationIdentifier(fields map[string]any, key string, required bool) (string, *ParamError) {
	raw, present := fields[key]
	if !present {
		if required {
			return "", paramError(fieldSubmission, key)
		}

		return "", nil
	}

	value, ok := raw.(string)
	if !ok || value == "" || len(value) > IdentifierBound {
		return "", paramError(fieldSubmission, key)
	}

	return value, nil
}

// ActionCorrelation renders the value stamped on every
// `session/request_permission` and `elicitation/create` while version 1 is
// negotiated. It names the emitting stream and the action's lifecycle identity;
// it is never a routing or authenticating envelope, so the reserved route object
// keeps that job wherever it already had it.
//
// The sibling registers the inbound request against actionId before the ordered
// `action_update` announcing it is emitted, so a host can never see an action id
// it cannot yet answer.
func ActionCorrelation(streamID string, action ActionUpdate) map[string]any {
	correlation := map[string]any{
		fieldActionID: action.ActionID,
		fieldOwner: map[string]any{
			fieldType: string(action.Owner.Type),
			fieldID:   action.Owner.ID,
		},
	}
	withOptional(correlation, fieldRunID, action.RunID)

	return map[string]any{
		fieldVersion:  Version,
		fieldStreamID: streamID,
		fieldAction:   correlation,
	}
}
