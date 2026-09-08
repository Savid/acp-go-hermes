package lifecycle

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"
)

// MetaPath is the request path a rejection names. Negotiation and correlation
// values are rejected as invalid params rather than as stream violations,
// because they are read before any stream exists.
const MetaPath = `_meta["` + MetaKey + `"]`

// Verdict names why a value was refused. The two are distinct facts a host acts
// on differently and are never collapsed: VerdictMissing says a required key was
// left out, VerdictUnsupported says a key or member that is present is refused.
type Verdict string

const (
	// VerdictUnsupported refuses a value that is present — the key on a surface
	// that does not carry it, or a malformed member of one that does.
	VerdictUnsupported Verdict = "unsupported"
	// VerdictMissing refuses the absence of a value the contract requires.
	VerdictMissing Verdict = "missing"
)

// ParamError refuses a negotiation or correlation value. It names the exact
// member path so a host can tell which value it got wrong, and it is the one
// family literal this adapter validates on `initialize` itself.
type ParamError struct {
	// Field is the full request path, from MetaPath down to the offending
	// member.
	Field string
	// Verdict distinguishes a required key the host omitted from a present
	// value that is refused.
	Verdict Verdict
}

// Error implements error.
func (e *ParamError) Error() string { return string(e.Verdict) + " " + e.Field }

func paramError(members ...string) *ParamError {
	var field strings.Builder
	field.WriteString(MetaPath)

	for _, member := range members {
		field.WriteString("." + member)
	}

	return &ParamError{Field: field.String(), Verdict: VerdictUnsupported}
}

// missingParamError refuses the absence of the key on a surface that requires
// it. The path is always the bare key: there is no member to name.
func missingParamError() *ParamError {
	return &ParamError{Field: MetaPath, Verdict: VerdictMissing}
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

	fields, refusal := paramFields(raw)
	if refusal != nil {
		return false, refusal
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

// paramFields preserves number spelling and duplicate members until the owned
// semantic validator runs. Submission keeps its raw object for the same checks
// at its own field path. Embedded Go requests retain their existing map shape.
func paramFields(raw any, members ...string) (map[string]any, *ParamError) {
	if refusal, ok := raw.(*ParamError); ok {
		return nil, refusal
	}

	if fields, ok := raw.(map[string]any); ok {
		return fields, nil
	}

	encoded, ok := raw.(json.RawMessage)
	if !ok || !json.Valid(encoded) {
		return nil, paramError(members...)
	}

	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()

	opening, _ := decoder.Token() // json.Valid proved the token stream.
	if opening != json.Delim('{') {
		return nil, paramError(members...)
	}

	fields := make(map[string]any)

	for decoder.More() {
		token, _ := decoder.Token()

		key, _ := token.(string)
		if _, duplicate := fields[key]; duplicate {
			return nil, paramError(append(members, key)...)
		}

		if key == fieldSubmission && len(members) == 0 {
			var submission json.RawMessage

			_ = decoder.Decode(&submission)
			fields[key] = submission
		} else {
			var value any

			_ = decoder.Decode(&value)
			fields[key] = value
		}
	}

	return fields, nil
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
		return Submission{}, missingParamError()
	}

	fields, refusal := paramFields(raw)
	if refusal != nil {
		return Submission{}, refusal
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

		return int(number), err == nil && int64(int(number)) == number
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
	fields, refusal := paramFields(raw, fieldSubmission)
	if refusal != nil {
		return Submission{}, refusal
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
