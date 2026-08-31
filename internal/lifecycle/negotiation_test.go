package lifecycle

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func capabilityMeta(version any) map[string]any {
	return map[string]any{MetaKey: map[string]any{fieldVersion: version}}
}

// TestDecodeCapabilityReportsAbsence pins that the host asking for nothing is
// not a refusal.
func TestDecodeCapabilityReportsAbsence(t *testing.T) {
	t.Parallel()

	offered, refusal := DecodeCapability(nil)
	require.Nil(t, refusal)
	require.False(t, offered)
}

// TestDecodeCapabilityExactVersion pins that every refusal names the exact
// member path and only the current scalar version is accepted.
func TestDecodeCapabilityExactVersion(t *testing.T) {
	t.Parallel()
	for _, version := range []any{1, 1.0, json.Number("1")} {
		offered, refusal := DecodeCapability(capabilityMeta(version))
		require.True(t, offered)
		require.Nil(t, refusal)
	}

	for _, tc := range []struct {
		name  string
		meta  map[string]any
		field string
	}{
		{"non-object", map[string]any{MetaKey: []any{1.0}}, MetaPath},
		{"unknown member", map[string]any{MetaKey: map[string]any{fieldVersion: 1, "activityKinds": []any{}}}, MetaPath + ".activityKinds"},
		{"missing version", map[string]any{MetaKey: map[string]any{}}, MetaPath + ".version"},
		{"other integer", capabilityMeta(2), MetaPath + ".version"},
		{"fractional version", capabilityMeta(1.5), MetaPath + ".version"},
		{"string version", capabilityMeta("1"), MetaPath + ".version"},
		{"boolean version", capabilityMeta(true), MetaPath + ".version"},
		{"array version", capabilityMeta([]any{1}), MetaPath + ".version"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			offered, refusal := DecodeCapability(tc.meta)
			require.False(t, offered)
			require.NotNil(t, refusal)
			require.Equal(t, tc.field, refusal.Field)
			require.Equal(t, "unsupported "+tc.field, refusal.Error())
		})
	}
}

// TestIntegerValueNamesExactlyOneInt pins the reader both surfaces share: a
// spelling is one version only where it names exactly one int. Integrality alone
// is not that — a magnitude past the target int is integral in the float's own
// terms and holdable by nothing — and no wire lexeme survives the SDK's
// pre-decode to `map[string]any`, so this value test is the whole of what the
// integrality rule can mean here.
func TestIntegerValueNamesExactlyOneInt(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		raw   any
		value int
		reads bool
	}{
		{"wire float", 1.0, 1, true},
		{"large holdable float", float64(1 << 53), 1 << 53, true},
		{"negative float", -2.0, -2, true},
		{"host int", 7, 7, true},
		{"preserved number", json.Number("3"), 3, true},
		{"fractional", 1.5, 0, false},
		{"unholdable magnitude", 1e300, 0, false},
		{"unholdable negative magnitude", -1e300, 0, false},
		{"unholdable number", json.Number("1e300"), 0, false},
		{"string", "1", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			value, ok := integerValue(tc.raw)
			require.Equal(t, tc.reads, ok)
			if tc.reads {
				require.Equal(t, tc.value, value)
			}
		})
	}
}

func correlationMeta(value any) map[string]any {
	return map[string]any{MetaKey: value}
}

// TestDecodePromptCorrelationRequiresTheKeyWhileNegotiated pins both halves of
// the presence rule.
func TestDecodePromptCorrelationRequiresTheKeyWhileNegotiated(t *testing.T) {
	t.Parallel()

	negotiated := Negotiated{Version: Version}

	submission, refusal := DecodePromptCorrelation(nil, Negotiated{})
	require.Nil(t, refusal)
	require.Equal(t, Submission{}, submission)

	_, refusal = DecodePromptCorrelation(correlationMeta(map[string]any{}), Negotiated{})
	require.NotNil(t, refusal)
	require.Equal(t, MetaPath, refusal.Field)

	_, refusal = DecodePromptCorrelation(nil, negotiated)
	require.NotNil(t, refusal)
	require.Equal(t, MetaPath, refusal.Field)
}

// TestDecodePromptCorrelationStrictness pins the value's fixed shape: two members
// on the object, three inside the submission, and bounded opaque handles.
func TestDecodePromptCorrelationStrictness(t *testing.T) {
	t.Parallel()

	negotiated := Negotiated{Version: Version}
	submission := map[string]any{"submissionId": "sub-1", "clientNonce": "non-1"}

	for _, tc := range []struct {
		name  string
		value any
		field string
	}{
		{"non-object", "v1", MetaPath},
		{"unknown member", map[string]any{"version": 1.0, "submission": submission, "streamId": "x"}, MetaPath + ".streamId"},
		{"missing version", map[string]any{"submission": submission}, MetaPath + ".version"},
		{"fractional version", map[string]any{"version": 1.5, "submission": submission}, MetaPath + ".version"},
		// No fractional part and still no integer this adapter can hold. The
		// refusal is the integer guard's — what an out-of-range
		// conversion would have produced is implementation-defined, so which
		// exact version it might have collided with is not a thing to reason
		// about. TestIntegerValueNamesExactlyOneInt pins the guard itself.
		{"unholdable version", map[string]any{"version": 1e300, "submission": submission}, MetaPath + ".version"},
		{"unsupported version", map[string]any{"version": 2.0, "submission": submission}, MetaPath + ".version"},
		{"missing submission", map[string]any{"version": 1.0}, MetaPath + ".submission"},
		{"unknown submission member", map[string]any{"version": 1.0, "submission": map[string]any{
			"submissionId": "sub-1", "clientNonce": "non-1", "turnId": "t",
		}}, MetaPath + ".submission.turnId"},
		{"missing submission id", map[string]any{"version": 1.0, "submission": map[string]any{
			"clientNonce": "non-1",
		}}, MetaPath + ".submission.submissionId"},
		{"empty submission id", map[string]any{"version": 1.0, "submission": map[string]any{
			"submissionId": "", "clientNonce": "non-1",
		}}, MetaPath + ".submission.submissionId"},
		{"non-string nonce", map[string]any{"version": 1.0, "submission": map[string]any{
			"submissionId": "sub-1", "clientNonce": 1.0,
		}}, MetaPath + ".submission.clientNonce"},
		{"over-bound run id", map[string]any{"version": 1.0, "submission": map[string]any{
			"submissionId": "sub-1", "clientNonce": "non-1", "runId": strings.Repeat("r", IdentifierBound+1),
		}}, MetaPath + ".submission.runId"},
		{"empty run id", map[string]any{"version": 1.0, "submission": map[string]any{
			"submissionId": "sub-1", "clientNonce": "non-1", "runId": "",
		}}, MetaPath + ".submission.runId"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, refusal := DecodePromptCorrelation(correlationMeta(tc.value), negotiated)
			require.NotNil(t, refusal)
			require.Equal(t, tc.field, refusal.Field)
		})
	}
}

// TestDecodePromptCorrelationReadsTheWholeSubmission pins that an optional run id
// is read when present and left empty when absent.
func TestDecodePromptCorrelationReadsTheWholeSubmission(t *testing.T) {
	t.Parallel()

	negotiated := Negotiated{Version: Version}

	submission, refusal := DecodePromptCorrelation(correlationMeta(map[string]any{
		"version":    1.0,
		"submission": map[string]any{"submissionId": "sub-1", "clientNonce": "non-1", "runId": "run-1"},
	}), negotiated)
	require.Nil(t, refusal)
	require.Equal(t, Submission{SubmissionID: "sub-1", ClientNonce: "non-1", RunID: "run-1"}, submission)

	submission, refusal = DecodePromptCorrelation(correlationMeta(map[string]any{
		"version":    1,
		"submission": map[string]any{"submissionId": "sub-1", "clientNonce": "non-1"},
	}), negotiated)
	require.Nil(t, refusal)
	require.Equal(t, Submission{SubmissionID: "sub-1", ClientNonce: "non-1"}, submission)
}
