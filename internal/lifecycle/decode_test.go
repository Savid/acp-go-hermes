package lifecycle

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// richConfiguration answers every fact this package can validate, so a decoder
// refusal in these vectors is structural rather than a fact the answer withheld.
func richConfiguration() Negotiated {
	return Negotiated{
		Versions:                []int{Version},
		UpdatesOutsidePrompt:    true,
		AuthoritativeQuiescence: true,
		QuiescenceSource:        ProofClassProcessContainment,
		ActivityKinds: []ActivityKind{
			ActivityTask, ActivityMonitor, ActivitySubagent, ActivityGoal, ActivityOther,
		},
	}
}

// notification frames one envelope on the eligible carrier.
func notification(envelope string) string {
	return `{"sessionId":"s","update":{"sessionUpdate":"session_info_update"},"_meta":{"` + MetaKey + `":` + envelope + `}}`
}

// enveloped wraps one event in a well-formed envelope at sequence 1.
func enveloped(event string) string {
	return notification(`{"version":1,"streamId":"strm","sequence":1,"event":` + event + `}`)
}

func requireRefusal(t *testing.T, params string, negotiated Negotiated, kind ViolationKind) {
	t.Helper()

	_, err := DecodeSessionUpdate(json.RawMessage(params), negotiated)

	var refusal *ViolationError

	require.ErrorAs(t, err, &refusal, params)
	require.Equal(t, kind, refusal.Kind, params)
}

// TestDecodeReportsNoEnvelopeForOrdinaryContent pins that ordinary content is not
// a violation: it rides its own surfaces without an envelope.
func TestDecodeReportsNoEnvelopeForOrdinaryContent(t *testing.T) {
	t.Parallel()

	for _, params := range []string{
		`{"sessionId":"s","update":{"sessionUpdate":"agent_message_chunk"}}`,
		`{"sessionId":"s","update":{"sessionUpdate":"agent_message_chunk"},"_meta":{"amp":{}}}`,
	} {
		_, err := DecodeSessionUpdate(json.RawMessage(params), richConfiguration())
		require.ErrorIs(t, err, ErrNoEnvelope, params)
	}
}

// TestDecodeRefusesAnUnframedNotification pins that a payload this decoder cannot
// even read as a notification is malformed before anything else is judged.
func TestDecodeRefusesAnUnframedNotification(t *testing.T) {
	t.Parallel()

	requireRefusal(t, `{`, richConfiguration(), ViolationMalformedEnvelope)
	requireRefusal(t, `[1,2]`, richConfiguration(), ViolationMalformedEnvelope)
}

// TestDecodeRefusesEveryEnvelopeWhenTheKeyWasNotAnswered pins that an envelope on
// a connection where the key was omitted asserts a fact the answer never claimed.
func TestDecodeRefusesEveryEnvelopeWhenTheKeyWasNotAnswered(t *testing.T) {
	t.Parallel()

	requireRefusal(t, enveloped(`{"type":"prompt_accepted","submissionId":"a","clientNonce":"b","turnId":"c"}`),
		Negotiated{}, ViolationUnnegotiatedFact)
}

// TestDecodeCarrierRule pins the closed positive set: the identity-only
// session_info_update is the only eligible carrier, and it sets neither title nor
// updatedAt because a carrier must mutate no state.
func TestDecodeCarrierRule(t *testing.T) {
	t.Parallel()

	envelope := `{"version":1,"streamId":"strm","sequence":1,"event":{"type":"prompt_accepted","submissionId":"a","clientNonce":"b","turnId":"c"}}`

	for _, update := range []string{
		`{"sessionUpdate":"agent_message_chunk"}`,
		`{"sessionUpdate":"tool_call_update"}`,
		`{"sessionUpdate":"session_info_update","title":"named"}`,
		`{"sessionUpdate":"session_info_update","updatedAt":"now"}`,
		`{"sessionUpdate":7}`,
		`"session_info_update"`,
	} {
		requireRefusal(t,
			`{"sessionId":"s","update":`+update+`,"_meta":{"`+MetaKey+`":`+envelope+`}}`,
			richConfiguration(), ViolationIllegalCarrier)
	}
}

// TestDecodeEnvelopeStructure pins the four fixed members. Structural validity is
// checked before ordering, so none of these ever reports an ordering token.
func TestDecodeEnvelopeStructure(t *testing.T) {
	t.Parallel()

	event := `{"type":"prompt_accepted","submissionId":"a","clientNonce":"b","turnId":"c"}`

	for _, tc := range []struct {
		envelope string
		kind     ViolationKind
	}{
		{`["array"]`, ViolationIllegalCarrier},
		{`"string"`, ViolationIllegalCarrier},
		{`{"version":1,"sequence":1,"event":` + event + `}`, ViolationMalformedEnvelope},
		{`{"version":1,"streamId":7,"sequence":1,"event":` + event + `}`, ViolationMalformedEnvelope},
		{`{"version":1,"streamId":"","sequence":1,"event":` + event + `}`, ViolationMalformedEnvelope},
		{`{"version":1,"streamId":"` + strings.Repeat("s", IdentifierBound+1) + `","sequence":1,"event":` + event + `}`, ViolationMalformedEnvelope},
		{`{"version":1,"streamId":"strm","event":` + event + `}`, ViolationMalformedEnvelope},
		{`{"version":1,"streamId":"strm","sequence":0,"event":` + event + `}`, ViolationMalformedEnvelope},
		{`{"version":1,"streamId":"strm","sequence":"1","event":` + event + `}`, ViolationMalformedEnvelope},
		{`{"streamId":"strm","sequence":1,"event":` + event + `}`, ViolationMalformedEnvelope},
		{`{"version":"1","streamId":"strm","sequence":1,"event":` + event + `}`, ViolationMalformedEnvelope},
		{`{"version":2,"streamId":"strm","sequence":1,"event":` + event + `}`, ViolationUnsupportedVersion},
		{`{"version":1,"streamId":"strm","sequence":1,"event":` + event + `,"extra":1}`, ViolationUnknownField},
		{`{"version":1,"streamId":"strm","sequence":1}`, ViolationMalformedEnvelope},
		{`{"version":1,"streamId":"strm","sequence":1,"event":[]}`, ViolationMalformedEnvelope},
		{`{"version":1,"streamId":"strm","sequence":1,"event":{}}`, ViolationMalformedEnvelope},
		{`{"version":1,"streamId":"strm","sequence":1,"event":{"type":"goal_update"}}`, ViolationUnknownEventType},
	} {
		requireRefusal(t, notification(tc.envelope), richConfiguration(), tc.kind)
	}
}

// TestDecodeEventStructure pins every event object's fixed member set and the
// closed vocabularies inside it.
func TestDecodeEventStructure(t *testing.T) {
	t.Parallel()

	quiescence := `{"quiescent":true,"source":"process-containment","watermark":0}`

	for _, tc := range []struct {
		event string
		kind  ViolationKind
	}{
		// lifecycle_snapshot
		{`{"type":"lifecycle_snapshot","foreground":{"state":"idle","cycleId":"c"},"activities":[],"actions":[],"quiescence":` + quiescence + `,"extra":1}`, ViolationUnknownField},
		{`{"type":"lifecycle_snapshot","activities":[],"actions":[],"quiescence":` + quiescence + `}`, ViolationMalformedEnvelope},
		{`{"type":"lifecycle_snapshot","foreground":{"state":"idle","cycleId":"c","extra":1},"activities":[],"actions":[],"quiescence":` + quiescence + `}`, ViolationUnknownField},
		{`{"type":"lifecycle_snapshot","foreground":{"state":"waiting","cycleId":"c"},"activities":[],"actions":[],"quiescence":` + quiescence + `}`, ViolationMalformedEnvelope},
		{`{"type":"lifecycle_snapshot","foreground":{"state":"idle"},"activities":[],"actions":[],"quiescence":` + quiescence + `}`, ViolationMalformedEnvelope},
		{`{"type":"lifecycle_snapshot","foreground":{"state":"idle","cycleId":"c"},"actions":[],"quiescence":` + quiescence + `}`, ViolationMalformedEnvelope},
		{`{"type":"lifecycle_snapshot","foreground":{"state":"idle","cycleId":"c"},"activities":{},"actions":[],"quiescence":` + quiescence + `}`, ViolationMalformedEnvelope},
		{`{"type":"lifecycle_snapshot","foreground":{"state":"idle","cycleId":"c"},"activities":[],"quiescence":` + quiescence + `}`, ViolationMalformedEnvelope},
		{`{"type":"lifecycle_snapshot","foreground":{"state":"idle","cycleId":"c"},"activities":[],"actions":[]}`, ViolationMalformedEnvelope},
		{`{"type":"lifecycle_snapshot","foreground":{"state":"idle","cycleId":"c"},"activities":[],"actions":[],"quiescence":{"quiescent":true,"source":"process-containment","watermark":0,"extra":1}}`, ViolationUnknownField},
		{`{"type":"lifecycle_snapshot","foreground":{"state":"idle","cycleId":"c"},"activities":[{"activityId":"a"}],"actions":[],"quiescence":` + quiescence + `}`, ViolationMalformedEnvelope},
		{`{"type":"lifecycle_snapshot","foreground":{"state":"idle","cycleId":"c"},"activities":[],"actions":[{"actionId":"x"}],"quiescence":` + quiescence + `}`, ViolationMalformedEnvelope},

		// prompt_accepted
		{`{"type":"prompt_accepted","submissionId":"a","clientNonce":"b","turnId":"c","extra":1}`, ViolationUnknownField},
		{`{"type":"prompt_accepted","clientNonce":"b","turnId":"c"}`, ViolationMalformedEnvelope},

		// state_update
		{`{"type":"state_update","state":"running","cycleId":"c","turnId":"t","cause":"submission","extra":1}`, ViolationUnknownField},
		{`{"type":"state_update","state":"waiting","cycleId":"c","turnId":"t","cause":"submission"}`, ViolationMalformedEnvelope},
		{`{"type":"state_update","state":"running","cycleId":"c","turnId":"t","cause":"whim"}`, ViolationMalformedEnvelope},
		{`{"type":"state_update","state":"running","cycleId":"c","cause":"submission"}`, ViolationMalformedEnvelope},
		{`{"type":"state_update","state":"running","cycleId":"c","turnId":"t","cause":"submission","stopReason":"end_turn"}`, ViolationMalformedEnvelope},
		{`{"type":"state_update","state":"idle","cycleId":"c","turnId":"t","cause":"submission","stopReason":"stop"}`, ViolationMalformedEnvelope},
		{`{"type":"state_update","state":"idle","cycleId":"c","turnId":"t","cause":"submission","outcome":"partial"}`, ViolationMalformedEnvelope},

		// activity_update
		{`{"type":"activity_update","activity":{"activityId":"a","state":"running"},"extra":1}`, ViolationUnknownField},
		{`{"type":"activity_update"}`, ViolationMalformedEnvelope},
		{`{"type":"activity_update","activity":[]}`, ViolationMalformedEnvelope},
		{`{"type":"activity_update","activity":{"activityId":"a","state":"running","extra":1}}`, ViolationUnknownField},
		{`{"type":"activity_update","activity":{"state":"running"}}`, ViolationMalformedEnvelope},
		{`{"type":"activity_update","activity":{"activityId":"a","kind":"chore","state":"running"}}`, ViolationMalformedEnvelope},
		{`{"type":"activity_update","activity":{"activityId":"a","state":"stalled"}}`, ViolationMalformedEnvelope},
		{`{"type":"activity_update","activity":{"activityId":"a","state":"running","cause":"whim"}}`, ViolationMalformedEnvelope},
		{`{"type":"activity_update","activity":{"activityId":"a","state":"running","progress":[]}}`, ViolationMalformedEnvelope},
		{`{"type":"activity_update","activity":{"activityId":"a","state":"running","progress":{"note":"` + strings.Repeat("p", IdentifierBound) + `"}}}`, ViolationMalformedEnvelope},

		// action_update
		{`{"type":"action_update","action":{"actionId":"x","state":"pending"},"extra":1}`, ViolationUnknownField},
		{`{"type":"action_update"}`, ViolationMalformedEnvelope},
		{`{"type":"action_update","action":[]}`, ViolationMalformedEnvelope},
		{`{"type":"action_update","action":{"actionId":"x","state":"pending","extra":1}}`, ViolationUnknownField},
		{`{"type":"action_update","action":{"actionId":"x","kind":"approval","state":"pending"}}`, ViolationMalformedEnvelope},
		{`{"type":"action_update","action":{"actionId":"x","state":"held"}}`, ViolationMalformedEnvelope},
		{`{"type":"action_update","action":{"actionId":"x","state":"pending","owner":[]}}`, ViolationMalformedEnvelope},
		{`{"type":"action_update","action":{"actionId":"x","state":"pending","owner":{"type":"turn","id":"t","extra":1}}}`, ViolationUnknownField},
		{`{"type":"action_update","action":{"actionId":"x","state":"pending","owner":{"type":"session","id":"t"}}}`, ViolationMalformedEnvelope},
		{`{"type":"action_update","action":{"actionId":"x","state":"pending","owner":{"type":"turn"}}}`, ViolationMalformedEnvelope},
		{`{"type":"action_update","action":{"actionId":"x","state":"pending","blocksForeground":"yes"}}`, ViolationMalformedEnvelope},

		// quiescence_update
		{`{"type":"quiescence_update","quiescent":false,"extra":1}`, ViolationUnknownField},
		{`{"type":"quiescence_update"}`, ViolationMalformedEnvelope},
		{`{"type":"quiescence_update","quiescent":"yes"}`, ViolationMalformedEnvelope},
		{`{"type":"quiescence_update","quiescent":false,"watermark":0}`, ViolationMalformedEnvelope},
		{`{"type":"quiescence_update","quiescent":false,"source":"process-containment"}`, ViolationMalformedEnvelope},
		{`{"type":"quiescence_update","quiescent":true,"source":"process-containment"}`, ViolationMalformedEnvelope},
		{`{"type":"quiescence_update","quiescent":true,"source":"vibes","watermark":0}`, ViolationMalformedEnvelope},
		{`{"type":"quiescence_update","quiescent":true,"source":"process-containment","watermark":"0"}`, ViolationMalformedEnvelope},
		{`{"type":"quiescence_update","quiescent":true,"source":"process-containment","watermark":1}`, ViolationMalformedEnvelope},
		{`{"type":"lifecycle_snapshot","foreground":{"state":"running","cycleId":"c","turnId":"t","origin":"session"},` +
			`"activities":[],"actions":[],"quiescence":{"quiescent":false}}`, ViolationMalformedEnvelope},
		{`{"type":"lifecycle_snapshot","foreground":{"state":"running","cycleId":"c","turnId":"t"},` +
			`"activities":[],"actions":[],"quiescence":{"quiescent":false}}`, ViolationMalformedEnvelope},
		{`{"type":"state_update","state":"idle","cycleId":"c","turnId":"t","cause":"submission","outcome":"success"}`,
			ViolationMalformedEnvelope},
		{`{"type":"state_update","state":"idle","cycleId":"c","turnId":"t","cause":"submission",` +
			`"stopReason":"end_turn","outcome":"failed"}`, ViolationMalformedEnvelope},
	} {
		requireRefusal(t, enveloped(tc.event), richConfiguration(), tc.kind)
	}
}

// TestDecodeAcceptsTheWholeEventSet proves the decoder reads every event the
// contract fixes, including the two a prompt-contained configuration never emits.
func TestDecodeAcceptsTheWholeEventSet(t *testing.T) {
	t.Parallel()

	for _, event := range []string{
		`{"type":"lifecycle_snapshot","foreground":{"state":"running","cycleId":"c","turnId":"t","origin":"submission"},"activities":[],"actions":[],"quiescence":{"quiescent":false}}`,
		`{"type":"prompt_accepted","submissionId":"a","clientNonce":"b","turnId":"c","runId":"r"}`,
		`{"type":"state_update","state":"idle","cycleId":"c","cause":"session"}`,
		`{"type":"activity_update","activity":{"activityId":"a","kind":"task","state":"running","cause":"submission","originTurnId":"t","parentId":"p","toolCallId":"tool","runId":"r","progress":{"done":1}}}`,
		`{"type":"action_update","action":{"actionId":"x","kind":"permission","state":"pending","owner":{"type":"turn","id":"t"},"runId":"r","blocksForeground":true}}`,
		`{"type":"quiescence_update","quiescent":true,"source":"native-settled-barrier","watermark":0,"barrier":"b"}`,
	} {
		delivery, err := DecodeSessionUpdate(json.RawMessage(enveloped(event)), richConfiguration())
		require.NoError(t, err, event)
		require.Equal(t, "strm", delivery.StreamID)
		require.Equal(t, uint64(1), delivery.Sequence)
		require.Equal(t, CarrierSessionInfo, delivery.Carrier)
	}
}

// TestCarrierClassForSessionUpdate pins the classifier's own answers, including
// the unclassified carrier a malformed update produces.
func TestCarrierClassForSessionUpdate(t *testing.T) {
	t.Parallel()

	require.Equal(t, CarrierUnknown, CarrierClassForSessionUpdate(nil))
	require.Equal(t, CarrierUnknown, CarrierClassForSessionUpdate(json.RawMessage(`{}`)))
	require.Equal(t, CarrierIneligible, CarrierClassForSessionUpdate(json.RawMessage(`{"sessionUpdate":"plan"}`)))
	require.Equal(t, CarrierSessionInfo, CarrierClassForSessionUpdate(json.RawMessage(`{"sessionUpdate":"session_info_update"}`)))
}

// TestDecodeRefusesANonObjectEntityInASnapshotSet pins that the sets a snapshot
// carries are objects, not whatever the emitter happened to put in the array.
func TestDecodeRefusesANonObjectEntityInASnapshotSet(t *testing.T) {
	t.Parallel()

	quiescence := `{"quiescent":true,"source":"process-containment","watermark":0}`

	requireRefusal(t, enveloped(`{"type":"lifecycle_snapshot","foreground":{"state":"idle","cycleId":"c"},"activities":[7],"actions":[],"quiescence":`+quiescence+`}`),
		richConfiguration(), ViolationMalformedEnvelope)
	requireRefusal(t, enveloped(`{"type":"lifecycle_snapshot","foreground":{"state":"idle","cycleId":"c"},"activities":[],"actions":[7],"quiescence":`+quiescence+`}`),
		richConfiguration(), ViolationMalformedEnvelope)
}
