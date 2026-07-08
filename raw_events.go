package hermesacp

import (
	"encoding/json"
)

const (
	ForkSessionMethod = "_hermes/session/fork"
	RawEventMethod    = "_hermes/rawEvent"

	hermesMetaKey         = "hermes"
	rawEventKey           = "rawEvent"
	rawEventEnabledKey    = "enabled"
	rawEventCapabilityKey = "rawEvent"
	rawEventEnabledByPath = "_meta.hermes.rawEvent.enabled"
	rawEventMaxBytes      = 64 * 1024
	hermesNativeIDMetaKey = "nativeSessionId"
	configModel           = "model"
	configTypeSelect      = "select"
	idmapSubpath          = "idmap"
	stateDBSubpath        = "state-db"
	jsonFieldError        = "error"
	jsonFieldMessage      = "message"
	jsonFieldMethod       = "method"
	jsonFieldSessionID    = "sessionId"
	jsonFieldCwd          = "cwd"
	validationRequired    = "required"
)

type rawMessageConfig struct {
	enabled bool
}

func rawMessageConfigFromMeta(meta map[string]any) rawMessageConfig {
	hermesMeta, _ := meta[hermesMetaKey].(map[string]any)
	if hermesMeta == nil {
		return rawMessageConfig{}
	}

	rawEvent, _ := hermesMeta[rawEventKey].(map[string]any)

	enabled, _ := rawEvent[rawEventEnabledKey].(bool)
	if enabled {
		return rawMessageConfig{enabled: true}
	}

	return rawMessageConfig{}
}

func (c rawMessageConfig) Enabled() bool {
	return c.enabled
}

// capRawEventPayload returns the payload unchanged when it marshals within the
// size limit. Otherwise it replaces the event with the fixed family truncation
// marker, consuming the sequence rather than dropping the notification: an
// oversize event keeps its byte size, a marshal failure is reported as
// unserializable. The marker is always valid JSON, so a consumer never receives
// an invalid payload and never sees an unexplained gap in the sequence.
func capRawEventPayload(payload map[string]any) map[string]any {
	encoded, err := json.Marshal(payload)
	if err == nil && len(encoded) <= rawEventMaxBytes {
		return payload
	}

	marker := map[string]any{
		rawEventKeyTruncated: true,
		rawEventKeyMaxBytes:  rawEventMaxBytes,
	}
	if err != nil {
		marker[rawEventKeyReason] = rawEventReasonUnserialized
	} else {
		marker[rawEventKeyReason] = rawEventReasonOversize
		marker[rawEventKeySizeBytes] = len(encoded)
	}

	return map[string]any{
		jsonFieldSessionID: payload[jsonFieldSessionID],
		keySequence:        payload[keySequence],
		keySource:          payload[keySource],
		keyEvent:           marker,
	}
}
