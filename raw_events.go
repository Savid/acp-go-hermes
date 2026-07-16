package hermesacp

import (
	"encoding/json"
	"fmt"
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
	validationDuplicate   = "duplicate"

	keySequence = "sequence"
	keySource   = "source"
	keyEvent    = "event"
	keyData     = "data"

	rawEventReasonOversize     = "oversize"
	rawEventReasonUnserialized = "unserializable"
	rawEventKeyTruncated       = "truncated"
	rawEventKeyReason          = "reason"
	rawEventKeyMaxBytes        = "maxBytes"
	rawEventKeySizeBytes       = "sizeBytes"
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

// capRawEventPayload returns the complete routed payload unchanged when it
// marshals within the size limit. Otherwise it replaces only the event with the
// fixed truncation marker, consuming the sequence rather than dropping the
// notification: an oversize event keeps its byte size, a marshal failure is
// reported as unserializable. It then proves the final marker envelope also
// fits; the route nonce bound ensures route metadata alone cannot make the
// marker exceed the cap.
func capRawEventPayload(payload map[string]any) (map[string]any, error) {
	encoded, err := json.Marshal(payload)
	if err == nil && len(encoded) <= rawEventMaxBytes {
		return payload, nil
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

	capped := map[string]any{
		jsonFieldSessionID: payload[jsonFieldSessionID],
		keySequence:        payload[keySequence],
		keySource:          payload[keySource],
		keyEvent:           marker,
	}
	if meta, ok := payload["_meta"]; ok {
		capped["_meta"] = meta
	}

	encoded, err = json.Marshal(capped)
	if err != nil {
		return nil, fmt.Errorf("marshal capped raw event payload: %w", err)
	}

	if len(encoded) > rawEventMaxBytes {
		return nil, fmt.Errorf("capped raw event payload is %d bytes, exceeds %d", len(encoded), rawEventMaxBytes)
	}

	return capped, nil
}
