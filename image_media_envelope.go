package hermesacp

// Family-reserved inbound media advertisement. Both literals sit beside
// acp-go.dev/route in the agent capability metadata under the same rules: a
// reserved key, a versioned or exactly-shaped object, and no vendor namespace.
const (
	mediaEnvelopeMetaKey = "acp-go.dev/mediaEnvelope"

	mediaEnvelopeFieldMaxPromptBytes  = "maxPromptBytes"
	mediaEnvelopeFieldMaxDimension    = "maxDimension"
	mediaEnvelopeFieldImageFormats    = "imageFormats"
	mediaEnvelopeFieldDocumentFormats = "documentFormats"

	keyVersions = "versions"
)

// mediaEnvelopeMaxDimension is 0: Hermes enforces no per-dimension pixel bound.
// The adapter's dimension gate only asks whether a header yields readable
// dimensions, never whether they are large, and the driven gateway publishes no
// pixel ceiling to pin.
const mediaEnvelopeMaxDimension = 0

// mediaEnvelopeDocumentFormats is empty because Hermes maps no MIME to a native
// document representation. It is a non-nil slice so the advertisement carries an
// empty JSON array.
var mediaEnvelopeDocumentFormats = []string{}

// mediaEnvelopeMeta reports the inbound media bounds a host can rely on before
// it sends. Every value is computed by the same function the gate calls, never
// read from a raw ImageLimits field, so a configured limit the adapter clamps is
// advertised at the number a rejection would report rather than at the number
// the host asked for. Hermes' native image.attach_bytes ceiling is looser than
// the policy defaults, so no native envelope tightens these values.
func mediaEnvelopeMeta(limits ImageLimits) map[string]any {
	return map[string]any{
		keyMaxBytes:                       effectiveInputImageLimit(limits.MaxInputBytesPerImage),
		mediaEnvelopeFieldMaxPromptBytes:  effectiveInputPromptLimit(limits.MaxInputBytesPerPrompt),
		mediaEnvelopeFieldMaxDimension:    mediaEnvelopeMaxDimension,
		mediaEnvelopeFieldImageFormats:    inputImageMIMEAllowlist(),
		mediaEnvelopeFieldDocumentFormats: mediaEnvelopeDocumentFormats,
	}
}

// capabilityMediaMeta builds the media entries of the agent capability
// metadata. The envelope is unconditional; the handoff advertisement appears
// only when an input handoff root is configured, so its absence is the host's
// signal that its option never reached this adapter.
func capabilityMediaMeta(options Options) map[string]any {
	meta := map[string]any{
		mediaEnvelopeMetaKey: mediaEnvelopeMeta(options.ImageLimits),
	}

	if options.InputHandoffRoot != "" {
		meta[handoffMetaKey] = map[string]any{keyVersions: []int{handoffVersion}}
	}

	return meta
}
