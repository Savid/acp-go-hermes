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

// mediaEnvelopeMeta reports the inbound media bounds a host can rely on before
// it sends. Every byte value is read from the same ImageLimits fields the input
// gates enforce, so WithImageLimits moves the advertisement and the gate
// together. Hermes maps no MIME to a native document representation, so
// documentFormats is empty, and its native image.attach_bytes ceiling is looser
// than the policy defaults, so no native envelope tightens these values.
func mediaEnvelopeMeta(limits ImageLimits) map[string]any {
	return map[string]any{
		keyMaxBytes:                       limits.MaxInputBytesPerImage,
		mediaEnvelopeFieldMaxPromptBytes:  limits.MaxInputBytesPerPrompt,
		mediaEnvelopeFieldMaxDimension:    mediaEnvelopeMaxDimension,
		mediaEnvelopeFieldImageFormats:    []string{mimePNG, mimeJPEG, mimeGIF, mimeWebP},
		mediaEnvelopeFieldDocumentFormats: []string{},
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
