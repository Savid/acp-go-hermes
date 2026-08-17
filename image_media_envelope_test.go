package hermesacp

import (
	"testing"
)

// paddedPNG grows a valid PNG to exactly size bytes. The structural walk stops
// at the first IDAT chunk, so trailing bytes leave the image readable and let a
// byte-limit case be built at any size the gates care about.
func paddedPNG(t *testing.T, png []byte, size int64) []byte {
	t.Helper()

	if size < int64(len(png)) {
		t.Fatalf("padded size %d is below the fixture's %d bytes", size, len(png))
	}

	padded := make([]byte, size)
	copy(padded, png)

	return padded
}

// TestMediaEnvelopeAdvertisesTheEnforcedPerImageGate pins the advertisement to
// the number a real rejection reports, at each shape of the configured limit.
// A disabled or above-retention limit is not advertised as the host wrote it:
// the adapter clamps it, so the clamp is what a host is told.
func TestMediaEnvelopeAdvertisesTheEnforcedPerImageGate(t *testing.T) {
	png := fixtureBytes(t, "valid.png")
	size := int64(len(png))

	for _, test := range []struct {
		name  string
		limit int64
		want  int64
	}{
		{name: "configured", limit: size, want: size},
		{name: "disabled clamps to the retention bound", limit: 0, want: maxDecodableImageBytes},
		{name: "above the retention bound clamps to it", limit: maxDecodableImageBytes + 1, want: maxDecodableImageBytes},
	} {
		t.Run(test.name, func(t *testing.T) {
			limits := ImageLimits{MaxInputBytesPerImage: test.limit}

			advertised, ok := mediaEnvelopeMeta(limits)[keyMaxBytes].(int64)
			if !ok || advertised != test.want {
				t.Fatalf("advertised maxBytes = %#v, want %d", mediaEnvelopeMeta(limits)[keyMaxBytes], test.want)
			}

			if _, err := newImagePromptBudget(limits, "").admitImage(
				acpFieldPromptImage, 0, paddedPNG(t, png, advertised), advertised, mimePNG,
			); err != nil {
				t.Fatalf("a payload at the advertised bound was rejected: %v", err)
			}

			_, err := newImagePromptBudget(limits, "").admitImage(
				acpFieldPromptImage, 0, paddedPNG(t, png, advertised+1), advertised+1, mimePNG,
			)
			requireImageInputError(t, err, map[string]any{
				keyField:       acpFieldPromptImage,
				jsonFieldError: imageErrTooLarge,
				keyIndex:       0,
				keySizeBytes:   advertised + 1,
				keyMaxBytes:    advertised,
			})
		})
	}
}

// TestMediaEnvelopeAdvertisesTheEnforcedPromptGate pins the aggregate half of
// the same property: the advertised number is the one the aggregate rejection
// reports rather than the configured field restated.
func TestMediaEnvelopeAdvertisesTheEnforcedPromptGate(t *testing.T) {
	png := fixtureBytes(t, "valid.png")
	limits := ImageLimits{MaxInputBytesPerPrompt: 4096}

	advertised, ok := mediaEnvelopeMeta(limits)[mediaEnvelopeFieldMaxPromptBytes].(int64)
	if !ok {
		t.Fatalf("advertised maxPromptBytes = %#v", mediaEnvelopeMeta(limits)[mediaEnvelopeFieldMaxPromptBytes])
	}

	budget := newImagePromptBudget(limits, "")
	payload := paddedPNG(t, png, 3000)

	if _, err := budget.admitImage(acpFieldPromptImage, 0, payload, 3000, mimePNG); err != nil {
		t.Fatalf("the first payload under the aggregate was rejected: %v", err)
	}

	_, err := budget.admitImage(acpFieldPromptImage, 1, payload, 3000, mimePNG)
	requireImageInputError(t, err, map[string]any{
		keyField:       acpFieldPromptImage,
		jsonFieldError: imageErrTooLarge,
		keyIndex:       1,
		keySizeBytes:   int64(6000),
		keyMaxBytes:    advertised,
	})
}

// TestMediaEnvelopeImageFormatsAreTheAllowlist pins that the advertisement and
// the gate read one list, so a format added to either reaches both.
func TestMediaEnvelopeImageFormatsAreTheAllowlist(t *testing.T) {
	advertised, ok := mediaEnvelopeMeta(ImageLimits{})[mediaEnvelopeFieldImageFormats].([]string)
	if !ok {
		t.Fatalf("advertised imageFormats = %#v", mediaEnvelopeMeta(ImageLimits{})[mediaEnvelopeFieldImageFormats])
	}

	for _, mimeType := range advertised {
		if !isAllowlistedImageMime(mimeType) {
			t.Fatalf("advertised format %q is rejected by the gate", mimeType)
		}
	}

	for _, mimeType := range []string{"image/jpg", "image/svg+xml", "image/PNG"} {
		if isAllowlistedImageMime(mimeType) {
			t.Fatalf("gate admits %q, which is not advertised", mimeType)
		}
	}
}
