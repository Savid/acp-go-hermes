package hermesacp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

func fixtureBytes(t *testing.T, name string) []byte {
	t.Helper()

	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}

	return data
}

func fixtureBase64(t *testing.T, name string) string {
	t.Helper()

	return base64.StdEncoding.EncodeToString(fixtureBytes(t, name))
}

func TestImageInputStaticFormatsAndDataAuthority(t *testing.T) {
	for _, test := range []struct {
		name     string
		mimeType string
	}{
		{name: "valid.png", mimeType: mimePNG},
		{name: "valid.jpg", mimeType: mimeJPEG},
		{name: "valid.gif", mimeType: mimeGIF},
		{name: "valid.webp", mimeType: mimeWebP},
	} {
		t.Run(test.name, func(t *testing.T) {
			want := fixtureBytes(t, test.name)
			uri := "https://example.test/different.png?token=secret"
			parts, err := promptToHermesParts([]acp.ContentBlock{{Image: &acp.ContentBlockImage{
				Data:     base64.StdEncoding.EncodeToString(want),
				MimeType: test.mimeType,
				Uri:      &uri,
			}}}, ImageLimits{})
			if err != nil {
				t.Fatalf("promptToHermesParts: %v", err)
			}
			if len(parts) != 1 || parts[0][keyType] != valFile || parts[0][keyMime] != test.mimeType ||
				parts[0][keyFilename] != "different.png" {
				t.Fatalf("part metadata = %#v", parts)
			}
			if got, _ := parts[0][keyData].([]byte); !bytes.Equal(got, want) {
				t.Fatalf("decoded data differs: got %d bytes, want %d", len(got), len(want))
			}
		})
	}
}

func TestImageInputErrorTaxonomyAndOrder(t *testing.T) {
	png := fixtureBase64(t, "valid.png")
	tests := []struct {
		name     string
		data     string
		mimeType string
		want     string
	}{
		{name: "missing data", mimeType: mimePNG, want: imageErrMissingData},
		{name: "media type before base64", data: "!", mimeType: "image/jpg", want: imageErrInvalidMediaType},
		{name: "empty media type", data: png, want: imageErrInvalidMediaType},
		{name: "unsupported media type", data: png, mimeType: "image/svg+xml", want: imageErrInvalidMediaType},
		{name: "non-canonical media type", data: png, mimeType: "image/PNG", want: imageErrInvalidMediaType},
		{name: "invalid base64", data: "not-base64", mimeType: mimePNG, want: imageErrInvalidBase64},
		{name: "unknown raster", data: base64.StdEncoding.EncodeToString([]byte("BM-not-portable")), mimeType: mimePNG, want: imageErrMediaTypeMismatch},
		{name: "MIME mismatch", data: fixtureBase64(t, "mismatch.png"), mimeType: mimePNG, want: imageErrMediaTypeMismatch},
		{name: "invalid dimensions", data: fixtureBase64(t, "truncated.png"), mimeType: mimePNG, want: imageErrInvalidDimensions},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := promptToHermesParts([]acp.ContentBlock{{Image: &acp.ContentBlockImage{
				Data: test.data, MimeType: test.mimeType,
			}}}, ImageLimits{})
			requireImageInputError(t, err, map[string]any{
				keyField:       acpFieldPromptImage,
				jsonFieldError: test.want,
				keyIndex:       0,
			})
		})
	}

	uriOnly := "data:image/png;base64," + png
	_, uriErr := promptToHermesParts([]acp.ContentBlock{{Image: &acp.ContentBlockImage{
		Uri: &uriOnly, MimeType: mimePNG,
	}}}, ImageLimits{})
	requireImageInputError(t, uriErr, map[string]any{
		keyField:       acpFieldPromptImage,
		jsonFieldError: imageErrMissingData,
		keyIndex:       0,
	})

	_, err := promptToHermesParts([]acp.ContentBlock{
		acp.TextBlock("before"),
		{Image: &acp.ContentBlockImage{Data: png, MimeType: mimePNG}},
		{Image: &acp.ContentBlockImage{Data: "!", MimeType: mimePNG}},
	}, ImageLimits{})
	requireImageInputError(t, err, map[string]any{
		keyField:       acpFieldPromptImage,
		jsonFieldError: imageErrInvalidBase64,
		keyIndex:       1,
	})
}

func TestImageInputRejectsAnimation(t *testing.T) {
	for _, test := range []struct {
		name     string
		mimeType string
	}{
		{name: "animated-apng.png", mimeType: mimePNG},
		{name: "single-frame-actl.png", mimeType: mimePNG},
		{name: "animated.gif", mimeType: mimeGIF},
		{name: "animated.webp", mimeType: mimeWebP},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := promptToHermesParts([]acp.ContentBlock{{Image: &acp.ContentBlockImage{
				Data: fixtureBase64(t, test.name), MimeType: test.mimeType,
			}}}, ImageLimits{})
			requireImageInputError(t, err, map[string]any{
				keyField:       acpFieldPromptImage,
				jsonFieldError: imageErrAnimatedNotSupported,
				keyIndex:       0,
			})
		})
	}
}

func TestImageInputDecodedByteLimits(t *testing.T) {
	png := fixtureBytes(t, "valid.png")
	block := acp.ContentBlock{Image: &acp.ContentBlockImage{
		Data: base64.StdEncoding.EncodeToString(png), MimeType: mimePNG,
	}}
	size := int64(len(png))

	if _, err := promptToHermesParts([]acp.ContentBlock{block}, ImageLimits{
		MaxInputBytesPerImage: size,
	}); err != nil {
		t.Fatalf("per-image boundary rejected: %v", err)
	}

	_, err := promptToHermesParts([]acp.ContentBlock{block}, ImageLimits{
		MaxInputBytesPerImage: size - 1,
	})
	requireImageInputError(t, err, map[string]any{
		keyField:       acpFieldPromptImage,
		jsonFieldError: imageErrTooLarge,
		keyIndex:       0,
		keySizeBytes:   size,
		keyMaxBytes:    size - 1,
	})

	if _, aggregateErr := promptToHermesParts([]acp.ContentBlock{block, block}, ImageLimits{
		MaxInputBytesPerPrompt: size * 2,
	}); aggregateErr != nil {
		t.Fatalf("aggregate boundary rejected: %v", aggregateErr)
	}

	_, err = promptToHermesParts([]acp.ContentBlock{block, block}, ImageLimits{
		MaxInputBytesPerPrompt: size*2 - 1,
	})
	requireImageInputError(t, err, map[string]any{
		keyField:       acpFieldPromptImage,
		jsonFieldError: imageErrTooLarge,
		keyIndex:       1,
		keySizeBytes:   size * 2,
		keyMaxBytes:    size*2 - 1,
	})

	if _, err := promptToHermesParts([]acp.ContentBlock{block, block}, ImageLimits{}); err != nil {
		t.Fatalf("zero limits did not disable adapter policy: %v", err)
	}
}

func TestPromptImageValidationPrecedesNativeTurnAndUnknownModelForwards(t *testing.T) {
	client := newFakeHermesClient()
	client.providers = nativehermes.ProvidersResponse{Providers: []nativehermes.ProviderInfo{{
		ID: "provider",
		Models: map[string]nativehermes.ProviderModel{
			"text-looking": {ID: "text-looking", Modalities: nativehermes.ProviderModelModalities{Input: []string{"text"}}},
		},
	}}}
	session := testSession(NewAgent(), client)
	session.providerID = "provider"
	session.modelID = "text-looking"

	sendCalls := 0
	client.sendMessage = func(_ context.Context, id string, req nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		sendCalls++
		if len(req.Parts) != 2 {
			t.Fatalf("native request = %#v", req)
		}
		nativeImage, imageOK := req.Parts[0][keyData].([]byte)
		if !imageOK || req.Parts[0][keyType] != valFile ||
			!bytes.Equal(nativeImage, fixtureBytes(t, "valid.png")) || req.Parts[1][valText] != "inspect" {
			t.Fatalf("native request = %#v", req)
		}

		return nativehermes.NativeMessage{Info: nativehermes.NativeMessageInfo{
			ID: "assistant", SessionID: id, Role: valAssistant, Finish: "stop",
		}}, nil
	}

	_, err := session.Prompt(t.Context(), acp.PromptRequest{
		Meta:      turnRouteMeta("invalid-image"),
		SessionId: session.id,
		Prompt: []acp.ContentBlock{
			{Image: &acp.ContentBlockImage{Data: fixtureBase64(t, "valid.png"), MimeType: mimePNG}},
			{Image: &acp.ContentBlockImage{Data: "!", MimeType: mimePNG}},
		},
	})
	requireImageInputError(t, err, map[string]any{
		keyField:       acpFieldPromptImage,
		jsonFieldError: imageErrInvalidBase64,
		keyIndex:       1,
	})
	if sendCalls != 0 {
		t.Fatalf("native turn started before all images passed validation: %d calls", sendCalls)
	}

	resp, err := session.Prompt(t.Context(), acp.PromptRequest{
		Meta:      turnRouteMeta("unknown-model"),
		SessionId: session.id,
		Prompt: []acp.ContentBlock{
			{Image: &acp.ContentBlockImage{Data: fixtureBase64(t, "valid.png"), MimeType: mimePNG}},
			acp.TextBlock("inspect"),
		},
	})
	if err != nil || resp.StopReason != acp.StopReasonEndTurn || sendCalls != 1 {
		t.Fatalf("unknown model prompt = %#v err=%v sendCalls=%d", resp, err, sendCalls)
	}
}

func TestImageInputBase64StreamingAndStructuralResidual(t *testing.T) {
	png := fixtureBytes(t, "valid.png")
	encoded := base64.StdEncoding.EncodeToString(png)
	encoded = encoded[:12] + "\r\n" + encoded[12:]
	decoded, size, err := decodeImageBase64(encoded, int64(len(png)))
	if err != nil || size != int64(len(png)) || !bytes.Equal(decoded, png) {
		t.Fatalf("decodeImageBase64 = %d bytes size=%d err=%v", len(decoded), size, err)
	}

	writer := &boundedImageDecode{limit: 2}
	if n, err := writer.Write([]byte{1, 2, 3}); err != nil || n != 3 {
		t.Fatalf("bounded write = %d err=%v", n, err)
	}
	if n, err := writer.Write([]byte{4}); err != nil || n != 1 || !reflect.DeepEqual(writer.data, []byte{1, 2}) {
		t.Fatalf("second bounded write = %d data=%v err=%v", n, writer.data, err)
	}

	corruptedTail := append([]byte(nil), png...)
	corruptedTail[len(corruptedTail)-1] ^= 0xff
	if _, err := promptToHermesParts([]acp.ContentBlock{{Image: &acp.ContentBlockImage{
		Data: base64.StdEncoding.EncodeToString(corruptedTail), MimeType: mimePNG,
	}}}, ImageLimits{}); err != nil {
		t.Fatalf("corruption outside structural walk was rejected: %v", err)
	}
}

func TestImageStructureMalformedHeaders(t *testing.T) {
	validPNG := fixtureBytes(t, "valid.png")
	zeroPNG := append([]byte(nil), validPNG...)
	binary.BigEndian.PutUint32(zeroPNG[16:20], 0)
	badPNGChunk := append([]byte(nil), validPNG...)
	binary.BigEndian.PutUint32(badPNGChunk[33:37], ^uint32(0))
	truncatedUnknownPNGChunk := append([]byte(nil), validPNG[:33]...)
	truncatedUnknownPNGChunk = append(truncatedUnknownPNGChunk, 0xff, 0xff, 0xff, 0xff, 'z', 'z', 'z', 'z')
	completeUnknownPNGChunk := append([]byte(nil), validPNG[:33]...)
	completeUnknownPNGChunk = append(completeUnknownPNGChunk, 0, 0, 0, 0, 'z', 'z', 'z', 'z', 0, 0, 0, 0)

	validJPEG := fixtureBytes(t, "valid.jpg")
	zeroJPEG := append([]byte(nil), validJPEG...)
	sof := bytes.Index(zeroJPEG, []byte{0xff, 0xc0})
	if sof < 0 {
		t.Fatal("JPEG fixture has no SOF0 marker")
	}
	zeroJPEG[sof+5], zeroJPEG[sof+6] = 0, 0

	validGIF := fixtureBytes(t, "valid.gif")
	zeroGIF := append([]byte(nil), validGIF...)
	zeroGIF[6], zeroGIF[7] = 0, 0

	validWebP := fixtureBytes(t, "valid.webp")
	zeroWebP := append([]byte(nil), validWebP...)
	zeroWebP[26], zeroWebP[27], zeroWebP[28], zeroWebP[29] = 0, 0, 0, 0

	tests := []struct {
		name     string
		data     []byte
		mimeType string
	}{
		{name: "short PNG", data: pngSignature, mimeType: mimePNG},
		{name: "wrong PNG first chunk", data: append(append([]byte(nil), pngSignature...), make([]byte, 21)...), mimeType: mimePNG},
		{name: "zero PNG width", data: zeroPNG, mimeType: mimePNG},
		{name: "short JPEG", data: []byte{0xff, 0xd8, 0xff}, mimeType: mimeJPEG},
		{name: "bad JPEG marker", data: []byte{0xff, 0xd8, 0xff, 0x00, 0x00, 0x01}, mimeType: mimeJPEG},
		{name: "zero JPEG height", data: zeroJPEG, mimeType: mimeJPEG},
		{name: "short GIF", data: []byte("GIF89a"), mimeType: mimeGIF},
		{name: "zero GIF width", data: zeroGIF, mimeType: mimeGIF},
		{name: "short WebP", data: []byte("RIFFxxxxWEBP"), mimeType: mimeWebP},
		{name: "zero VP8 dimensions", data: zeroWebP, mimeType: mimeWebP},
		{name: "unknown WebP chunk", data: []byte("RIFFxxxxWEBPFAILxxxx"), mimeType: mimeWebP},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := promptToHermesParts([]acp.ContentBlock{{Image: &acp.ContentBlockImage{
				Data: base64.StdEncoding.EncodeToString(test.data), MimeType: test.mimeType,
			}}}, ImageLimits{})
			requireImageInputError(t, err, map[string]any{
				keyField:       acpFieldPromptImage,
				jsonFieldError: imageErrInvalidDimensions,
				keyIndex:       0,
			})
		})
	}

	if _, err := promptToHermesParts([]acp.ContentBlock{{Image: &acp.ContentBlockImage{
		Data: base64.StdEncoding.EncodeToString(badPNGChunk), MimeType: mimePNG,
	}}}, ImageLimits{}); err != nil {
		t.Fatalf("truncated chunk after a valid PNG header was rejected: %v", err)
	}
	for name, data := range map[string][]byte{
		"truncated unknown PNG chunk": truncatedUnknownPNGChunk,
		"complete unknown PNG chunk":  completeUnknownPNGChunk,
	} {
		t.Run(name, func(t *testing.T) {
			animated, err := inspectPNG(data)
			if err != nil || animated {
				t.Fatalf("inspectPNG = animated=%v err=%v", animated, err)
			}
		})
	}

	for name, data := range map[string][]byte{
		"non-marker after segment": {0xff, 0xd8, 0xff, 0xe0, 0x00, 0x02, 0, 0, 0, 0},
		"marker fill":              {0xff, 0xd8, 0xff, 0xff, 0xff, 0xd9, 0, 0},
		"standalone marker":        {0xff, 0xd8, 0xff, 0xd8, 0xff, 0xd9, 0, 0},
		"end marker":               {0xff, 0xd8, 0xff, 0xd9, 0, 0},
		"short frame header":       {0xff, 0xd8, 0xff, 0xc0, 0, 0},
	} {
		t.Run(name, func(t *testing.T) {
			if err := inspectJPEG(data); !errors.Is(err, errImageStructure) {
				t.Fatalf("inspectJPEG error = %v", err)
			}
		})
	}

	staticGIF := make([]byte, 13)
	copy(staticGIF, "GIF89a")
	staticGIF[6], staticGIF[8] = 1, 1
	shortGIFImage := append(append([]byte(nil), staticGIF...), 0x2c)
	localTableDescriptor := make([]byte, 10)
	localTableDescriptor[0], localTableDescriptor[9] = 0x2c, 0x80
	localTableGIF := append(append([]byte(nil), staticGIF...), localTableDescriptor...)
	localTableGIF = append(localTableGIF, make([]byte, 6)...)
	localTableGIF = append(localTableGIF, 2, 0)
	truncatedGIFExtension := append(append([]byte(nil), staticGIF...), 0x21)
	for name, data := range map[string][]byte{
		"empty static GIF":      staticGIF,
		"short image block":     shortGIFImage,
		"local color table":     localTableGIF,
		"truncated extension":   truncatedGIFExtension,
		"unknown terminal byte": append(append([]byte(nil), staticGIF...), 0x3b),
	} {
		t.Run(name, func(t *testing.T) {
			animated, err := inspectGIF(data)
			if err != nil || animated {
				t.Fatalf("inspectGIF = animated=%v err=%v", animated, err)
			}
		})
	}

	for name, data := range map[string][]byte{
		"static VP8X": append([]byte("RIFFxxxxWEBPVP8X"), []byte{10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}...),
		"static VP8L": []byte("RIFFxxxxWEBPVP8L\x05\x00\x00\x00/\x00\x00\x00\x00"),
	} {
		t.Run(name, func(t *testing.T) {
			animated, err := inspectWebP(data)
			if err != nil || animated {
				t.Fatalf("inspectWebP = animated=%v err=%v", animated, err)
			}
		})
	}

	shortVP8X := []byte("RIFFxxxxWEBPVP8X\x00\x00\x00\x00")
	badVP8 := append([]byte(nil), validWebP...)
	badVP8[23] = 0
	badVP8L := []byte("RIFFxxxxWEBPVP8L\x05\x00\x00\x00!\x00\x00\x00\x00")
	for name, data := range map[string][]byte{
		"short VP8X": shortVP8X,
		"bad VP8":    badVP8,
		"bad VP8L":   badVP8L,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := inspectWebP(data); !errors.Is(err, errImageStructure) {
				t.Fatalf("inspectWebP error = %v", err)
			}
		})
	}
}

func requireImageInputError(t *testing.T, err error, want map[string]any) {
	t.Helper()

	var requestErr *acp.RequestError
	if !errors.As(err, &requestErr) {
		t.Fatalf("error = %v, want ACP request error", err)
	}
	if requestErr.Code != -32602 {
		t.Fatalf("code = %d, want -32602", requestErr.Code)
	}
	if !reflect.DeepEqual(requestErr.Data, want) {
		t.Fatalf("data = %#v, want %#v", requestErr.Data, want)
	}
}
