package hermesacp

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"strings"

	"github.com/coder/acp-go-sdk"
)

// Image prompt validation vocabulary. Every pre-turn image rejection is
// -32602 invalid params carrying {"field":"prompt.image","error":<value>,
// "index":<image ordinal>} plus sizeBytes/maxBytes when a byte limit is at
// fault.
const (
	acpFieldPromptImage = "prompt.image"

	imageErrMissingData          = "missing_data"
	imageErrInvalidBase64        = "invalid_base64"
	imageErrInvalidMediaType     = "invalid_media_type"
	imageErrMediaTypeMismatch    = "media_type_mismatch"
	imageErrAnimatedNotSupported = "animated_not_supported"
	imageErrInvalidDimensions    = "invalid_dimensions"
	imageErrTooLarge             = "too_large"

	keyIndex     = "index"
	keySizeBytes = "sizeBytes"
	keyMaxBytes  = "maxBytes"

	mimePNG  = "image/png"
	mimeJPEG = "image/jpeg"
	mimeGIF  = "image/gif"
	mimeWebP = "image/webp"
)

// errImageStructure signals that a sniffed raster's header yields no valid
// dimensions or cannot complete the structural walk needed to read them.
var errImageStructure = errors.New("image structure invalid")

func imageInputError(errValue string, index int) error {
	return acp.NewInvalidParams(map[string]any{
		keyField:       acpFieldPromptImage,
		jsonFieldError: errValue,
		keyIndex:       index,
	})
}

func imageInputSizeError(index int, sizeBytes, maxBytes int64) error {
	return acp.NewInvalidParams(map[string]any{
		keyField:       acpFieldPromptImage,
		jsonFieldError: imageErrTooLarge,
		keyIndex:       index,
		keySizeBytes:   sizeBytes,
		keyMaxBytes:    maxBytes,
	})
}

// imagePromptBudget validates every image in one prompt in request order,
// assigning stable image indexes and enforcing the configured decoded-byte
// limits. Validation stops on the first failing image.
type imagePromptBudget struct {
	limits     ImageLimits
	nextIndex  int
	totalBytes int64
}

// validate runs the deterministic per-image pipeline and returns the one
// decoded byte slice passed to Hermes. The driven gateway publishes no
// exhaustive selected-model modality data, so validated images always
// forward and the native provider remains authoritative.
func (b *imagePromptBudget) validate(data, mimeType string) ([]byte, error) {
	index := b.nextIndex
	b.nextIndex++

	if data == "" {
		return nil, imageInputError(imageErrMissingData, index)
	}

	if !isAllowlistedImageMime(mimeType) {
		return nil, imageInputError(imageErrInvalidMediaType, index)
	}

	decoded, size, err := decodeImageBase64(data, b.limits.MaxInputBytesPerImage)
	if err != nil {
		return nil, imageInputError(imageErrInvalidBase64, index)
	}

	if limit := b.limits.MaxInputBytesPerImage; limit > 0 && size > limit {
		return nil, imageInputSizeError(index, size, limit)
	}

	sniffed := sniffImageMime(decoded)
	if sniffed == "" {
		return nil, imageInputError(imageErrMediaTypeMismatch, index)
	}

	animated, structureErr := inspectImageStructure(sniffed, decoded)
	if structureErr != nil {
		return nil, imageInputError(imageErrInvalidDimensions, index)
	}

	if animated {
		return nil, imageInputError(imageErrAnimatedNotSupported, index)
	}

	if sniffed != mimeType {
		return nil, imageInputError(imageErrMediaTypeMismatch, index)
	}

	b.totalBytes += size
	if limit := b.limits.MaxInputBytesPerPrompt; limit > 0 && b.totalBytes > limit {
		return nil, imageInputSizeError(index, b.totalBytes, limit)
	}

	return decoded, nil
}

type boundedImageDecode struct {
	data  []byte
	limit int64
	size  int64
}

func (w *boundedImageDecode) Write(p []byte) (int, error) {
	w.size += int64(len(p))

	retain := len(p)
	if w.limit > 0 {
		remaining := w.limit - int64(len(w.data))
		if remaining <= 0 {
			retain = 0
		} else if int64(retain) > remaining {
			retain = int(remaining)
		}
	}

	w.data = append(w.data, p[:retain]...)

	return len(p), nil
}

func decodeImageBase64(data string, limit int64) ([]byte, int64, error) {
	decoded := &boundedImageDecode{limit: limit}

	_, err := io.Copy(decoded, base64.NewDecoder(base64.StdEncoding, strings.NewReader(data)))
	if err != nil {
		return nil, 0, err
	}

	return decoded.data, decoded.size, nil
}

func isAllowlistedImageMime(mimeType string) bool {
	switch mimeType {
	case mimePNG, mimeJPEG, mimeGIF, mimeWebP:
		return true
	default:
		return false
	}
}

var pngSignature = []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1A, '\n'}

func sniffImageMime(data []byte) string {
	switch {
	case len(data) >= 8 && bytes.Equal(data[:8], pngSignature):
		return mimePNG
	case len(data) >= 3 && data[0] == 0xFF && data[1] == 0xD8 && data[2] == 0xFF:
		return mimeJPEG
	case len(data) >= 6 && (string(data[:6]) == "GIF87a" || string(data[:6]) == "GIF89a"):
		return mimeGIF
	case len(data) >= 12 && string(data[:4]) == "RIFF" && string(data[8:12]) == "WEBP":
		return mimeWebP
	default:
		return ""
	}
}

// inspectImageStructure reports whether the raster is animated, walking the
// container's block/chunk list without decoding any pixels. It returns
// errImageStructure when the header yields no valid dimensions.
func inspectImageStructure(sniffedMime string, data []byte) (bool, error) {
	switch sniffedMime {
	case mimePNG:
		return inspectPNG(data)
	case mimeJPEG:
		return false, inspectJPEG(data)
	case mimeGIF:
		return inspectGIF(data)
	default:
		return inspectWebP(data)
	}
}

// inspectPNG requires a complete leading IHDR with non-zero dimensions, then
// walks chunks for acTL (APNG). The walk ends at the first IDAT because the
// APNG spec constrains acTL to precede it; truncation after a valid header is
// left for the native provider to judge.
func inspectPNG(data []byte) (bool, error) {
	offset := uint64(len(pngSignature))
	if offset+8+13 > uint64(len(data)) {
		return false, errImageStructure
	}

	length := uint64(binary.BigEndian.Uint32(data[offset : offset+4]))
	if string(data[offset+4:offset+8]) != "IHDR" || length != 13 {
		return false, errImageStructure
	}

	width := binary.BigEndian.Uint32(data[offset+8 : offset+12])
	height := binary.BigEndian.Uint32(data[offset+12 : offset+16])

	if width == 0 || height == 0 {
		return false, errImageStructure
	}

	offset += 8 + length + 4
	for offset+8 <= uint64(len(data)) {
		length = uint64(binary.BigEndian.Uint32(data[offset : offset+4]))

		switch string(data[offset+4 : offset+8]) {
		case "acTL":
			return true, nil
		case "IDAT":
			return false, nil
		}

		if length > uint64(len(data))-offset-8 {
			return false, nil
		}

		offset += 8 + length + 4
	}

	return false, nil
}

// inspectJPEG walks marker segments to the first frame header (SOFn) and
// requires it to carry non-zero dimensions. JPEG has no animation container.
func inspectJPEG(data []byte) error {
	offset := 2
	for offset+4 <= len(data) {
		if data[offset] != 0xFF {
			return errImageStructure
		}

		marker := data[offset+1]

		switch {
		case marker == 0xFF:
			offset++
		case marker == 0x01 || (marker >= 0xD0 && marker <= 0xD8):
			offset += 2
		case marker == 0xD9 || marker == 0xDA:
			return errImageStructure
		case isJPEGFrameMarker(marker):
			return checkJPEGFrameHeader(data, offset)
		default:
			length := int(binary.BigEndian.Uint16(data[offset+2 : offset+4]))
			if length < 2 {
				return errImageStructure
			}

			offset += 2 + length
		}
	}

	return errImageStructure
}

func isJPEGFrameMarker(marker byte) bool {
	if marker < 0xC0 || marker > 0xCF {
		return false
	}

	return marker != 0xC4 && marker != 0xC8 && marker != 0xCC
}

func checkJPEGFrameHeader(data []byte, offset int) error {
	if offset+9 > len(data) {
		return errImageStructure
	}

	height := binary.BigEndian.Uint16(data[offset+5 : offset+7])
	width := binary.BigEndian.Uint16(data[offset+7 : offset+9])

	if width == 0 || height == 0 {
		return errImageStructure
	}

	return nil
}

// inspectGIF requires a logical screen descriptor with non-zero dimensions,
// then counts image descriptors through the block stream: more than one is
// animation. A stream that ends or turns unparseable after a valid header is
// left for the native provider to judge.
func inspectGIF(data []byte) (bool, error) {
	if len(data) < 13 {
		return false, errImageStructure
	}

	width := binary.LittleEndian.Uint16(data[6:8])
	height := binary.LittleEndian.Uint16(data[8:10])

	if width == 0 || height == 0 {
		return false, errImageStructure
	}

	offset := 13
	if data[10]&0x80 != 0 {
		offset += 3 << ((data[10] & 0x07) + 1)
	}

	descriptors := 0

	for offset < len(data) {
		switch data[offset] {
		case 0x2C:
			descriptors++
			if descriptors > 1 {
				return true, nil
			}

			offset = skipGIFImage(data, offset)
		case 0x21:
			offset = skipGIFSubBlocks(data, offset+2)
		default:
			return false, nil
		}
	}

	return false, nil
}

func skipGIFImage(data []byte, offset int) int {
	if offset+10 > len(data) {
		return len(data)
	}

	packed := data[offset+9]
	offset += 10

	if packed&0x80 != 0 {
		offset += 3 << ((packed & 0x07) + 1)
	}

	return skipGIFSubBlocks(data, offset+1)
}

func skipGIFSubBlocks(data []byte, offset int) int {
	for offset < len(data) {
		size := int(data[offset])
		offset++

		if size == 0 {
			return offset
		}

		offset += size
	}

	return offset
}

// inspectWebP reads the first RIFF chunk: VP8X carries the ANIM flag, and the
// VP8/VP8L frame headers must carry valid dimensions.
func inspectWebP(data []byte) (bool, error) {
	if len(data) < 20 {
		return false, errImageStructure
	}

	payload := data[20:]

	switch string(data[12:16]) {
	case "VP8X":
		if len(payload) < 10 {
			return false, errImageStructure
		}

		return payload[0]&0x02 != 0, nil
	case "VP8 ":
		return false, checkVP8FrameHeader(payload)
	case "VP8L":
		return false, checkVP8LFrameHeader(payload)
	default:
		return false, errImageStructure
	}
}

func checkVP8FrameHeader(payload []byte) error {
	if len(payload) < 10 || payload[3] != 0x9D || payload[4] != 0x01 || payload[5] != 0x2A {
		return errImageStructure
	}

	width := binary.LittleEndian.Uint16(payload[6:8]) & 0x3FFF
	height := binary.LittleEndian.Uint16(payload[8:10]) & 0x3FFF

	if width == 0 || height == 0 {
		return errImageStructure
	}

	return nil
}

func checkVP8LFrameHeader(payload []byte) error {
	if len(payload) < 5 || payload[0] != 0x2F {
		return errImageStructure
	}

	return nil
}
