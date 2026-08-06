package hermesacp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/coder/acp-go-sdk"
)

// Image prompt validation vocabulary. Every pre-turn media rejection is
// -32602 invalid params carrying {"field":<request member>,"error":<value>,
// "index":<image ordinal>} plus sizeBytes/maxBytes when a byte limit is at
// fault. Routing is chosen by MIME and the field is chosen by the inbound block
// type, so a resource blob names the resource channel even when its declared
// raster type sent it through the image gates.
const (
	acpFieldPromptImage    = "prompt.image"
	acpFieldPromptResource = "prompt.resource"

	imageErrMissingData          = "missing_data"
	imageErrInvalidBase64        = "invalid_base64"
	imageErrInvalidMediaType     = "invalid_media_type"
	imageErrMediaTypeMismatch    = "media_type_mismatch"
	imageErrAnimatedNotSupported = "animated_not_supported"
	imageErrInvalidDimensions    = "invalid_dimensions"
	imageErrTooLarge             = "too_large"

	// Handoff-form verdicts. invalid_handoff claims a block that is malformed
	// as a block; path_not_allowed and missing_file describe the file it names.
	imageErrInvalidHandoff        = "invalid_handoff"
	imageErrPathNotAllowed        = "path_not_allowed"
	imageErrMissingFile           = "missing_file"
	imageErrHandoffDigestMismatch = "handoff_digest_mismatch"

	keyIndex     = "index"
	keySizeBytes = "sizeBytes"
	keyMaxBytes  = "maxBytes"

	mimePNG  = "image/png"
	mimeJPEG = "image/jpeg"
	mimeGIF  = "image/gif"
	mimeWebP = "image/webp"
)

// maxDecodableImageBytes is the most decoded bytes a base64 image can carry
// inside the pinned ACP SDK's 10 MiB inbound frame once the enclosing JSON-RPC
// envelope is accounted for. Decode retention is bounded here rather than by
// the configurable per-image policy limit so structural inspection always walks
// the whole decodable payload. It is a retention bound and never a gate: the
// enforced per-image bound is derived from it and can never exceed it, so bytes
// offered past it always fail a byte verdict instead of being truncated and
// forwarded. The process has already received at most one 10 MiB frame, so
// retaining a whole image is memory-safe.
const maxDecodableImageBytes int64 = 7_864_155

// effectiveInputImageLimit resolves the configured per-image input policy limit
// into the bound the adapter actually enforces: a disabled (zero) or
// above-retention limit clamps to the retention bound, because a decoded image
// still has to fit one JSON-RPC frame and a handoff read still has to be
// bounded before it allocates a file's declared size. Advertisement and gate
// both read this, so the number a host is told is the number it is judged by.
func effectiveInputImageLimit(configured int64) int64 {
	if configured <= 0 || configured > maxDecodableImageBytes {
		return maxDecodableImageBytes
	}

	return configured
}

// effectiveInputPromptLimit resolves the configured per-prompt aggregate input
// limit into the bound the gate enforces. A disabled (zero) aggregate stays
// disabled: the handoff-form block count bounds the read work instead, so
// restating "disabled" as a byte number would reject the multi-image turn the
// handoff form exists to carry.
func effectiveInputPromptLimit(configured int64) int64 {
	return configured
}

// errImageStructure signals that a sniffed raster's header yields no valid
// dimensions or cannot complete the structural walk needed to read them.
var errImageStructure = errors.New("image structure invalid")

// promptMediaError reports a gated-media verdict against the request member the
// block arrived on.
func promptMediaError(field string, errValue string, index int, sizeBytes, maxBytes int64) error {
	data := map[string]any{
		keyField:       field,
		jsonFieldError: errValue,
		keyIndex:       index,
	}

	if sizeBytes > 0 {
		data[keySizeBytes] = sizeBytes
	}

	if maxBytes > 0 {
		data[keyMaxBytes] = maxBytes
	}

	return acp.NewInvalidParams(data)
}

// imagePromptBudget validates every image in one prompt in request order,
// assigning stable image indexes and enforcing the decoded-byte bounds the
// adapter advertises. Validation stops on the first failing image.
type imagePromptBudget struct {
	perImage    int64
	perPrompt   int64
	handoffRoot string
	nextIndex   int
	totalBytes  int64
	// handoffBlocks counts the handoff-form blocks this prompt has asked the
	// adapter to read, which is bounded independently of the byte aggregate a
	// host may disable.
	handoffBlocks int
	// root is the opened read root, held for the life of one prompt mapping so
	// every handoff open in that prompt is relative to one kernel-checked
	// descriptor.
	root *os.Root
}

func newImagePromptBudget(limits ImageLimits, handoffRoot string) *imagePromptBudget {
	return &imagePromptBudget{
		perImage:    effectiveInputImageLimit(limits.MaxInputBytesPerImage),
		perPrompt:   effectiveInputPromptLimit(limits.MaxInputBytesPerPrompt),
		handoffRoot: handoffRoot,
	}
}

func (b *imagePromptBudget) claimIndex() int {
	index := b.nextIndex
	b.nextIndex++

	return index
}

// validateEmbedded runs the deterministic per-image pipeline over base64 data
// carried in the block and returns the one decoded byte slice passed to Hermes.
// The driven gateway publishes no exhaustive selected-model modality data, so
// validated images always forward and the native provider remains
// authoritative.
func (b *imagePromptBudget) validateEmbedded(field, data, mimeType string) ([]byte, error) {
	index := b.claimIndex()

	if data == "" {
		return nil, promptMediaError(field, imageErrMissingData, index, 0, 0)
	}

	if !isAllowlistedImageMime(mimeType) {
		return nil, promptMediaError(field, imageErrInvalidMediaType, index, 0, 0)
	}

	decoded, size, err := decodeImageBase64(data, maxDecodableImageBytes)
	if err != nil {
		return nil, promptMediaError(field, imageErrInvalidBase64, index, 0, 0)
	}

	return b.admitImage(field, index, decoded, size, mimeType)
}

// validateHandoff reads and verifies a handoff block's file before the embedded
// gate chain runs on those bytes. The pre-gate sits ahead of every embedded
// gate: only the two gates that are meaningless without base64 (missing_data,
// invalid_base64) drop out.
func (b *imagePromptBudget) validateHandoff(ctx context.Context, image *acp.ContentBlockImage) ([]byte, error) {
	index := b.claimIndex()

	data, failure := b.handoffBytes(ctx, image)
	if failure != nil {
		return nil, imageHandoffError(failure, index)
	}

	return b.admitImage(acpFieldPromptImage, index, data, int64(len(data)), image.MimeType)
}

// admitImage runs the gates shared by both transport forms — sniff, structure,
// animation, declared-vs-sniffed, per-image bytes, per-prompt bytes — over the
// bytes one image contributed. size is the image's full decoded size even when
// data holds only the retained prefix, so a byte verdict never understates what
// arrived while structural inspection still reads the header. The per-image
// bound can never exceed the retention bound, so a payload whose prefix is all
// that survived decoding always fails the byte gate rather than reaching the
// harness truncated.
func (b *imagePromptBudget) admitImage(field string, index int, data []byte, size int64, mimeType string) ([]byte, error) {
	sniffed := sniffImageMime(data)
	if sniffed == "" {
		return nil, promptMediaError(field, imageErrMediaTypeMismatch, index, 0, 0)
	}

	animated, structureErr := inspectImageStructure(sniffed, data)
	if structureErr != nil {
		return nil, promptMediaError(field, imageErrInvalidDimensions, index, 0, 0)
	}

	if animated {
		return nil, promptMediaError(field, imageErrAnimatedNotSupported, index, 0, 0)
	}

	if sniffed != mimeType {
		return nil, promptMediaError(field, imageErrMediaTypeMismatch, index, 0, 0)
	}

	if size > b.perImage {
		return nil, promptMediaError(field, imageErrTooLarge, index, size, b.perImage)
	}

	if err := b.accountPromptBytes(field, index, size); err != nil {
		return nil, err
	}

	return data, nil
}

// accountBlobResource applies the per-image byte gate and per-prompt accounting
// to an embedded blob resource of any MIME. A blob carries bytes whatever it
// declares, so base64 validity and the byte budget bind before the resource's
// own downstream mapping decides what to do with it.
func (b *imagePromptBudget) accountBlobResource(blob string) error {
	index := b.claimIndex()

	_, size, err := decodeImageBase64(blob, maxDecodableImageBytes)
	if err != nil {
		return promptMediaError(acpFieldPromptResource, imageErrInvalidBase64, index, 0, 0)
	}

	if size > b.perImage {
		return promptMediaError(acpFieldPromptResource, imageErrTooLarge, index, size, b.perImage)
	}

	return b.accountPromptBytes(acpFieldPromptResource, index, size)
}

// chargeText adds a text resource's bytes to the same per-prompt accumulator the
// media forms use. Bytes are bytes: declaring them as text rather than as a blob
// must not buy a prompt more of them than the aggregate allows. It reports at
// the position the next media block would take without consuming it, because a
// text resource carries no media the index is meant to identify.
func (b *imagePromptBudget) chargeText(size int64) error {
	return b.accountPromptBytes(acpFieldPromptResource, b.nextIndex, size)
}

func (b *imagePromptBudget) accountPromptBytes(field string, index int, size int64) error {
	b.totalBytes += size

	if b.perPrompt > 0 && b.totalBytes > b.perPrompt {
		return promptMediaError(field, imageErrTooLarge, index, b.totalBytes, b.perPrompt)
	}

	return nil
}

// normalizeMediaType reduces a declared MIME to its bare lowercase type for
// prefix routing: surrounding space is trimmed and any parameters are dropped.
// Routing only — the format allowlist still matches the declared string
// exactly, so a non-canonical declaration reaches the image gates and is
// rejected there instead of escaping them.
func normalizeMediaType(mimeType string) string {
	bare, _, _ := strings.Cut(mimeType, ";")

	return strings.ToLower(strings.TrimSpace(bare))
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

// inputImageMIMEAllowlist is the ordered inbound raster allowlist: exactly the
// four canonical static raster MIME strings, in advertisement order. The gate
// and the advertisement read this one list, so neither can drift from the other.
func inputImageMIMEAllowlist() []string {
	return []string{mimePNG, mimeJPEG, mimeGIF, mimeWebP}
}

// isAllowlistedImageMime matches the declared MIME verbatim, so a non-canonical
// raster spelling reaches the image gates and is rejected there.
func isAllowlistedImageMime(mimeType string) bool {
	return slices.Contains(inputImageMIMEAllowlist(), mimeType)
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
// animation. A stream that ends or turns unparsable after a valid header is
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
