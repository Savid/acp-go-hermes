package hermesacp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/coder/acp-go-sdk"
)

// Local-handoff image transport. A host that configures an input handoff root
// may replace an image block's embedded base64 with a digest-verified file
// under that root. The root is read-only: the adapter reads and verifies bytes
// there and never writes, moves, or removes anything.
const (
	handoffMetaKey = "acp-go.dev/handoff"
	handoffVersion = 1

	handoffFieldVersion   = "version"
	handoffFieldDigest    = "digest"
	handoffFieldSizeBytes = "sizeBytes"

	handoffEnvelopeFieldCount = 3

	handoffURIScheme    = "file"
	handoffURILocalHost = "localhost"
	handoffDigestLength = 64
)

// handoffSizeBytesExclusiveMax is 2^63 as a float64, the first value at or
// above the int64 range. A declared size must be strictly below it, because
// converting an out-of-range float64 to int64 wraps on one architecture and
// saturates on another.
const handoffSizeBytesExclusiveMax = 9223372036854775808.0

// maxHandoffBlocksPerPrompt bounds the handoff-form blocks one prompt may ask
// this adapter to read. Each block is a few hundred bytes on the wire and
// drives a whole file read, so without a count bound a single legal frame
// commits the adapter to arbitrarily much I/O and resident memory whenever the
// per-prompt byte aggregate is disabled. It bounds the work rather than the
// byte policy, and sits far enough above any real multi-image turn that a
// conforming host never meets it.
const maxHandoffBlocksPerPrompt = 64

// Handoff failure messages. Every one is a compile-time constant: a verdict
// travels to the client and into telemetry, so it may never carry a path, a
// URI, a filename, a digest, a byte count of a file the caller did not
// describe, or operating-system error text. Each envelope defect class gets its
// own message, because "malformed" is not an answer a host can act on.
const (
	handoffRootUnsetMessage            = "the local handoff root is not configured"
	handoffRootUnopenableMessage       = "the configured handoff root could not be opened"
	handoffEnvelopeAbsentMessage       = "the " + handoffMetaKey + " envelope is absent"
	handoffEnvelopeNotObjectMessage    = "the " + handoffMetaKey + " envelope must be a JSON object"
	handoffEnvelopeMissingFieldMessage = "the " + handoffMetaKey + " envelope is missing version, digest, or sizeBytes"
	handoffEnvelopeUnknownFieldMessage = "the " + handoffMetaKey + " envelope carries a field beyond version, digest, and sizeBytes"
	handoffVersionMessage              = "the " + handoffMetaKey + " envelope version is unsupported"
	handoffDigestFormatMessage         = "the " + handoffMetaKey + " digest must be 64 lowercase hexadecimal characters"
	handoffSizeFormatMessage           = "the " + handoffMetaKey + " sizeBytes must be a non-negative integer"
	handoffURIRequiredMessage          = "a handoff block requires a file URI"
	handoffURIInvalidMessage           = "the handoff URI is not a valid URI"
	handoffURISchemeMessage            = "the handoff URI scheme must be file"
	handoffURIRemoteHostMessage        = "the handoff URI must not name a remote host"
	handoffURIRelativeMessage          = "the handoff URI must carry an absolute path"
	handoffOutsideRootMessage          = "the handoff path is outside the configured handoff root"
	handoffUnopenableMessage           = "the handoff file could not be opened"
	handoffAbsentMessage               = "the handoff path does not exist"
	handoffUninspectableMessage        = "the handoff file could not be inspected"
	handoffNotRegularMessage           = "the handoff path is not a regular file"
	handoffUnreadableMessage           = "the handoff file could not be read"
	handoffSizeMismatchMessage         = "the handoff file size does not match the declared sizeBytes"
	handoffDigestMismatchMessage       = "the handoff file bytes do not match the declared digest"
)

// openHandoffRoot opens the configured read root. Containment is delegated to
// the kernel from here on: every handoff open is relative to this descriptor,
// a symlink beneath it may not name a location outside it, and there is no
// window between deciding a path is inside the root and reading it. It is a
// seam so the failure an unusable root produces stays exercisable.
var openHandoffRoot = os.OpenRoot

// handoffFile is what the bounded read needs from an opened handoff file.
type handoffFile interface {
	io.ReadCloser
	Stat() (os.FileInfo, error)
}

// openHandoffFile opens one name inside an already-opened read root. It is a
// seam so the inspection and read failures a real filesystem only produces
// under a race with the host that owns the file stay exercisable; containment
// is the root's, not this function's, on every path.
var openHandoffFile = func(root *os.Root, rel string) (handoffFile, error) {
	return root.OpenFile(rel, os.O_RDONLY|handoffOpenFlags, 0)
}

// handoffEnvelope is one decoded acp-go.dev/handoff block envelope.
type handoffEnvelope struct {
	digest    string
	sizeBytes int64
}

// handoffError is one handoff pre-gate failure: the image error value, the
// constant message naming the cause, and the byte pair a count or size verdict
// reports.
type handoffError struct {
	value     string
	message   string
	sizeBytes int64
	maxBytes  int64
}

func (e *handoffError) Error() string {
	return e.message
}

func handoffInvalid(message string) *handoffError {
	return &handoffError{value: imageErrInvalidHandoff, message: message}
}

// imageHandoffError builds the -32602 prompt image error for a handoff
// pre-gate failure. A byte verdict carries the size pair every other byte
// verdict carries and no message; every other verdict carries its constant
// message.
func imageHandoffError(failure *handoffError, index int) error {
	data := map[string]any{
		keyField:       acpFieldPromptImage,
		jsonFieldError: failure.value,
		keyIndex:       index,
	}

	if failure.message != "" {
		data[jsonFieldMessage] = failure.message
	}

	if failure.sizeBytes > 0 {
		data[keySizeBytes] = failure.sizeBytes
	}

	if failure.maxBytes > 0 {
		data[keyMaxBytes] = failure.maxBytes
	}

	return acp.NewInvalidParams(data)
}

// imageBlockIsHandoff reports whether an image block selects the handoff form:
// empty embedded data plus handoff intent. Non-empty data always wins, so a
// block carrying both bytes and a handoff envelope stays the embedded form.
func imageBlockIsHandoff(image *acp.ContentBlockImage) bool {
	return image.Data == "" && imageHandoffIntent(image)
}

// imageHandoffIntent reports whether a block tried to be a handoff block: it
// carries a handoff envelope or a file URI. Intent is what separates a
// malformed handoff block from a plain empty-data block, which stays
// missing_data.
func imageHandoffIntent(image *acp.ContentBlockImage) bool {
	if _, ok := image.Meta[handoffMetaKey]; ok {
		return true
	}

	if image.Uri == nil {
		return false
	}

	parsed, err := url.Parse(*image.Uri)

	return err == nil && parsed.Scheme == handoffURIScheme
}

// parseHandoffEnvelope decodes the acp-go.dev/handoff block envelope. It
// carries exactly version, digest, and sizeBytes; the defect classes are
// reported apart because an absent envelope, a non-object, a missing field, and
// an unknown field are four different mistakes on the host side.
func parseHandoffEnvelope(meta map[string]any) (handoffEnvelope, *handoffError) {
	value, present := meta[handoffMetaKey]
	if !present {
		return handoffEnvelope{}, handoffInvalid(handoffEnvelopeAbsentMessage)
	}

	object, ok := value.(map[string]any)
	if !ok {
		return handoffEnvelope{}, handoffInvalid(handoffEnvelopeNotObjectMessage)
	}

	for _, field := range [...]string{handoffFieldVersion, handoffFieldDigest, handoffFieldSizeBytes} {
		if _, present := object[field]; !present {
			return handoffEnvelope{}, handoffInvalid(handoffEnvelopeMissingFieldMessage)
		}
	}

	if len(object) != handoffEnvelopeFieldCount {
		return handoffEnvelope{}, handoffInvalid(handoffEnvelopeUnknownFieldMessage)
	}

	if !handoffVersionIsOne(object[handoffFieldVersion]) {
		return handoffEnvelope{}, handoffInvalid(handoffVersionMessage)
	}

	digest, ok := object[handoffFieldDigest].(string)
	if !ok || !isHandoffDigest(digest) {
		return handoffEnvelope{}, handoffInvalid(handoffDigestFormatMessage)
	}

	sizeBytes, ok := handoffSizeBytes(object[handoffFieldSizeBytes])
	if !ok {
		return handoffEnvelope{}, handoffInvalid(handoffSizeFormatMessage)
	}

	return handoffEnvelope{digest: digest, sizeBytes: sizeBytes}, nil
}

func handoffVersionIsOne(value any) bool {
	version, ok := handoffNumber(value)

	return ok && version == handoffVersion
}

// handoffNumber reads an envelope numeric as a float64 whatever shape it
// arrived in. A JSON number decodes to float64 under the pinned SDK and to
// json.Number if the decoder is ever asked for one, and an in-process host
// builds its own block metadata with a Go int. All three are the same number,
// and reading them as float64 keeps the range check off an int64 conversion
// whose out-of-range behaviour Go does not define.
func handoffNumber(value any) (float64, bool) {
	switch number := value.(type) {
	case float64:
		return number, true
	case json.Number:
		parsed, err := number.Float64()

		return parsed, err == nil
	case int:
		return float64(number), true
	default:
		return 0, false
	}
}

func isHandoffDigest(digest string) bool {
	if len(digest) != handoffDigestLength {
		return false
	}

	for _, char := range digest {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}

	return true
}

// handoffSizeBytes validates a declared byte count entirely in float64, before
// any int64 conversion: non-negative, integral, and strictly below 2^63.
func handoffSizeBytes(value any) (int64, bool) {
	size, ok := handoffNumber(value)
	if !ok || size < 0 || size != math.Trunc(size) || size >= handoffSizeBytesExclusiveMax {
		return 0, false
	}

	return int64(size), true
}

// handoffURIPath maps a handoff block URI to a cleaned local path. The URI must
// be a file URI with no remote host and an absolute path; nothing else is read.
func handoffURIPath(uri *string) (string, *handoffError) {
	if uri == nil || *uri == "" {
		return "", handoffInvalid(handoffURIRequiredMessage)
	}

	parsed, err := url.Parse(*uri)
	if err != nil {
		return "", handoffInvalid(handoffURIInvalidMessage)
	}

	if parsed.Scheme != handoffURIScheme {
		return "", handoffInvalid(handoffURISchemeMessage)
	}

	if parsed.Host != "" && parsed.Host != handoffURILocalHost {
		return "", handoffInvalid(handoffURIRemoteHostMessage)
	}

	path := filepath.FromSlash(parsed.Path)
	if !filepath.IsAbs(path) {
		return "", handoffInvalid(handoffURIRelativeMessage)
	}

	return filepath.Clean(path), nil
}

// handoffRelativePath maps an absolute handoff path to the name it has inside
// the read root. The lexical test only decides which member to blame for a path
// that was never under the root; the kernel, not this function, is what keeps a
// resolved path inside it.
func handoffRelativePath(root, path string) (string, *handoffError) {
	rel, err := filepath.Rel(filepath.Clean(root), path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", &handoffError{value: imageErrPathNotAllowed, message: handoffOutsideRootMessage}
	}

	return rel, nil
}

// readHandoffFile reads a handoff file by its name inside the read root, at
// most one byte more than the envelope declared.
//
// Nothing the filesystem reports feeds a verdict. The size the read is bounded
// by is the caller's own declared sizeBytes, already checked against the
// per-image gate, so a file that grew, shrank, or was swapped after the block
// was written fails verification rather than passing a size gate on a stale
// number. The descriptor is opened without blocking and required to be a
// regular file, because a root bounds where a path may lead and not what kind
// of object it names.
func readHandoffFile(ctx context.Context, root *os.Root, rel string, declared int64) ([]byte, *handoffError) {
	if ctx.Err() != nil {
		return nil, &handoffError{value: imageErrMissingFile, message: handoffUnreadableMessage}
	}

	file, err := openHandoffFile(root, rel)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, &handoffError{value: imageErrMissingFile, message: handoffAbsentMessage}
		}

		return nil, &handoffError{value: imageErrPathNotAllowed, message: handoffUnopenableMessage}
	}

	defer func() {
		_ = file.Close()
	}()

	info, err := file.Stat()
	if err != nil {
		return nil, &handoffError{value: imageErrMissingFile, message: handoffUninspectableMessage}
	}

	if !info.Mode().IsRegular() {
		return nil, &handoffError{value: imageErrPathNotAllowed, message: handoffNotRegularMessage}
	}

	data, err := io.ReadAll(io.LimitReader(file, declared+1))
	if err != nil {
		return nil, &handoffError{value: imageErrMissingFile, message: handoffUnreadableMessage}
	}

	return data, nil
}

// verifyHandoffBytes fails closed when the bytes read do not match the
// envelope's declared size and sha256 digest. The digest is recomputed over
// exactly the slice the native request will carry, so no byte reaches the
// harness unverified. Neither message reports what was observed: the caller
// already knows what it declared, and the file it named may not be one it is
// entitled to learn the length or the content hash of.
func verifyHandoffBytes(envelope handoffEnvelope, data []byte) *handoffError {
	if int64(len(data)) != envelope.sizeBytes {
		return &handoffError{value: imageErrHandoffDigestMismatch, message: handoffSizeMismatchMessage}
	}

	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != envelope.digest {
		return &handoffError{value: imageErrHandoffDigestMismatch, message: handoffDigestMismatchMessage}
	}

	return nil
}

// handoffRootHandle opens the read root once per prompt. A root that cannot be
// opened is a deployment defect rather than a host cleaning a file up early, so
// it is path_not_allowed however the open failed.
func (b *imagePromptBudget) handoffRootHandle() (*os.Root, *handoffError) {
	if b.root != nil {
		return b.root, nil
	}

	root, err := openHandoffRoot(b.handoffRoot)
	if err != nil {
		return nil, &handoffError{value: imageErrPathNotAllowed, message: handoffRootUnopenableMessage}
	}

	b.root = root

	return root, nil
}

// closeHandoffRoot releases the read root's descriptor at the end of the prompt
// mapping that opened it.
func (b *imagePromptBudget) closeHandoffRoot() {
	if b.root != nil {
		_ = b.root.Close()
		b.root = nil
	}
}

// handoffBytes runs the handoff pre-gate for one prompt image block: the block
// count bound, envelope and URI strictness, the declared media type, the
// declared size against the per-image gate, containment, a bounded
// root-relative read, then digest verification. Every path that returns bytes
// has verified them against the envelope the caller sent.
func (b *imagePromptBudget) handoffBytes(ctx context.Context, image *acp.ContentBlockImage) ([]byte, *handoffError) {
	if b.handoffRoot == "" {
		return nil, handoffInvalid(handoffRootUnsetMessage)
	}

	b.handoffBlocks++
	if b.handoffBlocks > maxHandoffBlocksPerPrompt {
		return nil, &handoffError{
			value:     imageErrTooLarge,
			sizeBytes: int64(b.handoffBlocks),
			maxBytes:  maxHandoffBlocksPerPrompt,
		}
	}

	envelope, failure := parseHandoffEnvelope(image.Meta)
	if failure != nil {
		return nil, failure
	}

	path, failure := handoffURIPath(image.Uri)
	if failure != nil {
		return nil, failure
	}

	// The declaration is judged in full before the filesystem is consulted at
	// all, as the declared type is in the embedded form. A block this adapter was
	// never going to accept costs it no open, no read and no hash, and its
	// refusal cannot report whether the path it named exists.
	if !isAllowlistedImageMime(image.MimeType) {
		return nil, &handoffError{value: imageErrInvalidMediaType}
	}

	// The size gate reads the caller's own declaration, so an oversize handoff
	// is rejected before anything is opened and without measuring a file the
	// caller may not be entitled to measure.
	if envelope.sizeBytes > b.perImage {
		return nil, &handoffError{
			value:     imageErrTooLarge,
			sizeBytes: envelope.sizeBytes,
			maxBytes:  b.perImage,
		}
	}

	rel, failure := handoffRelativePath(b.handoffRoot, path)
	if failure != nil {
		return nil, failure
	}

	root, failure := b.handoffRootHandle()
	if failure != nil {
		return nil, failure
	}

	data, failure := readHandoffFile(ctx, root, rel, envelope.sizeBytes)
	if failure != nil {
		return nil, failure
	}

	if failure := verifyHandoffBytes(envelope, data); failure != nil {
		return nil, failure
	}

	return data, nil
}
