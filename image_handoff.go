package hermesacp

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/coder/acp-go-sdk"
)

// Local-handoff image transport. A host that configures an input handoff root
// may replace an image block's embedded base64 with a digest-verified file
// under that root. The root is read-only: the adapter resolves, reads, and
// verifies bytes there and never writes, moves, or removes anything.
const (
	handoffMetaKey = "acp-go.dev/handoff"
	handoffVersion = 1

	handoffFieldVersion   = "version"
	handoffFieldDigest    = "digest"
	handoffFieldSizeBytes = "sizeBytes"

	handoffURIScheme    = "file"
	handoffURILocalHost = "localhost"
	handoffDigestLength = 64
)

// handoffStatFile inspects a resolved handoff path and handoffOpenFile opens it.
// Both are package variables so tests can exercise inspection, open, and read
// failures without depending on filesystem permissions.
var (
	handoffStatFile = os.Lstat
	handoffOpenFile = os.Open
)

// handoffRequest is a validated handoff envelope paired with the local path its
// block names.
type handoffRequest struct {
	path      string
	digest    string
	sizeBytes int64
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

// parseHandoffBlock validates a handoff block as a block: the configured root,
// the envelope's exact three fields, and the file URI. Every defect here is
// invalid_handoff, so a host can tell a bad block from a bad deployment.
func parseHandoffBlock(image *acp.ContentBlockImage, root string, index int) (handoffRequest, error) {
	if root == "" {
		return handoffRequest{}, imageHandoffError(imageErrInvalidHandoff, index, "the local handoff root is not configured")
	}

	envelope, ok := image.Meta[handoffMetaKey].(map[string]any)
	if !ok || len(envelope) != 3 {
		return handoffRequest{}, imageHandoffError(imageErrInvalidHandoff, index, "the "+handoffMetaKey+" envelope must carry exactly version, digest, and sizeBytes")
	}

	if !handoffVersionIsOne(envelope[handoffFieldVersion]) {
		return handoffRequest{}, imageHandoffError(imageErrInvalidHandoff, index, "the "+handoffMetaKey+" envelope version is unsupported")
	}

	digest, ok := envelope[handoffFieldDigest].(string)
	if !ok || !isHandoffDigest(digest) {
		return handoffRequest{}, imageHandoffError(imageErrInvalidHandoff, index, "the "+handoffMetaKey+" digest must be 64 lowercase hexadecimal characters")
	}

	sizeBytes, ok := handoffSizeBytes(envelope[handoffFieldSizeBytes])
	if !ok {
		return handoffRequest{}, imageHandoffError(imageErrInvalidHandoff, index, "the "+handoffMetaKey+" sizeBytes must be a non-negative integer")
	}

	path, err := handoffURIPath(image.Uri, index)
	if err != nil {
		return handoffRequest{}, err
	}

	return handoffRequest{path: path, digest: digest, sizeBytes: sizeBytes}, nil
}

func handoffVersionIsOne(value any) bool {
	switch version := value.(type) {
	case int:
		return version == handoffVersion
	case float64:
		return version == handoffVersion
	default:
		return false
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

func handoffSizeBytes(value any) (int64, bool) {
	switch size := value.(type) {
	case int:
		return int64(size), size >= 0
	case int64:
		return size, size >= 0
	case float64:
		if size != float64(int64(size)) {
			return 0, false
		}

		return int64(size), size >= 0
	default:
		return 0, false
	}
}

// handoffURIPath extracts the local path a handoff block names. The URI must be
// a file URI with no remote host and an absolute path; nothing else is read.
func handoffURIPath(uri *string, index int) (string, error) {
	if uri == nil || *uri == "" {
		return "", imageHandoffError(imageErrInvalidHandoff, index, "a handoff block requires a file URI")
	}

	parsed, err := url.Parse(*uri)
	if err != nil {
		return "", imageHandoffError(imageErrInvalidHandoff, index, "the handoff URI is not a valid URI")
	}

	if parsed.Scheme != handoffURIScheme {
		return "", imageHandoffError(imageErrInvalidHandoff, index, "the handoff URI scheme must be file")
	}

	if parsed.Host != "" && parsed.Host != handoffURILocalHost {
		return "", imageHandoffError(imageErrInvalidHandoff, index, "the handoff URI must not name a remote host")
	}

	path := filepath.FromSlash(parsed.Path)
	if !filepath.IsAbs(path) {
		return "", imageHandoffError(imageErrInvalidHandoff, index, "the handoff URI must carry an absolute path")
	}

	return path, nil
}

// resolveHandoffPath resolves a handoff path inside the configured root without
// following an escape. Containment is checked lexically on the cleaned path,
// symlinks are then resolved, and containment is re-checked on the resolved
// path before its mode is read: a path outside the root, an escaping symlink,
// and a non-regular file are all path_not_allowed, while a path inside the root
// that does not exist is missing_file.
func resolveHandoffPath(root, path string, index int) (string, fs.FileInfo, error) {
	cleanRoot := filepath.Clean(root)
	if !pathWithinRoot(cleanRoot, filepath.Clean(path)) {
		return "", nil, imageHandoffError(imageErrPathNotAllowed, index, "the handoff path is outside the configured handoff root")
	}

	resolvedRoot, err := filepath.EvalSymlinks(cleanRoot)
	if err != nil {
		return "", nil, handoffResolutionError(err, index, "the configured handoff root does not exist", "the configured handoff root could not be resolved")
	}

	resolved, err := filepath.EvalSymlinks(filepath.Clean(path))
	if err != nil {
		return "", nil, handoffResolutionError(err, index, "the handoff path does not exist", "the handoff path could not be resolved")
	}

	if !pathWithinRoot(resolvedRoot, resolved) {
		return "", nil, imageHandoffError(imageErrPathNotAllowed, index, "the handoff path resolves outside the configured handoff root")
	}

	info, err := handoffStatFile(resolved)
	if err != nil {
		return "", nil, handoffResolutionError(err, index, "the handoff path does not exist", "the handoff path could not be inspected")
	}

	if !info.Mode().IsRegular() {
		return "", nil, imageHandoffError(imageErrPathNotAllowed, index, "the handoff path is not a regular file")
	}

	return resolved, info, nil
}

// handoffResolutionError separates an absent path from a path that exists but
// cannot be resolved: the first is the expected operational failure when a host
// cleans up early, the second is a containment failure.
func handoffResolutionError(err error, index int, missingMessage, resolveMessage string) error {
	if errors.Is(err, fs.ErrNotExist) {
		return imageHandoffError(imageErrMissingFile, index, missingMessage)
	}

	return imageHandoffError(imageErrPathNotAllowed, index, resolveMessage)
}

// pathWithinRoot reports whether a cleaned path is the root or sits under it.
func pathWithinRoot(root, path string) bool {
	if path == root {
		return true
	}

	return strings.HasPrefix(path, root+string(filepath.Separator))
}

// readHandoffFile reads at most bound+1 bytes so an oversize file is detected
// without being held. The reported size is the file's real size, so a too_large
// verdict names what the host actually offered.
func readHandoffFile(resolved string, info fs.FileInfo, bound int64, index int) ([]byte, int64, error) {
	file, err := handoffOpenFile(resolved)
	if err != nil {
		return nil, 0, imageHandoffError(imageErrMissingFile, index, "the handoff file could not be opened")
	}

	defer func() {
		_ = file.Close()
	}()

	data, err := io.ReadAll(io.LimitReader(file, bound+1))
	if err != nil {
		return nil, 0, imageHandoffError(imageErrMissingFile, index, "the handoff file could not be read")
	}

	size := int64(len(data))
	if size > bound && info.Size() > size {
		size = info.Size()
	}

	return data, size, nil
}

// verifyHandoffBytes fails closed on any disagreement between the file and its
// declared identity. Verification is possible only when the whole file fit
// inside the read bound; a file above the bound is rejected by the byte gate
// that follows, so unverified bytes never reach the native harness.
func verifyHandoffBytes(request handoffRequest, data []byte, size, bound int64, index int) error {
	if size > bound {
		return nil
	}

	if size != request.sizeBytes {
		return imageHandoffError(imageErrHandoffDigestMismatch, index, "the handoff file size does not match the declared sizeBytes")
	}

	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != request.digest {
		return imageHandoffError(imageErrHandoffDigestMismatch, index, "the handoff file bytes do not match the declared digest")
	}

	return nil
}
