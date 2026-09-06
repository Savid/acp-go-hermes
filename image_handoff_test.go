package hermesacp

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
	"github.com/stretchr/testify/require"
)

func TestHandoffCapabilityScalar(t *testing.T) {
	response, err := newTestAgent(WithInputHandoffRoot(durableTempDir(t))).Initialize(t.Context(), acp.InitializeRequest{})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"version": 1}, response.AgentCapabilities.Meta["acp-go.dev/handoff"])
}

// keyFilename is the native attachment field neither input form derives, kept
// here so the tests that pin its absence name it.
const keyFilename = "filename"

func handoffFileURI(path string) string {
	return (&url.URL{Scheme: handoffURIScheme, Path: handoffURIPathOf(path)}).String()
}

// handoffURIPathOf spells a local path the way a file URI's path component
// must: rooted, and with forward slashes. A Windows path starts at its drive
// letter, so without the leading slash it would render as an authority.
func handoffURIPathOf(path string) string {
	slashed := filepath.ToSlash(path)
	if !strings.HasPrefix(slashed, "/") {
		slashed = "/" + slashed
	}

	return slashed
}

func writeHandoffFile(t *testing.T, root, name string, data []byte) string {
	t.Helper()

	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("create handoff directory: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write handoff file: %v", err)
	}

	return path
}

func handoffDigest(data []byte) string {
	sum := sha256.Sum256(data)

	return hex.EncodeToString(sum[:])
}

func handoffEnvelopeFor(data []byte) map[string]any {
	return map[string]any{
		handoffFieldVersion:   handoffVersion,
		handoffFieldDigest:    handoffDigest(data),
		handoffFieldSizeBytes: len(data),
	}
}

func handoffBlock(path, mimeType string, envelope map[string]any) acp.ContentBlock {
	uri := handoffFileURI(path)
	image := &acp.ContentBlockImage{Type: "image", MimeType: mimeType, Uri: &uri}

	if envelope != nil {
		image.Meta = map[string]any{handoffMetaKey: envelope}
	}

	return acp.ContentBlock{Image: image}
}

// stagedHandoff writes a fixture under a fresh handoff root and returns the
// root, the block naming it, and the bytes the host declared.
func stagedHandoff(t *testing.T, fixture, mimeType string) (string, acp.ContentBlock, []byte) {
	t.Helper()

	root := durableTempDir(t)
	data := fixtureBytes(t, fixture)
	path := writeHandoffFile(t, root, filepath.Join("session-1", "operation-1", fixture), data)

	return root, handoffBlock(path, mimeType, handoffEnvelopeFor(data)), data
}

func TestHandoffFormAcceptsEveryAllowlistedFormat(t *testing.T) {
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
			root, block, data := stagedHandoff(t, test.name, test.mimeType)

			parts, err := promptToHermesParts(t.Context(), []acp.ContentBlock{block}, ImageLimits{}, root)
			if err != nil {
				t.Fatalf("promptToHermesParts: %v", err)
			}
			if !reflect.DeepEqual(parts, []map[string]any{{
				keyType: valFile,
				keyMime: test.mimeType,
				keyData: data,
			}}) {
				t.Fatalf("handoff part = %#v", parts)
			}
		})
	}
}

// TestHandoffAndEmbeddedFormsBuildIdenticalNativeParts pins that the transport
// a host chooses is invisible below the adapter. Both fixtures carry a uri: the
// claim is proved on the shape that carries the most provenance, not on one
// that omits the field.
func TestHandoffAndEmbeddedFormsBuildIdenticalNativeParts(t *testing.T) {
	root, handoff, data := stagedHandoff(t, "valid.png", mimePNG)
	embeddedURI := "https://example.test/provenance.png"

	handoffParts, err := promptToHermesParts(t.Context(), []acp.ContentBlock{acp.TextBlock("look"), handoff}, ImageLimits{}, root)
	if err != nil {
		t.Fatalf("handoff form: %v", err)
	}

	embeddedParts, err := promptToHermesParts(t.Context(), []acp.ContentBlock{acp.TextBlock("look"), {Image: &acp.ContentBlockImage{
		Type: "image", Data: base64.StdEncoding.EncodeToString(data), MimeType: mimePNG, Uri: &embeddedURI,
	}}}, ImageLimits{}, root)
	if err != nil {
		t.Fatalf("embedded form: %v", err)
	}

	if !reflect.DeepEqual(handoffParts, embeddedParts) {
		t.Fatalf("handoff parts = %#v, embedded parts = %#v", handoffParts, embeddedParts)
	}
	if _, ok := embeddedParts[1][keyFilename]; ok {
		t.Fatalf("embedded part derived a native filename from its uri: %#v", embeddedParts[1])
	}
}

// TestHandoffPathNeverReachesNativeRequest pins the no-pass-through rule on the
// request the wrapper actually sends: the host-owned path may not outlive the
// validation read.
func TestHandoffPathNeverReachesNativeRequest(t *testing.T) {
	root, block, data := stagedHandoff(t, "valid.png", mimePNG)
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	agent := newTestAgent(WithInputHandoffRoot(root))
	agent.setAgentClient(conn)
	session := testSession(agent, client)

	var sent nativehermes.MessageRequest

	client.sendMessage = func(_ context.Context, id string, req nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		sent = req

		return nativehermes.NativeMessage{Info: nativehermes.NativeMessageInfo{ID: "assistant", SessionID: id, Role: "assistant", Finish: "stop"}}, nil
	}

	resp, err := session.Prompt(context.Background(), acp.PromptRequest{
		Meta:      turnRouteMeta("test-turn"),
		SessionId: session.id,
		Prompt:    []acp.ContentBlock{block},
	})
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if resp.StopReason != acp.StopReasonEndTurn {
		t.Fatalf("stop reason = %q", resp.StopReason)
	}

	encoded, err := json.Marshal(sent)
	if err != nil {
		t.Fatalf("marshal native request: %v", err)
	}

	for _, leaked := range []string{root, "operation-1", "valid.png", handoffDigest(data), handoffMetaKey} {
		if strings.Contains(string(encoded), leaked) {
			t.Fatalf("native request leaked %q: %s", leaked, encoded)
		}
	}

	if len(sent.Parts) != 1 || sent.Parts[0][keyFilename] != nil {
		t.Fatalf("native parts = %#v", sent.Parts)
	}

	if decoded, _ := sent.Parts[0][keyData].([]byte); !reflect.DeepEqual(decoded, data) {
		t.Fatalf("native bytes differ from the handoff file")
	}
}

func TestHandoffFormSelection(t *testing.T) {
	root := durableTempDir(t)
	png := fixtureBytes(t, "valid.png")
	path := writeHandoffFile(t, root, "valid.png", png)
	jpeg := fixtureBytes(t, "valid.jpg")

	t.Run("embedded data wins over a handoff envelope", func(t *testing.T) {
		block := handoffBlock(path, mimeJPEG, handoffEnvelopeFor(png))
		block.Image.Data = base64.StdEncoding.EncodeToString(jpeg)

		parts, err := promptToHermesParts(t.Context(), []acp.ContentBlock{block}, ImageLimits{}, root)
		if err != nil {
			t.Fatalf("promptToHermesParts: %v", err)
		}
		if decoded, _ := parts[0][keyData].([]byte); !reflect.DeepEqual(decoded, jpeg) {
			t.Fatal("embedded bytes did not win over the handoff file")
		}
	})

	t.Run("empty data without handoff intent stays missing_data", func(t *testing.T) {
		remote := "https://example.test/pixels.png"
		unparsable := "file://%zz"

		for name, uri := range map[string]*string{
			"no uri":            nil,
			"remote uri":        &remote,
			"unparsable uri":    &unparsable,
			"empty string uri":  acp.Ptr(""),
			"data uri":          acp.Ptr("data:image/png;base64,AAAA"),
			"relative file uri": acp.Ptr("File-Not-A-Scheme/x.png"),
		} {
			t.Run(name, func(t *testing.T) {
				_, err := promptToHermesParts(t.Context(), []acp.ContentBlock{{Image: &acp.ContentBlockImage{
					Type: "image", MimeType: mimePNG, Uri: uri,
				}}}, ImageLimits{}, root)
				requireImageInputError(t, err, map[string]any{
					jsonFieldField: acpFieldPromptImage,
					jsonFieldError: imageErrMissingData,
					keyIndex:       0,
				})
			})
		}
	})

	t.Run("a file uri alone signals handoff intent", func(t *testing.T) {
		uri := handoffFileURI(path)

		_, err := promptToHermesParts(t.Context(), []acp.ContentBlock{{Image: &acp.ContentBlockImage{
			Type: "image", MimeType: mimePNG, Uri: &uri,
		}}}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrInvalidHandoff, 0, handoffEnvelopeAbsentMessage)
	})

	t.Run("an envelope alone signals handoff intent", func(t *testing.T) {
		_, err := promptToHermesParts(t.Context(), []acp.ContentBlock{{Image: &acp.ContentBlockImage{
			Type: "image", MimeType: mimePNG, Meta: map[string]any{handoffMetaKey: handoffEnvelopeFor(png)},
		}}}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrInvalidHandoff, 0, handoffURIRequiredMessage)
	})
}

func TestHandoffBlockDefectsAreInvalidHandoff(t *testing.T) {
	root := durableTempDir(t)
	png := fixtureBytes(t, "valid.png")
	path := writeHandoffFile(t, root, "valid.png", png)
	digest := handoffDigest(png)

	envelopeWith := func(mutate func(map[string]any)) map[string]any {
		envelope := handoffEnvelopeFor(png)
		mutate(envelope)

		return envelope
	}

	for _, test := range []struct {
		name    string
		block   acp.ContentBlock
		root    string
		message string
	}{
		{
			name:    "root unset",
			block:   handoffBlock(path, mimePNG, handoffEnvelopeFor(png)),
			message: handoffRootUnsetMessage,
		},
		{
			name:    "envelope absent",
			block:   handoffBlock(path, mimePNG, nil),
			root:    root,
			message: handoffEnvelopeAbsentMessage,
		},
		{
			name: "envelope not an object",
			block: acp.ContentBlock{Image: &acp.ContentBlockImage{
				Type: "image", MimeType: mimePNG, Uri: acp.Ptr(handoffFileURI(path)),
				Meta: map[string]any{handoffMetaKey: "version=1"},
			}},
			root:    root,
			message: handoffEnvelopeNotObjectMessage,
		},
		{
			name: "unknown envelope field",
			block: handoffBlock(path, mimePNG, envelopeWith(func(envelope map[string]any) {
				envelope["compression"] = "none"
			})),
			root:    root,
			message: handoffEnvelopeUnknownFieldMessage,
		},
		{
			name: "missing envelope field",
			block: handoffBlock(path, mimePNG, envelopeWith(func(envelope map[string]any) {
				delete(envelope, handoffFieldSizeBytes)
			})),
			root:    root,
			message: handoffEnvelopeMissingFieldMessage,
		},
		{
			name: "three fields under the wrong names",
			block: handoffBlock(path, mimePNG, envelopeWith(func(envelope map[string]any) {
				delete(envelope, handoffFieldSizeBytes)
				envelope["size"] = len(png)
			})),
			root:    root,
			message: handoffEnvelopeMissingFieldMessage,
		},
		{
			name: "unsupported version",
			block: handoffBlock(path, mimePNG, envelopeWith(func(envelope map[string]any) {
				envelope[handoffFieldVersion] = 2
			})),
			root:    root,
			message: handoffVersionMessage,
		},
		{
			name: "version is not a number",
			block: handoffBlock(path, mimePNG, envelopeWith(func(envelope map[string]any) {
				envelope[handoffFieldVersion] = "1"
			})),
			root:    root,
			message: handoffVersionMessage,
		},
		{
			name: "digest is not a string",
			block: handoffBlock(path, mimePNG, envelopeWith(func(envelope map[string]any) {
				envelope[handoffFieldDigest] = 1
			})),
			root:    root,
			message: handoffDigestFormatMessage,
		},
		{
			name: "digest too short",
			block: handoffBlock(path, mimePNG, envelopeWith(func(envelope map[string]any) {
				envelope[handoffFieldDigest] = digest[:63]
			})),
			root:    root,
			message: handoffDigestFormatMessage,
		},
		{
			name: "digest is uppercase",
			block: handoffBlock(path, mimePNG, envelopeWith(func(envelope map[string]any) {
				envelope[handoffFieldDigest] = strings.ToUpper(digest)
			})),
			root:    root,
			message: handoffDigestFormatMessage,
		},
		{
			name: "size is not a number",
			block: handoffBlock(path, mimePNG, envelopeWith(func(envelope map[string]any) {
				envelope[handoffFieldSizeBytes] = "70"
			})),
			root:    root,
			message: handoffSizeFormatMessage,
		},
		{
			name: "size is negative",
			block: handoffBlock(path, mimePNG, envelopeWith(func(envelope map[string]any) {
				envelope[handoffFieldSizeBytes] = -1
			})),
			root:    root,
			message: handoffSizeFormatMessage,
		},
		{
			name: "size is fractional",
			block: handoffBlock(path, mimePNG, envelopeWith(func(envelope map[string]any) {
				envelope[handoffFieldSizeBytes] = 70.5
			})),
			root:    root,
			message: handoffSizeFormatMessage,
		},
		{
			name: "size is negative and fractional",
			block: handoffBlock(path, mimePNG, envelopeWith(func(envelope map[string]any) {
				envelope[handoffFieldSizeBytes] = float64(-70)
			})),
			root:    root,
			message: handoffSizeFormatMessage,
		},
		{
			name: "size is at the int64 boundary",
			block: handoffBlock(path, mimePNG, envelopeWith(func(envelope map[string]any) {
				envelope[handoffFieldSizeBytes] = handoffSizeBytesExclusiveMax
			})),
			root:    root,
			message: handoffSizeFormatMessage,
		},
		{
			name: "uri unparsable",
			block: acp.ContentBlock{Image: &acp.ContentBlockImage{
				Type: "image", MimeType: mimePNG, Uri: acp.Ptr("file://%zz"),
				Meta: map[string]any{handoffMetaKey: handoffEnvelopeFor(png)},
			}},
			root:    root,
			message: handoffURIInvalidMessage,
		},
		{
			name: "uri scheme is not file",
			block: acp.ContentBlock{Image: &acp.ContentBlockImage{
				Type: "image", MimeType: mimePNG, Uri: acp.Ptr("https://example.test/valid.png"),
				Meta: map[string]any{handoffMetaKey: handoffEnvelopeFor(png)},
			}},
			root:    root,
			message: handoffURISchemeMessage,
		},
		{
			name: "uri names a remote host",
			block: acp.ContentBlock{Image: &acp.ContentBlockImage{
				Type: "image", MimeType: mimePNG, Uri: acp.Ptr("file://example.test/valid.png"),
				Meta: map[string]any{handoffMetaKey: handoffEnvelopeFor(png)},
			}},
			root:    root,
			message: handoffURIRemoteHostMessage,
		},
		{
			name: "uri path is not absolute",
			block: acp.ContentBlock{Image: &acp.ContentBlockImage{
				Type: "image", MimeType: mimePNG, Uri: acp.Ptr("file:valid.png"),
				Meta: map[string]any{handoffMetaKey: handoffEnvelopeFor(png)},
			}},
			root:    root,
			message: handoffURIRelativeMessage,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := promptToHermesParts(t.Context(), []acp.ContentBlock{test.block}, ImageLimits{}, test.root)
			requireHandoffError(t, err, imageErrInvalidHandoff, 0, test.message)
		})
	}
}

// TestHandoffEnvelopeAcceptsNumbersFromADecoder drives the envelope through the
// decoder the SDK actually uses and through the same decoder asked for
// json.Number, because the shapes a host can produce are the decoder's, not the
// ones a Go literal happens to build.
func TestHandoffEnvelopeAcceptsNumbersFromADecoder(t *testing.T) {
	root := durableTempDir(t)
	png := fixtureBytes(t, "valid.png")
	path := writeHandoffFile(t, root, "valid.png", png)

	raw := fmt.Sprintf(`{"version":1,"digest":%q,"sizeBytes":%d}`, handoffDigest(png), len(png))

	for _, useNumber := range []bool{false, true} {
		t.Run(fmt.Sprintf("useNumber=%v", useNumber), func(t *testing.T) {
			decoder := json.NewDecoder(strings.NewReader(raw))
			if useNumber {
				decoder.UseNumber()
			}

			var envelope map[string]any
			if err := decoder.Decode(&envelope); err != nil {
				t.Fatalf("decode envelope: %v", err)
			}

			parts, err := promptToHermesParts(t.Context(), []acp.ContentBlock{handoffBlock(path, mimePNG, envelope)}, ImageLimits{}, root)
			if err != nil {
				t.Fatalf("promptToHermesParts: %v", err)
			}
			if decoded, _ := parts[0][keyData].([]byte); !reflect.DeepEqual(decoded, png) {
				t.Fatalf("decoded bytes differ from the handoff file")
			}
		})
	}

	t.Run("a decoder number out of the int64 range is refused", func(t *testing.T) {
		decoder := json.NewDecoder(strings.NewReader(fmt.Sprintf(`{"version":1,"digest":%q,"sizeBytes":1e400}`, handoffDigest(png))))
		decoder.UseNumber()

		var envelope map[string]any
		if err := decoder.Decode(&envelope); err != nil {
			t.Fatalf("decode envelope: %v", err)
		}

		_, err := promptToHermesParts(t.Context(), []acp.ContentBlock{handoffBlock(path, mimePNG, envelope)}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrInvalidHandoff, 0, handoffSizeFormatMessage)
	})
}

func TestHandoffPathContainment(t *testing.T) {
	png := fixtureBytes(t, "valid.png")

	t.Run("outside the root", func(t *testing.T) {
		root := durableTempDir(t)
		outside := writeHandoffFile(t, durableTempDir(t), "valid.png", png)

		_, err := promptToHermesParts(t.Context(), []acp.ContentBlock{handoffBlock(outside, mimePNG, handoffEnvelopeFor(png))}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrPathNotAllowed, 0, handoffOutsideRootMessage)
	})

	t.Run("percent-encoded traversal out of the root", func(t *testing.T) {
		root := durableTempDir(t)
		outside := writeHandoffFile(t, durableTempDir(t), "secret.png", png)
		uri := "file://" + handoffURIPathOf(root) + "/%2e%2e/" + filepath.Base(filepath.Dir(outside)) + "/secret.png"

		_, err := promptToHermesParts(t.Context(), []acp.ContentBlock{{Image: &acp.ContentBlockImage{
			Type: "image", MimeType: mimePNG, Uri: &uri,
			Meta: map[string]any{handoffMetaKey: handoffEnvelopeFor(png)},
		}}}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrPathNotAllowed, 0, handoffOutsideRootMessage)
	})

	t.Run("unresolvable symlink cycle", func(t *testing.T) {
		root := durableTempDir(t)
		first := filepath.Join(root, "first.png")
		second := filepath.Join(root, "second.png")

		if err := os.Symlink(second, first); err != nil {
			t.Fatalf("symlink: %v", err)
		}
		if err := os.Symlink(first, second); err != nil {
			t.Fatalf("symlink: %v", err)
		}

		_, err := promptToHermesParts(t.Context(), []acp.ContentBlock{handoffBlock(first, mimePNG, handoffEnvelopeFor(png))}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrPathNotAllowed, 0, handoffUnopenableMessage)
	})

	t.Run("directory", func(t *testing.T) {
		root := durableTempDir(t)
		writeHandoffFile(t, root, filepath.Join("session-1", "valid.png"), png)

		_, err := promptToHermesParts(t.Context(), []acp.ContentBlock{handoffBlock(filepath.Join(root, "session-1"), mimePNG, handoffEnvelopeFor(png))}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrPathNotAllowed, 0, handoffNotRegularMessage)
	})

	t.Run("the root itself", func(t *testing.T) {
		root := durableTempDir(t)

		_, err := promptToHermesParts(t.Context(), []acp.ContentBlock{handoffBlock(root, mimePNG, handoffEnvelopeFor(png))}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrPathNotAllowed, 0, handoffNotRegularMessage)
	})

	t.Run("localhost host is local", func(t *testing.T) {
		root := durableTempDir(t)
		path := writeHandoffFile(t, root, "valid.png", png)
		uri := "file://" + handoffURILocalHost + handoffURIPathOf(path)

		if _, err := promptToHermesParts(t.Context(), []acp.ContentBlock{{Image: &acp.ContentBlockImage{
			Type: "image", MimeType: mimePNG, Uri: &uri,
			Meta: map[string]any{handoffMetaKey: handoffEnvelopeFor(png)},
		}}}, ImageLimits{}, root); err != nil {
			t.Fatalf("promptToHermesParts: %v", err)
		}
	})
}

// TestHandoffSymlinkContainmentIsKernelEnforced drives real links against a real
// root: containment is the kernel's answer at open time, so there is no window
// between deciding a path is inside the root and reading it.
func TestHandoffSymlinkContainmentIsKernelEnforced(t *testing.T) {
	png := fixtureBytes(t, "valid.png")

	outsideDir := durableTempDir(t)
	if err := os.WriteFile(filepath.Join(outsideDir, "secret.png"), png, 0o600); err != nil {
		t.Fatalf("write outside file: %v", err)
	}

	for _, test := range []struct {
		name    string
		link    string
		target  string
		value   string
		message string
	}{
		{
			name:   "a relative link inside the root resolves",
			link:   "inside.png",
			target: "valid.png",
		},
		{
			name:    "a relative link out of the root is refused",
			link:    "escape.png",
			target:  filepath.Join("..", filepath.Base(outsideDir), "secret.png"),
			value:   imageErrPathNotAllowed,
			message: handoffUnopenableMessage,
		},
		{
			name:    "an absolute link is refused even inside the root",
			link:    "absolute.png",
			value:   imageErrPathNotAllowed,
			message: handoffUnopenableMessage,
		},
		{
			name:    "a link whose target was cleaned up is missing",
			link:    "dangling.png",
			target:  "gone.png",
			value:   imageErrMissingFile,
			message: handoffAbsentMessage,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := durableTempDir(t)
			if err := os.WriteFile(filepath.Join(root, "valid.png"), png, 0o600); err != nil {
				t.Fatalf("write handoff file: %v", err)
			}

			target := test.target
			if target == "" {
				target = filepath.Join(root, "valid.png")
			}

			link := filepath.Join(root, test.link)
			if err := os.Symlink(target, link); err != nil {
				t.Fatalf("symlink: %v", err)
			}

			parts, err := promptToHermesParts(t.Context(), []acp.ContentBlock{handoffBlock(link, mimePNG, handoffEnvelopeFor(png))}, ImageLimits{}, root)
			if test.value == "" {
				if err != nil {
					t.Fatalf("promptToHermesParts: %v", err)
				}
				if decoded, _ := parts[0][keyData].([]byte); !reflect.DeepEqual(decoded, png) {
					t.Fatal("in-root link did not read the file it names")
				}

				return
			}

			requireHandoffError(t, err, test.value, 0, test.message)
		})
	}
}

func TestHandoffMissingFile(t *testing.T) {
	png := fixtureBytes(t, "valid.png")

	t.Run("path inside the root vanished", func(t *testing.T) {
		root := durableTempDir(t)
		path := writeHandoffFile(t, root, "valid.png", png)
		block := handoffBlock(path, mimePNG, handoffEnvelopeFor(png))

		if err := os.Remove(path); err != nil {
			t.Fatalf("remove handoff file: %v", err)
		}

		_, err := promptToHermesParts(t.Context(), []acp.ContentBlock{block}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrMissingFile, 0, handoffAbsentMessage)
	})

	// A root that cannot be opened is a deployment defect rather than a host
	// cleaning a file up early, so it is path_not_allowed however the open
	// failed — the two verdicts exist to tell those apart.
	t.Run("root does not exist", func(t *testing.T) {
		root := filepath.Join(durableTempDir(t), "absent")

		_, err := promptToHermesParts(t.Context(), []acp.ContentBlock{handoffBlock(filepath.Join(root, "valid.png"), mimePNG, handoffEnvelopeFor(png))}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrPathNotAllowed, 0, handoffRootUnopenableMessage)
	})

	t.Run("root is a regular file", func(t *testing.T) {
		root := writeHandoffFile(t, durableTempDir(t), "root.png", png)

		_, err := promptToHermesParts(t.Context(), []acp.ContentBlock{handoffBlock(filepath.Join(root, "valid.png"), mimePNG, handoffEnvelopeFor(png))}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrPathNotAllowed, 0, handoffRootUnopenableMessage)
	})

	t.Run("file cannot be inspected", func(t *testing.T) {
		root := durableTempDir(t)
		path := writeHandoffFile(t, root, "valid.png", png)

		restore := openHandoffFile
		openHandoffFile = func(*os.Root, string) (handoffFile, error) {
			return stubHandoffFile{statErr: errors.New("stat refused")}, nil
		}

		defer func() { openHandoffFile = restore }()

		_, err := promptToHermesParts(t.Context(), []acp.ContentBlock{handoffBlock(path, mimePNG, handoffEnvelopeFor(png))}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrMissingFile, 0, handoffUninspectableMessage)
	})

	t.Run("file cannot be read", func(t *testing.T) {
		root := durableTempDir(t)
		path := writeHandoffFile(t, root, "valid.png", png)

		restore := openHandoffFile
		openHandoffFile = func(*os.Root, string) (handoffFile, error) {
			return stubHandoffFile{readErr: errors.New("read refused")}, nil
		}

		defer func() { openHandoffFile = restore }()

		_, err := promptToHermesParts(t.Context(), []acp.ContentBlock{handoffBlock(path, mimePNG, handoffEnvelopeFor(png))}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrMissingFile, 0, handoffUnreadableMessage)
	})
}

// stubHandoffFile stands in for an opened descriptor whose inspection or read
// fails, which on a real filesystem needs a race with the host that owns the
// file. Containment is still the root's: the seam only replaces what the
// descriptor answers.
type stubHandoffFile struct {
	statErr error
	readErr error
}

func (f stubHandoffFile) Read([]byte) (int, error) {
	return 0, f.readErr
}

func (f stubHandoffFile) Close() error {
	return nil
}

func (f stubHandoffFile) Stat() (os.FileInfo, error) {
	if f.statErr != nil {
		return nil, f.statErr
	}

	return regularFileInfo{}, nil
}

// regularFileInfo reports the mode a stubbed descriptor needs to reach the read.
type regularFileInfo struct{ os.FileInfo }

func (regularFileInfo) Mode() os.FileMode {
	return 0o600
}

func TestHandoffDigestVerificationFailsClosed(t *testing.T) {
	png := fixtureBytes(t, "valid.png")

	t.Run("bytes tampered after the envelope was built", func(t *testing.T) {
		root := durableTempDir(t)
		envelope := handoffEnvelopeFor(png)
		tampered := append([]byte(nil), png...)
		tampered[len(tampered)-1] ^= 0xFF
		path := writeHandoffFile(t, root, "valid.png", tampered)

		_, err := promptToHermesParts(t.Context(), []acp.ContentBlock{handoffBlock(path, mimePNG, envelope)}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrHandoffDigestMismatch, 0, handoffDigestMismatchMessage)
	})

	t.Run("the file is shorter than the declaration", func(t *testing.T) {
		root := durableTempDir(t)
		envelope := handoffEnvelopeFor(png)
		path := writeHandoffFile(t, root, "valid.png", png[:len(png)-1])

		_, err := promptToHermesParts(t.Context(), []acp.ContentBlock{handoffBlock(path, mimePNG, envelope)}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrHandoffDigestMismatch, 0, handoffSizeMismatchMessage)
	})
}

// TestHandoffRunsTheEmbeddedGateChain mirrors the embedded-form taxonomy over
// bytes that arrived as files.
func TestHandoffRunsTheEmbeddedGateChain(t *testing.T) {
	for _, test := range []struct {
		name     string
		fixture  string
		mimeType string
		want     string
	}{
		{name: "non-canonical media type", fixture: "valid.png", mimeType: "image/PNG", want: imageErrInvalidMediaType},
		{name: "unsupported media type", fixture: "valid.png", mimeType: "image/svg+xml", want: imageErrInvalidMediaType},
		{name: "unknown raster", fixture: "mismatch.png", mimeType: mimeGIF, want: imageErrMediaTypeMismatch},
		{name: "declared versus sniffed", fixture: "valid.jpg", mimeType: mimePNG, want: imageErrMediaTypeMismatch},
		{name: "invalid dimensions", fixture: "truncated.png", mimeType: mimePNG, want: imageErrInvalidDimensions},
		{name: "animated", fixture: "animated.gif", mimeType: mimeGIF, want: imageErrAnimatedNotSupported},
	} {
		t.Run(test.name, func(t *testing.T) {
			root, block, _ := stagedHandoff(t, test.fixture, test.mimeType)

			_, err := promptToHermesParts(t.Context(), []acp.ContentBlock{block}, ImageLimits{}, root)
			requireImageInputError(t, err, map[string]any{
				jsonFieldField: acpFieldPromptImage,
				jsonFieldError: test.want,
				keyIndex:       0,
			})
		})
	}
}

// TestHandoffDeclaredMediaTypeIsJudgedBeforeTheFilesystem pins the pre-gate
// order: a declaration this adapter was never going to accept costs it no open,
// no read and no hash. The absent name proves the verdict needs no file at all;
// the name outside the root proves the type outranks the location, so a
// bad-MIME probe cannot learn whether the file it named is there.
func TestHandoffDeclaredMediaTypeIsJudgedBeforeTheFilesystem(t *testing.T) {
	png := fixtureBytes(t, "valid.png")
	root := durableTempDir(t)
	outside := writeHandoffFile(t, durableTempDir(t), "outside.png", png)

	for _, test := range []struct {
		name string
		path string
	}{
		{name: "the name does not exist", path: filepath.Join(root, "absent.png")},
		{name: "the name leaves the root", path: outside},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := promptToHermesParts(t.Context(), []acp.ContentBlock{
				handoffBlock(test.path, "image/svg+xml", handoffEnvelopeFor(png)),
			}, ImageLimits{}, root)
			requireImageInputError(t, err, map[string]any{
				jsonFieldField: acpFieldPromptImage,
				jsonFieldError: imageErrInvalidMediaType,
				keyIndex:       0,
			})
		})
	}
}

func TestHandoffByteGates(t *testing.T) {
	png := fixtureBytes(t, "valid.png")
	size := int64(len(png))

	t.Run("per-image limit reports the declared size", func(t *testing.T) {
		root, block, _ := stagedHandoff(t, "valid.png", mimePNG)

		_, err := promptToHermesParts(t.Context(), []acp.ContentBlock{block}, ImageLimits{MaxInputBytesPerImage: size - 1}, root)
		requireImageInputError(t, err, map[string]any{
			jsonFieldField: acpFieldPromptImage,
			jsonFieldError: imageErrTooLarge,
			keyIndex:       0,
			keySizeBytes:   size,
			keyMaxBytes:    size - 1,
		})
	})

	t.Run("handoff bytes join the per-prompt aggregate", func(t *testing.T) {
		root, handoff, data := stagedHandoff(t, "valid.png", mimePNG)
		embedded := acp.ContentBlock{Image: &acp.ContentBlockImage{
			Type: "image", Data: base64.StdEncoding.EncodeToString(data), MimeType: mimePNG,
		}}

		_, err := promptToHermesParts(t.Context(), []acp.ContentBlock{embedded, handoff}, ImageLimits{
			MaxInputBytesPerPrompt: size*2 - 1,
		}, root)
		requireImageInputError(t, err, map[string]any{
			jsonFieldField: acpFieldPromptImage,
			jsonFieldError: imageErrTooLarge,
			keyIndex:       1,
			keySizeBytes:   size * 2,
			keyMaxBytes:    size*2 - 1,
		})
	})

	t.Run("a disabled per-image limit still verifies the file", func(t *testing.T) {
		root, block, _ := stagedHandoff(t, "valid.png", mimePNG)

		if _, err := promptToHermesParts(t.Context(), []acp.ContentBlock{block}, ImageLimits{MaxInputBytesPerImage: 0}, root); err != nil {
			t.Fatalf("promptToHermesParts: %v", err)
		}
	})
}

// TestHandoffOversizeReadIsRejectedWithoutForwardingBytes pins that no byte
// leaves the read unverified: the declaration is judged before anything is
// opened, and a file that disagrees with its declaration is rejected rather than
// truncated into the native request.
func TestHandoffOversizeReadIsRejectedWithoutForwardingBytes(t *testing.T) {
	png := fixtureBytes(t, "valid.png")
	bound := int64(len(png))
	limits := ImageLimits{MaxInputBytesPerImage: bound}

	t.Run("a declared size past the gate is rejected before anything is opened", func(t *testing.T) {
		root := durableTempDir(t)
		outside := writeHandoffFile(t, durableTempDir(t), "outside.png", png)

		// No file is written inside the root and the second name would be
		// refused for its location, so the only thing that can produce too_large
		// for either is the caller's own declaration, judged ahead of the open.
		envelope := handoffEnvelopeFor(png)
		envelope[handoffFieldSizeBytes] = int(bound + 1)

		for _, path := range []string{filepath.Join(root, "valid.png"), outside} {
			_, err := promptToHermesParts(t.Context(), []acp.ContentBlock{
				handoffBlock(path, mimePNG, envelope),
			}, limits, root)
			requireImageInputError(t, err, map[string]any{
				jsonFieldField: acpFieldPromptImage,
				jsonFieldError: imageErrTooLarge,
				keyIndex:       0,
				keySizeBytes:   bound + 1,
				keyMaxBytes:    bound,
			})
		}
	})

	t.Run("a file larger than its declaration forwards nothing", func(t *testing.T) {
		root := durableTempDir(t)

		// The file on disk holds one byte more than the envelope describes,
		// which is what a file appended to after the block was written looks
		// like. Its bytes were never verified, so none may survive the read.
		grown := make([]byte, bound+1)
		copy(grown, png)
		path := writeHandoffFile(t, root, "valid.png", grown)

		parts, err := promptToHermesParts(t.Context(), []acp.ContentBlock{
			handoffBlock(path, mimePNG, handoffEnvelopeFor(png)),
		}, ImageLimits{MaxInputBytesPerImage: bound + 1}, root)
		requireHandoffError(t, err, imageErrHandoffDigestMismatch, 0, handoffSizeMismatchMessage)

		if parts != nil {
			t.Fatalf("parts built over unverified bytes: %#v", parts)
		}
	})
}

// TestHandoffBlockCountCapRejectsWithAggregateDisabled pins the bound on work
// rather than on bytes: every block here is a small valid image and the byte
// aggregate is disabled, so nothing but the count can reject any of them.
func TestHandoffBlockCountCapRejectsWithAggregateDisabled(t *testing.T) {
	root := durableTempDir(t)
	png := fixtureBytes(t, "valid.png")
	path := writeHandoffFile(t, root, "valid.png", png)

	blocks := make([]acp.ContentBlock, 0, maxHandoffBlocksPerPrompt+1)
	for range maxHandoffBlocksPerPrompt + 1 {
		blocks = append(blocks, handoffBlock(path, mimePNG, handoffEnvelopeFor(png)))
	}

	_, err := promptToHermesParts(t.Context(), blocks, ImageLimits{MaxInputBytesPerPrompt: 0}, root)
	requireImageInputError(t, err, map[string]any{
		jsonFieldField: acpFieldPromptImage,
		jsonFieldError: imageErrTooLarge,
		keyIndex:       maxHandoffBlocksPerPrompt,
		keySizeBytes:   int64(maxHandoffBlocksPerPrompt + 1),
		keyMaxBytes:    int64(maxHandoffBlocksPerPrompt),
	})

	accepted, err := promptToHermesParts(t.Context(), blocks[:maxHandoffBlocksPerPrompt], ImageLimits{MaxInputBytesPerPrompt: 0}, root)
	if err != nil {
		t.Fatalf("promptToHermesParts: %v", err)
	}
	if len(accepted) != maxHandoffBlocksPerPrompt {
		t.Fatalf("accepted parts = %d, want %d", len(accepted), maxHandoffBlocksPerPrompt)
	}
}

func TestHandoffReadHonoursACancelledContext(t *testing.T) {
	root := durableTempDir(t)
	png := fixtureBytes(t, "valid.png")
	path := writeHandoffFile(t, root, "valid.png", png)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := promptToHermesParts(ctx, []acp.ContentBlock{handoffBlock(path, mimePNG, handoffEnvelopeFor(png))}, ImageLimits{}, root)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("prompt mapping error = %v, want context cancellation", err)
	}

	budget := newImagePromptBudget(ImageLimits{}, root)

	defer budget.closeHandoffRoot()

	handle, failure := budget.handoffRootHandle()
	if failure != nil {
		t.Fatalf("open handoff root: %v", failure)
	}

	_, readFailure := readHandoffFile(ctx, handle, "valid.png", int64(len(png)))
	if readFailure == nil || readFailure.value != imageErrMissingFile {
		t.Fatalf("cancelled read failure = %#v", readFailure)
	}
}

// rootSnapshot records every entry under root with the identity and size that
// would change if the adapter wrote, moved, or removed anything.
func rootSnapshot(t *testing.T, root string) map[string]string {
	t.Helper()

	snapshot := map[string]string{}

	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		info, statErr := entry.Info()
		if statErr != nil {
			return statErr
		}

		snapshot[path] = handoffEntryStamp(info)

		return nil
	}); err != nil {
		t.Fatalf("walk handoff root: %v", err)
	}

	return snapshot
}

func TestHandoffReadNeverMutatesTheRoot(t *testing.T) {
	root, block, data := stagedHandoff(t, "valid.png", mimePNG)

	before := rootSnapshot(t, root)

	parts, err := promptToHermesParts(t.Context(), []acp.ContentBlock{block}, ImageLimits{}, root)
	if err != nil {
		t.Fatalf("promptToHermesParts: %v", err)
	}
	if decoded, _ := parts[0][keyData].([]byte); !reflect.DeepEqual(decoded, data) {
		t.Fatal("native bytes differ from the handoff file")
	}

	// The root is a read root: a turn that consumed a file from it leaves the
	// tree byte-for-byte as it found it.
	if after := rootSnapshot(t, root); !reflect.DeepEqual(before, after) {
		t.Fatalf("handoff root changed: before %#v, after %#v", before, after)
	}
}

// TestHandoffMessagesCarryNoObservedValues drives real failures across the whole
// taxonomy and requires every client-visible message to be one of this file's
// declared constants. A verdict travels to the caller and into telemetry, so the
// set it can draw from is closed by construction.
func TestHandoffMessagesCarryNoObservedValues(t *testing.T) {
	root := durableTempDir(t)
	png := fixtureBytes(t, "valid.png")
	gif := fixtureBytes(t, "valid.gif")

	path := writeHandoffFile(t, root, "valid.png", png)
	outside := writeHandoffFile(t, durableTempDir(t), "outside.png", png)

	tampered := append([]byte(nil), png...)
	tampered[len(tampered)-1] ^= 0xFF
	tamperedPath := writeHandoffFile(t, root, "tampered.png", tampered)

	allowed := map[string]bool{
		handoffRootUnsetMessage:            true,
		handoffRootUnopenableMessage:       true,
		handoffEnvelopeAbsentMessage:       true,
		handoffEnvelopeNotObjectMessage:    true,
		handoffEnvelopeMissingFieldMessage: true,
		handoffEnvelopeUnknownFieldMessage: true,
		handoffVersionMessage:              true,
		handoffDigestFormatMessage:         true,
		handoffSizeFormatMessage:           true,
		handoffURIRequiredMessage:          true,
		handoffURIInvalidMessage:           true,
		handoffURISchemeMessage:            true,
		handoffURIRemoteHostMessage:        true,
		handoffURIRelativeMessage:          true,
		handoffOutsideRootMessage:          true,
		handoffUnopenableMessage:           true,
		handoffAbsentMessage:               true,
		handoffUninspectableMessage:        true,
		handoffNotRegularMessage:           true,
		handoffUnreadableMessage:           true,
		handoffSizeMismatchMessage:         true,
		handoffDigestMismatchMessage:       true,
	}

	for _, block := range []acp.ContentBlock{
		handoffBlock(path, mimePNG, nil),
		handoffBlock(outside, mimePNG, handoffEnvelopeFor(png)),
		handoffBlock(filepath.Join(root, "absent.png"), mimePNG, handoffEnvelopeFor(png)),
		handoffBlock(tamperedPath, mimePNG, handoffEnvelopeFor(png)),
		handoffBlock(path, mimePNG, handoffEnvelopeFor(gif)),
		handoffBlock(root, mimePNG, handoffEnvelopeFor(png)),
	} {
		_, err := promptToHermesParts(t.Context(), []acp.ContentBlock{block}, ImageLimits{}, root)

		var requestErr *acp.RequestError
		if !errors.As(err, &requestErr) {
			t.Fatalf("error = %v, want ACP request error", err)
		}

		data, _ := requestErr.Data.(map[string]any)

		message, _ := data[jsonFieldMessage].(string)
		if !allowed[message] {
			t.Fatalf("message %q is not a declared constant", message)
		}
	}
}

func TestTextResourceBytesCountTowardPromptAggregate(t *testing.T) {
	text := strings.Repeat("a", 4096)

	// Declaring bytes as text rather than as a blob must not buy a prompt more
	// of them than the aggregate allows.
	blocks := []acp.ContentBlock{acp.ResourceBlock(acp.EmbeddedResourceResource{
		TextResourceContents: &acp.TextResourceContents{Uri: "file:///a.txt", Text: text},
	})}

	parts, err := promptToHermesParts(t.Context(), blocks, ImageLimits{MaxInputBytesPerPrompt: int64(len(text))}, "")
	if err != nil {
		t.Fatalf("promptToHermesParts: %v", err)
	}
	if parts[0][valText] != text {
		t.Fatalf("text part = %#v", parts[0])
	}

	_, err = promptToHermesParts(t.Context(), blocks, ImageLimits{MaxInputBytesPerPrompt: int64(len(text)) - 1}, "")
	requireImageInputError(t, err, map[string]any{
		jsonFieldField: acpFieldPromptResource,
		jsonFieldError: imageErrTooLarge,
		keyIndex:       0,
		keySizeBytes:   int64(len(text)),
		keyMaxBytes:    int64(len(text)) - 1,
	})
}

func TestHandoffErrorReportsItsCause(t *testing.T) {
	var failure error = &handoffError{value: imageErrMissingFile, message: handoffAbsentMessage}

	if failure.Error() != handoffAbsentMessage {
		t.Fatalf("handoff error = %q", failure)
	}
}

func TestHandoffRelativePath(t *testing.T) {
	root := filepath.Join(string(filepath.Separator), "root")

	rel, failure := handoffRelativePath(root, filepath.Join(root, "sub", "a.png"))
	if failure != nil || rel != filepath.Join("sub", "a.png") {
		t.Fatalf("in-root path = %q failure=%#v", rel, failure)
	}

	// A sibling whose name merely starts with the root is not under it.
	for _, path := range []string{
		filepath.Join(string(filepath.Separator), "rootx", "a.png"),
		filepath.Join(string(filepath.Separator), "etc", "passwd"),
	} {
		if _, failure := handoffRelativePath(root, path); failure == nil || failure.value != imageErrPathNotAllowed {
			t.Fatalf("out-of-root path %q failure = %#v", path, failure)
		}
	}

	// The root itself is relative to itself, and is refused later for not being
	// a regular file rather than for being out of the root.
	if rel, failure := handoffRelativePath(root, root); failure != nil || rel != "." {
		t.Fatalf("root path = %q failure=%#v", rel, failure)
	}
}

// requireHandoffError requires the whole client-visible payload of a handoff
// verdict: the four keys it may carry and nothing else, and a message equal to
// the declared constant. Equality rather than containment is what makes this a
// regression guard — an appended path, byte count, or operating-system error
// string fails here instead of passing a substring check.
func requireHandoffError(t *testing.T, err error, want string, index int, wantMessage string) {
	t.Helper()

	var requestErr *acp.RequestError
	if !errors.As(err, &requestErr) {
		t.Fatalf("error = %v, want ACP request error", err)
	}
	if requestErr.Code != -32602 {
		t.Fatalf("code = %d, want -32602", requestErr.Code)
	}

	data, _ := requestErr.Data.(map[string]any)
	if !reflect.DeepEqual(data, map[string]any{
		jsonFieldField:   acpFieldPromptImage,
		jsonFieldError:   want,
		keyIndex:         index,
		jsonFieldMessage: wantMessage,
	}) {
		t.Fatalf("data = %#v, want %s at index %d with message %q", data, want, index, wantMessage)
	}
}

var _ io.ReadCloser = stubHandoffFile{}
