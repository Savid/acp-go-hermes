package hermesacp

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

func handoffFileURI(path string) string {
	return (&url.URL{Scheme: handoffURIScheme, Path: filepath.ToSlash(path)}).String()
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

func handoffEnvelope(data []byte) map[string]any {
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

	root := t.TempDir()
	data := fixtureBytes(t, fixture)
	path := writeHandoffFile(t, root, filepath.Join("session-1", "operation-1", fixture), data)

	return root, handoffBlock(path, mimeType, handoffEnvelope(data)), data
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

			parts, err := promptToHermesParts([]acp.ContentBlock{block}, ImageLimits{}, root)
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
// a host chooses is invisible below the adapter.
func TestHandoffAndEmbeddedFormsBuildIdenticalNativeParts(t *testing.T) {
	root, handoff, data := stagedHandoff(t, "valid.png", mimePNG)

	handoffParts, err := promptToHermesParts([]acp.ContentBlock{acp.TextBlock("look"), handoff}, ImageLimits{}, root)
	if err != nil {
		t.Fatalf("handoff form: %v", err)
	}

	embeddedParts, err := promptToHermesParts([]acp.ContentBlock{acp.TextBlock("look"), {Image: &acp.ContentBlockImage{
		Type: "image", Data: base64.StdEncoding.EncodeToString(data), MimeType: mimePNG,
	}}}, ImageLimits{}, root)
	if err != nil {
		t.Fatalf("embedded form: %v", err)
	}

	if !reflect.DeepEqual(handoffParts, embeddedParts) {
		t.Fatalf("handoff parts = %#v, embedded parts = %#v", handoffParts, embeddedParts)
	}
}

// TestHandoffPathNeverReachesNativeRequest pins the no-pass-through rule on the
// request the wrapper actually sends: the host-owned path may not outlive the
// validation read.
func TestHandoffPathNeverReachesNativeRequest(t *testing.T) {
	root, block, data := stagedHandoff(t, "valid.png", mimePNG)
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	agent := NewAgent(WithInputHandoffRoot(root))
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
	root := t.TempDir()
	png := fixtureBytes(t, "valid.png")
	path := writeHandoffFile(t, root, "valid.png", png)
	jpeg := fixtureBytes(t, "valid.jpg")

	t.Run("embedded data wins over a handoff envelope", func(t *testing.T) {
		block := handoffBlock(path, mimeJPEG, handoffEnvelope(png))
		block.Image.Data = base64.StdEncoding.EncodeToString(jpeg)

		parts, err := promptToHermesParts([]acp.ContentBlock{block}, ImageLimits{}, root)
		if err != nil {
			t.Fatalf("promptToHermesParts: %v", err)
		}
		if decoded, _ := parts[0][keyData].([]byte); !reflect.DeepEqual(decoded, jpeg) {
			t.Fatal("embedded bytes did not win over the handoff file")
		}
		if parts[0][keyFilename] != "valid.png" {
			t.Fatalf("embedded form dropped its filename provenance: %#v", parts[0])
		}
	})

	t.Run("empty data without handoff intent stays missing_data", func(t *testing.T) {
		remote := "https://example.test/pixels.png"
		unparseable := "file://%zz"

		for name, uri := range map[string]*string{
			"no uri":            nil,
			"remote uri":        &remote,
			"unparseable uri":   &unparseable,
			"empty string uri":  acp.Ptr(""),
			"data uri":          acp.Ptr("data:image/png;base64,AAAA"),
			"relative file uri": acp.Ptr("File-Not-A-Scheme/x.png"),
		} {
			t.Run(name, func(t *testing.T) {
				_, err := promptToHermesParts([]acp.ContentBlock{{Image: &acp.ContentBlockImage{
					Type: "image", MimeType: mimePNG, Uri: uri,
				}}}, ImageLimits{}, root)
				requireImageInputError(t, err, map[string]any{
					keyField:       acpFieldPromptImage,
					jsonFieldError: imageErrMissingData,
					keyIndex:       0,
				})
			})
		}
	})

	t.Run("a file uri alone signals handoff intent", func(t *testing.T) {
		uri := handoffFileURI(path)

		_, err := promptToHermesParts([]acp.ContentBlock{{Image: &acp.ContentBlockImage{
			Type: "image", MimeType: mimePNG, Uri: &uri,
		}}}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrInvalidHandoff, 0, "must carry exactly version, digest, and sizeBytes")
	})

	t.Run("an envelope alone signals handoff intent", func(t *testing.T) {
		_, err := promptToHermesParts([]acp.ContentBlock{{Image: &acp.ContentBlockImage{
			Type: "image", MimeType: mimePNG, Meta: map[string]any{handoffMetaKey: handoffEnvelope(png)},
		}}}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrInvalidHandoff, 0, "requires a file URI")
	})
}

func TestHandoffBlockDefectsAreInvalidHandoff(t *testing.T) {
	root := t.TempDir()
	png := fixtureBytes(t, "valid.png")
	path := writeHandoffFile(t, root, "valid.png", png)
	digest := handoffDigest(png)

	envelopeWith := func(mutate func(map[string]any)) map[string]any {
		envelope := handoffEnvelope(png)
		mutate(envelope)

		return envelope
	}

	for _, test := range []struct {
		name     string
		block    acp.ContentBlock
		root     string
		contains string
	}{
		{
			name:     "root unset",
			block:    handoffBlock(path, mimePNG, handoffEnvelope(png)),
			contains: "the local handoff root is not configured",
		},
		{
			name:     "envelope absent",
			block:    handoffBlock(path, mimePNG, nil),
			root:     root,
			contains: "exactly version, digest, and sizeBytes",
		},
		{
			name: "envelope not an object",
			block: acp.ContentBlock{Image: &acp.ContentBlockImage{
				Type: "image", MimeType: mimePNG, Uri: acp.Ptr(handoffFileURI(path)),
				Meta: map[string]any{handoffMetaKey: "version=1"},
			}},
			root:     root,
			contains: "exactly version, digest, and sizeBytes",
		},
		{
			name: "unknown envelope field",
			block: handoffBlock(path, mimePNG, envelopeWith(func(envelope map[string]any) {
				envelope["compression"] = "none"
			})),
			root:     root,
			contains: "exactly version, digest, and sizeBytes",
		},
		{
			name: "missing envelope field",
			block: handoffBlock(path, mimePNG, envelopeWith(func(envelope map[string]any) {
				delete(envelope, handoffFieldSizeBytes)
			})),
			root:     root,
			contains: "exactly version, digest, and sizeBytes",
		},
		{
			name: "unsupported version",
			block: handoffBlock(path, mimePNG, envelopeWith(func(envelope map[string]any) {
				envelope[handoffFieldVersion] = 2
			})),
			root:     root,
			contains: "version is unsupported",
		},
		{
			name: "version is not a number",
			block: handoffBlock(path, mimePNG, envelopeWith(func(envelope map[string]any) {
				envelope[handoffFieldVersion] = "1"
			})),
			root:     root,
			contains: "version is unsupported",
		},
		{
			name: "digest is not a string",
			block: handoffBlock(path, mimePNG, envelopeWith(func(envelope map[string]any) {
				envelope[handoffFieldDigest] = 1
			})),
			root:     root,
			contains: "64 lowercase hexadecimal characters",
		},
		{
			name: "digest too short",
			block: handoffBlock(path, mimePNG, envelopeWith(func(envelope map[string]any) {
				envelope[handoffFieldDigest] = digest[:63]
			})),
			root:     root,
			contains: "64 lowercase hexadecimal characters",
		},
		{
			name: "digest is uppercase",
			block: handoffBlock(path, mimePNG, envelopeWith(func(envelope map[string]any) {
				envelope[handoffFieldDigest] = strings.ToUpper(digest)
			})),
			root:     root,
			contains: "64 lowercase hexadecimal characters",
		},
		{
			name: "size is not a number",
			block: handoffBlock(path, mimePNG, envelopeWith(func(envelope map[string]any) {
				envelope[handoffFieldSizeBytes] = "70"
			})),
			root:     root,
			contains: "non-negative integer",
		},
		{
			name: "size is negative",
			block: handoffBlock(path, mimePNG, envelopeWith(func(envelope map[string]any) {
				envelope[handoffFieldSizeBytes] = -1
			})),
			root:     root,
			contains: "non-negative integer",
		},
		{
			name: "size is fractional",
			block: handoffBlock(path, mimePNG, envelopeWith(func(envelope map[string]any) {
				envelope[handoffFieldSizeBytes] = 70.5
			})),
			root:     root,
			contains: "non-negative integer",
		},
		{
			name: "size is negative and fractional",
			block: handoffBlock(path, mimePNG, envelopeWith(func(envelope map[string]any) {
				envelope[handoffFieldSizeBytes] = float64(-70)
			})),
			root:     root,
			contains: "non-negative integer",
		},
		{
			name: "uri unparseable",
			block: acp.ContentBlock{Image: &acp.ContentBlockImage{
				Type: "image", MimeType: mimePNG, Uri: acp.Ptr("file://%zz"),
				Meta: map[string]any{handoffMetaKey: handoffEnvelope(png)},
			}},
			root:     root,
			contains: "not a valid URI",
		},
		{
			name: "uri scheme is not file",
			block: acp.ContentBlock{Image: &acp.ContentBlockImage{
				Type: "image", MimeType: mimePNG, Uri: acp.Ptr("https://example.test/valid.png"),
				Meta: map[string]any{handoffMetaKey: handoffEnvelope(png)},
			}},
			root:     root,
			contains: "scheme must be file",
		},
		{
			name: "uri names a remote host",
			block: acp.ContentBlock{Image: &acp.ContentBlockImage{
				Type: "image", MimeType: mimePNG, Uri: acp.Ptr("file://example.test/valid.png"),
				Meta: map[string]any{handoffMetaKey: handoffEnvelope(png)},
			}},
			root:     root,
			contains: "must not name a remote host",
		},
		{
			name: "uri path is not absolute",
			block: acp.ContentBlock{Image: &acp.ContentBlockImage{
				Type: "image", MimeType: mimePNG, Uri: acp.Ptr("file:valid.png"),
				Meta: map[string]any{handoffMetaKey: handoffEnvelope(png)},
			}},
			root:     root,
			contains: "must carry an absolute path",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := promptToHermesParts([]acp.ContentBlock{test.block}, ImageLimits{}, test.root)
			requireHandoffError(t, err, imageErrInvalidHandoff, 0, test.contains)
		})
	}
}

// TestHandoffEnvelopeAcceptsDecodedNumberShapes covers the JSON number shapes a
// decoded envelope carries over the wire.
func TestHandoffEnvelopeAcceptsDecodedNumberShapes(t *testing.T) {
	root := t.TempDir()
	png := fixtureBytes(t, "valid.png")
	path := writeHandoffFile(t, root, "valid.png", png)

	for _, test := range []struct {
		name    string
		version any
		size    any
	}{
		{name: "json numbers", version: float64(1), size: float64(len(png))},
		{name: "int64 size", version: handoffVersion, size: int64(len(png))},
	} {
		t.Run(test.name, func(t *testing.T) {
			block := handoffBlock(path, mimePNG, map[string]any{
				handoffFieldVersion:   test.version,
				handoffFieldDigest:    handoffDigest(png),
				handoffFieldSizeBytes: test.size,
			})

			if _, err := promptToHermesParts([]acp.ContentBlock{block}, ImageLimits{}, root); err != nil {
				t.Fatalf("promptToHermesParts: %v", err)
			}
		})
	}
}

func TestHandoffPathContainment(t *testing.T) {
	png := fixtureBytes(t, "valid.png")

	t.Run("outside the root", func(t *testing.T) {
		root := t.TempDir()
		outside := writeHandoffFile(t, t.TempDir(), "valid.png", png)

		_, err := promptToHermesParts([]acp.ContentBlock{handoffBlock(outside, mimePNG, handoffEnvelope(png))}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrPathNotAllowed, 0, "is outside the configured handoff root")
	})

	t.Run("escaping symlink", func(t *testing.T) {
		root := t.TempDir()
		outside := writeHandoffFile(t, t.TempDir(), "valid.png", png)
		link := filepath.Join(root, "escape.png")

		if err := os.Symlink(outside, link); err != nil {
			t.Fatalf("symlink: %v", err)
		}

		_, err := promptToHermesParts([]acp.ContentBlock{handoffBlock(link, mimePNG, handoffEnvelope(png))}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrPathNotAllowed, 0, "resolves outside the configured handoff root")
	})

	t.Run("symlink inside the root resolves", func(t *testing.T) {
		root := t.TempDir()
		target := writeHandoffFile(t, root, filepath.Join("bytes", "valid.png"), png)
		link := filepath.Join(root, "link.png")

		if err := os.Symlink(target, link); err != nil {
			t.Fatalf("symlink: %v", err)
		}

		if _, err := promptToHermesParts([]acp.ContentBlock{handoffBlock(link, mimePNG, handoffEnvelope(png))}, ImageLimits{}, root); err != nil {
			t.Fatalf("promptToHermesParts: %v", err)
		}
	})

	t.Run("unresolvable symlink cycle", func(t *testing.T) {
		root := t.TempDir()
		first := filepath.Join(root, "first.png")
		second := filepath.Join(root, "second.png")

		if err := os.Symlink(second, first); err != nil {
			t.Fatalf("symlink: %v", err)
		}
		if err := os.Symlink(first, second); err != nil {
			t.Fatalf("symlink: %v", err)
		}

		_, err := promptToHermesParts([]acp.ContentBlock{handoffBlock(first, mimePNG, handoffEnvelope(png))}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrPathNotAllowed, 0, "the handoff path could not be resolved")
	})

	t.Run("unresolvable root", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "cycle")
		if err := os.Symlink(root, root); err != nil {
			t.Fatalf("symlink: %v", err)
		}

		_, err := promptToHermesParts([]acp.ContentBlock{handoffBlock(filepath.Join(root, "valid.png"), mimePNG, handoffEnvelope(png))}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrPathNotAllowed, 0, "the configured handoff root could not be resolved")
	})

	t.Run("directory", func(t *testing.T) {
		root := t.TempDir()
		writeHandoffFile(t, root, filepath.Join("session-1", "valid.png"), png)

		_, err := promptToHermesParts([]acp.ContentBlock{handoffBlock(filepath.Join(root, "session-1"), mimePNG, handoffEnvelope(png))}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrPathNotAllowed, 0, "is not a regular file")
	})

	t.Run("the root itself", func(t *testing.T) {
		root := t.TempDir()

		_, err := promptToHermesParts([]acp.ContentBlock{handoffBlock(root, mimePNG, handoffEnvelope(png))}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrPathNotAllowed, 0, "is not a regular file")
	})

	t.Run("localhost host is local", func(t *testing.T) {
		root := t.TempDir()
		path := writeHandoffFile(t, root, "valid.png", png)
		uri := "file://" + handoffURILocalHost + filepath.ToSlash(path)

		if _, err := promptToHermesParts([]acp.ContentBlock{{Image: &acp.ContentBlockImage{
			Type: "image", MimeType: mimePNG, Uri: &uri,
			Meta: map[string]any{handoffMetaKey: handoffEnvelope(png)},
		}}}, ImageLimits{}, root); err != nil {
			t.Fatalf("promptToHermesParts: %v", err)
		}
	})
}

func TestHandoffMissingFile(t *testing.T) {
	png := fixtureBytes(t, "valid.png")

	t.Run("path inside the root vanished", func(t *testing.T) {
		root := t.TempDir()
		path := writeHandoffFile(t, root, "valid.png", png)
		block := handoffBlock(path, mimePNG, handoffEnvelope(png))

		if err := os.Remove(path); err != nil {
			t.Fatalf("remove handoff file: %v", err)
		}

		_, err := promptToHermesParts([]acp.ContentBlock{block}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrMissingFile, 0, "the handoff path does not exist")
	})

	t.Run("root does not exist", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "absent")

		_, err := promptToHermesParts([]acp.ContentBlock{handoffBlock(filepath.Join(root, "valid.png"), mimePNG, handoffEnvelope(png))}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrMissingFile, 0, "the configured handoff root does not exist")
	})

	t.Run("file cannot be opened", func(t *testing.T) {
		root := t.TempDir()
		path := writeHandoffFile(t, root, "valid.png", png)

		restore := handoffOpenFile
		handoffOpenFile = func(string) (*os.File, error) {
			return nil, errors.New("open refused")
		}

		defer func() { handoffOpenFile = restore }()

		_, err := promptToHermesParts([]acp.ContentBlock{handoffBlock(path, mimePNG, handoffEnvelope(png))}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrMissingFile, 0, "could not be opened")
	})

	t.Run("file cannot be read", func(t *testing.T) {
		root := t.TempDir()
		path := writeHandoffFile(t, root, "valid.png", png)

		restore := handoffOpenFile
		handoffOpenFile = func(name string) (*os.File, error) {
			return os.OpenFile(name, os.O_WRONLY, 0o600)
		}

		defer func() { handoffOpenFile = restore }()

		_, err := promptToHermesParts([]acp.ContentBlock{handoffBlock(path, mimePNG, handoffEnvelope(png))}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrMissingFile, 0, "could not be read")
	})

	t.Run("file cannot be inspected", func(t *testing.T) {
		root := t.TempDir()
		path := writeHandoffFile(t, root, "valid.png", png)

		restore := handoffStatFile
		handoffStatFile = func(string) (os.FileInfo, error) {
			return nil, errors.New("stat refused")
		}

		defer func() { handoffStatFile = restore }()

		_, err := promptToHermesParts([]acp.ContentBlock{handoffBlock(path, mimePNG, handoffEnvelope(png))}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrPathNotAllowed, 0, "could not be inspected")
	})
}

func TestHandoffDigestVerificationFailsClosed(t *testing.T) {
	png := fixtureBytes(t, "valid.png")

	t.Run("bytes tampered after the envelope was built", func(t *testing.T) {
		root := t.TempDir()
		envelope := handoffEnvelope(png)
		tampered := append([]byte(nil), png...)
		tampered[len(tampered)-1] ^= 0xFF
		path := writeHandoffFile(t, root, "valid.png", tampered)

		_, err := promptToHermesParts([]acp.ContentBlock{handoffBlock(path, mimePNG, envelope)}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrHandoffDigestMismatch, 0, "do not match the declared digest")
	})

	t.Run("size disagrees with the file", func(t *testing.T) {
		root := t.TempDir()
		envelope := handoffEnvelope(png)
		envelope[handoffFieldSizeBytes] = len(png) - 1
		path := writeHandoffFile(t, root, "valid.png", png)

		_, err := promptToHermesParts([]acp.ContentBlock{handoffBlock(path, mimePNG, envelope)}, ImageLimits{}, root)
		requireHandoffError(t, err, imageErrHandoffDigestMismatch, 0, "does not match the declared sizeBytes")
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

			_, err := promptToHermesParts([]acp.ContentBlock{block}, ImageLimits{}, root)
			requireImageInputError(t, err, map[string]any{
				keyField:       acpFieldPromptImage,
				jsonFieldError: test.want,
				keyIndex:       0,
			})
		})
	}
}

func TestHandoffByteGates(t *testing.T) {
	png := fixtureBytes(t, "valid.png")
	size := int64(len(png))

	t.Run("per-image limit reports the file size", func(t *testing.T) {
		root, block, _ := stagedHandoff(t, "valid.png", mimePNG)

		_, err := promptToHermesParts([]acp.ContentBlock{block}, ImageLimits{MaxInputBytesPerImage: size - 1}, root)
		requireImageInputError(t, err, map[string]any{
			keyField:       acpFieldPromptImage,
			jsonFieldError: imageErrTooLarge,
			keyIndex:       0,
			keySizeBytes:   size,
			keyMaxBytes:    size - 1,
		})
	})

	t.Run("a file past the read bound reports its real size", func(t *testing.T) {
		root := t.TempDir()
		padded := append(append([]byte(nil), png...), make([]byte, 512)...)
		path := writeHandoffFile(t, root, "padded.png", padded)
		bound := size + 8

		_, err := promptToHermesParts([]acp.ContentBlock{handoffBlock(path, mimePNG, handoffEnvelope(padded))},
			ImageLimits{MaxInputBytesPerImage: bound}, root)
		requireImageInputError(t, err, map[string]any{
			keyField:       acpFieldPromptImage,
			jsonFieldError: imageErrTooLarge,
			keyIndex:       0,
			keySizeBytes:   int64(len(padded)),
			keyMaxBytes:    bound,
		})
	})

	t.Run("handoff bytes join the per-prompt aggregate", func(t *testing.T) {
		root, handoff, data := stagedHandoff(t, "valid.png", mimePNG)
		embedded := acp.ContentBlock{Image: &acp.ContentBlockImage{
			Type: "image", Data: base64.StdEncoding.EncodeToString(data), MimeType: mimePNG,
		}}

		_, err := promptToHermesParts([]acp.ContentBlock{embedded, handoff}, ImageLimits{
			MaxInputBytesPerPrompt: size*2 - 1,
		}, root)
		requireImageInputError(t, err, map[string]any{
			keyField:       acpFieldPromptImage,
			jsonFieldError: imageErrTooLarge,
			keyIndex:       1,
			keySizeBytes:   size * 2,
			keyMaxBytes:    size*2 - 1,
		})
	})

	t.Run("a disabled per-image limit still verifies the file", func(t *testing.T) {
		root, block, _ := stagedHandoff(t, "valid.png", mimePNG)

		if _, err := promptToHermesParts([]acp.ContentBlock{block}, ImageLimits{MaxInputBytesPerImage: 0}, root); err != nil {
			t.Fatalf("promptToHermesParts: %v", err)
		}
	})
}

func requireHandoffError(t *testing.T, err error, want string, index int, messageContains string) {
	t.Helper()

	var requestErr *acp.RequestError
	if !errors.As(err, &requestErr) {
		t.Fatalf("error = %v, want ACP request error", err)
	}
	if requestErr.Code != -32602 {
		t.Fatalf("code = %d, want -32602", requestErr.Code)
	}

	data, _ := requestErr.Data.(map[string]any)
	if data[keyField] != acpFieldPromptImage || data[jsonFieldError] != want || data[keyIndex] != index {
		t.Fatalf("data = %#v, want %s at index %d", data, want, index)
	}

	message, _ := data[jsonFieldMessage].(string)
	if !strings.Contains(message, messageContains) {
		t.Fatalf("message = %q, want it to contain %q", message, messageContains)
	}
	if len(data) != 4 {
		t.Fatalf("data = %#v, want exactly field, error, index, and message", data)
	}
}
