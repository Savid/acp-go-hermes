package hermesacp

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
	"github.com/stretchr/testify/require"
)

func TestManagedHandoffPinsDisjointRootBeforeLoadPreparation(t *testing.T) {
	for _, relation := range []string{"dedicated", "scratch", "ancestor", "descendant", "symlink alias"} {
		t.Run(relation, func(t *testing.T) {
			if relation == "symlink alias" && runtime.GOOS == "windows" {
				return // Creating a symlink requires an unrelated host privilege.
			}
			base := durableTempDir(t)
			scratch := filepath.Join(base, "scratch")
			require.NoError(t, os.Mkdir(scratch, 0o700))
			handoff := filepath.Join(base, "handoff")
			switch relation {
			case "scratch":
				handoff = scratch
			case "ancestor":
				handoff = base
			case "descendant":
				handoff = filepath.Join(scratch, "handoff")
			case "symlink alias":
				require.NoError(t, os.Symlink(scratch, handoff))
			}
			require.NoError(t, os.MkdirAll(handoff, 0o700))
			data := fixtureBytes(t, "valid.png")
			path := writeHandoffFile(t, handoff, "image.png", data)
			block := handoffBlock(path, mimePNG, handoffEnvelopeFor(data))
			store := NewInMemorySessionStore()
			source := testSession(t, newTestAgent(WithSessionStore(store)), newFakeHermesClient())
			require.NoError(t, source.snapshotToStore(t.Context()))
			require.NoError(t, source.Close(t.Context()))

			authority := newTestHostAuthority()
			authority.moveTrees = true
			agent := newTestAgent(WithHostAuthority(authority), WithScratchDir(scratch), WithInputHandoffRoot(handoff), WithSessionStore(store))
			agent.setAgentClient(newRecordingAgentClient())
			defer func() { require.NoError(t, agent.Close()) }()
			client := newFakeHermesClient()
			client.getSession = testNativeSession("native-1")
			prepared := false
			originalOpen := openHandoffRoot
			openHandoffRoot = func(path string) (*os.Root, error) {
				require.False(t, prepared, "resolved a handoff root after native preparation")

				return originalOpen(path)
			}
			t.Cleanup(func() { openHandoffRoot = originalOpen })
			agent.options.clientFactory = func(ctx context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
				require.NoError(t, start.PrepareNativeTree(ctx, start.ExistingXDG.Root))
				prepared = true
				client.xdg = start.ExistingXDG
				client.closeFunc = func(ctx context.Context) error {
					return start.ReclaimNativeTree(ctx, start.ExistingXDG.Root)
				}

				return client, nil
			}
			_, err := agent.LoadSession(t.Context(), acp.LoadSessionRequest{
				SessionId: source.id, Cwd: source.cwd, McpServers: []acp.McpServer{},
			})
			require.NoError(t, err)
			require.True(t, prepared)
			loaded, err := agent.session(source.id)
			require.NoError(t, err)
			pinned := agent.managedHandoff.root
			if relation == "dedicated" && runtime.GOOS != "windows" {
				// The prepared home is now exposed at the original handoff spelling.
				// Reads must continue through the descriptor pinned before preparation.
				require.NoError(t, os.Rename(handoff, handoff+".retained"))
				require.NoError(t, os.Symlink(client.xdg.Root+".native", handoff))
			}
			parts, readErr := loaded.promptParts(t.Context(), []acp.ContentBlock{block})
			if relation == "dedicated" {
				require.NoError(t, readErr)
				require.Len(t, parts, 1)
				require.NotNil(t, pinned)
			} else {
				var requestErr *acp.RequestError
				require.ErrorAs(t, readErr, &requestErr)
				data, ok := requestErr.Data.(map[string]any)
				require.True(t, ok)
				require.Equal(t, imageErrPathNotAllowed, data[jsonFieldError])
				require.Nil(t, pinned)
			}
			// Declarations still fail at their original no-I/O pre-gates.
			invalid := handoffBlock(path, "image/unsupported", handoffEnvelopeFor(data))
			_, readErr = loaded.promptParts(t.Context(), []acp.ContentBlock{invalid})
			var requestErr *acp.RequestError
			require.ErrorAs(t, readErr, &requestErr)
			errorData, ok := requestErr.Data.(map[string]any)
			require.True(t, ok)
			require.Equal(t, imageErrInvalidMediaType, errorData[jsonFieldError])
			require.Zero(t, client.promptDispatchCount())
			require.NoError(t, agent.Close())
			if pinned != nil {
				_, statErr := pinned.Stat(".")
				require.Error(t, statErr, "Agent.Close leaked the retained read descriptor")
			}
		})
	}
}
