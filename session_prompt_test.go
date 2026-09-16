package hermesacp

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

const tinyPNG = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="

func TestImageNativeHandoffAndGates(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	data, err := base64.StdEncoding.DecodeString(tinyPNG)
	require.NoError(t, err)
	path := filepath.Join(root, "input.png")
	require.NoError(t, os.WriteFile(path, data, 0600))
	digest := sha256.Sum256(data)
	agent := NewAgent(testOptions(t, WithInputHandoffRoot(root))...)
	rec := newRecorder()
	agent.attach(rec, nil)
	t.Cleanup(func() { require.NoError(t, agent.Close()) })
	_, err = agent.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	session, err := agent.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	uri := "file://" + path
	handoff := acp.ContentBlock{Image: &acp.ContentBlockImage{MimeType: "image/png", Uri: &uri, Meta: map[string]any{wire.HandoffKey: map[string]any{"version": 1, "digest": hex.EncodeToString(digest[:]), "sizeBytes": len(data)}}}}
	for _, block := range []acp.ContentBlock{acp.ImageBlock(tinyPNG, "image/png"), handoff} {
		before := len(rec.snapshot())
		_, err = agent.Prompt(t.Context(), wire.PromptRequest(session.SessionId, acp.TextBlock("IMAGE"), block))
		require.NoError(t, err)
		require.Equal(t, tinyPNG, agentText(rec.snapshot()[before:]))
	}
	_, err = agent.Prompt(t.Context(), wire.PromptRequest(session.SessionId, acp.ImageBlock("not base64!", "image/png")))
	require.Equal(t, "invalid_base64", requestErrorData(t, err)[stopReasonError])
	_, err = agent.Prompt(t.Context(), wire.PromptRequest(session.SessionId, acp.ImageBlock(tinyPNG, "image/jpeg")))
	require.Error(t, err)
}

func TestNativeEnvironmentRemainsSessionScoped(t *testing.T) {
	t.Parallel()
	h := newHarness(t, WithEnv(map[string]string{fakeHermesEnv: "1", "ACP_MARKER": "agent"}))
	h.initialize()
	for _, marker := range []string{"first", "second"} {
		t.Run(marker, func(t *testing.T) {
			t.Parallel()
			first, second := t.TempDir(), t.TempDir()
			session, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(t.TempDir(), WithSessionHermesOptions(NewHermesOptions(WithHermesEnv(map[string]string{"ACP_MARKER": marker, "PATH": "/usr/bin:/bin"}), WithHermesExtraPathDirs(first, second)))))
			require.NoError(t, err)
			_, err = h.prompt(session.SessionId, "ENV", nil)
			require.NoError(t, err)
			var updates []acp.SessionNotification
			for _, update := range h.rec.snapshot() {
				if update.SessionId == session.SessionId {
					updates = append(updates, update)
				}
			}
			require.Equal(t, marker+"|"+first+string(os.PathListSeparator)+second+string(os.PathListSeparator)+"/usr/bin:/bin", agentText(updates))
		})
	}
}

// tinyGIF is a second, distinctly encoded 1x1 image, so an ordering assertion
// cannot be satisfied by sending the same bytes twice.
const tinyGIF = "R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7"

func TestMultipleImagesKeepPromptOrder(t *testing.T) {
	t.Parallel()

	agent := NewAgent(testOptions(t)...)
	rec := newRecorder()
	agent.attach(rec, nil)
	t.Cleanup(func() { require.NoError(t, agent.Close()) })

	_, err := agent.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)

	session, err := agent.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)

	_, err = agent.Prompt(t.Context(), wire.PromptRequest(session.SessionId,
		acp.TextBlock("IMAGE"),
		acp.ImageBlock(tinyPNG, "image/png"),
		acp.ImageBlock(tinyGIF, "image/gif"),
	))
	require.NoError(t, err)
	require.Equal(t, tinyPNG+","+tinyGIF, agentText(rec.snapshot()), "both images reach the harness, in prompt order")
}

// A session/cancel ends the turn with the cancelled stop reason and a
// terminal idle whose outcome is cancelled.
func TestCancelEndsTheTurnAsCancelled(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize(withLifecycle())
	session := h.newSession()

	done := make(chan acp.PromptResponse, 1)
	failed := make(chan error, 1)

	go func() {
		resp, err := h.prompt(session.SessionId, "SLOW", promptMeta(1))
		done <- resp
		failed <- err
	}()

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool { return len(lifecycleEvents(updates)) >= 3 })
	require.NoError(t, h.conn.Cancel(h.ctx(), wire.CancelRequest(session.SessionId)))
	require.NoError(t, <-failed)
	require.Equal(t, acp.StopReasonCancelled, (<-done).StopReason)

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool {
		for _, event := range lifecycleEvents(updates) {
			if event["type"] == "state_update" && event["state"] == "idle" && event["outcome"] == "cancelled" {
				return true
			}
		}

		return false
	})
}

// TestRefusedPeerPromptLeavesTheLiveTurnAlone covers the SDK cancelling the
// previous prompt's request context when a second prompt arrives: only the
// session cancels a turn, so the refused peer must not end the live one.
func TestRefusedPeerPromptLeavesTheLiveTurnAlone(t *testing.T) {
	t.Parallel()

	h := newHarness(t)

	release := make(chan struct{})
	h.rec.answer = func(acp.RequestPermissionRequest) acp.RequestPermissionResponse {
		<-release

		return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected(approvalOnce)}
	}

	h.initialize(withLifecycle())
	session := h.newSession()

	type outcome struct {
		resp acp.PromptResponse
		err  error
	}

	live := make(chan outcome, 1)

	go func() {
		resp, err := h.prompt(session.SessionId, "PERMISSION", promptMeta(1))
		live <- outcome{resp: resp, err: err}
	}()

	require.Eventually(t, func() bool {
		h.rec.mu.Lock()
		defer h.rec.mu.Unlock()

		return len(h.rec.permissions) > 0
	}, testTimeout, time.Millisecond)

	_, err := h.prompt(session.SessionId, "HELLO", promptMeta(2))
	require.Equal(t, "backpressure", requestErrorData(t, err)[stopReasonError])
	require.Equal(t, limitSessionPrompt, requestErrorData(t, err)["limit"])

	close(release)

	got := <-live
	require.NoError(t, got.err)
	require.Equal(t, acp.StopReasonEndTurn, got.resp.StopReason, "a refused peer prompt must not cancel the live turn")
}

// TestCancelDuringImageUploadEndsTheTurnAsCancelled proves a cancel that lands
// while a prompt image is still uploading answers cancelled instead of being
// classified as a dispatch failure against the torn-down gateway.
func TestCancelDuringImageUploadEndsTheTurnAsCancelled(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	h := newHarness(t, WithHome(home))
	h.initialize(withLifecycle())
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "HOLD", promptMeta(1))
	require.NoError(t, err)

	type outcome struct {
		resp acp.PromptResponse
		err  error
	}

	held := make(chan outcome, 1)

	go func() {
		request := wire.PromptRequest(session.SessionId, acp.TextBlock("IMAGE"), acp.ImageBlock(tinyPNG, "image/png"))
		request.Meta = promptMeta(2)
		resp, promptErr := h.conn.Prompt(h.ctx(), request)
		held <- outcome{resp: resp, err: promptErr}
	}()

	marker := filepath.Join(home, heldAttachMarker)
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(marker)

		return statErr == nil
	}, testTimeout, time.Millisecond, "the attachment reaches the gateway and is never answered")

	require.NoError(t, h.conn.Cancel(h.ctx(), wire.CancelRequest(session.SessionId)))

	got := <-held
	require.NoError(t, got.err)
	require.Equal(t, acp.StopReasonCancelled, got.resp.StopReason)
}

const (
	// mappingBlocks makes one prompt expensive enough to map that the moment
	// the turn is installed is unambiguous.
	mappingBlocks = 1_000_000
	// mappingInstallBound is the budget the turn install has to beat; mapping
	// mappingBlocks takes at least five times as long.
	mappingInstallBound = 20 * time.Millisecond
)

// The turn is installed before the prompt is mapped: a session/cancel that
// lands while the prompt is still being validated ends it from the turn's own
// context, creates no native turn and publishes no acceptance.
func TestCancelDuringPromptMappingAnswersCancelled(t *testing.T) {
	t.Parallel()

	blocks := make([]acp.ContentBlock, mappingBlocks)
	for index := range blocks {
		blocks[index] = acp.ImageBlock(tinyPNG, "image/png")
	}

	a := NewAgent(testOptions(t, WithImageLimits(ImageLimits{MaxInputBytesPerImage: 1 << 30, MaxInputBytesPerPrompt: 1 << 30}))...)
	t.Cleanup(func() { _ = a.Close() })

	rec := newRecorder()
	a.attach(rec, nil)

	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber, Meta: map[string]any{wire.LifecycleKey: map[string]any{"version": 1}}})
	require.NoError(t, err)

	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)

	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)

	type outcome struct {
		resp acp.PromptResponse
		err  error
	}

	done := make(chan outcome, 1)
	started := make(chan struct{})

	go func() {
		request := wire.PromptRequest(created.SessionId, blocks...)
		request.Meta = promptMeta(1)
		close(started)
		resp, promptErr := a.Prompt(t.Context(), request)
		done <- outcome{resp: resp, err: promptErr}
	}()

	<-started

	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()

		return s.turn != nil
	}, mappingInstallBound, 100*time.Microsecond, "the turn is installed before the prompt is mapped")

	require.NoError(t, a.Cancel(t.Context(), wire.CancelRequest(created.SessionId)))

	got := <-done
	require.NoError(t, got.err)
	require.Equal(t, acp.StopReasonCancelled, got.resp.StopReason)

	for _, event := range lifecycleEvents(rec.snapshot()) {
		require.NotEqual(t, "prompt_accepted", event["type"], "a cancel before dispatch creates no native turn")
	}
}

// A $/cancel_request ends only the addressed handler's context: the turn it
// was driving stays the session's, completes successfully once, and the
// session keeps serving prompts.
func TestCancelRequestSettlesTheOriginalRequestOnce(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	permissionCtx, releasePermission := context.WithCancel(t.Context())
	defer releasePermission()
	entered := make(chan struct{}, 1)
	answer := h.rec.answer
	h.rec.answer = func(request acp.RequestPermissionRequest) acp.RequestPermissionResponse {
		entered <- struct{}{}
		<-permissionCtx.Done()

		return answer(request)
	}
	h.initialize(withLifecycle())
	session := h.newSession()

	request := wire.TextPromptRequest(session.SessionId, "PERMISSION")
	request.Meta = promptMeta(1)
	failed := make(chan error, 1)

	go func() {
		response, err := h.conn.Prompt(h.ctx(), request)
		if err == nil && response.StopReason != acp.StopReasonEndTurn {
			err = errors.New("request cancellation ended the native turn")
		}
		failed <- err
	}()

	select {
	case <-entered:
	case <-h.ctx().Done():
		t.Fatal("native permission request did not arrive")
	}
	require.NoError(t, h.input.cancelPrompt())

	_, busyErr := h.prompt(session.SessionId, "HELLO", promptMeta(2))
	require.Equal(t, "backpressure", requestErrorData(t, busyErr)["error"])
	releasePermission()
	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool {
		return slices.Contains(eventTypes(lifecycleEvents(updates)), "state_update:idle")
	})

	idles := 0

	for _, update := range h.rec.snapshot() {
		envelope, _ := update.Meta[wire.LifecycleKey].(map[string]any)
		event, _ := envelope["event"].(map[string]any)
		if event["type"] == "state_update" && event["state"] == "idle" {
			idles++
			require.Equal(t, "success", event["outcome"])
			require.Equal(t, string(acp.StopReasonEndTurn), event["stopReason"])
		}
	}

	require.Equal(t, 1, idles, "the turn the cancelled request started settles exactly once")
	require.NoError(t, <-failed)

	resp, err := h.prompt(session.SessionId, "HELLO", promptMeta(3))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
}

// A turn is the session's before the prompt has anything to dispatch, so a
// session/cancel that lands while hermes is still being relaunched ends it
// there: the prompt answers cancelled, hermes never receives the turn, and
// the lifecycle stream carries nothing for it.
func TestPromptCancelledWhileRelaunching(t *testing.T) {
	t.Parallel()

	held := filepath.Join(t.TempDir(), "relaunch-held")
	h := newHarness(t, WithEnv(map[string]string{fakeHermesEnv: "1", fakeHermesEnvResumeHold: held}))
	h.initialize(withLifecycle())
	session := h.newSession()

	// The gateway dies mid-turn, so the next prompt starts a replacement and
	// resumes the conversation on it; the replacement never answers that
	// resume.
	_, err := h.prompt(session.SessionId, "CRASH", promptMeta(1))
	require.Equal(t, "process_exit", requestErrorData(t, err)["cause"])

	before := eventTypes(lifecycleEvents(h.rec.snapshot()))

	type result struct {
		resp acp.PromptResponse
		err  error
	}

	done := make(chan result, 1)

	go func() {
		resp, promptErr := h.prompt(session.SessionId, "HELLO", promptMeta(2))
		done <- result{resp, promptErr}
	}()

	require.Eventually(t, func() bool {
		_, statErr := os.Stat(held)

		return statErr == nil
	}, testTimeout, time.Millisecond)

	require.NoError(t, h.conn.Cancel(h.ctx(), wire.CancelRequest(session.SessionId)))

	got := <-done
	require.NoError(t, got.err)
	require.Equal(t, acp.StopReasonCancelled, got.resp.StopReason)
	require.Equal(t, before, eventTypes(lifecycleEvents(h.rec.snapshot())),
		"a prompt hermes never received opens no incarnation and publishes no acceptance")
}

func TestPromptRefusesTextAfterImagesBeforeDispatch(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		tail acp.ContentBlock
	}{
		{"text", acp.TextBlock("after")},
		{"resource link", acp.ContentBlock{ResourceLink: &acp.ContentBlockResourceLink{Uri: "file:///notes.txt", Name: "notes"}}},
		{"text resource", acp.ContentBlock{Resource: &acp.ContentBlockResource{Resource: acp.EmbeddedResourceResource{
			TextResourceContents: &acp.TextResourceContents{Uri: "file:///notes.txt", Text: "after"},
		}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.initialize()
			session := h.newSession()
			before := h.rec.snapshot()
			_, err := h.conn.Prompt(h.ctx(), wire.PromptRequest(session.SessionId,
				acp.TextBlock("before"), acp.ImageBlock(tinyPNG, "image/png"), tc.tail))
			require.Equal(t, map[string]any{"error": "unsupported", "field": "prompt"}, requestErrorData(t, err))
			require.Equal(t, before, h.rec.snapshot(), "refusal publishes no turn updates")
			_, err = h.conn.Prompt(h.ctx(), wire.PromptRequest(session.SessionId, acp.TextBlock("IMAGE"), acp.ImageBlock(tinyGIF, "image/gif")))
			require.NoError(t, err)
			require.Equal(t, tinyGIF, agentText(h.rec.snapshot()[len(before):]), "refused images never enter the native attachment queue")
		})
	}
}

func TestMapPromptAcceptsImageGroup(t *testing.T) {
	t.Parallel()
	s := &session{agent: NewAgent(testOptions(t)...)}
	mime := "image/png"
	blob := acp.ContentBlock{Resource: &acp.ContentBlockResource{Resource: acp.EmbeddedResourceResource{
		BlobResourceContents: &acp.BlobResourceContents{Uri: "file:///provenance.png", MimeType: &mime, Blob: tinyPNG},
	}}}
	for _, prefix := range [][]acp.ContentBlock{nil, {acp.TextBlock("caption")}} {
		blocks := append(slices.Clone(prefix), acp.ImageBlock(tinyPNG, "image/png"), blob,
			acp.ContentBlock{Text: &acp.ContentBlockText{Text: "display only", Annotations: &acp.Annotations{Audience: []acp.Role{acp.RoleUser}}}})
		mapped, err := s.mapPrompt(t.Context(), blocks)
		require.NoError(t, err)
		require.Len(t, mapped.images, 2)
		require.NotContains(t, mapped.message, "provenance")
		require.NotContains(t, mapped.message, "display only")
		if len(prefix) == 0 {
			require.Empty(t, mapped.message)
		} else {
			require.Equal(t, "caption", mapped.message)
		}
	}
}
