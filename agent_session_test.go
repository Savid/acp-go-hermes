package hermesacp

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

type commitBarrier struct {
	acpcore.SessionStore
	block   atomic.Bool
	entered chan acpcore.SessionKey
	release chan struct{}
}

func (s *commitBarrier) Replace(ctx context.Context, key acpcore.SessionKey, replacements []acpcore.SessionStoreReplacement) error {
	if s.block.CompareAndSwap(true, false) {
		s.entered <- key
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return s.SessionStore.Replace(ctx, key, replacements)
}
func TestEstablishmentExcludesPrompt(t *testing.T) {
	for _, phase := range []string{"new", "cold_load"} {
		t.Run(phase, func(t *testing.T) {
			store := &commitBarrier{SessionStore: acpcore.NewInMemorySessionStore(), entered: make(chan acpcore.SessionKey, 1), release: make(chan struct{})}
			release := sync.OnceFunc(func() { close(store.release) })
			h := newHarness(t, WithSessionStore(store))
			h.initialize()
			t.Cleanup(release)
			cwd := t.TempDir()
			var id acp.SessionId
			if phase == "cold_load" {
				created, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
				require.NoError(t, err)
				id = created.SessionId
				_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: id})
				require.NoError(t, err)
			}
			store.block.Store(true)
			done := make(chan error, 1)
			ctx := h.ctx()
			go func() {
				if phase == "new" {
					_, err := h.conn.NewSession(ctx, wire.NewSessionRequest(cwd))
					done <- err
				} else {
					_, err := h.conn.LoadSession(ctx, wire.LoadSessionRequest(id, cwd))
					done <- err
				}
			}()
			select {
			case key := <-store.entered:
				id = acp.SessionId(key.SessionID)
			case <-ctx.Done():
				t.Fatal("establishment never reached commit")
			}
			// An empty prompt cannot dispatch native work, but admission must still reject
			// it as busy before parsing content while establishment holds the session.
			_, err := h.conn.Prompt(ctx, wire.PromptRequest(id))
			data := requestErrorData(t, err)
			release()
			require.NoError(t, <-done)
			require.Equal(t, "session_prompt", data["limit"], "establishing session admitted a prompt into content validation: %v", data)
		})
	}
}

// The shutdown ladder detaches whatever its verdict: a failed close leaves no
// session installed with a permanently cached error.
func TestFailedCloseReleasesTheSessionSlot(t *testing.T) {
	t.Parallel()

	store := &faultStore{SessionStore: acpcore.NewInMemorySessionStore()}
	h := newHarness(t, WithSessionStore(store), WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))
	h.initialize()

	first := h.newSession()

	store.fail.Store(true)

	_, err := h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: first.SessionId})
	require.Equal(t, "hermes_internal_failure", requestErrorData(t, err)[stopReasonError])

	store.fail.Store(false)

	_, err = h.conn.NewSession(h.ctx(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err, "a failed close still releases the active-sessions slot")
}

func TestFailedRestoreCloseReleasesSlot(t *testing.T) {
	for _, method := range []string{acp.AgentMethodSessionLoad, acp.AgentMethodSessionResume} {
		t.Run(method, func(t *testing.T) {
			store := &recoveryFaultStore{SessionStore: acpcore.NewInMemorySessionStore()}
			h := newHarness(t, WithSessionStore(store), WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))
			h.initialize()
			cwd := t.TempDir()
			created, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
			require.NoError(t, err)
			before, err := store.Load(h.ctx(), string(created.SessionId))
			require.NoError(t, err)
			option := wire.WithSessionMetaValue(map[string]any{"hermes": map[string]any{"options": map[string]any{"env": map[string]string{"RESTORE_TEST": "changed"}}}})
			store.fail.Store(true)
			if method == acp.AgentMethodSessionLoad {
				_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(created.SessionId, cwd, option))
			} else {
				_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(created.SessionId, cwd, option))
			}
			store.fail.Store(false)
			require.Error(t, err, "store failure must fail restore")
			after, err := store.Load(h.ctx(), string(created.SessionId))
			require.NoError(t, err)
			require.Equal(t, before, after, "failed teardown must retain the durable generation")
			_, err = h.conn.NewSession(h.ctx(), wire.NewSessionRequest(t.TempDir()))
			require.NoError(t, err, "a failed restore-close leaked its active-session slot")
		})
	}
}

// blockedOpeningClient keeps the first publication in progress until released.
type blockedOpeningClient struct {
	*recorder
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockedOpeningClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	c.once.Do(func() {
		close(c.entered)
		<-c.release
	})

	return c.recorder.SessionUpdate(ctx, notification)
}

func TestRestoreWaitsForPreviousOpening(t *testing.T) {
	t.Parallel()
	for _, method := range []string{acp.AgentMethodSessionLoad, acp.AgentMethodSessionResume} {
		t.Run(method, func(t *testing.T) {
			t.Parallel()
			a := NewAgent(testOptions(t)...)
			t.Cleanup(func() { _ = a.Close() })
			client := &blockedOpeningClient{recorder: newRecorder(), entered: make(chan struct{}), release: make(chan struct{})}
			release := sync.OnceFunc(func() { close(client.release) })
			t.Cleanup(release)
			transport, meta := prepareOpeningResponse(t)
			a.attach(client, transport)
			initialize := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
			withLifecycle()(&initialize)
			_, err := a.Initialize(t.Context(), initialize)
			require.NoError(t, err)
			cwd := t.TempDir()
			request := wire.NewSessionRequest(cwd)
			request.Meta = meta
			created, err := a.NewSession(t.Context(), request)
			require.NoError(t, err)
			_, err = transport.Writer().Write([]byte("{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}\n"))
			require.NoError(t, err)
			select {
			case <-client.entered:
			case <-time.After(testTimeout):
				t.Fatal("initial opening did not reach the client")
			}

			restored := make(chan error, 1)
			go func() {
				if method == acp.AgentMethodSessionLoad {
					_, restoreErr := a.LoadSession(t.Context(), wire.LoadSessionRequest(created.SessionId, cwd))
					restored <- restoreErr

					return
				}

				_, restoreErr := a.ResumeSession(t.Context(), wire.ResumeSessionRequest(created.SessionId, cwd))
				restored <- restoreErr
			}()

			select {
			case restoreErr := <-restored:
				t.Fatalf("restore completed before the previous opening: %v", restoreErr)
			case <-time.After(100 * time.Millisecond):
			}

			release()
			select {
			case restoreErr := <-restored:
				require.NoError(t, restoreErr, "completed opening must release restore admission")
			case <-time.After(testTimeout):
				t.Fatal("restore did not continue after the previous opening")
			}
			s, err := a.session(t.Context(), created.SessionId)
			require.NoError(t, err)
			require.True(t, s.lc.Active())
		})
	}
}
