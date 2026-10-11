package hermesacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/sessionlog"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-hermes/internal/hermes"
	"github.com/stretchr/testify/require"
)

// nativeConversation reads the harness's own copy of one conversation.
func nativeConversation(t *testing.T, home string, id acp.SessionId) map[string]any {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(home, string(id)+".json"))
	require.NoError(t, err)

	var native map[string]any
	require.NoError(t, json.Unmarshal(data, &native))

	return native
}

func writeNativeConversation(t *testing.T, home string, id acp.SessionId, native map[string]any) {
	t.Helper()

	data, err := json.Marshal(native)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(home, string(id)+".json"), data, 0o600))
}

// The default in-memory store decides which sessions exist: a second adapter
// over the same native home, with its own fresh store, discovers nothing.
func TestStoreIsTheSoleAuthority(t *testing.T) {
	t.Parallel()

	home, cwd := t.TempDir(), t.TempDir()

	h := newHarness(t, WithHome(home))
	h.initialize()

	created, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)

	_, err = h.prompt(created.SessionId, "HELLO", nil)
	require.NoError(t, err)

	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.NoError(t, err)

	_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(created.SessionId, cwd))
	require.NoError(t, err, "the adapter's own default store restores the session")

	other := newHarness(t, WithHome(home))
	other.initialize()

	_, err = other.conn.LoadSession(other.ctx(), wire.LoadSessionRequest(created.SessionId, cwd))
	require.Equal(t, "unknown session", requestErrorData(t, err)[stopReasonError],
		"native state alone never makes a session loadable")

	list, err := other.conn.ListSessions(other.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Empty(t, list.Sessions)
}

// TestNativeHistoryMustExtendTheMirror refuses a restore when the existing
// native conversation is shorter than, or diverges from, the stored history:
// it wins only when it contains every stored message in order.
func TestNativeHistoryMustExtendTheMirror(t *testing.T) {
	t.Parallel()

	cases := map[string]func(map[string]any){
		"shorter": func(native map[string]any) { native["messages"] = []any{} },
		"divergent": func(native map[string]any) {
			messages, _ := native["messages"].([]any)
			messages[0] = map[string]any{"role": roleUser, "content": "a different first message"}
			native["messages"] = messages
		},
	}

	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			home, cwd := t.TempDir(), t.TempDir()
			store := newSharedStore()

			h := newHarness(t, WithHome(home), WithSessionStore(store))
			h.initialize()

			created, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
			require.NoError(t, err)

			_, err = h.prompt(created.SessionId, "HELLO", nil)
			require.NoError(t, err)

			_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: created.SessionId})
			require.NoError(t, err)

			native := nativeConversation(t, home, created.SessionId)
			mutate(native)
			writeNativeConversation(t, home, created.SessionId, native)

			restored := newHarness(t, WithHome(home), WithSessionStore(store))
			restored.initialize()

			_, err = restored.conn.LoadSession(restored.ctx(), wire.LoadSessionRequest(created.SessionId, cwd))
			require.Equal(t, "hermes_restore_failed", requestErrorData(t, err)[stopReasonError])
		})
	}
}

// A restore adopts an existing native conversation instead of replacing it;
// the scripted gateway refuses a replacing import exactly as hermes does.
func TestRestoreNeverImportsOverNativeHistory(t *testing.T) {
	t.Parallel()

	home, cwd := t.TempDir(), t.TempDir()
	store := newSharedStore()

	h := newHarness(t, WithHome(home), WithSessionStore(store))
	h.initialize()

	created, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)

	_, err = h.prompt(created.SessionId, "HELLO", nil)
	require.NoError(t, err)

	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.NoError(t, err)

	native := nativeConversation(t, home, created.SessionId)
	messages, _ := native["messages"].([]any)
	native["messages"] = append(messages,
		map[string]any{"role": roleUser, "content": "native continuation"},
		map[string]any{"role": roleAssistant, "content": "native answer"})
	writeNativeConversation(t, home, created.SessionId, native)

	restored := newHarness(t, WithHome(home), WithSessionStore(store))
	restored.initialize()

	_, err = restored.conn.LoadSession(restored.ctx(), wire.LoadSessionRequest(created.SessionId, cwd))
	require.NoError(t, err)
	require.Contains(t, agentText(restored.rec.snapshot()), "native answer")

	after := nativeConversation(t, home, created.SessionId)
	remaining, _ := after["messages"].([]any)
	require.Len(t, remaining, len(messages)+2, "the native conversation is adopted, never replaced")
}

// TestRestoreRefusesAForeignNativeSession proves a generation is restored only
// with the native session object of its own conversation, without messages.
func TestRestoreRefusesAForeignNativeSession(t *testing.T) {
	t.Parallel()

	for name, session := range map[string]string{
		"missing":       ``,
		"not an object": `[]`,
		"other id":      `{"id":"native-other"}`,
		"with messages": `{"id":"native-empty","messages":[]}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := newSharedStore()
			cwd := t.TempDir()
			id := "foreign-native-session"

			record := sessionRecord{SessionID: id, NativeSessionID: "native-empty", Cwd: cwd}
			if session != "" {
				record.Session = json.RawMessage(session)
			}

			_, err := sessionlog.NewMirror(store, id).Commit(t.Context(), sessionlog.Rows(nil), recordOf(record))
			require.NoError(t, err)

			h := newHarness(t, WithSessionStore(store))
			h.initialize()

			_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(acp.SessionId(id), cwd))
			require.Equal(t, "hermes_restore_failed", requestErrorData(t, err)[stopReasonError])
		})
	}
}

// appendFailStore fails the next Append it is armed for, and every write
// while it is down.
type appendFailStore struct {
	acpcore.SessionStore
	failAppend atomic.Bool
	down       atomic.Bool
}

func (s *appendFailStore) Replace(ctx context.Context, main acpcore.SessionKey, replacements []acpcore.SessionStoreReplacement) error {
	if s.down.Load() {
		return errors.New("store unavailable")
	}

	return s.SessionStore.Replace(ctx, main, replacements)
}

func (s *appendFailStore) Append(ctx context.Context, main acpcore.SessionKey, base int, entries []acpcore.SessionStoreEntry, replacements []acpcore.SessionStoreReplacement) error {
	if s.down.Load() || s.failAppend.CompareAndSwap(true, false) {
		return errors.New("append unavailable")
	}

	return s.SessionStore.Append(ctx, main, base, entries, replacements)
}

// TestSessionObjectFollowsTheCommittedRead proves the session keeps the
// session object of the read a commit published: a failed commit keeps the
// previous one, and a commit that read again after a failed append keeps the
// one it published.
func TestSessionObjectFollowsTheCommittedRead(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	store := &appendFailStore{SessionStore: acpcore.NewInMemorySessionStore()}
	a := NewAgent(testOptions(t, WithHome(home), WithSessionStore(store))...)
	t.Cleanup(func() { _ = a.Close() })
	a.attach(newRecorder(), nil)

	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)

	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)

	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)

	s.mu.Lock()
	rt, committed := s.runtime, s.nativeSession
	s.mu.Unlock()

	continueNatively := func(title string) {
		native := nativeConversation(t, home, created.SessionId)
		messages, _ := native["messages"].([]any)
		native["messages"] = append(messages, map[string]any{"role": roleUser, "content": title})
		native["title"] = title
		writeNativeConversation(t, home, created.SessionId, native)
	}

	continueNatively("first")
	store.down.Store(true)
	require.Error(t, s.commitMirror(t.Context(), rt))
	store.down.Store(false)

	require.Equal(t, string(committed), string(nativeSessionOf(s)), "a failed commit keeps the committed session object")

	continueNatively("second")
	store.failAppend.Store(true)
	require.NoError(t, s.commitMirror(t.Context(), rt))

	var record sessionRecord

	_, err = sessionlog.LoadRecord(t.Context(), store, string(created.SessionId), &record)
	require.NoError(t, err)
	require.Contains(t, string(record.Session), `"title":"second"`)

	require.Equal(t, string(record.Session), string(nativeSessionOf(s)), "the session keeps the object the retried commit published")
}

func nativeSessionOf(s *session) json.RawMessage {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.nativeSession
}

// TestCommitFailsWhenTheGatewayIsGone proves a commit that cannot be attempted
// is an error: the snapshot is read from the runtime the caller dispatched on,
// so a torn-down generation never reports a durability it did not achieve.
func TestCommitFailsWhenTheGatewayIsGone(t *testing.T) {
	t.Parallel()

	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	a.attach(newRecorder(), nil)

	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)

	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)

	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)

	s.mu.Lock()
	rt := s.runtime
	s.mu.Unlock()

	s.stopRuntime(t.Context(), rt)

	s.mu.Lock()
	bound := s.runtime
	s.mu.Unlock()
	require.Nil(t, bound, "stopping the generation releases the binding")

	require.Error(t, s.commitMirror(t.Context(), rt), "a commit that cannot be attempted never reports success")
}

// Residual native state with no store entry is neither listed nor adopted.
func TestResidualNativeStateIsNeverAdopted(t *testing.T) {
	t.Parallel()

	home, cwd := filepath.Join(t.TempDir(), "home"), t.TempDir()
	orphan := "0123456789abcdef0123456789abcdef"
	require.NoError(t, os.MkdirAll(home, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(home, orphan+".json"), []byte(`{"id":"`+orphan+`","source":"cli","cwd":"`+cwd+`","model":"vision","started_at":1,"messages":[{"role":"user","content":"hello"}]}`), 0o600))

	h := newHarness(t, WithHome(home))
	h.initialize()

	list, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Empty(t, list.Sessions)

	_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(acp.SessionId(orphan), cwd))
	require.Equal(t, "unknown session", requestErrorData(t, err)[stopReasonError])

	_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(acp.SessionId(orphan), cwd))
	require.Equal(t, "unknown session", requestErrorData(t, err)[stopReasonError])
}

type recoveryFaultStore struct {
	acpcore.SessionStore
	fail atomic.Bool
}

func (s *recoveryFaultStore) Replace(ctx context.Context, main acpcore.SessionKey, replacements []acpcore.SessionStoreReplacement) error {
	if s.fail.Load() {
		return errors.New("injected store failure")
	}

	return s.SessionStore.Replace(ctx, main, replacements)
}

func (s *recoveryFaultStore) Append(ctx context.Context, main acpcore.SessionKey, base int, entries []acpcore.SessionStoreEntry, replacements []acpcore.SessionStoreReplacement) error {
	if s.fail.Load() {
		return errors.New("injected store failure")
	}

	return s.SessionStore.Append(ctx, main, base, entries, replacements)
}

func TestNativeBindingSurvivesLoadAndResume(t *testing.T) {
	t.Parallel()
	store := acpcore.NewInMemorySessionStore()
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	cwd := t.TempDir()
	created, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)
	_, err = h.prompt(created.SessionId, "HELLO", nil)
	require.NoError(t, err)
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.NoError(t, err)
	var record sessionRecord
	rows, found, err := sessionlog.Load(t.Context(), store, string(created.SessionId), &record)
	require.NoError(t, err)
	require.True(t, found)
	require.NotEmpty(t, record.NativeSessionID)
	require.Equal(t, wire.NativeSessionMeta(vendor, record.NativeSessionID), created.Meta)
	id := acp.SessionId("acp-conversation-independent-of-native-id")
	record.SessionID = string(id)
	_, err = sessionlog.NewMirror(store, string(id)).Commit(t.Context(), sessionlog.Rows(rows), recordOf(record))
	require.NoError(t, err)
	require.NoError(t, store.Delete(t.Context(), acpcore.SessionKey{SessionID: string(created.SessionId)}))

	before := len(h.rec.snapshot())
	loaded, err := h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(id, cwd))
	require.NoError(t, err)
	require.Equal(t, wire.NativeSessionMeta(vendor, record.NativeSessionID), loaded.Meta)
	_, err = h.prompt(id, "HELLO", nil)
	require.NoError(t, err)
	for _, update := range h.rec.snapshot()[before:] {
		require.Equal(t, id, update.SessionId)
	}
	listed, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Len(t, listed.Sessions, 1)
	require.Equal(t, id, listed.Sessions[0].SessionId)
	require.Equal(t, loaded.Meta, listed.Sessions[0].Meta)
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: id})
	require.NoError(t, err)
	listed, err = h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Len(t, listed.Sessions, 1)
	require.Equal(t, loaded.Meta, listed.Sessions[0].Meta)
	resumed, err := h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(id, cwd))
	require.NoError(t, err)
	require.Equal(t, loaded.Meta, resumed.Meta)
	_, err = h.prompt(id, "HELLO", nil)
	require.NoError(t, err)
	var after sessionRecord
	_, found, err = sessionlog.Load(t.Context(), store, string(id), &after)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, record.NativeSessionID, after.NativeSessionID)
	require.Equal(t, string(id), after.SessionID)
}

type firstMirrorFailureStore struct {
	acpcore.SessionStore
	calls atomic.Int32
}

func (s *firstMirrorFailureStore) Replace(ctx context.Context, key acpcore.SessionKey, rows []acpcore.SessionStoreReplacement) error {
	if s.calls.Add(1) == 1 {
		return errors.New("initial mirror unavailable")
	}

	return s.SessionStore.Replace(ctx, key, rows)
}

func TestFailedNewSessionDoesNotPersistDuringCleanup(t *testing.T) {
	t.Parallel()
	store := &firstMirrorFailureStore{SessionStore: acpcore.NewInMemorySessionStore()}
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	response, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(t.TempDir()))
	require.Error(t, err)
	require.Empty(t, response.SessionId)
	rows, err := store.ListSessions(h.ctx())
	require.NoError(t, err)
	require.Empty(t, rows)
	listed, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Empty(t, listed.Sessions)
}

type firstOpenFailureClient struct {
	*recorder
	failed atomic.Bool
}

func (c *firstOpenFailureClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	if c.failed.CompareAndSwap(false, true) {
		return errors.New("initial publication unavailable")
	}

	return c.recorder.SessionUpdate(ctx, notification)
}

func TestFailedSessionOpenReleasesActiveSlot(t *testing.T) {
	t.Parallel()
	a := NewAgent(testOptions(t, WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))...)
	t.Cleanup(func() { _ = a.Close() })
	a.attach(&firstOpenFailureClient{recorder: newRecorder()}, nil)
	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	withLifecycle()(&request)
	_, err := a.Initialize(t.Context(), request)
	require.NoError(t, err)
	first, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.Error(t, err)
	require.Empty(t, first.SessionId)
	_, err = a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
}

type blockedLoadStore struct {
	acpcore.SessionStore
	block            atomic.Bool
	entered, release chan struct{}
}

func (s *blockedLoadStore) Load(ctx context.Context, sessionID string) (map[string][]acpcore.SessionStoreEntry, error) {
	rows, err := s.SessionStore.Load(ctx, sessionID)
	if err == nil && s.block.CompareAndSwap(true, false) {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return rows, err
}

func TestDeleteWinsAgainstPreparedLoad(t *testing.T) {
	t.Parallel()
	store := &blockedLoadStore{SessionStore: acpcore.NewInMemorySessionStore(), entered: make(chan struct{}), release: make(chan struct{})}
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	cwd := t.TempDir()
	session, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)
	_, err = h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	store.block.Store(true)
	done := make(chan error, 1)
	ctx := h.ctx()
	go func() {
		_, loadErr := h.conn.LoadSession(ctx, wire.LoadSessionRequest(session.SessionId, cwd))
		done <- loadErr
	}()
	select {
	case <-store.entered:
	case <-ctx.Done():
		t.Fatal("load did not reach stored configuration")
	}
	_, err = h.conn.UnstableDeleteSession(h.ctx(), acp.UnstableDeleteSessionRequest{SessionId: session.SessionId})
	close(store.release)
	require.NoError(t, err)
	require.Equal(t, "unknown session", requestErrorData(t, <-done)["error"])
	list, err := h.conn.ListSessions(h.ctx(), acp.ListSessionsRequest{})
	require.NoError(t, err)
	require.Empty(t, list.Sessions)
}

// storeCallCounter counts the writes a session sends to its store.
type storeCallCounter struct {
	acpcore.SessionStore
	replaces atomic.Int32
	appends  atomic.Int32
	deletes  atomic.Int32
}

func (s *storeCallCounter) Replace(ctx context.Context, key acpcore.SessionKey, rows []acpcore.SessionStoreReplacement) error {
	s.replaces.Add(1)

	return s.SessionStore.Replace(ctx, key, rows)
}

func (s *storeCallCounter) Append(ctx context.Context, key acpcore.SessionKey, base int, entries []acpcore.SessionStoreEntry, replacements []acpcore.SessionStoreReplacement) error {
	s.appends.Add(1)

	return s.SessionStore.Append(ctx, key, base, entries, replacements)
}

func (s *storeCallCounter) Delete(ctx context.Context, key acpcore.SessionKey) error {
	s.deletes.Add(1)

	return s.SessionStore.Delete(ctx, key)
}

func ephemeralSessionMeta() wire.SessionRequestOption {
	return wire.WithSessionMeta(wire.SessionMeta{Ephemeral: true}.Apply(nil))
}

func TestEphemeralSessionNeverReachesTheStore(t *testing.T) {
	t.Parallel()

	store := &storeCallCounter{SessionStore: acpcore.NewInMemorySessionStore()}
	h := newHarness(t, WithSessionStore(store))
	h.initialize(withLifecycle())

	created := h.newSession(ephemeralSessionMeta())
	_, err := h.prompt(created.SessionId, "HELLO", promptMeta(1))
	require.NoError(t, err)
	list, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Empty(t, list.Sessions, "a live ephemeral session is never listed")
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.NoError(t, err)
	_, err = h.conn.UnstableDeleteSession(h.ctx(), wire.DeleteSessionRequest(created.SessionId))
	require.NoError(t, err)

	require.Zero(t, store.replaces.Load()+store.appends.Load(), "an ephemeral session is never mirrored")
	require.Zero(t, store.deletes.Load(), "an ephemeral session leaves no tombstone")
	generation, err := store.Load(h.ctx(), string(created.SessionId))
	require.NoError(t, err)
	require.Nil(t, generation)
	list, err = h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Empty(t, list.Sessions)

	durable := h.newSession()
	require.Positive(t, store.replaces.Load(), "a session without the flag still mirrors")
	_, err = h.conn.UnstableDeleteSession(h.ctx(), wire.DeleteSessionRequest(durable.SessionId))
	require.NoError(t, err)
	require.EqualValues(t, 1, store.deletes.Load())
}

func TestHostSessionMetaIsReadOnSessionNewOnly(t *testing.T) {
	t.Parallel()

	store := acpcore.NewInMemorySessionStore()
	cwd := t.TempDir()
	h := newHarness(t, WithSessionStore(store))
	h.initialize()

	_, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd, wire.WithSessionMeta(map[string]any{
		wire.SessionMetaKey: map[string]any{"persist": false},
	})))
	require.Equal(t, "_meta."+wire.SessionMetaKey+".persist", requestErrorData(t, err)["field"])

	created := h.newSession()
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.NoError(t, err)

	_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(created.SessionId, cwd, ephemeralSessionMeta()))
	require.Equal(t, "_meta."+wire.SessionMetaKey, requestErrorData(t, err)["field"])
	_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(created.SessionId, cwd, ephemeralSessionMeta()))
	require.Equal(t, "_meta."+wire.SessionMetaKey, requestErrorData(t, err)["field"])
}

func TestMirrorFailureFencesTurnAndAllowsRetry(t *testing.T) {
	t.Parallel()

	for _, prompt := range []string{"HELLO", "ERROR"} {
		t.Run(prompt, func(t *testing.T) {
			t.Parallel()

			store := &faultStore{SessionStore: acpcore.NewInMemorySessionStore()}
			h := newHarness(t, WithSessionStore(store))
			h.initialize(withLifecycle())
			created := h.newSession()
			before, err := store.Load(t.Context(), string(created.SessionId))
			require.NoError(t, err)
			store.fail.Store(true)
			t.Cleanup(func() { store.fail.Store(false) })
			response, err := h.prompt(created.SessionId, prompt, promptMeta(1))
			require.Empty(t, response.StopReason)
			require.Equal(t, -32603, requestErrorCode(t, err))
			data := requestErrorData(t, err)
			require.Equal(t, "hermes_turn_failed", data["error"])
			if prompt == "ERROR" {
				require.Equal(t, "provider", data["cause"])
				require.Equal(t, "provider unavailable", data["message"])
			} else {
				require.Equal(t, "transport", data["cause"])
				require.Equal(t, "session mirror commit failed", data["message"])
			}
			after, err := store.Load(t.Context(), string(created.SessionId))
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.Equal(t, []string{"lifecycle_snapshot", "prompt_accepted", "state_update:running"}, eventTypes(lifecycleEvents(h.rec.snapshot())))

			store.fail.Store(false)
			response, err = h.prompt(created.SessionId, "HELLO", promptMeta(2))
			require.NoError(t, err)
			require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
			require.Equal(t, []string{"lifecycle_snapshot", "prompt_accepted", "state_update:running", "lifecycle_snapshot", "prompt_accepted", "state_update:running", "state_update:idle"}, eventTypes(lifecycleEvents(h.rec.snapshot())))
			var streams []string
			for _, update := range h.rec.snapshot() {
				envelope, ok := update.Meta[wire.LifecycleKey].(map[string]any)
				if !ok {
					continue
				}
				event, ok := envelope["event"].(map[string]any)
				if ok && event["type"] == "lifecycle_snapshot" {
					id, ok := envelope["streamId"].(string)
					require.True(t, ok)
					streams = append(streams, id)
				}
			}
			require.Len(t, streams, 2)
			require.NotEqual(t, streams[0], streams[1])
		})
	}
}

// TestMirrorFollowsInPlaceCompaction proves the mirror over Hermes's in-place
// compaction: the export keeps every archived message, so it never shrinks;
// the commit after the archive republishes the conversation once, and the
// turns after it append again.
func TestMirrorFollowsInPlaceCompaction(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	store := &storeCallCounter{SessionStore: acpcore.NewInMemorySessionStore()}
	h := newHarness(t, WithHome(home), WithSessionStore(store))
	h.initialize()
	created := h.newSession()

	for _, prompt := range []string{"HELLO", "HELLO"} {
		_, err := h.prompt(created.SessionId, prompt, nil)
		require.NoError(t, err)
	}

	require.EqualValues(t, 1, store.replaces.Load(), "only establishment republishes")
	require.EqualValues(t, 2, store.appends.Load(), "each turn appends its messages")

	_, err := h.prompt(created.SessionId, "ARCHIVE", nil)
	require.NoError(t, err)
	require.EqualValues(t, 2, store.replaces.Load(), "the archive republishes the conversation")

	_, err = h.prompt(created.SessionId, "HELLO", nil)
	require.NoError(t, err)
	require.EqualValues(t, 2, store.replaces.Load())
	require.EqualValues(t, 3, store.appends.Load(), "turns after the archive append again")

	rows, err := loadEntries(t.Context(), store, acpcore.SessionKey{SessionID: string(created.SessionId)})
	require.NoError(t, err)

	native := nativeConversation(t, home, created.SessionId)
	messages, _ := native["messages"].([]any)
	require.Len(t, rows, len(messages), "the store holds every native message, archived ones included")
	require.Len(t, messages, 4+3+2)
}

// TestRestoreAdoptsAnUnchangedConversation proves a load over the native
// conversation the store already holds writes nothing, and the next turn
// appends.
func TestRestoreAdoptsAnUnchangedConversation(t *testing.T) {
	t.Parallel()

	home, cwd := t.TempDir(), t.TempDir()
	store := &storeCallCounter{SessionStore: acpcore.NewInMemorySessionStore()}
	h := newHarness(t, WithHome(home), WithSessionStore(store))
	h.initialize()

	created, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)
	_, err = h.prompt(created.SessionId, "HELLO", nil)
	require.NoError(t, err)
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.NoError(t, err)

	replaces, appends := store.replaces.Load(), store.appends.Load()

	restored := newHarness(t, WithHome(home), WithSessionStore(store))
	restored.initialize()
	_, err = restored.conn.LoadSession(restored.ctx(), wire.LoadSessionRequest(created.SessionId, cwd))
	require.NoError(t, err)
	require.Equal(t, replaces, store.replaces.Load(), "the store already holds the conversation")
	require.Equal(t, appends, store.appends.Load())

	_, err = restored.prompt(created.SessionId, "HELLO", nil)
	require.NoError(t, err)
	require.Equal(t, replaces, store.replaces.Load())
	require.Equal(t, appends+1, store.appends.Load(), "the first turn after the load appends")
}

// TestRestoreAcceptsAnArchiveTheStoreMissed proves a conversation Hermes
// archived in place after the last commit, as when the process dies between
// a compaction and its commit, restores: archive flags are not content, and
// the restore republishes the archived conversation.
func TestRestoreAcceptsAnArchiveTheStoreMissed(t *testing.T) {
	t.Parallel()

	home, cwd := t.TempDir(), t.TempDir()
	store := &storeCallCounter{SessionStore: acpcore.NewInMemorySessionStore()}
	h := newHarness(t, WithHome(home), WithSessionStore(store))
	h.initialize()

	created, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)
	_, err = h.prompt(created.SessionId, "HELLO", nil)
	require.NoError(t, err)
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.NoError(t, err)

	native := nativeConversation(t, home, created.SessionId)
	messages, _ := native["messages"].([]any)

	for _, message := range messages {
		object, _ := message.(map[string]any)
		object["active"], object["compacted"] = 0, 1
	}

	native["messages"] = append(messages, map[string]any{"role": roleUser, "content": "summary", "active": 1, "compacted": 0})
	writeNativeConversation(t, home, created.SessionId, native)

	replaces := store.replaces.Load()

	restored := newHarness(t, WithHome(home), WithSessionStore(store))
	restored.initialize()
	_, err = restored.conn.LoadSession(restored.ctx(), wire.LoadSessionRequest(created.SessionId, cwd))
	require.NoError(t, err)
	require.Equal(t, replaces+1, store.replaces.Load(), "the archived conversation is republished")

	rows, err := loadEntries(t.Context(), store, acpcore.SessionKey{SessionID: string(created.SessionId)})
	require.NoError(t, err)
	require.Len(t, rows, len(messages)+1)
	require.Contains(t, string(rows[0]), `"compacted":1`)
}

// syntheticMessage is one exported message of about size bytes.
func syntheticMessage(index, size int) []byte {
	message, _ := json.Marshal(map[string]any{
		"id": index + 1, "session_id": "bench", "role": []string{roleUser, roleAssistant}[index%2],
		"content": strings.Repeat("x", size), "timestamp": 1791701016.5 + float64(index), "active": 1,
	})

	return message
}

// BenchmarkCommitMirror measures one turn's commit of a long conversation:
// reading Hermes's export after the committed messages and appending the rest
// to the store, where each turn adds a user and an assistant message to about
// 8 MiB of history.
func BenchmarkCommitMirror(b *testing.B) {
	const history, size = 4096, 2048

	session := []byte(`{"id":"bench","source":"desktop","cwd":"/work","message_count":0}`)
	messages := make([][]byte, 0, history+2*b.N)

	for index := range history {
		messages = append(messages, syntheticMessage(index, size))
	}

	record := sessionRecord{SessionID: "bench", NativeSessionID: "bench", Cwd: "/work", Session: session}
	mirror := sessionlog.NewMirror(acpcore.NewInMemorySessionStore(), "bench")

	commit := func() {
		export, err := hermes.Conversation{Session: session, Messages: messages}.Encode()
		require.NoError(b, err)

		source := func(from sessionlog.Cursor) (sessionlog.Tail, error) {
			prefix, _ := from.Position().(hermes.Prefix)
			read, readErr := hermes.DecodeExport(bytes.NewReader(export), prefix)

			return sessionlog.Tail{Rows: read.Messages, Next: sessionlog.Position(read.Next), Full: prefix.Count == 0 || !read.Continued}, readErr
		}

		b.StartTimer()
		_, err = mirror.Commit(b.Context(), source, recordOf(record))
		require.NoError(b, err)
		b.StopTimer()
	}

	b.StopTimer()
	commit()
	b.ReportAllocs()
	b.ResetTimer()
	b.StopTimer()

	for range b.N {
		messages = append(messages, syntheticMessage(len(messages), size), syntheticMessage(len(messages)+1, size))
		commit()
	}
}

// recordOf is the record function of a commit whose configuration is fixed.
func recordOf(record sessionRecord) func(sessionlog.Tail) (any, error) {
	return func(sessionlog.Tail) (any, error) { return record, nil }
}
