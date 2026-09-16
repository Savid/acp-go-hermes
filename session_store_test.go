package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/sessionlog"
	"github.com/savid/acp-go-core/wire"
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

// TestRestoreRefusesACommittedEmptyMainRecord proves a generation whose main
// record carries no native snapshot is refused rather than indexed: one export
// is the whole record, so any other row count is not a hermes conversation.
func TestRestoreRefusesACommittedEmptyMainRecord(t *testing.T) {
	t.Parallel()

	store := newSharedStore()
	cwd := t.TempDir()
	id := "empty-main-record"

	require.NoError(t, sessionlog.Commit(t.Context(), store, id, nil,
		sessionRecord{SessionID: id, NativeSessionID: "native-empty", Cwd: cwd, UpdatedAtUnixMilli: time.Now().UnixMilli()}))

	h := newHarness(t, WithSessionStore(store))
	h.initialize()

	_, err := h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(acp.SessionId(id), cwd))
	require.Equal(t, "hermes_restore_failed", requestErrorData(t, err)[stopReasonError])
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
	require.NoError(t, sessionlog.Commit(t.Context(), store, string(id), rows, record))
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
