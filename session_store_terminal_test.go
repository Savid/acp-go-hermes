package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
	"github.com/savid/acp-go-hermes/internal/lifecycle"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

func TestTerminalSnapshotFromMessagesSelectsLatestFinishedAssistant(t *testing.T) {
	messages := []nativehermes.NativeMessage{
		testHistoryMessage("history-1", "native-1", "user", ""),
		testHistoryMessage("history-2", "native-1", valAssistant, "stop"),
		testHistoryMessage("history-3", "native-1", valAssistant, ""),
		testHistoryMessage("history-4", "native-1", "user", ""),
	}

	terminal, err := terminalSnapshotFromMessages("native-1", messages)
	if err != nil {
		t.Fatalf("terminalSnapshotFromMessages: %v", err)
	}
	if want := (&stateSnapshotTerminal{MessageID: "history-2", Role: valAssistant, Finish: "stop"}); !reflect.DeepEqual(terminal, want) {
		t.Fatalf("terminal = %#v, want %#v", terminal, want)
	}

	for name, test := range map[string]struct {
		nativeID string
		messages []nativehermes.NativeMessage
	}{
		"empty native id":      {nativeID: ""},
		"padded native id":     {nativeID: " native-1 "},
		"noncanonical history": {nativeID: "native-1", messages: []nativehermes.NativeMessage{testHistoryMessage("live-1", "native-1", valAssistant, "stop")}},
		"skipped position":     {nativeID: "native-1", messages: []nativehermes.NativeMessage{testHistoryMessage("history-2", "native-1", valAssistant, "stop")}},
		"mismatched native id": {nativeID: "native-1", messages: []nativehermes.NativeMessage{testHistoryMessage("history-1", "native-2", valAssistant, "stop")}},
		"invalid finish":       {nativeID: "native-1", messages: []nativehermes.NativeMessage{testHistoryMessage("history-1", "native-1", valAssistant, " stop ")}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := terminalSnapshotFromMessages(test.nativeID, test.messages); err == nil {
				t.Fatal("terminalSnapshotFromMessages accepted invalid history")
			}
		})
	}
}

func TestInspectSessionStoreTerminalStateStrictlyValidatesSnapshot(t *testing.T) {
	valid := terminalInspectorSnapshot(&stateSnapshotTerminal{
		MessageID: "history-2",
		Role:      valAssistant,
		Finish:    "stop",
	})
	state, err := InspectSessionStoreTerminalState("session-1", []SessionStoreEntry{mustStateJSON(t, valid)})
	if err != nil {
		t.Fatalf("InspectSessionStoreTerminalState: %v", err)
	}
	if state.MessageID != "history-2" {
		t.Fatalf("terminal state = %#v", state)
	}

	empty := terminalInspectorSnapshot(&stateSnapshotTerminal{})
	state, err = InspectSessionStoreTerminalState("session-1", []SessionStoreEntry{mustStateJSON(t, empty)})
	if err != nil || state != (SessionStoreTerminalState{}) {
		t.Fatalf("empty terminal state = %#v, err=%v", state, err)
	}

	wrongFormat := valid
	wrongFormat.Format = "other"
	zeroCapture := valid
	zeroCapture.CapturedAtUnixMilli = 0
	wrongLogical := valid
	wrongLogical.Session.SessionID = "session-2"
	emptyNative := valid
	emptyNative.Session.NativeSessionID = ""
	paddedNative := valid
	paddedNative.Session.NativeSessionID = " native-1 "
	missingTerminal := valid
	missingTerminal.Terminal = nil
	missingArchives := valid
	missingArchives.Archives = nil
	missingWrapper := valid
	missingWrapper.Wrapper = nil
	partialTerminal := terminalInspectorSnapshot(&stateSnapshotTerminal{MessageID: "history-2"})
	wrongRole := terminalInspectorSnapshot(&stateSnapshotTerminal{MessageID: "history-2", Role: "user", Finish: "stop"})
	emptyFinish := terminalInspectorSnapshot(&stateSnapshotTerminal{MessageID: "history-2", Role: valAssistant})
	paddedFinish := terminalInspectorSnapshot(&stateSnapshotTerminal{MessageID: "history-2", Role: valAssistant, Finish: " stop "})

	for name, test := range map[string]struct {
		logicalID string
		entries   []SessionStoreEntry
	}{
		"empty logical id":    {entries: []SessionStoreEntry{mustStateJSON(t, valid)}},
		"padded logical id":   {logicalID: " session-1 ", entries: []SessionStoreEntry{mustStateJSON(t, valid)}},
		"no entries":          {logicalID: "session-1"},
		"multiple entries":    {logicalID: "session-1", entries: []SessionStoreEntry{mustStateJSON(t, valid), mustStateJSON(t, valid)}},
		"malformed json":      {logicalID: "session-1", entries: []SessionStoreEntry{json.RawMessage(`{`)}},
		"trailing json":       {logicalID: "session-1", entries: []SessionStoreEntry{json.RawMessage(`{} {}`)}},
		"unknown field":       {logicalID: "session-1", entries: []SessionStoreEntry{snapshotWithUnknownField(t, valid)}},
		"wrong format":        {logicalID: "session-1", entries: []SessionStoreEntry{mustStateJSON(t, wrongFormat)}},
		"zero capture":        {logicalID: "session-1", entries: []SessionStoreEntry{mustStateJSON(t, zeroCapture)}},
		"wrong logical id":    {logicalID: "session-1", entries: []SessionStoreEntry{mustStateJSON(t, wrongLogical)}},
		"empty native id":     {logicalID: "session-1", entries: []SessionStoreEntry{mustStateJSON(t, emptyNative)}},
		"padded native id":    {logicalID: "session-1", entries: []SessionStoreEntry{mustStateJSON(t, paddedNative)}},
		"missing terminal":    {logicalID: "session-1", entries: []SessionStoreEntry{mustStateJSON(t, missingTerminal)}},
		"missing archives":    {logicalID: "session-1", entries: []SessionStoreEntry{mustStateJSON(t, missingArchives)}},
		"missing wrapper":     {logicalID: "session-1", entries: []SessionStoreEntry{mustStateJSON(t, missingWrapper)}},
		"omitted terminal":    {logicalID: "session-1", entries: []SessionStoreEntry{snapshotWithoutField(t, valid, "terminal")}},
		"omitted archives":    {logicalID: "session-1", entries: []SessionStoreEntry{snapshotWithoutField(t, valid, "archives")}},
		"omitted wrapper":     {logicalID: "session-1", entries: []SessionStoreEntry{snapshotWithoutField(t, valid, "wrapper")}},
		"partial terminal":    {logicalID: "session-1", entries: []SessionStoreEntry{mustStateJSON(t, partialTerminal)}},
		"wrong terminal role": {logicalID: "session-1", entries: []SessionStoreEntry{mustStateJSON(t, wrongRole)}},
		"empty finish":        {logicalID: "session-1", entries: []SessionStoreEntry{mustStateJSON(t, emptyFinish)}},
		"padded finish":       {logicalID: "session-1", entries: []SessionStoreEntry{mustStateJSON(t, paddedFinish)}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := InspectSessionStoreTerminalState(test.logicalID, test.entries); err == nil {
				t.Fatal("InspectSessionStoreTerminalState accepted invalid snapshot")
			}
		})
	}

	for _, messageID := range []string{"history-0", "history-01", "history-x", "live-1", " history-1"} {
		t.Run(messageID, func(t *testing.T) {
			snapshot := terminalInspectorSnapshot(&stateSnapshotTerminal{
				MessageID: messageID,
				Role:      valAssistant,
				Finish:    "stop",
			})
			if _, err := InspectSessionStoreTerminalState("session-1", []SessionStoreEntry{mustStateJSON(t, snapshot)}); err == nil {
				t.Fatalf("accepted terminal message id %q", messageID)
			}
		})
	}
}

func TestPromptCommitsReplayStableTerminalIdentityAcrossTailCloseAndHydrate(t *testing.T) {
	ctx := context.Background()
	store := NewInMemorySessionStore()
	client := newFakeHermesClient()
	agent := newTestAgent(WithSessionStore(store))
	session := testSession(agent, client)
	if err := agent.storeStartedSession(session); err != nil {
		t.Fatalf("storeStartedSession: %v", err)
	}

	for turn, wantID := range []string{"history-2", "history-4"} {
		response, err := session.Prompt(ctx, TextPromptRequest(session.id, "terminal-turn-"+wantID, "reply"))
		if err != nil {
			t.Fatalf("Prompt turn %d: %v", turn+1, err)
		}
		if want := terminalResponseMeta(SessionStoreTerminalState{MessageID: wantID}); !reflect.DeepEqual(response.Meta, want) {
			t.Fatalf("Prompt turn %d meta = %#v, want %#v", turn+1, response.Meta, want)
		}
	}
	assertStoredTerminal(t, store, string(session.id), "history-4")

	client.mu.Lock()
	client.messages = append(client.messages, testHistoryMessage("history-5", "native-1", "user", ""))
	client.mu.Unlock()
	if err := session.snapshotToStore(ctx); err != nil {
		t.Fatalf("snapshot later user tail: %v", err)
	}
	assertStoredTerminal(t, store, string(session.id), "history-4")

	if _, err := agent.CloseSession(ctx, acp.CloseSessionRequest{SessionId: session.id}); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	assertStoredTerminal(t, store, string(session.id), "history-4")

	hydrateRoot := t.TempDir()
	hydrateXDG, err := testGenerationXDG(hydrateRoot)
	if err != nil {
		t.Fatalf("create hydrate XDG generation: %v", err)
	}
	_, snapshot, ok, err := hydrateStateFromStore(ctx, store, string(session.id), hydrateXDG)
	if err != nil {
		t.Fatalf("hydrateStateFromStore: %v", err)
	}
	if !ok || snapshot.Terminal == nil || snapshot.Terminal.MessageID != "history-4" {
		t.Fatalf("hydrated terminal = %#v, ok=%v", snapshot.Terminal, ok)
	}
}

func TestPromptRequiresDurableTerminalBeforeStoreOrResponse(t *testing.T) {
	t.Run("empty history performs no replace", func(t *testing.T) {
		store := newCountingSessionStore()
		client := newFakeHermesClient()
		client.skipHistory = true
		session := testSession(newTestAgent(WithSessionStore(store)), client)

		response, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "empty-history", "reply"))
		if err == nil || !strings.Contains(err.Error(), "missing a durable terminal assistant identity") {
			t.Fatalf("Prompt error = %v", err)
		}
		if response.Meta != nil {
			t.Fatalf("Prompt returned terminal metadata: %#v", response.Meta)
		}
		if store.replaceCount() != 0 {
			t.Fatalf("Replace count = %d, want 0", store.replaceCount())
		}
	})

	t.Run("later user without assistant does not advance", func(t *testing.T) {
		store := newCountingSessionStore()
		client := newFakeHermesClient()
		session := testSession(newTestAgent(WithSessionStore(store)), client)

		first, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "first", "reply"))
		if err != nil {
			t.Fatalf("first Prompt: %v", err)
		}
		if want := terminalResponseMeta(SessionStoreTerminalState{MessageID: "history-2"}); !reflect.DeepEqual(first.Meta, want) {
			t.Fatalf("first Prompt meta = %#v, want %#v", first.Meta, want)
		}
		if store.replaceCount() != 1 {
			t.Fatalf("first Replace count = %d, want 1", store.replaceCount())
		}

		client.skipAssistantHistory = true
		response, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "stale-second", "reply"))
		if err == nil || !strings.Contains(err.Error(), "did not advance terminal identity") {
			t.Fatalf("stale second Prompt error = %v", err)
		}
		if response.Meta != nil {
			t.Fatalf("stale second Prompt returned terminal metadata: %#v", response.Meta)
		}
		if store.replaceCount() != 1 {
			t.Fatalf("stale second Replace count = %d, want 1", store.replaceCount())
		}
		assertStoredTerminal(t, store, string(session.id), "history-2")
	})

	t.Run("failed replace preserves previous identity", func(t *testing.T) {
		store := &toggleReplaceStore{InMemorySessionStore: NewInMemorySessionStore()}
		client := newFakeHermesClient()
		agent := newTestAgent(WithSessionStore(store))
		session := testSession(agent, client)
		if err := agent.storeStartedSession(session); err != nil {
			t.Fatalf("storeStartedSession: %v", err)
		}

		first, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "first", "reply"))
		if err != nil {
			t.Fatalf("first Prompt: %v", err)
		}
		if want := terminalResponseMeta(SessionStoreTerminalState{MessageID: "history-2"}); !reflect.DeepEqual(first.Meta, want) {
			t.Fatalf("first Prompt meta = %#v, want %#v", first.Meta, want)
		}

		store.setFail(true)
		response, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "second", "reply"))
		if err == nil || !strings.Contains(err.Error(), "replace failed") {
			t.Fatalf("second Prompt error = %v", err)
		}
		if response.Meta != nil {
			t.Fatalf("failed Prompt returned terminal metadata: %#v", response.Meta)
		}
		if terminal := session.committedTerminalState(); terminal.MessageID != "history-2" {
			t.Fatalf("committed terminal = %#v", terminal)
		}
		assertStoredTerminal(t, store, string(session.id), "history-2")

		retry, retryErr := session.Prompt(t.Context(), TextPromptRequest(session.id, "retry", "reply"))
		if retryErr == nil || !strings.Contains(retryErr.Error(), "session_poisoned") {
			t.Fatalf("retry Prompt error = %v", retryErr)
		}
		if retry.Meta != nil {
			t.Fatalf("retry Prompt returned terminal metadata: %#v", retry.Meta)
		}

		if _, closeErr := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id}); closeErr != nil {
			t.Fatalf("CloseSession after failed Replace: %v", closeErr)
		}
		assertStoredTerminal(t, store, string(session.id), "history-2")
	})
}

func TestSnapshotRejectsTerminalRegressionBeforeReplace(t *testing.T) {
	store := newCountingSessionStore()
	client := newFakeHermesClient()
	session := testSession(newTestAgent(WithSessionStore(store)), client)

	for _, nonce := range []string{"regression-first", "regression-second"} {
		if _, err := session.Prompt(t.Context(), TextPromptRequest(session.id, nonce, "reply")); err != nil {
			t.Fatalf("Prompt(%s): %v", nonce, err)
		}
	}
	if store.replaceCount() != 2 {
		t.Fatalf("Replace count before regression = %d, want 2", store.replaceCount())
	}
	assertStoredTerminal(t, store, string(session.id), "history-4")

	client.mu.Lock()
	client.messages = append([]nativehermes.NativeMessage(nil), client.messages[:2]...)
	client.mu.Unlock()
	if err := session.snapshotToStore(t.Context()); err == nil || !strings.Contains(err.Error(), "terminal identity regressed") {
		t.Fatalf("regressed snapshot error = %v", err)
	}
	if store.replaceCount() != 2 {
		t.Fatalf("Replace count after regression = %d, want 2", store.replaceCount())
	}
	assertStoredTerminal(t, store, string(session.id), "history-4")
}

func TestLoadedTerminalBaselineRequiresNextPromptToAdvance(t *testing.T) {
	store, loaded := loadSeededTerminalSession(t, true)

	replaces := store.replaceCount()
	response, err := loaded.Prompt(t.Context(), TextPromptRequest(loaded.id, "loaded-stale", "reply"))
	if err == nil || !strings.Contains(err.Error(), "did not advance terminal identity") {
		t.Fatalf("loaded stale Prompt error = %v", err)
	}
	if response.Meta != nil {
		t.Fatalf("loaded stale Prompt returned terminal metadata: %#v", response.Meta)
	}
	if store.replaceCount() != replaces {
		t.Fatalf("loaded stale Prompt Replace count = %d, want %d", store.replaceCount(), replaces)
	}
	assertStoredTerminal(t, store, string(loaded.id), "history-2")
}

func TestLoadedTerminalBaselineAllowsStrictAdvance(t *testing.T) {
	store, loaded := loadSeededTerminalSession(t, false)

	response, err := loaded.Prompt(t.Context(), TextPromptRequest(loaded.id, "loaded-advance", "reply"))
	if err != nil {
		t.Fatalf("loaded advancing Prompt: %v", err)
	}
	if want := terminalResponseMeta(SessionStoreTerminalState{MessageID: "history-4"}); !reflect.DeepEqual(response.Meta, want) {
		t.Fatalf("loaded advancing Prompt meta = %#v, want %#v", response.Meta, want)
	}
	assertStoredTerminal(t, store, string(loaded.id), "history-4")
}

func loadSeededTerminalSession(t *testing.T, skipAssistantHistory bool) (*countingSessionStore, *session) {
	t.Helper()

	store := newCountingSessionStore()
	cwd := t.TempDir()
	sourceClient := newFakeHermesClient()
	source := testSession(newTestAgent(WithSessionStore(store)), sourceClient)
	source.cwd = cwd
	if _, err := source.Prompt(t.Context(), TextPromptRequest(source.id, "seed", "reply")); err != nil {
		t.Fatalf("seed Prompt: %v", err)
	}
	assertStoredTerminal(t, store, string(source.id), "history-2")

	loadedClient := newFakeHermesClient()
	loadedClient.getSession = testNativeSession("native-1")
	sourceClient.mu.Lock()
	loadedClient.messages = append([]nativehermes.NativeMessage(nil), sourceClient.messages...)
	sourceClient.mu.Unlock()
	loadedClient.skipAssistantHistory = skipAssistantHistory
	loadedAgent := newTestAgent(
		WithScratchDir(t.TempDir()),
		WithSessionStore(store),
		func(options *Options) {
			options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
				loadedClient.xdg = opts.ExistingXDG

				return loadedClient, nil
			}
		},
	)
	if _, err := loadedAgent.LoadSession(t.Context(), LoadSessionRequest(source.id, cwd)); err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	loaded := loadedAgent.activeSession(source.id)
	if loaded == nil || loaded.committedTerminalState().MessageID != "history-2" {
		t.Fatalf("loaded terminal baseline = %#v", loaded)
	}

	return store, loaded
}

func TestPromptSnapshotHistoryReadUsesIndependentDeadline(t *testing.T) {
	store := newCountingSessionStore()
	client := newFakeHermesClient()
	deadlineSeen := make(chan bool, 1)
	client.messagesFunc = func(ctx context.Context, _ string) ([]nativehermes.NativeMessage, error) {
		_, ok := ctx.Deadline()
		deadlineSeen <- ok
		<-ctx.Done()

		return nil, ctx.Err()
	}
	agent := newTestAgent(WithSessionStore(store), func(options *Options) {
		options.storeWriteTTL = time.Millisecond
	})
	session := testSession(agent, client)

	response, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "history-timeout", "reply"))
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Prompt history timeout error = %v", err)
	}
	if response.Meta != nil {
		t.Fatalf("timed-out Prompt returned terminal metadata: %#v", response.Meta)
	}
	if !<-deadlineSeen {
		t.Fatal("Messages context had no deadline")
	}
	if store.replaceCount() != 0 {
		t.Fatalf("timed-out Prompt Replace count = %d, want 0", store.replaceCount())
	}
}

func TestPromptRequiresLiveClientForTerminalSnapshotCommit(t *testing.T) {
	store := newCountingSessionStore()
	client := newFakeHermesClient()
	var session *session
	agent := newTestAgent(WithSessionStore(store), func(options *Options) {
		options.beforeTerminalCommit = func() {
			session.mu.Lock()
			session.client = nil
			session.mu.Unlock()
		}
	})
	session = testSession(agent, client)

	response, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "nil-client-commit", "reply"))
	if err == nil || !strings.Contains(err.Error(), "runtime is unavailable for terminal snapshot commit") {
		t.Fatalf("Prompt nil-client terminal error = %v", err)
	}
	if response.Meta != nil {
		t.Fatalf("nil-client Prompt returned terminal metadata: %#v", response.Meta)
	}
	if store.replaceCount() != 0 {
		t.Fatalf("nil-client Prompt Replace count = %d, want 0", store.replaceCount())
	}
	if poisonErr := session.ensureNotPoisoned(); poisonErr == nil || !strings.Contains(poisonErr.Error(), "session_poisoned") {
		t.Fatalf("nil-client session poison error = %v", poisonErr)
	}
}

func TestCancelAndTerminalReplaceHaveOneSettlementBoundary(t *testing.T) {
	t.Run("cancel wins during capture", func(t *testing.T) {
		store := newCountingSessionStore()
		client := newFakeHermesClient()
		session := testSession(newTestAgent(WithSessionStore(store)), client)
		if _, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "cancel-seed", "reply")); err != nil {
			t.Fatalf("seed Prompt: %v", err)
		}

		messagesEntered := make(chan struct{})
		releaseMessages := make(chan struct{})
		client.messagesFunc = func(context.Context, string) ([]nativehermes.NativeMessage, error) {
			close(messagesEntered)
			<-releaseMessages

			client.mu.Lock()
			defer client.mu.Unlock()

			return append([]nativehermes.NativeMessage(nil), client.messages...), nil
		}

		promptDone := make(chan struct {
			response acp.PromptResponse
			err      error
		}, 1)
		go func() {
			response, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "cancel-wins", "reply"))
			promptDone <- struct {
				response acp.PromptResponse
				err      error
			}{response: response, err: err}
		}()
		<-messagesEntered

		if err := session.cancelRouted(turnRouteMeta("cancel-wins")); err != nil {
			t.Fatalf("Cancel: %v", err)
		}
		close(releaseMessages)

		result := <-promptDone
		if result.err != nil {
			t.Fatalf("cancel-winning Prompt error: %v", result.err)
		}
		if result.response.StopReason != acp.StopReasonCancelled || result.response.Meta != nil {
			t.Fatalf("cancel-winning Prompt response = %#v", result.response)
		}
		if store.replaceCount() != 2 {
			t.Fatalf("cancel-winning Replace count = %d, want 2", store.replaceCount())
		}
		if !session.needsRuntimeResume() || client.closeCount() != 1 {
			t.Fatalf("cancel-winning fence needsResume=%v close=%d", session.needsRuntimeResume(), client.closeCount())
		}
		assertStoredTerminal(t, store, string(session.id), "history-2")
	})

	t.Run("commit wins before replace", func(t *testing.T) {
		store := newBlockingTerminalReplaceStore(2)
		client := newFakeHermesClient()
		session := testSession(newTestAgent(WithSessionStore(store)), client)
		if _, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "commit-seed", "reply")); err != nil {
			t.Fatalf("seed Prompt: %v", err)
		}

		promptDone := make(chan struct {
			response acp.PromptResponse
			err      error
		}, 1)
		go func() {
			response, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "commit-wins", "reply"))
			promptDone <- struct {
				response acp.PromptResponse
				err      error
			}{response: response, err: err}
		}()
		<-store.started

		cancelDone := make(chan error, 1)
		go func() { cancelDone <- session.cancelRouted(turnRouteMeta("commit-wins")) }()
		select {
		case err := <-cancelDone:
			if err != nil {
				t.Fatalf("post-claim Cancel: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("post-claim Cancel waited for terminal Replace")
		}
		if client.closeCount() != 0 {
			t.Fatalf("post-claim Cancel closed runtime %d times", client.closeCount())
		}

		close(store.release)
		result := <-promptDone
		if result.err != nil {
			t.Fatalf("commit-winning Prompt: %v", result.err)
		}
		if want := terminalResponseMeta(SessionStoreTerminalState{MessageID: "history-4"}); !reflect.DeepEqual(result.response.Meta, want) {
			t.Fatalf("commit-winning Prompt meta = %#v, want %#v", result.response.Meta, want)
		}
		if store.replaceCount() != 2 || session.needsRuntimeResume() {
			t.Fatalf("commit-winning state replaces=%d needsResume=%v", store.replaceCount(), session.needsRuntimeResume())
		}
		assertStoredTerminal(t, store, string(session.id), "history-4")
	})
}

type blockingTerminalReplaceStore struct {
	*InMemorySessionStore
	mu      sync.Mutex
	calls   int
	blockOn int
	started chan struct{}
	release chan struct{}
}

func newBlockingTerminalReplaceStore(blockOn int) *blockingTerminalReplaceStore {
	return &blockingTerminalReplaceStore{
		InMemorySessionStore: NewInMemorySessionStore(),
		blockOn:              blockOn,
		started:              make(chan struct{}),
		release:              make(chan struct{}),
	}
}

func (s *blockingTerminalReplaceStore) Replace(
	ctx context.Context,
	main SessionKey,
	replacements []SessionStoreReplacement,
) error {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()

	if call == s.blockOn {
		close(s.started)
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return s.InMemorySessionStore.Replace(ctx, main, replacements)
}

func (s *blockingTerminalReplaceStore) replaceCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.calls
}

func TestFinalEmitFailureCannotBeAdoptedByCloseOrRetry(t *testing.T) {
	t.Run("close preserves prior terminal", func(t *testing.T) {
		store, session, client, conn := seededTerminalFailureSession(t)
		agent := session.agent

		response, err := promptWithFinalUpdate(t, session, client, "emit-close")
		if err == nil || !strings.Contains(err.Error(), "final update failed") {
			t.Fatalf("final emit Prompt error = %v", err)
		}
		if response.Meta != nil {
			t.Fatalf("final emit failure returned terminal metadata: %#v", response.Meta)
		}
		if !session.needsRuntimeResume() || client.closeCount() != 1 {
			t.Fatalf("final emit fence needsResume=%v close=%d", session.needsRuntimeResume(), client.closeCount())
		}
		if store.replaceCount() != 2 {
			t.Fatalf("Replace count after final emit failure = %d, want 2", store.replaceCount())
		}
		assertStoredTerminal(t, store, string(session.id), "history-2")
		entries, loadErr := store.Load(t.Context(), SessionKey{
			SessionID: string(session.id), Subpath: SessionStoreMainSubpath,
		})
		if loadErr != nil || len(entries) != 1 {
			t.Fatalf("load failed boundary: entries=%d err=%v", len(entries), loadErr)
		}

		var snapshot stateSnapshot
		if decodeErr := json.Unmarshal(entries[0], &snapshot); decodeErr != nil {
			t.Fatalf("decode failed boundary: %v", decodeErr)
		}
		if foreground := snapshot.Wrapper.Foreground; foreground == nil ||
			foreground.Outcome != string(lifecycle.OutcomeFailed) || foreground.StopReason != "" || foreground.Text != "" {
			t.Fatalf("stored failed foreground = %#v", foreground)
		}

		conn.updateErr = nil
		if _, closeErr := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id}); closeErr != nil {
			t.Fatalf("CloseSession after final emit failure: %v", closeErr)
		}
		if store.replaceCount() != 2 {
			t.Fatalf("CloseSession adopted failed history; Replace count = %d", store.replaceCount())
		}
		assertStoredTerminal(t, store, string(session.id), "history-2")
	})

	t.Run("retry resumes prior terminal before advancing", func(t *testing.T) {
		store, session, client, conn := seededTerminalFailureSession(t)
		agent := session.agent

		if _, err := promptWithFinalUpdate(t, session, client, "emit-retry"); err == nil ||
			!strings.Contains(err.Error(), "final update failed") {
			t.Fatalf("final emit Prompt error = %v", err)
		}
		assertStoredTerminal(t, store, string(session.id), "history-2")

		resumedClient := newFakeHermesClient()
		resumedClient.getSession = testNativeSession("native-1")
		resumedClient.messages = []nativehermes.NativeMessage{
			testHistoryMessage("history-1", "native-1", "user", ""),
			testHistoryMessage("history-2", "native-1", valAssistant, "stop"),
		}
		agent.options.clientFactory = func(_ context.Context, opts nativehermes.StartOptions) (nativehermes.Server, error) {
			resumedClient.xdg = opts.ExistingXDG

			return resumedClient, nil
		}
		conn.updateErr = nil

		response, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "safe-retry", "reply"))
		if err != nil {
			t.Fatalf("safe retry Prompt: %v", err)
		}
		if want := terminalResponseMeta(SessionStoreTerminalState{MessageID: "history-4"}); !reflect.DeepEqual(response.Meta, want) {
			t.Fatalf("safe retry meta = %#v, want %#v", response.Meta, want)
		}
		if store.replaceCount() != 3 {
			t.Fatalf("safe retry Replace count = %d, want 3", store.replaceCount())
		}
		assertStoredTerminal(t, store, string(session.id), "history-4")
	})
}

func TestFailedTurnPersistsVisiblePrefixWithoutAdvancingNativeTerminal(t *testing.T) {
	store := newCountingSessionStore()
	client := newFakeHermesClient()
	session := testSession(newTestAgent(WithSessionStore(store)), client)
	if _, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "failure-seed", "reply")); err != nil {
		t.Fatalf("seed Prompt: %v", err)
	}

	baseline := session.committedTerminalState()
	turnCtx := session.beginTurn(t.Context(), "failed-turn")
	session.mu.Lock()
	session.turnInFlight = true
	session.mu.Unlock()
	session.recordForegroundPrefix("visible prefix")
	wantErr := errors.New("provider failed")

	_, published, err := session.settlePrompt(t.Context(), turnCtx, session.currentTurnEpoch(), baseline, promptRun{
		settle: true, err: wantErr, endsIncarnation: true,
	}, nil)
	if !errors.Is(err, wantErr) || !published {
		t.Fatalf("failed settlement published=%v err=%v", published, err)
	}
	if store.replaceCount() != 2 {
		t.Fatalf("failed settlement Replace count = %d, want 2", store.replaceCount())
	}
	assertStoredTerminal(t, store, string(session.id), baseline.MessageID)

	entries, loadErr := store.Load(t.Context(), SessionKey{
		SessionID: string(session.id), Subpath: SessionStoreMainSubpath,
	})
	if loadErr != nil || len(entries) != 1 {
		t.Fatalf("load failed boundary: entries=%d err=%v", len(entries), loadErr)
	}

	var snapshot stateSnapshot
	if decodeErr := json.Unmarshal(entries[0], &snapshot); decodeErr != nil {
		t.Fatalf("decode failed boundary: %v", decodeErr)
	}
	foreground := snapshot.Wrapper.Foreground
	if foreground == nil || foreground.Outcome != string(lifecycle.OutcomeFailed) ||
		foreground.StopReason != "" || foreground.Text != "visible prefix" {
		t.Fatalf("stored failed foreground = %#v", foreground)
	}
}

func seededTerminalFailureSession(t *testing.T) (*countingSessionStore, *session, *fakeHermesClient, *recordingAgentClient) {
	t.Helper()

	store := newCountingSessionStore()
	client := newFakeHermesClient()
	conn := newRecordingAgentClient()
	agent := newTestAgent(WithScratchDir(t.TempDir()), WithSessionStore(store))
	agent.setAgentClient(conn)
	session := testSession(agent, client)
	session.cwd = t.TempDir()
	if err := agent.storeStartedSession(session); err != nil {
		t.Fatalf("storeStartedSession: %v", err)
	}
	if _, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "emit-seed", "reply")); err != nil {
		t.Fatalf("seed Prompt: %v", err)
	}
	assertStoredTerminal(t, store, string(session.id), "history-2")
	conn.updateErr = errors.New("final update failed")

	return store, session, client, conn
}

func promptWithFinalUpdate(
	t *testing.T,
	session *session,
	client *fakeHermesClient,
	nonce string,
) (acp.PromptResponse, error) {
	t.Helper()

	client.sendMessage = func(_ context.Context, id string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		return nativehermes.NativeMessage{
			Info: nativehermes.NativeMessageInfo{ID: "live-final", SessionID: id, Role: valAssistant, Finish: "stop"},
			Parts: []nativehermes.Part{{
				ID: "final-part", SessionID: id, MessageID: "live-final", Type: valText, Text: "done",
			}},
		}, nil
	}

	return session.Prompt(t.Context(), TextPromptRequest(session.id, nonce, "reply"))
}

func TestCloseSessionWaitsForTerminalSnapshotCommit(t *testing.T) {
	store := newCountingSessionStore()
	client := newFakeHermesClient()
	agent := newTestAgent(WithSessionStore(store))
	session := testSession(agent, client)
	if err := agent.storeStartedSession(session); err != nil {
		t.Fatalf("storeStartedSession: %v", err)
	}
	if _, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "close-seed", "reply")); err != nil {
		t.Fatalf("seed Prompt: %v", err)
	}

	messagesEntered := make(chan struct{})
	releaseMessages := make(chan struct{})
	var enteredOnce sync.Once
	client.messagesFunc = func(context.Context, string) ([]nativehermes.NativeMessage, error) {
		enteredOnce.Do(func() { close(messagesEntered) })
		<-releaseMessages

		client.mu.Lock()
		defer client.mu.Unlock()

		return append([]nativehermes.NativeMessage(nil), client.messages...), nil
	}
	closedAtTerminal := make(chan string, 1)
	client.closeFunc = func(ctx context.Context) error {
		entries, err := store.Load(ctx, SessionKey{SessionID: string(session.id), Subpath: SessionStoreMainSubpath})
		if err != nil {
			return err
		}
		terminal, err := InspectSessionStoreTerminalState(string(session.id), entries)
		if err != nil {
			return err
		}
		closedAtTerminal <- terminal.MessageID

		return nil
	}

	promptDone := make(chan struct {
		response acp.PromptResponse
		err      error
	}, 1)
	go func() {
		response, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "close-race", "reply"))
		promptDone <- struct {
			response acp.PromptResponse
			err      error
		}{response: response, err: err}
	}()
	<-messagesEntered
	if session.lifecycleMu.TryLock() {
		session.lifecycleMu.Unlock()
		t.Fatal("terminal snapshot did not hold the lifecycle lock")
	}

	closeStarted := make(chan struct{})
	closeDone := make(chan error, 1)
	go func() {
		close(closeStarted)
		_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
		closeDone <- err
	}()
	<-closeStarted

	close(releaseMessages)
	promptResult := <-promptDone
	if promptResult.err != nil {
		t.Fatalf("racing Prompt: %v", promptResult.err)
	}
	if want := terminalResponseMeta(SessionStoreTerminalState{MessageID: "history-4"}); !reflect.DeepEqual(promptResult.response.Meta, want) {
		t.Fatalf("racing Prompt meta = %#v, want %#v", promptResult.response.Meta, want)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	if terminal := <-closedAtTerminal; terminal != "history-4" {
		t.Fatalf("client closed at terminal %q, want history-4", terminal)
	}
}

func TestCloseBeforeTerminalCommitWaitsForCancelledSettlement(t *testing.T) {
	store := newCountingSessionStore()
	client := newFakeHermesClient()
	commitReady := make(chan struct{})
	releaseCommit := make(chan struct{})
	agent := newTestAgent(WithSessionStore(store), func(options *Options) {
		options.beforeTerminalCommit = func() {
			close(commitReady)
			<-releaseCommit
		}
	})
	session := testSession(agent, client)
	if err := agent.storeStartedSession(session); err != nil {
		t.Fatalf("storeStartedSession: %v", err)
	}

	promptDone := make(chan struct {
		response acp.PromptResponse
		err      error
	}, 1)
	go func() {
		response, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "close-wins", "reply"))
		promptDone <- struct {
			response acp.PromptResponse
			err      error
		}{response: response, err: err}
	}()
	<-commitReady

	closeDone := make(chan error, 1)
	go func() {
		_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
		closeDone <- err
	}()
	select {
	case err := <-closeDone:
		t.Fatalf("CloseSession returned before settlement: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	if store.replaceCount() != 0 {
		t.Fatalf("pre-settlement Replace count = %d, want 0", store.replaceCount())
	}
	close(releaseCommit)

	result := <-promptDone
	if result.err != nil || result.response.StopReason != acp.StopReasonCancelled {
		t.Fatalf("cancelled Prompt = %#v err=%v", result.response, result.err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	if store.replaceCount() != 1 {
		t.Fatalf("post-settlement Replace count = %d, want 1", store.replaceCount())
	}
}

func TestIdleCloseSerializesAgainstPromptAdmission(t *testing.T) {
	store := newCountingSessionStore()
	client := newFakeHermesClient()
	closeEntered := make(chan struct{})
	releaseClose := make(chan struct{})
	client.closeFunc = func(context.Context) error {
		close(closeEntered)
		<-releaseClose

		return nil
	}
	sendCalled := make(chan struct{}, 1)
	client.sendMessage = func(_ context.Context, id string, _ nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		sendCalled <- struct{}{}

		return testHistoryMessage("live", id, valAssistant, "stop"), nil
	}
	agent := newTestAgent(WithSessionStore(store))
	session := testSession(agent, client)
	if err := agent.storeStartedSession(session); err != nil {
		t.Fatalf("storeStartedSession: %v", err)
	}

	closeDone := make(chan error, 1)
	go func() {
		_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
		closeDone <- err
	}()
	<-closeEntered

	promptDone := make(chan error, 1)
	go func() {
		_, err := session.Prompt(t.Context(), TextPromptRequest(session.id, "idle-close-race", "reply"))
		promptDone <- err
	}()
	close(releaseClose)
	if err := <-closeDone; err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	if err := <-promptDone; err == nil || !strings.Contains(err.Error(), "session closed") {
		t.Fatalf("Prompt racing idle close error = %v", err)
	}
	select {
	case <-sendCalled:
		t.Fatal("closed session admitted native SendMessage")
	default:
	}
}

func TestRequiredTerminalSnapshotRejectsOtherPendingWork(t *testing.T) {
	for name, configure := range map[string]func(*session){
		"permission": func(session *session) {
			session.pending["permission"] = nativehermes.PermissionRequest{}
		},
		"elicitation": func(session *session) {
			session.questions["question"] = nativehermes.QuestionRequest{}
		},
		"generation": func(session *session) {
			session.activeMessageIDs["message"] = struct{}{}
		},
	} {
		t.Run(name, func(t *testing.T) {
			store := newCountingSessionStore()
			client := newFakeHermesClient()
			client.messages = []nativehermes.NativeMessage{
				testHistoryMessage("history-1", "native-1", "user", ""),
				testHistoryMessage("history-2", "native-1", valAssistant, "stop"),
			}
			session := testSession(newTestAgent(WithSessionStore(store)), client)
			session.turnInFlight = true
			configure(session)

			session.lifecycleMu.Lock()
			err := session.snapshotToStoreLocked(t.Context(), &terminalSnapshotRequirement{
				baseline: SessionStoreTerminalState{}, turnEpoch: session.turnEpoch,
			})
			session.lifecycleMu.Unlock()
			if err == nil || !strings.Contains(err.Error(), "pending") {
				t.Fatalf("required snapshot with %s error = %v", name, err)
			}
			if store.replaceCount() != 0 {
				t.Fatalf("required snapshot with %s Replace count = %d", name, store.replaceCount())
			}
		})
	}
}

func TestTerminalCommitClaimRejectsInvalidSettlement(t *testing.T) {
	newActive := func() *session {
		session := testSession(newTestAgent(), newFakeHermesClient())
		session.turnInFlight = true
		session.turnEpoch = 7
		session.turnSettlement = turnSettlementOpen

		return session
	}

	stale := newActive()
	if _, err := stale.claimTerminalCommit(8); err == nil || !strings.Contains(err.Error(), "stale turn epoch") {
		t.Fatalf("stale claim error = %v", err)
	}

	notActive := newActive()
	notActive.turnInFlight = false
	if _, err := notActive.claimTerminalCommit(7); err == nil || !strings.Contains(err.Error(), "stale turn epoch") {
		t.Fatalf("inactive claim error = %v", err)
	}

	cancelled := newActive()
	cancelled.turnSettlement = turnSettlementCancelled
	if raced, err := cancelled.claimTerminalCommit(7); err != nil || !raced {
		t.Fatalf("cancelled claim error = %v", err)
	}

	alreadyClaimed := newActive()
	alreadyClaimed.turnSettlement = turnSettlementCommitting
	if _, err := alreadyClaimed.claimTerminalCommit(7); err == nil ||
		!strings.Contains(err.Error(), "already claimed") {
		t.Fatalf("duplicate claim error = %v", err)
	}
}

func TestCloseLifecycleAdmissionCancelsOnlyBeforeSettlement(t *testing.T) {
	for _, test := range []struct {
		name       string
		state      turnSettlementState
		wantCancel bool
	}{
		{name: "admitted before native turn", state: turnSettlementIdle, wantCancel: true},
		{name: "native turn", state: turnSettlementOpen, wantCancel: true},
		{name: "settling", state: turnSettlementCapturing},
		{name: "committing", state: turnSettlementCommitting},
		{name: "already cancelled", state: turnSettlementCancelled},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := testSession(newTestAgent(), newFakeHermesClient())
			session.mu.Lock()
			settlement := session.reservePromptLocked()
			session.settlement = settlement
			session.turnInFlight = true
			session.turnSettlement = test.state
			session.mu.Unlock()
			defer settlement.complete()

			if got := session.closeLifecycleAdmission(); got != test.wantCancel {
				t.Fatalf("cancel = %v, want %v", got, test.wantCancel)
			}
			if !session.lifecycleClosing {
				t.Fatal("prompt admission remained open")
			}

			wantState := test.state
			if test.wantCancel {
				wantState = turnSettlementCancelled
			}
			if session.turnSettlement != wantState {
				t.Fatalf("settlement state = %d, want %d", session.turnSettlement, wantState)
			}
		})
	}
}

func TestAdmittedCloseCancellationSurvivesTurnStart(t *testing.T) {
	session := testSession(newTestAgent(), newFakeHermesClient())
	session.mu.Lock()
	settlement := session.reservePromptLocked()
	session.settlement = settlement
	session.turnInFlight = true
	session.mu.Unlock()
	defer settlement.complete()

	if !session.closeLifecycleAdmission() {
		t.Fatal("admitted turn was not cancelled")
	}

	turnCtx := session.beginTurn(t.Context(), "closing-turn")
	if !errors.Is(turnCtx.Err(), context.Canceled) {
		t.Fatalf("turn context error = %v, want cancellation", turnCtx.Err())
	}
	if !session.wasCancelled() {
		t.Fatal("turn lost admitted close cancellation")
	}

	session.finishTurn()
}

func TestRequiredTerminalSnapshotPropagatesCancelledCommitClaim(t *testing.T) {
	store := newCountingSessionStore()
	client := newFakeHermesClient()
	client.messages = []nativehermes.NativeMessage{
		testHistoryMessage("history-1", "native-1", "user", ""),
		testHistoryMessage("history-2", "native-1", valAssistant, "stop"),
	}
	session := testSession(newTestAgent(WithSessionStore(store)), client)
	session.turnInFlight = true
	session.turnEpoch = 1
	session.turnSettlement = turnSettlementCancelled

	session.lifecycleMu.Lock()
	err := session.snapshotToStoreLocked(t.Context(), &terminalSnapshotRequirement{
		baseline: SessionStoreTerminalState{}, turnEpoch: 1,
	})
	session.lifecycleMu.Unlock()
	if !errors.Is(err, errPromptCancelled) {
		t.Fatalf("cancelled required snapshot error = %v", err)
	}
	if store.replaceCount() != 0 {
		t.Fatalf("cancelled required snapshot Replace count = %d", store.replaceCount())
	}
}

func TestRequiredTerminalSnapshotStopsAfterCaptureCancellation(t *testing.T) {
	store := newCountingSessionStore()
	client := newFakeHermesClient()
	turnCtx, cancelTurn := context.WithCancel(t.Context())
	client.messagesFunc = func(snapshotCtx context.Context, _ string) ([]nativehermes.NativeMessage, error) {
		cancelTurn()
		<-snapshotCtx.Done()

		return []nativehermes.NativeMessage{
			testHistoryMessage("history-1", "native-1", "user", ""),
			testHistoryMessage("history-2", "native-1", valAssistant, "stop"),
		}, nil
	}
	session := testSession(newTestAgent(WithSessionStore(store)), client)
	session.turnInFlight = true
	session.turnEpoch = 1
	session.turnSettlement = turnSettlementOpen

	session.lifecycleMu.Lock()
	err := session.snapshotToStoreLocked(turnCtx, &terminalSnapshotRequirement{
		baseline: SessionStoreTerminalState{}, turnEpoch: 1,
	})
	session.lifecycleMu.Unlock()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled capture error = %v", err)
	}
	if store.replaceCount() != 0 {
		t.Fatalf("cancelled capture Replace count = %d", store.replaceCount())
	}
}

func TestUsageUpdateDoesNotClaimTerminalIdentity(t *testing.T) {
	update := usageUpdateFromTokens(nativehermes.Tokens{Total: 7}, 128)
	if update == nil || update.UsageUpdate == nil {
		t.Fatal("usageUpdateFromTokens returned nil")
	}
	if update.UsageUpdate.Meta != nil {
		t.Fatalf("usage update meta = %#v, want nil", update.UsageUpdate.Meta)
	}
}

func testHistoryMessage(id string, nativeSessionID string, role string, finish string) nativehermes.NativeMessage {
	return nativehermes.NativeMessage{Info: nativehermes.NativeMessageInfo{
		ID:        id,
		SessionID: nativeSessionID,
		Role:      role,
		Finish:    finish,
	}}
}

func terminalInspectorSnapshot(terminal *stateSnapshotTerminal) stateSnapshot {
	return stateSnapshot{
		Format:              SessionStoreFormat,
		CapturedAtUnixMilli: 1,
		Session: stateSnapshotSession{
			SessionID:       "session-1",
			NativeSessionID: "native-1",
			Env:             map[string]string{},
			ExtraPathDirs:   []string{},
		},
		Terminal: terminal,
		Archives: map[string]archiveInfo{},
		Wrapper:  &stateSnapshotWrapper{},
	}
}

func snapshotWithUnknownField(t *testing.T, snapshot stateSnapshot) SessionStoreEntry {
	t.Helper()

	var raw map[string]any
	if err := json.Unmarshal(mustStateJSON(t, snapshot), &raw); err != nil {
		t.Fatalf("json.Unmarshal snapshot: %v", err)
	}
	raw["unknown"] = true

	return mustStateJSON(t, raw)
}

func snapshotWithoutField(t *testing.T, snapshot stateSnapshot, field string) SessionStoreEntry {
	t.Helper()

	var raw map[string]any
	if err := json.Unmarshal(mustStateJSON(t, snapshot), &raw); err != nil {
		t.Fatalf("json.Unmarshal snapshot: %v", err)
	}
	delete(raw, field)

	return mustStateJSON(t, raw)
}

func assertStoredTerminal(t *testing.T, store SessionStore, sessionID string, wantMessageID string) {
	t.Helper()

	entries, err := store.Load(t.Context(), SessionKey{SessionID: sessionID, Subpath: SessionStoreMainSubpath})
	if err != nil {
		t.Fatalf("Load main snapshot: %v", err)
	}
	terminal, err := InspectSessionStoreTerminalState(sessionID, entries)
	if err != nil {
		t.Fatalf("InspectSessionStoreTerminalState: %v", err)
	}
	if terminal.MessageID != wantMessageID {
		t.Fatalf("stored terminal = %#v, want %q", terminal, wantMessageID)
	}
}

func TestCommittedTerminalStateAndResponseMetaAreExact(t *testing.T) {
	session := testSession(newTestAgent(), newFakeHermesClient())
	session.markMessageCompleted("")
	if got := session.committedTerminalState(); got != (SessionStoreTerminalState{}) {
		t.Fatalf("empty committedTerminalState = %#v", got)
	}
	terminal := SessionStoreTerminalState{
		MessageID: "history-9", Outcome: string(lifecycle.OutcomeSuccess), StopReason: lifecycle.StopReasonEndTurn,
	}
	session.mu.Lock()
	session.committed.terminal = terminal
	session.mu.Unlock()
	got := session.committedTerminalState()
	if got != terminal {
		t.Fatalf("committedTerminalState = %#v", got)
	}
	if want := map[string]any{hermesMetaKey: map[string]any{
		keyMessageID: "history-9", keyOutcome: lifecycle.OutcomeSuccess, keyStopReason: lifecycle.StopReasonEndTurn,
	}}; !reflect.DeepEqual(terminalResponseMeta(got), want) {
		t.Fatalf("terminalResponseMeta = %#v, want %#v", terminalResponseMeta(got), want)
	}
	if got := publicTerminalState(nil, nil); got != (SessionStoreTerminalState{}) {
		t.Fatalf("publicTerminalState(nil) = %#v", got)
	}
}

func TestValidateTerminalTransitionRejectsInvalidIdentities(t *testing.T) {
	for name, test := range map[string]struct {
		previous SessionStoreTerminalState
		next     SessionStoreTerminalState
	}{
		"invalid previous": {previous: SessionStoreTerminalState{MessageID: "live-1"}},
		"invalid next":     {next: SessionStoreTerminalState{MessageID: "live-1"}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateTerminalTransition(test.previous, test.next, false); err == nil {
				t.Fatal("validateTerminalTransition accepted invalid identity")
			}
		})
	}

	equal := SessionStoreTerminalState{MessageID: "history-2"}
	if err := validateTerminalTransition(equal, equal, false); err != nil {
		t.Fatalf("ordinary equal terminal transition: %v", err)
	}
}

func TestRequireJSONEOFPropagatesNonEOFDecoderError(t *testing.T) {
	decoder := json.NewDecoder(&alwaysFailReader{err: errors.New("read failed")})
	if err := requireJSONEOF(decoder); err == nil || !strings.Contains(err.Error(), "read failed") {
		t.Fatalf("requireJSONEOF error = %v", err)
	}
}

type alwaysFailReader struct {
	err error
}

func (r *alwaysFailReader) Read([]byte) (int, error) {
	return 0, r.err
}

func TestStoredLifecycleBoundaryValidation(t *testing.T) {
	valid := func() stateSnapshot {
		return stateSnapshot{
			Session: stateSnapshotSession{
				Env:           map[string]string{},
				ExtraPathDirs: []string{},
			},
			Archives: map[string]archiveInfo{},
			Terminal: &stateSnapshotTerminal{},
			Wrapper: &stateSnapshotWrapper{Foreground: &stateSnapshotForeground{
				StreamID: "stream", TurnID: "turn", CapturedAtUnixMilli: 1,
				Outcome: string(lifecycle.OutcomeSuccess), StopReason: string(acp.StopReasonEndTurn),
			}},
		}
	}

	for name, mutate := range map[string]func(*stateSnapshot){
		"missing session environment": func(snapshot *stateSnapshot) {
			snapshot.Session.Env = nil
		},
		"missing session path directories": func(snapshot *stateSnapshot) {
			snapshot.Session.ExtraPathDirs = nil
		},
		"reserved session environment": func(snapshot *stateSnapshot) {
			snapshot.Session.Env = map[string]string{"PATH": "/untrusted"}
		},
		"invalid session path directory": func(snapshot *stateSnapshot) {
			snapshot.Session.ExtraPathDirs = []string{"relative"}
		},
		"identity": func(snapshot *stateSnapshot) {
			snapshot.Wrapper.Foreground.TurnID = ""
		},
		"outcome": func(snapshot *stateSnapshot) {
			snapshot.Wrapper.Foreground.Outcome = "future"
		},
		"failed stop reason": func(snapshot *stateSnapshot) {
			snapshot.Wrapper.Foreground.Outcome = string(lifecycle.OutcomeFailed)
		},
		"missing stop reason": func(snapshot *stateSnapshot) {
			snapshot.Wrapper.Foreground.StopReason = ""
		},
		"unsupported stop reason": func(snapshot *stateSnapshot) {
			snapshot.Wrapper.Foreground.StopReason = "future"
		},
	} {
		t.Run(name, func(t *testing.T) {
			snapshot := valid()
			mutate(&snapshot)
			require.Error(t, validateStateSnapshotRequiredSections(snapshot))
		})
	}

	require.NoError(t, validateStateSnapshotRequiredSections(valid()))
}
