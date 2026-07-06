package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
	"golang.org/x/term"
)

type fakeAgentConnection struct {
	mu                  sync.Mutex
	cwd                 string
	prompts             []string
	cancelled           []acp.SessionId
	closed              bool
	sessionID           acp.SessionId
	initErr             error
	newErr              error
	promptErr           error
	cancelErr           error
	promptWait          <-chan struct{}
	ignorePromptContext bool
}

func (f *fakeAgentConnection) Initialize(context.Context, acp.InitializeRequest) (acp.InitializeResponse, error) {
	return acp.InitializeResponse{}, f.initErr
}

func (f *fakeAgentConnection) NewSession(_ context.Context, params acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.cwd = params.Cwd
	if f.sessionID == "" {
		f.sessionID = "session-1"
	}

	return acp.NewSessionResponse{SessionId: f.sessionID}, f.newErr
}

func (f *fakeAgentConnection) Prompt(ctx context.Context, params acp.PromptRequest) (acp.PromptResponse, error) {
	f.mu.Lock()
	if len(params.Prompt) > 0 && params.Prompt[0].Text != nil {
		f.prompts = append(f.prompts, params.Prompt[0].Text.Text)
	}
	wait := f.promptWait
	promptErr := f.promptErr
	f.mu.Unlock()

	if wait != nil {
		if f.ignorePromptContext {
			<-wait
		} else {
			select {
			case <-wait:
			case <-ctx.Done():
				return acp.PromptResponse{}, ctx.Err()
			}
		}
	}

	return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, promptErr
}

func (f *fakeAgentConnection) Cancel(_ context.Context, params acp.CancelNotification) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.cancelled = append(f.cancelled, params.SessionId)

	return f.cancelErr
}

func (f *fakeAgentConnection) CloseSession(context.Context, acp.CloseSessionRequest) (acp.CloseSessionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.closed = true

	return acp.CloseSessionResponse{}, nil
}

func (f *fakeAgentConnection) promptsSnapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.prompts...)
}

func (f *fakeAgentConnection) cancelledSnapshot() []acp.SessionId {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]acp.SessionId(nil), f.cancelled...)
}

func (f *fakeAgentConnection) closedSnapshot() bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.closed
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) {
	return 0, errors.New("read failed")
}

type signalWriter struct {
	needle string
	seen   chan struct{}
	once   sync.Once
}

func newSignalWriter(needle string) *signalWriter {
	return &signalWriter{
		needle: needle,
		seen:   make(chan struct{}),
	}
}

func (w *signalWriter) Write(p []byte) (int, error) {
	if strings.Contains(string(p), w.needle) {
		w.once.Do(func() { close(w.seen) })
	}

	return len(p), nil
}

func TestChatClientFilePermissionAndTerminalMethods(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	c := chatClient{ui: newChatUI(&output)}
	path := filepath.Join(t.TempDir(), "note.txt")

	_, err := c.WriteTextFile(context.Background(), acp.WriteTextFileRequest{
		Path:    path,
		Content: "hello",
	})
	require.NoError(t, err)

	read, err := c.ReadTextFile(context.Background(), acp.ReadTextFileRequest{Path: path})
	require.NoError(t, err)
	require.Equal(t, "hello", read.Content)

	_, err = c.WriteTextFile(context.Background(), acp.WriteTextFileRequest{Path: "relative"})
	require.Error(t, err)

	_, err = c.ReadTextFile(context.Background(), acp.ReadTextFileRequest{Path: "relative"})
	require.Error(t, err)

	_, err = c.ReadTextFile(context.Background(), acp.ReadTextFileRequest{Path: filepath.Join(t.TempDir(), "missing.txt")})
	require.Error(t, err)

	parentFile := filepath.Join(t.TempDir(), "parent")
	require.NoError(t, os.WriteFile(parentFile, []byte("file"), 0o600))
	_, err = c.WriteTextFile(context.Background(), acp.WriteTextFileRequest{Path: filepath.Join(parentFile, "child.txt")})
	require.Error(t, err)

	title := "Run tool"
	resp, err := c.RequestPermission(context.Background(), acp.RequestPermissionRequest{
		ToolCall: acp.ToolCallUpdate{Title: &title},
		Options: []acp.PermissionOption{
			{OptionId: "reject", Kind: acp.PermissionOptionKindRejectOnce},
			{OptionId: "allow", Kind: acp.PermissionOptionKindAllowAlways},
		},
	})
	require.NoError(t, err)
	require.Equal(t, acp.PermissionOptionId("allow"), resp.Outcome.Selected.OptionId)

	resp, err = c.RequestPermission(context.Background(), acp.RequestPermissionRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp.Outcome.Cancelled)
	require.Contains(t, output.String(), "permission> Run tool")
	require.Contains(t, output.String(), "permission> auto-allowing request")

	terminal, err := c.CreateTerminal(context.Background(), acp.CreateTerminalRequest{})
	require.NoError(t, err)
	require.Equal(t, "terminal-1", terminal.TerminalId)

	terminalOutput, err := c.TerminalOutput(context.Background(), acp.TerminalOutputRequest{})
	require.NoError(t, err)
	require.False(t, terminalOutput.Truncated)

	_, err = c.KillTerminal(context.Background(), acp.KillTerminalRequest{})
	require.NoError(t, err)
	_, err = c.ReleaseTerminal(context.Background(), acp.ReleaseTerminalRequest{})
	require.NoError(t, err)
	_, err = c.WaitForTerminalExit(context.Background(), acp.WaitForTerminalExitRequest{})
	require.NoError(t, err)
}

func TestChatUIAndSessionUpdates(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	ui := newChatUI(&output)
	require.Same(t, &output, ui.output)
	require.Equal(t, os.Stdout, newChatUI(nil).output)

	c := chatClient{ui: ui}
	status := acp.ToolCallStatusCompleted
	line := 12
	messageID := "33333333-3333-4333-8333-333333333333"
	emptyID := ""

	ui.writeHeader("/repo")
	ui.writePrompt()
	ui.writeUserPrompt("hello")
	ui.writeNotice("empty", " ")
	require.Same(t, &ui.fallback, ui.messageDisplay(nil))
	require.Same(t, &ui.fallback, ui.messageDisplay(&emptyID))

	require.NoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
		Update: acp.SessionUpdate{UserMessageChunk: &acp.SessionUpdateUserMessageChunk{Content: acp.TextBlock("user text")}},
	}))
	require.NoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
		Update: acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{Content: acp.TextBlock("agent text")}},
	}))
	require.NoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
		Update: acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
			MessageId: &messageID,
			Content:   acp.TextBlock("Hello"),
		}},
	}))
	require.NoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
		Update: acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
			MessageId: &messageID,
			Content:   acp.TextBlock("Hello world"),
		}},
	}))
	require.NoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
		Update: acp.SessionUpdate{AgentThoughtChunk: &acp.SessionUpdateAgentThoughtChunk{Content: acp.TextBlock("thinking")}},
	}))
	require.NoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
		Update: acp.SessionUpdate{ToolCall: &acp.SessionUpdateToolCall{
			ToolCallId: "tool-1",
			Title:      "Read file",
			Kind:       acp.ToolKindRead,
			Status:     acp.ToolCallStatusPending,
			Locations:  []acp.ToolCallLocation{{Path: "/repo/main.go", Line: &line}},
			RawInput:   map[string]any{"path": "/repo/main.go"},
			Content:    []acp.ToolCallContent{acp.ToolContent(acp.TextBlock("reading /repo/main.go"))},
		}},
	}))
	require.NoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
		Update: acp.SessionUpdate{ToolCallUpdate: &acp.SessionToolCallUpdate{ToolCallId: "tool-1"}},
	}))
	require.NoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
		Update: acp.SessionUpdate{ToolCallUpdate: &acp.SessionToolCallUpdate{
			ToolCallId: "tool-1",
			Status:     &status,
			Content:    []acp.ToolCallContent{acp.ToolContent(acp.TextBlock("done"))},
			RawOutput:  map[string]any{"bytes": 5},
		}},
	}))
	require.NoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{}))

	ui.endAgentTurn(acp.StopReasonEndTurn)
	ui.endAgentTurn(acp.StopReasonCancelled)

	var direct bytes.Buffer
	display := messageDisplay{}
	display.write(&direct, "")
	display.write(&direct, "Hi")
	display.write(&direct, "Hi")
	display.write(&direct, "Hi there")
	display.write(&direct, " reset")
	require.Equal(t, "Hi there reset", direct.String())

	text := output.String()
	require.Contains(t, text, "ACP interactive chat example")
	require.Contains(t, text, "you> user text")
	require.Contains(t, text, "hermes> agent text")
	require.Contains(t, text, "thinking> thinking")
	require.Contains(t, text, "tool> Read file [pending, read] (tool-1)")
	require.Contains(t, text, "location: /repo/main.go:12")
	require.Contains(t, text, "content: reading /repo/main.go")
	require.Contains(t, text, `"path": "/repo/main.go"`)
	require.Contains(t, text, "tool> Read file updated [pending, read] (tool-1)")
	require.Contains(t, text, "tool> Read file [completed, read] (tool-1)")
	require.Contains(t, text, "result: done")
	require.Contains(t, text, "stop> end_turn")
	require.Contains(t, text, "stop> cancelled")
}

func TestRawInputEchoAndLineLayout(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	ui := newChatUI(&output)
	ui.setRawInput(true)
	ui.writePrompt()
	ui.echoInputRune('h')
	ui.echoInputRune('i')
	require.True(t, ui.echoInputBackspace())
	ui.echoInputRune('!')
	require.True(t, ui.echoInputSubmit())
	ui.writeUserPrompt("h!")
	ui.beginAgentTurn("h!")
	ui.writeAgentText(nil, "hello\nthere")
	ui.writeNotice("interrupt", "requested")
	ui.endAgentTurn(acp.StopReasonCancelled)

	text := output.String()
	require.Contains(t, text, ansiAlternateScreen)
	require.Contains(t, text, ansiHideCursor)
	require.Contains(t, text, "message> h!")
	require.Contains(t, text, "\r\n")
	require.Contains(t, text, "you> h!")
	require.Contains(t, text, "hermes> hello\r\n  there")
	require.Contains(t, text, "interrupt> requested")
	require.Contains(t, text, "status> thinking")
	require.NotContains(t, text, "message> hermes>")
	require.Contains(t, text, "stop> cancelled")
}

func TestRawInputHeaderAndRestore(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	ui := newChatUI(&output)
	ui.setRawInput(true)
	ui.writeHeader("/repo")
	ui.writePrompt()
	ui.setRawInput(false)

	text := output.String()
	require.Contains(t, text, ansiAlternateScreen)
	require.Contains(t, text, ansiHideCursor)
	require.Contains(t, text, ansiShowCursor)
	require.Contains(t, text, ansiMainScreen)

	frame := lastRenderedFrame(text)
	requireLineOrder(t, frame,
		"ACP interactive chat example",
		"cwd: /repo",
		"Enter submits, Esc interrupts the current turn, Ctrl-C exits",
		"type /exit or /quit to leave",
		"message> ",
	)
}

func TestRawInputQueuedPromptOrdering(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	ui := newChatUI(&output)
	ui.setRawInput(true)
	ui.writeUserPrompt("first")
	ui.beginAgentTurn("first")
	ui.writeAgentText(nil, "working")
	ui.writeQueuedPrompt("second")

	frame := lastRenderedFrame(output.String())
	requireLineOrder(t, frame,
		"you> first",
		"hermes> working",
		"queued> second",
		"status> thinking",
		"message> ",
	)
}

func TestRawInputKeepsToolAndStreamingMessageOrder(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	ui := newChatUI(&output)
	ui.setRawInput(true)
	ui.writeUserPrompt("search")
	ui.beginAgentTurn("search")
	ui.writeToolCall(&acp.SessionUpdateToolCall{
		ToolCallId: "tool-1",
		Title:      "Web search",
		Status:     acp.ToolCallStatusPending,
	})
	ui.writeAgentText(nil, "answer")
	ui.writeAgentText(nil, "answer with more detail")

	frame := lastRenderedFrame(output.String())
	requireLineOrder(t, frame,
		"you> search",
		"tool> Web search [pending] (tool-1)",
		"hermes> answer with more detail",
		"status> thinking",
		"message> ",
	)
	require.NotContains(t, frame, "hermes> answer\r\nhermes> answer with more detail")
}

func TestReadRawInputEchoesVisiblePromptOnly(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	ui := newChatUI(&output)
	ui.setRawInput(true)
	ui.writePrompt()

	events := make(chan inputEvent, 4)
	readRawInput(context.Background(), strings.NewReader("ab\x7fcd\n\x1b\x03"), events, ui)

	var got []inputEvent
	for event := range events {
		got = append(got, event)
	}

	require.Len(t, got, 3)
	require.Equal(t, inputPrompt, got[0].kind)
	require.Equal(t, "acd", got[0].text)
	require.Equal(t, inputInterrupt, got[1].kind)
	require.Equal(t, inputExit, got[2].kind)
	require.Contains(t, output.String(), "message> acd")
}

func TestRawInputScreenWrappingAndCropping(t *testing.T) {
	t.Parallel()

	require.Equal(t, []string{"abcd", "ef"}, wrapLine("abcdef", 4))
	require.Equal(t, []string{""}, wrapLine("", 4))
	require.Equal(t, []string{"abcdef"}, wrapLine("abcdef", 0))

	ui := newChatUI(io.Discard)
	ui.width = 4
	ui.height = 3

	lines := ui.fitScreenLinesLocked([]string{"one", "twothree", "", "four"})
	require.Equal(t, []string{"hree", "", "four"}, lines)
}

func TestTerminalInputPathAndSize(t *testing.T) {
	originalIsTerminal := isTerminal
	originalGetTerminalSize := getTerminalSize
	originalMakeTerminalRaw := makeTerminalRaw
	originalRestoreTerminal := restoreTerminal
	t.Cleanup(func() {
		isTerminal = originalIsTerminal
		getTerminalSize = originalGetTerminalSize
		makeTerminalRaw = originalMakeTerminalRaw
		restoreTerminal = originalRestoreTerminal
	})

	isTerminal = func(int) bool { return true }

	var sizeErr error
	var width int
	var height int
	getTerminalSize = func(int) (int, int, error) {
		return width, height, sizeErr
	}

	var rawFD int
	makeTerminalRaw = func(fd int) (*term.State, error) {
		rawFD = fd

		return &term.State{}, nil
	}

	var restored bool
	restoreTerminal = func(int, *term.State) error {
		restored = true

		return nil
	}

	outputFile, err := os.CreateTemp(t.TempDir(), "output")
	require.NoError(t, err)
	defer outputFile.Close()

	ui := newChatUI(outputFile)
	require.Equal(t, int(outputFile.Fd()), ui.fd)

	sizeErr = errors.New("size")
	ui.refreshTerminalSizeLocked()
	require.Zero(t, ui.width)
	require.Zero(t, ui.height)

	sizeErr = nil
	width = 80
	height = 24
	ui.setRawInput(true)
	require.Equal(t, 80, ui.width)
	require.Equal(t, 24, ui.height)

	width = 100
	height = 30
	ui.setRawInput(true)
	require.Equal(t, 100, ui.width)
	require.Equal(t, 30, ui.height)

	inputFile, err := os.CreateTemp(t.TempDir(), "input")
	require.NoError(t, err)
	defer inputFile.Close()

	events, restore := startInputReader(context.Background(), inputFile, ui)
	event := <-events
	require.Equal(t, inputClosed, event.kind)
	restore()
	require.Equal(t, int(inputFile.Fd()), rawFD)
	require.True(t, restored)
}

func TestTerminalInputRawSetupFailureFallsBackToLineInput(t *testing.T) {
	originalIsTerminal := isTerminal
	originalMakeTerminalRaw := makeTerminalRaw
	t.Cleanup(func() {
		isTerminal = originalIsTerminal
		makeTerminalRaw = originalMakeTerminalRaw
	})

	isTerminal = func(int) bool { return true }
	makeTerminalRaw = func(int) (*term.State, error) {
		return nil, errors.New("raw")
	}

	inputFile, err := os.CreateTemp(t.TempDir(), "input")
	require.NoError(t, err)
	defer inputFile.Close()

	_, err = inputFile.WriteString("hello\n")
	require.NoError(t, err)
	_, err = inputFile.Seek(0, io.SeekStart)
	require.NoError(t, err)

	ui := newChatUI(io.Discard)
	ui.fd = int(inputFile.Fd())
	events, restore := startInputReader(context.Background(), inputFile, ui)
	defer restore()

	first := <-events
	require.Equal(t, inputPrompt, first.kind)
	require.Equal(t, "hello", first.text)

	second := <-events
	require.Equal(t, inputClosed, second.kind)
}

func TestChatUIRenderingBranches(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	ui := newChatUI(&output)

	ui.redrawScreenLocked()
	ui.redrawPromptLocked()
	ui.echoInputRune('x')
	require.True(t, ui.echoInputBackspace())
	require.False(t, ui.echoInputBackspace())
	require.False(t, ui.echoInputSubmit())

	ui.agentOpen = true
	ui.writeUserPrompt("during turn")
	ui.promptVisible = true
	ui.writeAgentText(nil, "with redraw")
	ui.endAgentTurn(acp.StopReasonEndTurn)

	ui.agentOpen = true
	ui.promptVisible = true
	ui.writeBlockLocked("notice", []string{"redraw while open"})

	ui.agentOpen = true
	ui.promptVisible = false
	ui.writeBlockLocked("notice", []string{"first", "second"})

	ui.agentOpen = false
	ui.promptVisible = true
	ui.writeBlockLocked("notice", []string{"third"})
	ui.writeBlockLocked("empty", []string{" ", "\n"})

	var raw bytes.Buffer
	rawUI := newChatUI(&raw)
	rawUI.setRawInput(true)
	rawUI.writePrompt()
	require.True(t, rawUI.clearPromptLocked())
	require.False(t, rawUI.clearPromptLocked())
	rawUI.inputText = "again"
	rawUI.redrawPromptLocked()

	text := output.String()
	require.Contains(t, text, "x\b \b")
	require.Contains(t, text, "you> during turn")
	require.Contains(t, text, "hermes> with redraw")
	require.Contains(t, text, "hermes> ")
	require.Contains(t, text, "stop> end_turn")
	require.Contains(t, text, "notice> redraw while open")
	require.Contains(t, text, "notice> first")
	require.Contains(t, text, "  second")
	require.Contains(t, raw.String(), ansiClearLine)
	require.Contains(t, raw.String(), "message> again")
}

func TestRawUIUpdatesUsageToolsAndSpinner(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	ui := newChatUI(&output)
	c := chatClient{ui: ui}

	ui.writeUsage(nil)
	require.NoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
		Update: acp.SessionUpdate{UsageUpdate: &acp.SessionUsageUpdate{
			Size: 100,
			Used: 40,
			Cost: &acp.Cost{Amount: 0.25, Currency: "USD"},
		}},
	}))
	ui.tick()
	require.Contains(t, output.String(), "usage> tokens 40/100, cost 0.2500 USD")

	var raw bytes.Buffer
	rawUI := newChatUI(&raw)
	rawUI.setRawInput(true)
	rawClient := chatClient{ui: rawUI}
	rawUI.tick()
	rawUI.writeUserChunk("raw user")
	rawUI.writeAgentText(nil, "late agent")
	rawUI.writeToolCall(nil)
	rawUI.writeToolCallUpdate(nil)

	title := "Search"
	kind := acp.ToolKindRead
	status := acp.ToolCallStatusCompleted
	rawUI.writeToolCallUpdate(&acp.SessionToolCallUpdate{
		ToolCallId: "tool-2",
		Title:      &title,
		Kind:       &kind,
		Status:     &status,
		Content:    []acp.ToolCallContent{acp.ToolContent(acp.TextBlock("result"))},
	})
	require.NoError(t, rawClient.SessionUpdate(context.Background(), acp.SessionNotification{
		Update: acp.SessionUpdate{UsageUpdate: &acp.SessionUsageUpdate{Size: 200, Used: 50}},
	}))
	require.Contains(t, raw.String(), "hermes> late agent")
	require.Contains(t, raw.String(), "tool> Search [completed, read] (tool-2)")
	require.Contains(t, raw.String(), "result: result")
	rawUI.beginAgentTurn("spin")
	rawUI.writeUsage(&acp.SessionUsageUpdate{Size: 200, Used: 50})
	rawUI.tick()

	frame := lastRenderedFrame(raw.String())
	require.Contains(t, frame, "you> raw user")
	require.Contains(t, frame, "status> thinking.")
	require.Contains(t, frame, "tokens 50/200")
}

func TestFormattingHelpers(t *testing.T) {
	t.Parallel()

	require.Empty(t, cleanNoticeLines([]string{" ", "\n"}))
	require.Nil(t, appendSection(nil, " ", "\n"))
	require.Nil(t, prefixedLines("x", []string{" ", "\n"}))
	require.Equal(t, []string{"a", "", "b"}, appendSection([]string{"a", ""}, "b"))

	require.Empty(t, usageSummary(nil))
	require.Equal(t, "tokens 1/2", usageSummary(&acp.SessionUsageUpdate{Size: 2, Used: 1}))
	require.Equal(t, "cost 1.2500 USD", usageSummary(&acp.SessionUsageUpdate{
		Cost: &acp.Cost{Amount: 1.25, Currency: "USD"},
	}))
	require.Equal(t, "tokens 1/2, cost 1.2500 USD", usageSummary(&acp.SessionUsageUpdate{
		Size: 2,
		Used: 1,
		Cost: &acp.Cost{Amount: 1.25, Currency: "USD"},
	}))
	require.Equal(t, "0s", formatElapsed(-time.Second))

	require.Equal(t, "tool updated", toolSummary("", &toolDisplay{}, true))

	line := 4
	require.Nil(t, toolLocationsLines(nil))
	require.Nil(t, toolLocationsLines([]acp.ToolCallLocation{{}}))
	require.Equal(t, []string{"locations: /a, /b:4"}, toolLocationsLines([]acp.ToolCallLocation{
		{Path: "/a"},
		{Path: "/b", Line: &line},
	}))

	uri := "file://image.png"
	mimeType := "text/plain"
	require.Contains(t, contentBlockLines("content", acp.ContentBlock{
		Image: &acp.ContentBlockImage{MimeType: "image/png", Uri: &uri, Data: "abcd"},
	})[0], "image/png file://image.png (4 base64 chars)")
	require.Contains(t, contentBlockLines("content", acp.AudioBlock("abcd", "audio/wav"))[0], "audio/wav (4 base64 chars)")
	require.Equal(t, []string{"resource: guide file://guide"}, contentBlockLines("content", acp.ResourceLinkBlock("guide", "file://guide")))
	require.Equal(t, []string{"resource: file://text", "content: hello"}, contentBlockLines("content", acp.ResourceBlock(acp.EmbeddedResourceResource{
		TextResourceContents: &acp.TextResourceContents{Uri: "file://text", Text: "hello"},
	})))
	require.Contains(t, contentBlockLines("content", acp.ResourceBlock(acp.EmbeddedResourceResource{
		BlobResourceContents: &acp.BlobResourceContents{Uri: "file://blob", MimeType: &mimeType, Blob: "abcd"},
	}))[0], "file://blob text/plain (4 base64 chars)")
	require.Nil(t, contentBlockLines("content", acp.ContentBlock{}))
	require.Nil(t, embeddedResourceLines("content", acp.EmbeddedResourceResource{}))

	oldText := "old"
	contentLines := toolContentLines("result", []acp.ToolCallContent{
		acp.ToolDiffContent("/tmp/a", "new", oldText),
		acp.ToolTerminalRef("terminal-1"),
	})
	require.Contains(t, strings.Join(contentLines, "\n"), "diff: /tmp/a, old 3 chars, new 3 chars")
	require.Contains(t, strings.Join(contentLines, "\n"), "terminal: terminal-1")
	require.Nil(t, diffContentLines(nil))
	require.Equal(t, []string{"diff: /tmp/new, new 3 chars"}, diffContentLines(&acp.ToolCallContentDiff{
		Path:    "/tmp/new",
		NewText: "new",
	}))

	require.Contains(t, strings.Join(toolValueLines("input", map[string]any{"a": 1}), "\n"), `"a": 1`)
	require.Contains(t, toolValueLines("input", math.NaN())[0], "NaN")
	require.Nil(t, toolValueLines("input", nil))

	require.Nil(t, previewLines("content", " ", 10))
	require.Equal(t, []string{"content: abc ..."}, previewLines("content", "abcdef", 3))
	multiline := strings.Join(previewLines("content", "a\nb\nc", 4), "\n")
	require.Contains(t, multiline, "content:")
	require.Contains(t, multiline, "  a")
	require.Equal(t, "", func() string {
		value, truncated := truncateString("abc", 0)
		require.True(t, truncated)

		return value
	}())
	value, truncated := truncateString("abcdef", 3)
	require.True(t, truncated)
	require.Equal(t, "abc", value)
}

func TestStateHelpers(t *testing.T) {
	t.Parallel()

	ui := newChatUI(io.Discard)
	ui.appendCurrentLinesLocked([]string{" ", "\n"})
	require.Empty(t, ui.currentBlocks)

	ui.upsertMessageBlockLocked(&messageDisplay{text: " "})
	require.Empty(t, ui.currentBlocks)

	ui.queued = []string{"first", "target"}
	require.True(t, ui.removeQueuedPromptLocked("target"))
	require.Equal(t, []string{"first"}, ui.queued)
	require.False(t, ui.removeQueuedPromptLocked("missing"))

	require.False(t, ui.historyEndsWithLocked("you", ""))
	ui.history = []string{"you> hello"}
	require.False(t, ui.historyEndsWithLocked("you", "bye"))
	require.True(t, ui.historyEndsWithLocked("you", "hello"))
}

func TestHandleInputEventBranches(t *testing.T) {
	t.Parallel()

	conn := &fakeAgentConnection{}
	ui := newChatUI(io.Discard)
	var enqueued []string
	var queuedFlags []bool
	enqueue := func(prompt string, queued bool) {
		enqueued = append(enqueued, prompt)
		queuedFlags = append(queuedFlags, queued)
	}

	control := handleInputEvent(context.Background(), conn, ui, "session-1", inputEvent{kind: inputPrompt, text: " /quit "}, false, enqueue)
	require.True(t, control.exit)
	require.Empty(t, conn.cancelledSnapshot())

	control = handleInputEvent(context.Background(), conn, ui, "session-1", inputEvent{kind: inputPrompt, text: "quit"}, true, enqueue)
	require.True(t, control.exit)
	require.Equal(t, []acp.SessionId{"session-1"}, conn.cancelledSnapshot())

	control = handleInputEvent(context.Background(), conn, ui, "session-1", inputEvent{kind: inputPrompt, text: " work "}, true, enqueue)
	require.False(t, control.exit)
	require.Equal(t, []string{"work"}, enqueued)
	require.Equal(t, []bool{true}, queuedFlags)

	control = handleInputEvent(context.Background(), conn, ui, "session-1", inputEvent{kind: inputInterrupt}, false, enqueue)
	require.NoError(t, control.err)

	conn.cancelErr = errors.New("cancel")
	control = handleInputEvent(context.Background(), conn, ui, "session-1", inputEvent{kind: inputInterrupt}, true, enqueue)
	require.Error(t, control.err)

	conn.cancelErr = nil
	control = handleInputEvent(context.Background(), conn, ui, "session-1", inputEvent{kind: inputInterrupt}, true, enqueue)
	require.NoError(t, control.err)

	control = handleInputEvent(context.Background(), conn, ui, "session-1", inputEvent{kind: inputExit}, true, enqueue)
	require.True(t, control.exit)

	control = handleInputEvent(context.Background(), conn, ui, "session-1", inputEvent{kind: inputClosed}, false, enqueue)
	require.True(t, control.inputDone)

	err := errors.New("input")
	control = handleInputEvent(context.Background(), conn, ui, "session-1", inputEvent{kind: inputError, err: err}, false, enqueue)
	require.ErrorIs(t, control.err, err)

	control = handleInputEvent(context.Background(), conn, ui, "session-1", inputEvent{kind: inputEventKind(99)}, false, enqueue)
	require.Empty(t, control)
}

func TestTurnTickerBranches(t *testing.T) {
	t.Parallel()

	var ticker turnTicker
	ticker.start()
	first := ticker.ch
	ticker.start()
	require.Equal(t, first, ticker.ch)
	ticker.stop()
	ticker.stop()
}

func TestReadRawInputAdditionalBranches(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	events := make(chan inputEvent, 1)
	readRawInput(ctx, strings.NewReader("ignored"), events, newChatUI(io.Discard))
	event := <-events
	require.Equal(t, inputExit, event.kind)

	events = make(chan inputEvent, 1)
	readRawInput(context.Background(), errReader{}, events, newChatUI(io.Discard))
	event = <-events
	require.Equal(t, inputError, event.kind)
	require.Error(t, event.err)

	var output bytes.Buffer
	ui := newChatUI(&output)
	ui.setRawInput(true)
	ui.writePrompt()
	events = make(chan inputEvent, 4)
	readRawInput(context.Background(), strings.NewReader("\n\b\ta\r\x01"), events, ui)

	var got []inputEvent
	for event := range events {
		got = append(got, event)
	}

	require.Len(t, got, 2)
	require.Equal(t, inputPrompt, got[0].kind)
	require.Equal(t, "a", got[0].text)
	require.Equal(t, inputClosed, got[1].kind)
	require.Contains(t, output.String(), "message> ")
}

func TestReadLineInputContextCancel(t *testing.T) {
	t.Parallel()

	reader, writer := io.Pipe()
	t.Cleanup(func() {
		_ = reader.Close()
		_ = writer.Close()
	})

	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan inputEvent, 2)
	done := make(chan struct{})
	go func() {
		readLineInput(ctx, reader, events)
		close(done)
	}()

	cancel()
	event := <-events
	require.Equal(t, inputExit, event.kind)
	_ = reader.Close()
	_ = writer.Close()
	<-done
}

func TestLineInputEventControlC(t *testing.T) {
	t.Parallel()

	event := lineInputEvent(string(rune(0x03)))
	require.Equal(t, inputExit, event.kind)
}

func TestRunInteractiveLoopAdditionalBranches(t *testing.T) {
	originalStartInput := startInput
	t.Cleanup(func() {
		startInput = originalStartInput
	})

	var restored bool
	startInput = func(context.Context, io.Reader, *chatUI) (<-chan inputEvent, func()) {
		events := make(chan inputEvent)
		close(events)

		return events, func() { restored = true }
	}
	err := runInteractiveLoop(context.Background(), &fakeAgentConnection{}, newChatUI(io.Discard), strings.NewReader(""), "session-1", "")
	require.NoError(t, err)
	require.True(t, restored)

	startInput = originalStartInput
	err = runInteractiveLoop(context.Background(), &fakeAgentConnection{promptErr: errors.New("prompt")}, newChatUI(io.Discard), strings.NewReader(""), "session-1", "go")
	require.Error(t, err)

	err = runInteractiveLoop(context.Background(), &fakeAgentConnection{promptErr: context.Canceled}, newChatUI(io.Discard), strings.NewReader(""), "session-1", "go")
	require.NoError(t, err)

	err = runInteractiveLoop(context.Background(), &fakeAgentConnection{}, newChatUI(io.Discard), strings.NewReader(""), "session-1", "   ")
	require.NoError(t, err)

	err = runInteractiveLoop(context.Background(), &fakeAgentConnection{}, newChatUI(io.Discard), strings.NewReader("/quit\n"), "session-1", "")
	require.NoError(t, err)
}

func TestRunInteractiveLoopWritesPromptAfterPromptCompletes(t *testing.T) {
	t.Parallel()

	reader, writer := io.Pipe()
	t.Cleanup(func() {
		_ = reader.Close()
		_ = writer.Close()
	})

	output := newSignalWriter("\nmessage> ")
	done := make(chan error, 1)
	go func() {
		done <- runInteractiveLoop(context.Background(), &fakeAgentConnection{}, newChatUI(output), reader, "session-1", "go")
	}()

	select {
	case <-output.seen:
	case <-time.After(time.Second):
		t.Fatal("prompt was not written")
	}

	require.NoError(t, writer.Close())
	require.NoError(t, <-done)
}

func TestRunInteractiveLoopTicksAndCancelsRunningPrompt(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	conn := &fakeAgentConnection{promptWait: release}
	var output bytes.Buffer
	ui := newChatUI(&output)
	ui.setRawInput(true)

	done := make(chan error, 1)
	go func() {
		done <- runInteractiveLoop(context.Background(), conn, ui, strings.NewReader(""), "session-1", "wait")
	}()

	require.Eventually(t, func() bool {
		return len(conn.promptsSnapshot()) == 1
	}, time.Second, 10*time.Millisecond)
	time.Sleep(300 * time.Millisecond)
	close(release)
	require.NoError(t, <-done)
	require.Greater(t, ui.spinner, 0)
}

func TestRunInteractiveLoopContextCancelWhileRunning(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	conn := &fakeAgentConnection{promptWait: release, ignorePromptContext: true}
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- runInteractiveLoop(ctx, conn, newChatUI(io.Discard), strings.NewReader(""), "session-1", "wait")
	}()

	require.Eventually(t, func() bool {
		return len(conn.promptsSnapshot()) == 1
	}, time.Second, 10*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	require.Equal(t, []acp.SessionId{"session-1"}, conn.cancelledSnapshot())
	close(release)
}

func lastRenderedFrame(output string) string {
	index := strings.LastIndex(output, ansiClearScreenHome)
	if index < 0 {
		return output
	}

	return output[index:]
}

func requireLineOrder(t *testing.T, text string, values ...string) {
	t.Helper()

	previous := -1
	for _, value := range values {
		index := strings.Index(text, value)
		require.NotEqual(t, -1, index, "missing %q in %q", value, text)
		require.Greater(t, index, previous, "%q was not after previous value", value)
		previous = index
	}
}

func TestRunChat(t *testing.T) {
	t.Parallel()

	conn := &fakeAgentConnection{}
	ui := newChatUI(io.Discard)
	err := runChat(context.Background(), conn, ui, strings.NewReader("first\n"), "/repo", "initial")
	require.NoError(t, err)
	require.Equal(t, "/repo", conn.cwd)
	require.Equal(t, []string{"initial", "first"}, conn.promptsSnapshot())
	require.True(t, conn.closedSnapshot())

	conn = &fakeAgentConnection{}
	err = runChat(context.Background(), conn, newChatUI(io.Discard), strings.NewReader(""), "/repo", "")
	require.NoError(t, err)
	require.True(t, conn.closedSnapshot())
}

func TestRunChatQueuesInputWhilePromptRuns(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	conn := &fakeAgentConnection{promptWait: release}
	done := make(chan error, 1)

	go func() {
		done <- runChat(context.Background(), conn, newChatUI(io.Discard), strings.NewReader("first\nsecond\n"), "/repo", "")
	}()

	require.Eventually(t, func() bool {
		prompts := conn.promptsSnapshot()

		return len(prompts) == 1 && prompts[0] == "first"
	}, time.Second, 10*time.Millisecond)

	close(release)

	require.NoError(t, <-done)
	require.Equal(t, []string{"first", "second"}, conn.promptsSnapshot())
	require.True(t, conn.closedSnapshot())
}

func TestRunChatEscapeInterruptsCurrentPrompt(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	conn := &fakeAgentConnection{promptWait: release}
	done := make(chan error, 1)

	go func() {
		done <- runChat(context.Background(), conn, newChatUI(io.Discard), strings.NewReader("first\n\x1b\n"), "/repo", "")
	}()

	require.Eventually(t, func() bool {
		return len(conn.cancelledSnapshot()) == 1
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, []acp.SessionId{"session-1"}, conn.cancelledSnapshot())

	close(release)

	require.NoError(t, <-done)
	require.True(t, conn.closedSnapshot())
}

func TestRunChatContextCancelQuitsCleanly(t *testing.T) {
	t.Parallel()

	reader, writer := io.Pipe()
	t.Cleanup(func() {
		_ = reader.Close()
		_ = writer.Close()
	})

	ctx, cancel := context.WithCancel(context.Background())
	conn := &fakeAgentConnection{}
	done := make(chan error, 1)

	go func() {
		done <- runChat(ctx, conn, newChatUI(io.Discard), reader, "/repo", "")
	}()

	cancel()

	require.NoError(t, <-done)
	require.True(t, conn.closedSnapshot())
}

func TestRunChatErrors(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		conn   *fakeAgentConnection
		input  io.Reader
		prompt string
	}{
		{name: "initialize", conn: &fakeAgentConnection{initErr: errors.New("init")}, input: strings.NewReader(""), prompt: ""},
		{name: "new session", conn: &fakeAgentConnection{newErr: errors.New("new")}, input: strings.NewReader(""), prompt: ""},
		{name: "initial prompt", conn: &fakeAgentConnection{promptErr: errors.New("prompt")}, input: strings.NewReader(""), prompt: "initial"},
		{name: "reader", conn: &fakeAgentConnection{}, input: errReader{}, prompt: ""},
		{name: "loop prompt", conn: &fakeAgentConnection{promptErr: errors.New("prompt")}, input: strings.NewReader("again\n"), prompt: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := runChat(context.Background(), tc.conn, newChatUI(io.Discard), tc.input, "/repo", tc.prompt)
			require.Error(t, err)
		})
	}
}

func TestRunPrompt(t *testing.T) {
	t.Parallel()

	conn := &fakeAgentConnection{}
	var output bytes.Buffer
	ui := newChatUI(&output)
	require.NoError(t, runPrompt(context.Background(), conn, ui, "session-1", "hello"))
	require.Equal(t, []string{"hello"}, conn.promptsSnapshot())
	require.Contains(t, output.String(), "hermes> ")

	conn.promptErr = errors.New("prompt")
	require.Error(t, runPrompt(context.Background(), conn, ui, "session-1", "again"))

	conn.promptErr = context.Canceled
	require.Error(t, runPrompt(context.Background(), conn, ui, "session-1", "cancel"))
	require.Contains(t, output.String(), "stop> cancelled")
}

func TestRun(t *testing.T) {
	originalStartAgent := startAgent
	originalGetwd := getwd
	t.Cleanup(func() {
		startAgent = originalStartAgent
		getwd = originalGetwd
	})

	conn := &fakeAgentConnection{}
	var closed bool
	var waited bool
	startAgent = func(context.Context, *chatUI, io.Writer) (*startedAgent, error) {
		return &startedAgent{
			conn: conn,
			close: func() {
				closed = true
			},
			wait: func() error {
				waited = true

				return nil
			},
		}, nil
	}
	getwd = func() (string, error) { return "/repo", nil }

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run(context.Background(), []string{"hello"}, strings.NewReader(""), &stdout, &stderr)
	require.Equal(t, 0, code)
	require.Equal(t, []string{"hello"}, conn.promptsSnapshot())
	require.True(t, closed)
	require.True(t, waited)
	require.Empty(t, stderr.String())

	startAgent = func(context.Context, *chatUI, io.Writer) (*startedAgent, error) {
		return &startedAgent{conn: &fakeAgentConnection{}}, nil
	}

	code = run(context.Background(), nil, strings.NewReader(""), &stdout, &stderr)
	require.Equal(t, 0, code)
}

func TestMain(t *testing.T) {
	originalStartAgent := startAgent
	originalGetwd := getwd
	originalExit := exit
	originalArgs := os.Args
	t.Cleanup(func() {
		startAgent = originalStartAgent
		getwd = originalGetwd
		exit = originalExit
		os.Args = originalArgs
	})

	startAgent = func(context.Context, *chatUI, io.Writer) (*startedAgent, error) {
		return &startedAgent{
			conn:  &fakeAgentConnection{},
			close: func() {},
			wait:  func() error { return nil },
		}, nil
	}
	getwd = func() (string, error) { return "/repo", nil }

	var gotCode int
	exit = func(code int) {
		gotCode = code
	}
	os.Args = []string{"interactive-chat", "hello"}

	main()

	require.Equal(t, 0, gotCode)
}

func TestRunErrors(t *testing.T) {
	originalStartAgent := startAgent
	originalGetwd := getwd
	t.Cleanup(func() {
		startAgent = originalStartAgent
		getwd = originalGetwd
	})

	getwd = func() (string, error) { return "", errors.New("cwd") }

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	require.Equal(t, 1, run(context.Background(), []string{"hello"}, strings.NewReader(""), &stdout, &stderr))
	require.Contains(t, stderr.String(), "cwd")

	getwd = func() (string, error) { return "/repo", nil }
	startAgent = func(context.Context, *chatUI, io.Writer) (*startedAgent, error) {
		return nil, errors.New("start")
	}

	stderr.Reset()
	require.Equal(t, 1, run(context.Background(), []string{"hello"}, strings.NewReader(""), &stdout, &stderr))
	require.Contains(t, stderr.String(), "start")

	startAgent = func(context.Context, *chatUI, io.Writer) (*startedAgent, error) {
		return &startedAgent{conn: &fakeAgentConnection{initErr: errors.New("init")}}, nil
	}

	stderr.Reset()
	require.Equal(t, 1, run(context.Background(), []string{"hello"}, strings.NewReader(""), &stdout, &stderr))
	require.Contains(t, stderr.String(), "init")

	startAgent = func(context.Context, *chatUI, io.Writer) (*startedAgent, error) {
		return &startedAgent{conn: &fakeAgentConnection{initErr: context.Canceled}}, nil
	}

	stderr.Reset()
	require.Equal(t, 0, run(context.Background(), []string{"hello"}, strings.NewReader(""), &stdout, &stderr))
	require.Empty(t, stderr.String())
}

func TestStartAgentProcess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test uses a POSIX shell script")
	}

	binDir := t.TempDir()
	goPath := filepath.Join(binDir, "go")
	err := os.WriteFile(goPath, []byte("#!/bin/sh\nwhile IFS= read -r _; do :; done\n"), 0o755)
	require.NoError(t, err)

	t.Setenv("PATH", binDir)

	var output bytes.Buffer
	agent, err := startAgentProcess(context.Background(), newChatUI(&output), io.Discard)
	require.NoError(t, err)
	require.NotNil(t, agent.conn)

	agent.close()
	require.NoError(t, agent.wait())
}

func TestStartAgentProcessUsesModuleEntrypoint(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test uses POSIX shell")
	}

	originalCommandContext := commandContext
	t.Cleanup(func() {
		commandContext = originalCommandContext
	})

	var gotName string
	var gotArgs []string
	commandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		gotName = name
		gotArgs = append([]string(nil), args...)

		return exec.CommandContext(ctx, "sh", "-c", "while IFS= read -r _; do :; done")
	}

	agent, err := startAgentProcess(context.Background(), newChatUI(io.Discard), io.Discard)
	require.NoError(t, err)
	require.NotNil(t, agent.conn)

	agent.close()
	require.NoError(t, agent.wait())
	require.Equal(t, "go", gotName)
	require.Equal(t, []string{"run", agentPackage}, gotArgs)
}

func TestStartAgentProcessStartError(t *testing.T) {
	t.Setenv("PATH", t.TempDir())

	agent, err := startAgentProcess(context.Background(), newChatUI(io.Discard), io.Discard)
	require.Error(t, err)
	require.Nil(t, agent)
}

func TestStartAgentProcessPipeErrors(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test uses POSIX shell")
	}

	originalCommandContext := commandContext
	t.Cleanup(func() {
		commandContext = originalCommandContext
	})

	commandContext = func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, "sh", "-c", "cat")
		cmd.Stdin = strings.NewReader("")

		return cmd
	}
	agent, err := startAgentProcess(context.Background(), newChatUI(io.Discard), io.Discard)
	require.Error(t, err)
	require.Nil(t, agent)

	commandContext = func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, "sh", "-c", "cat")
		cmd.Stdout = io.Discard

		return cmd
	}
	agent, err = startAgentProcess(context.Background(), newChatUI(io.Discard), io.Discard)
	require.Error(t, err)
	require.Nil(t, agent)
}

func TestQuitCommandAndPermissionTitle(t *testing.T) {
	t.Parallel()

	require.True(t, quitCommand(" /QUIT "))
	require.False(t, quitCommand("keep going"))

	title := "  "
	require.Equal(t, "auto-allowing request", permissionTitle(acp.RequestPermissionRequest{
		ToolCall: acp.ToolCallUpdate{Title: &title},
	}))

	title = "Inspect"
	require.Equal(t, "Inspect", permissionTitle(acp.RequestPermissionRequest{
		ToolCall: acp.ToolCallUpdate{Title: &title},
	}))
}

func TestPrintError(t *testing.T) {
	t.Parallel()

	var stderr bytes.Buffer
	printError(&stderr, fmt.Errorf("bad"))
	require.Contains(t, stderr.String(), "interactive-chat: bad")
}
