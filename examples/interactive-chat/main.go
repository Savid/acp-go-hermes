package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/coder/acp-go-sdk"
	hermesacp "github.com/savid/acp-go-hermes"
	"golang.org/x/term"
)

type agentConnection interface {
	Initialize(context.Context, acp.InitializeRequest) (acp.InitializeResponse, error)
	NewSession(context.Context, acp.NewSessionRequest) (acp.NewSessionResponse, error)
	Prompt(context.Context, acp.PromptRequest) (acp.PromptResponse, error)
	Cancel(context.Context, acp.CancelNotification) error
	CloseSession(context.Context, acp.CloseSessionRequest) (acp.CloseSessionResponse, error)
}

type startedAgent struct {
	conn  agentConnection
	close func()
	wait  func() error
}

type inputEventKind int

const (
	inputPrompt inputEventKind = iota
	inputInterrupt
	inputExit
	inputClosed
	inputError
)

type inputEvent struct {
	kind inputEventKind
	text string
	err  error
}

type promptResult struct {
	err error
}

type inputControl struct {
	inputDone bool
	exit      bool
	err       error
}

type turnTicker struct {
	ticker *time.Ticker
	ch     <-chan time.Time
}

type toolDisplay struct {
	title  string
	kind   acp.ToolKind
	status acp.ToolCallStatus
}

const (
	ansiAlternateScreen = "\x1b[?1049h"
	ansiMainScreen      = "\x1b[?1049l"
	ansiHideCursor      = "\x1b[?25l"
	ansiShowCursor      = "\x1b[?25h"
	ansiClearScreenHome = "\x1b[H\x1b[2J"
	ansiClearLine       = "\r\x1b[2K"
)

var spinnerFrames = []string{"thinking", "thinking.", "thinking..", "thinking..."}

type chatClient struct {
	ui *chatUI
}

var _ acp.Client = (*chatClient)(nil)

var startAgent = startAgentProcess
var getwd = os.Getwd
var exit = os.Exit
var commandContext = exec.CommandContext
var isTerminal = term.IsTerminal
var getTerminalSize = term.GetSize
var makeTerminalRaw = term.MakeRaw
var restoreTerminal = term.Restore
var startInput = startInputReader

const agentPackage = "github.com/savid/acp-go-hermes/cmd/acp-go-hermes"

func (*chatClient) ReadTextFile(_ context.Context, params acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
	if !filepath.IsAbs(params.Path) {
		return acp.ReadTextFileResponse{}, fmt.Errorf("path must be absolute: %s", params.Path)
	}

	data, err := os.ReadFile(params.Path)
	if err != nil {
		return acp.ReadTextFileResponse{}, err
	}

	return acp.ReadTextFileResponse{Content: string(data)}, nil
}

func (*chatClient) WriteTextFile(_ context.Context, params acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
	if !filepath.IsAbs(params.Path) {
		return acp.WriteTextFileResponse{}, fmt.Errorf("path must be absolute: %s", params.Path)
	}

	if err := os.MkdirAll(filepath.Dir(params.Path), 0o755); err != nil {
		return acp.WriteTextFileResponse{}, err
	}

	return acp.WriteTextFileResponse{}, os.WriteFile(params.Path, []byte(params.Content), 0o600)
}

func (c *chatClient) RequestPermission(
	_ context.Context,
	params acp.RequestPermissionRequest,
) (acp.RequestPermissionResponse, error) {
	c.ui.writeNotice("permission", permissionTitle(params))

	for _, option := range params.Options {
		if option.Kind == acp.PermissionOptionKindAllowOnce || option.Kind == acp.PermissionOptionKindAllowAlways {
			return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected(option.OptionId)}, nil
		}
	}

	return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeCancelled()}, nil
}

func (c *chatClient) SessionUpdate(_ context.Context, params acp.SessionNotification) error {
	update := params.Update

	switch {
	case update.UserMessageChunk != nil && update.UserMessageChunk.Content.Text != nil:
		c.ui.writeUserChunk(update.UserMessageChunk.Content.Text.Text)
	case update.AgentMessageChunk != nil && update.AgentMessageChunk.Content.Text != nil:
		chunk := update.AgentMessageChunk
		c.ui.writeAgentText(chunk.MessageId, chunk.Content.Text.Text)
	case update.AgentThoughtChunk != nil && update.AgentThoughtChunk.Content.Text != nil:
		c.ui.writeNotice("thinking", update.AgentThoughtChunk.Content.Text.Text)
	case update.ToolCall != nil:
		c.ui.writeToolCall(update.ToolCall)
	case update.ToolCallUpdate != nil:
		c.ui.writeToolCallUpdate(update.ToolCallUpdate)
	case update.UsageUpdate != nil:
		c.ui.writeUsage(update.UsageUpdate)
	}

	return nil
}

func (*chatClient) CreateTerminal(context.Context, acp.CreateTerminalRequest) (acp.CreateTerminalResponse, error) {
	return acp.CreateTerminalResponse{TerminalId: "terminal-1"}, nil
}

func (*chatClient) KillTerminal(context.Context, acp.KillTerminalRequest) (acp.KillTerminalResponse, error) {
	return acp.KillTerminalResponse{}, nil
}

func (*chatClient) TerminalOutput(context.Context, acp.TerminalOutputRequest) (acp.TerminalOutputResponse, error) {
	return acp.TerminalOutputResponse{Output: "", Truncated: false}, nil
}

func (*chatClient) ReleaseTerminal(context.Context, acp.ReleaseTerminalRequest) (acp.ReleaseTerminalResponse, error) {
	return acp.ReleaseTerminalResponse{}, nil
}

func (*chatClient) WaitForTerminalExit(context.Context, acp.WaitForTerminalExitRequest) (acp.WaitForTerminalExitResponse, error) {
	return acp.WaitForTerminalExitResponse{}, nil
}

type chatUI struct {
	output io.Writer
	fd     int

	mu            sync.Mutex
	header        []string
	history       []string
	currentBlocks []currentBlock
	queued        []string
	messages      map[string]*messageDisplay
	tools         map[acp.ToolCallId]*toolDisplay
	fallback      messageDisplay
	agentOpen     bool
	turnStart     time.Time
	spinner       int
	usage         *acp.SessionUsageUpdate

	rawInput      bool
	promptVisible bool
	inputText     string
	width         int
	height        int
}

type messageDisplay struct {
	text       string
	blockIndex int
	hasBlock   bool
}

type currentBlock struct {
	lines []string
}

func newChatUI(output io.Writer) *chatUI {
	if output == nil {
		output = os.Stdout
	}

	fd := -1
	if file, ok := output.(*os.File); ok && isTerminal(int(file.Fd())) {
		fd = int(file.Fd())
	}

	return &chatUI{output: output, fd: fd}
}

func (ui *chatUI) writeHeader(cwd string) {
	ui.mu.Lock()
	defer ui.mu.Unlock()

	ui.header = []string{
		"ACP interactive chat example",
		fmt.Sprintf("cwd: %s", cwd),
		"Enter submits, Esc interrupts the current turn, Ctrl-C exits",
		"type /exit or /quit to leave",
	}

	if ui.rawInput {
		ui.redrawScreenLocked()

		return
	}

	for _, line := range ui.header {
		ui.writeStringLocked(line + "\n")
	}
}

func (ui *chatUI) writePrompt() {
	ui.mu.Lock()
	defer ui.mu.Unlock()

	if ui.rawInput {
		ui.promptVisible = true
		ui.redrawScreenLocked()

		return
	}

	ui.writeStringLocked("\nmessage> ")
}

func (ui *chatUI) echoInputRune(value rune) {
	ui.mu.Lock()
	defer ui.mu.Unlock()

	ui.inputText += string(value)
	if ui.rawInput {
		ui.promptVisible = true
		ui.redrawScreenLocked()

		return
	}

	ui.writeStringLocked(string(value))
}

func (ui *chatUI) echoInputBackspace() bool {
	ui.mu.Lock()
	defer ui.mu.Unlock()

	if ui.inputText == "" {
		return false
	}

	runes := []rune(ui.inputText)
	ui.inputText = string(runes[:len(runes)-1])

	if ui.rawInput {
		ui.promptVisible = true
		ui.redrawScreenLocked()

		return true
	}

	fmt.Fprint(ui.output, "\b \b")

	return true
}

func (ui *chatUI) echoInputSubmit() bool {
	ui.mu.Lock()
	defer ui.mu.Unlock()

	wasPromptVisible := ui.promptVisible
	ui.inputText = ""

	if ui.rawInput {
		ui.promptVisible = false
		ui.redrawScreenLocked()

		return wasPromptVisible
	}

	ui.writeStringLocked("\n")

	return wasPromptVisible
}

func (ui *chatUI) writeUserPrompt(prompt string) {
	ui.writePromptRecord("you", prompt, false)
}

func (ui *chatUI) writeQueuedPrompt(prompt string) {
	ui.writePromptRecord("queued", prompt, true)
}

func (ui *chatUI) writePromptRecord(label string, prompt string, _ bool) {
	ui.mu.Lock()
	defer ui.mu.Unlock()

	if ui.rawInput {
		if label == "queued" {
			ui.queued = append(ui.queued, prompt)
		} else {
			ui.appendHistoryBlockLocked(label, prompt)
		}

		ui.promptVisible = true
		ui.redrawScreenLocked()

		return
	}

	clearedPrompt := ui.clearPromptLocked()
	if ui.agentOpen && !clearedPrompt {
		ui.writeStringLocked("\n")
	}

	ui.writeStringLocked(fmt.Sprintf("%s> %s\n", label, prompt))

	if ui.agentOpen {
		ui.writeStringLocked("hermes> ")
	}
}

func (ui *chatUI) beginAgentTurn(prompt string) {
	ui.mu.Lock()
	defer ui.mu.Unlock()

	if ui.rawInput {
		if ui.removeQueuedPromptLocked(prompt) || !ui.historyEndsWithLocked("you", prompt) {
			ui.appendHistoryBlockLocked("you", prompt)
		}

		ui.messages = nil
		ui.tools = nil
		ui.fallback = messageDisplay{}
		ui.currentBlocks = nil
		ui.agentOpen = true
		ui.turnStart = time.Now()
		ui.spinner = 0
		ui.usage = nil
		ui.promptVisible = true
		ui.redrawScreenLocked()

		return
	}

	ui.clearPromptLocked()
	ui.messages = nil
	ui.tools = nil
	ui.fallback = messageDisplay{}
	ui.currentBlocks = nil
	ui.agentOpen = true
	ui.writeStringLocked("hermes> ")
}

func (ui *chatUI) endAgentTurn(stopReason acp.StopReason) {
	ui.mu.Lock()
	defer ui.mu.Unlock()

	if ui.rawInput {
		ui.appendCurrentTurnLocked()
		ui.appendHistoryBlockLocked("stop", string(stopReason))
		ui.agentOpen = false
		ui.turnStart = time.Time{}
		ui.promptVisible = true
		ui.redrawScreenLocked()

		return
	}

	redrawPrompt := ui.promptVisible
	clearedPrompt := ui.clearPromptLocked()

	if ui.agentOpen && !clearedPrompt {
		ui.writeStringLocked("\n")
	}

	ui.agentOpen = false
	ui.writeStringLocked(fmt.Sprintf("stop> %s\n", stopReason))

	if redrawPrompt {
		ui.promptVisible = true
		ui.redrawPromptLocked()
	}
}

func (ui *chatUI) writeUserChunk(text string) {
	ui.mu.Lock()
	defer ui.mu.Unlock()

	if ui.rawInput {
		ui.appendHistoryBlockLocked("you", text)
		ui.redrawScreenLocked()

		return
	}

	ui.clearPromptLocked()
	ui.writeStringLocked(fmt.Sprintf("\nyou> %s\n", text))
}

func (ui *chatUI) writeAgentText(messageID *string, text string) {
	ui.mu.Lock()
	defer ui.mu.Unlock()

	if ui.rawInput {
		if !ui.agentOpen {
			ui.agentOpen = true
			ui.turnStart = time.Now()
		}

		display := ui.messageDisplay(messageID)
		if display.update(text) {
			ui.upsertMessageBlockLocked(display)
		}

		ui.promptVisible = true
		ui.redrawScreenLocked()

		return
	}

	redrawPrompt := ui.promptVisible
	ui.clearPromptLocked()

	if !ui.agentOpen {
		ui.agentOpen = true
		ui.writeStringLocked("hermes> ")
	}

	ui.messageDisplay(messageID).writeWith(ui.writeStringLocked, text)

	if redrawPrompt {
		ui.promptVisible = true
		ui.redrawPromptLocked()
	}
}

func (ui *chatUI) writeNotice(label string, text string) {
	text = strings.TrimSpace(text)
	if text == "" {
		return
	}

	ui.writeNoticeLines(label, []string{text})
}

func (ui *chatUI) writeToolCall(call *acp.SessionUpdateToolCall) {
	if call == nil {
		return
	}

	ui.mu.Lock()
	defer ui.mu.Unlock()

	display := ui.toolDisplay(call.ToolCallId)
	display.title = strings.TrimSpace(call.Title)
	display.kind = call.Kind
	display.status = call.Status

	if ui.rawInput {
		ui.appendCurrentLinesLocked(prefixedLines("tool", toolCallLines(call, display)))
		ui.promptVisible = true
		ui.redrawScreenLocked()

		return
	}

	ui.writeBlockLocked("tool", toolCallLines(call, display))
}

func (ui *chatUI) writeToolCallUpdate(update *acp.SessionToolCallUpdate) {
	if update == nil {
		return
	}

	ui.mu.Lock()
	defer ui.mu.Unlock()

	display := ui.toolDisplay(update.ToolCallId)
	if update.Title != nil && strings.TrimSpace(*update.Title) != "" {
		display.title = strings.TrimSpace(*update.Title)
	}

	if update.Kind != nil {
		display.kind = *update.Kind
	}

	if update.Status != nil {
		display.status = *update.Status
	}

	if ui.rawInput {
		ui.appendCurrentLinesLocked(prefixedLines("tool", toolUpdateLines(update, display)))
		ui.promptVisible = true
		ui.redrawScreenLocked()

		return
	}

	ui.writeBlockLocked("tool", toolUpdateLines(update, display))
}

func (ui *chatUI) writeUsage(update *acp.SessionUsageUpdate) {
	if update == nil {
		return
	}

	ui.mu.Lock()
	defer ui.mu.Unlock()

	usage := *update
	ui.usage = &usage

	if ui.rawInput {
		ui.redrawScreenLocked()

		return
	}

	if summary := usageSummary(&usage); summary != "" {
		ui.writeBlockLocked("usage", []string{summary})
	}
}

func (ui *chatUI) tick() {
	ui.mu.Lock()
	defer ui.mu.Unlock()

	if !ui.rawInput || !ui.agentOpen {
		return
	}

	ui.spinner++
	ui.redrawScreenLocked()
}

func (ui *chatUI) writeNoticeLines(label string, lines []string) {
	ui.mu.Lock()
	defer ui.mu.Unlock()

	if ui.rawInput {
		ui.appendCurrentLinesLocked(prefixedLines(label, lines))
		ui.promptVisible = true
		ui.redrawScreenLocked()

		return
	}

	ui.writeBlockLocked(label, lines)
}

func (ui *chatUI) writeBlockLocked(label string, lines []string) {
	lines = cleanNoticeLines(lines)
	if len(lines) == 0 {
		return
	}

	redrawPrompt := ui.promptVisible
	clearedPrompt := ui.clearPromptLocked()

	if ui.agentOpen && !clearedPrompt {
		ui.writeStringLocked("\n")
	}

	ui.writeStringLocked(fmt.Sprintf("%s> %s\n", label, lines[0]))

	for _, line := range lines[1:] {
		ui.writeStringLocked(fmt.Sprintf("  %s\n", line))
	}

	if ui.agentOpen {
		if redrawPrompt {
			ui.promptVisible = true
			ui.redrawPromptLocked()
		} else {
			ui.writeStringLocked("hermes> ")
		}
	} else if redrawPrompt {
		ui.promptVisible = true
		ui.redrawPromptLocked()
	}
}

func cleanNoticeLines(lines []string) []string {
	cleaned := make([]string, 0, len(lines))
	for _, line := range lines {
		for part := range strings.SplitSeq(strings.TrimRight(line, "\r\n"), "\n") {
			if strings.TrimSpace(part) == "" {
				continue
			}

			cleaned = append(cleaned, part)
		}
	}

	return cleaned
}

func (ui *chatUI) setRawInput(active bool) {
	ui.mu.Lock()
	defer ui.mu.Unlock()

	if active == ui.rawInput {
		if active {
			ui.refreshTerminalSizeLocked()
		}

		return
	}

	ui.rawInput = active

	if active {
		ui.refreshTerminalSizeLocked()
		ui.writeANSILocked(ansiAlternateScreen + ansiHideCursor + ansiClearScreenHome)
		ui.redrawScreenLocked()

		return
	}

	ui.promptVisible = false
	ui.width = 0
	ui.height = 0
	ui.writeANSILocked(ansiShowCursor + ansiMainScreen)
}

func (ui *chatUI) refreshTerminalSizeLocked() {
	if ui.fd < 0 {
		return
	}

	width, height, err := getTerminalSize(ui.fd)
	if err != nil {
		return
	}

	if width > 0 {
		ui.width = width
	}

	if height > 0 {
		ui.height = height
	}
}

func (ui *chatUI) writeANSILocked(sequence string) {
	fmt.Fprint(ui.output, sequence)
}

func (ui *chatUI) clearPromptLocked() bool {
	if !ui.rawInput || !ui.promptVisible {
		return false
	}

	ui.writeANSILocked(ansiClearLine)
	ui.promptVisible = false

	return true
}

func (ui *chatUI) redrawPromptLocked() {
	if !ui.rawInput {
		return
	}

	ui.writeANSILocked(ansiClearLine)
	ui.writeStringLocked("message> " + ui.inputText)
}

func (ui *chatUI) redrawScreenLocked() {
	if !ui.rawInput {
		return
	}

	ui.refreshTerminalSizeLocked()
	ui.writeANSILocked(ansiClearScreenHome)

	lines := ui.screenLinesLocked()
	for i, line := range lines {
		if i == len(lines)-1 {
			ui.writeStringLocked(line)

			continue
		}

		ui.writeStringLocked(line + "\n")
	}
}

func (ui *chatUI) screenLinesLocked() []string {
	lines := append([]string(nil), ui.header...)
	if len(lines) > 0 {
		lines = append(lines, "")
	}

	lines = append(lines, ui.history...)

	current := ui.currentOutputLinesLocked()
	if len(current) > 0 {
		lines = appendSection(lines, current...)
	}

	if len(ui.queued) > 0 {
		queued := make([]string, 0, len(ui.queued))
		for _, prompt := range ui.queued {
			queued = append(queued, "queued> "+prompt)
		}

		lines = appendSection(lines, queued...)
	}

	if ui.agentOpen {
		lines = appendSection(lines, ui.statusLineLocked())
	}

	lines = appendSection(lines, "message> "+ui.inputText)

	return ui.fitScreenLinesLocked(lines)
}

func (ui *chatUI) fitScreenLinesLocked(lines []string) []string {
	wrapped := make([]string, 0, len(lines))
	for _, line := range lines {
		wrapped = append(wrapped, wrapLine(line, ui.width)...)
	}

	if ui.height > 0 && len(wrapped) > ui.height {
		wrapped = wrapped[len(wrapped)-ui.height:]
	}

	return wrapped
}

func wrapLine(line string, width int) []string {
	if width <= 0 {
		return []string{line}
	}

	runes := []rune(line)
	if len(runes) == 0 {
		return []string{""}
	}

	lines := make([]string, 0, len(runes)/width+1)
	for len(runes) > width {
		lines = append(lines, string(runes[:width]))
		runes = runes[width:]
	}

	lines = append(lines, string(runes))

	return lines
}

func (ui *chatUI) currentOutputLinesLocked() []string {
	var lines []string
	for _, block := range ui.currentBlocks {
		lines = append(lines, block.lines...)
	}

	if len(lines) == 0 && ui.agentOpen {
		lines = append(lines, "hermes>")
	}

	return lines
}

func (ui *chatUI) statusLineLocked() string {
	frame := spinnerFrames[ui.spinner%len(spinnerFrames)]

	parts := []string{frame, formatElapsed(time.Since(ui.turnStart))}
	if summary := usageSummary(ui.usage); summary != "" {
		parts = append(parts, summary)
	}

	return "status> " + strings.Join(parts, " | ")
}

func (ui *chatUI) writeStringLocked(text string) {
	if ui.rawInput {
		text = normalizeTerminalNewlines(text)
	}

	fmt.Fprint(ui.output, text)
}

func normalizeTerminalNewlines(text string) string {
	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")

	return strings.ReplaceAll(text, "\n", "\r\n")
}

func (ui *chatUI) messageDisplay(messageID *string) *messageDisplay {
	if messageID == nil || *messageID == "" {
		return &ui.fallback
	}

	if ui.messages == nil {
		ui.messages = make(map[string]*messageDisplay)
	}

	display := ui.messages[*messageID]
	if display == nil {
		display = &messageDisplay{}
		ui.messages[*messageID] = display
	}

	return display
}

func (ui *chatUI) toolDisplay(toolCallID acp.ToolCallId) *toolDisplay {
	if ui.tools == nil {
		ui.tools = make(map[acp.ToolCallId]*toolDisplay)
	}

	display := ui.tools[toolCallID]
	if display == nil {
		display = &toolDisplay{}
		ui.tools[toolCallID] = display
	}

	return display
}

func (ui *chatUI) appendHistoryBlockLocked(label string, text string) {
	ui.history = append(ui.history, prefixedLines(label, []string{text})...)
}

func (ui *chatUI) appendCurrentLinesLocked(lines []string) {
	lines = cleanNoticeLines(lines)
	if len(lines) == 0 {
		return
	}

	ui.currentBlocks = append(ui.currentBlocks, currentBlock{lines: lines})
}

func (ui *chatUI) upsertMessageBlockLocked(display *messageDisplay) {
	lines := prefixedLines("hermes", []string{display.text})
	if len(lines) == 0 {
		return
	}

	if !display.hasBlock || display.blockIndex < 0 || display.blockIndex >= len(ui.currentBlocks) {
		display.blockIndex = len(ui.currentBlocks)
		display.hasBlock = true

		ui.currentBlocks = append(ui.currentBlocks, currentBlock{lines: lines})

		return
	}

	ui.currentBlocks[display.blockIndex].lines = lines
}

func (ui *chatUI) appendCurrentTurnLocked() {
	ui.history = appendSection(ui.history, ui.currentOutputLinesLocked()...)
	ui.currentBlocks = nil
	ui.messages = nil
	ui.tools = nil
	ui.fallback = messageDisplay{}
}

func (ui *chatUI) removeQueuedPromptLocked(prompt string) bool {
	for i, queued := range ui.queued {
		if queued != prompt {
			continue
		}

		ui.queued = append(ui.queued[:i], ui.queued[i+1:]...)

		return true
	}

	return false
}

func (ui *chatUI) historyEndsWithLocked(label string, text string) bool {
	lines := prefixedLines(label, []string{text})
	if len(lines) == 0 || len(ui.history) < len(lines) {
		return false
	}

	start := len(ui.history) - len(lines)
	for i, line := range lines {
		if ui.history[start+i] != line {
			return false
		}
	}

	return true
}

func appendSection(lines []string, section ...string) []string {
	section = cleanNoticeLines(section)
	if len(section) == 0 {
		return lines
	}

	if len(lines) > 0 && lines[len(lines)-1] != "" {
		lines = append(lines, "")
	}

	return append(lines, section...)
}

func prefixedLines(label string, lines []string) []string {
	lines = cleanNoticeLines(lines)
	if len(lines) == 0 {
		return nil
	}

	result := make([]string, 0, len(lines))
	result = append(result, label+"> "+lines[0])

	for _, line := range lines[1:] {
		result = append(result, "  "+line)
	}

	return result
}

func usageSummary(update *acp.SessionUsageUpdate) string {
	if update == nil {
		return ""
	}

	var parts []string
	if update.Size > 0 || update.Used > 0 {
		parts = append(parts, fmt.Sprintf("tokens %d/%d", update.Used, update.Size))
	}

	if update.Cost != nil {
		parts = append(parts, fmt.Sprintf("cost %.4f %s", update.Cost.Amount, update.Cost.Currency))
	}

	return strings.Join(parts, ", ")
}

func formatElapsed(elapsed time.Duration) string {
	if elapsed < 0 {
		elapsed = 0
	}

	return elapsed.Truncate(time.Second).String()
}

func toolCallLines(call *acp.SessionUpdateToolCall, display *toolDisplay) []string {
	lines := []string{toolSummary(call.ToolCallId, display, false)}
	lines = append(lines, toolLocationsLines(call.Locations)...)
	lines = append(lines, toolContentLines("content", call.Content)...)
	lines = append(lines, toolValueLines("input", call.RawInput)...)

	if len(call.Content) == 0 {
		lines = append(lines, toolValueLines("output", call.RawOutput)...)
	}

	return lines
}

func toolUpdateLines(update *acp.SessionToolCallUpdate, display *toolDisplay) []string {
	updated := update.Title == nil && update.Kind == nil && update.Status == nil
	lines := []string{toolSummary(update.ToolCallId, display, updated)}
	lines = append(lines, toolLocationsLines(update.Locations)...)
	lines = append(lines, toolContentLines("result", update.Content)...)

	if len(update.Content) == 0 {
		lines = append(lines, toolValueLines("output", update.RawOutput)...)
	}

	lines = append(lines, toolValueLines("input", update.RawInput)...)

	return lines
}

func toolSummary(toolCallID acp.ToolCallId, display *toolDisplay, updated bool) string {
	title := strings.TrimSpace(display.title)
	if title == "" {
		title = "tool"
	}

	var attrs []string
	if display.status != "" {
		attrs = append(attrs, string(display.status))
	}

	if display.kind != "" {
		attrs = append(attrs, string(display.kind))
	}

	summary := title
	if updated {
		summary += " updated"
	}

	if len(attrs) > 0 {
		summary += " [" + strings.Join(attrs, ", ") + "]"
	}

	if toolCallID != "" {
		summary += " (" + string(toolCallID) + ")"
	}

	return summary
}

func toolLocationsLines(locations []acp.ToolCallLocation) []string {
	if len(locations) == 0 {
		return nil
	}

	values := make([]string, 0, len(locations))
	for _, location := range locations {
		value := location.Path
		if location.Line != nil {
			value = fmt.Sprintf("%s:%d", value, *location.Line)
		}

		if value != "" {
			values = append(values, value)
		}
	}

	if len(values) == 0 {
		return nil
	}

	label := "location"
	if len(values) > 1 {
		label = "locations"
	}

	return []string{label + ": " + strings.Join(values, ", ")}
}

func toolContentLines(label string, content []acp.ToolCallContent) []string {
	lines := make([]string, 0, len(content))

	for _, item := range content {
		switch {
		case item.Content != nil:
			lines = append(lines, contentBlockLines(label, item.Content.Content)...)
		case item.Diff != nil:
			lines = append(lines, diffContentLines(item.Diff)...)
		case item.Terminal != nil:
			lines = append(lines, "terminal: "+item.Terminal.TerminalId)
		}
	}

	return lines
}

func contentBlockLines(label string, block acp.ContentBlock) []string {
	switch {
	case block.Text != nil:
		return previewLines(label, block.Text.Text, 1600)
	case block.Image != nil:
		value := block.Image.MimeType
		if block.Image.Uri != nil && *block.Image.Uri != "" {
			value += " " + *block.Image.Uri
		}

		if block.Image.Data != "" {
			value += fmt.Sprintf(" (%d base64 chars)", len(block.Image.Data))
		}

		return []string{"image: " + strings.TrimSpace(value)}
	case block.Audio != nil:
		value := block.Audio.MimeType
		if block.Audio.Data != "" {
			value += fmt.Sprintf(" (%d base64 chars)", len(block.Audio.Data))
		}

		return []string{"audio: " + strings.TrimSpace(value)}
	case block.ResourceLink != nil:
		return []string{"resource: " + block.ResourceLink.Name + " " + block.ResourceLink.Uri}
	case block.Resource != nil:
		return embeddedResourceLines(label, block.Resource.Resource)
	default:
		return nil
	}
}

func embeddedResourceLines(label string, resource acp.EmbeddedResourceResource) []string {
	switch {
	case resource.TextResourceContents != nil:
		lines := make([]string, 0, 2)
		lines = append(lines, "resource: "+resource.TextResourceContents.Uri)
		lines = append(lines, previewLines(label, resource.TextResourceContents.Text, 1600)...)

		return lines
	case resource.BlobResourceContents != nil:
		value := resource.BlobResourceContents.Uri
		if resource.BlobResourceContents.MimeType != nil && *resource.BlobResourceContents.MimeType != "" {
			value += " " + *resource.BlobResourceContents.MimeType
		}

		if resource.BlobResourceContents.Blob != "" {
			value += fmt.Sprintf(" (%d base64 chars)", len(resource.BlobResourceContents.Blob))
		}

		return []string{"resource: " + strings.TrimSpace(value)}
	default:
		return nil
	}
}

func diffContentLines(diff *acp.ToolCallContentDiff) []string {
	if diff == nil {
		return nil
	}

	parts := []string{diff.Path}
	if diff.OldText != nil {
		parts = append(parts, fmt.Sprintf("old %d chars", len(*diff.OldText)))
	}

	parts = append(parts, fmt.Sprintf("new %d chars", len(diff.NewText)))

	return []string{"diff: " + strings.Join(parts, ", ")}
}

func toolValueLines(label string, value any) []string {
	if value == nil {
		return nil
	}

	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return previewLines(label, fmt.Sprint(value), 1200)
	}

	return previewLines(label, string(data), 1200)
}

func previewLines(label string, text string, limit int) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}

	text, truncated := truncateString(text, limit)
	if !strings.Contains(text, "\n") && len(text) <= 160 {
		if truncated {
			text += " ..."
		}

		return []string{label + ": " + text}
	}

	lines := []string{label + ":"}
	for line := range strings.SplitSeq(text, "\n") {
		lines = append(lines, "  "+line)
	}

	if truncated {
		lines = append(lines, "  ...")
	}

	return lines
}

func truncateString(text string, limit int) (string, bool) {
	if limit <= 0 {
		return "", text != ""
	}

	runes := []rune(text)
	if len(runes) <= limit {
		return text, false
	}

	return string(runes[:limit]), true
}

func (m *messageDisplay) write(output io.Writer, text string) {
	m.writeWith(func(delta string) {
		fmt.Fprint(output, delta)
	}, text)
}

func (m *messageDisplay) writeWith(write func(string), text string) {
	delta, ok := m.updateDelta(text)
	if !ok {
		return
	}

	write(delta)
}

func (m *messageDisplay) update(text string) bool {
	_, ok := m.updateDelta(text)

	return ok
}

func (m *messageDisplay) updateDelta(text string) (string, bool) {
	switch {
	case text == "":
		return "", false
	case m.text == text:
		return "", false
	case strings.HasPrefix(text, m.text):
		delta := text[len(m.text):]
		m.text = text

		return delta, true
	default:
		m.text += text

		return text, true
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	exit(run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout io.Writer, stderr io.Writer) int {
	cwd, err := getwd()
	if err != nil {
		printError(stderr, err)

		return 1
	}

	ui := newChatUI(stdout)

	agent, err := startAgent(ctx, ui, stderr)
	if err != nil {
		printError(stderr, err)

		return 1
	}

	defer func() {
		if agent.close != nil {
			agent.close()
		}

		if agent.wait != nil {
			_ = agent.wait()
		}
	}()

	initialPrompt := strings.TrimSpace(strings.Join(args, " "))
	if err := runChat(ctx, agent.conn, ui, stdin, cwd, initialPrompt); err != nil {
		if errors.Is(err, context.Canceled) {
			return 0
		}

		printError(stderr, err)

		return 1
	}

	return 0
}

func startAgentProcess(ctx context.Context, ui *chatUI, stderr io.Writer) (*startedAgent, error) {
	cmd := commandContext(ctx, "go", "run", agentPackage)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}

	agentStdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	cmd.Stderr = stderr

	err = cmd.Start()
	if err != nil {
		return nil, err
	}

	agentOutputGate := newConnectionInputGate(agentStdout)
	conn := acp.NewClientSideConnection(&chatClient{ui: ui}, stdin, agentOutputGate)
	conn.SetLogger(slog.New(slog.DiscardHandler))
	agentOutputGate.open()

	return &startedAgent{
		conn: conn,
		close: func() {
			_ = stdin.Close()
		},
		wait: cmd.Wait,
	}, nil
}

type connectionInputGate struct {
	reader io.Reader
	ready  chan struct{}
	once   sync.Once
}

// connectionInputGate blocks the SDK receive goroutine until the connection
// logger is installed. The SDK starts receiving inside NewClientSideConnection.
func newConnectionInputGate(reader io.Reader) *connectionInputGate {
	return &connectionInputGate{
		reader: reader,
		ready:  make(chan struct{}),
	}
}

func (g *connectionInputGate) open() {
	g.once.Do(func() {
		close(g.ready)
	})
}

func (g *connectionInputGate) Read(p []byte) (int, error) {
	<-g.ready

	return g.reader.Read(p)
}

func runChat(
	ctx context.Context,
	conn agentConnection,
	ui *chatUI,
	stdin io.Reader,
	cwd string,
	initialPrompt string,
) error {
	_, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	if err != nil {
		return err
	}

	session, err := conn.NewSession(ctx, hermesacp.NewSessionRequest(cwd))
	if err != nil {
		return err
	}
	defer func() {
		_, _ = conn.CloseSession(context.Background(), acp.CloseSessionRequest{SessionId: session.SessionId})
	}()

	ui.writeHeader(cwd)

	return runInteractiveLoop(ctx, conn, ui, stdin, session.SessionId, initialPrompt)
}

func runInteractiveLoop(
	ctx context.Context,
	conn agentConnection,
	ui *chatUI,
	stdin io.Reader,
	sessionID acp.SessionId,
	initialPrompt string,
) error {
	events, restoreInput := startInput(ctx, stdin, ui)
	defer restoreInput()

	queue := make([]string, 0, 1)
	inputDone := false
	running := false
	activeTurnNonce := ""

	var done <-chan promptResult

	var ticks turnTicker
	defer ticks.stop()

	enqueue := func(prompt string, queued bool) {
		prompt = strings.TrimSpace(prompt)
		if prompt == "" {
			return
		}

		if queued {
			ui.writeQueuedPrompt(prompt)
		} else {
			ui.writeUserPrompt(prompt)
		}

		queue = append(queue, prompt)
	}

	if initialPrompt != "" {
		enqueue(initialPrompt, false)
	}

	if len(queue) == 0 {
		ui.writePrompt()
	}

	for {
		if !running && len(queue) > 0 {
			prompt := queue[0]
			queue = queue[1:]
			turnNonce := newTurnNonce()
			activeTurnNonce = turnNonce
			running = true

			result := make(chan promptResult, 1)
			done = result

			ticks.start()

			go func(nonce string) {
				result <- promptResult{err: runPrompt(ctx, conn, ui, sessionID, prompt, nonce)}
			}(turnNonce)

			continue
		}

		if !running && inputDone {
			return nil
		}

		select {
		case event, ok := <-events:
			if !ok {
				inputDone = true
				events = nil

				continue
			}

			control := handleInputEvent(ctx, conn, ui, sessionID, event, running, enqueue, activeTurnNonce)
			if control.err != nil {
				return control.err
			}

			if control.inputDone {
				inputDone = true
				events = nil
			}

			if control.exit {
				return nil
			}
		case result := <-done:
			running = false
			activeTurnNonce = ""
			done = nil

			ticks.stop()

			if result.err != nil {
				if errors.Is(result.err, context.Canceled) {
					return nil
				}

				return result.err
			}

			if len(queue) == 0 && !inputDone {
				ui.writePrompt()
			}
		case <-ticks.ch:
			ui.tick()
		case <-ctx.Done():
			if running {
				_ = conn.Cancel(context.Background(), hermesacp.CancelRequest(sessionID, activeTurnNonce))
			}

			return nil
		}
	}
}

func handleInputEvent(
	ctx context.Context,
	conn agentConnection,
	ui *chatUI,
	sessionID acp.SessionId,
	event inputEvent,
	running bool,
	enqueue func(string, bool),
	turnNonces ...string,
) inputControl {
	turnNonce := ""
	if len(turnNonces) > 0 {
		turnNonce = turnNonces[0]
	}

	switch event.kind {
	case inputPrompt:
		prompt := strings.TrimSpace(event.text)
		if quitCommand(prompt) {
			if running {
				_ = conn.Cancel(context.Background(), hermesacp.CancelRequest(sessionID, turnNonce))
			}

			return inputControl{exit: true}
		}

		enqueue(prompt, running)
	case inputInterrupt:
		if !running {
			ui.writeNotice("interrupt", "nothing running")

			return inputControl{}
		}

		if err := conn.Cancel(ctx, hermesacp.CancelRequest(sessionID, turnNonce)); err != nil {
			return inputControl{err: err}
		}

		ui.writeNotice("interrupt", "requested")
	case inputExit:
		if running {
			_ = conn.Cancel(context.Background(), hermesacp.CancelRequest(sessionID, turnNonce))
		}

		return inputControl{exit: true}
	case inputClosed:
		return inputControl{inputDone: true}
	case inputError:
		return inputControl{err: event.err}
	}

	return inputControl{}
}

func (t *turnTicker) start() {
	if t.ticker != nil {
		return
	}

	t.ticker = time.NewTicker(250 * time.Millisecond)
	t.ch = t.ticker.C
}

func (t *turnTicker) stop() {
	if t.ticker == nil {
		return
	}

	t.ticker.Stop()
	t.ticker = nil
	t.ch = nil
}

func startInputReader(ctx context.Context, stdin io.Reader, ui *chatUI) (<-chan inputEvent, func()) {
	events := make(chan inputEvent, 16)
	restore := func() {}

	if file, ok := stdin.(*os.File); ok && ui.fd >= 0 && isTerminal(int(file.Fd())) {
		state, err := makeTerminalRaw(int(file.Fd()))
		if err == nil {
			ui.setRawInput(true)

			restore = func() {
				ui.setRawInput(false)

				_ = restoreTerminal(int(file.Fd()), state)
			}

			go readRawInput(ctx, file, events, ui)

			return events, restore
		}
	}

	go readLineInput(ctx, stdin, events)

	return events, restore
}

func readLineInput(ctx context.Context, input io.Reader, events chan<- inputEvent) {
	defer close(events)

	reader := bufio.NewReader(input)
	for {
		line, err := readLine(ctx, reader)

		text := strings.TrimSpace(line)
		if text != "" {
			events <- lineInputEvent(text)
		}

		if err == nil {
			continue
		}

		if errors.Is(err, context.Canceled) {
			events <- inputEvent{kind: inputExit}

			return
		}

		if errors.Is(err, io.EOF) {
			events <- inputEvent{kind: inputClosed}

			return
		}

		events <- inputEvent{kind: inputError, err: err}

		return
	}
}

func readRawInput(ctx context.Context, input io.Reader, events chan<- inputEvent, ui *chatUI) {
	defer close(events)

	reader := bufio.NewReader(input)

	var buffer []rune

	for {
		select {
		case <-ctx.Done():
			events <- inputEvent{kind: inputExit}

			return
		default:
		}

		value, _, err := reader.ReadRune()
		if err != nil {
			if errors.Is(err, io.EOF) {
				events <- inputEvent{kind: inputClosed}
			} else {
				events <- inputEvent{kind: inputError, err: err}
			}

			return
		}

		switch value {
		case 0x03:
			ui.echoInputSubmit()

			events <- inputEvent{kind: inputExit}

			return
		case 0x1b:
			events <- inputEvent{kind: inputInterrupt}
		case '\r', '\n':
			text := strings.TrimSpace(string(buffer))
			buffer = buffer[:0]

			wasPromptVisible := ui.echoInputSubmit()
			if text != "" {
				events <- inputEvent{kind: inputPrompt, text: text}
			} else if wasPromptVisible {
				ui.writePrompt()
			}
		case '\b', 0x7f:
			if len(buffer) > 0 {
				buffer = buffer[:len(buffer)-1]

				ui.echoInputBackspace()
			}
		default:
			if value >= ' ' || value == '\t' {
				buffer = append(buffer, value)
				ui.echoInputRune(value)
			}
		}
	}
}

func lineInputEvent(text string) inputEvent {
	if text == string(rune(0x1b)) {
		return inputEvent{kind: inputInterrupt}
	}

	if text == string(rune(0x03)) {
		return inputEvent{kind: inputExit}
	}

	return inputEvent{kind: inputPrompt, text: text}
}

func readLine(ctx context.Context, reader *bufio.Reader) (string, error) {
	result := make(chan inputEvent, 1)

	go func() {
		line, err := reader.ReadString('\n')
		result <- inputEvent{text: line, err: err}
	}()

	select {
	case line := <-result:
		return line.text, line.err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func runPrompt(
	ctx context.Context,
	conn agentConnection,
	ui *chatUI,
	sessionID acp.SessionId,
	prompt string,
	turnNonces ...string,
) error {
	ui.beginAgentTurn(prompt)

	turnNonce := ""
	if len(turnNonces) > 0 {
		turnNonce = turnNonces[0]
	} else {
		turnNonce = newTurnNonce()
	}

	resp, err := conn.Prompt(ctx, hermesacp.TextPromptRequest(sessionID, turnNonce, prompt))
	if err != nil {
		if errors.Is(err, context.Canceled) {
			ui.endAgentTurn(acp.StopReasonCancelled)
		}

		return err
	}

	ui.endAgentTurn(resp.StopReason)

	return nil
}

func newTurnNonce() string {
	return rand.Text()
}

func quitCommand(prompt string) bool {
	switch strings.ToLower(strings.TrimSpace(prompt)) {
	case "/exit", "/quit", "exit", "quit":
		return true
	default:
		return false
	}
}

func permissionTitle(params acp.RequestPermissionRequest) string {
	if params.ToolCall.Title != nil && strings.TrimSpace(*params.ToolCall.Title) != "" {
		return *params.ToolCall.Title
	}

	return "auto-allowing request"
}

func printError(stderr io.Writer, err error) {
	_, _ = fmt.Fprintf(stderr, "interactive-chat: %v\n", err)
}
