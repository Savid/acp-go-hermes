package hermesacp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

// SessionStoreTerminalState is the durable terminal assistant identity from one
// committed hermes-state-db-v1 main snapshot.
type SessionStoreTerminalState struct {
	MessageID string
}

// InspectSessionStoreTerminalState validates exactly one current-format main
// snapshot for logicalSessionID and returns its latest finished assistant
// identity. A valid session that has not completed an assistant turn returns the
// zero state. Missing terminal summaries and snapshots from any other format are
// rejected.
func InspectSessionStoreTerminalState(
	logicalSessionID string,
	entries []SessionStoreEntry,
) (SessionStoreTerminalState, error) {
	if strings.TrimSpace(logicalSessionID) == "" || strings.TrimSpace(logicalSessionID) != logicalSessionID {
		return SessionStoreTerminalState{}, errors.New("logical session id is required")
	}

	if len(entries) != 1 {
		return SessionStoreTerminalState{}, fmt.Errorf(
			"hermes session-store snapshot requires exactly one entry, got %d", len(entries),
		)
	}

	var snapshot stateSnapshot

	decoder := json.NewDecoder(bytes.NewReader(entries[0]))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&snapshot); err != nil {
		return SessionStoreTerminalState{}, fmt.Errorf("decode Hermes session-store snapshot: %w", err)
	}

	if err := requireJSONEOF(decoder); err != nil {
		return SessionStoreTerminalState{}, fmt.Errorf("decode Hermes session-store snapshot: %w", err)
	}

	if snapshot.Format != SessionStoreFormat || snapshot.CapturedAtUnixMilli <= 0 {
		return SessionStoreTerminalState{}, errors.New("hermes session-store snapshot has an unsupported format or capture time")
	}

	if snapshot.Session.SessionID != logicalSessionID || strings.TrimSpace(snapshot.Session.NativeSessionID) == "" ||
		strings.TrimSpace(snapshot.Session.NativeSessionID) != snapshot.Session.NativeSessionID {
		return SessionStoreTerminalState{}, errors.New("hermes session-store snapshot has mismatched session identity")
	}

	if err := validateStateSnapshotRequiredSections(snapshot); err != nil {
		return SessionStoreTerminalState{}, fmt.Errorf("validate Hermes session-store main snapshot: %w", err)
	}

	return publicTerminalState(snapshot.Terminal), nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("snapshot has trailing JSON")
		}

		return err
	}

	return nil
}

func terminalSnapshotFromMessages(
	nativeSessionID string,
	messages []nativehermes.NativeMessage,
) (*stateSnapshotTerminal, error) {
	if strings.TrimSpace(nativeSessionID) == "" || strings.TrimSpace(nativeSessionID) != nativeSessionID {
		return nil, errors.New("hermes terminal history requires a native session id")
	}

	terminal := &stateSnapshotTerminal{}

	for index := range messages {
		message := &messages[index]

		expectedID := historyMessageID(index)
		if message.Info.ID != expectedID {
			return nil, fmt.Errorf(
				"hermes terminal history message %d has identity %q, want %q",
				index+1,
				message.Info.ID,
				expectedID,
			)
		}

		if message.Info.SessionID != nativeSessionID {
			return nil, fmt.Errorf("hermes terminal history message %q has mismatched native session id", message.Info.ID)
		}

		if message.Info.Role != valAssistant || strings.TrimSpace(message.Info.Finish) == "" {
			continue
		}

		terminal.MessageID = message.Info.ID
		terminal.Role = message.Info.Role
		terminal.Finish = message.Info.Finish
	}

	if err := validateStateSnapshotTerminal(terminal); err != nil {
		return nil, fmt.Errorf("validate Hermes terminal history: %w", err)
	}

	return terminal, nil
}

func historyMessageID(index int) string {
	return "history-" + strconv.Itoa(index+1)
}

func validateStateSnapshotTerminal(terminal *stateSnapshotTerminal) error {
	if terminal == nil {
		return errors.New("terminal summary is required")
	}

	if terminal.MessageID == "" && terminal.Role == "" && terminal.Finish == "" {
		return nil
	}

	if terminal.Role != valAssistant || strings.TrimSpace(terminal.Finish) == "" ||
		strings.TrimSpace(terminal.Finish) != terminal.Finish || !validHistoryMessageID(terminal.MessageID) {
		return errors.New("terminal summary is not a finished assistant history identity")
	}

	return nil
}

func validateStateSnapshotRequiredSections(snapshot stateSnapshot) error {
	if snapshot.Archives == nil {
		return errors.New("archives section is required")
	}

	if snapshot.Wrapper == nil {
		return errors.New("wrapper section is required")
	}

	return validateStateSnapshotTerminal(snapshot.Terminal)
}

func validHistoryMessageID(messageID string) bool {
	_, valid := historyMessagePosition(messageID)

	return valid
}

func historyMessagePosition(messageID string) (uint64, bool) {
	value, found := strings.CutPrefix(messageID, "history-")
	if !found || value == "" {
		return 0, false
	}

	position, err := strconv.ParseUint(value, 10, 64)
	if err != nil || position == 0 {
		return 0, false
	}

	return position, "history-"+strconv.FormatUint(position, 10) == messageID
}

func validateTerminalTransition(
	previous SessionStoreTerminalState,
	next SessionStoreTerminalState,
	requireAdvance bool,
) error {
	previousPosition := uint64(0)

	if previous.MessageID != "" {
		var valid bool

		previousPosition, valid = historyMessagePosition(previous.MessageID)
		if !valid {
			return fmt.Errorf("committed Hermes terminal identity %q is invalid", previous.MessageID)
		}
	}

	nextPosition := uint64(0)

	if next.MessageID != "" {
		var valid bool

		nextPosition, valid = historyMessagePosition(next.MessageID)
		if !valid {
			return fmt.Errorf("next Hermes terminal identity %q is invalid", next.MessageID)
		}
	}

	if nextPosition < previousPosition {
		return fmt.Errorf("hermes terminal identity regressed from %q to %q", previous.MessageID, next.MessageID)
	}

	if requireAdvance && nextPosition <= previousPosition {
		return fmt.Errorf("completed Hermes turn did not advance terminal identity beyond %q", previous.MessageID)
	}

	return nil
}

func publicTerminalState(terminal *stateSnapshotTerminal) SessionStoreTerminalState {
	if terminal == nil {
		return SessionStoreTerminalState{}
	}

	return SessionStoreTerminalState{MessageID: terminal.MessageID}
}

func (s *session) committedTerminalState() SessionStoreTerminalState {
	s.mu.Lock()
	defer s.mu.Unlock()

	terminal := s.committedTerminal

	return terminal
}

func terminalResponseMeta(terminal SessionStoreTerminalState) map[string]any {
	return map[string]any{
		hermesMetaKey: map[string]any{keyMessageID: terminal.MessageID},
	}
}
