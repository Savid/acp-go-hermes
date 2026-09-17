package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/sessionlog"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-hermes/internal/hermes"
)

// sessionRecord carries the accepted session configuration beside its native export.
type sessionRecord struct {
	SessionID             string            `json:"sessionId"`
	NativeSessionID       string            `json:"nativeSessionId"`
	Cwd                   string            `json:"cwd"`
	AdditionalDirectories []string          `json:"additionalDirectories,omitempty"`
	Env                   map[string]string `json:"env,omitempty"`
	ExtraPathDirs         []string          `json:"extraPathDirs,omitempty"`
	Model                 string            `json:"model,omitempty"`
	Effort                string            `json:"effort,omitempty"`
	UpdatedAtUnixMilli    int64             `json:"updatedAtUnixMilli"`
}

func (s *session) record() sessionRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	return sessionRecord{SessionID: string(s.id), NativeSessionID: s.nativeID, Cwd: s.cwd, AdditionalDirectories: slices.Clone(s.additionalDirectories), Env: maps.Clone(s.options.Env), ExtraPathDirs: slices.Clone(s.options.ExtraPathDirs), Model: s.model, Effort: s.effort, UpdatedAtUnixMilli: time.Now().UnixMilli()}
}

func (r sessionRecord) validate(id string) error {
	if r.NativeSessionID == "" || r.SessionID != id || !filepath.IsAbs(r.Cwd) || r.UpdatedAtUnixMilli <= 0 {
		return errors.New("invalid session record")
	}

	for _, dir := range r.AdditionalDirectories {
		if !filepath.IsAbs(dir) {
			return errors.New("invalid additional directory")
		}
	}

	_, err := parseSessionMeta(inheritCarrier(sessionMeta{}, r).Meta())
	if err != nil {
		return err
	}

	return nil
}

// commitMirror replaces only a complete native snapshot and its matching
// carrier. The runtime the caller dispatched on reads the snapshot, so a
// commit that cannot be attempted fails instead of reporting success.
func (s *session) commitMirror(ctx context.Context, rt *runtime) error {
	s.mirrorMu.Lock()
	defer s.mirrorMu.Unlock()

	rows, err := s.snapshotRows(ctx, rt)
	if err != nil {
		return err
	}

	ctx, finish := s.agent.observe.StartSessionStore(ctx, "replace")
	err = sessionlog.Commit(ctx, s.agent.store, string(s.id), rows, s.record())
	finish(err)

	return err
}

func (s *session) snapshotRows(ctx context.Context, rt *runtime) ([][]byte, error) {
	s.mu.Lock()
	id := s.nativeID
	s.mu.Unlock()

	if rt == nil || id == "" {
		return nil, errors.New("native session binding missing")
	}

	var active hermes.ActiveListResult
	if err := rt.client.Call(ctx, "session.active_list", map[string]any{}, &active); err != nil {
		return nil, err
	}

	found := false

	for _, native := range active.Sessions {
		if native.SessionID != rt.liveID {
			continue
		}

		found = true

		if native.SessionKey != id {
			s.poisonSession(ctx, "native_session_identity_drift")

			return nil, errors.New("native session identity changed")
		}
	}

	if !found {
		return nil, errors.New("native session binding missing")
	}

	snapshot, err := rt.endpoint.Export(ctx, id)
	if err != nil {
		return nil, err
	}

	if len(snapshot) == 0 {
		return nil, errors.New("native conversation missing")
	}

	return [][]byte{snapshot}, nil
}

type storedSession struct {
	rows   [][]byte
	record sessionRecord
	found  bool
}

func (a *Agent) loadStored(ctx context.Context, id acp.SessionId) (storedSession, error) {
	ctx, cancel := context.WithTimeout(ctx, acpcore.SessionStoreTimeout)
	defer cancel()

	ctx, finish := a.observe.StartSessionStore(ctx, "load")

	var record sessionRecord

	rows, found, err := sessionlog.Load(ctx, a.store, string(id), &record)
	if err == nil && found {
		err = record.validate(string(id))
	}

	// One native conversation export is the whole main record. A generation
	// with any other row count is not a hermes conversation.
	if err == nil && found && len(rows) != 1 {
		err = errors.New("invalid native snapshot count")
	}

	if err == nil && found {
		_, err = decodeSnapshot(rows[0], record.NativeSessionID)
	}

	finish(err)

	if err != nil {
		return storedSession{}, a.restoreRefused(ctx, id, err)
	}

	return storedSession{rows: rows, record: record, found: found}, nil
}

type nativeSnapshot struct {
	ID       string           `json:"id"`
	Messages []map[string]any `json:"messages"`
}

func decodeSnapshot(data []byte, id string) (nativeSnapshot, error) {
	var snapshot nativeSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return snapshot, err
	}

	if snapshot.ID != id || snapshot.Messages == nil {
		return snapshot, errors.New("invalid native snapshot identity or messages")
	}

	return snapshot, nil
}

// hydrate imports a missing native conversation. An existing conversation wins
// only when its history contains every stored message in order.
func (s *session) hydrate(ctx context.Context, rt *runtime, stored storedSession) ([][]byte, error) {
	if len(stored.rows) != 1 {
		return nil, s.agent.restoreRefused(ctx, s.id, errors.New("invalid native snapshot count"))
	}

	native, err := rt.endpoint.Export(ctx, s.nativeID)
	if err != nil {
		return nil, s.agent.restoreRefused(ctx, s.id, err)
	}

	if len(native) == 0 {
		if importErr := rt.endpoint.Import(ctx, stored.rows[0]); importErr != nil {
			return nil, s.agent.restoreRefused(ctx, s.id, importErr)
		}

		return stored.rows, nil
	}

	want, err := decodeSnapshot(stored.rows[0], s.nativeID)
	if err != nil {
		return nil, s.agent.restoreRefused(ctx, s.id, err)
	}

	have, err := decodeSnapshot(native, s.nativeID)
	if err != nil {
		return nil, s.agent.restoreRefused(ctx, s.id, err)
	}

	if len(have.Messages) < len(want.Messages) {
		return nil, s.agent.restoreRefused(ctx, s.id, errors.New("native conversation is shorter than its mirror"))
	}

	for index, message := range want.Messages {
		if !reflect.DeepEqual(messageContent(message), messageContent(have.Messages[index])) {
			return nil, s.agent.restoreRefused(ctx, s.id, fmt.Errorf("native message %d conflicts with the mirror", index))
		}
	}

	return [][]byte{native}, nil
}

// Native import assigns new row ids. They do not identify message content.
func messageContent(message map[string]any) map[string]any {
	cloned := wire.CloneMap(message)
	delete(cloned, fieldID)
	delete(cloned, nativeSessionIDKey)

	return cloned
}

func (a *Agent) restoreRefused(ctx context.Context, id acp.SessionId, err error) error {
	a.log.ErrorContext(ctx, "Hermes session restore failed", slog.String(nativeSessionIDKey, string(id)), slog.String("reason", err.Error()))

	return wire.RestoreFailed(vendor)
}

func storedTitle(id string, rows [][]byte) string {
	if len(rows) != 1 {
		return id
	}

	snapshot, err := decodeSnapshot(rows[0], id)
	if err != nil {
		return id
	}

	for _, message := range snapshot.Messages {
		if message["role"] == roleUser {
			if text, ok := message["content"].(string); ok && wire.NormalizeTitle(text) != "" {
				return wire.NormalizeTitle(text)
			}
		}
	}

	return id
}

func (s *session) replay(ctx context.Context, rows [][]byte) error {
	if len(rows) == 0 {
		return nil
	}

	snapshot, err := decodeSnapshot(rows[0], s.nativeID)
	if err != nil {
		return err
	}

	state := &cycleState{}

	for _, message := range snapshot.Messages {
		role, _ := message["role"].(string)
		for _, text := range nativeText(message["content"]) {
			switch role {
			case roleUser:
				err = s.emit(ctx, acp.UpdateUserMessageText(text))
			case roleAssistant:
				err = s.emit(ctx, acp.UpdateAgentMessageText(text))
			}

			if err != nil {
				return err
			}
		}

		if role == roleAssistant {
			if thought, ok := message["reasoning"].(string); ok && thought != "" {
				if err := s.emit(ctx, acp.UpdateAgentThoughtText(thought)); err != nil {
					return err
				}
			}

			calls, _ := message["tool_calls"].([]any)
			for _, raw := range calls {
				call, ok := raw.(map[string]any)
				if !ok {
					continue
				}

				function, _ := call["function"].(map[string]any)

				args := function["arguments"]
				if text, ok := args.(string); ok {
					var decoded any
					if json.Unmarshal([]byte(text), &decoded) == nil {
						args = decoded
					}
				}

				payload, marshalErr := json.Marshal(map[string]any{"tool_id": call[fieldID], fieldName: function[fieldName], "args": args})
				if marshalErr != nil {
					return marshalErr
				}

				if err := s.emitTool(ctx, state, hermes.Event{Type: eventToolStart, Payload: payload}); err != nil {
					return err
				}
			}
		}

		if role == "tool" {
			payload, marshalErr := json.Marshal(map[string]any{"tool_id": message["tool_call_id"], fieldName: message["tool_name"], fieldResult: message["content"], "summary": strings.Join(nativeText(message["content"]), "\n")})
			if marshalErr != nil {
				return marshalErr
			}

			if err := s.emitTool(ctx, state, hermes.Event{Type: eventToolComplete, Payload: payload}); err != nil {
				return err
			}
		}
	}

	return nil
}

// nativeText projects text parts without turning image payloads into visible text.
func nativeText(content any) []string {
	if text, ok := content.(string); ok {
		if text == "" {
			return nil
		}

		return []string{text}
	}

	parts, _ := content.([]any)

	var text []string

	for _, part := range parts {
		if object, ok := part.(map[string]any); ok && object[fieldType] == fieldText {
			if value, ok := object[fieldText].(string); ok && value != "" {
				text = append(text, value)
			}
		}
	}

	return text
}

// persistDraft gives an empty native conversation a durable row before NewSession returns.
func (s *session) persistDraft(ctx context.Context, rt *runtime) error {
	snapshot, err := rt.endpoint.Export(ctx, s.nativeID)
	if err != nil || len(snapshot) != 0 {
		return err
	}

	draft, err := json.Marshal(map[string]any{fieldID: s.nativeID, fieldSource: nativeSource, fieldCwd: s.cwd, "started_at": time.Now().Unix(), "messages": []any{}})
	if err != nil {
		return err
	}

	return rt.endpoint.Import(ctx, draft)
}
