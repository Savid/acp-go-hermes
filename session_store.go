package hermesacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/observer"
	"github.com/savid/acp-go-core/sessionlog"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-hermes/internal/hermes"
)

// sessionRecord carries the accepted session configuration and the native
// session object whose messages are the main record's rows.
type sessionRecord struct {
	SessionID             string            `json:"sessionId"`
	NativeSessionID       string            `json:"nativeSessionId"`
	Cwd                   string            `json:"cwd"`
	AdditionalDirectories []string          `json:"additionalDirectories,omitempty"`
	Env                   map[string]string `json:"env,omitempty"`
	ExtraPathDirs         []string          `json:"extraPathDirs,omitempty"`
	Model                 string            `json:"model,omitempty"`
	Effort                string            `json:"effort,omitempty"`
	Title                 string            `json:"title,omitempty"`
	// Session is Hermes's exported session object without its messages.
	Session json.RawMessage `json:"session"`
}

func (s *session) record() sessionRecord {
	s.mu.Lock()
	native := s.nativeSession
	s.mu.Unlock()

	return s.recordWith(native)
}

// recordWith is the session's record carrying native as its session object.
func (s *session) recordWith(native json.RawMessage) sessionRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	return sessionRecord{SessionID: string(s.id), NativeSessionID: s.nativeID, Cwd: s.cwd, AdditionalDirectories: slices.Clone(s.additionalDirectories), Env: maps.Clone(s.options.Env), ExtraPathDirs: slices.Clone(s.options.ExtraPathDirs), Model: s.model, Effort: s.effort, Title: s.title, Session: native}
}

func (r sessionRecord) validate(id string) error {
	if r.NativeSessionID == "" || r.SessionID != id || !filepath.IsAbs(r.Cwd) {
		return errors.New("invalid session record")
	}

	if err := hermes.ValidSession(r.Session, r.NativeSessionID); err != nil {
		return err
	}

	for _, dir := range r.AdditionalDirectories {
		if !filepath.IsAbs(dir) {
			return errors.New("invalid additional directory")
		}
	}

	// parseSessionMeta refuses with a typed request error, so only a
	// refusal becomes a non-nil error.
	if _, refusal := parseSessionMeta(inheritCarrier(sessionMeta{}, r).Meta()); refusal != nil {
		return refusal
	}

	return nil
}

// commitMirror publishes the conversation the runtime the caller dispatched
// on exports, so a commit that cannot be attempted fails instead of reporting
// success. The mirror continues from the messages it committed and appends
// the ones after them; a message Hermes changed in place republishes the
// whole conversation.
func (s *session) commitMirror(ctx context.Context, rt *runtime) error {
	if s.ephemeral {
		return nil
	}

	// Commit may run the source and record more than once; each read leaves
	// its session object here, and only the committed one reaches the session.
	var native json.RawMessage

	ctx, finish := s.agent.observe.StartSessionStore(ctx, observer.SessionStoreCommit)
	err := s.mirror.Commit(ctx, s.exportSource(ctx, rt, &native), func(sessionlog.Tail) (any, error) { return s.recordWith(native), nil })
	finish(err)

	if err == nil {
		s.mu.Lock()
		s.nativeSession = native
		s.persisted = true
		s.mu.Unlock()
	}

	return err
}

// exportSource reads the conversation the runtime is bound to after the
// prefix the mirror's cursor names, after checking that the gateway still
// serves it under the recorded identity. Each read leaves the exported
// session object in native for the record the commit publishes with it.
func (s *session) exportSource(ctx context.Context, rt *runtime, native *json.RawMessage) sessionlog.Source {
	return func(from sessionlog.Cursor) (sessionlog.Tail, error) {
		id, err := s.boundConversation(ctx, rt)
		if err != nil {
			return sessionlog.Tail{}, err
		}

		prefix, _ := from.Position().(hermes.Prefix)

		export, exported, err := rt.endpoint.Export(ctx, id, prefix)
		if err != nil {
			return sessionlog.Tail{}, err
		}

		if !exported {
			return sessionlog.Tail{}, errors.New("native conversation missing")
		}

		*native = export.Session

		return sessionlog.Tail{Rows: export.Messages, Next: sessionlog.Position(export.Next), Full: prefix.Count == 0 || !export.Continued}, nil
	}
}

// boundConversation is the durable conversation the runtime is bound to,
// once the gateway confirms it still serves it under the recorded identity.
func (s *session) boundConversation(ctx context.Context, rt *runtime) (string, error) {
	s.mu.Lock()
	id := s.nativeID
	s.mu.Unlock()

	if rt == nil || id == "" {
		return "", errors.New("native session binding missing")
	}

	var active hermes.ActiveListResult
	if err := rt.client.Call(ctx, "session.active_list", map[string]any{}, &active); err != nil {
		return "", err
	}

	for _, native := range active.Sessions {
		if native.SessionID != rt.liveID {
			continue
		}

		if native.SessionKey != id {
			s.poisonSession(ctx, wire.CauseNativeSessionIdentityDrift)

			return "", errors.New("native session identity changed")
		}

		return id, nil
	}

	return "", errors.New("native session binding missing")
}

type storedSession struct {
	rows   [][]byte
	record sessionRecord
	found  bool
}

func (a *Agent) loadStored(ctx context.Context, id acp.SessionId) (storedSession, error) {
	ctx, finish := a.observe.StartSessionStore(ctx, observer.SessionStoreLoad)

	var record sessionRecord

	rows, found, err := sessionlog.Load(ctx, a.store, string(id), &record)
	if err == nil && found {
		err = record.validate(string(id))
	}

	finish(err)

	if err != nil {
		return storedSession{}, wire.RestoreRefused(ctx, a.log, vendor, id, err)
	}

	return storedSession{rows: rows, record: record, found: found}, nil
}

// storedRecord reads a stored session's configuration without its messages.
func (a *Agent) storedRecord(ctx context.Context, id acp.SessionId) (sessionRecord, bool, error) {
	ctx, finish := a.observe.StartSessionStore(ctx, observer.SessionStoreLoadRecord)

	var record sessionRecord

	found, err := sessionlog.LoadRecord(ctx, a.store, string(id), &record)
	if err == nil && found {
		err = record.validate(string(id))
	}

	finish(err)

	return record, found, err
}

// hydrate imports a missing native conversation. An existing conversation wins
// only when its history contains every stored message in order, archive flags
// aside. When it begins with exactly the stored messages, the mirror adopts
// the stored generation, so the next commit appends. Hermes's import gives
// every message a new row id, so the commit after an import republishes the
// conversation. It returns the messages the session now holds.
func (s *session) hydrate(ctx context.Context, rt *runtime, stored storedSession) ([][]byte, error) {
	native, found, err := rt.endpoint.Export(ctx, s.nativeID, hermes.Prefix{})
	if err != nil {
		return nil, wire.RestoreRefused(ctx, s.agent.log, vendor, s.id, err)
	}

	if !found {
		conversation := hermes.Conversation{Session: stored.record.Session, Messages: stored.rows}
		if importErr := rt.endpoint.Import(ctx, conversation); importErr != nil {
			return nil, wire.RestoreRefused(ctx, s.agent.log, vendor, s.id, importErr)
		}

		return stored.rows, nil
	}

	if len(native.Messages) < len(stored.rows) {
		return nil, wire.RestoreRefused(ctx, s.agent.log, vendor, s.id, errors.New("native conversation is shorter than its mirror"))
	}

	identical := true

	for index, message := range stored.rows {
		if bytes.Equal(message, native.Messages[index]) {
			continue
		}

		identical = false

		same, sameErr := hermes.SameMessage(message, native.Messages[index])
		if sameErr != nil || !same {
			return nil, wire.RestoreRefused(ctx, s.agent.log, vendor, s.id, fmt.Errorf("native message %d conflicts with the mirror", index))
		}
	}

	s.mu.Lock()
	s.nativeSession = native.Session
	s.mu.Unlock()

	if identical {
		if err := s.mirror.Adopt(stored.rows, stored.record, sessionlog.Position(hermes.PrefixOf(stored.rows))); err != nil {
			return nil, wire.RestoreRefused(ctx, s.agent.log, vendor, s.id, err)
		}
	}

	return native.Messages, nil
}

// replay emits the stored messages as conversation history, decoding one
// message at a time.
func (s *session) replay(ctx context.Context, rows [][]byte) error {
	state := &cycleState{}

	for _, row := range rows {
		var message map[string]any
		if err := json.Unmarshal(row, &message); err != nil {
			return err
		}

		var err error

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

				payload, marshalErr := json.Marshal(map[string]any{fieldToolID: call[fieldID], fieldName: function[fieldName], "args": args})
				if marshalErr != nil {
					return marshalErr
				}

				if err := s.emitTool(ctx, state, hermes.Event{Type: eventToolStart, Payload: payload}); err != nil {
					return err
				}
			}
		}

		if role == "tool" {
			payload, marshalErr := json.Marshal(map[string]any{fieldToolID: message["tool_call_id"], fieldName: message["tool_name"], fieldResult: message["content"], "summary": strings.Join(nativeText(message["content"]), "\n")})
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

// persistDraft gives an empty native conversation a durable session row
// before NewSession returns.
func (s *session) persistDraft(ctx context.Context, rt *runtime) error {
	_, found, err := rt.endpoint.Export(ctx, s.nativeID, hermes.Prefix{})
	if err != nil || found {
		return err
	}

	session, err := json.Marshal(map[string]any{fieldID: s.nativeID, fieldSource: nativeSource, fieldCwd: s.cwd, "started_at": time.Now().Unix()})
	if err != nil {
		return err
	}

	return rt.endpoint.Import(ctx, hermes.Conversation{Session: session, Messages: [][]byte{}})
}
