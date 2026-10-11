package hermes

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const closeTimeout = 5 * time.Second

const (
	EnvAgentDir       = "HERMES_HOME"
	InternalEnvPrefix = "ACP_GO_HERMES_INTERNAL_"
	EnvSessionToken   = "HERMES_DASHBOARD_SESSION_TOKEN"
	valText           = "text"
	valReasoning      = "reasoning"
	fieldMessages     = "messages"
	// exportLimit bounds one conversation export.
	exportLimit = 64 << 20
	// resultLimit bounds a persistence call's JSON result.
	resultLimit = 1 << 20
)

// loopbackHost is the only interface a serve process binds.
const loopbackHost = "127.0.0.1"

// readyPrefix opens the line serve writes to its stdout once it listens,
// naming the port it bound.
const readyPrefix = "HERMES_BACKEND_READY port="

// Endpoint is the authenticated loopback surface of one serve process.
type Endpoint struct {
	URL   string
	Token string
}

// NewEndpoint mints the token for one serve process; the URL is known once
// the process announces its port.
func NewEndpoint() (Endpoint, error) {
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return Endpoint{}, err
	}

	return Endpoint{Token: hex.EncodeToString(secret[:])}, nil
}

// Bound is the endpoint served on port.
func (e Endpoint) Bound(port int) Endpoint {
	e.URL = "http://" + net.JoinHostPort(loopbackHost, strconv.Itoa(port))

	return e
}

// ServeArgs starts the session's own serve process. Port 0 has the OS assign
// the port at bind, so no other socket can take it between choice and bind;
// serve announces it on stdout. --isolated keeps it from attaching to, or
// being refused by, the backend another hermes serve already runs for this
// user.
func ServeArgs() []string {
	return []string{"serve", "--isolated", "--host", loopbackHost, "--port", "0"}
}

// ScanStdout reads serve's stdout to its end, so serve never blocks writing
// it, and sends the first port serve announces on bound, which must have room
// for it.
func ScanStdout(stdout io.Reader, bound chan<- int) {
	reader := bufio.NewReader(stdout)
	announced, partial := false, false

	for {
		line, err := reader.ReadSlice('\n')
		if !announced && !partial {
			if port, ok := readyPort(line); ok {
				announced = true

				bound <- port
			}
		}

		// A line longer than the buffer arrives in pieces; only its first
		// piece can open with the announcement.
		partial = errors.Is(err, bufio.ErrBufferFull)
		if err != nil && !partial {
			return
		}
	}
}

func readyPort(line []byte) (int, bool) {
	value, found := strings.CutPrefix(strings.TrimSpace(string(line)), readyPrefix)
	if !found {
		return 0, false
	}

	port, err := strconv.Atoi(value)
	if err != nil || port <= 0 || port > 65535 {
		return 0, false
	}

	return port, true
}

// Connect retries only the startup connection; the caller bounds readiness.
func (e Endpoint) Connect(ctx context.Context) (*Client, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		client, err := Dial(ctx, strings.Replace(e.URL, "http://", "ws://", 1)+"/api/ws?token="+url.QueryEscape(e.Token), http.Header{"X-Hermes-Session-Token": []string{e.Token}})
		if err == nil {
			return client, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

// Conversation is one persisted Hermes conversation as its export carries it:
// the session object without its messages, and each message as the raw
// object Hermes wrote.
type Conversation struct {
	Session  json.RawMessage
	Messages [][]byte
}

// Export streams the complete native record of one persisted conversation.
// found is false when Hermes has no conversation under id. Each message is
// kept as its own raw object, so no message is decoded or re-encoded.
func (e Endpoint) Export(ctx context.Context, id string) (conversation Conversation, found bool, err error) {
	response, err := e.send(ctx, http.MethodGet, "/api/sessions/"+url.PathEscape(id)+"/export", nil)
	if err != nil {
		return Conversation{}, false, err
	}
	defer response.Body.Close()

	switch response.StatusCode {
	case http.StatusNotFound:
		return Conversation{}, false, nil
	case http.StatusOK:
	default:
		return Conversation{}, false, fmt.Errorf("hermes persistence HTTP status %d", response.StatusCode)
	}

	body := &limitedReader{source: response.Body, remaining: exportLimit}

	conversation, err = DecodeConversation(body)
	if body.exceeded {
		return Conversation{}, false, errors.New("native session export exceeds size limit")
	}

	if err != nil {
		return Conversation{}, false, fmt.Errorf("invalid native session export: %w", err)
	}

	if SessionID(conversation.Session) != id {
		return Conversation{}, false, errors.New("invalid native session export: identity differs")
	}

	return conversation, true, nil
}

// DecodeConversation reads one exported conversation: a JSON object with
// exactly one "messages" array of objects. Every other member is kept, in
// order and byte for byte, in Session.
func DecodeConversation(reader io.Reader) (Conversation, error) {
	decoder := json.NewDecoder(reader)

	if err := expectDelim(decoder, '{'); err != nil {
		return Conversation{}, err
	}

	var (
		session  bytes.Buffer
		messages [][]byte
		listed   bool
	)

	session.WriteByte('{')

	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return Conversation{}, err
		}

		key, _ := token.(string)
		if key != fieldMessages {
			var value json.RawMessage
			if decodeErr := decoder.Decode(&value); decodeErr != nil {
				return Conversation{}, decodeErr
			}

			if session.Len() > 1 {
				session.WriteByte(',')
			}

			name, _ := json.Marshal(key)
			session.Write(name)
			session.WriteByte(':')
			session.Write(value)

			continue
		}

		if listed {
			return Conversation{}, errors.New("messages listed twice")
		}

		listed = true

		if messages, err = decodeMessages(decoder); err != nil {
			return Conversation{}, err
		}
	}

	if err := expectDelim(decoder, '}'); err != nil {
		return Conversation{}, err
	}

	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return Conversation{}, errors.New("trailing data after the conversation")
	}

	if !listed {
		return Conversation{}, errors.New("conversation lists no messages")
	}

	session.WriteByte('}')

	return Conversation{Session: session.Bytes(), Messages: messages}, nil
}

func decodeMessages(decoder *json.Decoder) ([][]byte, error) {
	if err := expectDelim(decoder, '['); err != nil {
		return nil, err
	}

	messages := [][]byte{}

	for decoder.More() {
		var message json.RawMessage
		if err := decoder.Decode(&message); err != nil {
			return nil, err
		}

		if len(message) == 0 || message[0] != '{' {
			return nil, fmt.Errorf("message %d is not an object", len(messages))
		}

		messages = append(messages, message)
	}

	return messages, expectDelim(decoder, ']')
}

func expectDelim(decoder *json.Decoder, want json.Delim) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}

	if token != want {
		return fmt.Errorf("expected %q", want)
	}

	return nil
}

// SessionID is the "id" member of an exported session object, or "" when it
// has none.
func SessionID(session json.RawMessage) string {
	var identity struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(session, &identity) != nil {
		return ""
	}

	return identity.ID
}

// Encode writes the conversation as Hermes's export shape: the session
// object with its messages appended as the last member.
func (c Conversation) Encode() ([]byte, error) {
	session := bytes.TrimSpace(c.Session)
	if len(session) < 2 || session[0] != '{' || session[len(session)-1] != '}' {
		return nil, errors.New("native session is not an object")
	}

	size := len(session) + len(`,"messages":[]`) + len(c.Messages)
	for _, message := range c.Messages {
		size += len(message)
	}

	encoded := make([]byte, 0, size)
	encoded = append(encoded, session[:len(session)-1]...)

	if len(bytes.TrimSpace(session[1:len(session)-1])) > 0 {
		encoded = append(encoded, ',')
	}

	encoded = append(encoded, `"messages":[`...)

	for index, message := range c.Messages {
		if index > 0 {
			encoded = append(encoded, ',')
		}

		encoded = append(encoded, message...)
	}

	return append(encoded, "]}"...), nil
}

// Import creates the missing conversation through Hermes's own persistence API.
func (e Endpoint) Import(ctx context.Context, conversation Conversation) error {
	encoded, err := conversation.Encode()
	if err != nil {
		return err
	}

	body := make([]byte, 0, len(encoded)+len(`{"sessions":[]}`))
	body = append(body, `{"sessions":[`...)
	body = append(body, encoded...)
	body = append(body, "]}"...)

	data, err := e.request(ctx, http.MethodPost, "/api/sessions/import", body)
	if err != nil {
		return err
	}

	var result struct {
		OK       bool `json:"ok"`
		Imported int  `json:"imported"`
	}
	if json.Unmarshal(data, &result) != nil || !result.OK || result.Imported != 1 {
		return errors.New("native session import refused")
	}

	return nil
}

// limitedReader reads at most remaining bytes and records whether the
// source held more.
type limitedReader struct {
	source    io.Reader
	remaining int64
	exceeded  bool
}

func (l *limitedReader) Read(buffer []byte) (int, error) {
	if l.remaining <= 0 {
		var probe [1]byte
		if n, _ := l.source.Read(probe[:]); n > 0 {
			l.exceeded = true
		}

		return 0, io.EOF
	}

	if int64(len(buffer)) > l.remaining {
		buffer = buffer[:l.remaining]
	}

	n, err := l.source.Read(buffer)
	l.remaining -= int64(n)

	return n, err
}

func (e Endpoint) send(ctx context.Context, method, path string, body []byte) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, method, e.URL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	request.Header.Set("X-Hermes-Session-Token", e.Token)
	request.Header.Set("Content-Type", "application/json")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, errors.New("hermes persistence request failed")
	}

	return response, nil
}

// request sends one persistence call whose answer is a small JSON result.
func (e Endpoint) request(ctx context.Context, method, path string, body []byte) ([]byte, error) {
	response, err := e.send(ctx, method, path, body)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("hermes persistence HTTP status %d", response.StatusCode)
	}

	data, err := io.ReadAll(io.LimitReader(response.Body, resultLimit+1))
	if err != nil {
		return nil, err
	}

	if len(data) > resultLimit {
		return nil, errors.New("hermes persistence result exceeds size limit")
	}

	return data, nil
}

func AgentDir(explicit string, lookup func(string) (string, bool)) string {
	if explicit != "" {
		return explicit
	}

	if home, _ := lookup(EnvAgentDir); home != "" {
		return home
	}

	home, _ := lookup("HOME")

	return filepath.Join(home, ".hermes")
}

func (c *Client) Err() error { return c.terminalCause() }

func String(payload json.RawMessage, key string) string {
	var object map[string]json.RawMessage
	if json.Unmarshal(payload, &object) != nil {
		return ""
	}

	var value string

	_ = json.Unmarshal(object[key], &value)

	return value
}

// ValidSession reports why session is not the exported session object of
// conversation id without its messages.
func ValidSession(session json.RawMessage, id string) error {
	var members map[string]json.RawMessage
	if err := json.Unmarshal(session, &members); err != nil || members == nil {
		return errors.New("native session is not an object")
	}

	if _, listed := members[fieldMessages]; listed {
		return errors.New("native session carries its messages")
	}

	if SessionID(session) != id {
		return errors.New("native session identity differs")
	}

	return nil
}

// SameMessage reports whether two exported messages carry the same content.
// Import assigns new row ids and the owning session's id, so neither counts.
// Members are compared as raw bytes, decoding only a member whose bytes
// differ.
func SameMessage(a, b []byte) (bool, error) {
	var left, right map[string]json.RawMessage
	if err := json.Unmarshal(a, &left); err != nil {
		return false, err
	}

	if err := json.Unmarshal(b, &right); err != nil {
		return false, err
	}

	for _, key := range []string{fieldID, fieldSessionID} {
		delete(left, key)
		delete(right, key)
	}

	if len(left) != len(right) {
		return false, nil
	}

	for key, value := range left {
		other, ok := right[key]
		if !ok {
			return false, nil
		}

		if bytes.Equal(value, other) {
			continue
		}

		var leftValue, rightValue any
		if err := json.Unmarshal(value, &leftValue); err != nil {
			return false, err
		}

		if err := json.Unmarshal(other, &rightValue); err != nil {
			return false, err
		}

		if !reflect.DeepEqual(leftValue, rightValue) {
			return false, nil
		}
	}

	return true, nil
}
