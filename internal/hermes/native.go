package hermes

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
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
	fieldActive       = "active"
	fieldCompacted    = "compacted"
	// valueLimit bounds one message or session member of an export.
	valueLimit = 64 << 20
	// keptLimit bounds the new messages one continuing export read keeps.
	keptLimit = 64 << 20
	// errorTextLimit bounds the failure text kept from a persistence answer.
	errorTextLimit = 4 << 10
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

// Prefix identifies the leading messages of a conversation: how many, their
// size, and a digest of their bytes. The zero Prefix is the empty one.
type Prefix struct {
	Count  int
	Bytes  int64
	Digest [sha256.Size]byte
}

// PrefixOf is the prefix that holds exactly messages.
func PrefixOf(messages [][]byte) Prefix {
	digest := sha256.New()
	prefix := Prefix{Count: len(messages)}

	for _, message := range messages {
		writeDigest(digest, message)
		prefix.Bytes += int64(len(message))
	}

	digest.Sum(prefix.Digest[:0])

	return prefix
}

// Export is what one export read returned.
type Export struct {
	// Session is the session object without its messages.
	Session json.RawMessage
	// Messages are the messages after the prefix the read was given, or every
	// message when Continued is false.
	Messages [][]byte
	// Continued reports that the conversation still begins with exactly the
	// prefix the read was given. A message Hermes changed in place, or a
	// shorter conversation, makes it false, and Messages then holds every
	// message.
	Continued bool
	// Next is the prefix that holds every message the conversation exported.
	Next Prefix
}

// Hermes's import refuses a conversation with more messages, or whose
// encoding is larger, than these.
const (
	ImportMessageLimit = 10_000
	ImportByteLimit    = 5 << 20
)

// ImportSize is about the size of the conversation's encoding as Hermes's
// import measures it: the session object with every message the export holds.
func (e Export) ImportSize() int64 {
	return int64(len(e.Session)+len(`,"messages":[]`)+max(e.Next.Count-1, 0)) + e.Next.Bytes
}

// ImportRefused reports whether Hermes's import would refuse the
// conversation, so a restore that must import it would fail.
func (e Export) ImportRefused() bool {
	return e.Next.Count > ImportMessageLimit || e.ImportSize() > ImportByteLimit
}

// Export streams the native record of one persisted conversation and keeps
// only the messages after prefix: the ones before it are digested from a
// reused buffer and dropped. found is false when Hermes has no conversation
// under id. A conversation that no longer begins with prefix is read again in
// full. Each kept message is its own raw object, never decoded or re-encoded.
func (e Endpoint) Export(ctx context.Context, id string, prefix Prefix) (export Export, found bool, err error) {
	export, found, err = e.export(ctx, id, prefix)
	if err != nil || !found || export.Continued || prefix.Count == 0 {
		return export, found, err
	}

	export, found, err = e.export(ctx, id, Prefix{})
	export.Continued = false

	return export, found, err
}

func (e Endpoint) export(ctx context.Context, id string, prefix Prefix) (Export, bool, error) {
	response, err := e.send(ctx, http.MethodGet, "/api/sessions/"+url.PathEscape(id)+"/export", nil)
	if err != nil {
		return Export{}, false, err
	}
	defer response.Body.Close()

	switch response.StatusCode {
	case http.StatusNotFound:
		return Export{}, false, nil
	case http.StatusOK:
	default:
		return Export{}, false, statusError(response)
	}

	export, err := DecodeExport(response.Body, prefix)
	if err != nil {
		return Export{}, false, fmt.Errorf("invalid native session export: %w", err)
	}

	if SessionID(export.Session) != id {
		return Export{}, false, errors.New("invalid native session export: identity differs")
	}

	return export, true, nil
}

// DecodeConversation reads one whole exported conversation.
func DecodeConversation(reader io.Reader) (Conversation, error) {
	export, err := DecodeExport(reader, Prefix{})

	return Conversation{Session: export.Session, Messages: export.Messages}, err
}

// DecodeExport reads one exported conversation, a JSON object with exactly
// one "messages" array of objects, keeping the messages after prefix. Every
// other member is kept, in order and byte for byte, in Session. When the
// messages do not begin with exactly prefix, Continued is false and Messages
// is nil. The export as a whole has no size bound: each message and session
// member is bounded, and so are the new messages a read after a nonempty
// prefix keeps; a read from the empty prefix keeps the whole conversation.
func DecodeExport(reader io.Reader, prefix Prefix) (Export, error) {
	return decodeExport(reader, prefix, exportBounds{value: valueLimit, kept: keptLimit})
}

// exportBounds are the largest message or session member, and the most new
// bytes a continuing read keeps.
type exportBounds struct {
	value, kept int64
}

func decodeExport(reader io.Reader, prefix Prefix, bounds exportBounds) (Export, error) {
	bounded := &valueReader{source: reader, value: bounds.value, limit: 2 * bounds.value}
	decoder := json.NewDecoder(bounded)

	if err := expectDelim(decoder, '{'); err != nil {
		return Export{}, err
	}

	var (
		session bytes.Buffer
		export  Export
		listed  bool
	)

	session.WriteByte('{')

	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return Export{}, err
		}

		key, _ := token.(string)
		if key != fieldMessages {
			var value json.RawMessage
			if decodeErr := decodeValue(decoder, bounded, &value); decodeErr != nil {
				return Export{}, fmt.Errorf("session member %q: %w", key, decodeErr)
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
			return Export{}, errors.New("messages listed twice")
		}

		listed = true

		if export, err = decodeMessages(decoder, bounded, prefix, bounds.kept); err != nil {
			return Export{}, err
		}
	}

	if err := expectDelim(decoder, '}'); err != nil {
		return Export{}, err
	}

	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return Export{}, errors.New("trailing data after the conversation")
	}

	if !listed {
		return Export{}, errors.New("conversation lists no messages")
	}

	session.WriteByte('}')
	export.Session = session.Bytes()

	return export, nil
}

// decodeMessages reads the messages array, digesting every message and
// keeping the ones after prefix.
func decodeMessages(decoder *json.Decoder, bounded *valueReader, prefix Prefix, keptLimit int64) (Export, error) {
	if err := expectDelim(decoder, '['); err != nil {
		return Export{}, err
	}

	var (
		digest  = sha256.New()
		message json.RawMessage
		count   int
		kept    int64
		sum     [sha256.Size]byte
	)

	export := Export{Messages: [][]byte{}, Continued: prefix.Count == 0}

	for decoder.More() {
		// The prefix's messages are dropped, so one buffer holds each in turn.
		if err := decodeValue(decoder, bounded, &message); err != nil {
			return Export{}, fmt.Errorf("message %d: %w", count, err)
		}

		if len(message) == 0 || message[0] != '{' {
			return Export{}, fmt.Errorf("message %d is not an object", count)
		}

		count++

		writeDigest(digest, message)
		export.Next.Bytes += int64(len(message))

		if count < prefix.Count {
			continue
		}

		if count == prefix.Count {
			export.Continued = [sha256.Size]byte(digest.Sum(sum[:0])) == prefix.Digest

			continue
		}

		if !export.Continued {
			continue
		}

		if kept += int64(len(message)); prefix.Count > 0 && kept > keptLimit {
			return Export{}, fmt.Errorf("new messages exceed %d bytes", keptLimit)
		}

		export.Messages = append(export.Messages, bytes.Clone(message))
	}

	if !export.Continued {
		export.Messages = nil
	}

	export.Next.Count = count
	digest.Sum(export.Next.Digest[:0])

	return export, expectDelim(decoder, ']')
}

// writeDigest adds one length-prefixed message to digest.
func writeDigest(digest hash.Hash, message []byte) {
	var length [binary.MaxVarintLen64]byte

	digest.Write(length[:binary.PutUvarint(length[:], uint64(len(message)))])
	digest.Write(message)
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
// Hermes fences every session mutation: a request id it keeps as the retry
// receipt, and the revision the caller expects, which is 0 for a session the
// store does not hold.
func (e Endpoint) Import(ctx context.Context, conversation Conversation) error {
	encoded, err := conversation.Encode()
	if err != nil {
		return err
	}

	// rand.Text is base32, so it needs no JSON escaping.
	head := `{"request_id":"` + rand.Text() + `","expected_revision":0,"sessions":[`

	body := make([]byte, 0, len(head)+len(encoded)+len("]}"))
	body = append(body, head...)
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
		return fmt.Errorf("native session import refused: %s", errorText(data))
	}

	return nil
}

// valueReader fails once more than limit bytes arrived since the decoder
// last finished a value, so a decoder never buffers one oversized value.
type valueReader struct {
	source io.Reader
	value  int64
	limit  int64
	since  int64
}

var errValueTooLarge = errors.New("value exceeds its bound")

func (r *valueReader) Read(buffer []byte) (int, error) {
	if r.since >= r.limit {
		return 0, errValueTooLarge
	}

	if int64(len(buffer)) > r.limit-r.since {
		buffer = buffer[:r.limit-r.since]
	}

	n, err := r.source.Read(buffer)
	r.since += int64(n)

	return n, err
}

// decodeValue decodes the next value into value and starts a new bound.
func decodeValue(decoder *json.Decoder, bounded *valueReader, value *json.RawMessage) error {
	if err := decoder.Decode(value); err != nil {
		return err
	}

	if int64(len(*value)) > bounded.value {
		return errValueTooLarge
	}

	// The decoder may already hold the start of the next value, which a new
	// bound of twice the value limit leaves room for.
	bounded.since = 0

	return nil
}

// statusError names a persistence answer's status and the failure text
// Hermes sent with it.
func statusError(response *http.Response) error {
	data, _ := io.ReadAll(io.LimitReader(response.Body, errorTextLimit))

	return fmt.Errorf("hermes persistence HTTP status %d: %s", response.StatusCode, errorText(data))
}

// errorText is a persistence answer as one line of bounded text.
func errorText(data []byte) string {
	text := strings.Join(strings.Fields(string(data)), " ")
	if len(text) > errorTextLimit {
		text = text[:errorTextLimit]
	}

	return text
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
		return nil, statusError(response)
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
// Import assigns new row ids and the owning session's id, and Hermes archives
// a message in place by clearing its active flag and setting its compacted
// flag, so none of these count. Members are compared as raw bytes, decoding
// only a member whose bytes differ.
func SameMessage(a, b []byte) (bool, error) {
	var left, right map[string]json.RawMessage
	if err := json.Unmarshal(a, &left); err != nil {
		return false, err
	}

	if err := json.Unmarshal(b, &right); err != nil {
		return false, err
	}

	for _, key := range []string{fieldID, fieldSessionID, fieldActive, fieldCompacted} {
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
