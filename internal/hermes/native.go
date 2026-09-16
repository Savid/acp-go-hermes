package hermes

import (
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
	"regexp"
	"strconv"
	"strings"
	"time"

	"os/exec"
)

const closeTimeout = 5 * time.Second

const (
	EnvAgentDir       = "HERMES_HOME"
	InternalEnvPrefix = "ACP_GO_HERMES_INTERNAL_"
	EnvSessionToken   = "HERMES_DASHBOARD_SESSION_TOKEN"
	valText           = "text"
	valReasoning      = "reasoning"
	snapshotLimit     = 64 << 20
)

// Endpoint is the authenticated loopback surface of one serve process.
type Endpoint struct {
	URL   string
	Token string
}

func NewEndpoint() (Endpoint, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return Endpoint{}, err
	}

	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		return Endpoint{}, err
	}

	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return Endpoint{}, err
	}

	return Endpoint{URL: "http://" + address, Token: hex.EncodeToString(secret[:])}, nil
}

func (e Endpoint) Args() []string {
	_, port, _ := net.SplitHostPort(strings.TrimPrefix(e.URL, "http://"))

	return []string{"serve", "--host", "127.0.0.1", "--port", port}
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

// Export obtains the complete native record for one persisted conversation.
func (e Endpoint) Export(ctx context.Context, id string) (json.RawMessage, error) {
	data, status, err := e.request(ctx, http.MethodGet, "/api/sessions/"+url.PathEscape(id)+"/export", nil)
	if status == http.StatusNotFound {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	var snapshot struct {
		ID       string            `json:"id"`
		Messages []json.RawMessage `json:"messages"`
	}
	if err := json.Unmarshal(data, &snapshot); err != nil || snapshot.ID != id || snapshot.Messages == nil {
		return nil, errors.New("invalid native session export")
	}

	return data, nil
}

// Import creates the missing conversation through Hermes's own persistence API.
func (e Endpoint) Import(ctx context.Context, snapshot json.RawMessage) error {
	body, err := json.Marshal(map[string]any{"sessions": []json.RawMessage{snapshot}})
	if err != nil {
		return err
	}

	data, _, err := e.request(ctx, http.MethodPost, "/api/sessions/import", body)
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

func (e Endpoint) request(ctx context.Context, method, path string, body []byte) ([]byte, int, error) {
	request, err := http.NewRequestWithContext(ctx, method, e.URL+path, bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}

	request.Header.Set("X-Hermes-Session-Token", e.Token)
	request.Header.Set("Content-Type", "application/json")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, 0, errors.New("hermes persistence request failed")
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, response.StatusCode, fmt.Errorf("hermes persistence HTTP status %d", response.StatusCode)
	}

	data, err := io.ReadAll(io.LimitReader(response.Body, snapshotLimit+1))
	if err != nil {
		return nil, response.StatusCode, err
	}

	if len(data) > snapshotLimit {
		return nil, response.StatusCode, errors.New("native session export exceeds size limit")
	}

	return data, response.StatusCode, nil
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

var nativeVersion = regexp.MustCompile(`\bv?(\d+\.\d+\.\d+)\b`)

func ProbeVersion(ctx context.Context, executable string, env []string) (string, error) {
	cmd := exec.CommandContext(ctx, executable, "--version")
	cmd.Env = env

	output, err := cmd.Output()
	if err != nil {
		return "", errors.New("hermes version probe failed")
	}

	match := nativeVersion.FindStringSubmatch(string(output))
	if len(match) != 2 {
		return "", errors.New("hermes version missing")
	}

	return match[1], nil
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

func Number(payload json.RawMessage, key string) int64 {
	var object map[string]json.RawMessage
	if json.Unmarshal(payload, &object) != nil {
		return 0
	}

	value, _ := strconv.ParseFloat(string(object[key]), 64)

	return int64(value)
}
