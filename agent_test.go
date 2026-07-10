package hermesacp

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
)

func TestServeCloseErrorAndAgentCloneFallbacks(t *testing.T) {
	ctx := context.Background()
	client := newFakeHermesClient()
	client.closeErr = errors.New("close failed")
	agent := NewAgent()
	session := testSession(agent, client)
	agent.sessions[session.id] = session

	oldNewAgent := newAgentForServe
	newAgentForServe = func(...Option) *Agent { return agent }
	t.Cleanup(func() { newAgentForServe = oldNewAgent })
	if err := Serve(ctx, strings.NewReader(""), io.Discard); err != nil {
		t.Fatalf("Serve: %v", err)
	}

	oldMarshal := agentJSONMarshal
	oldUnmarshal := agentJSONUnmarshal
	t.Cleanup(func() {
		agentJSONMarshal = oldMarshal
		agentJSONUnmarshal = oldUnmarshal
	})
	agentJSONMarshal = func(any) ([]byte, error) { return nil, errors.New("marshal failed") }
	if cloneClientCapabilities(acp.ClientCapabilities{Meta: map[string]any{"a": "b"}}).Meta["a"] != "b" {
		t.Fatal("cloneClientCapabilities marshal fallback changed caps")
	}
	agent.clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
	if agent.clientElicitationCapabilities() == nil {
		t.Fatal("clientElicitationCapabilities marshal fallback returned nil")
	}
	agentJSONMarshal = oldMarshal
	agentJSONUnmarshal = func([]byte, any) error { return errors.New("unmarshal failed") }
	if cloneClientCapabilities(acp.ClientCapabilities{Meta: map[string]any{"a": "b"}}).Meta["a"] != "b" {
		t.Fatal("cloneClientCapabilities unmarshal fallback changed caps")
	}
	if agent.clientElicitationCapabilities() == nil {
		t.Fatal("clientElicitationCapabilities unmarshal fallback returned nil")
	}
}
