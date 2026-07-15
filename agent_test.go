package hermesacp

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/coder/acp-go-sdk"
)

func TestServeCloseErrorAndAgentCloneFallbacks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := newFakeHermesClient()
	client.closeErr = errors.Join(errors.New("close failed"), ErrProcessTreeUnproven)
	agent := NewAgent()
	session := testSession(agent, client)
	agent.sessions[session.id] = session

	started := make(chan struct{})
	oldNewAgent := newAgentForServe
	newAgentForServe = func(...Option) *Agent {
		close(started)

		return agent
	}
	t.Cleanup(func() { newAgentForServe = oldNewAgent })
	input, inputWriter := io.Pipe()
	t.Cleanup(func() {
		_ = input.Close()
		_ = inputWriter.Close()
	})
	errCh := make(chan error, 1)
	go func() { errCh <- Serve(ctx, input, io.Discard) }()
	<-started
	cancel()
	if err := <-errCh; !errors.Is(err, ErrProcessTreeUnproven) {
		t.Fatalf("Serve close proof error = %v", err)
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
