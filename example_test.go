package hermesacp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
)

func ExampleServe_initialize() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientToAgentReader, clientToAgentWriter := io.Pipe()
	agentToClientReader, agentToClientWriter := io.Pipe()
	defer clientToAgentReader.Close()
	defer clientToAgentWriter.Close()
	defer agentToClientReader.Close()
	defer agentToClientWriter.Close()

	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, clientToAgentReader, agentToClientWriter)
	}()

	_, _ = fmt.Fprintln(clientToAgentWriter, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`)
	line, _ := bufio.NewReader(agentToClientReader).ReadString('\n')
	cancel()
	_ = clientToAgentWriter.Close()
	<-done

	var response struct {
		Result struct {
			AuthMethods       []any `json:"authMethods"`
			AgentCapabilities struct {
				LoadSession     bool `json:"loadSession"`
				McpCapabilities struct {
					Http bool `json:"http"`
				} `json:"mcpCapabilities"`
			} `json:"agentCapabilities"`
		} `json:"result"`
	}
	_ = json.Unmarshal([]byte(line), &response)

	fmt.Println(len(response.Result.AuthMethods) == 0)
	fmt.Println(response.Result.AgentCapabilities.LoadSession)
	fmt.Println(response.Result.AgentCapabilities.McpCapabilities.Http)
	// Output:
	// true
	// true
	// true
}
