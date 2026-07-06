# Interactive Chat

This example is an interactive ACP client that embeds an in-process
`hermesacp.NewAgent`, initializes the connection, and creates a session, then
runs a read-eval-print loop that sends each line you type as a prompt and
prints the stop reason of each turn back to the terminal.

```sh
go run ./examples/interactive-chat
```

At the `> ` prompt, Enter submits the current line. Type `exit` or submit an
empty line to leave, and press Ctrl-C to exit at any time. A local `hermes` CLI
must be installed and authenticated.
