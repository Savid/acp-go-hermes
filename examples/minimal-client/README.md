# Minimal Client

This example is a small ACP client that launches `acp-go-hermes` as a
subprocess, initializes the connection, creates a session, and sends one
prompt. It embeds a real `acp.Client` so the agent can read and write files
and request permissions, and it streams the message, thought, and tool-call
updates back to stdout.

```sh
go run ./examples/minimal-client "Reply with a short hello from ACP"
```

The prompt is taken from the trailing arguments; when none are given it uses a
default hello prompt. As the turn runs, assistant text, thoughts, and tool
calls print as they stream, followed by the final stop reason. The program
exits when the turn completes. A local `hermes` CLI must be installed and
authenticated.
