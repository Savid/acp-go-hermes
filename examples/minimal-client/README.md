# Minimal Client

This example is a small ACP client that launches `acp-go-hermes` as a
subprocess, initializes the connection, creates a session, and sends one
prompt. It embeds a real `acp.Client` so the agent can read and write files
and request permissions, and it streams the assistant message text back to
stdout.

```sh
go run ./examples/minimal-client "Reply with a short hello from ACP"
```

The prompt is taken from the trailing arguments; when none are given it uses a
default hello prompt. As the turn runs, assistant text prints as it streams,
followed by the final stop reason. The program exits when the turn completes. A
local `hermes` CLI must be installed and authenticated.
