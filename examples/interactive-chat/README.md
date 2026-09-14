# Interactive Chat

A line-oriented chat over an embedded agent. Each line you type is one
prompt; the answer streams back; an empty line or EOF closes the session.
Permission requests are answered from the terminal.

```sh
go run ./examples/interactive-chat -model anthropic/claude-sonnet-4-20250514
```
