# Resume From File

This example reads a Hermes JSONL transcript from `session.jsonl` in this
directory into a `SessionStore`, loads the session through ACP so previous
interactions are replayed, then sends one no-tools smoke-test prompt. It denies
tool permissions by default so a copied session cannot silently run commands
while you are checking resume behavior.

Use it with a stored transcript:

```sh
cd examples/resume-from-file
go run . -file ./session.jsonl -session <session-id> -cwd /absolute/path/to/project
```

If the JSONL rows include a `sessionId` or `cwd` field, `-session` and `-cwd`
can be omitted and are inferred from the transcript. Loading uses normal ACP
`session/load`, and the prompt uses normal ACP `session/prompt`.

Pass `-prompt "..."` to change the smoke-test turn, and `-path` / `-home` to
point at a specific `hermes` executable and isolated home root.
