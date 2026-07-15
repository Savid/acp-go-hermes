# Resume From File

This example reads a Hermes `hermes-state-db-v1` transcript from `session.jsonl`
in this directory into a `SessionStore`, loads the session through ACP so
previous interactions are replayed, then sends one no-tools smoke-test prompt
in-process. It denies tool permissions by default so a copied session cannot
silently run commands while you are checking resume behavior.

Use it with the shipped fixture or a stored transcript of your own:

```sh
cd examples/resume-from-file
go run . -session <session-id> -cwd /absolute/path/to/project
```

If the JSONL rows include a `sessionId` or `cwd` field, `-session` and `-cwd`
can be omitted and are inferred from the transcript. Loading uses normal ACP
`session/load`, and the prompt uses normal ACP `session/prompt`.

Pass `-prompt "..."` to change the smoke-test turn, `-path` to point at a
specific `hermes` CLI, and `-scratch-dir` to set the parent root for isolated
Hermes session state.
