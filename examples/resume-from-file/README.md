# Resume From File

This example reads a Hermes `hermes-state-db-v1` transcript from `session.jsonl`
in this directory into a `SessionStore`, loads the session through ACP, then
sends one no-tools smoke-test prompt in-process. It denies tool permissions by
default so a copied session cannot silently run commands while you are checking
resume behavior.

`session/load` replays every turn the transcript's native archive holds. The
shipped fixture is a real captured session that was never prompted, so it
replays nothing and only the smoke-test turn prints; point `-file` at a
transcript of your own to watch its history replay first.

Use it with the shipped fixture or a stored transcript of your own:

```sh
cd examples/resume-from-file
go run . -session <session-id> -cwd /absolute/path/to/project
```

Each JSONL row is one `hermes-state-db-v1` store entry, and rows are routed to
the store key their shape names: the main snapshot, the id mapping, and the
native-archive chunks. Writing them all under one key would leave the session
unreadable, because `session/load` reads those keys independently. The archive
chunks carry the session's own Hermes `state.db`, which is what lets a fresh
machine resume the native session the snapshot names.

`-session` and `-cwd` are inferred from the transcript where it names them, and
`-cwd` falls back to the current directory. The shipped fixture binds no `cwd`,
because `session/load` refuses a snapshot whose `cwd` disagrees with the request
and no shipped path exists on every machine. Loading uses normal ACP
`session/load`, and the prompt uses normal ACP `session/prompt`.

Pass `-prompt "..."` to change the smoke-test turn, `-path` to point at a
specific `hermes` CLI, and `-scratch-dir` to set the parent root for isolated
Hermes session state.
