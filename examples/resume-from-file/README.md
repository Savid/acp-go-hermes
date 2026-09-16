# Resume From File

Embeds the agent in-process with a session store persisted to a JSON file.
The first run creates a session and prints its id; a later run resumes it.

```sh
go run ./examples/resume-from-file "Remember the word pelican."
go run ./examples/resume-from-file -session <id> "What word did I ask you to remember?"
```

The store file is an example implementation of `acpcore.SessionStore`, not a
durable store. hermes's own session file is left in hermes's home either way.
