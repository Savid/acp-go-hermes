package hermesacp

import nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

// ErrProcessTreeUnproven reports that the adapter could not prove every
// native descendant exited. Callers must retain resources that may still be
// reachable by the process tree.
var ErrProcessTreeUnproven = nativehermes.ErrProcessTreeUnproven
