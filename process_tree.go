package hermesacp

import nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

// ErrProcessContainmentIncomplete reports that the selected native containment
// boundary did not complete. Callers must retain its quarantined resources.
var ErrProcessContainmentIncomplete = nativehermes.ErrProcessContainmentIncomplete
