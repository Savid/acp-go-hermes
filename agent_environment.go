package hermesacp

import (
	"os"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

// captureAmbientEnvironment is the seam the adapter's own environment is read
// through. Tests select a fixed environment rather than mutating the process's.
var captureAmbientEnvironment = os.Environ

// ambientEnvironment snapshots the adapter's environment once, at Agent
// construction. Ordinary same-identity execution inherits a sanitized copy of
// it, so reading it once is what keeps every session and every provider-auth
// leg in one Agent launching against the same environment: a later os.Environ
// call would let a mutation between two sessions change what the second one
// inherits.
//
// The ordered block is folded into a keyed phase by the same package that
// assembles a launch environment out of it, so a name an inherited block spells
// twice is resolved by that block's own order rather than carried forward as
// two live variables.
//
// An explicit authority supplies its own complete replacement environment and
// ignores this value entirely.
func ambientEnvironment() map[string]string {
	return nativehermes.AmbientEnvironmentSnapshot(captureAmbientEnvironment())
}
