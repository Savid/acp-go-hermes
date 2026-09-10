package hermesacp

import (
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

// captureAmbientEnvironment is the seam the adapter's own environment is read
// through. Tests select a fixed environment rather than mutating the process's.
var captureAmbientEnvironment = os.Environ

// ambientEnvironment snapshots the ambient block once, at Agent construction:
// the adapter's own environment, or the one WithAmbientEnvironment supplied in
// its place. Ordinary same-identity execution inherits a sanitized copy of it,
// so reading it once is what keeps every session and every provider-auth leg in
// one Agent launching against the same environment.
//
// The ordered block is folded into a keyed phase by the same package that
// assembles a launch environment out of it, so a name an inherited block spells
// twice is resolved by that block's own order rather than carried forward as
// two live variables. A supplied map has no order of its own, so its entries
// are folded in sorted key order.
//
// An explicit authority supplies its own complete replacement environment and
// ignores this value entirely.
func ambientEnvironment(options Options) map[string]string {
	return nativehermes.AmbientEnvironmentSnapshot(ambientEnvironmentEntries(options))
}

func ambientEnvironmentEntries(options Options) []string {
	if options.AmbientEnvironment == nil {
		return captureAmbientEnvironment()
	}

	keys := slices.Sorted(maps.Keys(options.AmbientEnvironment))
	entries := make([]string, 0, len(keys))

	for _, key := range keys {
		entries = append(entries, key+"="+options.AmbientEnvironment[key])
	}

	return entries
}

// validateAmbientEnvironment refuses a supplied block whose entries could not
// be environment entries at all. Which names the block then contributes is
// decided by the ordinary inheritance rules, never here.
func validateAmbientEnvironment(env map[string]string) error {
	for _, key := range slices.Sorted(maps.Keys(env)) {
		switch {
		case key == "" || strings.ContainsAny(key, "=\x00"):
			return fmt.Errorf("ambient environment key %q is not a variable name", key)
		case strings.ContainsRune(env[key], '\x00'):
			return fmt.Errorf("ambient environment value for %q contains NUL", key)
		}
	}

	return nil
}
