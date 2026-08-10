package hermesacp

import "os"

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
// This snapshot is not a ProcessIsolation and never becomes one. An explicit
// policy supplies its own complete replacement environment and ignores this
// value entirely.
func ambientEnvironment() map[string]string {
	entries := captureAmbientEnvironment()

	environment := make(map[string]string, len(entries))

	for _, entry := range entries {
		key, value, ok := splitEnvironmentEntry(entry)
		if !ok {
			continue
		}

		environment[key] = value
	}

	return environment
}

func splitEnvironmentEntry(entry string) (string, string, bool) {
	for index := 0; index < len(entry); index++ {
		if entry[index] == '=' {
			// A leading '=' names no variable; Windows uses that spelling for
			// per-drive working directories, which the harness never reads.
			if index == 0 {
				return "", "", false
			}

			return entry[:index], entry[index+1:], true
		}
	}

	return "", "", false
}
