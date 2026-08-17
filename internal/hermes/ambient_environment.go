package hermes

import "strings"

// The adapter's own environment is the first phase of an ordinary launch. It
// arrives as the ordered block the platform handed this process, and that order
// is the one thing the block carries which a keyed phase map cannot. Folding it
// here is what keeps the decision on the seam that still has the information.

// AmbientEnvironmentSnapshot folds an ordered environment block — os.Environ(),
// or any block a parent hand-built for CreateProcess — into the keyed ambient
// phase ProcessOptions.AmbientEnvironment is assembled from.
//
// Where names fold, a later "PATH" is not a second variable beside an earlier
// "Path"; it is one variable written twice, and the block's own order says
// which write is current. The last one wins, in spelling as well as value,
// which is exactly how os/exec resolves a block before handing it to
// CreateProcess. Deciding it here is also what keeps the intra-phase refusal in
// mergeProcessEnvironmentPhases scoped to what it can adjudicate: two spellings
// a caller wrote into one unordered map, which have no order to read. An
// inherited block always has one, so it is never the thing that refuses a
// launch.
func AmbientEnvironmentSnapshot(entries []string) map[string]string {
	folds := processEnvironmentKeysFold()
	environment := make(map[string]string, len(entries))

	for _, entry := range entries {
		key, value, ok := splitEnvironmentEntry(entry)
		if !ok {
			continue
		}

		if folds {
			for existing := range environment {
				if existing != key && processEnvironmentKeyMatches(existing, key) {
					delete(environment, existing)
				}
			}
		}

		environment[key] = value
	}

	return environment
}

// splitEnvironmentEntry splits one KEY=VALUE entry of an environment block. An
// entry carrying no '=' names no variable, and neither does one that starts
// with '=': Windows spells per-drive working directories that way, and the
// harness never reads one.
func splitEnvironmentEntry(entry string) (string, string, bool) {
	key, value, ok := strings.Cut(entry, "=")
	if !ok || key == "" {
		return "", "", false
	}

	return key, value, true
}
