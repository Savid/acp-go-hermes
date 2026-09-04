package hermes

import (
	"os"
	"sort"
	"strconv"
)

// fakeLauncherOwnerPIDEnv names the test process a launched fake belongs to.
// The fake reaps itself when that process is gone, and the name it is reached
// by has to survive the launcher, so it travels in the environment.
const fakeLauncherOwnerPIDEnv = "ACP_GO_HERMES_TEST_OWNER_PID"

// launcherOwnerEnv is the owner marker every launcher writes into the fake's
// environment alongside whatever the caller asked for.
func launcherOwnerEnv(env map[string]string) map[string]string {
	owned := make(map[string]string, len(env)+1)
	for key, value := range env {
		owned[key] = value
	}

	owned[fakeLauncherOwnerPIDEnv] = strconv.Itoa(os.Getpid())

	return owned
}

// sortedLauncherEnvKeys orders a launcher's environment so the script it writes
// is byte-identical from one run to the next.
func sortedLauncherEnvKeys(env map[string]string) []string {
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	return keys
}
