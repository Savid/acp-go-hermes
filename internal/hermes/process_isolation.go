package hermes

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

type ProcessIsolation struct {
	UID                  uint32
	GID                  uint32
	BaseEnvironment      map[string]string
	TestOnlyNoCredential bool
}

const (
	envIsolationUID  = "ACP_GO_HERMES_INTERNAL_ISOLATION_UID"
	envIsolationGID  = "ACP_GO_HERMES_INTERNAL_ISOLATION_GID"
	envIsolationTest = "ACP_GO_HERMES_INTERNAL_ISOLATION_TEST_ONLY"
)

func validateProcessIsolation(isolation *ProcessIsolation) error {
	if isolation == nil {
		return errors.New("process isolation policy is required")
	}

	if isolation.UID == 0 || isolation.GID == 0 {
		return errors.New("process isolation UID and GID must be nonzero")
	}

	for key := range isolation.BaseEnvironment {
		if key == "" || strings.ContainsRune(key, '=') || strings.IndexByte(key, 0) >= 0 {
			return fmt.Errorf("process isolation base environment contains invalid key %q", key)
		}

		upper := strings.ToUpper(key)
		if strings.HasPrefix(upper, "ACP_GO_HERMES_INTERNAL_") || strings.HasPrefix(upper, "ACP_GO_HERMES_PROCESS_SUPERVISOR") {
			return fmt.Errorf("process isolation base environment contains reserved key %q", key)
		}
	}

	return validateProcessIsolationPlatform()
}

func isolationEnvironment(isolation *ProcessIsolation, overlays ...map[string]string) ([]string, error) {
	if err := validateProcessIsolation(isolation); err != nil {
		return nil, err
	}

	env := make(map[string]string, len(isolation.BaseEnvironment))
	for key, value := range isolation.BaseEnvironment {
		env[key] = value
	}

	for _, overlay := range overlays {
		for key, value := range overlay {
			if key == "" || strings.ContainsRune(key, '=') || strings.IndexByte(key, 0) >= 0 {
				return nil, fmt.Errorf("process environment contains invalid key %q", key)
			}

			env[key] = value
		}
	}

	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, key+"="+env[key])
	}

	return out, nil
}

func envValue(env []string, name string) string {
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if ok && key == name {
			return value
		}
	}

	return ""
}

func lookPathInEnvironment(file string, environment []string) (string, error) {
	if file == "" {
		return "", errors.New("executable name is empty")
	}

	if strings.ContainsRune(file, os.PathSeparator) {
		if !filepath.IsAbs(file) {
			return "", fmt.Errorf("executable path %q is not absolute", file)
		}

		return executableFile(file)
	}

	search := envValue(environment, "PATH")
	if search == "" {
		return "", fmt.Errorf("executable %q cannot be resolved without policy PATH", file)
	}

	for _, dir := range filepath.SplitList(search) {
		if !filepath.IsAbs(dir) {
			return "", fmt.Errorf("policy PATH entry %q is not absolute", dir)
		}

		if path, err := executableFile(filepath.Join(dir, file)); err == nil {
			return path, nil
		}
	}

	return "", fmt.Errorf("executable %q not found in policy PATH", file)
}

func executableFile(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}

	if !info.Mode().IsRegular() || info.Mode()&0o111 == 0 {
		return "", fmt.Errorf("%q is not executable", path)
	}

	return path, nil
}

func supervisorEnvironment(native []string, isolation *ProcessIsolation, additions ...string) ([]string, error) {
	if err := validateProcessIsolation(isolation); err != nil {
		return nil, err
	}

	env := make([]string, 0, len(native)+len(additions)+3)
	reserved := map[string]struct{}{envIsolationUID: {}, envIsolationGID: {}, envIsolationTest: {}}

	for _, addition := range additions {
		name, _, ok := strings.Cut(addition, "=")
		if ok {
			reserved[name] = struct{}{}
		}
	}

	for _, entry := range native {
		name, _, ok := strings.Cut(entry, "=")
		_, isReserved := reserved[name]

		if ok && !isReserved {
			env = append(env, entry)
		}
	}

	env = append(env, additions...)

	return append(env,
		envIsolationUID+"="+strconv.FormatUint(uint64(isolation.UID), 10),
		envIsolationGID+"="+strconv.FormatUint(uint64(isolation.GID), 10),
		envIsolationTest+"="+strconv.FormatBool(isolation.TestOnlyNoCredential),
	), nil
}

func verifyInheritedProcessIsolation() error {
	uid, uidErr := strconv.ParseUint(os.Getenv(envIsolationUID), 10, 32)
	gid, gidErr := strconv.ParseUint(os.Getenv(envIsolationGID), 10, 32)

	if uidErr != nil || gidErr != nil {
		return errors.New("process isolation bootstrap identity is invalid")
	}

	if os.Getenv(envIsolationTest) == "true" {
		return nil
	}

	return verifyProcessIsolation(&ProcessIsolation{UID: uint32(uid), GID: uint32(gid), BaseEnvironment: map[string]string{}})
}
