package hermes

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"unicode"
	"unicode/utf8"
)

type ProcessIdentityLockCapability interface {
	Duplicate() (*os.File, error)
}

// ProcessIsolation is the explicit hardened Linux identity boundary. A nil
// policy is not a member of this type: omission selects ordinary same-identity
// execution, which manufactures no policy value at all and reaches none of the
// authority, credential, or supervisor machinery below.
type ProcessIsolation struct {
	UID                      uint32
	GID                      uint32
	BaseEnvironment          map[string]string
	TestOnlyNoCredential     bool
	TestOnlyIdentityLockRoot string
	IdentityLock             ProcessIdentityLockCapability `json:"-"`
	AuthorityDomain          ProcessIdentityLockCapability `json:"-"`
	StandaloneOwnerID        string                        `json:"standaloneOwnerId"`
	StandaloneStateRoot      string                        `json:"standaloneStateRoot"`
}

// processRuntimePlatform is the package's single platform seam. Containment
// selection and explicit-policy validation must observe the same simulated
// platform in tests; separate copies can otherwise produce an impossible
// half-Darwin, half-Linux launch verdict.
var processRuntimePlatform = runtime.GOOS

const (
	privateSupervisorEnvPrefix = "ACP_" + "GO_HERMES_INTERNAL_"
	processSupervisorEnvPrefix = "ACP_" + "GO_HERMES_PROCESS_SUPERVISOR"
	processPlatformLinux       = "linux"
	processPlatformDarwin      = "darwin"
	processPlatformWindows     = "windows"
)

// validateProcessIsolation validates an explicit hardened policy. Ordinary
// same-identity execution never reaches here: it is selected by a nil policy at
// the launch boundary, so a nil value arriving at this point is a caller that
// failed to branch rather than a mode to accept.
func validateProcessIsolation(isolation *ProcessIsolation) error {
	if isolation == nil {
		return errors.New("process isolation policy is required")
	}

	// The platform gate runs before every other check so that an embedder
	// calling the Go API directly is refused off Linux on exactly the terms the
	// command loader refuses there.
	if err := validateProcessIsolationPlatform(); err != nil {
		return err
	}

	if isolation.UID == 0 || isolation.GID == 0 {
		return errors.New("process isolation UID and GID must be nonzero")
	}

	if isolation.BaseEnvironment == nil {
		return errors.New("process isolation base environment is required")
	}

	if !isolation.TestOnlyNoCredential || isolation.StandaloneOwnerID != "" || isolation.StandaloneStateRoot != "" {
		if err := validateStandaloneIdentityDisposition(isolation); err != nil {
			return err
		}
	}

	return validateProcessIsolationBaseEnvironment(isolation.BaseEnvironment)
}

func validateProcessIsolationBaseEnvironment(environment map[string]string) error {
	for key := range environment {
		if key == "" || strings.ContainsRune(key, '=') || strings.IndexByte(key, 0) >= 0 {
			return fmt.Errorf("process isolation base environment contains invalid key %q", key)
		}

		upper := strings.ToUpper(key)
		if strings.HasPrefix(upper, privateSupervisorEnvPrefix) || strings.HasPrefix(upper, processSupervisorEnvPrefix) {
			return fmt.Errorf("process isolation base environment contains reserved key %q", key)
		}
	}

	return nil
}

func validateStandaloneIdentityDisposition(isolation *ProcessIsolation) error {
	identityLock := isolation.IdentityLock != nil
	authorityDomain := isolation.AuthorityDomain != nil

	if identityLock != authorityDomain {
		return errors.New("process identity lock and authority domain must be provided together")
	}

	if identityLock {
		if isolation.StandaloneOwnerID != "" || isolation.StandaloneStateRoot != "" {
			return errors.New("borrowed process identity forbids standalone owner fields")
		}

		return nil
	}

	if !validStandaloneOwnerID(isolation.StandaloneOwnerID) {
		return errors.New("standalone owner id must be 1..256 valid UTF-8 bytes without whitespace or control characters")
	}

	if !validStandaloneStateRootPath(isolation.StandaloneStateRoot) {
		return errors.New("standalone state root must be a clean absolute path")
	}

	return nil
}

func validStandaloneStateRootPath(value string) bool {
	if value == "" || len(value) > 4096 || !utf8.ValidString(value) || !filepath.IsAbs(value) ||
		filepath.Clean(value) != value || value == "/" || strings.IndexByte(value, 0) >= 0 {
		return false
	}

	const authorityRoot = "/var/lib/acp-go/agent-identities"

	if value == authorityRoot || strings.HasPrefix(value, authorityRoot+string(filepath.Separator)) {
		return false
	}

	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}

	return true
}

func validStandaloneOwnerID(value string) bool {
	if value == "" || len(value) > 256 {
		return false
	}

	letterOrDigit := func(value byte) bool {
		return value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z' || value >= '0' && value <= '9'
	}
	if !letterOrDigit(value[0]) {
		return false
	}

	for _, character := range []byte(value[1:]) {
		if letterOrDigit(character) || strings.ContainsRune("._:@/-", rune(character)) {
			continue
		}

		return false
	}

	return true
}

func isolationEnvironment(isolation *ProcessIsolation, overlays ...map[string]string) ([]string, error) {
	if err := validateProcessIsolation(isolation); err != nil {
		return nil, err
	}

	// No phase folding here: a closed policy is Linux-only, and Linux names
	// environment variables exactly. The folding rule belongs to ordinary
	// execution, which is the only mode a Windows host ever reaches.
	env := make(map[string]string, len(isolation.BaseEnvironment))
	for key, value := range isolation.BaseEnvironment {
		if strings.EqualFold(key, envHermesAuthHome) {
			continue
		}

		env[key] = value
	}

	for _, overlay := range overlays {
		for key, value := range overlay {
			if key == "" || strings.ContainsRune(key, '=') || strings.IndexByte(key, 0) >= 0 {
				return nil, fmt.Errorf("process environment contains invalid key %q", key)
			}

			if strings.EqualFold(key, envHermesAuthHome) {
				continue
			}

			env[key] = value
		}
	}

	return sortedProcessEnvironment(env), nil
}

// envValue reads a name out of a closed policy environment, where the policy
// author wrote every key and an exact match is the whole rule.
func envValue(env []string, name string) string {
	return envValueFold(env, name, false)
}

// envValueFold reads a name out of an environment block that may not have been
// written by this adapter. Folding is what an inherited Windows environment
// requires: it spells the search path "Path", and an exact compare against
// "PATH" would report an empty one rather than the operator's real search path.
//
// The last match wins, because that is the value the child actually receives:
// os/exec deduplicates a launch environment before spawning it and keeps the
// final value for each name, case-insensitively on Windows. Reading the first
// match instead would let this adapter resolve an executable against a search
// path, or a PATHEXT, that the harness process never sees.
func envValueFold(env []string, name string, fold bool) string {
	matched := ""

	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}

		if key == name || (fold && strings.EqualFold(key, name)) {
			matched = value
		}
	}

	return matched
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

// executableFile is the closed-policy candidate check. A policy launch is
// Linux-only, so the execute bit is always the rule here regardless of which
// platform this file happens to be compiled for.
func executableFile(path string) (string, error) {
	return matchExecutableFile(path, unixExecutableRules())
}
