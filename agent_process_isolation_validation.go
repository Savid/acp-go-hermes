package hermesacp

import (
	"errors"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"
)

// validateProcessIsolationOption validates a supplied hardened policy. It is
// never reached for an omitted one: omission selects ordinary same-identity
// execution, which has no policy to validate and is not a configuration error.
//
// Every check here fails closed. A policy that cannot be honoured on this
// platform, or that does not describe a complete authority disposition, refuses
// the session rather than falling back to ordinary or best-effort execution.
func validateProcessIsolationOption(isolation *ProcessIsolation) error {
	if isolation == nil {
		return errors.New("process isolation policy is required")
	}

	// The hardened boundary is built from Linux-only primitives, so an explicit
	// policy is refused everywhere else — including through the embedded Go API,
	// not merely through the command's policy loader.
	if agentRuntimePlatform != agentRuntimeLinux {
		return errors.New("explicit process isolation is supported only on linux, not " + agentRuntimePlatform)
	}

	if isolation.UID == 0 || isolation.GID == 0 {
		return errors.New("process isolation UID and GID must be nonzero")
	}

	return validateStandaloneIdentityOption(
		isolation.IdentityLock != nil, isolation.AuthorityDomain != nil,
		isolation.StandaloneOwnerID, isolation.StandaloneStateRoot,
	)
}

func validateStandaloneIdentityOption(identityLock, authorityDomain bool, ownerID, stateRoot string) error {
	if identityLock != authorityDomain {
		return errors.New("process identity lock and authority domain must be provided together")
	}

	if identityLock {
		if ownerID != "" || stateRoot != "" {
			return errors.New("borrowed process identity forbids standalone owner fields")
		}

		return nil
	}

	if !validStandaloneOwnerID(ownerID) {
		return errors.New("standalone owner id must be 1..256 valid UTF-8 bytes without whitespace or control characters")
	}

	if !validStandaloneStateRootPath(stateRoot) {
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
