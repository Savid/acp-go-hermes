//go:build linux

package main

import (
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

const maxProcessIsolationConfigSize = 1 << 20

var (
	processIsolationGeteuid       = os.Geteuid
	processIsolationLookupEnv     = os.LookupEnv
	processIsolationLookupUserID  = user.LookupId
	processIsolationLookupGroupID = user.LookupGroupId
	processIsolationGroupIDs      = func(account *user.User) ([]string, error) { return account.GroupIds() }
	processIsolationValidateHome  = validateTargetHome
	processIsolationValidatePath  = validatePath
)

func loadProcessIsolationConfig(path string) (processIsolationConfig, error) {
	if path == "" {
		return processIsolationConfig{}, fmt.Errorf("-%s is required", processIsolationConfigFlag)
	}
	if !filepath.IsAbs(path) {
		return processIsolationConfig{}, fmt.Errorf("-%s must be an absolute path", processIsolationConfigFlag)
	}
	if filepath.Clean(path) != path {
		return processIsolationConfig{}, fmt.Errorf("-%s must be a canonical absolute path", processIsolationConfigFlag)
	}
	if processIsolationGeteuid() != 0 {
		return processIsolationConfig{}, fmt.Errorf("standalone native mode requires a root supervisor")
	}

	fd, stat, err := openProtectedAbsolutePath(path, unix.O_RDONLY)
	if err != nil {
		return processIsolationConfig{}, fmt.Errorf("open -%s: %w", processIsolationConfigFlag, err)
	}

	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)

		return processIsolationConfig{}, fmt.Errorf("open -%s: invalid file descriptor", processIsolationConfigFlag)
	}
	defer file.Close()

	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0o777 != 0o600 || stat.Uid != 0 || stat.Nlink != 1 {
		return processIsolationConfig{}, fmt.Errorf("-%s must be a root-owned, single-link regular file with mode 0600", processIsolationConfigFlag)
	}
	if stat.Size > maxProcessIsolationConfigSize {
		return processIsolationConfig{}, fmt.Errorf("-%s exceeds %d bytes", processIsolationConfigFlag, maxProcessIsolationConfigSize)
	}

	data, err := io.ReadAll(io.LimitReader(file, maxProcessIsolationConfigSize+1))
	if err != nil {
		return processIsolationConfig{}, fmt.Errorf("read -%s: %w", processIsolationConfigFlag, err)
	}
	if len(data) > maxProcessIsolationConfigSize {
		return processIsolationConfig{}, fmt.Errorf("-%s exceeds %d bytes", processIsolationConfigFlag, maxProcessIsolationConfigSize)
	}

	config, err := decodeProcessIsolationConfig(data)
	if err != nil {
		return processIsolationConfig{}, err
	}

	return validateProcessIsolationConfig(config)
}

func validateProcessIsolationConfig(config processIsolationConfig) (processIsolationConfig, error) {
	if config.UID == 0 || config.GID == 0 {
		return processIsolationConfig{}, fmt.Errorf("uid and gid must be nonzero")
	}
	if config.BaseEnvironment == nil {
		return processIsolationConfig{}, fmt.Errorf("baseEnvironment must be a JSON object")
	}

	account, err := processIsolationLookupUserID(strconv.FormatUint(uint64(config.UID), 10))
	if err != nil {
		return processIsolationConfig{}, fmt.Errorf("lookup uid %d: %w", config.UID, err)
	}
	accountGID, err := strconv.ParseUint(account.Gid, 10, 32)
	if err != nil || uint32(accountGID) != config.GID {
		return processIsolationConfig{}, fmt.Errorf("gid %d is not uid %d's primary group", config.GID, config.UID)
	}
	primaryGroup, err := processIsolationLookupGroupID(strconv.FormatUint(uint64(config.GID), 10))
	if err != nil {
		return processIsolationConfig{}, fmt.Errorf("lookup gid %d: %w", config.GID, err)
	}
	if primaryGroup.Name != account.Username {
		return processIsolationConfig{}, fmt.Errorf("gid %d must be the same-name private group %q", config.GID, account.Username)
	}
	groupIDs, err := processIsolationGroupIDs(account)
	if err != nil {
		return processIsolationConfig{}, fmt.Errorf("lookup supplementary groups for uid %d: %w", config.UID, err)
	}
	for _, groupID := range groupIDs {
		if groupID != account.Gid {
			return processIsolationConfig{}, fmt.Errorf("uid %d must not belong to supplementary group %s", config.UID, groupID)
		}
	}
	if err := processIsolationValidateAccountAuthority(account, config.UID, config.GID); err != nil {
		return processIsolationConfig{}, err
	}

	finalEnvironment := make(map[string]string, len(config.BaseEnvironment)+len(config.InheritEnvironment))
	for name, value := range config.BaseEnvironment {
		if err := validateEnvironmentEntry(name, value); err != nil {
			return processIsolationConfig{}, err
		}
		if prohibitedPolicyEnvironment(name) {
			return processIsolationConfig{}, fmt.Errorf("baseEnvironment variable %q is reserved or unsafe", name)
		}
		finalEnvironment[name] = value
	}

	seenInherited := make(map[string]struct{}, len(config.InheritEnvironment))
	for _, name := range config.InheritEnvironment {
		if err := validateEnvironmentName(name); err != nil {
			return processIsolationConfig{}, fmt.Errorf("inheritEnvironment: %w", err)
		}
		if prohibitedInheritedEnvironment(name) {
			return processIsolationConfig{}, fmt.Errorf("inheritEnvironment variable %q is reserved or unsafe", name)
		}
		if _, exists := seenInherited[name]; exists {
			return processIsolationConfig{}, fmt.Errorf("inheritEnvironment variable %q is duplicated", name)
		}
		seenInherited[name] = struct{}{}
		if _, exists := finalEnvironment[name]; exists {
			return processIsolationConfig{}, fmt.Errorf("environment variable %q appears in both baseEnvironment and inheritEnvironment", name)
		}
		value, exists := processIsolationLookupEnv(name)
		if !exists {
			return processIsolationConfig{}, fmt.Errorf("inheritEnvironment variable %q is unset", name)
		}
		if strings.IndexByte(value, 0) >= 0 {
			return processIsolationConfig{}, fmt.Errorf("inheritEnvironment variable %q contains NUL", name)
		}
		finalEnvironment[name] = value
	}

	if finalEnvironment["USER"] != account.Username || finalEnvironment["LOGNAME"] != account.Username {
		return processIsolationConfig{}, fmt.Errorf("USER and LOGNAME must both equal account name %q", account.Username)
	}
	if finalEnvironment["HOME"] != filepath.Clean(account.HomeDir) || !filepath.IsAbs(finalEnvironment["HOME"]) {
		return processIsolationConfig{}, fmt.Errorf("HOME must equal account home %q", filepath.Clean(account.HomeDir))
	}
	if err := processIsolationValidateHome(finalEnvironment["HOME"], config.UID, config.GID); err != nil {
		return processIsolationConfig{}, err
	}
	if err := processIsolationValidatePath(finalEnvironment["PATH"]); err != nil {
		return processIsolationConfig{}, err
	}

	config.BaseEnvironment = finalEnvironment
	config.InheritEnvironment = nil

	return config, nil
}

func validateTargetHome(path string, uid uint32, gid uint32) error {
	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(path) || cleaned != path || path == "/root" || path == "/nonexistent" {
		return fmt.Errorf("HOME %q must be a canonical private account home, not /root or /nonexistent", path)
	}

	fd, stat, err := openProtectedAbsolutePath(path, unix.O_RDONLY|unix.O_DIRECTORY)
	if err != nil {
		return fmt.Errorf("open HOME %q: %w", path, err)
	}
	defer unix.Close(fd)

	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Mode&0o777 != 0o700 || stat.Uid != uid || stat.Gid != gid {
		return fmt.Errorf("HOME %q must be a uid %d, gid %d directory with mode 0700", path, uid, gid)
	}

	return nil
}

// openProtectedAbsolutePath resolves path from an open root directory. Every
// component is opened relative to the preceding descriptor with O_NOFOLLOW,
// and every ancestor must be a root-owned directory that is not writable by
// group or other. The caller receives the still-open final descriptor and the
// metadata from that descriptor, so validation and policy reads refer to the
// same inode.
func openProtectedAbsolutePath(path string, finalFlags int) (int, *unix.Stat_t, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" {
		return -1, nil, fmt.Errorf("path must be canonical, absolute, and non-root")
	}

	components := strings.Split(strings.TrimPrefix(path, "/"), "/")
	parentFD, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, nil, err
	}
	defer func() {
		if parentFD >= 0 {
			_ = unix.Close(parentFD)
		}
	}()

	if err := validateProtectedAncestor(parentFD, "/"); err != nil {
		return -1, nil, err
	}

	for index, component := range components {
		last := index == len(components)-1
		flags := unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW
		if last {
			flags |= finalFlags
		} else {
			flags |= unix.O_DIRECTORY
		}

		childFD, openErr := unix.Openat(parentFD, component, flags, 0)
		if openErr != nil {
			return -1, nil, fmt.Errorf("open component %q: %w", component, openErr)
		}

		var stat unix.Stat_t
		if statErr := unix.Fstat(childFD, &stat); statErr != nil {
			_ = unix.Close(childFD)

			return -1, nil, fmt.Errorf("stat component %q: %w", component, statErr)
		}
		if last {
			return childFD, &stat, nil
		}
		if err := validateProtectedAncestorStat(&stat, component); err != nil {
			_ = unix.Close(childFD)

			return -1, nil, err
		}

		_ = unix.Close(parentFD)
		parentFD = childFD
	}

	return -1, nil, fmt.Errorf("path has no final component")
}

func validateProtectedAncestor(fd int, component string) error {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return fmt.Errorf("stat ancestor %q: %w", component, err)
	}

	return validateProtectedAncestorStat(&stat, component)
}

func validateProtectedAncestorStat(stat *unix.Stat_t, component string) error {
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Uid != 0 || stat.Mode&0o022 != 0 {
		return fmt.Errorf("ancestor %q must be a root-owned directory not writable by group or other", component)
	}

	return nil
}

func validatePath(value string) error {
	if value == "" {
		return fmt.Errorf("baseEnvironment PATH must be nonempty")
	}
	for _, component := range filepath.SplitList(value) {
		if component == "" || !filepath.IsAbs(component) || filepath.Clean(component) != component {
			return fmt.Errorf("baseEnvironment PATH components must be canonical absolute paths")
		}
		info, err := os.Stat(component)
		if err != nil {
			return fmt.Errorf("stat PATH component %q: %w", component, err)
		}
		if !info.IsDir() {
			return fmt.Errorf("PATH component %q is not a directory", component)
		}
	}

	return nil
}

func validateEnvironmentEntry(name string, value string) error {
	if err := validateEnvironmentName(name); err != nil {
		return fmt.Errorf("baseEnvironment: %w", err)
	}
	if strings.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("baseEnvironment variable %q contains NUL", name)
	}

	return nil
}

func validateEnvironmentName(name string) error {
	if name == "" || strings.ContainsAny(name, "=\x00") {
		return fmt.Errorf("invalid environment variable name %q", name)
	}
	for index, char := range name {
		if (char < 'A' || char > 'Z') && (char < 'a' || char > 'z') && char != '_' && (index == 0 || char < '0' || char > '9') {
			return fmt.Errorf("invalid environment variable name %q", name)
		}
	}

	return nil
}

func prohibitedPolicyEnvironment(name string) bool {
	if strings.HasPrefix(name, "ACP_GO_") || strings.HasPrefix(name, "LD_") || strings.HasPrefix(name, "DYLD_") {
		return true
	}
	switch name {
	case "BASH_ENV", "ENV", "SHELLOPTS", "BASHOPTS",
		"NODE_OPTIONS", "NODE_PATH",
		"PYTHONHOME", "PYTHONPATH", "PYTHONSTARTUP", "PYTHONINSPECT", "PYTHONBREAKPOINT",
		"RUBYOPT", "RUBYLIB", "PERL5OPT", "PERL5LIB",
		"GODEBUG", "GOENV", "GOTRACEBACK",
		"JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS":
		return true
	default:
		return false
	}
}

func prohibitedInheritedEnvironment(name string) bool {
	if prohibitedPolicyEnvironment(name) {
		return true
	}
	switch name {
	case "PATH", "HOME", "USER", "LOGNAME", "SHELL", "TMPDIR", "CDPATH", "GLOBIGNORE", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME", "CLAUDE_CONFIG_DIR", "CODEX_HOME", "PI_CODING_AGENT_DIR", "HERMES_HOME":
		return true
	default:
		return false
	}
}
