//go:build linux

package main

import (
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// isolationConfigCovRequireRoot skips a test whose property depends on the
// whole ancestor chain from the filesystem root being root-owned, which is
// only true when the suite runs as root.
func isolationConfigCovRequireRoot(t *testing.T) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("protected-path resolution requires a root-owned temporary ancestor chain")
	}
}

// isolationConfigCovSeams captures every process-isolation seam this file
// rewrites and restores the production values when the test ends.
func isolationConfigCovSeams(t *testing.T) {
	t.Helper()
	geteuid := processIsolationGeteuid
	lookupEnv := processIsolationLookupEnv
	lookupUser := processIsolationLookupUserID
	lookupGroup := processIsolationLookupGroupID
	groupIDs := processIsolationGroupIDs
	validateHome := processIsolationValidateHome
	validatePathSeam := processIsolationValidatePath
	validateAccount := processIsolationValidateAccountAuthority
	openSeam := processIsolationOpen
	fstatSeam := processIsolationFstat
	t.Cleanup(func() {
		processIsolationGeteuid = geteuid
		processIsolationLookupEnv = lookupEnv
		processIsolationLookupUserID = lookupUser
		processIsolationLookupGroupID = lookupGroup
		processIsolationGroupIDs = groupIDs
		processIsolationValidateHome = validateHome
		processIsolationValidatePath = validatePathSeam
		processIsolationValidateAccountAuthority = validateAccount
		processIsolationOpen = openSeam
		processIsolationFstat = fstatSeam
	})
}

// isolationConfigCovStubAccount installs an account directory that resolves
// uid 20001 to a private, correctly grouped account, so that each test can
// break exactly one property and attribute the refusal to it.
func isolationConfigCovStubAccount(t *testing.T) {
	t.Helper()
	isolationConfigCovSeams(t)
	processIsolationGeteuid = func() int { return 0 }
	processIsolationLookupUserID = func(string) (*user.User, error) {
		return &user.User{Uid: "20001", Gid: "20002", Username: "acp-cov", HomeDir: "/var/lib/acp-cov"}, nil
	}
	processIsolationLookupGroupID = func(string) (*user.Group, error) {
		return &user.Group{Gid: "20002", Name: "acp-cov"}, nil
	}
	processIsolationGroupIDs = func(*user.User) ([]string, error) { return []string{"20002"}, nil }
	processIsolationLookupEnv = func(name string) (string, bool) {
		if name == "OPENAI_API_KEY" {
			return "explicit-secret", true
		}

		return "", false
	}
	processIsolationValidateHome = func(string, uint32, uint32) error { return nil }
	processIsolationValidatePath = func(string) error { return nil }
	processIsolationValidateAccountAuthority = func(*user.User, uint32, uint32) error { return nil }
}

const isolationConfigCovPolicyJSON = `{"uid":20001,"gid":20002,` +
	`"baseEnvironment":{"PATH":"/usr/bin","HOME":"/var/lib/acp-cov","USER":"acp-cov","LOGNAME":"acp-cov"},` +
	`"inheritEnvironment":["OPENAI_API_KEY"],` +
	`"standaloneOwnerId":"cov-owner","standaloneStateRoot":"/var/lib/acp-cov"}`

// isolationConfigCovConfig decodes the shared trusted policy so every case in
// this file starts from exactly the bytes the supervisor would have read.
func isolationConfigCovConfig(t *testing.T) processIsolationConfig {
	t.Helper()
	config, err := decodeProcessIsolationConfig([]byte(isolationConfigCovPolicyJSON))
	if err != nil {
		t.Fatal(err)
	}
	config.InheritEnvironment = nil

	return config
}

// TestValidateProcessIsolationConfigRefusesEveryUnprovenField proves that the
// isolation policy is only accepted when every identity, account and
// environment fact it asserts is independently re-derived, and that each
// individual violation produces its own refusal instead of being absorbed.
func TestValidateProcessIsolationConfigRefusesEveryUnprovenField(t *testing.T) {
	authorityFailure := errors.New("account authority unproven")
	for name, testCase := range map[string]struct {
		seams     func()
		mutate    func(*processIsolationConfig)
		wantError string
	}{
		"absent base environment": {
			mutate:    func(config *processIsolationConfig) { config.BaseEnvironment = nil },
			wantError: "baseEnvironment must be a JSON object",
		},
		"empty owner id": {
			mutate:    func(config *processIsolationConfig) { config.StandaloneOwnerID = "" },
			wantError: "standaloneOwnerId is invalid",
		},
		"owner id with punctuation lead": {
			mutate:    func(config *processIsolationConfig) { config.StandaloneOwnerID = "-cov" },
			wantError: "standaloneOwnerId is invalid",
		},
		"owner id with a space": {
			mutate:    func(config *processIsolationConfig) { config.StandaloneOwnerID = "cov owner" },
			wantError: "standaloneOwnerId is invalid",
		},
		"relative state root": {
			mutate:    func(config *processIsolationConfig) { config.StandaloneStateRoot = "var/lib/acp-cov" },
			wantError: "standaloneStateRoot must be a clean absolute UTF-8 path",
		},
		"state root with a control character": {
			mutate:    func(config *processIsolationConfig) { config.StandaloneStateRoot = "/var/lib/acp\tcov" },
			wantError: "standaloneStateRoot must be a clean absolute UTF-8 path",
		},
		"unknown uid": {
			seams: func() {
				processIsolationLookupUserID = func(string) (*user.User, error) {
					return nil, user.UnknownUserIdError(20001)
				}
			},
			wantError: "lookup uid 20001",
		},
		"gid is not the account primary group": {
			seams: func() {
				processIsolationLookupUserID = func(string) (*user.User, error) {
					return &user.User{Uid: "20001", Gid: "30002", Username: "acp-cov", HomeDir: "/var/lib/acp-cov"}, nil
				}
			},
			wantError: "gid 20002 is not uid 20001's primary group",
		},
		"unknown gid": {
			seams: func() {
				processIsolationLookupGroupID = func(string) (*user.Group, error) {
					return nil, user.UnknownGroupIdError("20002")
				}
			},
			wantError: "lookup gid 20002",
		},
		"supplementary group lookup fails": {
			seams: func() {
				processIsolationGroupIDs = func(*user.User) ([]string, error) {
					return nil, errors.New("group database unavailable")
				}
			},
			wantError: "lookup supplementary groups for uid 20001",
		},
		"extra supplementary group": {
			seams: func() {
				processIsolationGroupIDs = func(*user.User) ([]string, error) {
					return []string{"20002", "27"}, nil
				}
			},
			wantError: "uid 20001 must not belong to supplementary group 27",
		},
		"account authority unproven": {
			seams: func() {
				processIsolationValidateAccountAuthority = func(*user.User, uint32, uint32) error {
					return authorityFailure
				}
			},
			wantError: authorityFailure.Error(),
		},
		"base environment name is not an identifier": {
			mutate:    func(config *processIsolationConfig) { config.BaseEnvironment["1BAD"] = "value" },
			wantError: `baseEnvironment: invalid environment variable name "1BAD"`,
		},
		"base environment value carries NUL": {
			mutate:    func(config *processIsolationConfig) { config.BaseEnvironment["TERM"] = "xterm\x00extra" },
			wantError: `baseEnvironment variable "TERM" contains NUL`,
		},
		"base environment shell hook": {
			mutate:    func(config *processIsolationConfig) { config.BaseEnvironment["BASH_ENV"] = "/tmp/hook.sh" },
			wantError: `baseEnvironment variable "BASH_ENV" is reserved or unsafe`,
		},
		"inherited name is not an identifier": {
			mutate:    func(config *processIsolationConfig) { config.InheritEnvironment = []string{"BAD-KEY"} },
			wantError: `inheritEnvironment: invalid environment variable name "BAD-KEY"`,
		},
		"inherited loader override": {
			mutate:    func(config *processIsolationConfig) { config.InheritEnvironment = []string{"LD_PRELOAD"} },
			wantError: `inheritEnvironment variable "LD_PRELOAD" is reserved or unsafe`,
		},
		"inherited name also declared in base": {
			mutate: func(config *processIsolationConfig) {
				config.BaseEnvironment["OPENAI_API_KEY"] = "policy-secret"
				config.InheritEnvironment = []string{"OPENAI_API_KEY"}
			},
			wantError: `environment variable "OPENAI_API_KEY" appears in both baseEnvironment and inheritEnvironment`,
		},
		"inherited name is unset": {
			mutate:    func(config *processIsolationConfig) { config.InheritEnvironment = []string{"ANTHROPIC_API_KEY"} },
			wantError: `inheritEnvironment variable "ANTHROPIC_API_KEY" is unset`,
		},
		"inherited value carries NUL": {
			seams: func() {
				processIsolationLookupEnv = func(string) (string, bool) { return "secret\x00extra", true }
			},
			mutate:    func(config *processIsolationConfig) { config.InheritEnvironment = []string{"OPENAI_API_KEY"} },
			wantError: `inheritEnvironment variable "OPENAI_API_KEY" contains NUL`,
		},
		"USER does not name the account": {
			mutate:    func(config *processIsolationConfig) { config.BaseEnvironment["USER"] = "root" },
			wantError: `USER and LOGNAME must both equal account name "acp-cov"`,
		},
		"home is refused by the directory proof": {
			seams: func() {
				processIsolationValidateHome = func(string, uint32, uint32) error {
					return errors.New("home is not a private account directory")
				}
			},
			wantError: "home is not a private account directory",
		},
		"PATH is refused by the path proof": {
			seams: func() {
				processIsolationValidatePath = func(string) error {
					return errors.New("PATH component is not a directory")
				}
			},
			wantError: "PATH component is not a directory",
		},
	} {
		t.Run(name, func(t *testing.T) {
			isolationConfigCovStubAccount(t)
			if testCase.seams != nil {
				testCase.seams()
			}
			config := isolationConfigCovConfig(t)
			if testCase.mutate != nil {
				testCase.mutate(&config)
			}
			validated, err := validateProcessIsolationConfig(config)
			if err == nil || !strings.Contains(err.Error(), testCase.wantError) {
				t.Fatalf("validation error = %v, want one containing %q", err, testCase.wantError)
			}
			if validated.UID != 0 || validated.BaseEnvironment != nil {
				t.Fatalf("refused validation still returned a usable config: %#v", validated)
			}
		})
	}
}

// isolationConfigCovPolicy writes payload as a root-owned single-link 0600
// file inside a protected temporary ancestor chain and returns its path.
func isolationConfigCovPolicy(t *testing.T, payload string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "policy.json")
	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}

	return path
}

// TestLoadProcessIsolationConfigAcceptsOnlyABoundedTrustedPolicy proves the
// supervisor only adopts a policy that it read itself from a root-owned,
// bounded, single-link file, and that the bound is enforced against the bytes
// actually read rather than only the size the kernel reported at open time.
func TestLoadProcessIsolationConfigAcceptsOnlyABoundedTrustedPolicy(t *testing.T) {
	isolationConfigCovRequireRoot(t)
	isolationConfigCovStubAccount(t)
	config, err := loadProcessIsolationConfig(isolationConfigCovPolicy(t, isolationConfigCovPolicyJSON))
	if err != nil {
		t.Fatalf("load trusted policy: %v", err)
	}
	if config.UID != 20001 || config.GID != 20002 ||
		config.BaseEnvironment["OPENAI_API_KEY"] != "explicit-secret" || config.InheritEnvironment != nil {
		t.Fatalf("loaded policy = %#v", config)
	}

	if _, err = loadProcessIsolationConfig(""); err == nil ||
		!strings.Contains(err.Error(), "-"+processIsolationConfigFlag+" is required") {
		t.Fatalf("missing policy path error = %v", err)
	}
	if _, err = loadProcessIsolationConfig("/var/lib/../var/lib/acp-cov.json"); err == nil ||
		!strings.Contains(err.Error(), "must be a canonical absolute path") {
		t.Fatalf("non-canonical policy path error = %v", err)
	}
	if _, err = loadProcessIsolationConfig(isolationConfigCovPolicy(t, "{")); err == nil ||
		!strings.Contains(err.Error(), "decode policy") {
		t.Fatalf("malformed policy error = %v", err)
	}

	oversized := isolationConfigCovPolicy(t, strings.Repeat("x", maxProcessIsolationConfigSize+1))
	if _, err = loadProcessIsolationConfig(oversized); err == nil ||
		!strings.Contains(err.Error(), "exceeds "+strconv.Itoa(maxProcessIsolationConfigSize)+" bytes") {
		t.Fatalf("oversized policy error = %v", err)
	}

	// A policy that grows after its size was checked must still be refused by
	// the read bound, so a racing writer cannot smuggle in extra bytes.
	understated := processIsolationFstat
	processIsolationFstat = func(fd int, stat *unix.Stat_t) error {
		if statErr := understated(fd, stat); statErr != nil {
			return statErr
		}
		if stat.Mode&unix.S_IFMT == unix.S_IFREG {
			stat.Size = 16
		}

		return nil
	}
	if _, err = loadProcessIsolationConfig(oversized); err == nil ||
		!strings.Contains(err.Error(), "exceeds "+strconv.Itoa(maxProcessIsolationConfigSize)+" bytes") {
		t.Fatalf("understated policy size error = %v", err)
	}
	processIsolationFstat = understated

	// A root-owned 0600 single-link "regular" file whose contents cannot be
	// read must be refused, never treated as an empty policy.
	unreadable := "/proc/" + strconv.Itoa(os.Getpid()) + "/mem"
	if _, err = loadProcessIsolationConfig(unreadable); err == nil ||
		!strings.Contains(err.Error(), "read -"+processIsolationConfigFlag) {
		t.Fatalf("unreadable policy error = %v", err)
	}

	processIsolationGeteuid = func() int { return 1000 }
	if _, err = loadProcessIsolationConfig(isolationConfigCovPolicy(t, isolationConfigCovPolicyJSON)); err == nil ||
		!strings.Contains(err.Error(), "standalone native mode requires a root supervisor") {
		t.Fatalf("unprivileged supervisor error = %v", err)
	}
}

// TestOpenProtectedAbsolutePathRefusesUnprotectedResolution proves that
// resolving a policy path aborts whenever the chain from the filesystem root
// is not provably root-owned and closed to group and other, and whenever the
// kernel stops answering for a descriptor the resolution just opened.
func TestOpenProtectedAbsolutePathRefusesUnprotectedResolution(t *testing.T) {
	isolationConfigCovRequireRoot(t)
	isolationConfigCovStubAccount(t)
	if _, err := loadProcessIsolationConfig("/"); err == nil ||
		!strings.Contains(err.Error(), "path must be canonical, absolute, and non-root") {
		t.Fatalf("filesystem root as a policy path error = %v", err)
	}

	shared := filepath.Join(t.TempDir(), "shared")
	if err := os.Mkdir(shared, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(shared, 0o777); err != nil {
		t.Fatal(err)
	}
	exposed := filepath.Join(shared, "policy.json")
	if err := os.WriteFile(exposed, []byte(isolationConfigCovPolicyJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadProcessIsolationConfig(exposed); err == nil ||
		!strings.Contains(err.Error(), `ancestor "shared" must be a root-owned directory not writable by group or other`) {
		t.Fatalf("group-writable ancestor error = %v", err)
	}

	trusted := isolationConfigCovPolicy(t, isolationConfigCovPolicyJSON)
	rootFailure := errors.New("filesystem root unavailable")
	processIsolationOpen = func(string, int, uint32) (int, error) { return -1, rootFailure }
	if _, err := loadProcessIsolationConfig(trusted); !errors.Is(err, rootFailure) {
		t.Fatalf("unopenable filesystem root error = %v, want %v", err, rootFailure)
	}
	processIsolationOpen = unix.Open

	for name, failAt := range map[string]int{"filesystem root": 1, "policy component": 2} {
		t.Run(name, func(t *testing.T) {
			isolationConfigCovSeams(t)
			statFailure := errors.New("kernel stopped answering for " + name)
			calls := 0
			original := processIsolationFstat
			processIsolationFstat = func(fd int, stat *unix.Stat_t) error {
				calls++
				if calls == failAt {
					return statFailure
				}

				return original(fd, stat)
			}
			if _, err := loadProcessIsolationConfig(trusted); !errors.Is(err, statFailure) {
				t.Fatalf("descriptor fault error = %v, want %v", err, statFailure)
			}
		})
	}
}

// TestValidateTargetHomeProvesPrivateOwnership proves HOME is accepted only
// when the directory the supervisor opened is itself the target account's
// mode-0700 directory, and that a directory owned by anyone else is refused
// even though the path resolved cleanly.
func TestValidateTargetHomeProvesPrivateOwnership(t *testing.T) {
	isolationConfigCovRequireRoot(t)
	home := filepath.Join(t.TempDir(), "home")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(home, 0o700); err != nil {
		t.Fatal(err)
	}
	owner := uint32(os.Geteuid())
	group := uint32(os.Getegid())
	if err := validateTargetHome(home, owner, group); err != nil {
		t.Fatalf("validate private account home: %v", err)
	}
	if err := validateTargetHome(home, owner+1, group); err == nil ||
		!strings.Contains(err.Error(), "must be a uid "+strconv.FormatUint(uint64(owner+1), 10)) {
		t.Fatalf("foreign-owned home error = %v", err)
	}
	if err := os.Chmod(home, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := validateTargetHome(home, owner, group); err == nil ||
		!strings.Contains(err.Error(), "directory with mode 0700") {
		t.Fatalf("group-readable home error = %v", err)
	}
}

// TestProcessIsolationValueValidatorsBoundTheirInputs proves the standalone
// identifier, state-root, PATH and environment-entry validators accept only
// their documented shapes, so a policy cannot smuggle an unbounded or
// non-canonical value past them.
func TestProcessIsolationValueValidatorsBoundTheirInputs(t *testing.T) {
	for _, value := range []string{"cov-owner", "a", "Owner_1.2:3@x/y-z", strings.Repeat("a", 256)} {
		if !validProcessIsolationOwnerID(value) {
			t.Fatalf("owner id %q was refused", value)
		}
	}
	for _, value := range []string{"", strings.Repeat("a", 257), "-cov", "_cov", "cov owner", "cov#owner"} {
		if validProcessIsolationOwnerID(value) {
			t.Fatalf("owner id %q was accepted", value)
		}
	}

	for _, value := range []string{"/var/lib/acp-cov", "/a"} {
		if !validProcessIsolationStateRoot(value) {
			t.Fatalf("state root %q was refused", value)
		}
	}
	for _, value := range []string{
		"", "relative", "/var/lib/../lib", "/var/lib/acp\x00cov", "/var/lib/acp\ncov",
		"/" + strings.Repeat("a", 4096),
	} {
		if validProcessIsolationStateRoot(value) {
			t.Fatalf("state root %q was accepted", value)
		}
	}

	directory := t.TempDir()
	if err := validatePath(directory); err != nil {
		t.Fatalf("validate PATH %q: %v", directory, err)
	}
	missing := filepath.Join(directory, "absent")
	if err := validatePath(missing); err == nil || !strings.Contains(err.Error(), "stat PATH component") {
		t.Fatalf("absent PATH component error = %v", err)
	}
	file := filepath.Join(directory, "tool")
	if err := os.WriteFile(file, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := validatePath(file); err == nil || !strings.Contains(err.Error(), "is not a directory") {
		t.Fatalf("file PATH component error = %v", err)
	}

	if err := validateEnvironmentEntry("1BAD", "value"); err == nil ||
		!strings.Contains(err.Error(), "baseEnvironment: invalid environment variable name") {
		t.Fatalf("invalid environment name error = %v", err)
	}
	if err := validateEnvironmentEntry("TERM", "xterm\x00extra"); err == nil ||
		!strings.Contains(err.Error(), "contains NUL") {
		t.Fatalf("NUL environment value error = %v", err)
	}
	if err := validateEnvironmentEntry("TERM", "xterm"); err != nil {
		t.Fatalf("validate environment entry: %v", err)
	}
}

// TestProcessIsolationGroupProbeReportsTheAccountsRealGroups proves the
// default supplementary-group probe consults the operating system rather than
// the policy, which is what makes the "no supplementary groups" rule
// meaningful.
func TestProcessIsolationGroupProbeReportsTheAccountsRealGroups(t *testing.T) {
	account, err := user.LookupId(strconv.Itoa(os.Geteuid()))
	if err != nil {
		t.Skipf("current account is not in the operating-system account database: %v", err)
	}
	groups, err := processIsolationGroupIDs(account)
	if err != nil {
		t.Fatalf("probe supplementary groups: %v", err)
	}
	if !slices.Contains(groups, account.Gid) {
		t.Fatalf("probe groups %q do not include the primary group %q", groups, account.Gid)
	}
}
