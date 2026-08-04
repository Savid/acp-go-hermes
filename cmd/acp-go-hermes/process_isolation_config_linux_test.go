//go:build linux

package main

import (
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateProcessIsolationConfigBuildsClosedEnvironment(t *testing.T) {
	oldLookupUser := processIsolationLookupUserID
	oldLookupGroup := processIsolationLookupGroupID
	oldGroupIDs := processIsolationGroupIDs
	oldLookupEnv := processIsolationLookupEnv
	oldValidateHome := processIsolationValidateHome
	oldValidatePath := processIsolationValidatePath
	oldValidateAccount := processIsolationValidateAccountAuthority
	t.Cleanup(func() {
		processIsolationLookupUserID = oldLookupUser
		processIsolationLookupGroupID = oldLookupGroup
		processIsolationGroupIDs = oldGroupIDs
		processIsolationLookupEnv = oldLookupEnv
		processIsolationValidateHome = oldValidateHome
		processIsolationValidatePath = oldValidatePath
		processIsolationValidateAccountAuthority = oldValidateAccount
	})

	processIsolationLookupUserID = func(string) (*user.User, error) {
		return &user.User{Uid: "20001", Gid: "20002", Username: "acp", HomeDir: "/var/lib/acp"}, nil
	}
	processIsolationLookupGroupID = func(string) (*user.Group, error) {
		return &user.Group{Gid: "20002", Name: "acp"}, nil
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

	config, err := validateProcessIsolationConfig(processIsolationConfig{
		UID: 20001,
		GID: 20002,
		BaseEnvironment: map[string]string{
			"PATH": "/usr/bin", "HOME": "/var/lib/acp", "USER": "acp", "LOGNAME": "acp",
		},
		InheritEnvironment: []string{"OPENAI_API_KEY"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if config.BaseEnvironment["OPENAI_API_KEY"] != "explicit-secret" || config.InheritEnvironment != nil {
		t.Fatalf("validated config = %#v", config)
	}

	for name, mutate := range map[string]func(*processIsolationConfig){
		"root identity": func(value *processIsolationConfig) { value.UID = 0 },
		"reserved base": func(value *processIsolationConfig) { value.BaseEnvironment["ACP_GO_TOKEN"] = "leak" },
		"identity inheritance": func(value *processIsolationConfig) {
			value.InheritEnvironment = []string{"HOME"}
		},
		"duplicate inheritance": func(value *processIsolationConfig) {
			value.InheritEnvironment = []string{"OPENAI_API_KEY", "OPENAI_API_KEY"}
		},
		"implicit account home": func(value *processIsolationConfig) { value.BaseEnvironment["HOME"] = "/root" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := processIsolationConfig{
				UID: 20001,
				GID: 20002,
				BaseEnvironment: map[string]string{
					"PATH": "/usr/bin", "HOME": "/var/lib/acp", "USER": "acp", "LOGNAME": "acp",
				},
				InheritEnvironment: []string{"OPENAI_API_KEY"},
			}
			mutate(&candidate)
			if _, err := validateProcessIsolationConfig(candidate); err == nil {
				t.Fatalf("validation accepted %#v", candidate)
			}
		})
	}

	processIsolationLookupGroupID = func(string) (*user.Group, error) {
		return &user.Group{Gid: "20002", Name: "shared"}, nil
	}
	if _, err := validateProcessIsolationConfig(processIsolationConfig{
		UID: 20001,
		GID: 20002,
		BaseEnvironment: map[string]string{
			"PATH": "/usr/bin", "HOME": "/var/lib/acp", "USER": "acp", "LOGNAME": "acp",
		},
	}); err == nil {
		t.Fatal("shared primary group was accepted")
	}
}

func TestLoadProcessIsolationConfigSecureFileChecks(t *testing.T) {
	oldGeteuid := processIsolationGeteuid
	processIsolationGeteuid = func() int { return 0 }
	t.Cleanup(func() { processIsolationGeteuid = oldGeteuid })

	if _, err := loadProcessIsolationConfig("relative.json"); err == nil || !strings.Contains(err.Error(), "absolute path") {
		t.Fatalf("relative path error = %v", err)
	}

	if _, err := loadProcessIsolationConfig("/etc/passwd"); err == nil || !strings.Contains(err.Error(), "root-owned, single-link regular file with mode 0600") {
		t.Fatalf("insecure file error = %v", err)
	}

	path := filepath.Join(t.TempDir(), "policy.json")
	if err := os.WriteFile(path, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "policy-link.json")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := loadProcessIsolationConfig(link); err == nil {
		t.Fatal("symlink policy was accepted")
	}
	if _, err := loadProcessIsolationConfig("/proc/self/fd/0"); err == nil {
		t.Fatal("policy path with a symlink parent was accepted")
	}
}

func TestValidateTargetHomeRejectsSymlinkAndForbiddenHomes(t *testing.T) {
	if err := validateTargetHome("/proc/self", 20001, 20001); err == nil {
		t.Fatal("symlink HOME was accepted")
	}
	for _, path := range []string{"/root", "/nonexistent"} {
		if err := validateTargetHome(path, 20001, 20001); err == nil {
			t.Fatalf("forbidden HOME %q was accepted", path)
		}
	}
}

func TestEnvironmentAndPathValidation(t *testing.T) {
	for _, name := range []string{"", "1KEY", "BAD=KEY", "BAD-KEY"} {
		if err := validateEnvironmentName(name); err == nil {
			t.Fatalf("environment name %q accepted", name)
		}
	}
	for _, name := range []string{"KEY", "_KEY", "KEY_1"} {
		if err := validateEnvironmentName(name); err != nil {
			t.Fatalf("environment name %q: %v", name, err)
		}
	}

	dir := t.TempDir()
	if err := validatePath(dir); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"", "relative", dir + string(os.PathListSeparator)} {
		if err := validatePath(value); err == nil {
			t.Fatalf("PATH %q accepted", value)
		}
	}
}
