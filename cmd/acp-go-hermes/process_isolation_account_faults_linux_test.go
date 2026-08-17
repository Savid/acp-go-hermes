//go:build linux

package main

import (
	"context"
	"errors"
	"os/user"
	"slices"
	"strings"
	"testing"
)

// isolationConfigCovAccountRunner installs a scripted replacement for the
// account-authority probe and restores the production runner afterwards. Each
// recorded invocation is appended to the returned slice pointer so a test can
// prove which operating-system databases were interrogated.
func isolationConfigCovAccountRunner(
	t *testing.T,
	reply func(path string, args []string) ([]byte, error),
) *[][]string {
	t.Helper()
	original := processIsolationAccountAuthorityCommand
	t.Cleanup(func() { processIsolationAccountAuthorityCommand = original })
	calls := new([][]string)
	processIsolationAccountAuthorityCommand = func(
		_ context.Context, path string, args ...string,
	) ([]byte, error) {
		*calls = append(*calls, append([]string{path}, args...))

		return reply(path, args)
	}

	return calls
}

const (
	isolationConfigCovPasswd = "acp-cov:x:20001:20002::/var/lib/acp-cov:/usr/sbin/nologin\n"
	isolationConfigCovGroup  = "acp-cov:x:20002:\n"
	isolationConfigCovStatus = "acp-cov L 2026-08-05 0 99999 7 -1\n"
)

func isolationConfigCovAccount() *user.User {
	return &user.User{Username: "acp-cov", Uid: "20001", Gid: "20002", HomeDir: "/var/lib/acp-cov"}
}

// TestTargetAccountAuthorityRefusesEveryUnprovenStage proves that target
// account validation re-derives every fact from the live operating-system
// account databases and refuses the account as soon as one stage cannot be
// proven, rather than trusting the *user.User the caller already resolved.
func TestTargetAccountAuthorityRefusesEveryUnprovenStage(t *testing.T) {
	stageFailure := errors.New("probe unavailable")
	for name, testCase := range map[string]struct {
		reply     func(path string, args []string) ([]byte, error)
		wantError string
		wantCalls [][]string
	}{
		"account enumeration unavailable": {
			reply:     func(string, []string) ([]byte, error) { return nil, stageFailure },
			wantError: "enumerate operating-system accounts",
			wantCalls: [][]string{{"/usr/bin/getent", "passwd"}},
		},
		"group enumeration unavailable": {
			reply: func(_ string, args []string) ([]byte, error) {
				if args[0] == "group" {
					return nil, stageFailure
				}

				return []byte(isolationConfigCovPasswd), nil
			},
			wantError: "enumerate operating-system groups",
			wantCalls: [][]string{{"/usr/bin/getent", "passwd"}, {"/usr/bin/getent", "group"}},
		},
		"account is not private": {
			reply: func(_ string, args []string) ([]byte, error) {
				if args[0] == "group" {
					return []byte(isolationConfigCovGroup), nil
				}

				return []byte(isolationConfigCovPasswd +
					"other:x:20001:30000::/nonexistent:/usr/sbin/nologin\n"), nil
			},
			wantError: "is shared by another operating-system account",
			wantCalls: [][]string{{"/usr/bin/getent", "passwd"}, {"/usr/bin/getent", "group"}},
		},
		"password status unavailable": {
			reply: func(path string, args []string) ([]byte, error) {
				switch {
				case path == "/usr/bin/passwd":
					return nil, stageFailure
				case args[0] == "group":
					return []byte(isolationConfigCovGroup), nil
				default:
					return []byte(isolationConfigCovPasswd), nil
				}
			},
			wantError: "read target account password status",
			wantCalls: [][]string{
				{"/usr/bin/getent", "passwd"},
				{"/usr/bin/getent", "group"},
				{"/usr/bin/passwd", "-S", "acp-cov"},
			},
		},
		"password is not locked": {
			reply: func(path string, args []string) ([]byte, error) {
				switch {
				case path == "/usr/bin/passwd":
					return []byte("acp-cov P 2026-08-05 0 99999 7 -1\n"), nil
				case args[0] == "group":
					return []byte(isolationConfigCovGroup), nil
				default:
					return []byte(isolationConfigCovPasswd), nil
				}
			},
			wantError: `target account "acp-cov" must have a locked password`,
			wantCalls: [][]string{
				{"/usr/bin/getent", "passwd"},
				{"/usr/bin/getent", "group"},
				{"/usr/bin/passwd", "-S", "acp-cov"},
			},
		},
		"sudo policy cannot be proven absent": {
			reply: func(path string, args []string) ([]byte, error) {
				switch {
				case path == "/usr/bin/passwd":
					return []byte(isolationConfigCovStatus), nil
				case args[0] == "group":
					return []byte(isolationConfigCovGroup), nil
				default:
					return []byte(isolationConfigCovPasswd), nil
				}
			},
			// The account does not exist on this host, so `sudo -l -U` cannot
			// answer for it. Validation must refuse rather than assume.
			wantError: `cannot prove target account "acp-cov" has no sudo policy`,
			wantCalls: [][]string{
				{"/usr/bin/getent", "passwd"},
				{"/usr/bin/getent", "group"},
				{"/usr/bin/passwd", "-S", "acp-cov"},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			calls := isolationConfigCovAccountRunner(t, testCase.reply)
			err := validateTargetAccountAuthority(isolationConfigCovAccount(), 20001, 20002)
			if err == nil || !strings.Contains(err.Error(), testCase.wantError) {
				t.Fatalf("authority error = %v, want one containing %q", err, testCase.wantError)
			}
			if !slices.EqualFunc(*calls, testCase.wantCalls, slices.Equal) {
				t.Fatalf("authority probes = %q, want %q", *calls, testCase.wantCalls)
			}
		})
	}
}

// TestAccountAuthorityCommandRunsWithAFixedEnvironment proves the account
// authority probe executes with a fixed C-locale environment and a fixed PATH,
// so a poisoned supervisor environment can neither localise nor redirect what
// the account databases report.
func TestAccountAuthorityCommandRunsWithAFixedEnvironment(t *testing.T) {
	t.Setenv("LC_ALL", "en_US.UTF-8")
	t.Setenv("LD_PRELOAD", "/tmp/evil.so")
	output, err := runAccountAuthorityCommand(t.Context(), "/usr/bin/env")
	if err != nil {
		t.Fatalf("run account authority probe: %v", err)
	}
	got := strings.Split(strings.TrimSpace(string(output)), "\n")
	slices.Sort(got)
	const want = "LANG=C\nLC_ALL=C\nPATH=/usr/bin:/bin"
	if strings.Join(got, "\n") != want {
		t.Fatalf("probe environment = %q, want %q", got, want)
	}
}

// TestValidatePrivateTargetAccountIgnoresUnrelatedRecords proves the private
// account proof only consults records that claim the target uid or gid, and
// still refuses when the target itself is absent from either database — an
// unmatched target must never be read as "nothing suspicious found".
func TestValidatePrivateTargetAccountIgnoresUnrelatedRecords(t *testing.T) {
	account := isolationConfigCovAccount()
	noise := "root:x:0:0:root:/root:/bin/bash\ndaemon:x:1:1:daemon:/usr/sbin:/usr/sbin/nologin\n"
	groupNoise := "root:x:0:\nadm:x:4:syslog\n"
	if err := validatePrivateTargetAccount(
		[]byte(noise+isolationConfigCovPasswd), []byte(groupNoise+isolationConfigCovGroup),
		account, 20001, 20002,
	); err != nil {
		t.Fatalf("unrelated passwd and group records were not ignored: %v", err)
	}

	if err := validatePrivateTargetAccount(
		[]byte(noise), []byte(groupNoise+isolationConfigCovGroup), account, 20001, 20002,
	); err == nil || !strings.Contains(err.Error(), "resolve to 0 target accounts") {
		t.Fatalf("absent passwd record error = %v", err)
	}
	if err := validatePrivateTargetAccount(
		[]byte(noise+isolationConfigCovPasswd), []byte(groupNoise), account, 20001, 20002,
	); err == nil || !strings.Contains(err.Error(), "resolves to 0 target groups") {
		t.Fatalf("absent group record error = %v", err)
	}
}
