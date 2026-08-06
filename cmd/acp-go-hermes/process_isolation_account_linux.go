//go:build linux

package main

import (
	"context"
	"fmt"
	"os/exec"
	"os/user"
	"strconv"
	"strings"
	"time"
)

var processIsolationValidateAccountAuthority = validateTargetAccountAuthority

var processIsolationAccountAuthorityCommand = runAccountAuthorityCommand

func validateTargetAccountAuthority(account *user.User, uid uint32, gid uint32) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	passwd, err := processIsolationAccountAuthorityCommand(ctx, "/usr/bin/getent", "passwd")
	if err != nil {
		return fmt.Errorf("enumerate operating-system accounts: %w", err)
	}

	groups, err := processIsolationAccountAuthorityCommand(ctx, "/usr/bin/getent", "group")
	if err != nil {
		return fmt.Errorf("enumerate operating-system groups: %w", err)
	}

	if err = validatePrivateTargetAccount(passwd, groups, account, uid, gid); err != nil {
		return err
	}

	status, err := processIsolationAccountAuthorityCommand(ctx, "/usr/bin/passwd", "-S", account.Username)
	if err != nil {
		return fmt.Errorf("read target account password status: %w", err)
	}

	if err = validateLockedTargetAccount(status, account.Username); err != nil {
		return err
	}

	sudo := exec.CommandContext(ctx, "/usr/bin/sudo", "-n", "-U", account.Username, "-l")
	sudo.Env = []string{"LANG=C", "LC_ALL=C", "PATH=/usr/bin:/bin"}
	output, commandErr := sudo.CombinedOutput()

	return validateTargetAccountHasNoSudo(output, commandErr, account.Username)
}

func runAccountAuthorityCommand(ctx context.Context, path string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, path, args...)
	command.Env = []string{"LANG=C", "LC_ALL=C", "PATH=/usr/bin:/bin"}

	return command.Output()
}

func validatePrivateTargetAccount(passwd []byte, groups []byte, account *user.User, uid uint32, gid uint32) error {
	wantedUID := strconv.FormatUint(uint64(uid), 10)
	wantedGID := strconv.FormatUint(uint64(gid), 10)
	matches := 0

	for line := range strings.SplitSeq(strings.TrimSpace(string(passwd)), "\n") {
		fields := strings.Split(line, ":")
		if len(fields) != 7 || (fields[2] != wantedUID && fields[3] != wantedGID) {
			continue
		}

		if fields[0] != account.Username || fields[2] != wantedUID || fields[3] != wantedGID {
			return fmt.Errorf("uid %s or gid %s is shared by another operating-system account", wantedUID, wantedGID)
		}

		if fields[5] != account.HomeDir {
			return fmt.Errorf("target account home changed from %q to %q", account.HomeDir, fields[5])
		}

		switch fields[6] {
		case "/usr/sbin/nologin", "/sbin/nologin", "/bin/false":
		default:
			return fmt.Errorf("target account %q must use a non-login shell", account.Username)
		}

		matches++
	}

	if matches != 1 {
		return fmt.Errorf("uid %s and gid %s resolve to %d target accounts", wantedUID, wantedGID, matches)
	}

	groupMatches := 0

	for line := range strings.SplitSeq(strings.TrimSpace(string(groups)), "\n") {
		fields := strings.Split(line, ":")
		if len(fields) < 3 || fields[2] != wantedGID {
			continue
		}

		if len(fields) != 4 || fields[0] != account.Username {
			return fmt.Errorf("gid %s is not the unique private group %q", wantedGID, account.Username)
		}

		for member := range strings.SplitSeq(fields[3], ",") {
			member = strings.TrimSpace(member)
			if member != "" && member != account.Username {
				return fmt.Errorf("target group %q includes another account %q", account.Username, member)
			}
		}

		groupMatches++
	}

	if groupMatches != 1 {
		return fmt.Errorf("gid %s resolves to %d target groups", wantedGID, groupMatches)
	}

	return nil
}

func validateLockedTargetAccount(output []byte, username string) error {
	fields := strings.Fields(string(output))
	if len(fields) < 2 || fields[0] != username || (fields[1] != "L" && fields[1] != "LK") {
		return fmt.Errorf("target account %q must have a locked password", username)
	}

	return nil
}

func validateTargetAccountHasNoSudo(output []byte, commandErr error, username string) error {
	trimmed := strings.TrimSpace(string(output))

	wantedPrefix := "User " + username + " is not allowed to run sudo on "
	if strings.HasPrefix(trimmed, wantedPrefix) && !strings.ContainsAny(trimmed, "\r\n") {
		return nil
	}

	if commandErr == nil {
		return fmt.Errorf("target account %q has a sudo policy: %s", username, trimmed)
	}

	return fmt.Errorf("cannot prove target account %q has no sudo policy: %s: %w", username, trimmed, commandErr)
}
