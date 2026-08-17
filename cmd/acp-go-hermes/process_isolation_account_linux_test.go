//go:build linux

package main

import (
	"errors"
	"os/user"
	"testing"
)

func TestValidatePrivateTargetAccount(t *testing.T) {
	account := &user.User{Username: "acp", Uid: "20001", Gid: "20002", HomeDir: "/var/lib/acp"}
	passwd := []byte("acp:x:20001:20002::/var/lib/acp:/usr/sbin/nologin\n")
	groups := []byte("acp:x:20002:\n")
	if err := validatePrivateTargetAccount(passwd, groups, account, 20001, 20002); err != nil {
		t.Fatal(err)
	}
	for name, candidate := range map[string]struct {
		passwd []byte
		groups []byte
	}{
		"shared uid":      {append(passwd, []byte("other:x:20001:30000::/nonexistent:/usr/sbin/nologin\n")...), groups},
		"shared gid":      {append(passwd, []byte("other:x:30000:20002::/nonexistent:/usr/sbin/nologin\n")...), groups},
		"login shell":     {[]byte("acp:x:20001:20002::/var/lib/acp:/bin/sh\n"), groups},
		"changed home":    {[]byte("acp:x:20001:20002::/tmp/acp:/usr/sbin/nologin\n"), groups},
		"group member":    {passwd, []byte("acp:x:20002:other\n")},
		"duplicate group": {passwd, []byte("acp:x:20002:\nother:x:20002:\n")},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validatePrivateTargetAccount(candidate.passwd, candidate.groups, account, 20001, 20002); err == nil {
				t.Fatal("unsafe account authority was accepted")
			}
		})
	}
}

func TestValidateLockedTargetAccount(t *testing.T) {
	for _, state := range []string{"L", "LK"} {
		if err := validateLockedTargetAccount([]byte("acp "+state+" 2026-08-05 0 99999 7 -1"), "acp"); err != nil {
			t.Fatal(err)
		}
	}
	if err := validateLockedTargetAccount([]byte("acp P 2026-08-05 0 99999 7 -1"), "acp"); err == nil {
		t.Fatal("password-enabled account was accepted")
	}
}

func TestValidateTargetAccountHasNoSudo(t *testing.T) {
	denial := []byte("User acp is not allowed to run sudo on runner.\n")
	for _, commandErr := range []error{nil, errors.New("exit status 1")} {
		if err := validateTargetAccountHasNoSudo(denial, commandErr, "acp"); err != nil {
			t.Fatal(err)
		}
	}
	if err := validateTargetAccountHasNoSudo([]byte("(root) NOPASSWD: /bin/sh"), nil, "acp"); err == nil {
		t.Fatal("sudo-enabled account was accepted")
	}
	if err := validateTargetAccountHasNoSudo([]byte("sudoers parse error"), errors.New("exit status 1"), "acp"); err == nil {
		t.Fatal("unproved sudo denial was accepted")
	}
}
