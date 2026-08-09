package hermesacp

import (
	"errors"
	"os"
	"strings"
	"testing"
)

type validationIdentityCapability struct{}

func (validationIdentityCapability) Duplicate() (*os.File, error) {
	return nil, errors.New("validation capability is not duplicable")
}

func TestValidateProcessIsolationOption(t *testing.T) {
	originalPlatform := agentRuntimePlatform
	t.Cleanup(func() { agentRuntimePlatform = originalPlatform })

	if err := validateProcessIsolationOption(nil); err == nil {
		t.Fatal("nil process isolation was accepted")
	}
	if err := validateProcessIsolationOption(&ProcessIsolation{UID: 0, GID: 1}); err == nil {
		t.Fatal("zero process identity was accepted")
	}

	agentRuntimePlatform = agentRuntimeLinux
	borrowed := &ProcessIsolation{
		UID: 1, GID: 2,
		IdentityLock: validationIdentityCapability{}, AuthorityDomain: validationIdentityCapability{},
	}
	if err := validateProcessIsolationOption(borrowed); err != nil {
		t.Fatalf("valid borrowed identity: %v", err)
	}
	if err := validateProcessIsolationOption(&ProcessIsolation{
		UID: 1, GID: 2, IdentityLock: validationIdentityCapability{},
	}); err == nil {
		t.Fatal("partial borrowed identity was accepted")
	}
	if err := validateProcessIsolationOption(&ProcessIsolation{
		UID: 1, GID: 2,
		IdentityLock: validationIdentityCapability{}, AuthorityDomain: validationIdentityCapability{},
		StandaloneOwnerID: "mixed",
	}); err == nil {
		t.Fatal("mixed borrowed and standalone identity was accepted")
	}
	standalone := &ProcessIsolation{
		UID: 1, GID: 2, StandaloneOwnerID: "deployment-1", StandaloneStateRoot: "/var/lib/hermes",
	}
	if err := validateProcessIsolationOption(standalone); err != nil {
		t.Fatalf("valid standalone identity: %v", err)
	}

	agentRuntimePlatform = agentRuntimeWindows
	if err := validateProcessIsolationOption(standalone); err == nil {
		t.Fatal("Windows process isolation was accepted")
	}

	agentRuntimePlatform = agentRuntimeDarwin
	if err := validateProcessIsolationOption(standalone); err != nil {
		t.Fatalf("Darwin process isolation: %v", err)
	}

	agentRuntimePlatform = agentRuntimeLinux
	agent := NewAgent()
	if err := agent.rejectInvalidConfiguration(); err == nil {
		t.Fatal("agent accepted its missing process isolation policy")
	}
}

func TestValidateStandaloneIdentityOption(t *testing.T) {
	validStateRoot := "/var/lib/hermes"

	tests := []struct {
		name            string
		identityLock    bool
		authorityDomain bool
		ownerID         string
		stateRoot       string
		shared          bool
		valid           bool
	}{
		{name: "borrowed", identityLock: true, authorityDomain: true, valid: true},
		{name: "standalone", ownerID: "deployment-1", stateRoot: validStateRoot, valid: true},
		{name: "partial borrowed", identityLock: true},
		{name: "mixed", identityLock: true, authorityDomain: true, ownerID: "deployment-1"},
		{name: "invalid owner", ownerID: "-deployment", stateRoot: validStateRoot},
		{name: "invalid state root", ownerID: "deployment-1", stateRoot: "relative"},
		{name: "shared", shared: true, valid: true},
		{name: "shared owner", shared: true, ownerID: "deployment-1"},
		{name: "shared state root", shared: true, stateRoot: validStateRoot},
		{name: "shared borrowed", identityLock: true, authorityDomain: true, shared: true, valid: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateStandaloneIdentityOption(
				test.identityLock, test.authorityDomain, test.ownerID, test.stateRoot, test.shared,
			)
			if (err == nil) != test.valid {
				t.Fatalf("validation error = %v, valid = %v", err, test.valid)
			}
		})
	}
}

func TestStandaloneIdentityFieldGrammar(t *testing.T) {
	validStateRoot := "/var/lib/hermes"
	for _, value := range []string{
		"", strings.Repeat("a", 257), "-deployment", "deployment space",
	} {
		if validStandaloneOwnerID(value) {
			t.Fatalf("owner id %q was accepted", value)
		}
	}
	for _, value := range []string{"A", "deployment-1", "org.example:worker/1"} {
		if !validStandaloneOwnerID(value) {
			t.Fatalf("owner id %q was rejected", value)
		}
	}

	for _, value := range []string{
		"", strings.Repeat("/a", 2049), string([]byte{0xff}), "relative", "/tmp/../tmp/native",
		"/", "/tmp/native\x00", "/tmp/native\n", "/var/lib/acp-go/agent-identities",
		"/var/lib/acp-go/agent-identities/provider",
	} {
		if validStandaloneStateRootPath(value) {
			t.Fatalf("state root %q was accepted", value)
		}
	}
	if !validStandaloneStateRootPath(validStateRoot) {
		t.Fatalf("state root %q was rejected", validStateRoot)
	}
}
