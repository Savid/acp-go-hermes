//go:build !unix && !windows

package hermesacp

import "testing"

// Without flock or LockFileEx there is no way to fence the provider-auth
// residence, so the lock must refuse outright. Reporting an unheld lease as
// acquired would let two processes mutate one native credential file at once.
func TestTryAuthProviderFileLockIsUnsupported(t *testing.T) {
	unlock, acquired, err := tryAuthProviderFileLock(nil)
	if err == nil {
		t.Fatal("unsupported platform reported a provider lock result")
	}
	if unlock != nil || acquired {
		t.Fatalf("unsupported lock returned unlock=%v acquired=%v", unlock != nil, acquired)
	}
}
