package hermesacp

import (
	"errors"
	"regexp"
	"testing"
)

func TestNewSessionIDShapeAndReaderError(t *testing.T) {
	id, err := newSessionID()
	if err != nil {
		t.Fatalf("newSessionID: %v", err)
	}
	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(id) {
		t.Fatalf("session id %q is not a v4 UUID", id)
	}

	oldReader := sessionIDRandReader
	sessionIDRandReader = errorReader{err: errors.New("id failed")}
	t.Cleanup(func() { sessionIDRandReader = oldReader })

	if _, err := newSessionID(); err == nil {
		t.Fatal("newSessionID ignored reader error")
	}
}
