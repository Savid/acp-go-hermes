package hermes

import (
	"testing"
	"time"
)

func TestDirectChildWaitAwaitReaped(t *testing.T) {
	if err := (*directChildWait)(nil).awaitReaped(time.Second); err == nil {
		t.Fatal("nil direct-child waiter succeeded")
	}

	pending := &directChildWait{done: make(chan struct{})}
	if err := pending.awaitReaped(time.Nanosecond); err == nil {
		t.Fatal("pending direct-child waiter succeeded")
	}

	complete := &directChildWait{done: make(chan struct{})}
	close(complete.done)
	if err := complete.awaitReaped(time.Second); err != nil {
		t.Fatal(err)
	}
}
