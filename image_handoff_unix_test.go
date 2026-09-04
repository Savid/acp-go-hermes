//go:build !windows

package hermesacp

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
)

// TestHandoffFIFOInsideRootIsRejected pins that a root bounds where a path may
// lead and never what kind of object it names. A FIFO with no writer blocks an
// ordinary open until one appears, so the verdict has to arrive without the open
// ever waiting on it. Only POSIX names a FIFO in the filesystem, so the proof
// lives here.
func TestHandoffFIFOInsideRootIsRejected(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "valid.png")

	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}

	png := fixtureBytes(t, "valid.png")
	block := handoffBlock(path, mimePNG, handoffEnvelopeFor(png))

	done := make(chan error, 1)

	go func() {
		_, err := promptToHermesParts(context.Background(), []acp.ContentBlock{block}, ImageLimits{}, root)
		done <- err
	}()

	select {
	case err := <-done:
		requireHandoffError(t, err, imageErrPathNotAllowed, 0, handoffNotRegularMessage)
	case <-time.After(10 * time.Second):
		t.Fatal("opening a FIFO inside the handoff root blocked the read")
	}
}

// handoffEntryStamp is the state of one entry under a read root that a handoff
// read must leave alone. POSIX updates a directory's modification time as part
// of the change that caused it, so the time is a sound witness for every entry.
func handoffEntryStamp(info os.FileInfo) string {
	return fmt.Sprintf("%v|%d|%v", info.Mode(), info.Size(), info.ModTime())
}
