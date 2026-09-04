//go:build windows

package hermesacp

import (
	"fmt"
	"os"
)

// handoffEntryStamp is the state of one entry under a read root that a handoff
// read must leave alone. Windows does not update a directory's last-write time
// as part of the change that caused it: NTFS caches the value and flushes it
// later, so two reads taken microseconds apart can disagree with nothing having
// happened in between. A directory therefore witnesses through its mode and
// size, and a file — whose time Windows does update in step — through all three.
func handoffEntryStamp(info os.FileInfo) string {
	if info.IsDir() {
		return fmt.Sprintf("%v|%d", info.Mode(), info.Size())
	}

	return fmt.Sprintf("%v|%d|%v", info.Mode(), info.Size(), info.ModTime())
}
