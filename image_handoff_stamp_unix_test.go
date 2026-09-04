//go:build !windows

package hermesacp

import (
	"fmt"
	"os"
)

// handoffEntryStamp is the state of one entry under a read root that a handoff
// read must leave alone. POSIX updates a directory's modification time as part
// of the change that caused it, so the time is a sound witness for every entry.
func handoffEntryStamp(info os.FileInfo) string {
	return fmt.Sprintf("%v|%d|%v", info.Mode(), info.Size(), info.ModTime())
}
