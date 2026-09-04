//go:build windows

package hermesacp

// Windows FlushFileBuffers does not accept a directory handle opened through
// os.Open. What stays durable without it: every journal entry is written to a
// temporary file that is flushed before the rename that publishes it, and NTFS
// commits that rename through its own metadata journal. What is absent is only
// the POSIX ordering guarantee that the directory entry cannot outlive the
// file's data.
func syncSessionOperationDirectory(string) error { return nil }
