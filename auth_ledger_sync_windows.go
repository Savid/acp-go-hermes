//go:build windows

package hermesacp

// Windows FlushFileBuffers does not accept a directory handle opened through
// os.Open, so flushing the ledger root the way POSIX does fails every commit
// with "Access is denied". What stays durable without it: the entry file is
// written, chmod'd and flushed before the rename that publishes it, and NTFS
// commits that rename through its own metadata journal, so a ledger entry that
// a reader can see has its contents on disk. What is absent is only the POSIX
// ordering guarantee that the directory entry cannot outlive the file's data.
func syncAuthLedgerDirectory(string) error { return nil }
