//go:build windows

package hermes

// Windows FlushFileBuffers does not accept directory handles opened through
// os.Open. The temp file itself is flushed before the atomic rename.
func syncSharedHermesDirectory(string) error { return nil }
