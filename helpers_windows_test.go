//go:build windows

package hermesacp

import "os"

// windowsSharedHomeRefusal is the message every shared-home surface answers
// with on this platform.
const windowsSharedHomeRefusal = "shared Hermes home is unsupported on windows"

// wantRestrictedPerm is the permission a path the adapter restricted to owner
// access reports back. Windows has no POSIX mode: os.Stat synthesises 0777 for
// a directory and 0666 for a writable file, and the access control that
// matters lives in the ACL the adapter does not set. Asserting the synthesised
// value still pins the one thing the mode carries here — that the adapter did
// not leave the path read-only.
func wantRestrictedPerm(isDir bool) os.FileMode {
	if isDir {
		return 0o777
	}

	return 0o666
}
