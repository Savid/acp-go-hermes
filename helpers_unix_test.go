//go:build !windows

package hermesacp

import "os"

// wantRestrictedPerm is the permission a path the adapter restricted to owner
// access reports back. POSIX stores the mode the adapter asked for, so the
// assertion is the ask itself.
func wantRestrictedPerm(isDir bool) os.FileMode {
	if isDir {
		return 0o700
	}

	return 0o600
}
