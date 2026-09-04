//go:build !windows

package hermesacp

import "path/filepath"

// absolutePathSpelling reports whether name is spelled as a path that names its
// own location rather than one relative to a root this adapter chose. POSIX has
// exactly one such spelling and filepath.IsAbs recognises it.
func absolutePathSpelling(name string) bool {
	return filepath.IsAbs(name)
}
