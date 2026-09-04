//go:build windows

package hermesacp

import "path/filepath"

// absolutePathSpelling reports whether name is spelled as a path that names its
// own location rather than one relative to a root this adapter chose. Windows
// needs more than filepath.IsAbs, which answers false for two spellings that
// still escape: a rooted name carrying no volume ("/etc/passwd", "\\dir"),
// which every POSIX host already refuses, and a volume qualifier with no root
// ("C:name"), which resolves against that drive's own current directory. Both
// join the refusal so a name is refused here exactly when it would be refused
// on POSIX, and never accepted because Windows spells rootedness differently.
func absolutePathSpelling(name string) bool {
	if filepath.IsAbs(name) || filepath.VolumeName(name) != "" {
		return true
	}

	return name != "" && (name[0] == '/' || name[0] == '\\')
}
