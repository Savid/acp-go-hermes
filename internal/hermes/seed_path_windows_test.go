//go:build windows

package hermes

import "testing"

// TestSeedPathRefusalCoversEveryWindowsRootedSpelling pins that a seed key
// naming its own location is refused however it is spelled. Windows calls a
// rooted name with no volume relative, and a volume qualifier with no root
// relative too, so filepath.IsAbs alone would let a seed escape the home the
// adapter chose for it.
func TestSeedPathRefusalCoversEveryWindowsRootedSpelling(t *testing.T) {
	home := t.TempDir()

	for _, relative := range []string{"/etc/passwd", `\etc\passwd`, `C:\etc\passwd`, "C:passwd", `//server/share/passwd`} {
		if _, _, err := resolveSeedFilePath(home, relative); err == nil {
			t.Fatalf("seed path accepted rooted key %q", relative)
		}
	}
}
