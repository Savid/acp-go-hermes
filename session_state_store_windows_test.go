//go:build windows

package hermesacp

import (
	"archive/tar"
	"testing"
)

// TestArchivePathRefusalCoversEveryWindowsRootedSpelling pins that the archive
// refuses a member that names its own location however it is spelled. Windows
// calls a rooted name with no volume relative and a volume qualifier with no
// root relative too, so filepath.IsAbs alone would admit two spellings the same
// archive is refused for everywhere else.
func TestArchivePathRefusalCoversEveryWindowsRootedSpelling(t *testing.T) {
	for _, name := range []string{`\abs`, `C:\abs`, `C:abs`, `//server/share/abs`} {
		header := tar.Header{Name: name, Typeflag: tar.TypeReg, Size: 0}
		if err := decodeXDGArchive(testTarZstd(t, []tar.Header{header}, nil), durableTempDir(t)); err == nil {
			t.Fatalf("archive accepted rooted member %q", name)
		}
	}
}
