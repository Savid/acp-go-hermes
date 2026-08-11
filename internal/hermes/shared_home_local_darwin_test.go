//go:build darwin

package hermes

import "testing"

func TestDarwinSharedHomeFilesystemLocalityAllowlist(t *testing.T) {
	for _, local := range []string{"apfs", "hfs", "ufs"} {
		if !darwinSharedHomeFilesystemLocal(local) {
			t.Fatalf("local filesystem %q rejected", local)
		}
	}
	for _, remote := range []string{"msdos", "exfat", "nfs", "smbfs", "osxfuse", "fusefs", "macfuse", "virtiofs", "webdav", "afpfs", "9p"} {
		if darwinSharedHomeFilesystemLocal(remote) {
			t.Fatalf("remote filesystem %q accepted", remote)
		}
	}
}
