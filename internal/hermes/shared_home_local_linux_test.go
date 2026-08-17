//go:build linux

package hermes

import "testing"

func TestLinuxSharedHomeFilesystemLocalityAllowlist(t *testing.T) {
	for _, local := range []int64{0xef53, 0x58465342, 0x9123683e, 0xf2f52010, 0x2fc12fc1, 0xca451a4e} {
		if !linuxSharedHomeFilesystemLocal(local) {
			t.Fatalf("local filesystem %#x rejected", local)
		}
	}
	for _, remote := range []int64{0x01021994, 0x858458f6, 0x794c7630, 0x6969, 0xff534d42, 0xfe534d42, 0x517b, 0x65735546, 0x01021997, 0xabba1974} {
		if linuxSharedHomeFilesystemLocal(remote) {
			t.Fatalf("remote filesystem %#x accepted", remote)
		}
	}
}
