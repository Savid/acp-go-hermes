//go:build linux

package hermes

import (
	"encoding/json"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// authorityDomainCovSeams restores every agentAuthorityDomain* seam when the
// test ends, so a fault injected for one kernel fact cannot leak into the
// next case.
func authorityDomainCovSeams(t *testing.T) {
	t.Helper()
	fstat := agentAuthorityDomainFstat
	fstatat := agentAuthorityDomainFstatat
	fstatfs := agentAuthorityDomainFstatfs
	stat := agentAuthorityDomainStat
	statfs := agentAuthorityDomainStatfs
	readFile := agentAuthorityDomainReadFile
	t.Cleanup(func() {
		agentAuthorityDomainFstat = fstat
		agentAuthorityDomainFstatat = fstatat
		agentAuthorityDomainFstatfs = fstatfs
		agentAuthorityDomainStat = stat
		agentAuthorityDomainStatfs = statfs
		agentAuthorityDomainReadFile = readFile
	})
}

// authorityDomainCovAuthority bootstraps an empty trusted authority root and
// returns its open directory descriptor together with the path the domain
// record must occupy inside it.
func authorityDomainCovAuthority(t *testing.T) (*os.File, string) {
	t.Helper()
	restoreAgentIdentityLockTestSeams(t)
	authorityDomainCovSeams(t)
	root := configureAgentIdentityLockTestRoot(t)
	directory, err := bootstrapAgentIdentityLockDirectory(
		root, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := directory.Close(); closeErr != nil {
			t.Errorf("close authority domain fixture: %v", closeErr)
		}
	})

	return directory, filepath.Join(root, "acp-go", "agent-identities", agentAuthorityDomainRecordName)
}

const authorityDomainCovID = "0123456789abcdef0123456789abcdef"

// authorityDomainCovFields returns the current domain of the authority root as
// a mutable JSON object, so each case can corrupt exactly one member of an
// otherwise acceptable record.
func authorityDomainCovFields(t *testing.T, directory *os.File) map[string]json.RawMessage {
	t.Helper()
	record, err := currentAgentAuthorityDomain(directory)
	if err != nil {
		t.Fatal(err)
	}
	record.AuthorityID = authorityDomainCovID
	payload, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}

	return fields
}

func authorityDomainCovPayload(
	t *testing.T,
	base map[string]json.RawMessage,
	overrides map[string]string,
) []byte {
	t.Helper()
	fields := maps.Clone(base)
	for name, value := range overrides {
		fields[name] = json.RawMessage(value)
	}
	payload, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}

	return append(payload, '\n')
}

func authorityDomainCovWrite(t *testing.T, path string, payload []byte) {
	t.Helper()
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestAgentAuthorityDomainRecordRejectsEveryMalformedShape proves the domain
// record is parsed strictly: every object must carry exactly its declared
// members, every scalar must have its declared type and range, and no
// structural trick — a duplicate key, a widened integer, a spare array
// element — is allowed to reach the decoded record.
func TestAgentAuthorityDomainRecordRejectsEveryMalformedShape(t *testing.T) {
	directory, recordPath := authorityDomainCovAuthority(t)
	base := authorityDomainCovFields(t, directory)
	trusted := authorityDomainCovPayload(t, base, nil)
	authorityDomainCovWrite(t, recordPath, trusted)
	if _, err := loadAgentAuthorityDomainRecord(
		directory, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
	); err != nil {
		t.Fatalf("load trusted domain record: %v", err)
	}

	for name, testCase := range map[string]struct {
		overrides map[string]string
		raw       string
		wantError string
	}{
		"duplicate member": {
			raw:       `{"version":1,"version":1}`,
			wantError: `duplicate key "version"`,
		},
		"authority root is not an object": {
			overrides: map[string]string{"authorityRoot": `5`},
			wantError: "invalid agent authority root",
		},
		"authority root names the wrong member": {
			overrides: map[string]string{"authorityRoot": `{"dev":1,"inode":2}`},
			wantError: `invalid agent authority root: object is missing required field "ino"`,
		},
		"filesystem carries a spare member": {
			overrides: map[string]string{"filesystem": `{"type":1,"id":[1,2],"spare":3}`},
			wantError: "invalid agent authority filesystem",
		},
		"filesystem id has three components": {
			overrides: map[string]string{"filesystem": `{"type":1,"id":[1,2,3]}`},
			wantError: "filesystem id must contain exactly two integers",
		},
		"filesystem id component is not an integer": {
			overrides: map[string]string{"filesystem": `{"type":1,"id":["a",2]}`},
			wantError: "filesystem id contains an invalid signed 32-bit integer",
		},
		"pid namespace names the wrong member": {
			overrides: map[string]string{"pidNamespace": `{"dev":1,"inode":2}`},
			wantError: "invalid agent authority PID namespace",
		},
		"user namespace names the wrong member": {
			overrides: map[string]string{"userNamespace": `{"dev":1,"inode":2}`},
			wantError: "invalid agent authority user namespace",
		},
		"version is not a number": {
			overrides: map[string]string{"version": `"one"`},
			wantError: "cannot unmarshal string",
		},
		"version is unsupported": {
			overrides: map[string]string{"version": `2`},
			wantError: "agent authority domain record is incomplete",
		},
		"authority root inode is absent": {
			overrides: map[string]string{"authorityRoot": `{"dev":1,"ino":0}`},
			wantError: "agent authority domain record is incomplete",
		},
		"authority id is not hexadecimal": {
			overrides: map[string]string{"authorityId": `"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"`},
			wantError: "agent authority domain id is invalid",
		},
		"authority id is upper case": {
			overrides: map[string]string{"authorityId": `"0123456789ABCDEF0123456789ABCDEF"`},
			wantError: "agent authority domain id is invalid",
		},
		"boot id is not canonical": {
			overrides: map[string]string{"bootId": `"not-a-boot-identifier-at-all-0000000"`},
			wantError: "agent authority domain boot id is invalid",
		},
		"uid map extent has no length": {
			overrides: map[string]string{"uidMap": `[{"inside":0,"outside":0,"length":0}]`},
			wantError: "invalid agent authority uid map",
		},
		"uid map extent is missing a member": {
			overrides: map[string]string{"uidMap": `[{"inside":0,"outside":0}]`},
			wantError: `invalid agent authority uid map: object does not contain its exact required fields`,
		},
		"gid map extent has no length": {
			overrides: map[string]string{"gidMap": `[{"inside":0,"outside":0,"length":0}]`},
			wantError: "invalid agent authority gid map",
		},
	} {
		t.Run(name, func(t *testing.T) {
			payload := []byte(testCase.raw + "\n")
			if testCase.raw == "" {
				payload = authorityDomainCovPayload(t, base, testCase.overrides)
			}
			authorityDomainCovWrite(t, recordPath, payload)
			_, err := loadAgentAuthorityDomainRecord(
				directory, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
			)
			if err == nil || !strings.Contains(err.Error(), testCase.wantError) {
				t.Fatalf("domain record error = %v, want one containing %q", err, testCase.wantError)
			}
		})
	}
}

// TestAgentAuthorityDomainRecordRequiresItsTrustedBoundedInode proves the
// domain record is only read from the exact trusted named inode: a record that
// is group-readable, multiply linked, oversized, not valid UTF-8, or that the
// kernel will no longer describe or hand over its bytes is refused rather than
// parsed.
func TestAgentAuthorityDomainRecordRequiresItsTrustedBoundedInode(t *testing.T) {
	directory, recordPath := authorityDomainCovAuthority(t)
	base := authorityDomainCovFields(t, directory)
	trusted := authorityDomainCovPayload(t, base, nil)
	authorityRoot := filepath.Dir(recordPath)
	load := func() error {
		_, err := loadAgentAuthorityDomainRecord(
			directory, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
		)

		return err
	}

	authorityDomainCovWrite(t, recordPath, trusted)
	if err := os.Chmod(recordPath, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := load(); err == nil || !strings.Contains(err.Error(), "trusted bounded named inode") {
		t.Fatalf("group-readable record error = %v", err)
	}
	if err := os.Chmod(recordPath, 0o600); err != nil {
		t.Fatal(err)
	}

	linked := filepath.Join(authorityRoot, "domain.json.link")
	if err := os.Link(recordPath, linked); err != nil {
		t.Fatal(err)
	}
	if err := load(); err == nil || !strings.Contains(err.Error(), "trusted bounded named inode") {
		t.Fatalf("multiply linked record error = %v", err)
	}
	if err := os.Remove(linked); err != nil {
		t.Fatal(err)
	}

	authorityDomainCovWrite(t, recordPath, []byte(strings.Repeat("x", agentAuthorityDomainMaxSize+1)))
	if err := load(); err == nil || !strings.Contains(err.Error(), "trusted bounded named inode") {
		t.Fatalf("oversized record error = %v", err)
	}

	authorityDomainCovWrite(t, recordPath, []byte{'{', 0xff, 0xfe, '}', '\n'})
	if err := load(); err == nil || !strings.Contains(err.Error(), "not valid UTF-8") {
		t.Fatalf("non UTF-8 record error = %v", err)
	}

	authorityDomainCovWrite(t, recordPath, trusted)
	statFailure := errors.New("kernel stopped describing the domain record")
	agentAuthorityDomainFstat = func(int, *unix.Stat_t) error { return statFailure }
	if err := load(); !errors.Is(err, statFailure) {
		t.Fatalf("undescribable record error = %v, want %v", err, statFailure)
	}
	agentAuthorityDomainFstat = unix.Fstat

	namedFailure := errors.New("kernel stopped resolving the domain record name")
	agentAuthorityDomainFstatat = func(int, string, *unix.Stat_t, int) error { return namedFailure }
	if err := load(); !errors.Is(err, namedFailure) {
		t.Fatalf("unresolvable record name error = %v, want %v", err, namedFailure)
	}
	agentAuthorityDomainFstatat = unix.Fstatat

	// A record inode the kernel describes as a bounded trusted regular file
	// but which refuses to hand over any bytes must abort the domain proof,
	// never be read as an empty record.
	if err := os.Remove(recordPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(recordPath, 0o700); err != nil {
		t.Fatal(err)
	}
	agentAuthorityDomainFstat = func(fd int, stat *unix.Stat_t) error {
		if err := unix.Fstat(fd, stat); err != nil {
			return err
		}
		stat.Mode = unix.S_IFREG | 0o600
		stat.Nlink = 1
		stat.Size = int64(len(trusted))

		return nil
	}
	if err := load(); !errors.Is(err, unix.EISDIR) {
		t.Fatalf("unreadable record error = %v, want EISDIR", err)
	}
}

// TestAgentAuthorityDomainRecordMustDescribeTheRunningDomain proves that a
// syntactically perfect record which describes a different boot, PID namespace
// or user namespace is refused, so a record carried across a reboot or into
// another namespace can never be adopted as this host's authority.
func TestAgentAuthorityDomainRecordMustDescribeTheRunningDomain(t *testing.T) {
	directory, recordPath := authorityDomainCovAuthority(t)
	base := authorityDomainCovFields(t, directory)
	authorityDomainCovWrite(t, recordPath, authorityDomainCovPayload(t, base, nil))
	if err := validateAgentAuthorityDomainRecord(
		directory, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
	); err != nil {
		t.Fatalf("validate the running domain: %v", err)
	}

	authorityDomainCovWrite(t, recordPath, authorityDomainCovPayload(t, base, map[string]string{
		"bootId": `"3f2504e0-4f89-11d3-9a0c-0305e82c3301"`,
	}))
	if err := validateAgentAuthorityDomainRecord(
		directory, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
	); err == nil || !strings.Contains(err.Error(), "belongs to another PID/user namespace domain") {
		t.Fatalf("foreign-domain record error = %v", err)
	}

	authorityDomainCovWrite(t, recordPath, authorityDomainCovPayload(t, base, nil))
	domainFailure := errors.New("kernel stopped answering for the authority root")
	agentAuthorityDomainFstatfs = func(int, *unix.Statfs_t) error { return domainFailure }
	if err := validateAgentAuthorityDomainRecord(
		directory, agentIdentityLockTrustedUID, agentIdentityLockTrustedGID,
	); !errors.Is(err, domainFailure) {
		t.Fatalf("undeterminable current domain error = %v, want %v", err, domainFailure)
	}
}

// authorityDomainCovReadFile makes the named /proc file answer with payload
// and failure, leaving every other file answered by the kernel. The caller
// restores the seam through authorityDomainCovSeams.
func authorityDomainCovReadFile(path string, payload []byte, failure error) {
	original := agentAuthorityDomainReadFile
	agentAuthorityDomainReadFile = func(name string) ([]byte, error) {
		if name == path {
			return payload, failure
		}

		return original(name)
	}
}

// authorityDomainCovStat makes the named namespace link answer with mutate and
// failure, leaving every other path answered by the kernel. The caller
// restores the seam through authorityDomainCovSeams.
func authorityDomainCovStat(path string, mutate func(*unix.Stat_t), failure error) {
	original := agentAuthorityDomainStat
	agentAuthorityDomainStat = func(name string, stat *unix.Stat_t) error {
		if name != path {
			return original(name, stat)
		}
		if failure != nil {
			return failure
		}
		if err := original(name, stat); err != nil {
			return err
		}
		mutate(stat)

		return nil
	}
}

// TestCurrentAgentAuthorityDomainRefusesUnprovenKernelFacts proves that every
// fact the running domain is built from — the authority filesystem, the boot
// id, the PID namespace and its visibility of every task, the user namespace
// and both id maps — must be answered and canonical, and that a missing or
// implausible answer aborts the domain instead of producing a partial one.
func TestCurrentAgentAuthorityDomainRefusesUnprovenKernelFacts(t *testing.T) {
	directory, _ := authorityDomainCovAuthority(t)
	if _, err := currentAgentAuthorityDomain(directory); err != nil {
		t.Fatalf("determine the running domain: %v", err)
	}

	probeFailure := errors.New("kernel fact unavailable")
	for name, testCase := range map[string]struct {
		seam      func()
		wantError string
	}{
		"authority root is not described": {
			seam: func() {
				agentAuthorityDomainFstat = func(int, *unix.Stat_t) error { return probeFailure }
			},
			wantError: probeFailure.Error(),
		},
		"authority filesystem is not described": {
			seam: func() {
				agentAuthorityDomainFstatfs = func(int, *unix.Statfs_t) error { return probeFailure }
			},
			wantError: probeFailure.Error(),
		},
		"authority filesystem has no identity": {
			seam: func() {
				agentAuthorityDomainFstatfs = func(int, *unix.Statfs_t) error { return nil }
			},
			wantError: "agent authority filesystem id is unavailable",
		},
		"boot id is unreadable": {
			seam: func() {
				authorityDomainCovReadFile("/proc/sys/kernel/random/boot_id", nil, probeFailure)
			},
			wantError: probeFailure.Error(),
		},
		"boot id is not canonical": {
			seam: func() {
				authorityDomainCovReadFile("/proc/sys/kernel/random/boot_id", []byte("nope\n"), nil)
			},
			wantError: "kernel agent authority boot id is not canonical",
		},
		"pid namespace is not described": {
			seam: func() {
				authorityDomainCovStat("/proc/self/ns/pid", nil, probeFailure)
			},
			wantError: probeFailure.Error(),
		},
		"child pid namespace is not described": {
			seam: func() {
				authorityDomainCovStat("/proc/self/ns/pid_for_children", nil, probeFailure)
			},
			wantError: probeFailure.Error(),
		},
		"child pid namespace differs": {
			seam: func() {
				authorityDomainCovStat("/proc/self/ns/pid_for_children", func(stat *unix.Stat_t) {
					stat.Ino++
				}, nil)
			},
			wantError: "requires self and child PID namespaces to match",
		},
		"proc is not procfs": {
			seam: func() {
				agentAuthorityDomainStatfs = func(string, *unix.Statfs_t) error { return probeFailure }
			},
			wantError: "agent authority requires /proc to be procfs",
		},
		"proc mounts are unreadable": {
			seam: func() {
				authorityDomainCovReadFile("/proc/mounts", nil, probeFailure)
			},
			wantError: probeFailure.Error(),
		},
		"proc hides other tasks": {
			seam: func() {
				authorityDomainCovReadFile("/proc/mounts", []byte(
					"proc /proc proc rw,nosuid,nodev,noexec,relatime,hidepid=2 0 0\n",
				), nil)
			},
			wantError: `agent authority rejects procfs option "hidepid=2"`,
		},
		"proc mount is unidentifiable": {
			seam: func() {
				authorityDomainCovReadFile("/proc/mounts", []byte(
					"tmpfs /tmp tmpfs rw 0 0\n",
				), nil)
			},
			wantError: "cannot identify the root procfs mount",
		},
		"user namespace is not described": {
			seam: func() {
				authorityDomainCovStat("/proc/self/ns/user", nil, probeFailure)
			},
			wantError: probeFailure.Error(),
		},
		"uid map is unreadable": {
			seam: func() {
				authorityDomainCovReadFile("/proc/self/uid_map", nil, probeFailure)
			},
			wantError: probeFailure.Error(),
		},
		"gid map is unreadable": {
			seam: func() {
				authorityDomainCovReadFile("/proc/self/gid_map", nil, probeFailure)
			},
			wantError: probeFailure.Error(),
		},
		"uid map extent is truncated": {
			seam: func() {
				authorityDomainCovReadFile("/proc/self/uid_map", []byte("0 0\n"), nil)
			},
			wantError: "agent authority id map has an invalid extent",
		},
		"uid map extent value is not a number": {
			seam: func() {
				authorityDomainCovReadFile("/proc/self/uid_map", []byte("0 0 many\n"), nil)
			},
			wantError: "agent authority id map has an invalid extent value",
		},
		"uid map extent has no length": {
			seam: func() {
				authorityDomainCovReadFile("/proc/self/uid_map", []byte("0 0 0\n"), nil)
			},
			wantError: "agent authority id map has an invalid extent value",
		},
		"uid map extents overlap outside": {
			seam: func() {
				authorityDomainCovReadFile("/proc/self/uid_map", []byte(
					"0 1000 10\n100 0 10\n200 500 10\n300 500 10\n",
				), nil)
			},
			wantError: "id map extents overlap by outside id",
		},
	} {
		t.Run(name, func(t *testing.T) {
			authorityDomainCovSeams(t)
			testCase.seam()
			_, err := currentAgentAuthorityDomain(directory)
			if err == nil || !strings.Contains(err.Error(), testCase.wantError) {
				t.Fatalf("running domain error = %v, want one containing %q", err, testCase.wantError)
			}
		})
	}

	t.Run("disjoint multi-extent id map", func(t *testing.T) {
		authorityDomainCovSeams(t)
		authorityDomainCovReadFile("/proc/self/uid_map", []byte("0 1000 10\n100 0 10\n200 500 10\n"), nil)
		domain, err := currentAgentAuthorityDomain(directory)
		if err != nil {
			t.Fatalf("determine the running domain: %v", err)
		}
		want := []agentAuthorityDomainExtent{
			{Inside: 0, Outside: 1000, Length: 10},
			{Inside: 100, Outside: 0, Length: 10},
			{Inside: 200, Outside: 500, Length: 10},
		}
		if len(domain.UIDMap) != len(want) {
			t.Fatalf("uid map = %v, want %v", domain.UIDMap, want)
		}
		for index, extent := range want {
			if domain.UIDMap[index] != extent {
				t.Fatalf("uid map = %v, want %v", domain.UIDMap, want)
			}
		}
	})
}

// TestAgentAuthorityExtentValidationBoundsTheIDMap proves the id map is
// accepted only as a bounded, ascending, non-overlapping mapping in both the
// inside and outside id spaces, and that the array actually decoded must still
// be the array that was validated.
func TestAgentAuthorityExtentValidationBoundsTheIDMap(t *testing.T) {
	if err := validateAgentAuthorityExtents(nil); err == nil ||
		!strings.Contains(err.Error(), "id map must contain between 1 and "+
			strconv.Itoa(agentAuthorityDomainMaxExtents)+" extents") {
		t.Fatalf("empty id map error = %v", err)
	}
	oversized := make([]agentAuthorityDomainExtent, agentAuthorityDomainMaxExtents+1)
	for index := range oversized {
		oversized[index] = agentAuthorityDomainExtent{
			Inside: uint32(index) * 2, Outside: uint32(index) * 2, Length: 1,
		}
	}
	if err := validateAgentAuthorityExtents(oversized); err == nil ||
		!strings.Contains(err.Error(), "extents") {
		t.Fatalf("oversized id map error = %v", err)
	}
	for name, extents := range map[string][]agentAuthorityDomainExtent{
		"inside overflows": {{Inside: 4294967295, Outside: 0, Length: 2}},
		"outside overflows": {
			{Inside: 0, Outside: 4294967295, Length: 2},
		},
		"inside descends": {
			{Inside: 100, Outside: 0, Length: 10},
			{Inside: 0, Outside: 100, Length: 10},
		},
		"inside overlaps": {
			{Inside: 0, Outside: 0, Length: 10},
			{Inside: 5, Outside: 100, Length: 10},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateAgentAuthorityExtents(extents); err == nil ||
				!strings.Contains(err.Error(), "invalid, overflowing, overlapping, or noncanonical") {
				t.Fatalf("id map error = %v", err)
			}
		})
	}

	if err := validateAgentAuthorityExtentFields([]byte(`{}`), nil); err == nil ||
		!strings.Contains(err.Error(), "agent authority id map changed while decoding") {
		t.Fatalf("non-array id map error = %v", err)
	}
	if err := validateAgentAuthorityExtentFields(
		[]byte(`[{"inside":0,"outside":0,"length":10}]`),
		[]agentAuthorityDomainExtent{{Inside: 0, Outside: 0, Length: 10}},
	); err != nil {
		t.Fatalf("validate a single-extent id map: %v", err)
	}
}

// TestAgentAuthorityBootIDRequiresTheCanonicalUUIDForm proves the boot id is
// accepted only in the exact lower-case dashed UUID form the kernel emits, so
// no other string can stand in for a boot identity.
func TestAgentAuthorityBootIDRequiresTheCanonicalUUIDForm(t *testing.T) {
	if !canonicalAgentAuthorityBootID("3f2504e0-4f89-11d3-9a0c-0305e82c3301") {
		t.Fatal("canonical boot id was refused")
	}
	for name, value := range map[string]string{
		"too short":     "3f2504e0-4f89-11d3-9a0c-0305e82c330",
		"wrong dash":    "3f2504e0-4f89-11d3-9a0c0-305e82c3301",
		"upper case":    "3F2504E0-4f89-11d3-9a0c-0305e82c3301",
		"non hex digit": "3f2504e0-4f89-11d3-9a0c-0305e82c330z",
	} {
		t.Run(name, func(t *testing.T) {
			if canonicalAgentAuthorityBootID(value) {
				t.Fatalf("boot id %q was accepted", value)
			}
		})
	}
}

// TestAgentAuthorityDuplicateJSONKeyScannerWalksNestedValues proves the
// duplicate-key scanner descends into nested objects and arrays rather than
// only checking the top level, and that it surfaces the decoder's own error
// when the document stops being well formed part-way through an object.
func TestAgentAuthorityDuplicateJSONKeyScannerWalksNestedValues(t *testing.T) {
	if err := rejectAgentAuthorityDuplicateJSONKeys(
		[]byte(`{"a":{"b":[1,{"c":2}]},"d":[]}`),
	); err != nil {
		t.Fatalf("well formed document was refused: %v", err)
	}
	for name, testCase := range map[string]struct {
		payload   string
		wantError string
	}{
		"duplicate inside a nested object": {
			payload:   `{"a":{"b":1,"b":2}}`,
			wantError: `duplicate key "b"`,
		},
		"duplicate inside an array element": {
			payload:   `{"a":[{"b":1,"b":2}]}`,
			wantError: `duplicate key "b"`,
		},
		"object member separator is missing": {
			payload:   `{"a":1 "b":2}`,
			wantError: "after object key:value pair",
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := rejectAgentAuthorityDuplicateJSONKeys([]byte(testCase.payload))
			if err == nil || !strings.Contains(err.Error(), testCase.wantError) {
				t.Fatalf("scanner error = %v, want one containing %q", err, testCase.wantError)
			}
		})
	}
}
