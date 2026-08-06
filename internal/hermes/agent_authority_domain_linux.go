//go:build linux

package hermes

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"golang.org/x/sys/unix"
)

const (
	agentAuthorityDomainVersion    = 1
	agentAuthorityDomainRecordName = "domain.json"
	agentAuthorityDomainLockName   = "domain.lock"
	agentAuthorityProcRoot         = "/proc"
	agentAuthorityDomainMaxSize    = 64 << 10
	agentAuthorityDomainMaxExtents = 340
)

// The agentAuthorityDomain* seams stand in for the kernel answers this file
// depends on. They always hold their production syscall, and exist so a test
// can prove the domain proof aborts when the kernel stops answering for a
// descriptor or a /proc fact it has already accepted.
var (
	agentAuthorityDomainFstat    = unix.Fstat
	agentAuthorityDomainFstatat  = unix.Fstatat
	agentAuthorityDomainFstatfs  = unix.Fstatfs
	agentAuthorityDomainStat     = unix.Stat
	agentAuthorityDomainStatfs   = unix.Statfs
	agentAuthorityDomainReadFile = os.ReadFile
)

type agentAuthorityDomainRecord struct {
	Version       int                          `json:"version"`
	AuthorityID   string                       `json:"authorityId"`
	AuthorityRoot agentAuthorityDomainInode    `json:"authorityRoot"`
	Filesystem    agentAuthorityDomainFS       `json:"filesystem"`
	BootID        string                       `json:"bootId"`
	PIDNamespace  agentAuthorityDomainInode    `json:"pidNamespace"`
	UserNamespace agentAuthorityDomainInode    `json:"userNamespace"`
	UIDMap        []agentAuthorityDomainExtent `json:"uidMap"`
	GIDMap        []agentAuthorityDomainExtent `json:"gidMap"`
}

type agentAuthorityDomainInode struct {
	Dev uint64 `json:"dev"`
	Ino uint64 `json:"ino"`
}

type agentAuthorityDomainFS struct {
	Type int64    `json:"type"`
	ID   [2]int32 `json:"id"`
}

type agentAuthorityDomainExtent struct {
	Inside  uint32 `json:"inside"`
	Outside uint32 `json:"outside"`
	Length  uint32 `json:"length"`
}

func validateAgentAuthorityDomainRecord(directory *os.File, ownerUID, ownerGID uint32) error {
	record, err := loadAgentAuthorityDomainRecord(directory, ownerUID, ownerGID)
	if err != nil {
		return err
	}

	current, err := currentAgentAuthorityDomain(directory)
	if err != nil {
		return err
	}

	if !record.sameDomain(current) {
		return errors.New("inherited agent authority domain belongs to another PID/user namespace domain")
	}

	return nil
}

func loadAgentAuthorityDomainRecord(directory *os.File, ownerUID, ownerGID uint32) (agentAuthorityDomainRecord, error) {
	fd, err := unix.Openat(int(directory.Fd()), agentAuthorityDomainRecordName, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return agentAuthorityDomainRecord{}, fmt.Errorf("open agent authority domain record: %w", err)
	}

	file := os.NewFile(uintptr(fd), agentAuthorityDomainRecordName)
	defer file.Close()

	var descriptor, named unix.Stat_t
	if err = agentAuthorityDomainFstat(fd, &descriptor); err != nil {
		return agentAuthorityDomainRecord{}, err
	}

	if err = agentAuthorityDomainFstatat(
		int(directory.Fd()), agentAuthorityDomainRecordName, &named, unix.AT_SYMLINK_NOFOLLOW,
	); err != nil {
		return agentAuthorityDomainRecord{}, err
	}

	if descriptor.Dev != named.Dev || descriptor.Ino != named.Ino ||
		descriptor.Mode&unix.S_IFMT != unix.S_IFREG || descriptor.Uid != ownerUID || descriptor.Gid != ownerGID ||
		descriptor.Nlink != 1 || descriptor.Mode&0o777 != 0o600 || descriptor.Size <= 0 || descriptor.Size > agentAuthorityDomainMaxSize {
		return agentAuthorityDomainRecord{}, errors.New("agent authority domain record is not its trusted bounded named inode")
	}

	payload, err := io.ReadAll(io.LimitReader(file, agentAuthorityDomainMaxSize+1))
	if err != nil {
		return agentAuthorityDomainRecord{}, err
	}

	if !utf8.Valid(payload) {
		return agentAuthorityDomainRecord{}, errors.New("agent authority domain record is not valid UTF-8")
	}

	if err = rejectAgentAuthorityDuplicateJSONKeys(payload); err != nil {
		return agentAuthorityDomainRecord{}, err
	}

	fields, err := exactAgentAuthorityFields(payload,
		"version", "authorityId", "authorityRoot", "filesystem", "bootId",
		"pidNamespace", "userNamespace", "uidMap", "gidMap",
	)
	if err != nil {
		return agentAuthorityDomainRecord{}, err
	}

	if _, err = exactAgentAuthorityFields(fields["authorityRoot"], "dev", "ino"); err != nil {
		return agentAuthorityDomainRecord{}, fmt.Errorf("invalid agent authority root: %w", err)
	}

	filesystemFields, err := exactAgentAuthorityFields(fields["filesystem"], "type", "id")
	if err != nil {
		return agentAuthorityDomainRecord{}, fmt.Errorf("invalid agent authority filesystem: %w", err)
	}

	var filesystemID []json.RawMessage
	if err = json.Unmarshal(filesystemFields["id"], &filesystemID); err != nil || len(filesystemID) != 2 {
		return agentAuthorityDomainRecord{}, errors.New("agent authority filesystem id must contain exactly two integers")
	}

	for _, component := range filesystemID {
		var value int32
		if err = json.Unmarshal(component, &value); err != nil {
			return agentAuthorityDomainRecord{}, errors.New("agent authority filesystem id contains an invalid signed 32-bit integer")
		}
	}

	if _, err = exactAgentAuthorityFields(fields["pidNamespace"], "dev", "ino"); err != nil {
		return agentAuthorityDomainRecord{}, fmt.Errorf("invalid agent authority PID namespace: %w", err)
	}

	if _, err = exactAgentAuthorityFields(fields["userNamespace"], "dev", "ino"); err != nil {
		return agentAuthorityDomainRecord{}, fmt.Errorf("invalid agent authority user namespace: %w", err)
	}

	var record agentAuthorityDomainRecord

	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()

	if err = decoder.Decode(&record); err != nil {
		return agentAuthorityDomainRecord{}, err
	}

	if record.Version != agentAuthorityDomainVersion || len(record.AuthorityID) != 32 ||
		record.AuthorityRoot.Dev == 0 || record.AuthorityRoot.Ino == 0 || record.Filesystem.Type == 0 ||
		record.Filesystem.ID == [2]int32{} || record.PIDNamespace.Dev == 0 || record.PIDNamespace.Ino == 0 ||
		record.UserNamespace.Dev == 0 || record.UserNamespace.Ino == 0 {
		return agentAuthorityDomainRecord{}, errors.New("agent authority domain record is incomplete")
	}

	if _, err = hex.DecodeString(record.AuthorityID); err != nil || record.AuthorityID != strings.ToLower(record.AuthorityID) {
		return agentAuthorityDomainRecord{}, errors.New("agent authority domain id is invalid")
	}

	if !canonicalAgentAuthorityBootID(record.BootID) {
		return agentAuthorityDomainRecord{}, errors.New("agent authority domain boot id is invalid")
	}

	if err = validateAgentAuthorityExtentFields(fields["uidMap"], record.UIDMap); err != nil {
		return agentAuthorityDomainRecord{}, fmt.Errorf("invalid agent authority uid map: %w", err)
	}

	if err = validateAgentAuthorityExtentFields(fields["gidMap"], record.GIDMap); err != nil {
		return agentAuthorityDomainRecord{}, fmt.Errorf("invalid agent authority gid map: %w", err)
	}

	return record, nil
}

func currentAgentAuthorityDomain(directory *os.File) (agentAuthorityDomainRecord, error) {
	var root unix.Stat_t
	if err := agentAuthorityDomainFstat(int(directory.Fd()), &root); err != nil {
		return agentAuthorityDomainRecord{}, err
	}

	var filesystem unix.Statfs_t
	if err := agentAuthorityDomainFstatfs(int(directory.Fd()), &filesystem); err != nil {
		return agentAuthorityDomainRecord{}, err
	}

	if filesystem.Fsid.Val == [2]int32{} {
		return agentAuthorityDomainRecord{}, errors.New("agent authority filesystem id is unavailable")
	}

	bootID, err := agentAuthorityDomainReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return agentAuthorityDomainRecord{}, err
	}

	boot := strings.TrimSpace(string(bootID))
	if !canonicalAgentAuthorityBootID(boot) {
		return agentAuthorityDomainRecord{}, errors.New("kernel agent authority boot id is not canonical")
	}

	pidNamespace, err := validateAgentAuthorityPIDVisibility()
	if err != nil {
		return agentAuthorityDomainRecord{}, err
	}

	userNamespace, err := agentAuthorityNamespaceIdentity("/proc/self/ns/user")
	if err != nil {
		return agentAuthorityDomainRecord{}, err
	}

	uidMap, err := canonicalAgentAuthorityIDMap("/proc/self/uid_map")
	if err != nil {
		return agentAuthorityDomainRecord{}, err
	}

	gidMap, err := canonicalAgentAuthorityIDMap("/proc/self/gid_map")
	if err != nil {
		return agentAuthorityDomainRecord{}, err
	}

	return agentAuthorityDomainRecord{
		Version:       agentAuthorityDomainVersion,
		AuthorityRoot: agentAuthorityDomainInode{Dev: root.Dev, Ino: root.Ino},
		Filesystem:    agentAuthorityDomainFS{Type: filesystem.Type, ID: [2]int32{filesystem.Fsid.Val[0], filesystem.Fsid.Val[1]}},
		BootID:        boot, PIDNamespace: pidNamespace, UserNamespace: userNamespace, UIDMap: uidMap, GIDMap: gidMap,
	}, nil
}

func validateAgentAuthorityPIDVisibility() (agentAuthorityDomainInode, error) {
	self, err := agentAuthorityNamespaceIdentity("/proc/self/ns/pid")
	if err != nil {
		return agentAuthorityDomainInode{}, err
	}

	children, err := agentAuthorityNamespaceIdentity("/proc/self/ns/pid_for_children")
	if err != nil {
		return agentAuthorityDomainInode{}, err
	}

	if self != children {
		return agentAuthorityDomainInode{}, errors.New("agent authority requires self and child PID namespaces to match")
	}

	var procfs unix.Statfs_t
	if err = agentAuthorityDomainStatfs(agentAuthorityProcRoot, &procfs); err != nil || procfs.Type != 0x9fa0 {
		return agentAuthorityDomainInode{}, errors.New("agent authority requires /proc to be procfs")
	}

	mounts, err := agentAuthorityDomainReadFile("/proc/mounts")
	if err != nil {
		return agentAuthorityDomainInode{}, err
	}

	found := false

	for _, line := range strings.Split(string(mounts), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 4 || fields[1] != agentAuthorityProcRoot || fields[2] != "proc" {
			continue
		}

		found = true

		for _, option := range strings.Split(fields[3], ",") {
			if strings.HasPrefix(option, "hidepid=") && option != "hidepid=0" {
				return agentAuthorityDomainInode{}, fmt.Errorf("agent authority rejects procfs option %q", option)
			}
		}
	}

	if !found {
		return agentAuthorityDomainInode{}, errors.New("agent authority cannot identify the root procfs mount")
	}

	return self, nil
}

func agentAuthorityNamespaceIdentity(path string) (agentAuthorityDomainInode, error) {
	var stat unix.Stat_t
	if err := agentAuthorityDomainStat(path, &stat); err != nil {
		return agentAuthorityDomainInode{}, err
	}

	return agentAuthorityDomainInode{Dev: stat.Dev, Ino: stat.Ino}, nil
}

func canonicalAgentAuthorityIDMap(path string) ([]agentAuthorityDomainExtent, error) {
	payload, err := agentAuthorityDomainReadFile(path)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(strings.TrimSpace(string(payload)), "\n")

	extents := make([]agentAuthorityDomainExtent, 0, len(lines))
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) != 3 {
			return nil, errors.New("agent authority id map has an invalid extent")
		}

		values := make([]uint64, 3)
		for index, field := range fields {
			values[index], err = strconv.ParseUint(field, 10, 32)
			if err != nil || (index == 2 && values[index] == 0) {
				return nil, errors.New("agent authority id map has an invalid extent value")
			}
		}

		extents = append(extents, agentAuthorityDomainExtent{Inside: uint32(values[0]), Outside: uint32(values[1]), Length: uint32(values[2])})
	}

	if err = validateAgentAuthorityExtents(extents); err != nil {
		return nil, err
	}

	return extents, nil
}

func (record agentAuthorityDomainRecord) sameDomain(other agentAuthorityDomainRecord) bool {
	return record.Version == agentAuthorityDomainVersion && record.AuthorityRoot == other.AuthorityRoot &&
		record.Filesystem == other.Filesystem && record.BootID == other.BootID &&
		record.PIDNamespace == other.PIDNamespace && record.UserNamespace == other.UserNamespace &&
		slices.Equal(record.UIDMap, other.UIDMap) && slices.Equal(record.GIDMap, other.GIDMap)
}

func exactAgentAuthorityFields(payload []byte, expected ...string) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return nil, err
	}

	if fields == nil || len(fields) != len(expected) {
		return nil, errors.New("object does not contain its exact required fields")
	}

	for _, name := range expected {
		if _, present := fields[name]; !present {
			return nil, fmt.Errorf("object is missing required field %q", name)
		}
	}

	return fields, nil
}

func canonicalAgentAuthorityBootID(value string) bool {
	if len(value) != 36 {
		return false
	}

	for index, character := range []byte(value) {
		switch index {
		case 8, 13, 18, 23:
			if character != '-' {
				return false
			}
		default:
			if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
				return false
			}
		}
	}

	return true
}

func validateAgentAuthorityExtentFields(payload []byte, extents []agentAuthorityDomainExtent) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(payload, &raw); err != nil || len(raw) != len(extents) {
		return errors.New("agent authority id map changed while decoding")
	}

	for _, item := range raw {
		if _, err := exactAgentAuthorityFields(item, "inside", "outside", "length"); err != nil {
			return err
		}
	}

	return validateAgentAuthorityExtents(extents)
}

func validateAgentAuthorityExtents(extents []agentAuthorityDomainExtent) error {
	if len(extents) == 0 || len(extents) > agentAuthorityDomainMaxExtents {
		return fmt.Errorf("id map must contain between 1 and %d extents", agentAuthorityDomainMaxExtents)
	}

	var priorInsideEnd uint64

	for index, extent := range extents {
		insideEnd := uint64(extent.Inside) + uint64(extent.Length)

		outsideEnd := uint64(extent.Outside) + uint64(extent.Length)
		if extent.Length == 0 || insideEnd > 1<<32 || outsideEnd > 1<<32 ||
			(index > 0 && uint64(extent.Inside) < priorInsideEnd) {
			return errors.New("id map extents are invalid, overflowing, overlapping, or noncanonical")
		}

		priorInsideEnd = insideEnd
	}

	outside := slices.Clone(extents)
	slices.SortFunc(outside, func(left, right agentAuthorityDomainExtent) int {
		if left.Outside < right.Outside {
			return -1
		}

		if left.Outside > right.Outside {
			return 1
		}

		return 0
	})

	for index := 1; index < len(outside); index++ {
		if uint64(outside[index].Outside) < uint64(outside[index-1].Outside)+uint64(outside[index-1].Length) {
			return errors.New("id map extents overlap by outside id")
		}
	}

	return nil
}

func rejectAgentAuthorityDuplicateJSONKeys(payload []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))

	var visit func() error

	visit = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}

		delimiter, ok := token.(json.Delim)
		if !ok {
			return nil
		}

		if delimiter == '{' {
			seen := make(map[string]struct{})

			for decoder.More() {
				keyToken, keyErr := decoder.Token()
				if keyErr != nil {
					return keyErr
				}
				// Decoder object-member tokens are strings by contract.
				key, _ := keyToken.(string)
				if _, duplicate := seen[key]; duplicate {
					return fmt.Errorf("json object contains duplicate key %q", key)
				}

				seen[key] = struct{}{}

				if err = visit(); err != nil {
					return err
				}
			}

			_, err = decoder.Token()

			return err
		}

		for decoder.More() {
			if err = visit(); err != nil {
				return err
			}
		}

		_, err = decoder.Token()

		return err
	}
	if err := visit(); err != nil {
		return err
	}

	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("json contains multiple values")
	}

	return nil
}
