//go:build darwin

package hermes

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	containmentRecordSchema = 1
	containmentRecordLimit  = 4096
	containmentRecordMaxAge = 30 * 24 * time.Hour
	containmentRegistryName = "acp-go-hermes-containment"
	containmentVendor       = "hermes"
	containmentBestEffort   = "best_effort"
	containmentPromptKind   = "prompt"
)

var containmentRegistryProcessLock sync.Mutex

var (
	containmentRecordMkdir      = os.Mkdir
	containmentRecordAbs        = filepath.Abs
	containmentRecordReadDir    = os.ReadDir
	containmentRecordNow        = time.Now
	containmentRecordRemove     = os.Remove
	containmentRecordOpen       = unix.Open
	containmentRecordNewFile    = os.NewFile
	containmentRecordFileStat   = func(file *os.File) (os.FileInfo, error) { return file.Stat() }
	containmentRecordReadAll    = io.ReadAll
	containmentRecordMarshal    = json.Marshal
	containmentRecordCreateTemp = os.CreateTemp
	containmentRecordFileWrite  = func(file *os.File, contents []byte) (int, error) { return file.Write(contents) }
	containmentRecordFileSync   = func(file *os.File) error { return file.Sync() }
	containmentRecordFileClose  = func(file *os.File) error { return file.Close() }
	containmentRecordLink       = os.Link
	containmentRecordRename     = os.Rename
	containmentRecordFlock      = unix.Flock
	containmentRecordLstat      = os.Lstat
	containmentRecordSysctl     = unix.SysctlKinfoProc
)

type containmentRecordData struct {
	SchemaVersion        int    `json:"schema_version"` //nolint:tagliatelle // Registry schema uses snake_case.
	Vendor               string `json:"vendor"`
	Containment          string `json:"containment"`
	LifecycleKind        string `json:"lifecycle_kind"`                    //nolint:tagliatelle // Registry schema uses snake_case.
	RuntimeID            string `json:"runtime_id"`                        //nolint:tagliatelle // Registry schema uses snake_case.
	GenerationRoot       string `json:"generation_root"`                   //nolint:tagliatelle // Registry schema uses snake_case.
	WrapperPID           int    `json:"wrapper_pid"`                       //nolint:tagliatelle // Registry schema uses snake_case.
	WrapperStartSec      int64  `json:"wrapper_start_sec"`                 //nolint:tagliatelle // Registry schema uses snake_case.
	WrapperStartUsec     int32  `json:"wrapper_start_usec"`                //nolint:tagliatelle // Registry schema uses snake_case.
	DirectChildPID       *int   `json:"direct_child_pid,omitempty"`        //nolint:tagliatelle // Registry schema uses snake_case.
	DirectChildStartSec  *int64 `json:"direct_child_start_sec,omitempty"`  //nolint:tagliatelle // Registry schema uses snake_case.
	DirectChildStartUsec *int32 `json:"direct_child_start_usec,omitempty"` //nolint:tagliatelle // Registry schema uses snake_case.
	OriginalProcessGID   *int   `json:"original_pgid,omitempty"`           //nolint:tagliatelle // Registry schema uses snake_case.
	State                string `json:"state"`
}

func prepareContainmentRecord(spec ContainmentSpec) (containmentRecord, error) {
	if !spec.DarwinBestEffort {
		return containmentRecord{}, nil
	}

	if err := validateContainmentSpec(spec); err != nil {
		return containmentRecord{}, err
	}

	registry := filepath.Join(spec.ScratchParent, containmentRegistryName)
	if err := containmentRecordMkdir(registry, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return containmentRecord{}, fmt.Errorf("create containment registry: %w", err)
	}

	if err := validateContainmentRegistry(registry); err != nil {
		return containmentRecord{}, err
	}

	lock, err := lockContainmentRegistry(registry)
	if err != nil {
		return containmentRecord{}, err
	}
	defer unlockContainmentRegistry(lock)

	entries, err := containmentRecordReadDir(registry)
	if err != nil {
		return containmentRecord{}, fmt.Errorf("read containment registry: %w", err)
	}

	now := containmentRecordNow()
	records := 0

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		info, infoErr := entry.Info()
		if infoErr != nil {
			return containmentRecord{}, fmt.Errorf("inspect containment record: %w", infoErr)
		}

		if now.Sub(info.ModTime()) > containmentRecordMaxAge {
			path := filepath.Join(registry, entry.Name())

			data, readErr := readContainmentRecord(path)
			if errors.Is(readErr, os.ErrNotExist) {
				continue
			}

			if readErr != nil {
				return containmentRecord{}, fmt.Errorf("inspect expired containment record: %w", readErr)
			}

			if data.State == containmentStateAbsent {
				if removeErr := containmentRecordRemove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
					return containmentRecord{}, fmt.Errorf("expire containment record: %w", removeErr)
				}

				continue
			}
		}

		records++
	}

	if records >= containmentRecordLimit {
		return containmentRecord{}, fmt.Errorf("containment registry reached %d current records", containmentRecordLimit)
	}

	wrapperStartSec, wrapperStartUsec, err := darwinProcessStartIdentity(os.Getpid())
	if err != nil {
		return containmentRecord{}, fmt.Errorf("identify containment wrapper: %w", err)
	}

	record := containmentRecordData{
		SchemaVersion:    containmentRecordSchema,
		Vendor:           containmentVendor,
		Containment:      containmentBestEffort,
		LifecycleKind:    spec.LifecycleKind,
		RuntimeID:        spec.RuntimeID,
		GenerationRoot:   spec.GenerationRoot,
		WrapperPID:       os.Getpid(),
		WrapperStartSec:  wrapperStartSec,
		WrapperStartUsec: wrapperStartUsec,
		State:            containmentStateRunning,
	}

	path := filepath.Join(registry, spec.RuntimeID+".json")
	if err := writeContainmentRecordLocked(path, record, true); err != nil {
		return containmentRecord{}, err
	}

	return containmentRecord{path: path}, nil
}

func validateContainmentSpec(spec ContainmentSpec) error {
	decoded, err := hex.DecodeString(spec.RuntimeID)
	if err != nil || len(decoded) != 16 || spec.RuntimeID != strings.ToLower(spec.RuntimeID) {
		return errors.New("darwin containment runtime id must be 128-bit lowercase hex")
	}

	if !validContainmentLifecycleKind(spec.LifecycleKind) {
		return errors.New("darwin containment lifecycle kind is invalid")
	}

	parent, err := containmentRecordAbs(spec.ScratchParent)
	if err != nil {
		return fmt.Errorf("resolve containment scratch parent: %w", err)
	}

	root, err := containmentRecordAbs(spec.GenerationRoot)
	if err != nil {
		return fmt.Errorf("resolve containment generation root: %w", err)
	}

	relative, err := filepath.Rel(parent, root)
	if err != nil || relative == "." || relative == containmentParentPath || strings.HasPrefix(relative, containmentParentPath+string(filepath.Separator)) {
		return errors.New("darwin containment generation root must be inside the scratch parent")
	}

	if !strings.HasPrefix(filepath.Base(root), "acp-go-hermes-runtime-") {
		return errors.New("darwin containment generation root has an invalid prefix")
	}

	return nil
}

func activateContainmentRecord(record containmentRecord, pid int, pgid int) error {
	if record.path == "" {
		return nil
	}

	startSec, startUsec, err := darwinProcessStartIdentity(pid)
	if err != nil {
		return fmt.Errorf("identify contained direct child: %w", err)
	}

	return updateContainmentRecord(record, func(data *containmentRecordData) {
		data.DirectChildPID = &pid
		data.DirectChildStartSec = &startSec
		data.DirectChildStartUsec = &startUsec
		data.OriginalProcessGID = &pgid
		data.State = containmentStateRunning
	})
}

func completeContainmentRecord(record containmentRecord, state string) error {
	if record.path == "" {
		return nil
	}

	return updateContainmentRecord(record, func(data *containmentRecordData) { data.State = state })
}

func updateContainmentRecord(record containmentRecord, update func(*containmentRecordData)) error {
	registry := filepath.Dir(record.path)

	lock, err := lockContainmentRegistry(registry)
	if err != nil {
		return err
	}
	defer unlockContainmentRegistry(lock)

	data, err := readContainmentRecord(record.path)
	if err != nil {
		return err
	}

	update(&data)

	return writeContainmentRecordLocked(record.path, data, false)
}

func readContainmentRecord(path string) (containmentRecordData, error) {
	if err := validateContainmentRegistry(filepath.Dir(path)); err != nil {
		return containmentRecordData{}, err
	}

	fd, err := containmentRecordOpen(path, unix.O_RDONLY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return containmentRecordData{}, fmt.Errorf("containment record must be a non-symlink regular file with mode 0600: %w", err)
	}

	file := containmentRecordNewFile(uintptr(fd), path)
	defer func() { _ = file.Close() }()

	info, err := containmentRecordFileStat(file)
	if err != nil {
		return containmentRecordData{}, fmt.Errorf("inspect containment record: %w", err)
	}

	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 {
		return containmentRecordData{}, errors.New("containment record must be a non-symlink regular file with mode 0600")
	}

	contents, err := containmentRecordReadAll(file)
	if err != nil {
		return containmentRecordData{}, fmt.Errorf("read containment record: %w", err)
	}

	var record containmentRecordData

	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&record); err != nil {
		return containmentRecordData{}, fmt.Errorf("decode containment record: %w", err)
	}

	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return containmentRecordData{}, errors.New("decode containment record: trailing JSON value")
	}

	if record.SchemaVersion != containmentRecordSchema || record.Vendor != containmentVendor || record.Containment != containmentBestEffort {
		return containmentRecordData{}, errors.New("containment record is not current hermes format")
	}

	if err := validateContainmentRecordData(record); err != nil {
		return containmentRecordData{}, fmt.Errorf("validate containment record: %w", err)
	}

	base := filepath.Base(path)
	if !strings.HasSuffix(base, ".json") || strings.TrimSuffix(base, ".json") != record.RuntimeID {
		return containmentRecordData{}, errors.New("containment record filename does not match runtime id")
	}

	if _, err := validatedContainmentRootPath(filepath.Dir(filepath.Dir(path)), record.GenerationRoot); err != nil {
		return containmentRecordData{}, err
	}

	return record, nil
}

func validateContainmentRecordData(record containmentRecordData) error {
	if !validRuntimeID(record.RuntimeID) {
		return errors.New("runtime id must be 128-bit lowercase hex")
	}

	if !validContainmentLifecycleKind(record.LifecycleKind) {
		return errors.New("lifecycle kind is invalid")
	}

	switch record.State {
	case containmentStateRunning, containmentStateAbsent, containmentStateFailed:
	default:
		return errors.New("state is invalid")
	}

	if !filepath.IsAbs(record.GenerationRoot) || !strings.HasPrefix(filepath.Base(record.GenerationRoot), "acp-go-hermes-runtime-") {
		return errors.New("generation root is invalid")
	}

	if record.WrapperPID <= 0 || record.WrapperStartSec < 0 || record.WrapperStartUsec < 0 || record.WrapperStartUsec >= 1_000_000 {
		return errors.New("wrapper identity is invalid")
	}

	childFields := 0

	for _, present := range []bool{
		record.DirectChildPID != nil,
		record.DirectChildStartSec != nil,
		record.DirectChildStartUsec != nil,
		record.OriginalProcessGID != nil,
	} {
		if present {
			childFields++
		}
	}

	if childFields != 0 && childFields != 4 {
		return errors.New("direct child identity must be all present or all absent")
	}

	if childFields == 4 && (*record.DirectChildPID <= 0 || *record.DirectChildStartSec < 0 || *record.DirectChildStartUsec < 0 || *record.DirectChildStartUsec >= 1_000_000 || *record.OriginalProcessGID != *record.DirectChildPID) {
		return errors.New("direct child identity is invalid")
	}

	return nil
}

func validContainmentLifecycleKind(kind string) bool {
	switch kind {
	case "runtime", containmentSessionKind, containmentPromptKind, "discovery":
		return true
	default:
		return false
	}
}

func writeContainmentRecord(path string, record containmentRecordData, exclusive bool) error {
	registry := filepath.Dir(path)

	lock, err := lockContainmentRegistry(registry)
	if err != nil {
		return err
	}
	defer unlockContainmentRegistry(lock)

	return writeContainmentRecordLocked(path, record, exclusive)
}

func writeContainmentRecordLocked(path string, record containmentRecordData, exclusive bool) error {
	if err := validateContainmentRegistry(filepath.Dir(path)); err != nil {
		return err
	}

	contents, err := containmentRecordMarshal(record)
	if err != nil {
		return fmt.Errorf("encode containment record: %w", err)
	}

	contents = append(contents, '\n')

	file, err := containmentRecordCreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-")
	if err != nil {
		return fmt.Errorf("create containment record temporary: %w", err)
	}

	temporary := file.Name()
	_, writeErr := containmentRecordFileWrite(file, contents)
	syncErr := containmentRecordFileSync(file)

	closeErr := containmentRecordFileClose(file)
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		_ = containmentRecordRemove(temporary)

		return fmt.Errorf("write containment record: %w", err)
	}

	if exclusive {
		if err := containmentRecordLink(temporary, path); err != nil {
			_ = containmentRecordRemove(temporary)

			return fmt.Errorf("publish exclusive containment record: %w", err)
		}

		_ = containmentRecordRemove(temporary)

		return nil
	}

	if err := containmentRecordRename(temporary, path); err != nil {
		_ = containmentRecordRemove(temporary)

		return fmt.Errorf("publish containment record: %w", err)
	}

	return nil
}

func lockContainmentRegistry(registry string) (*os.File, error) {
	containmentRegistryProcessLock.Lock()

	if err := validateContainmentRegistry(registry); err != nil {
		containmentRegistryProcessLock.Unlock()

		return nil, err
	}

	path := filepath.Join(registry, ".lock")

	fd, err := containmentRecordOpen(path, unix.O_RDWR|unix.O_CREAT|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		containmentRegistryProcessLock.Unlock()

		return nil, fmt.Errorf("open containment registry lock: %w", err)
	}

	file := containmentRecordNewFile(uintptr(fd), path)

	info, err := containmentRecordFileStat(file)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		_ = file.Close()
		containmentRegistryProcessLock.Unlock()

		return nil, errors.New("containment registry lock must be a regular file with mode 0600")
	}

	if err := containmentRecordFlock(int(file.Fd()), unix.LOCK_EX); err != nil {
		_ = file.Close()
		containmentRegistryProcessLock.Unlock()

		return nil, fmt.Errorf("lock containment registry: %w", err)
	}

	return file, nil
}

func validateContainmentRegistry(registry string) error {
	info, err := containmentRecordLstat(registry)
	if err != nil {
		return fmt.Errorf("inspect containment registry: %w", err)
	}

	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return errors.New("containment registry must be a non-symlink directory with mode 0700")
	}

	return nil
}

func unlockContainmentRegistry(file *os.File) {
	_ = containmentRecordFlock(int(file.Fd()), unix.LOCK_UN)
	_ = file.Close()
	containmentRegistryProcessLock.Unlock()
}

func darwinProcessStartIdentity(pid int) (int64, int32, error) {
	info, err := containmentRecordSysctl("kern.proc.pid", pid)
	if err != nil {
		return 0, 0, err
	}

	if int(info.Proc.P_pid) != pid {
		return 0, 0, syscall.ESRCH
	}

	started := info.Proc.P_starttime

	return started.Sec, started.Usec, nil
}
