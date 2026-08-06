//go:build linux

package hermes

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/stretchr/testify/require"
)

// agentStandaloneResRemoveProbeTemporaries deletes whatever the durability
// probe has created in the registry so far. The probe names its temporary with
// fresh randomness, so a case that wants to stage "a peer swept the registry
// mid-probe" can only find it by prefix.
func agentStandaloneResRemoveProbeTemporaries(t *testing.T, directory *os.File) {
	t.Helper()
	entries, err := os.ReadDir(directory.Name())
	require.NoError(t, err)
	removed := 0
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".authority-probe-") {
			require.NoError(t, os.Remove(filepath.Join(directory.Name(), entry.Name())))
			removed++
		}
	}
	require.Equal(t, 1, removed, "the probe should have exactly one temporary in flight")
}

// agentStandaloneResProbeLeftNothing proves a refused probe still swept its own
// temporaries out of the registry.
func agentStandaloneResProbeLeftNothing(t *testing.T, directory *os.File) {
	t.Helper()
	entries, err := os.ReadDir(directory.Name())
	require.NoError(t, err)
	for _, entry := range entries {
		require.NotContains(t, entry.Name(), ".authority-probe-")
	}
}

// TestAgentStandaloneResProbeRefusesWhenItsTemporaryVanishesUnderIt proves the
// durability probe aborts if its own temporary disappears before the probe can
// test separate-open flock exclusion, or before it can test that a rename
// preserves inode identity. Either step would otherwise reach a verdict about a
// file it no longer holds, and the probe's verdict is what admits the filesystem
// as a place agent authority may live.
func TestAgentStandaloneResProbeRefusesWhenItsTemporaryVanishesUnderIt(t *testing.T) {
	t.Run("before the exclusion test", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		previous := agentStandaloneProbeFcntl
		t.Cleanup(func() { agentStandaloneProbeFcntl = previous })
		calls := 0
		agentStandaloneProbeFcntl = func(fd uintptr, cmd, arg int) (int, error) {
			calls++
			if calls == 3 {
				agentStandaloneResRemoveProbeTemporaries(t, directory)
			}

			return previous(fd, cmd, arg)
		}

		err := probeAgentStandaloneFilesystem(directory, true)
		require.ErrorIs(t, err, unix.ENOENT)
		require.Equal(t, 3, calls)
		agentStandaloneResProbeLeftNothing(t, directory)
	})

	t.Run("before the rename test", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovRestoreDurableSeams(t)
		previous := agentStandaloneDurableFstat
		swept := false
		agentStandaloneDurableFstat = func(fd int, stat *unix.Stat_t) error {
			if !swept {
				swept = true
				agentStandaloneResRemoveProbeTemporaries(t, directory)
			}

			return previous(fd, stat)
		}

		err := probeAgentStandaloneFilesystem(directory, true)
		require.True(t, swept, "the probe never described its temporary")
		require.ErrorIs(t, err, unix.ENOENT)
		agentStandaloneResProbeLeftNothing(t, directory)
	})
}

// TestAgentStandaloneResProbeRefusesADescriptorItCannotRelease proves the
// durability probe aborts when the kernel refuses to close the contending
// descriptor it opened to test flock exclusion, instead of accepting the
// filesystem while leaking a second writable handle into the authority registry.
func TestAgentStandaloneResProbeRefusesADescriptorItCannotRelease(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	wantErr := errors.New("kernel refused to release the contending descriptor")
	previous := agentStandaloneProbeCloseFD
	t.Cleanup(func() { agentStandaloneProbeCloseFD = previous })
	closes := 0
	agentStandaloneProbeCloseFD = func(fd int) error {
		closes++
		if closeErr := previous(fd); closeErr != nil {
			return closeErr
		}

		return wantErr
	}

	err := probeAgentStandaloneFilesystem(directory, true)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, 1, closes)
	agentStandaloneResProbeLeftNothing(t, directory)
}

// TestAgentStandaloneResRegistryFileThatWithholdsItsBytesIsNeverReadAsEmpty
// proves that a registry entry the kernel describes as a trusted bounded regular
// file but which refuses to hand over any bytes aborts the read, rather than
// being decoded as an empty owner record. An owner binding that read as empty
// would be an identity nobody is accountable for.
func TestAgentStandaloneResRegistryFileThatWithholdsItsBytesIsNeverReadAsEmpty(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	require.NoError(t, os.Mkdir(filepath.Join(directory.Name(), "62951.owner"), 0o700))
	agentStandaloneCovRestoreDurableSeams(t)
	previous := agentStandaloneDurableFstat
	agentStandaloneDurableFstat = func(fd int, stat *unix.Stat_t) error {
		if statErr := previous(fd, stat); statErr != nil {
			return statErr
		}
		if stat.Mode&unix.S_IFMT == unix.S_IFDIR {
			stat.Mode = unix.S_IFREG | 0o600
			stat.Nlink = 1
			stat.Size = agentStandaloneOwnerMax
		}

		return nil
	}

	owner, err := loadAgentStandaloneOwner(directory, 62951, ownerUID, ownerGID)
	require.ErrorIs(t, err, unix.EISDIR)
	require.Equal(t, agentStandaloneOwner{}, owner)
}

// TestAgentStandaloneResDomainPublicationRefusesBytesItDidNotWrite proves the
// domain record publication compares the bytes that actually landed under
// domain.json against the bytes it meant to publish, and refuses when they
// differ. The record names the authority every later claim is measured against,
// so a publication that trusted its own intent would let a record nobody wrote
// become the authority of record.
func TestAgentStandaloneResDomainPublicationRefusesBytesItDidNotWrite(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	record, err := currentAgentAuthorityDomain(directory)
	require.NoError(t, err)
	record.AuthorityID = "0123456789abcdef0123456789abcdef"
	forged := record
	forged.AuthorityID = "fedcba9876543210fedcba9876543210"
	intended, err := json.Marshal(record)
	require.NoError(t, err)
	substitute, err := json.Marshal(forged)
	require.NoError(t, err)
	require.Len(t, substitute, len(intended), "the substitute must occupy the same bytes as the intended record")

	previous := agentStandaloneCloseTemporary
	t.Cleanup(func() { agentStandaloneCloseTemporary = previous })
	substituted := false
	agentStandaloneCloseTemporary = func(file *os.File) error {
		if !substituted {
			substituted = true
			written, writeErr := file.WriteAt(append(substitute, '\n'), 0)
			require.NoError(t, writeErr)
			require.Equal(t, len(substitute)+1, written)
		}

		return previous(file)
	}

	err = replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, record)
	require.True(t, substituted, "the publication never closed its temporary")
	require.ErrorContains(t, err, "published agent authority record payload changed")
	recordPath := filepath.Join(directory.Name(), "domain.json")
	published, readErr := os.ReadFile(recordPath)
	require.NoError(t, readErr)
	require.Equal(t, string(substitute)+"\n", string(published))
	// The substitution kept the very inode the publication created, so the
	// trusted-inode gate that follows would have accepted it. The byte
	// comparison is the only thing standing between it and the authority.
	var named unix.Stat_t
	require.NoError(t, unix.Stat(recordPath, &named))
	require.EqualValues(t, unix.S_IFREG|0o600, named.Mode)
	require.EqualValues(t, 1, named.Nlink)
	require.Equal(t, ownerUID, named.Uid)
	require.Equal(t, ownerGID, named.Gid)
}
