//go:build linux

package hermes

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/stretchr/testify/require"
)

// agentStandaloneCovOwner builds a well-formed standalone owner tuple so a case
// only has to state the field it wants to collide.
func agentStandaloneCovOwner(uid, gid uint32, ownerID, stateRoot string, dev, ino uint64) agentStandaloneOwner {
	return agentStandaloneOwner{
		Version: 1, UID: uid, GID: gid, Kind: agentStandaloneOwnerKind,
		Provider: agentStandaloneOwnerID, OwnerID: ownerID,
		StateRoot: agentStandaloneStateRoot{Path: stateRoot, Dev: dev, Ino: ino},
	}
}

// agentStandaloneCovWriteOwner plants an owner binding in its canonical
// on-disk encoding without going through the publication path, so a case can
// stage registry states the publication path itself would refuse to create.
func agentStandaloneCovWriteOwner(t *testing.T, directory *os.File, owner agentStandaloneOwner) {
	t.Helper()
	payload, err := json.Marshal(owner)
	require.NoError(t, err)
	agentStandaloneCovWriteRegistryFile(
		t, directory, strconv.FormatUint(uint64(owner.UID), 10)+".owner", string(payload)+"\n",
	)
}

// agentStandaloneCovWriteActiveMarker plants the retained ACTIVE marker that
// matches an owner exactly.
func agentStandaloneCovWriteActiveMarker(t *testing.T, directory *os.File, owner agentStandaloneOwner) {
	t.Helper()
	key := agentStandaloneSessionKey(owner)
	agentStandaloneCovWriteRegistryFile(
		t, directory, strconv.FormatUint(uint64(owner.UID), 10)+".quarantine",
		agentStandaloneCovActiveMarker(owner.UID, owner.GID, key, "0123456789abcdef0123456789abcdef", "[]")+"\n",
	)
}

// agentStandaloneCovWriteCleanMarker plants an ownerless CLEAN marker.
func agentStandaloneCovWriteCleanMarker(t *testing.T, directory *os.File, uid, gid uint32, key string) {
	t.Helper()
	agentStandaloneCovWriteRegistryFile(
		t, directory, strconv.FormatUint(uint64(uid), 10)+".quarantine",
		`{"version":2,"uid":`+strconv.FormatUint(uint64(uid), 10)+
			`,"gid":`+strconv.FormatUint(uint64(gid), 10)+
			`,"ownerDigest":"`+key+`","state":"clean-ready"}`+"\n",
	)
}

// agentStandaloneCovPermanentLock creates a permanent registry lock and
// immediately releases it, leaving only the named inode behind.
func agentStandaloneCovPermanentLock(t *testing.T, directory *os.File, name string) {
	t.Helper()
	lock := createAgentStandaloneTestLock(t, directory, name, uint32(os.Geteuid()), uint32(os.Getegid()))
	require.NoError(t, lock.Close())
}

// TestAgentStandaloneCovAuthorityRootAuditRefusesEveryUnaccountableEntry
// proves the registry audit accounts for every entry it finds and refuses the
// whole registry when it cannot. The audit is what decides whether a claim may
// mint a fresh authority or adopt an existing one, so an entry it silently
// skipped would be an entry an attacker could leave behind.
func TestAgentStandaloneCovAuthorityRootAuditRefusesEveryUnaccountableEntry(t *testing.T) {
	suffix := agentStandaloneCovSuffix
	for _, testCase := range []struct {
		name                 string
		requireEmpty         bool
		allowCleanup         bool
		allowOwnerlessActive bool
		expired              bool
		setup                func(t *testing.T, directory *os.File)
		want                 string
	}{
		{
			name:    "expired budget",
			expired: true,
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteRegistryFile(t, directory, "domain.json", "{}\n")
			},
			want: "exceeded 30 seconds",
		},
		{
			name: "owner name is not a uid",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteRegistryFile(t, directory, "bad.owner", "{}\n")
			},
			want: `invalid standalone owner name "bad.owner"`,
		},
		{
			name: "unreadable owner binding",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteRegistryFile(t, directory, "62601.owner", "not json\n")
			},
			want: "invalid character",
		},
		{
			name:         "owners lock in a registry with no domain record",
			requireEmpty: true,
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovPermanentLock(t, directory, "owners.lock")
			},
			want: "record is missing but permanent owners.lock exists",
		},
		{
			name: "owners lock with wrong mode",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovPermanentLock(t, directory, "owners.lock")
				require.NoError(t, os.Chmod(filepath.Join(directory.Name(), "owners.lock"), 0o644))
			},
			want: "mode",
		},
		{
			name: "owner temporary",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteRegistryFile(t, directory, "62603.owner.next-"+suffix, "partial")
			},
			want: "standalone owner temporary requires registry cleanup",
		},
		{
			name: "domain temporary without exclusive cleanup",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteRegistryFile(t, directory, "domain.json.next-"+suffix, "partial")
			},
			want: "requires domain-exclusive cleanup",
		},
		{
			name:         "untrusted domain temporary under exclusive cleanup",
			allowCleanup: true,
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteRegistryFile(t, directory, "domain.json.next-"+suffix, "partial")
				require.NoError(t, os.Chmod(filepath.Join(directory.Name(), "domain.json.next-"+suffix), 0o644))
			},
			want: "not a trusted bounded regular file",
		},
		{
			name: "probe temporary without exclusive cleanup",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteRegistryFile(t, directory, ".authority-probe-"+suffix, "partial")
			},
			want: "requires domain-exclusive cleanup",
		},
		{
			name:         "malformed probe temporary under exclusive cleanup",
			allowCleanup: true,
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteRegistryFile(t, directory, ".authority-probe-nope", "partial")
			},
			want: "invalid name",
		},
		{
			name: "malformed marker temporary",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteRegistryFile(t, directory, "bad.quarantine.next-"+suffix, "partial")
			},
			want: "invalid name",
		},
		{
			name: "untrusted marker temporary",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteRegistryFile(t, directory, "62605.quarantine.next-"+suffix, "partial")
				require.NoError(t, os.Chmod(filepath.Join(directory.Name(), "62605.quarantine.next-"+suffix), 0o644))
			},
			want: "not a trusted bounded regular file",
		},
		{
			name:         "live marker temporary under exclusive cleanup",
			allowCleanup: true,
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteRegistryFile(t, directory, "62607.quarantine.next-"+suffix, "partial")
				held := createAgentStandaloneTestLock(
					t, directory, "62607.lock", uint32(os.Geteuid()), uint32(os.Getegid()),
				)
				require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB))
			},
			want: "standalone marker temporary has a live UID holder",
		},
		{
			name:         "owner binding in a registry with no domain record",
			requireEmpty: true,
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteOwner(t, directory,
					agentStandaloneCovOwner(62609, 62610, "no-domain", "/srv/hermes/no-domain", 1, 2),
				)
			},
			want: "record is missing but a permanent owner binding exists",
		},
		{
			name: "marker name is not a uid",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteRegistryFile(t, directory, "bad.quarantine", "{}\n")
			},
			want: `invalid standalone marker name "bad.quarantine"`,
		},
		{
			name:         "durable marker in a registry with no domain record",
			requireEmpty: true,
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteCleanMarker(t, directory, 62611, 62612, "no-domain-marker")
			},
			want: "record is missing but a durable marker exists",
		},
		{
			name:         "prior lock in a registry with no domain record",
			requireEmpty: true,
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovPermanentLock(t, directory, "62613.lock")
			},
			want: `record is missing but root contains prior lock "62613.lock"`,
		},
		{
			name: "malformed affinity lock",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovPermanentLock(t, directory, "affinity-zzzz.lock")
			},
			want: "invalid affinity lock",
		},
		{
			name: "lock that names no uid",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovPermanentLock(t, directory, "scratch.lock")
			},
			want: `unknown lock "scratch.lock"`,
		},
		{
			name: "uid lock with wrong mode",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovPermanentLock(t, directory, "owners.lock")
				agentStandaloneCovPermanentLock(t, directory, "62615.lock")
				require.NoError(t, os.Chmod(filepath.Join(directory.Name(), "62615.lock"), 0o644))
			},
			want: "mode",
		},
		{
			name: "entry that belongs to nothing",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteRegistryFile(t, directory, "leftover", "x")
			},
			want: `unknown entry "leftover"`,
		},
		{
			name: "registry state without owners lock",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovPermanentLock(t, directory, "62617.lock")
			},
			want: "registry state exists without permanent owners.lock",
		},
		{
			name: "owner whose state root is the registry",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovPermanentLock(t, directory, "owners.lock")
				agentStandaloneCovPermanentLock(t, directory, "62619.lock")
				agentStandaloneCovWriteOwner(t, directory,
					agentStandaloneCovOwner(62619, 62620, "self-rooted", directory.Name(), 1, 2),
				)
			},
			want: "uses the authority registry as its state root",
		},
		{
			name: "owner without its permanent uid lock",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovPermanentLock(t, directory, "owners.lock")
				agentStandaloneCovWriteOwner(t, directory,
					agentStandaloneCovOwner(62621, 62622, "lockless", "/srv/hermes/lockless", 1, 2),
				)
			},
			want: "exists without its permanent UID lock",
		},
		{
			name: "two owners sharing a gid",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovPermanentLock(t, directory, "owners.lock")
				agentStandaloneCovPermanentLock(t, directory, "62623.lock")
				agentStandaloneCovPermanentLock(t, directory, "62625.lock")
				agentStandaloneCovWriteOwner(t, directory,
					agentStandaloneCovOwner(62623, 62624, "gid-a", "/srv/hermes/gid-a", 1, 2),
				)
				agentStandaloneCovWriteOwner(t, directory,
					agentStandaloneCovOwner(62625, 62624, "gid-b", "/srv/hermes/gid-b", 3, 4),
				)
			},
			want: "is duplicated by uids",
		},
		{
			name: "two owners sharing a provider owner id",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovPermanentLock(t, directory, "owners.lock")
				agentStandaloneCovPermanentLock(t, directory, "62627.lock")
				agentStandaloneCovPermanentLock(t, directory, "62629.lock")
				agentStandaloneCovWriteOwner(t, directory,
					agentStandaloneCovOwner(62627, 62628, "shared-owner", "/srv/hermes/owner-a", 1, 2),
				)
				agentStandaloneCovWriteOwner(t, directory,
					agentStandaloneCovOwner(62629, 62630, "shared-owner", "/srv/hermes/owner-b", 3, 4),
				)
			},
			want: "standalone provider owner",
		},
		{
			name: "two owners sharing a state root path",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovPermanentLock(t, directory, "owners.lock")
				agentStandaloneCovPermanentLock(t, directory, "62631.lock")
				agentStandaloneCovPermanentLock(t, directory, "62633.lock")
				agentStandaloneCovWriteOwner(t, directory,
					agentStandaloneCovOwner(62631, 62632, "path-a", "/srv/hermes/shared", 1, 2),
				)
				agentStandaloneCovWriteOwner(t, directory,
					agentStandaloneCovOwner(62633, 62634, "path-b", "/srv/hermes/shared", 3, 4),
				)
			},
			want: "state root path",
		},
		{
			name: "two owners sharing a state root inode",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovPermanentLock(t, directory, "owners.lock")
				agentStandaloneCovPermanentLock(t, directory, "62635.lock")
				agentStandaloneCovPermanentLock(t, directory, "62637.lock")
				agentStandaloneCovWriteOwner(t, directory,
					agentStandaloneCovOwner(62635, 62636, "inode-a", "/srv/hermes/inode-a", 9, 9),
				)
				agentStandaloneCovWriteOwner(t, directory,
					agentStandaloneCovOwner(62637, 62638, "inode-b", "/srv/hermes/inode-b", 9, 9),
				)
			},
			want: "state root inode is duplicated",
		},
		{
			name: "durable marker without its permanent uid lock",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovPermanentLock(t, directory, "owners.lock")
				agentStandaloneCovPermanentLock(t, directory, "62639.lock")
				agentStandaloneCovWriteOwner(t, directory,
					agentStandaloneCovOwner(62639, 62640, "anchor", "/srv/hermes/anchor", 1, 2),
				)
				agentStandaloneCovWriteCleanMarker(t, directory, 62641, 62642, "lockless-marker")
			},
			want: "durable marker uid 62641 exists without its permanent UID lock",
		},
		{
			name: "durable marker reusing a bound gid",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovPermanentLock(t, directory, "owners.lock")
				agentStandaloneCovPermanentLock(t, directory, "62643.lock")
				agentStandaloneCovPermanentLock(t, directory, "62645.lock")
				agentStandaloneCovWriteOwner(t, directory,
					agentStandaloneCovOwner(62643, 62644, "bound-gid", "/srv/hermes/bound-gid", 1, 2),
				)
				agentStandaloneCovWriteCleanMarker(t, directory, 62645, 62644, "gid-reuse")
			},
			want: "gid 62644 is duplicated by uids",
		},
		{
			name: "owner with an incompatible retained marker",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovPermanentLock(t, directory, "owners.lock")
				agentStandaloneCovPermanentLock(t, directory, "62647.lock")
				agentStandaloneCovWriteOwner(t, directory,
					agentStandaloneCovOwner(62647, 62648, "stale-marker", "/srv/hermes/stale-marker", 1, 2),
				)
				agentStandaloneCovWriteCleanMarker(t, directory, 62647, 62648, "not-the-session-key")
			},
			want: "incompatible retained marker",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			directory := openAgentStandaloneTestDirectory(t)
			ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
			testCase.setup(t, directory)
			deadline := time.Now().Add(time.Second)
			if testCase.expired {
				deadline = time.Now().Add(-time.Millisecond)
			}

			err := auditAgentStandaloneAuthorityRoot(
				directory, ownerUID, ownerGID,
				testCase.requireEmpty, testCase.allowCleanup, testCase.allowOwnerlessActive,
				deadline, nil, nil,
			)
			require.ErrorContains(t, err, testCase.want)
		})
	}
}

// TestAgentStandaloneCovAuthorityRootAuditCleansTrustedTemporaries proves the
// domain-exclusive audit removes exactly the domain-record and authority-probe
// temporaries it can account for, and then still accepts the registry. Without
// this the refusal cases above could pass merely because the audit rejects
// every temporary.
func TestAgentStandaloneCovAuthorityRootAuditCleansTrustedTemporaries(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	domainTemporary := agentStandaloneCovWriteRegistryFile(
		t, directory, "domain.json.next-"+agentStandaloneCovSuffix, "partial",
	)
	probeTemporary := agentStandaloneCovWriteRegistryFile(
		t, directory, ".authority-probe-"+agentStandaloneCovSuffix, "partial",
	)

	require.NoError(t, auditAgentStandaloneAuthorityRoot(
		directory, ownerUID, ownerGID, false, true, false, time.Now().Add(time.Second), nil, nil,
	))
	require.NoFileExists(t, domainTemporary)
	require.NoFileExists(t, probeTemporary)
}

// TestAgentStandaloneCovAuthorityRootAuditAcceptsACoherentRegistry proves a
// registry whose owners, markers and locks all agree is accepted, so every
// refusal above is attributable to the single inconsistency it introduced.
func TestAgentStandaloneCovAuthorityRootAuditAcceptsACoherentRegistry(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	agentStandaloneCovPermanentLock(t, directory, "owners.lock")
	agentStandaloneCovPermanentLock(t, directory, "62651.lock")
	agentStandaloneCovPermanentLock(t, directory, "62653.lock")
	first := agentStandaloneCovOwner(62651, 62652, "coherent-a", "/srv/hermes/coherent-a", 11, 12)
	second := agentStandaloneCovOwner(62653, 62654, "coherent-b", "/srv/hermes/coherent-b", 13, 14)
	agentStandaloneCovWriteOwner(t, directory, first)
	agentStandaloneCovWriteOwner(t, directory, second)
	agentStandaloneCovWriteActiveMarker(t, directory, first)

	require.NoError(t, auditAgentStandaloneAuthorityRoot(
		directory, ownerUID, ownerGID, false, false, false, time.Now().Add(time.Second), nil, nil,
	))
}

// TestAgentStandaloneCovOwnerUniquenessRefusesEveryCollision proves the
// uniqueness sweep refuses a claim whose gid, provider owner id or state root
// is already bound to another uid, refuses a durable marker that reserves the
// claimed gid, and refuses any registry entry it cannot account for. These are
// the checks that stop two standalone providers sharing one identity.
func TestAgentStandaloneCovOwnerUniquenessRefusesEveryCollision(t *testing.T) {
	want := agentStandaloneCovOwner(62501, 62502, "cov-uniqueness", "/srv/hermes/cov-uniqueness", 71, 72)
	suffix := agentStandaloneCovSuffix
	for _, testCase := range []struct {
		name    string
		expired bool
		setup   func(t *testing.T, directory *os.File)
		want    string
	}{
		{
			name:    "expired budget",
			expired: true,
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteRegistryFile(t, directory, "domain.json", "{}\n")
			},
			want: "exceeded 30 seconds",
		},
		{
			name: "malformed owner temporary",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteRegistryFile(t, directory, "bad.owner.next-"+suffix, "partial")
			},
			want: "invalid name",
		},
		{
			name: "malformed marker temporary",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteRegistryFile(t, directory, "bad.quarantine.next-"+suffix, "partial")
			},
			want: "invalid name",
		},
		{
			name: "untrusted marker temporary",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteRegistryFile(t, directory, "62503.quarantine.next-"+suffix, "partial")
				require.NoError(t, os.Chmod(filepath.Join(directory.Name(), "62503.quarantine.next-"+suffix), 0o644))
			},
			want: "not a trusted bounded regular file",
		},
		{
			name: "owner name is not a uid",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteRegistryFile(t, directory, "bad.owner", "{}\n")
			},
			want: `invalid standalone owner name "bad.owner"`,
		},
		{
			name: "unreadable owner binding",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteRegistryFile(t, directory, "62505.owner", "not json\n")
			},
			want: "invalid character",
		},
		{
			name: "same uid bound to another tuple",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteOwner(t, directory,
					agentStandaloneCovOwner(62501, 62502, "someone-else", "/srv/hermes/someone-else", 81, 82),
				)
			},
			want: "standalone uid 62501 is already bound to another tuple",
		},
		{
			name: "gid already bound",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteOwner(t, directory,
					agentStandaloneCovOwner(62507, 62502, "gid-holder", "/srv/hermes/gid-holder", 81, 82),
				)
			},
			want: "standalone gid 62502 is already bound to uid 62507",
		},
		{
			name: "provider owner id already bound",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteOwner(t, directory,
					agentStandaloneCovOwner(62509, 62510, "cov-uniqueness", "/srv/hermes/other-root", 81, 82),
				)
			},
			want: `standalone provider owner "cov-uniqueness" is already bound to uid 62509`,
		},
		{
			name: "state root path already bound",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteOwner(t, directory,
					agentStandaloneCovOwner(62511, 62512, "path-holder", "/srv/hermes/cov-uniqueness", 81, 82),
				)
			},
			want: "standalone state root is already bound to uid 62511",
		},
		{
			name: "state root inode already bound",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteOwner(t, directory,
					agentStandaloneCovOwner(62513, 62514, "inode-holder", "/srv/hermes/other-inode-root", 71, 72),
				)
			},
			want: "standalone state root is already bound to uid 62513",
		},
		{
			name: "marker name is not a uid",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteRegistryFile(t, directory, "bad.quarantine", "{}\n")
			},
			want: `invalid agent identity marker name "bad.quarantine"`,
		},
		{
			name: "unreadable durable marker",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteRegistryFile(t, directory, "62515.quarantine", "not json\n")
			},
			want: "invalid character",
		},
		{
			name: "gid reserved by a durable marker",
			setup: func(t *testing.T, directory *os.File) {
				t.Helper()
				agentStandaloneCovWriteCleanMarker(t, directory, 62517, 62502, "reserving-marker")
			},
			want: "standalone gid 62502 is reserved by durable uid 62517 marker",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			directory := openAgentStandaloneTestDirectory(t)
			ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
			testCase.setup(t, directory)
			deadline := time.Now().Add(time.Second)
			if testCase.expired {
				deadline = time.Now().Add(-time.Millisecond)
			}

			require.ErrorContains(t, validateAgentStandaloneOwnerUniqueness(
				directory, want, ownerUID, ownerGID, deadline, nil, nil,
			), testCase.want)
		})
	}
}

// TestAgentStandaloneCovOwnerUniquenessAcceptsDisjointRegistryState proves the
// sweep accepts a registry that already holds this exact tuple and unrelated
// owners and markers, so the refusals above are attributable to the collision
// each case introduced.
func TestAgentStandaloneCovOwnerUniquenessAcceptsDisjointRegistryState(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	want := agentStandaloneCovOwner(62501, 62502, "cov-uniqueness", "/srv/hermes/cov-uniqueness", 71, 72)
	agentStandaloneCovWriteOwner(t, directory, want)
	agentStandaloneCovWriteOwner(t, directory,
		agentStandaloneCovOwner(62519, 62520, "disjoint", "/srv/hermes/disjoint", 91, 92),
	)
	agentStandaloneCovWriteCleanMarker(t, directory, 62521, 62522, "disjoint-marker")

	require.NoError(t, validateAgentStandaloneOwnerUniqueness(
		directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
	))
}

// TestAgentStandaloneCovPriorDispositionRequiresTheExactRetainedMarker proves
// a returning owner is only admitted when its retained marker is the exact
// ACTIVE marker for its own session key, that a missing marker is fine, and
// that an unreadable marker is a refusal rather than "no marker".
func TestAgentStandaloneCovPriorDispositionRequiresTheExactRetainedMarker(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	owner := agentStandaloneCovOwner(62531, 62532, "prior-disposition", "/srv/hermes/prior-disposition", 51, 52)

	t.Run("no marker", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		require.NoError(t, validateAgentStandalonePriorDisposition(directory, owner, ownerUID, ownerGID))
	})

	t.Run("unreadable marker", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovWriteRegistryFile(t, directory, "62531.quarantine", "not json\n")
		require.ErrorContains(t,
			validateAgentStandalonePriorDisposition(directory, owner, ownerUID, ownerGID),
			"invalid character",
		)
	})

	t.Run("marker for another session", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovWriteCleanMarker(t, directory, owner.UID, owner.GID, "another-session")
		require.ErrorContains(t,
			validateAgentStandalonePriorDisposition(directory, owner, ownerUID, ownerGID),
			"incompatible retained ACTIVE marker",
		)
	})

	t.Run("exact retained marker", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovWriteActiveMarker(t, directory, owner)
		require.NoError(t, validateAgentStandalonePriorDisposition(directory, owner, ownerUID, ownerGID))
	})
}

// TestAgentStandaloneCovOwnerClaimRefusesEveryUnsafeRegistryState proves the
// owner claim refuses a UID already bound to a different tuple, refuses an
// unreadable binding, refuses to invent a binding that vanished under its own
// lock, refuses an ownerless UID that still carries disposition state, and
// refuses when the vacancy proof or the claim budget says no. Each refusal must
// leave the registry unchanged.
func TestAgentStandaloneCovOwnerClaimRefusesEveryUnsafeRegistryState(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	want := agentStandaloneCovOwner(62541, 62542, "cov-claim", "/srv/hermes/cov-claim", 61, 62)

	t.Run("uid bound to another tuple", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovWriteOwner(t, directory,
			agentStandaloneCovOwner(62541, 62542, "another-claim", "/srv/hermes/another-claim", 63, 64),
		)

		require.ErrorContains(t, claimAgentStandaloneOwner(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		), "permanently bound to another standalone owner")
	})

	t.Run("existing binding with a colliding peer", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovWriteOwner(t, directory, want)
		agentStandaloneCovWriteOwner(t, directory,
			agentStandaloneCovOwner(62543, 62542, "gid-collider", "/srv/hermes/gid-collider", 65, 66),
		)

		require.ErrorContains(t, claimAgentStandaloneOwner(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		), "already bound to uid 62543")
	})

	t.Run("existing binding is admitted with its retained marker", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovWriteOwner(t, directory, want)
		agentStandaloneCovWriteActiveMarker(t, directory, want)

		require.NoError(t, claimAgentStandaloneOwner(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		))
	})

	t.Run("unreadable binding", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovWriteRegistryFile(t, directory, "62541.owner", "not json\n")

		require.ErrorContains(t, claimAgentStandaloneOwner(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		), "invalid character")
	})

	t.Run("binding vanished under its own lock", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)

		require.ErrorContains(t, claimAgentStandaloneOwner(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		), "disappeared while its immutable binding was locked")
		require.NoFileExists(t, filepath.Join(directory.Name(), "62541.owner"))
	})

	t.Run("ownerless uid with prior disposition", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovWriteCleanMarker(t, directory, want.UID, want.GID, "prior-state")

		require.ErrorContains(t, claimAgentStandaloneOwner(
			directory, want, ownerUID, ownerGID, false, time.Now().Add(time.Second), nil, nil,
		), "has prior disposition state")
		require.NoFileExists(t, filepath.Join(directory.Name(), "62541.owner"))
	})

	t.Run("unreadable prior disposition", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovWriteRegistryFile(t, directory, "62541.quarantine", "not json\n")

		require.ErrorContains(t, claimAgentStandaloneOwner(
			directory, want, ownerUID, ownerGID, false, time.Now().Add(time.Second), nil, nil,
		), "invalid character")
		require.NoFileExists(t, filepath.Join(directory.Name(), "62541.owner"))
	})

	t.Run("colliding peer blocks a fresh binding", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovWriteOwner(t, directory,
			agentStandaloneCovOwner(62545, 62542, "fresh-collider", "/srv/hermes/fresh-collider", 67, 68),
		)

		require.ErrorContains(t, claimAgentStandaloneOwner(
			directory, want, ownerUID, ownerGID, false, time.Now().Add(time.Second), nil, nil,
		), "already bound to uid 62545")
		require.NoFileExists(t, filepath.Join(directory.Name(), "62541.owner"))
	})

	t.Run("live task blocks a fresh binding", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		wantErr := errors.New("a task still holds the identity")
		previous := agentStandaloneVacancyScan
		agentStandaloneVacancyScan = func(
			uid, gid uint32, _ time.Time, _ <-chan struct{}, _ <-chan os.Signal,
		) error {
			require.Equal(t, want.UID, uid)
			require.Equal(t, want.GID, gid)

			return wantErr
		}
		t.Cleanup(func() { agentStandaloneVacancyScan = previous })

		require.ErrorIs(t, claimAgentStandaloneOwner(
			directory, want, ownerUID, ownerGID, false, time.Now().Add(time.Second), nil, nil,
		), wantErr)
		require.NoFileExists(t, filepath.Join(directory.Name(), "62541.owner"))
	})

	t.Run("budget gone after the vacancy proof", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		previous := agentStandaloneVacancyScan
		agentStandaloneVacancyScan = func(uint32, uint32, time.Time, <-chan struct{}, <-chan os.Signal) error {
			return nil
		}
		t.Cleanup(func() { agentStandaloneVacancyScan = previous })

		require.ErrorContains(t, claimAgentStandaloneOwner(
			directory, want, ownerUID, ownerGID, false, time.Now().Add(-time.Millisecond), nil, nil,
		), "exceeded 30 seconds")
		require.NoFileExists(t, filepath.Join(directory.Name(), "62541.owner"))
	})
}
