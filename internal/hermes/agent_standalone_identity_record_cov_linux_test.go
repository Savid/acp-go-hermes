//go:build linux

package hermes

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// agentStandaloneCovWriteRegistryFile plants a registry entry with the exact
// metadata the trusted-inode gate demands, so a case that wants to prove a
// payload refusal is never rejected earlier for its mode or ownership.
func agentStandaloneCovWriteRegistryFile(t *testing.T, directory *os.File, name, payload string) string {
	t.Helper()
	path := filepath.Join(directory.Name(), name)
	require.NoError(t, os.WriteFile(path, []byte(payload), 0o600))
	require.NoError(t, os.Chmod(path, 0o600))

	return path
}

// agentStandaloneCovActiveMarker renders a v2 ACTIVE marker with caller-chosen
// owner digest, lease id and paths array so each case can corrupt exactly one
// field.
func agentStandaloneCovActiveMarker(uid, gid uint32, ownerDigest, leaseID, paths string) string {
	return `{"version":2,"uid":` + strconv.FormatUint(uint64(uid), 10) +
		`,"gid":` + strconv.FormatUint(uint64(gid), 10) +
		`,"ownerDigest":"` + ownerDigest + `","state":"active","leaseId":"` + leaseID +
		`","paths":` + paths + `}`
}

// TestAgentStandaloneCovMarkerRefusesEveryMalformedPayload proves the durable
// marker loader refuses each class of tampered quarantine record with its own
// named reason, and that no refusal leaves a decoded marker behind. A marker
// that decoded loosely would let a tampered file re-describe an identity's
// disposition, so each refusal is asserted by its distinct message rather than
// by "an error happened".
func TestAgentStandaloneCovMarkerRefusesEveryMalformedPayload(t *testing.T) {
	const uid, gid = uint32(62301), uint32(62302)
	const lease = "0123456789abcdef0123456789abcdef"
	manyPaths := make([]string, 0, 129)
	for index := 0; index < 129; index++ {
		manyPaths = append(manyPaths,
			`{"base":"/srv/hermes","segments":["p`+strconv.Itoa(index)+`"],"action":"revoke-path","rootDev":1,"rootIno":1}`,
		)
	}
	for _, testCase := range []struct {
		name    string
		payload string
		want    string
	}{
		{
			name:    "not utf-8",
			payload: "\xff\xfe\n",
			want:    "marker is not UTF-8",
		},
		{
			name:    "not a json object",
			payload: "[1,2]\n",
			want:    "cannot unmarshal array",
		},
		{
			name:    "unknown field",
			payload: `{"version":2,"uid":62301,"gid":62302,"ownerDigest":"k","state":"clean-ready","surprise":1}`,
			want:    `unknown field "surprise"`,
		},
		{
			name:    "empty owner digest",
			payload: agentStandaloneCovActiveMarker(uid, gid, "", lease, "[]"),
			want:    "marker is incomplete",
		},
		{
			name:    "control character owner digest",
			payload: agentStandaloneCovActiveMarker(uid, gid, "a\\u0001b", lease, "[]"),
			want:    "marker is incomplete",
		},
		{
			name:    "active marker without lease and paths",
			payload: `{"version":2,"uid":62301,"gid":62302,"ownerDigest":"k","state":"active"}`,
			want:    "ACTIVE marker lacks exact v2 fields",
		},
		{
			name:    "uppercase lease id",
			payload: agentStandaloneCovActiveMarker(uid, gid, "k", strings.ToUpper(lease), "[]"),
			want:    "ACTIVE marker lease id is invalid",
		},
		{
			name:    "unknown state",
			payload: `{"version":2,"uid":62301,"gid":62302,"ownerDigest":"k","state":"quarantined"}`,
			want:    "marker state is invalid",
		},
		{
			name:    "too many paths",
			payload: agentStandaloneCovActiveMarker(uid, gid, "k", lease, "["+strings.Join(manyPaths, ",")+"]"),
			want:    "too many paths",
		},
		{
			name: "path object missing a field",
			payload: agentStandaloneCovActiveMarker(uid, gid, "k", lease,
				`[{"base":"/srv/hermes","segments":["a"],"action":"revoke-path","rootDev":1}]`,
			),
			want: "invalid marker path 0 schema",
		},
		{
			name: "path without a root inode",
			payload: agentStandaloneCovActiveMarker(uid, gid, "k", lease,
				`[{"base":"/srv/hermes","segments":["a"],"action":"revoke-path","rootDev":1,"rootIno":0}]`,
			),
			want: "invalid marker path 0: manifest path is incomplete",
		},
		{
			name: "duplicated path",
			payload: agentStandaloneCovActiveMarker(uid, gid, "k", lease,
				`[{"base":"/srv/hermes","segments":["a"],"action":"revoke-path","rootDev":1,"rootIno":1},`+
					`{"base":"/srv/hermes","segments":["a"],"action":"revoke-tree","rootDev":1,"rootIno":1}]`,
			),
			want: `marker path "/srv/hermes/a" is duplicated with actions "revoke-path" and "revoke-tree"`,
		},
		{
			name: "removal overlapping a retained subtree",
			payload: agentStandaloneCovActiveMarker(uid, gid, "k", lease,
				`[{"base":"/srv/hermes","segments":["a"],"action":"remove-path","rootDev":1,"rootIno":1},`+
					`{"base":"/srv/hermes","segments":["a","b"],"action":"revoke-path","rootDev":1,"rootIno":1}]`,
			),
			want: `marker removal path "/srv/hermes/a/b" conflicts with "/srv/hermes/a"`,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			directory := openAgentStandaloneTestDirectory(t)
			ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
			agentStandaloneCovWriteRegistryFile(t, directory, "62301.quarantine", testCase.payload)

			marker, err := loadAgentStandaloneMarker(directory, uid, ownerUID, ownerGID)
			require.ErrorContains(t, err, testCase.want)
			require.Equal(t, agentStandaloneMarker{}, marker)
		})
	}
}

// TestAgentStandaloneCovMarkerAcceptsDisjointManifestPaths proves the loader
// accepts a manifest whose paths are distinct and non-overlapping, so the
// refusals above are attributable to the corruption each case introduced
// rather than to the manifest shape itself.
func TestAgentStandaloneCovMarkerAcceptsDisjointManifestPaths(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	const uid, gid = uint32(62303), uint32(62304)
	agentStandaloneCovWriteRegistryFile(t, directory, "62303.quarantine",
		agentStandaloneCovActiveMarker(uid, gid, "disjoint", "0123456789abcdef0123456789abcdef",
			`[{"base":"/srv/hermes","segments":["a"],"action":"remove-path","rootDev":7,"rootIno":8},`+
				`{"base":"/srv/hermes","segments":["b"],"action":"revoke-tree","rootDev":7,"rootIno":8}]`,
		),
	)

	marker, err := loadAgentStandaloneMarker(directory, uid, ownerUID, ownerGID)
	require.NoError(t, err)
	require.Equal(t, "active", marker.State)
	require.Len(t, marker.Paths, 2)
	require.Equal(t, "remove-path", marker.Paths[0].Action)
	require.Equal(t, "revoke-tree", marker.Paths[1].Action)
}

// TestAgentStandaloneCovManifestPathValidation proves the manifest path
// validator names exactly one clean absolute base, at least one traversal-free
// segment, a bound root inode and one of the three known actions. Anything
// looser would let a marker instruct a cleanup pass to walk out of the state
// root it claims.
func TestAgentStandaloneCovManifestPathValidation(t *testing.T) {
	valid := agentStandaloneManifestPath{
		Base: "/srv/hermes", Segments: []string{"a"}, Action: "revoke-path", RootDev: 1, RootIno: 2,
	}
	require.NoError(t, validateAgentStandaloneManifestPath(valid))
	filesystemRoot := valid
	filesystemRoot.Base = "/"
	require.NoError(t, validateAgentStandaloneManifestPath(filesystemRoot))
	for _, action := range []string{"revoke-tree", "remove-path"} {
		accepted := valid
		accepted.Action = action
		require.NoError(t, validateAgentStandaloneManifestPath(accepted))
	}

	for _, testCase := range []struct {
		name string
		path agentStandaloneManifestPath
		want string
	}{
		{
			name: "relative base",
			path: agentStandaloneManifestPath{
				Base: "srv/hermes", Segments: []string{"a"}, Action: "revoke-path", RootDev: 1, RootIno: 2,
			},
			want: "manifest base is not a clean absolute path",
		},
		{
			name: "no segments",
			path: agentStandaloneManifestPath{
				Base: "/srv/hermes", Segments: nil, Action: "revoke-path", RootDev: 1, RootIno: 2,
			},
			want: "manifest path is incomplete",
		},
		{
			name: "no root device",
			path: agentStandaloneManifestPath{
				Base: "/srv/hermes", Segments: []string{"a"}, Action: "revoke-path", RootDev: 0, RootIno: 2,
			},
			want: "manifest path is incomplete",
		},
		{
			name: "parent traversal segment",
			path: agentStandaloneManifestPath{
				Base: "/srv/hermes", Segments: []string{".."}, Action: "revoke-path", RootDev: 1, RootIno: 2,
			},
			want: "manifest path segment is invalid",
		},
		{
			name: "separator inside a segment",
			path: agentStandaloneManifestPath{
				Base: "/srv/hermes", Segments: []string{"a/b"}, Action: "revoke-path", RootDev: 1, RootIno: 2,
			},
			want: "manifest path segment is invalid",
		},
		{
			name: "control character segment",
			path: agentStandaloneManifestPath{
				Base: "/srv/hermes", Segments: []string{"a\x01b"}, Action: "revoke-path", RootDev: 1, RootIno: 2,
			},
			want: "manifest path segment contains a control character",
		},
		{
			name: "unknown action",
			path: agentStandaloneManifestPath{
				Base: "/srv/hermes", Segments: []string{"a"}, Action: "delete-everything", RootDev: 1, RootIno: 2,
			},
			want: "manifest path action is invalid",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			require.ErrorContains(t, validateAgentStandaloneManifestPath(testCase.path), testCase.want)
		})
	}
}

// TestAgentStandaloneCovOwnerRecordRefusesNonCanonicalAndInvalidBindings
// proves the immutable owner binding is only readable when it is the exact
// canonical encoding of a complete standalone tuple. A loose read here would
// let a rewritten owner file re-point a UID at another provider, state root or
// group.
func TestAgentStandaloneCovOwnerRecordRefusesNonCanonicalAndInvalidBindings(t *testing.T) {
	const uid = uint32(62311)
	canonical := agentStandaloneOwner{
		Version: 1, UID: uid, GID: 62312, Kind: agentStandaloneOwnerKind,
		Provider: agentStandaloneOwnerID, OwnerID: "canonical-owner",
		StateRoot: agentStandaloneStateRoot{Path: "/srv/hermes/canonical", Dev: 5, Ino: 6},
	}
	encode := func(t *testing.T, owner agentStandaloneOwner) string {
		t.Helper()
		payload, err := json.Marshal(owner)
		require.NoError(t, err)

		return string(payload) + "\n"
	}
	missingGID := canonical
	missingGID.GID = 0
	controlStateRoot := canonical
	controlStateRoot.StateRoot.Path = "/srv/hermes/\x01"

	for _, testCase := range []struct {
		name    string
		payload string
		want    string
	}{
		{
			name:    "empty file",
			payload: "",
			want:    "is not its trusted bounded named inode",
		},
		{
			name: "missing field",
			payload: `{"version":1,"uid":62311,"gid":62312,"kind":"standalone-provider",` +
				`"provider":"github.com/savid/acp-go-hermes","ownerId":"x"}` + "\n",
			want: "object does not contain its exact required fields",
		},
		{
			name: "state root with extra field",
			payload: `{"version":1,"uid":62311,"gid":62312,"kind":"standalone-provider",` +
				`"provider":"github.com/savid/acp-go-hermes","ownerId":"x",` +
				`"stateRoot":{"path":"/srv/hermes/x","dev":1,"ino":2,"extra":3}}` + "\n",
			want: "invalid standalone state root",
		},
		{
			name: "uid encoded as a string",
			payload: `{"version":1,"uid":"62311","gid":62312,"kind":"standalone-provider",` +
				`"provider":"github.com/savid/acp-go-hermes","ownerId":"x",` +
				`"stateRoot":{"path":"/srv/hermes/x","dev":1,"ino":2}}` + "\n",
			want: "cannot unmarshal string",
		},
		{
			name:    "non-canonical spacing",
			payload: strings.Replace(encode(t, canonical), `{"version":1,`, `{"version":1, `, 1),
			want:    "not canonical compact JSON with one newline",
		},
		{
			name:    "no trailing newline",
			payload: strings.TrimSuffix(encode(t, canonical), "\n"),
			want:    "not canonical compact JSON with one newline",
		},
		{
			name:    "unbound group",
			payload: encode(t, missingGID),
			want:    "standalone owner record is invalid",
		},
		{
			name:    "control character state root",
			payload: encode(t, controlStateRoot),
			want:    "standalone owner record is invalid",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			directory := openAgentStandaloneTestDirectory(t)
			ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
			agentStandaloneCovWriteRegistryFile(t, directory, "62311.owner", testCase.payload)

			owner, err := loadAgentStandaloneOwner(directory, uid, ownerUID, ownerGID)
			require.ErrorContains(t, err, testCase.want)
			require.Equal(t, agentStandaloneOwner{}, owner)
		})
	}

	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	agentStandaloneCovWriteRegistryFile(t, directory, "62311.owner", encode(t, canonical))
	loaded, err := loadAgentStandaloneOwner(directory, uid, ownerUID, ownerGID)
	require.NoError(t, err)
	require.Equal(t, canonical, loaded)
}
