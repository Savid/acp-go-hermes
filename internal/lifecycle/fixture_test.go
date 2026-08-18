package lifecycle

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// fixtureDir holds the canonical family reducer battery, copied verbatim. The
// vectors are wire-level expectations: reducing those bytes must reach the stated
// verdict, and a locally adjusted vector would no longer be the family's
// contract.
const fixtureDir = "../../testdata/lifecycle"

// manifest indexes the battery.
type manifest struct {
	Version   int    `json:"version"`
	Extension string `json:"extension"`
	Fixtures  []struct {
		File      string `json:"file"`
		Invariant string `json:"invariant"`
	} `json:"fixtures"`
}

type fixture struct {
	Name       string         `json:"name"`
	Purpose    string         `json:"purpose"`
	Negotiated Negotiated     `json:"negotiated"`
	Input      []fixtureInput `json:"input"`
	// PostRefusal is delivered after the refusal. Fail closed latches, so every
	// one of these must report the same token at the same identity: a consumer
	// that stopped reducing and one that kept going past its own verdict are
	// otherwise indistinguishable.
	PostRefusal []fixtureInput  `json:"postRefusal"`
	Expect      fixtureExpected `json:"expect"`
}

// fixtureInput is either a delivered notification or an out-of-band control event
// with no notification of its own.
type fixtureInput struct {
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	Control string          `json:"control"`
}

type fixtureExpected struct {
	Verdict   string          `json:"verdict"`
	Violation string          `json:"violation"`
	AtInput   *int            `json:"atInput"`
	State     json.RawMessage `json:"state"`
}

func TestFixtureManifestListsEveryVector(t *testing.T) {
	t.Parallel()

	index := loadManifest(t)
	require.Equal(t, Version, index.Version)
	require.Equal(t, MetaKey, index.Extension)

	listed := make(map[string]struct{}, len(index.Fixtures))

	for _, entry := range index.Fixtures {
		listed[entry.File] = struct{}{}

		require.NotEmpty(t, entry.Invariant, entry.File)
	}

	entries, err := os.ReadDir(fixtureDir)
	require.NoError(t, err)

	for _, entry := range entries {
		if entry.Name() == "manifest.json" {
			continue
		}

		require.Contains(t, listed, entry.Name(), "fixture is not listed in the manifest")
	}

	require.Len(t, listed, len(entries)-1)
}

// TestFixtureBatteryPinsEveryViolationToken proves the battery discharges the
// whole closed vocabulary: a token with no vector would be a rule this reducer
// could claim without evidence.
func TestFixtureBatteryPinsEveryViolationToken(t *testing.T) {
	t.Parallel()

	pinned := make(map[ViolationKind]string)

	for _, entry := range loadManifest(t).Fixtures {
		vector := loadFixture(t, entry.File)
		if vector.Expect.Verdict == "fail_closed" {
			pinned[ViolationKind(vector.Expect.Violation)] = entry.File
		}
	}

	for _, token := range violationVocabulary {
		require.Contains(t, pinned, token, "no vector pins %s", token)
	}

	require.Len(t, pinned, len(violationVocabulary))
}

func TestReducerFixtures(t *testing.T) {
	t.Parallel()

	for _, entry := range loadManifest(t).Fixtures {
		t.Run(strings.TrimSuffix(entry.File, ".json"), func(t *testing.T) {
			t.Parallel()

			vector := loadFixture(t, entry.File)
			reducer := NewReducer(Options{Negotiated: vector.Negotiated})
			refusal, refusedAt := driveFixture(t, reducer, vector)

			switch vector.Expect.Verdict {
			case "accepted":
				require.Nil(t, refusal, "the fixture expects every input to reduce")
			case "fail_closed":
				require.NotNil(t, refusal, "the fixture expects a refusal")
				require.Equal(t, ViolationKind(vector.Expect.Violation), refusal.Kind)
				require.NotNil(t, vector.Expect.AtInput)
				require.Equal(t, *vector.Expect.AtInput, refusedAt)
				requireLatched(t, reducer, vector, refusal)
			default:
				t.Fatalf("unknown verdict %q", vector.Expect.Verdict)
			}

			requireStateEquals(t, vector.Expect.State, reducer.State())
		})
	}
}

// driveFixture reduces every input in delivery order, stopping at the first
// refusal so the projection is the one that stood at the moment it was refused.
func driveFixture(t *testing.T, reducer *Reducer, vector fixture) (*ViolationError, int) {
	t.Helper()

	for index, input := range vector.Input {
		if input.Control != "" {
			require.Equal(t, "session_closed", input.Control, "unknown control event")
			reducer.Close()

			continue
		}

		require.Equal(t, "session/update", input.Method)

		err := reducer.ReduceSessionUpdate(input.Params)
		if err == nil {
			continue
		}

		var refusal *ViolationError

		require.True(t, errors.As(err, &refusal), "input %d: %v", index, err)

		return refusal, index
	}

	return nil, -1
}

// requireLatched feeds every post-refusal input and proves the latch holds: each
// one reports the same token at the same identity the refusal named, and the
// projection is the one that stood when the stream failed closed. A consumer that
// stopped reducing and one that kept going past its own verdict are otherwise
// indistinguishable.
func requireLatched(t *testing.T, reducer *Reducer, vector fixture, refusal *ViolationError) {
	t.Helper()

	for index, input := range vector.PostRefusal {
		require.Equal(t, "session/update", input.Method, "post-refusal input %d", index)

		var latched *ViolationError

		err := reducer.ReduceSessionUpdate(input.Params)
		require.True(t, errors.As(err, &latched), "post-refusal input %d: %v", index, err)
		require.Equal(t, refusal, latched, "post-refusal input %d", index)
	}

	requireStateEquals(t, vector.Expect.State, reducer.State())
}

func loadManifest(t *testing.T) manifest {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(fixtureDir, "manifest.json"))
	require.NoError(t, err)

	var index manifest

	require.NoError(t, json.Unmarshal(data, &index))
	require.NotEmpty(t, index.Fixtures)

	return index
}

func loadFixture(t *testing.T, file string) fixture {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(fixtureDir, file))
	require.NoError(t, err)

	var vector fixture

	require.NoError(t, json.Unmarshal(data, &vector))
	require.NotEmpty(t, vector.Purpose, "a fixture states the invariant it proves")

	return vector
}

// requireStateEquals compares the projection against the fixture's expected state
// for exact equality. The member set is fixed by the manifest's state shape: an
// extra or a missing member fails the fixture.
func requireStateEquals(t *testing.T, expected json.RawMessage, actual State) {
	t.Helper()

	encoded, err := json.Marshal(actual)
	require.NoError(t, err)

	var want, got any

	require.NoError(t, json.Unmarshal(expected, &want))
	require.NoError(t, json.Unmarshal(encoded, &got))
	require.Equal(t, want, got)
}
