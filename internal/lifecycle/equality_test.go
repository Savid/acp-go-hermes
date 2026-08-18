package lifecycle

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// decodeFrame reads one notification the way the reducer does, so these vectors
// compare exactly the decoded forms a delivered frame carries.
func decodeFrame(t *testing.T, raw string) any {
	t.Helper()

	value := decodedValue(json.RawMessage(raw))
	require.NotNil(t, value)

	return value
}

// TestLifecycleValueEqualityComparesNumbersAsValues pins the number predicate:
// equality is of mathematical values, not of the lexemes they were written with,
// and it holds exactly where a float64 would lose the difference.
func TestLifecycleValueEqualityComparesNumbersAsValues(t *testing.T) {
	t.Parallel()

	for _, equal := range [][2]string{
		{"1", "1.0"},
		{"1", "1e0"},
		{"1", "0.1E1"},
		{"0", "-0"},
		{"0", "0.000"},
		{"0", "0e999999999"},
		{"-2.5", "-25e-1"},
		{"1e999999999", "1E999999999"},
		{"1e999999999999999999999", "10e999999999999999999998"},
		{"1234567890123456789", "1234567890123456789"},
	} {
		require.True(t, numberEqual(json.Number(equal[0]), json.Number(equal[1])),
			"%s and %s name the same value", equal[0], equal[1])
	}

	for _, different := range [][2]string{
		{"1234567890123456788", "1234567890123456789"},
		{"1", "-1"},
		{"1", "10"},
		{"1e999999999", "1e999999998"},
		{"1", "2"},
	} {
		require.False(t, numberEqual(json.Number(different[0]), json.Number(different[1])),
			"%s and %s name different values", different[0], different[1])
	}
}

// TestLifecycleValueEqualityComparesDecodedForms pins the structural half: key
// order and whitespace are never differences, a missing or extra member always
// is, and a value of another type never equals one of this one.
func TestLifecycleValueEqualityComparesDecodedForms(t *testing.T) {
	t.Parallel()

	require.True(t, valueEqual(
		decodeFrame(t, `{"a": 1, "b": [1, {"c": "x"}], "d": null, "e": true}`),
		decodeFrame(t, `{"e":true,"d":null,"b":[1.0,{"c":"x"}],"a":1e0}`),
	))

	for _, different := range [][2]string{
		{`{"a":1}`, `{"a":1,"b":2}`},
		{`{"a":1}`, `{"b":1}`},
		{`{"a":1}`, `[1]`},
		{`[1,2]`, `[1,2,3]`},
		{`[1,2]`, `{"a":1}`},
		{`[1,2]`, `[1,3]`},
		{`"1"`, `1`},
		{`true`, `false`},
	} {
		require.False(t, valueEqual(decodeFrame(t, different[0]), decodeFrame(t, different[1])),
			"%s and %s are different values", different[0], different[1])
	}
}

// TestRawMemberEqualityDistinguishesAbsenceFromEmptiness pins that an omitted
// opaque member is compared as absent rather than as an empty value: only two
// absences are equal, because a member the event never carried states nothing.
func TestRawMemberEqualityDistinguishesAbsenceFromEmptiness(t *testing.T) {
	t.Parallel()

	require.True(t, rawValueEqual(nil, nil))
	require.False(t, rawValueEqual(nil, json.RawMessage(`{}`)))
	require.False(t, rawValueEqual(json.RawMessage(`{}`), nil))
	require.True(t, rawValueEqual(json.RawMessage(`{"n": 1}`), json.RawMessage(`{"n":1.0}`)))
	require.False(t, rawValueEqual(json.RawMessage(`{"n": 1}`), json.RawMessage(`{"n":2}`)))
}
