package lifecycle

import (
	"bytes"
	"encoding/json"
	"math/big"
	"strings"
)

// valueEqual reports lifecycle value equality between two decoded values. Two
// values are equal when their decoded forms are deeply equal: key order and
// insignificant whitespace are never differences, and numbers compare as exact
// mathematical values rather than as the lexemes they arrived in.
//
// This equality is the extension's own and is used at every comparison site it
// has — the whole-notification basis a retransmission is judged on, and the
// member-wise patch basis a terminal restatement is judged on. It is
// deliberately stricter than a canonicalization that collapses numbers to
// binary floating point: a consumer that certifies a boundary cannot tell a
// faithful retransmission from a changed frame once precision is gone.
func valueEqual(left, right any) bool {
	switch value := left.(type) {
	case map[string]any:
		return objectEqual(value, right)
	case []any:
		return arrayEqual(value, right)
	case json.Number:
		counterpart, ok := right.(json.Number)

		return ok && numberEqual(value, counterpart)
	default:
		return left == right
	}
}

func objectEqual(left map[string]any, right any) bool {
	counterpart, ok := right.(map[string]any)
	if !ok || len(left) != len(counterpart) {
		return false
	}

	for key, value := range left {
		other, present := counterpart[key]
		if !present || !valueEqual(value, other) {
			return false
		}
	}

	return true
}

func arrayEqual(left []any, right any) bool {
	counterpart, ok := right.([]any)
	if !ok || len(left) != len(counterpart) {
		return false
	}

	for index := range left {
		if !valueEqual(left[index], counterpart[index]) {
			return false
		}
	}

	return true
}

// numberEqual decides two JSON numbers from their normalized decimal forms —
// sign, coefficient with trailing zeros stripped, and adjusted exponent. The
// comparison is non-expanding by construction: it costs the length of the two
// lexemes and never the magnitude they name, so a twelve-byte literal with a
// huge exponent is decided without materializing the value it stands for.
// Every zero is the same zero, which is what makes -0 equal 0.
func numberEqual(left, right json.Number) bool {
	leftNegative, leftCoefficient, leftExponent := normalizedNumber(left.String())
	rightNegative, rightCoefficient, rightExponent := normalizedNumber(right.String())

	if leftCoefficient == "" || rightCoefficient == "" {
		return leftCoefficient == rightCoefficient
	}

	return leftNegative == rightNegative &&
		leftCoefficient == rightCoefficient &&
		leftExponent.Cmp(rightExponent) == 0
}

// normalizedNumber reads one JSON number lexeme into its normalized decimal
// triple. The empty coefficient is the zero of every spelling: 0, -0, 0.000, and
// 0e999 all reduce to it. The exponent is held as an arbitrary-precision integer
// because a lexeme may carry one longer than a machine word, which is a fact
// about the digits it was written with rather than about the value.
func normalizedNumber(lexeme string) (bool, string, *big.Int) {
	negative := strings.HasPrefix(lexeme, "-")
	digits := strings.TrimPrefix(lexeme, "-")
	exponent := new(big.Int)

	if marker := strings.IndexAny(digits, "eE"); marker >= 0 {
		exponent.SetString(digits[marker+1:], 10)
		digits = digits[:marker]
	}

	if point := strings.IndexByte(digits, '.'); point >= 0 {
		fraction := digits[point+1:]
		digits = digits[:point] + fraction
		exponent.Sub(exponent, big.NewInt(int64(len(fraction))))
	}

	digits = strings.TrimLeft(digits, "0")
	coefficient := strings.TrimRight(digits, "0")
	exponent.Add(exponent, big.NewInt(int64(len(digits)-len(coefficient))))

	return negative, coefficient, exponent
}

// rawValueEqual compares two opaque JSON members under the same equality. An
// absent member is not an empty one: only two absences are equal, because a
// member the event never carried states nothing at all.
func rawValueEqual(left, right json.RawMessage) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}

	return valueEqual(decodedValue(left), decodedValue(right))
}

// decodedValue reads one retained raw member back into the decoded form the
// comparison works on. Numbers keep their lexemes rather than collapsing to
// float64, which is what makes the comparison lossless. The bytes reaching here
// have already decoded as JSON once, so a read that cannot complete has no
// verdict of its own to report.
func decodedValue(raw json.RawMessage) any {
	var value any

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	_ = decoder.Decode(&value)

	return value
}
