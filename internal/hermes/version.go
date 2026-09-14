package hermes

import (
	"fmt"
	"strconv"
	"strings"
)

// MinimumVersion is the lowest `hermes --version` the adapter accepts: the
// version its behavior was verified against.
const MinimumVersion = "0.21.2"

// CheckMinimumVersion fails when version sorts below minimum.
func CheckMinimumVersion(version string, minimum string) error {
	comparison, err := compareVersions(version, minimum)
	if err != nil {
		return err
	}

	if comparison < 0 {
		return fmt.Errorf("hermes version %s is below the minimum supported version %s", version, minimum)
	}

	return nil
}

func compareVersions(left string, right string) (int, error) {
	leftParts, err := versionParts(left)
	if err != nil {
		return 0, err
	}

	rightParts, err := versionParts(right)
	if err != nil {
		return 0, err
	}

	for index := range max(len(leftParts), len(rightParts)) {
		leftValue := 0
		if index < len(leftParts) {
			leftValue = leftParts[index]
		}

		rightValue := 0
		if index < len(rightParts) {
			rightValue = rightParts[index]
		}

		if leftValue != rightValue {
			if leftValue < rightValue {
				return -1, nil
			}

			return 1, nil
		}
	}

	return 0, nil
}

func versionParts(version string) ([]int, error) {
	trimmed := strings.TrimPrefix(strings.TrimSpace(version), "v")
	if release, _, found := strings.Cut(trimmed, "-"); found {
		trimmed = release
	}

	if trimmed == "" {
		return nil, fmt.Errorf("invalid version %q", version)
	}

	segments := strings.Split(trimmed, ".")
	parts := make([]int, 0, len(segments))

	for _, segment := range segments {
		value, err := strconv.Atoi(segment)
		if err != nil || value < 0 {
			return nil, fmt.Errorf("invalid version %q", version)
		}

		parts = append(parts, value)
	}

	return parts, nil
}
