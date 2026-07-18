//go:build !darwin

package hermes

func prepareContainmentRecord(ContainmentSpec, string) (containmentRecord, error) {
	return containmentRecord{}, nil
}

func activateContainmentRecord(containmentRecord, int, int) error { return nil }
func completeContainmentRecord(containmentRecord, string) error   { return nil }
