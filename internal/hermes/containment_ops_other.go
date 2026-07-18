//go:build !darwin

package hermes

import "errors"

type ContainmentCandidate struct {
	PID       int
	StartSec  int64
	StartUsec int32
}
type ContainmentDiagnostic struct {
	RuntimeID, LifecycleKind, State, GenerationRoot string
	Candidates                                      []ContainmentCandidate
	AmbiguousPIDs                                   []int
}
type ContainmentCleanupResult struct {
	RuntimeID, GenerationRoot                         string
	TermSignalled, KillSignalled, RemainingCorrelated []ContainmentCandidate
	AmbiguousPIDs                                     []int
	RootRemoved                                       bool
	Reportable                                        bool
}

func DiagnoseContainment(string) ([]ContainmentDiagnostic, error) {
	return nil, errors.New("containment diagnostics are available only on darwin")
}

func CleanupContainment(string, string, bool) (ContainmentCleanupResult, error) {
	return ContainmentCleanupResult{}, errors.New("containment cleanup is available only on darwin")
}
