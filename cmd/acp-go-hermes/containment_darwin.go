//go:build darwin

//nolint:wsl_v5,nlreturn // Sorting stays adjacent to the collected operator result.
package main

import (
	"path/filepath"
	"slices"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

var (
	containmentResolveAbs       = filepath.Abs
	containmentDiagnoseRegistry = nativehermes.DiagnoseContainment
	containmentCleanupRegistry  = nativehermes.CleanupContainment
)

func containmentDiagnose(scratchDir string) (containmentDiagnoseOutput, error) {
	parent, err := containmentResolveAbs(scratchDir)
	if err != nil {
		return containmentDiagnoseOutput{}, err
	}
	diagnostics, err := containmentDiagnoseRegistry(parent)
	if err != nil {
		return containmentDiagnoseOutput{}, err
	}
	records := make([]containmentDiagnoseRecord, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		record := containmentDiagnoseRecord{RuntimeID: diagnostic.RuntimeID, State: diagnostic.State, GenerationRoot: diagnostic.GenerationRoot, CorrelatedPIDs: []int{}, AmbiguousPIDs: append([]int{}, diagnostic.AmbiguousPIDs...)}
		for _, candidate := range diagnostic.Candidates {
			record.CorrelatedPIDs = append(record.CorrelatedPIDs, candidate.PID)
		}
		slices.Sort(record.CorrelatedPIDs)
		slices.Sort(record.AmbiguousPIDs)
		records = append(records, record)
	}
	slices.SortFunc(records, compareContainmentDiagnoseRecords)
	return containmentDiagnoseOutput{Vendor: containmentVendor, Containment: containmentBestEffort, ScratchParent: parent, Records: records, Warning: containmentOutputWarning}, nil
}

func compareContainmentDiagnoseRecords(left, right containmentDiagnoseRecord) int {
	if left.RuntimeID < right.RuntimeID {
		return -1
	}
	if left.RuntimeID > right.RuntimeID {
		return 1
	}
	return 0
}

func containmentCleanup(scratchDir, runtimeID string, force bool) (containmentCleanupOutput, error) {
	parent, err := containmentResolveAbs(scratchDir)
	if err != nil {
		return containmentCleanupOutput{}, err
	}
	result, err := containmentCleanupRegistry(parent, runtimeID, force)
	output := containmentCleanupOutput{Vendor: containmentVendor, Containment: containmentBestEffort, ScratchParent: parent, RuntimeID: result.RuntimeID, GenerationRoot: result.GenerationRoot, TermSignalledPIDs: candidatePIDs(result.TermSignalled), KillSignalledPIDs: candidatePIDs(result.KillSignalled), RemainingCorrelatedPIDs: candidatePIDs(result.RemainingCorrelated), AmbiguousPIDs: append([]int{}, result.AmbiguousPIDs...), RootRemoved: result.RootRemoved, Reportable: result.Reportable, Warning: containmentOutputWarning}
	slices.Sort(output.AmbiguousPIDs)
	return output, err
}

func candidatePIDs(candidates []nativehermes.ContainmentCandidate) []int {
	pids := make([]int, 0, len(candidates))
	for _, candidate := range candidates {
		pids = append(pids, candidate.PID)
	}
	slices.Sort(pids)
	return pids
}
