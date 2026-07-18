//go:build !darwin

package main

import "errors"

func containmentDiagnose(string) (containmentDiagnoseOutput, error) {
	return containmentDiagnoseOutput{}, errors.New("containment diagnostics are available only on darwin")
}
func containmentCleanup(string, string, bool) (containmentCleanupOutput, error) {
	return containmentCleanupOutput{}, errors.New("containment cleanup is available only on darwin")
}
