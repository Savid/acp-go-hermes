//go:build darwin

package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
	"github.com/stretchr/testify/require"
)

type containmentErrorWriter struct{}

func (containmentErrorWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

func restoreContainmentCommandSeams(t *testing.T) {
	t.Helper()
	diagnose := containmentDiagnoseCommand
	cleanup := containmentCleanupCommand
	resolveAbs := containmentResolveAbs
	diagnoseRegistry := containmentDiagnoseRegistry
	cleanupRegistry := containmentCleanupRegistry
	t.Cleanup(func() {
		containmentDiagnoseCommand = diagnose
		containmentCleanupCommand = cleanup
		containmentResolveAbs = resolveAbs
		containmentDiagnoseRegistry = diagnoseRegistry
		containmentCleanupRegistry = cleanupRegistry
	})
}

func TestRunContainmentCommandValidation(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "missing command", want: "requires diagnose or cleanup"},
		{name: "unknown command", args: []string{"other"}, want: `unknown containment command "other"`},
		{name: "diagnose parse", args: []string{"diagnose", "-bad"}, want: "flag provided but not defined"},
		{name: "diagnose positional", args: []string{"diagnose", "-scratch-dir", "/tmp", "extra"}, want: "accepts no positional arguments"},
		{name: "diagnose scratch", args: []string{"diagnose"}, want: "requires -scratch-dir"},
		{name: "diagnose malformed scratch", args: []string{"diagnose", "-scratch-dir", "bad\x00path"}, want: "requires -scratch-dir"},
		{name: "cleanup parse", args: []string{"cleanup", "-bad"}, want: "flag provided but not defined"},
		{name: "cleanup positional", args: []string{"cleanup", "-runtime-id", strings.Repeat("0", 32), "-scratch-dir", "/tmp", "-force", "extra"}, want: "accepts no positional arguments"},
		{name: "cleanup runtime", args: []string{"cleanup"}, want: "requires -runtime-id"},
		{name: "cleanup scratch", args: []string{"cleanup", "-runtime-id", strings.Repeat("0", 32)}, want: "requires -scratch-dir"},
		{name: "cleanup malformed runtime", args: []string{"cleanup", "-runtime-id", strings.Repeat("G", 32), "-scratch-dir", "/tmp"}, want: "128-bit lowercase hex"},
		{name: "cleanup force", args: []string{"cleanup", "-runtime-id", strings.Repeat("0", 32), "-scratch-dir", "/tmp"}, want: "requires -force"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			require.Equal(t, 2, runContainmentCommand(test.args, &stdout, &stderr))
			require.Empty(t, stdout.String())
			require.Contains(t, stderr.String(), test.want)
		})
	}

	require.True(t, validContainmentRuntimeID(strings.Repeat("a", 32)))
	require.False(t, validContainmentRuntimeID(strings.Repeat("A", 32)))
	require.False(t, validContainmentRuntimeID("abc"))
	require.False(t, validContainmentRuntimeID(strings.Repeat("g", 32)))
	require.True(t, validContainmentScratchDir("relative"))
	require.False(t, validContainmentScratchDir(" \t"))
	require.False(t, validContainmentScratchDir("bad\x00path"))

	var stdout, stderr bytes.Buffer
	require.Equal(t, 2, runContainmentCommand([]string{"diagnose", "-h"}, &stdout, &stderr))
	require.Contains(t, stderr.String(), "PID-reuse time-of-check/time-of-use race")
	stdout.Reset()
	stderr.Reset()
	require.Equal(t, 2, runContainmentCommand([]string{"cleanup", "-h"}, &stdout, &stderr))
	require.Contains(t, stderr.String(), "unrelated reused PID")
}

func TestRunContainmentCommandJSONAndErrors(t *testing.T) {
	restoreContainmentCommandSeams(t)
	runtimeID := strings.Repeat("1", 32)
	root := "/scratch/acp-go-hermes-runtime-one"
	containmentDiagnoseCommand = func(string) (containmentDiagnoseOutput, error) {
		return containmentDiagnoseOutput{
			Vendor: "hermes", Containment: "best_effort", ScratchParent: "/scratch",
			Warning: containmentOutputWarning,
			Records: []containmentDiagnoseRecord{{
				RuntimeID: runtimeID, State: "running", GenerationRoot: root,
				CorrelatedPIDs: []int{7}, AmbiguousPIDs: []int{},
			}},
		}, nil
	}
	var stdout, stderr bytes.Buffer
	require.Zero(t, runContainmentCommand([]string{"diagnose", "-scratch-dir", "/scratch"}, &stdout, &stderr))
	require.Empty(t, stderr.String())
	require.Equal(t, `{"vendor":"hermes","containment":"best_effort","scratch_parent":"/scratch","warning":"PID-by-PID cleanup has a PID-reuse time-of-check/time-of-use race and can signal an unrelated reused PID; correlation is not ownership or proof of absence; inherited markers can be scrubbed","records":[{"runtime_id":"11111111111111111111111111111111","state":"running","generation_root":"/scratch/acp-go-hermes-runtime-one","correlated_pids":[7],"ambiguous_pids":[]}]}`+"\n", stdout.String())

	containmentDiagnoseCommand = func(string) (containmentDiagnoseOutput, error) {
		return containmentDiagnoseOutput{}, errors.New("diagnose failed")
	}
	stdout.Reset()
	stderr.Reset()
	require.Equal(t, 1, runContainmentCommand([]string{"diagnose", "-scratch-dir", "/scratch"}, &stdout, &stderr))
	require.Empty(t, stdout.String())
	require.Contains(t, stderr.String(), "diagnose failed")

	partial := containmentCleanupOutput{
		Vendor: "hermes", Containment: "best_effort", ScratchParent: "/scratch",
		Warning: containmentOutputWarning, RuntimeID: runtimeID, GenerationRoot: root,
		TermSignalledPIDs: []int{9}, KillSignalledPIDs: []int{}, RemainingCorrelatedPIDs: []int{9}, AmbiguousPIDs: []int{},
		Reportable: true,
	}
	containmentCleanupCommand = func(string, string, bool) (containmentCleanupOutput, error) {
		return partial, errors.New("cleanup incomplete")
	}
	stdout.Reset()
	stderr.Reset()
	require.Equal(t, 1, runContainmentCommand([]string{"cleanup", "-scratch-dir", "/scratch", "-runtime-id", runtimeID, "-force"}, &stdout, &stderr))
	require.Equal(t, `{"vendor":"hermes","containment":"best_effort","scratch_parent":"/scratch","warning":"PID-by-PID cleanup has a PID-reuse time-of-check/time-of-use race and can signal an unrelated reused PID; correlation is not ownership or proof of absence; inherited markers can be scrubbed","runtime_id":"11111111111111111111111111111111","generation_root":"/scratch/acp-go-hermes-runtime-one","term_signaled_pids":[9],"kill_signaled_pids":[],"remaining_correlated_pids":[9],"ambiguous_pids":[],"root_removed":false}`+"\n", stdout.String())
	require.Contains(t, stderr.String(), "cleanup incomplete")

	partial.Reportable = false
	stdout.Reset()
	stderr.Reset()
	require.Equal(t, 1, runContainmentCommand([]string{"cleanup", "-scratch-dir", "/scratch", "-runtime-id", runtimeID, "-force"}, &stdout, &stderr))
	require.Empty(t, stdout.String())
	partial.Reportable = true

	containmentCleanupCommand = func(string, string, bool) (containmentCleanupOutput, error) {
		return containmentCleanupOutput{}, errors.New("no record")
	}
	stdout.Reset()
	stderr.Reset()
	require.Equal(t, 1, runContainmentCommand([]string{"cleanup", "-scratch-dir", "/scratch", "-runtime-id", runtimeID, "-force"}, &stdout, &stderr))
	require.Empty(t, stdout.String())

	containmentCleanupCommand = func(string, string, bool) (containmentCleanupOutput, error) { return partial, nil }
	require.Equal(t, 1, runContainmentCommand([]string{"cleanup", "-scratch-dir", "/scratch", "-runtime-id", runtimeID, "-force"}, containmentErrorWriter{}, &stderr))
	require.Contains(t, stderr.String(), "encode containment result")

	containmentCleanupCommand = func(string, string, bool) (containmentCleanupOutput, error) {
		return partial, errors.New("partial cleanup")
	}
	require.Equal(t, 1, runContainmentCommand([]string{"cleanup", "-scratch-dir", "/scratch", "-runtime-id", runtimeID, "-force"}, containmentErrorWriter{}, &stderr))
}

func TestContainmentPlatformCommands(t *testing.T) {
	restoreContainmentCommandSeams(t)
	parent := t.TempDir()
	output, err := containmentDiagnose(parent)
	require.NoError(t, err)
	require.Equal(t, containmentOutputWarning, output.Warning)
	require.Empty(t, output.Records)

	runtimeID := strings.Repeat("2", 32)
	root, err := os.MkdirTemp(parent, "acp-go-hermes-runtime-")
	require.NoError(t, err)
	registry := filepath.Join(parent, "acp-go-hermes-containment")
	require.NoError(t, os.Mkdir(registry, 0o700))
	record := `{"schema_version":1,"vendor":"hermes","containment":"best_effort","lifecycle_kind":"session","runtime_id":"` + runtimeID + `","generation_root":"` + root + `","wrapper_pid":1,"wrapper_start_sec":1,"wrapper_start_usec":1,"state":"group_absent"}`
	require.NoError(t, os.WriteFile(filepath.Join(registry, runtimeID+".json"), []byte(record), 0o600))

	output, err = containmentDiagnose(parent)
	require.NoError(t, err)
	require.Len(t, output.Records, 1)
	require.Equal(t, []int{}, output.Records[0].CorrelatedPIDs)
	require.Equal(t, []int{}, output.Records[0].AmbiguousPIDs)

	cleanup, err := containmentCleanup(parent, runtimeID, true)
	require.NoError(t, err)
	require.True(t, cleanup.RootRemoved)
	require.Equal(t, []int{}, candidatePIDs(nil))
	_, err = os.Stat(root)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestContainmentPlatformCommandErrorsAndSorting(t *testing.T) {
	restoreContainmentCommandSeams(t)
	wantErr := errors.New("containment operation failed")
	a := containmentDiagnoseRecord{RuntimeID: "a"}
	b := containmentDiagnoseRecord{RuntimeID: "b"}
	require.Equal(t, -1, compareContainmentDiagnoseRecords(a, b))
	require.Equal(t, 1, compareContainmentDiagnoseRecords(b, a))
	require.Zero(t, compareContainmentDiagnoseRecords(a, a))

	containmentResolveAbs = func(string) (string, error) { return "", wantErr }
	_, err := containmentDiagnose("scratch")
	require.ErrorIs(t, err, wantErr)
	_, err = containmentCleanup("scratch", strings.Repeat("a", 32), true)
	require.ErrorIs(t, err, wantErr)

	containmentResolveAbs = filepath.Abs
	containmentDiagnoseRegistry = func(string) ([]nativehermes.ContainmentDiagnostic, error) {
		return nil, wantErr
	}
	_, err = containmentDiagnose(t.TempDir())
	require.ErrorIs(t, err, wantErr)

	containmentDiagnoseRegistry = func(string) ([]nativehermes.ContainmentDiagnostic, error) {
		return []nativehermes.ContainmentDiagnostic{
			{
				RuntimeID:      strings.Repeat("b", 32),
				State:          "running",
				GenerationRoot: "/scratch/acp-go-hermes-runtime-b",
				Candidates: []nativehermes.ContainmentCandidate{
					{PID: 9},
					{PID: 3},
				},
				AmbiguousPIDs: []int{8, 2},
			},
			{
				RuntimeID:      strings.Repeat("a", 32),
				State:          "group_absent",
				GenerationRoot: "/scratch/acp-go-hermes-runtime-a",
			},
		}, nil
	}
	output, err := containmentDiagnose("/scratch")
	require.NoError(t, err)
	require.Equal(t, strings.Repeat("a", 32), output.Records[0].RuntimeID)
	require.Equal(t, []int{3, 9}, output.Records[1].CorrelatedPIDs)
	require.Equal(t, []int{2, 8}, output.Records[1].AmbiguousPIDs)

	containmentCleanupRegistry = func(string, string, bool) (nativehermes.ContainmentCleanupResult, error) {
		return nativehermes.ContainmentCleanupResult{
			RuntimeID:      strings.Repeat("a", 32),
			GenerationRoot: "/scratch/acp-go-hermes-runtime-a",
			TermSignalled: []nativehermes.ContainmentCandidate{
				{PID: 7},
				{PID: 4},
			},
			KillSignalled: []nativehermes.ContainmentCandidate{{PID: 6}},
			RemainingCorrelated: []nativehermes.ContainmentCandidate{
				{PID: 5},
			},
			AmbiguousPIDs: []int{9, 1},
		}, wantErr
	}
	cleanup, err := containmentCleanup("/scratch", strings.Repeat("a", 32), true)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, []int{4, 7}, cleanup.TermSignalledPIDs)
	require.Equal(t, []int{6}, cleanup.KillSignalledPIDs)
	require.Equal(t, []int{5}, cleanup.RemainingCorrelatedPIDs)
	require.Equal(t, []int{1, 9}, cleanup.AmbiguousPIDs)
}
