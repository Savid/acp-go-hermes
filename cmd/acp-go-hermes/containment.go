//nolint:tagliatelle,wsl_v5,nlreturn // Stable operator JSON is snake_case; command parsing is intentionally compact.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"
)

const (
	containmentOutputWarning = "PID-by-PID cleanup has a PID-reuse time-of-check/time-of-use race and can signal an unrelated reused PID; correlation is not ownership or proof of absence; inherited markers can be scrubbed"
	containmentVendor        = "hermes"
	containmentBestEffort    = "best_effort"
	containmentDiagnoseName  = "diagnose"
	containmentCleanupName   = "cleanup"
)

var containmentDiagnoseCommand = containmentDiagnose
var containmentCleanupCommand = containmentCleanup

type containmentDiagnoseRecord struct {
	RuntimeID      string `json:"runtime_id"`
	State          string `json:"state"`
	GenerationRoot string `json:"generation_root"`
	CorrelatedPIDs []int  `json:"correlated_pids"`
	AmbiguousPIDs  []int  `json:"ambiguous_pids"`
}

type containmentDiagnoseOutput struct {
	Vendor        string                      `json:"vendor"`
	Containment   string                      `json:"containment"`
	ScratchParent string                      `json:"scratch_parent"`
	Warning       string                      `json:"warning"`
	Records       []containmentDiagnoseRecord `json:"records"`
}

type containmentCleanupOutput struct {
	Vendor                  string `json:"vendor"`
	Containment             string `json:"containment"`
	ScratchParent           string `json:"scratch_parent"`
	Warning                 string `json:"warning"`
	RuntimeID               string `json:"runtime_id"`
	GenerationRoot          string `json:"generation_root"`
	TermSignalledPIDs       []int  `json:"term_signaled_pids"`
	KillSignalledPIDs       []int  `json:"kill_signaled_pids"`
	RemainingCorrelatedPIDs []int  `json:"remaining_correlated_pids"`
	AmbiguousPIDs           []int  `json:"ambiguous_pids"`
	RootRemoved             bool   `json:"root_removed"`
	Reportable              bool   `json:"-"`
}

func runContainmentCommand(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		_, _ = fmt.Fprintln(stderr, "acp-go-hermes: containment requires diagnose or cleanup")
		return 2
	}
	usage := func(message string) int { _, _ = fmt.Fprintln(stderr, "acp-go-hermes: "+message); return 2 }
	var value any
	var err error
	emitPartial := false
	switch args[0] {
	case containmentDiagnoseName:
		flags := flag.NewFlagSet("acp-go-hermes containment diagnose", flag.ContinueOnError)
		flags.SetOutput(stderr)
		scratchDir := flags.String("scratch-dir", "", "scratch parent containing the Hermes containment registry; warning: "+containmentOutputWarning)
		if flags.Parse(args[1:]) != nil {
			return 2
		}
		if flags.NArg() != 0 {
			return usage("containment diagnose accepts no positional arguments")
		}
		if !validContainmentScratchDir(*scratchDir) {
			return usage("containment diagnose requires -scratch-dir")
		}
		value, err = containmentDiagnoseCommand(*scratchDir)
	case containmentCleanupName:
		flags := flag.NewFlagSet("acp-go-hermes containment cleanup", flag.ContinueOnError)
		flags.SetOutput(stderr)
		scratchDir := flags.String("scratch-dir", "", "scratch parent containing the Hermes containment registry")
		runtimeID := flags.String("runtime-id", "", "operator-selected runtime id")
		force := flags.Bool("force", false, containmentOutputWarning+"; PID-reuse TOCTOU and collateral-signalling risk; signal correlated PIDs and remove the selected generation root")
		if flags.Parse(args[1:]) != nil {
			return 2
		}
		if flags.NArg() != 0 {
			return usage("containment cleanup accepts no positional arguments")
		}
		if *runtimeID == "" {
			return usage("containment cleanup requires -runtime-id")
		}
		if !validContainmentScratchDir(*scratchDir) {
			return usage("containment cleanup requires -scratch-dir")
		}
		if !validContainmentRuntimeID(*runtimeID) {
			return usage("containment cleanup runtime id must be 128-bit lowercase hex")
		}
		if !*force {
			return usage("containment cleanup requires -force")
		}
		output, cleanupErr := containmentCleanupCommand(*scratchDir, *runtimeID, *force)
		value, err = output, cleanupErr
		emitPartial = cleanupErr != nil && output.Reportable
	default:
		return usage(fmt.Sprintf("unknown containment command %q", args[0]))
	}
	if err != nil {
		if emitPartial {
			if encodeErr := json.NewEncoder(stdout).Encode(value); encodeErr != nil {
				_, _ = fmt.Fprintf(stderr, "acp-go-hermes: encode containment result: %v\n", encodeErr)
				return 1
			}
		}
		_, _ = fmt.Fprintf(stderr, "acp-go-hermes: %v\n", err)
		return 1
	}
	if err := json.NewEncoder(stdout).Encode(value); err != nil {
		_, _ = fmt.Fprintf(stderr, "acp-go-hermes: encode containment result: %v\n", err)
		return 1
	}
	return 0
}

func validContainmentScratchDir(path string) bool {
	return strings.TrimSpace(path) != "" && !strings.ContainsRune(path, '\x00')
}
func validContainmentRuntimeID(runtimeID string) bool {
	if len(runtimeID) != 32 || runtimeID != strings.ToLower(runtimeID) {
		return false
	}
	for _, value := range runtimeID {
		if (value < '0' || value > '9') && (value < 'a' || value > 'f') {
			return false
		}
	}
	return true
}
