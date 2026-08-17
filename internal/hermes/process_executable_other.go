//go:build !windows

package hermes

// ordinaryExecutableRules selects the resolution rules for this platform. Off
// Windows the execute bit is authoritative and file names carry no meaning, so
// there is nothing environment-dependent to read.
func ordinaryExecutableRules([]string) executableSearchRules {
	return unixExecutableRules()
}
