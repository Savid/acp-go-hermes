//go:build windows

package hermes

// ordinaryExecutableRules selects the resolution rules for this platform.
// Windows reads PATHEXT out of the very environment the launch will inherit, so
// the rules depend on it rather than on the adapter's own process environment.
func ordinaryExecutableRules(environment []string) executableSearchRules {
	return windowsExecutableRules(environment)
}
