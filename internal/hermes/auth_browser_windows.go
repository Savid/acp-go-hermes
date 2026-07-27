//go:build windows

package hermes

// neutralizedBrowserCommand names a command that succeeds and opens nothing.
// Hermes launches a browser during a login flow regardless of --no-browser, and
// the launcher stops at the first entry that reports success, so a succeeding
// no-op command is what keeps the flow headless.
func neutralizedBrowserCommand() string {
	return "cmd /c exit"
}
