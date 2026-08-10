//go:build windows

package hermes

// newBrowserShim reports that this platform has no shim. CreateProcess resolves
// cmd.exe, explorer.exe, and rundll32.exe out of the system directory ahead of
// every PATH entry, and the `start` that opens a URL is a cmd.exe builtin with
// no image to shadow, so a shim directory on PATH neutralises nothing here.
// Ordinary same-identity launch is supported on this platform, so the session
// starts without a shim and only the login leg refuses; explicit isolation is
// what stays unavailable, because its UID/GID boundary is Unix-only.
//
//nolint:nilnil // The absent shim is this platform's answer, not a launch failure.
func newBrowserShim(string) (*browserShim, error) {
	return nil, nil
}
