//go:build windows

package hermes

// newBrowserShim reports that this platform has no shim. CreateProcess resolves
// cmd.exe, explorer.exe, and rundll32.exe out of the system directory ahead of
// every PATH entry, and the `start` that opens a URL is a cmd.exe builtin with
// no image to shadow, so a shim directory on PATH neutralises nothing here. The
// native launch is already unavailable because this platform cannot apply the
// mandatory Unix UID/GID isolation.
//
//nolint:nilnil // The absent shim is this platform's answer, not a launch failure.
func newBrowserShim(string) (*browserShim, error) {
	return nil, nil
}
