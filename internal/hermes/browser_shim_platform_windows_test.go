//go:build windows

package hermes

// browserShimTreeCount is how many native trees a managed launch prepares for
// the browser-launcher shim on this platform. Windows installs none:
// CreateProcess resolves the launchers a shim would shadow out of the system
// directory ahead of every PATH entry, so there is nothing for one to
// neutralise, and the login leg refuses rather than the launch.
const browserShimTreeCount = 0
