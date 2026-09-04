//go:build !windows

package hermes

// browserShimTreeCount is how many native trees a managed launch prepares for
// the browser-launcher shim on this platform. POSIX installs one.
const browserShimTreeCount = 1
