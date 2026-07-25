//go:build windows

package hermesacp

// handoffOpenFlags adds no flags on Windows, which exposes no non-blocking open
// mode here. The handoff form is unreachable on this platform anyway, because
// every file:///C:/... spelling fails filepath.IsAbs once FromSlash has run;
// leaving it unreachable is preferable to half-enabling it without a containment
// story for DOS device names. Containment is still the read root's, and the
// descriptor's regular-file check still runs.
const handoffOpenFlags = 0
