//go:build windows

package hermesacp

// handoffOpenFlags carries the platform's own contribution to a root-relative
// handoff open, and Windows contributes none: it exposes no non-blocking open
// mode, and no open flag adds to containment in any case. Containment is the
// read root's here exactly as it is elsewhere — a name that leads out of the
// root is refused as part of the open — and the descriptor's regular-file check
// still decides what kind of object the name reached.
const handoffOpenFlags = 0
