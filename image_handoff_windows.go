//go:build windows

package hermesacp

import "path/filepath"

// handoffOpenFlags carries the platform's own contribution to a root-relative
// handoff open, and Windows contributes none: it exposes no non-blocking open
// mode, and no open flag adds to containment in any case. Containment is the
// read root's here exactly as it is elsewhere — a name that leads out of the
// root is refused as part of the open — and the descriptor's regular-file check
// still decides what kind of object the name reached.
const handoffOpenFlags = 0

// handoffURILocalPath maps the path component of a file URI to a local path.
// A file URI's path is always rooted, so a Windows drive path arrives with a
// leading slash ahead of the drive letter ("/C:/dir/file.png"); left in place
// it renders a perfectly ordinary local path un-absolute. Only that one form
// is unwrapped, so a rooted path with no drive stays exactly as it arrived.
func handoffURILocalPath(path string) string {
	if len(path) >= 3 && path[0] == '/' && path[2] == ':' && isHandoffDriveLetter(path[1]) {
		path = path[1:]
	}

	return filepath.FromSlash(path)
}

func isHandoffDriveLetter(value byte) bool {
	return (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z')
}
