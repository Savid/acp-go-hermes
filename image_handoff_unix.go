//go:build unix

package hermesacp

import "syscall"

// handoffOpenFlags hardens the root-relative open of a handoff path.
// O_NOFOLLOW refuses a final component that became a symlink, and O_NONBLOCK
// means a component that is a FIFO or a device fails the descriptor's
// regular-file check instead of blocking the turn inside open(2). Neither flag
// changes how a regular file reads.
const handoffOpenFlags = syscall.O_NOFOLLOW | syscall.O_NONBLOCK
