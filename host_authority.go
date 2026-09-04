package hermesacp

import (
	"context"
	"errors"
	"io"
)

var (
	// ErrHostAuthorityUnavailable reports that the borrowed host boundary cannot continue.
	ErrHostAuthorityUnavailable = errors.New("host authority unavailable")
	// ErrContainmentIncomplete reports that native process containment is uncertain.
	ErrContainmentIncomplete = errors.New("native containment incomplete")
	// ErrNativeTreeBusy reports a non-mutating reclaim refusal while lease processes remain.
	ErrNativeTreeBusy = errors.New("native tree has live lease processes")
)

// HostAuthority owns native process execution and prepared-tree transitions.
type HostAuthority interface {
	NativeEnvironment() map[string]string
	PrepareNativeTree(context.Context, string) error
	ReadNativeAppendLog(context.Context, string, uint64) ([][]byte, error)
	ReclaimNativeTree(context.Context, string) error
	StartNative(context.Context, NativeRequest) (NativeProcess, error)
}

// NativeRequest describes one host-authorized native process launch.
type NativeRequest struct {
	Executable       string
	Arguments        []string
	Environment      []string
	WorkingDirectory string
}

// NativeProcess is a host-owned native process with revocation and terminal observation.
type NativeProcess interface {
	Stdin() io.WriteCloser
	Stdout() io.ReadCloser
	Stderr() io.ReadCloser
	Wait(context.Context) (NativeResult, error)
	Revoke(context.Context) error
}

// NativeResult describes a terminal native process.
type NativeResult struct {
	ExitCode int
	Signal   int
	Revoked  bool
}
