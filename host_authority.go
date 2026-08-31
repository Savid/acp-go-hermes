package hermesacp

import (
	"context"
	"errors"
	"io"
)

var (
	ErrHostAuthorityUnavailable = errors.New("host authority unavailable")
	ErrContainmentIncomplete    = errors.New("native containment incomplete")
	ErrNativeTreeBusy           = errors.New("native tree has live lease processes")
)

type HostAuthority interface {
	NativeEnvironment() map[string]string
	PrepareNativeTree(context.Context, string) error
	ReclaimNativeTree(context.Context, string) error
	StartNative(context.Context, NativeRequest) (NativeProcess, error)
}

type NativeRequest struct {
	Executable       string
	Arguments        []string
	Environment      []string
	WorkingDirectory string
}

type NativeProcess interface {
	Stdin() io.WriteCloser
	Stdout() io.ReadCloser
	Stderr() io.ReadCloser
	Wait(context.Context) (NativeResult, error)
	Revoke(context.Context) error
}

type NativeResult struct {
	ExitCode int
	Signal   int
	Revoked  bool
}
