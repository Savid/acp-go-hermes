package hermes

import (
	"context"
	"io"
)

type NativeRequest struct {
	Executable       string
	Arguments        []string
	Environment      []string
	WorkingDirectory string
}

type NativeResult struct {
	ExitCode int
	Signal   int
	Revoked  bool
}

type NativeProcess interface {
	Stdin() io.WriteCloser
	Stdout() io.ReadCloser
	Stderr() io.ReadCloser
	Wait(context.Context) (NativeResult, error)
	Revoke(context.Context) error
}

type NativeStarter func(context.Context, NativeRequest) (NativeProcess, error)
