package hermes

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
)

type ordinaryNativeProcess struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr io.ReadCloser
	kill   func() error

	waitOnce sync.Once
	waitDone chan struct{}
	result   NativeResult
	waitErr  error

	revokeOnce sync.Once
	revokeErr  error
	outcomeMu  sync.Mutex
	terminal   bool
	revoked    bool
}

func startOrdinaryNative(ctx context.Context, request NativeRequest) (NativeProcess, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	cmd := exec.Command(request.Executable, request.Arguments...) //nolint:gosec // Ordinary mode executes the configured Hermes harness.
	cmd.Dir = request.WorkingDirectory
	cmd.Env = append([]string(nil), request.Environment...)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("create native stdin: %w", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()

		return nil, fmt.Errorf("create native stdout: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		_ = stdin.Close()
		_ = stdout.Close()

		return nil, fmt.Errorf("create native stderr: %w", err)
	}

	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stderr.Close()

		return nil, err
	}

	return &ordinaryNativeProcess{
		cmd: cmd, stdin: stdin, stdout: stdout, stderr: stderr, kill: cmd.Process.Kill, waitDone: make(chan struct{}),
	}, nil
}

func (p *ordinaryNativeProcess) Stdin() io.WriteCloser { return p.stdin }
func (p *ordinaryNativeProcess) Stdout() io.ReadCloser { return p.stdout }
func (p *ordinaryNativeProcess) Stderr() io.ReadCloser { return p.stderr }

func (p *ordinaryNativeProcess) Wait(ctx context.Context) (NativeResult, error) {
	p.waitOnce.Do(func() {
		go func() {
			waitErr := p.cmd.Wait()

			result := NativeResult{ExitCode: p.cmd.ProcessState.ExitCode()}
			if status, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
				result.Signal = int(status.Signal())
			}

			p.outcomeMu.Lock()
			p.terminal = true
			result.Revoked = p.revoked
			p.result = result
			p.waitErr = waitErr
			p.outcomeMu.Unlock()
			close(p.waitDone)
		}()
	})

	select {
	case <-ctx.Done():
		return NativeResult{}, ctx.Err()
	case <-p.waitDone:
		return p.result, p.waitErr
	}
}

func (p *ordinaryNativeProcess) Revoke(ctx context.Context) error {
	p.revokeOnce.Do(func() {
		p.outcomeMu.Lock()
		if !p.terminal && p.cmd.Process != nil {
			kill := p.kill
			if kill == nil {
				kill = p.cmd.Process.Kill
			}

			p.revokeErr = kill()
			switch {
			case p.revokeErr == nil:
				p.revoked = true
			case errors.Is(p.revokeErr, os.ErrProcessDone):
				p.revokeErr = nil
			}
		}
		p.outcomeMu.Unlock()
	})

	if p.revokeErr != nil {
		return p.revokeErr
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
