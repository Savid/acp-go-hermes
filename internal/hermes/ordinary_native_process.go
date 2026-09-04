package hermes

import (
	"context"
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

// newProcessPipe is the seam the pipe-exhaustion branches are proven through.
var newProcessPipe = os.Pipe

func startOrdinaryNative(ctx context.Context, request NativeRequest) (NativeProcess, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	cmd := exec.Command(request.Executable, request.Arguments...) //nolint:gosec // Ordinary mode executes the configured Hermes harness.
	cmd.Dir = request.WorkingDirectory
	cmd.Env = append([]string(nil), request.Environment...)

	return startOrdinaryNativeWithPipes(cmd, newProcessPipe)
}

func startOrdinaryNativeWithPipes(cmd *exec.Cmd, newPipe func() (*os.File, *os.File, error)) (NativeProcess, error) {
	pipes, err := ordinaryProcessPipes(cmd, newPipe)
	if err != nil {
		return nil, err
	}

	if err := cmd.Start(); err != nil {
		pipes.closeChildEnds()
		pipes.closeParentEnds()

		return nil, err
	}

	// The child holds its own copies now; keeping the parent's copies of the
	// child ends open would leave every reader waiting on an EOF that never
	// arrives.
	pipes.closeChildEnds()

	return &ordinaryNativeProcess{
		cmd: cmd, stdin: pipes.stdin, stdout: pipes.stdout, stderr: pipes.stderr, kill: cmd.Process.Kill, waitDone: make(chan struct{}),
	}, nil
}

// ordinaryPipes holds both ends of the child's three standard streams while the
// process is being started, so a failure at any point releases every descriptor
// it already claimed.
type ordinaryPipes struct {
	stdin  *os.File
	stdout *os.File
	stderr *os.File

	childStdin  *os.File
	childStdout *os.File
	childStderr *os.File
}

func (p *ordinaryPipes) closeChildEnds() {
	closeFile(p.childStdin)
	closeFile(p.childStdout)
	closeFile(p.childStderr)
}

func (p *ordinaryPipes) closeParentEnds() {
	closeFile(p.stdin)
	closeFile(p.stdout)
	closeFile(p.stderr)
}

func closeFile(file *os.File) {
	if file != nil {
		_ = file.Close()
	}
}

// ordinaryProcessPipes wires the child's three standard streams as ordinary OS
// pipes this process owns outright.
//
// exec.Cmd's own StdinPipe/StdoutPipe/StderrPipe hand their parent ends to
// Cmd.Wait, which closes them the moment the child exits — a close that races
// whoever is still draining what the child already wrote. The version probe
// reads a short-lived child whose whole answer lands just before it exits, so
// on a busy machine that race is lost routinely and the probe reads an empty
// version. Owning the pipes keeps each parent end open until its reader sees
// EOF, so the child's bytes survive whatever the scheduler does with the exit.
func ordinaryProcessPipes(cmd *exec.Cmd, newPipe func() (*os.File, *os.File, error)) (_ *ordinaryPipes, err error) {
	pipes := &ordinaryPipes{}

	defer func() {
		if err != nil {
			pipes.closeChildEnds()
			pipes.closeParentEnds()
		}
	}()

	pipes.childStdin, pipes.stdin, err = newPipe()
	if err != nil {
		return nil, fmt.Errorf("create native stdin: %w", err)
	}

	pipes.stdout, pipes.childStdout, err = newPipe()
	if err != nil {
		return nil, fmt.Errorf("create native stdout: %w", err)
	}

	pipes.stderr, pipes.childStderr, err = newPipe()
	if err != nil {
		return nil, fmt.Errorf("create native stderr: %w", err)
	}

	cmd.Stdin = pipes.childStdin
	cmd.Stdout = pipes.childStdout
	cmd.Stderr = pipes.childStderr

	return pipes, nil
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
			case nativeProcessAlreadyFinished(p.revokeErr):
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
