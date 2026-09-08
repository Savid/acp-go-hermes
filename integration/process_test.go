//go:build integration

package integration

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
)

// integrationProcess owns the direct child and both parent pipe ends. Its one
// Wait publishes a reusable result; closing stdin gives the adapter time to
// settle before forced termination. This is not a descendant-containment proof.
type integrationProcess struct {
	cmd    *exec.Cmd
	stdin  *os.File
	stdout *os.File
	stderr processLog
	done   chan struct{}
	err    error
	close  func()
}

type processLog struct {
	mu   sync.Mutex
	data []byte
}

func (b *processLog) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data = append(b.data, p...)
	return len(p), nil
}
func (b *processLog) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.data)
}

func startIntegrationProcess(t *testing.T, cmd *exec.Cmd) *integrationProcess {
	t.Helper()
	childStdin, stdin, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, childStdout, err := os.Pipe()
	if err != nil {
		_ = childStdin.Close()
		_ = stdin.Close()
		t.Fatal(err)
	}
	p := &integrationProcess{cmd: cmd, stdin: stdin, stdout: stdout, done: make(chan struct{})}
	cmd.Stdin, cmd.Stdout, cmd.Stderr = childStdin, childStdout, &p.stderr
	cmd.Cancel = stdin.Close
	cmd.WaitDelay = 5 * time.Second
	err = cmd.Start()
	_ = childStdin.Close()
	_ = childStdout.Close()
	if err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		t.Fatal(err)
	}
	go func() { p.err = cmd.Wait(); close(p.done) }()
	p.close = sync.OnceFunc(func() {
		defer stdout.Close()
		_ = stdin.Close()
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				t.Errorf("kill integration child: %v", err)
			}
			select {
			case <-p.done:
			case <-time.After(6 * time.Second):
				t.Error("integration child Wait did not finish after kill and pipe deadline")
			}
		}
	})
	t.Cleanup(p.close)
	return p
}

func (p *integrationProcess) wait(ctx context.Context) error {
	select {
	case <-p.done:
		return p.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestIntegrationProcessCleanup(t *testing.T) {
	for _, mode := range []string{"eof", "cancel", "ignore-eof"} {
		t.Run(mode, func(t *testing.T) {
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, "-test.run=^TestIntegrationProcessChild$", "--", mode)
			p := startIntegrationProcess(t, cmd)
			ready := make([]byte, 1)
			if _, err := io.ReadFull(p.stdout, ready); err != nil || ready[0] != 'R' {
				t.Fatalf("child readiness: %q %v", ready, err)
			}
			if mode == "cancel" {
				cancel()
			}
			p.close()
			p.close()
			select {
			case <-p.done:
			default:
				t.Fatal("cleanup returned before child Wait")
			}
			if p.cmd.ProcessState == nil {
				t.Fatal("child exit not observed")
			}
			if mode == "ignore-eof" && p.cmd.ProcessState.Success() {
				t.Fatal("child that ignored EOF was not terminated")
			}
			if mode == "eof" && p.err != nil {
				t.Fatalf("normal EOF: %v", p.err)
			}
		})
	}
}

func TestIntegrationProcessChild(t *testing.T) {
	if len(os.Args) < 3 || os.Args[len(os.Args)-2] != "--" {
		return
	}
	mode := os.Args[len(os.Args)-1]
	_, _ = os.Stdout.Write([]byte("R"))
	if mode == "ignore-eof" {
		<-time.After(time.Minute)
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
	os.Exit(0)
}
