package hermes

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
)

type contextRespectingWaitProcess struct {
	mu            sync.Mutex
	attempts      int
	firstStarted  chan struct{}
	firstCanceled chan struct{}
	releaseFirst  chan struct{}
	firstErr      error
	result        NativeResult
	terminalErr   error
	stdout        io.ReadCloser
	stderr        io.ReadCloser
}

func (p *contextRespectingWaitProcess) Stdin() io.WriteCloser {
	return waitTestWriteCloser{Writer: io.Discard}
}
func (p *contextRespectingWaitProcess) Stdout() io.ReadCloser {
	if p.stdout != nil {
		return p.stdout
	}

	return io.NopCloser(strings.NewReader(""))
}
func (p *contextRespectingWaitProcess) Stderr() io.ReadCloser {
	if p.stderr != nil {
		return p.stderr
	}

	return io.NopCloser(strings.NewReader(""))
}
func (p *contextRespectingWaitProcess) Wait(ctx context.Context) (NativeResult, error) {
	p.mu.Lock()
	p.attempts++
	attempt := p.attempts
	p.mu.Unlock()
	if attempt == 1 && p.firstStarted != nil {
		close(p.firstStarted)
		<-ctx.Done()
		close(p.firstCanceled)
		<-p.releaseFirst
		if p.firstErr != nil {
			return NativeResult{}, p.firstErr
		}

		return NativeResult{}, ctx.Err()
	}

	return p.result, p.terminalErr
}
func (*contextRespectingWaitProcess) Revoke(context.Context) error { return nil }

type waitTestWriteCloser struct{ io.Writer }

func (waitTestWriteCloser) Close() error { return nil }

type blockingOutput struct {
	started chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func newBlockingOutput() *blockingOutput {
	return &blockingOutput{started: make(chan struct{}), closed: make(chan struct{})}
}

func (o *blockingOutput) Read([]byte) (int, error) {
	o.once.Do(func() { close(o.started) })
	<-o.closed

	return 0, io.EOF
}

func (o *blockingOutput) Close() error {
	select {
	case <-o.closed:
	default:
		close(o.closed)
	}

	return nil
}

func (p *contextRespectingWaitProcess) attemptCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.attempts
}

func TestProcessWaitCancellationRejoinsAndRetries(t *testing.T) {
	native := &contextRespectingWaitProcess{
		firstStarted: make(chan struct{}), firstCanceled: make(chan struct{}), releaseFirst: make(chan struct{}),
		result: NativeResult{ExitCode: 9, Revoked: true},
	}
	wantContainment := errors.New("containment incomplete")
	process := &Process{native: native, containmentIncomplete: wantContainment}
	firstDone := process.beginWait()
	<-native.firstStarted

	waitCtx, cancelWait := context.WithCancel(t.Context())
	cancelWait()
	result := make(chan error, 1)
	go func() {
		_, err := process.awaitCloseWait(firstDone, waitCtx)
		result <- err
	}()
	<-native.firstCanceled
	select {
	case err := <-result:
		t.Fatalf("wait returned before its owned goroutine rejoined: %v", err)
	default:
	}
	close(native.releaseFirst)
	if err := <-result; !errors.Is(err, wantContainment) || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled wait = %v", err)
	}
	process.waitMu.Lock()
	active := process.waitActive
	process.waitMu.Unlock()
	if active {
		t.Fatal("canceled wait flight remained active after rejoin")
	}

	secondDone := process.beginWait()
	if secondDone == firstDone {
		t.Fatal("retry reused the canceled wait flight")
	}
	secondResult, err := process.awaitCloseWait(secondDone, t.Context())
	if err != nil || secondResult != native.result || native.attemptCount() != 2 {
		t.Fatalf("retried wait = %#v, %v, attempts=%d", secondResult, err, native.attemptCount())
	}
}

func TestProcessWaitNaturalResultRemainsCached(t *testing.T) {
	native := &contextRespectingWaitProcess{result: NativeResult{ExitCode: 4}}
	process := &Process{native: native}
	firstDone := process.beginWait()
	result, err := process.awaitCloseWait(firstDone, t.Context())
	if err != nil || result != native.result {
		t.Fatalf("natural wait = %#v, %v", result, err)
	}
	if secondDone := process.beginWait(); secondDone != firstDone {
		t.Fatal("natural terminal wait was not cached")
	}
	if native.attemptCount() != 1 {
		t.Fatalf("natural wait attempts = %d", native.attemptCount())
	}
}

func TestProcessWaitAuthorityErrorRemainsCachedAfterCancellation(t *testing.T) {
	wantContainment := errors.New("containment incomplete")
	native := &contextRespectingWaitProcess{
		firstStarted: make(chan struct{}), firstCanceled: make(chan struct{}), releaseFirst: make(chan struct{}),
		firstErr: errors.Join(context.Canceled, wantContainment),
	}
	process := &Process{native: native, containmentIncomplete: wantContainment}
	done := process.beginWait()
	<-native.firstStarted

	waitCtx, cancelWait := context.WithCancel(t.Context())
	cancelWait()
	result := make(chan error, 1)
	go func() {
		_, err := process.awaitCloseWait(done, waitCtx)
		result <- err
	}()
	<-native.firstCanceled
	close(native.releaseFirst)
	if err := <-result; !errors.Is(err, wantContainment) {
		t.Fatalf("authority wait error = %v", err)
	}
	if cached := process.beginWait(); cached != done || native.attemptCount() != 1 {
		t.Fatalf("authority wait was not cached: same=%v attempts=%d", cached == done, native.attemptCount())
	}
}

func TestManagedProcessCanceledWaitRetainsTree(t *testing.T) {
	home := t.TempDir()
	stdout := newBlockingOutput()
	stderr := newBlockingOutput()
	native := &contextRespectingWaitProcess{
		firstStarted: make(chan struct{}), firstCanceled: make(chan struct{}), releaseFirst: make(chan struct{}),
		result: NativeResult{Revoked: true}, stdout: stdout, stderr: stderr,
	}
	wantContainment := errors.New("containment incomplete")
	reclaims := 0
	process := &Process{
		Home: home, native: native, managed: true, preparedHome: true, containmentIncomplete: wantContainment,
		reclaimNativeTree: func(context.Context, string) error {
			reclaims++

			return nil
		},
	}
	process.beginWait()
	process.drainOutput(io.Discard)
	<-native.firstStarted
	<-stdout.started
	<-stderr.started
	go func() {
		<-native.firstCanceled
		close(native.releaseFirst)
	}()
	closeCtx, cancelClose := context.WithCancel(t.Context())
	cancelClose()
	if err := process.Close(closeCtx); !errors.Is(err, wantContainment) || !errors.Is(err, context.Canceled) {
		t.Fatalf("timed out managed close = %v", err)
	}
	if reclaims != 0 {
		t.Fatalf("timed out managed close reclaims = %d", reclaims)
	}
	if _, err := os.Stat(home); err != nil {
		t.Fatalf("timed out managed close removed residence: %v", err)
	}
	process.outputMu.Lock()
	outputActive, outputWorkers := process.outputActive, process.outputWorkers
	process.outputMu.Unlock()
	if outputActive || outputWorkers != 0 {
		t.Fatalf("incomplete close output lifetime = active:%v workers:%d", outputActive, outputWorkers)
	}
	if err := process.Close(t.Context()); err != nil {
		t.Fatalf("retried managed close = %v", err)
	}
	if reclaims != 1 || native.attemptCount() != 2 {
		t.Fatalf("retried managed close reclaims=%d attempts=%d", reclaims, native.attemptCount())
	}
}
