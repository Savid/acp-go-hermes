package hermesacp

import (
	"context"
	"errors"
	"io"
	"os"
	"reflect"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

var retiredNativeRemoveAll = os.RemoveAll

func hostAuthorityNil(authority HostAuthority) bool {
	if authority == nil {
		return true
	}

	value := reflect.ValueOf(authority)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func readHostEnvironment(authority HostAuthority) (environment map[string]string, err error) {
	if hostAuthorityNil(authority) {
		return nil, ErrHostAuthorityUnavailable
	}

	defer func() {
		if recover() != nil {
			environment = nil
			err = ErrHostAuthorityUnavailable
		}
	}()

	environment = authority.NativeEnvironment()
	if environment == nil {
		return nil, ErrHostAuthorityUnavailable
	}

	return cloneStringMap(environment), nil
}

func validateHostAuthority(options Options) error {
	if !options.hostAuthoritySupplied {
		return nil
	}

	if hostAuthorityNil(options.HostAuthority) {
		return ErrHostAuthorityUnavailable
	}

	if options.SharedHermesHome != "" {
		return errors.New("host authority requires isolated Hermes residences")
	}

	return nil
}

func (a *Agent) hostAuthorityAdmissionError() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.authorityErr
}

func (a *Agent) recordHostAuthorityError(err error) error {
	if err == nil {
		return nil
	}

	authorityUnavailable := errors.Is(err, ErrHostAuthorityUnavailable)
	if authorityUnavailable {
		err = errors.Join(err, ErrContainmentIncomplete)
	}

	if !authorityUnavailable && !errors.Is(err, ErrContainmentIncomplete) {
		return err
	}

	a.mu.Lock()

	firstUnavailable := authorityUnavailable && a.authorityErr == nil
	if authorityUnavailable && a.authorityErr == nil {
		a.authorityErr = err
	}

	if a.containmentErr == nil {
		a.containmentErr = err
	}

	sessions := make([]*session, 0, len(a.sessions))
	if firstUnavailable {
		for _, current := range a.sessions {
			sessions = append(sessions, current)
		}
	}
	a.mu.Unlock()

	for _, current := range sessions {
		current.closeLifecycleAdmission()
		current.lifecycleStream().fence()
	}

	if len(sessions) != 0 {
		go a.fenceAuthoritySessions(sessions)
	}

	return err
}

func (a *Agent) fenceAuthoritySessions(sessions []*session) {
	for _, current := range sessions {
		current.prepareClose()

		ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		err := current.Close(ctx)

		cancel()

		a.recordIncompleteContainment(err, current.id, hermesServerRoot(current.client))
	}
}

func (a *Agent) configureHostAuthority(start *nativehermes.StartOptions) {
	start.NativeEnvironment = cloneStringMap(a.nativeEnv)
	if a.options.HostAuthority == nil {
		return
	}

	start.RetainNativeTree = a.retainNativeTree
	start.ContainmentIncomplete = ErrContainmentIncomplete
	start.NativeTreeBusy = ErrNativeTreeBusy
	start.NativeTreeSettled = func() {
		ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		_ = a.retryRetiredNativeRoots(ctx)

		cancel()
	}

	start.PrepareNativeTree = func(ctx context.Context, root string) (err error) {
		if admissionErr := a.hostAuthorityAdmissionError(); admissionErr != nil {
			return admissionErr
		}

		defer func() {
			if recover() != nil {
				err = a.recordHostAuthorityError(ErrHostAuthorityUnavailable)
			}
		}()

		err = a.recordHostAuthorityError(a.options.HostAuthority.PrepareNativeTree(ctx, root))
		if err == nil {
			return nil
		}

		if errors.Is(err, ErrNativeTreeBusy) {
			return err
		}

		return a.recordHostAuthorityError(errors.Join(err, ErrContainmentIncomplete))
	}
	start.ReclaimNativeTree = a.reclaimNativeTree
	start.StartNative = func(ctx context.Context, request nativehermes.NativeRequest) (process nativehermes.NativeProcess, err error) {
		if admissionErr := a.hostAuthorityAdmissionError(); admissionErr != nil {
			return nil, admissionErr
		}

		defer func() {
			if recover() != nil {
				process = nil
				err = a.recordHostAuthorityError(ErrHostAuthorityUnavailable)
			}
		}()

		hostProcess, err := a.options.HostAuthority.StartNative(ctx, NativeRequest{
			Executable: request.Executable, Arguments: append([]string(nil), request.Arguments...),
			Environment: append([]string(nil), request.Environment...), WorkingDirectory: request.WorkingDirectory,
		})
		if err != nil {
			return nil, a.recordHostAuthorityError(errors.Join(err, ErrContainmentIncomplete))
		}

		if nativeProcessNil(hostProcess) {
			return nil, a.recordHostAuthorityError(ErrHostAuthorityUnavailable)
		}

		return nativeProcessBridge{process: hostProcess, record: a.recordHostAuthorityError}, nil
	}
}

func (a *Agent) reclaimNativeTree(ctx context.Context, root string) (err error) {
	defer func() {
		if recover() != nil {
			err = a.recordHostAuthorityError(ErrHostAuthorityUnavailable)
		}
	}()

	err = a.options.HostAuthority.ReclaimNativeTree(ctx, root)
	if errors.Is(err, ErrNativeTreeBusy) {
		return err
	}

	if err != nil {
		return a.recordHostAuthorityError(errors.Join(err, ErrContainmentIncomplete))
	}

	return nil
}

func (a *Agent) retainNativeTree(root string, err error) bool {
	if root == "" || (err != nil && !errors.Is(err, ErrNativeTreeBusy)) {
		return false
	}

	a.retiredNativeRetry.Lock()
	defer a.retiredNativeRetry.Unlock()

	a.mu.Lock()
	pending, retained := a.retiredNativeRoots[root]
	a.retiredNativeRoots[root] = err != nil || (retained && pending)
	a.mu.Unlock()

	return true
}

func (a *Agent) retryRetiredNativeRoots(ctx context.Context) error {
	if a.options.HostAuthority == nil {
		return nil
	}

	a.retiredNativeRetry.Lock()
	defer a.retiredNativeRetry.Unlock()

	a.mu.Lock()

	type retainedRoot struct {
		root           string
		reclaimPending bool
	}

	roots := make([]retainedRoot, 0, len(a.retiredNativeRoots))
	for root, reclaimPending := range a.retiredNativeRoots {
		roots = append(roots, retainedRoot{root: root, reclaimPending: reclaimPending})
	}
	a.mu.Unlock()

	var (
		joined error
		busy   bool
	)

	for _, retained := range roots {
		if err := ctx.Err(); err != nil {
			return errors.Join(joined, err)
		}

		if retained.reclaimPending {
			err := a.reclaimNativeTree(ctx, retained.root)
			if errors.Is(err, ErrNativeTreeBusy) {
				busy = true

				continue
			}

			if err != nil {
				joined = errors.Join(joined, err)

				continue
			}

			a.mu.Lock()
			a.retiredNativeRoots[retained.root] = false
			a.mu.Unlock()
		}

		if err := retiredNativeRemoveAll(retained.root); err != nil {
			joined = errors.Join(joined, err)

			continue
		}

		a.mu.Lock()
		delete(a.retiredNativeRoots, retained.root)
		a.mu.Unlock()
	}

	if busy {
		joined = errors.Join(joined, ErrNativeTreeBusy)
	}

	return joined
}

func (a *Agent) beginManagedNativeGeneration(ctx context.Context) (func(), error) {
	if a.options.HostAuthority == nil {
		return func() {}, nil
	}

	a.nativeAdmissionMu.Lock()

	if err := a.hostAuthorityAdmissionError(); err != nil {
		a.nativeAdmissionMu.Unlock()

		return nil, err
	}

	if err := a.retryRetiredNativeRoots(ctx); err != nil {
		a.nativeAdmissionMu.Unlock()

		return nil, err
	}

	return a.nativeAdmissionMu.Unlock, nil
}

func (a *Agent) ownsRetiredNativeRoot(root string) bool {
	a.mu.Lock()
	_, retained := a.retiredNativeRoots[root]
	a.mu.Unlock()

	return retained
}

func nativeProcessNil(process NativeProcess) bool {
	if process == nil {
		return true
	}

	value := reflect.ValueOf(process)

	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

type nativeProcessBridge struct {
	process NativeProcess
	record  func(error) error
}

func (p nativeProcessBridge) recordError(err error) error {
	if p.record == nil {
		return err
	}

	return p.record(err)
}

func (p nativeProcessBridge) Stdin() (stream io.WriteCloser) {
	defer func() {
		if recover() != nil {
			stream = nil
			_ = p.recordError(ErrHostAuthorityUnavailable)
		}
	}()

	stream = p.process.Stdin()
	if stream == nil {
		_ = p.recordError(ErrHostAuthorityUnavailable)
	}

	return stream
}

func (p nativeProcessBridge) Stdout() (stream io.ReadCloser) {
	defer func() {
		if recover() != nil {
			stream = nil
			_ = p.recordError(ErrHostAuthorityUnavailable)
		}
	}()

	stream = p.process.Stdout()
	if stream == nil {
		_ = p.recordError(ErrHostAuthorityUnavailable)
	}

	return stream
}

func (p nativeProcessBridge) Stderr() (stream io.ReadCloser) {
	defer func() {
		if recover() != nil {
			stream = nil
			_ = p.recordError(ErrHostAuthorityUnavailable)
		}
	}()

	stream = p.process.Stderr()
	if stream == nil {
		_ = p.recordError(ErrHostAuthorityUnavailable)
	}

	return stream
}

func (p nativeProcessBridge) Wait(ctx context.Context) (result nativehermes.NativeResult, err error) {
	defer func() {
		if recover() != nil {
			result = nativehermes.NativeResult{}
			err = p.recordError(ErrHostAuthorityUnavailable)
		}
	}()

	hostResult, err := p.process.Wait(ctx)
	if err != nil {
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) &&
			!errors.Is(err, ErrHostAuthorityUnavailable) && !errors.Is(err, ErrContainmentIncomplete) {
			return nativehermes.NativeResult{
				ExitCode: hostResult.ExitCode, Signal: hostResult.Signal, Revoked: hostResult.Revoked,
			}, err
		}

		err = p.recordError(errors.Join(err, ErrContainmentIncomplete))
	}

	return nativehermes.NativeResult{ExitCode: hostResult.ExitCode, Signal: hostResult.Signal, Revoked: hostResult.Revoked}, err
}

func (p nativeProcessBridge) Revoke(ctx context.Context) (err error) {
	defer func() {
		if recover() != nil {
			err = p.recordError(ErrHostAuthorityUnavailable)
		}
	}()

	err = p.process.Revoke(ctx)
	if err == nil {
		return nil
	}

	if ctx.Err() != nil && errors.Is(err, ctx.Err()) &&
		!errors.Is(err, ErrHostAuthorityUnavailable) && !errors.Is(err, ErrContainmentIncomplete) {
		return err
	}

	if errors.Is(err, ErrHostAuthorityUnavailable) || errors.Is(err, ErrContainmentIncomplete) {
		return p.recordError(err)
	}

	return err
}
