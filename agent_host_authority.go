package hermesacp

import (
	"context"
	"errors"
	"io"
	"reflect"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

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
	if authorityUnavailable && a.authorityErr == nil {
		a.authorityErr = err
	}

	if a.containmentErr == nil {
		a.containmentErr = err
	}
	a.mu.Unlock()

	return err
}

func (a *Agent) configureHostAuthority(start *nativehermes.StartOptions) {
	start.NativeEnvironment = cloneStringMap(a.nativeEnv)
	if a.options.HostAuthority == nil {
		return
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

		return a.recordHostAuthorityError(errors.Join(err, ErrContainmentIncomplete))
	}
	start.ReclaimNativeTree = func(ctx context.Context, root string) (err error) {
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

	return p.process.Stdin()
}

func (p nativeProcessBridge) Stdout() (stream io.ReadCloser) {
	defer func() {
		if recover() != nil {
			stream = nil
			_ = p.recordError(ErrHostAuthorityUnavailable)
		}
	}()

	return p.process.Stdout()
}

func (p nativeProcessBridge) Stderr() (stream io.ReadCloser) {
	defer func() {
		if recover() != nil {
			stream = nil
			_ = p.recordError(ErrHostAuthorityUnavailable)
		}
	}()

	return p.process.Stderr()
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

	return p.process.Revoke(ctx)
}
