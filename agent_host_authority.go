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
	if !errors.Is(err, ErrHostAuthorityUnavailable) {
		return err
	}

	err = errors.Join(err, ErrContainmentIncomplete)

	a.mu.Lock()
	if a.authorityErr == nil {
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

		err = a.options.HostAuthority.PrepareNativeTree(ctx, root)
		err = a.recordHostAuthorityError(err)

		if err != nil && !errors.Is(err, ErrNativeTreeBusy) {
			err = errors.Join(err, ErrContainmentIncomplete)
		}

		return err
	}
	start.ReclaimNativeTree = func(ctx context.Context, root string) (err error) {
		defer func() {
			if recover() != nil {
				err = a.recordHostAuthorityError(ErrHostAuthorityUnavailable)
			}
		}()

		err = a.options.HostAuthority.ReclaimNativeTree(ctx, root)

		err = a.recordHostAuthorityError(err)
		if err != nil {
			err = errors.Join(err, ErrContainmentIncomplete)
		}

		return err
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
			return nil, errors.Join(a.recordHostAuthorityError(err), ErrContainmentIncomplete)
		}

		if nativeProcessNil(hostProcess) {
			return nil, a.recordHostAuthorityError(ErrHostAuthorityUnavailable)
		}

		return nativeProcessBridge{process: hostProcess}, nil
	}
}

func nativeProcessNil(process NativeProcess) bool {
	if process == nil {
		return true
	}

	value := reflect.ValueOf(process)

	return value.Kind() == reflect.Pointer && value.IsNil()
}

type nativeProcessBridge struct{ process NativeProcess }

func (p nativeProcessBridge) Stdin() io.WriteCloser { return p.process.Stdin() }
func (p nativeProcessBridge) Stdout() io.ReadCloser { return p.process.Stdout() }
func (p nativeProcessBridge) Stderr() io.ReadCloser { return p.process.Stderr() }

func (p nativeProcessBridge) Wait(ctx context.Context) (nativehermes.NativeResult, error) {
	result, err := p.process.Wait(ctx)

	return nativehermes.NativeResult{ExitCode: result.ExitCode, Signal: result.Signal, Revoked: result.Revoked}, err
}

func (p nativeProcessBridge) Revoke(ctx context.Context) error { return p.process.Revoke(ctx) }
