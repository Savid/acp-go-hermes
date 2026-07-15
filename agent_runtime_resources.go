package hermesacp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

var runtimeRemoveAll = os.RemoveAll

type managedHermesServer struct {
	nativehermes.Server
	root           string
	nativeRelease  func()
	scratchRelease func()
	retainUnproven func(string)
	once           sync.Once
	closeErr       error
}

func (s *managedHermesServer) Close(ctx context.Context) error {
	s.once.Do(func() {
		s.closeErr = s.Server.Close(ctx)
		if errors.Is(s.closeErr, nativehermes.ErrProcessTreeUnproven) {
			if s.retainUnproven != nil {
				s.retainUnproven(s.root)
			}

			return
		}

		s.nativeRelease()

		if err := deleteHermesScratchRoot(s.root, s.scratchRelease); err != nil {
			s.closeErr = errors.Join(s.closeErr, err)

			return
		}
	})

	return s.closeErr
}

func (a *Agent) retainUnprovenHermesRoot(root string) {
	a.mu.Lock()
	a.unprovenRoots[root] = struct{}{}
	a.mu.Unlock()
}

func (a *Agent) rejectUnprovenHermesRoot(root string) error {
	a.mu.Lock()
	_, retained := a.unprovenRoots[root]
	a.mu.Unlock()

	if retained {
		return fmt.Errorf("%w: Hermes XDG root %q remains owned by an unproven process tree", nativehermes.ErrProcessTreeUnproven, root)
	}

	return nil
}

func deleteHermesScratchRoot(root string, scratchRelease func()) error {
	if err := runtimeRemoveAll(root); err != nil {
		return err
	}

	scratchRelease()

	return nil
}

func acquireNativeRoot(ctx context.Context, hooks RuntimeResourceHooks, kind RuntimeResourceKind) (func(), error) {
	return acquireRuntimeResource(ctx, hooks.AcquireNativeRoot, kind, "native root")
}

func reserveScratchRoot(ctx context.Context, hooks RuntimeResourceHooks, kind RuntimeResourceKind) (func(), error) {
	return acquireRuntimeResource(ctx, hooks.ReserveScratchRoot, kind, "scratch root")
}

func acquireRuntimeResource(
	ctx context.Context,
	acquire func(context.Context, RuntimeResourceKind) (func(), error),
	kind RuntimeResourceKind,
	resource string,
) (func(), error) {
	if acquire == nil {
		return func() {}, nil
	}

	release, err := acquire(ctx, kind)
	if err != nil {
		return nil, err
	}

	if release == nil {
		return nil, errors.New(resource + " hook returned nil release")
	}

	var once sync.Once

	return func() { once.Do(release) }, nil
}
