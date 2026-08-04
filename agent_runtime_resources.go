package hermesacp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/coder/acp-go-sdk"
	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
)

var runtimeRemoveAll = os.RemoveAll

type managedHermesServer struct {
	nativehermes.Server
	root             string
	sessionID        acp.SessionId
	nativeRelease    func()
	scratchRelease   func()
	retainIncomplete func(error, acp.SessionId, string)
	processRoot      *providerProcessRoot
	once             sync.Once
	closeErr         error
}

func (s *managedHermesServer) Close(ctx context.Context) error {
	s.once.Do(func() {
		if s.processRoot != nil {
			s.processRoot.observe(ctx, s.Server)
		}

		s.closeErr = s.Server.Close(ctx)
		if s.processRoot != nil {
			s.processRoot.retire(ctx, providerProcessTreeProven(s.closeErr))
		}

		if errors.Is(s.closeErr, nativehermes.ErrProcessContainmentIncomplete) {
			if s.retainIncomplete != nil {
				s.retainIncomplete(s.closeErr, s.sessionID, s.root)
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

func (s *managedHermesServer) ProviderDescendantCount() (int, bool) {
	inventory, ok := s.Server.(providerProcessInventory)
	if !ok {
		return 0, false
	}

	return inventory.ProviderDescendantCount()
}

func (a *Agent) retainIncompleteHermesRoot(id acp.SessionId, root string) {
	a.recordIncompleteContainment(nativehermes.ErrProcessContainmentIncomplete, id, root)
}

func (a *Agent) recordIncompleteContainment(err error, id acp.SessionId, root string) {
	if !errors.Is(err, nativehermes.ErrProcessContainmentIncomplete) {
		return
	}

	a.mu.Lock()
	if a.incompleteRoots[id] == nil {
		a.incompleteRoots[id] = make(map[string]struct{})
	}

	if root != "" {
		a.incompleteRoots[id][root] = struct{}{}
	}

	if a.containmentErr == nil {
		a.containmentErr = err
	}
	a.mu.Unlock()
}

func hermesServerRoot(server nativehermes.Server) string {
	if server == nil {
		return ""
	}

	return server.XDGDirs().Root
}

func (a *Agent) rejectIncompleteHermesSession(id acp.SessionId) error {
	a.mu.Lock()
	roots, retained := a.incompleteRoots[id]
	a.mu.Unlock()

	if retained {
		return fmt.Errorf("%w: Hermes session %q retains incomplete containment at %d generation roots", nativehermes.ErrProcessContainmentIncomplete, id, len(roots))
	}

	return nil
}

func deleteHermesScratchRoot(root string, scratchRelease func()) error {
	if err := errors.Join(runtimeRemoveAll(root), runtimeRemoveAll(nativehermes.ControlDirForXDG(root))); err != nil {
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
