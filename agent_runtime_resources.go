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
	root                  string
	sessionID             acp.SessionId
	nativeRelease         func()
	scratchRelease        func()
	retainIncomplete      func(error, acp.SessionId, string)
	processRoot           *providerProcessRoot
	providerAuthSupported bool
	nativeSessionOwner    *nativehermes.SharedSessionOwner
	once                  sync.Once
	closeErr              error
}

func (s *managedHermesServer) CreateSessionWithDraft(
	ctx context.Context,
	title string,
	bind func(nativehermes.SessionDraft) error,
) (nativehermes.Session, error) {
	creator, ok := s.Server.(nativehermes.DraftSessionCreator)
	if !ok {
		return nativehermes.Session{}, errors.New("hermes server does not expose draft session creation")
	}

	return creator.CreateSessionWithDraft(ctx, title, bind)
}

func (s *managedHermesServer) PersistedSessions(ctx context.Context) ([]nativehermes.Session, error) {
	lister, ok := s.Server.(nativehermes.PersistedSessionLister)
	if !ok {
		return nil, errors.New("hermes server does not expose persisted session inventory")
	}

	return lister.PersistedSessions(ctx)
}

func (s *managedHermesServer) ForkWithBaseline(
	ctx context.Context,
	id string,
	marker string,
	baseline []string,
) (nativehermes.Session, error) {
	forker, ok := s.Server.(nativehermes.RecoverableSessionForker)
	if !ok {
		return nativehermes.Session{}, errors.New("hermes server does not expose recoverable session fork")
	}

	return forker.ForkWithBaseline(ctx, id, marker, baseline)
}

// SetModel forwards the required session-scoped mutation through the managed
// lifecycle wrapper without widening the base native Server contract.
func (s *managedHermesServer) SetModel(ctx context.Context, id string, value string) error {
	setter, ok := s.Server.(interface {
		SetModel(context.Context, string, string) error
	})
	if !ok {
		return errors.New("hermes server does not expose session model selection")
	}

	return setter.SetModel(ctx, id, value)
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
			s.nativeSessionOwner.Retain()

			if s.retainIncomplete != nil {
				s.retainIncomplete(s.closeErr, s.sessionID, s.root)
			}

			return
		}

		s.closeErr = errors.Join(s.closeErr, s.nativeSessionOwner.Release())

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

func (s *managedHermesServer) ProviderTreeVacant() (bool, bool) {
	inventory, ok := s.Server.(providerTreeInventory)
	if !ok {
		return false, false
	}

	return inventory.ProviderTreeVacant()
}

func (s *managedHermesServer) ProviderAuthSupported() bool {
	if s.providerAuthSupported {
		return true
	}

	supported, ok := s.Server.(interface{ ProviderAuthSupported() bool })
	if ok {
		return supported.ProviderAuthSupported()
	}

	return false
}

func (a *Agent) retainIncompleteHermesRoot(id acp.SessionId, root string) {
	a.recordIncompleteContainment(nativehermes.ErrProcessContainmentIncomplete, id, root)
}

func (a *Agent) claimSharedNativeSession(client nativehermes.Server, nativeSessionID string) error {
	owner, err := a.acquireSharedNativeSessionOwner(nativeSessionID)
	if err != nil {
		return err
	}

	if owner == nil {
		return nil
	}

	managed, ok := client.(*managedHermesServer)
	if !ok {
		return errors.Join(errors.New("shared Hermes native session claim requires a managed server"), owner.Release())
	}

	if managed.nativeSessionOwner != nil {
		return errors.Join(errors.New("shared Hermes native session is already claimed by this server"), owner.Release())
	}

	if err := nativehermes.BindSharedSessionOwnerToServer(owner, managed.Server); err != nil {
		return errors.Join(err, owner.Release())
	}

	managed.nativeSessionOwner = owner

	return nil
}

func (a *Agent) acquireSharedNativeSessionOwner(nativeSessionID string) (*nativehermes.SharedSessionOwner, error) {
	if a.options.SharedHermesHome == "" {
		return nil, nil //nolint:nilnil // No owner is the explicit isolated-home result.
	}

	return nativehermes.AcquireSharedNativeSessionOwner(a.options.SharedHermesHome, nativeSessionID)
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
