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

var managedRemoveAll = os.RemoveAll

type managedHermesServer struct {
	nativehermes.Server
	root               string
	sessionID          acp.SessionId
	managed            bool
	scratchRelease     func()
	retainIncomplete   func(error, acp.SessionId, string)
	nativeSessionOwner *nativehermes.SharedSessionOwner
	mu                 sync.Mutex
	settled            bool
	closed             bool
	ownerReleased      bool
	settlementErr      error
	closeErr           error
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

func (s *managedHermesServer) ForkWithBaseline(ctx context.Context, id, marker string, baseline []string) (nativehermes.Session, error) {
	forker, ok := s.Server.(nativehermes.RecoverableSessionForker)
	if !ok {
		return nativehermes.Session{}, errors.New("hermes server does not expose recoverable session fork")
	}

	return forker.ForkWithBaseline(ctx, id, marker, baseline)
}

func (s *managedHermesServer) SetModel(ctx context.Context, id, value string) error {
	setter, ok := s.Server.(interface {
		SetModel(context.Context, string, string) error
	})
	if !ok {
		return errors.New("hermes server does not expose session model selection")
	}

	return setter.SetModel(ctx, id, value)
}

func (s *managedHermesServer) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return s.closeErr
	}

	var closeErr error
	if !s.settled {
		closeErr = s.Server.Close(ctx)
		if closeErr != nil {
			if s.retainIncomplete != nil {
				if errors.Is(closeErr, ErrContainmentIncomplete) || errors.Is(closeErr, ErrHostAuthorityUnavailable) {
					s.retainIncomplete(closeErr, s.sessionID, s.root)
				}
			}

			return closeErr
		}

		if s.nativeSessionOwner != nil && !s.ownerReleased {
			closeErr = errors.Join(closeErr, s.nativeSessionOwner.Release())
			s.ownerReleased = true
		}

		s.settlementErr = closeErr
		s.settled = true
	}

	cleanupErr := deleteHermesScratchRoot(s.root, s.scratchRelease)
	if cleanupErr != nil {
		return errors.Join(s.settlementErr, cleanupErr)
	}

	s.closeErr = s.settlementErr
	s.closed = true

	return s.closeErr
}

func (s *managedHermesServer) reclaimForSnapshot(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.managed {
		return "", errors.New("hermes snapshot reclaim requires host authority")
	}

	if s.closed {
		return "", errors.New("hermes snapshot residence is already closed")
	}

	if s.settled {
		return s.root, nil
	}

	if err := s.Server.Close(ctx); err != nil {
		if s.retainIncomplete != nil && (errors.Is(err, ErrContainmentIncomplete) || errors.Is(err, ErrHostAuthorityUnavailable)) {
			s.retainIncomplete(err, s.sessionID, s.root)
		}

		return "", err
	}

	if s.nativeSessionOwner != nil && !s.ownerReleased {
		if err := s.nativeSessionOwner.Release(); err != nil {
			return "", err
		}

		s.ownerReleased = true
	}

	s.settled = true

	return s.root, nil
}

func (s *managedHermesServer) finishReclaimedSnapshot() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return s.closeErr
	}

	if !s.settled {
		return errors.New("hermes snapshot residence has not been reclaimed")
	}

	if err := deleteHermesScratchRoot(s.root, s.scratchRelease); err != nil {
		return err
	}

	s.closed = true

	return nil
}

func (s *managedHermesServer) ProviderAuthSupported() bool {
	supported, ok := s.Server.(interface{ ProviderAuthSupported() bool })

	return ok && supported.ProviderAuthSupported()
}

func (a *Agent) retainIncompleteHermesRoot(id acp.SessionId, root string) {
	a.recordIncompleteContainment(ErrContainmentIncomplete, id, root)
}

func (a *Agent) claimSharedNativeSession(client nativehermes.Server, nativeSessionID string) error {
	owner, err := a.acquireSharedNativeSessionOwner(nativeSessionID)
	if err != nil || owner == nil {
		return err
	}

	managed, ok := client.(*managedHermesServer)
	if !ok {
		return errors.Join(errors.New("shared Hermes native session claim requires a managed server"), owner.Release())
	}

	managed.nativeSessionOwner = owner

	return nil
}

func (a *Agent) acquireSharedNativeSessionOwner(nativeSessionID string) (*nativehermes.SharedSessionOwner, error) {
	if a.options.SharedHermesHome == "" {
		return nil, nil //nolint:nilnil // No shared home has no native-session owner.
	}

	return nativehermes.AcquireSharedNativeSessionOwner(a.options.SharedHermesHome, nativeSessionID)
}

func (a *Agent) recordIncompleteContainment(err error, id acp.SessionId, root string) {
	if !errors.Is(err, ErrContainmentIncomplete) && !errors.Is(err, ErrHostAuthorityUnavailable) {
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
		return fmt.Errorf("%w: Hermes session %q retains %d native trees", ErrContainmentIncomplete, id, len(roots))
	}

	return nil
}

func deleteHermesScratchRoot(root string, release func()) error {
	if root == "" {
		return nil
	}

	if err := errors.Join(managedRemoveAll(root), managedRemoveAll(nativehermes.ControlDirForXDG(root))); err != nil {
		return err
	}

	if release != nil {
		release()
	}

	return nil
}
