package hermesacp

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

	"github.com/coder/acp-go-sdk"
)

// Effort selection vocabulary.
const (
	configEffort                    = "effort"
	valHermesEffortSelectionRefused = "hermes_effort_selection_refused"

	// effortReadTimeout bounds one native config.get reasoning read, which the
	// gateway answers from session state without a provider round trip.
	effortReadTimeout = 5 * time.Second
)

// hermesEffortLevels are the reasoning efforts Hermes selects for a session,
// low to high, with none disabling thinking. Hermes's reasoning key also takes
// display words, which this option never forwards.
var hermesEffortLevels = []string{"none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"}

func hermesEffortLevel(value string) bool {
	return slices.Contains(hermesEffortLevels, value)
}

// modelChoiceMeta is the per-choice metadata of a model option: the qualified
// selection value and the efforts a session on that model can select. Hermes
// takes every level for every model and clamps it to what the provider accepts
// when the request is sent.
func modelChoiceMeta(value string) map[string]any {
	return map[string]any{hermesMetaKey: map[string]any{
		"modelId":               value,
		"supportedEffortLevels": slices.Clone(hermesEffortLevels),
	}}
}

// effortSetter is the native surface that applies and reads a session's
// reasoning effort.
type effortSetter interface {
	SetEffort(context.Context, string, string) (string, error)
	Effort(context.Context, string) (string, error)
}

// applyNativeEffort binds effort on the native session and returns the level
// Hermes acknowledged.
func applyNativeEffort(ctx context.Context, client any, nativeSessionID string, effort string) (string, error) {
	setter, ok := client.(effortSetter)
	if !ok {
		return "", errors.New("hermes server does not expose session effort selection")
	}

	return setter.SetEffort(ctx, nativeSessionID, effort)
}

func (s *session) setEffort(value string) {
	s.mu.Lock()
	s.effort = value
	s.mu.Unlock()
}

func (s *session) currentEffort() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.effort
}

// effortConfigOption publishes the effort select. The current value is the
// level the session runs at: the one this adapter applied, else the one
// Hermes reports for the session, which is the config default until a
// selection pins it. A session with neither publishes no effort option.
func (s *session) effortConfigOption(ctx context.Context) acp.SessionConfigOption {
	current := s.currentEffort()
	if current == "" {
		snapshot := s.snapshot()

		client := snapshot.client
		if managed, ok := client.(*managedHermesServer); ok {
			client = managed.Server
		}

		reader, ok := client.(effortSetter)
		if !ok {
			return acp.SessionConfigOption{}
		}

		readCtx, cancel := context.WithTimeout(ctx, effortReadTimeout)
		defer cancel()

		effort, err := reader.Effort(readCtx, snapshot.idmap.NativeSessionID)
		if err != nil || effort == "" {
			return acp.SessionConfigOption{}
		}

		current = effort
		s.setEffort(current)
	}

	values := make(acp.SessionConfigSelectOptionsUngrouped, 0, len(hermesEffortLevels)+1)
	for _, level := range hermesEffortLevels {
		values = append(values, acp.SessionConfigSelectOption{Name: level, Value: acp.SessionConfigValueId(level)})
	}

	if !hermesEffortLevel(current) {
		values = append(values, acp.SessionConfigSelectOption{Name: current, Value: acp.SessionConfigValueId(current)})
	}

	category := acp.SessionConfigOptionCategoryThoughtLevel

	return acp.SessionConfigOption{Select: &acp.SessionConfigOptionSelect{
		Id:           configEffort,
		Name:         "Effort",
		Category:     &category,
		Type:         configTypeSelect,
		CurrentValue: acp.SessionConfigValueId(current),
		Options:      acp.SessionConfigSelectOptions{Ungrouped: &values},
	}}
}

// claimAndBindNativeSession claims a freshly created native session for this
// Agent and binds the requested effort on it, closing the client and recording
// containment on either failure so the caller has nothing left to release.
func (a *Agent) claimAndBindNativeSession(
	ctx context.Context,
	client nativehermes.Server,
	id acp.SessionId,
	nativeSessionID string,
	meta *sessionMeta,
) error {
	err := a.claimSharedNativeSession(client, nativeSessionID)
	if err == nil && meta.Effort != "" {
		var applied string

		applied, err = applyNativeEffort(ctx, client, nativeSessionID, meta.Effort)
		if err != nil {
			err = fmt.Errorf("bind Hermes session effort: %w", err)
		} else {
			meta.Effort = applied
		}
	}

	if err == nil {
		return nil
	}

	closeErr := closeHermesClientAfterStartupFailure(client)
	a.recordIncompleteContainment(closeErr, id, hermesServerRoot(client))

	return errors.Join(err, closeErr)
}
