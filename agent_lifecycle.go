//nolint:tagliatelle // ACP wire member names keep their protocol spelling.
package hermesacp

import (
	"encoding/json"
	"slices"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-hermes/internal/lifecycle"
)

// negotiateLifecycle reads the host's `acp-go.dev/lifecycle` offer and answers
// with the facts this connection's active configuration proved. The answer is
// the contract for the whole connection. With no offer, the key is omitted
// from the response and no envelope, correlation read, or lifecycle fact
// exists on the connection at all.
func (a *Agent) negotiateLifecycle(meta map[string]any) (map[string]any, error) {
	offered, refusal := lifecycle.DecodeCapability(meta)
	if refusal != nil {
		return nil, lifecycleParamError(refusal)
	}

	if !offered {
		if retainErr := a.retainNegotiatedLifecycle(lifecycle.Negotiated{}); retainErr != nil {
			return nil, retainErr
		}

		//nolint:nilnil // An omitted capability produces no advertisement and no refusal.
		return nil, nil
	}

	answer := a.provenLifecycleFacts()
	answer.Version = lifecycle.Version

	if retainErr := a.retainNegotiatedLifecycle(answer); retainErr != nil {
		return nil, retainErr
	}

	return map[string]any{lifecycle.MetaKey: answer.Advertisement()}, nil
}

// provenLifecycleFacts states what this configuration can actually prove.
//
// One `hermes serve` process owns a session for its whole lifetime, so the
// session's ordered stream is session-owned and delivers between prompts:
// `updatesOutsidePrompt` is true, and the opening snapshot rides that channel
// after the establishing response. The gateway's structured event vocabulary
// carries no background activity entity — a todo list and a plan are
// presentation state, never evidence — so `activityKinds` is empty. A host
// authority supplies the terminal whole-tree proof required to advertise
// authoritative quiescence; ordinary execution does not make that claim.
func (a *Agent) provenLifecycleFacts() lifecycle.Negotiated {
	proven := lifecycle.Negotiated{
		UpdatesOutsidePrompt: true,
		ActivityKinds:        []lifecycle.ActivityKind{},
	}
	if a.options.HostAuthority != nil {
		proven.AuthoritativeQuiescence = true
		proven.QuiescenceSource = lifecycle.ProofClassProcessContainment
	}

	return proven
}

// retainNegotiatedLifecycle records this connection's one answer. The answer is
// the contract for the whole connection, not a value the latest `initialize`
// happens to hold: a second negotiation that would change it is refused on the
// lifecycle key rather than admitted. Withdrawing a present answer is the case
// that matters — enabling version 1 obligates the foreground stream for every
// session on the connection, and a later key-less `initialize` would cancel
// that obligation while the sessions it was published for are still live, so a
// host reducing the stream would see it stop mid-turn with no terminal event.
// Introducing an answer the connection never gave is refused for the mirror
// reason: the sessions already open on it published no opening snapshot, so
// their streams could never be completed. Repeating the identical negotiation
// asserts the same contract and changes nothing, so it is admitted.
//
// The refusal is reached before `Initialize` records any client capability, so
// a refused re-negotiation leaves the connection — and every live session's
// obligated stream — exactly as the first answer left it.
func (a *Agent) retainNegotiatedLifecycle(answer lifecycle.Negotiated) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.lifecycleAnswered && !sameNegotiatedLifecycle(a.lifecycleAnswer, answer) {
		return acp.NewInvalidParams(map[string]any{
			jsonFieldError: valUnsupported,
			jsonFieldField: lifecycle.MetaPath,
		})
	}

	a.lifecycleAnswer = answer
	a.lifecycleAnswered = true

	return nil
}

// sameNegotiatedLifecycle compares two answers member by member. Negotiated
// carries a slice, so it is not comparable with ==, and every member is part of
// the connection's exact answer.
func sameNegotiatedLifecycle(current, next lifecycle.Negotiated) bool {
	return current.Version == next.Version &&
		current.UpdatesOutsidePrompt == next.UpdatesOutsidePrompt &&
		current.AuthoritativeQuiescence == next.AuthoritativeQuiescence &&
		current.QuiescenceSource == next.QuiescenceSource &&
		slices.Equal(current.ActivityKinds, next.ActivityKinds)
}

// negotiatedLifecycle reports the answer this connection is bound by.
func (a *Agent) negotiatedLifecycle() lifecycle.Negotiated {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.lifecycleAnswer
}

// lifecycleParamError renders a refused negotiation or correlation value. The
// lifecycle key is the one family literal this adapter validates on the request
// itself, so the rejection names the exact member path that failed.
func lifecycleParamError(refusal *lifecycle.ParamError) error {
	return acp.NewInvalidParams(map[string]any{
		jsonFieldError: string(refusal.Verdict),
		jsonFieldField: refusal.Field,
	})
}

// rejectLifecycleMeta refuses the lifecycle key on a surface that never carries
// it. A family literal is never foreign and never a no-op: every inbound surface
// outside `initialize`, `session/prompt`, and `session/cancel` rejects it rather
// than ignoring it as another namespace's business, and it does so before the
// surface's own work.
//
// On a surface that also carries the route envelope, the route is validated
// first: the authenticator precedes the placement rule, so a request that is
// both unroutable and misplaced reports the route verdict rather than leaving
// the choice of two to an implementation.
func rejectLifecycleMeta(meta map[string]any) error {
	if _, present := meta[lifecycle.MetaKey]; !present {
		return nil
	}

	return acp.NewInvalidParams(map[string]any{
		jsonFieldError: valUnsupported,
		jsonFieldField: lifecycle.MetaPath,
	})
}

// rejectLifecycleRawMeta refuses the lifecycle key on an extension route whose
// params are still raw JSON. It reads only the reserved member: a route that
// decodes its own params later still refuses the family literal first.
func rejectLifecycleRawMeta(params json.RawMessage) error {
	var envelope struct {
		Meta map[string]json.RawMessage `json:"_meta"`
	}

	if err := json.Unmarshal(params, &envelope); err != nil {
		return nil //nolint:nilerr // The route reports malformed params against its own shape.
	}

	if _, present := envelope.Meta[lifecycle.MetaKey]; !present {
		return nil
	}

	return acp.NewInvalidParams(map[string]any{
		jsonFieldError: valUnsupported,
		jsonFieldField: lifecycle.MetaPath,
	})
}
