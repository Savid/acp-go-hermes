//nolint:tagliatelle // ACP wire member names keep their protocol spelling.
package hermesacp

import (
	"encoding/json"

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
		a.retainNegotiatedLifecycle(lifecycle.Negotiated{})

		//nolint:nilnil // An omitted capability produces no advertisement and no refusal.
		return nil, nil
	}

	answer := a.provenLifecycleFacts()
	answer.Version = lifecycle.Version

	a.retainNegotiatedLifecycle(answer)

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

func (a *Agent) retainNegotiatedLifecycle(answer lifecycle.Negotiated) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.lifecycleAnswer = answer
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
		jsonFieldError: valUnsupported,
		keyField:       refusal.Field,
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
		keyField:       lifecycle.MetaPath,
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
		keyField:       lifecycle.MetaPath,
	})
}
