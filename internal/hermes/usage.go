package hermes

import "encoding/json"

// Usage is the usage block session.usage and message.complete carry. The
// token counters are cumulative over the gateway agent's lifetime and restart
// at zero when Hermes builds a new agent. ContextUsed is the prompt tokens of
// the agent's last provider response, the figure Hermes compacts against; it
// is absent before a response reports usage, after a compaction or a
// response whose usage was all zero until the next response reports some, and
// while the context window is unknown.
//
//nolint:tagliatelle // Hermes uses context_used and context_max on the wire.
type Usage struct {
	Prompt      int64 `json:"prompt"`
	Completion  int64 `json:"completion"`
	Reasoning   int64 `json:"reasoning"`
	Total       int64 `json:"total"`
	ContextUsed int64 `json:"context_used"`
	ContextMax  int64 `json:"context_max"`
}

// DecodeUsage reads the usage member of a session.usage or message.complete
// payload. A payload without one reports false.
func DecodeUsage(payload json.RawMessage) (Usage, bool) {
	var envelope struct {
		Usage *Usage `json:"usage"`
	}
	if json.Unmarshal(payload, &envelope) != nil || envelope.Usage == nil {
		return Usage{}, false
	}

	return *envelope.Usage, true
}

// Since is the consumption between an earlier reading and this one. If any
// counter decreased, the agent restarted and every counter belongs to its
// new lifetime.
func (u Usage) Since(earlier Usage) Usage {
	if u.Prompt < earlier.Prompt || u.Completion < earlier.Completion || u.Reasoning < earlier.Reasoning || u.Total < earlier.Total {
		earlier = Usage{}
	}

	return Usage{
		Prompt:     u.Prompt - earlier.Prompt,
		Completion: u.Completion - earlier.Completion,
		Reasoning:  u.Reasoning - earlier.Reasoning,
		Total:      u.Total - earlier.Total,
	}
}

// Add sums the token counters of two consumptions.
func (u Usage) Add(other Usage) Usage {
	return Usage{
		Prompt:     u.Prompt + other.Prompt,
		Completion: u.Completion + other.Completion,
		Reasoning:  u.Reasoning + other.Reasoning,
		Total:      u.Total + other.Total,
	}
}
