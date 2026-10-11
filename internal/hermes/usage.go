package hermes

import "encoding/json"

// Usage is the usage block session.usage and message.complete carry. The
// token counters are cumulative over the gateway agent's lifetime and restart
// at zero when Hermes builds a new agent. ContextUsed is the prompt tokens of
// the agent's last provider response, the figure Hermes compacts against; it
// is absent before a response reports usage, after a compaction or a
// response whose usage was all zero until the next response reports some, and
// while the context window is unknown. ContextMax is the window of Model, the
// model the agent runs.
//
//nolint:tagliatelle // Hermes uses context_used and context_max on the wire.
type Usage struct {
	Model       string `json:"model"`
	Prompt      int64  `json:"prompt"`
	Completion  int64  `json:"completion"`
	Reasoning   int64  `json:"reasoning"`
	Total       int64  `json:"total"`
	ContextUsed int64  `json:"context_used"`
	ContextMax  int64  `json:"context_max"`
	// Compressions counts the compactions the agent committed. An attempt
	// raises it before it commits and lowers it again when it aborts, so
	// only a reading taken after the turn settled proves an outcome.
	Compressions int64 `json:"compressions"`
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
	if u.Restarted(earlier) {
		earlier = Usage{}
	}

	return Usage{
		Prompt:     u.Prompt - earlier.Prompt,
		Completion: u.Completion - earlier.Completion,
		Reasoning:  u.Reasoning - earlier.Reasoning,
		Total:      u.Total - earlier.Total,
	}
}

// Restarted reports whether a token counter decreased since an earlier
// reading, as it does when Hermes builds a new agent.
func (u Usage) Restarted(earlier Usage) bool {
	return u.Prompt < earlier.Prompt || u.Completion < earlier.Completion || u.Reasoning < earlier.Reasoning || u.Total < earlier.Total
}

// CompactionsSince is the number of compactions the agent committed after an
// earlier settled reading; a restarted agent counts from zero.
func (u Usage) CompactionsSince(earlier Usage) int64 {
	if u.Restarted(earlier) {
		earlier = Usage{}
	}

	return max(u.Compressions-earlier.Compressions, 0)
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
