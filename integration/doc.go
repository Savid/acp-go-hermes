// Package integration holds the tests that run against an installed Hermes.
//
// The tests are behind the integration build tag and ACP_GO_HERMES_RUN_INTEGRATION=1.
// The smoke tier spends no model tokens; ACP_GO_HERMES_RUN_LIVE_TOKENS=1 enables
// prompts that do.
//
// ACP_GO_HERMES_HARNESS_PATH selects the harness binary, which otherwise comes
// from PATH; an absent binary skips.
// ACP_GO_HERMES_HOME supplies native configuration and credentials copied into
// a temporary home. ACP_GO_HERMES_MODEL selects the model for live tests.
package integration
