//go:build integration

package hermes

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"
)

// measuredHermesVersion names the release every literal in this file was read
// off. It is provenance, not a floor: MinimumVersion already answers what this
// adapter supports.
const measuredHermesVersion = "0.21.1"

// measuredUnknownProviderRefusal is what a real hermes answers config.set when
// the selection names a provider it does not have.
const measuredUnknownProviderRefusal = "Unknown provider 'missing-provider'. " +
	"Check 'hermes model' for available providers, or define it in config.yaml under 'providers:'."

// TestLiveModelSelectionNativeAnswers measures what Hermes itself answers a
// model mutation: an unknown provider is refused with JSON-RPC 5001 and the
// message pinned above, while a model absent from a real provider's published
// catalogue is accepted when Hermes cannot reach that provider's live model
// listing. Hermes decides the model verdict, not the adapter, so the adapter
// forwards the exact value and publishes whatever Hermes answers. The version
// is asserted so a Hermes bump fails here with both versions named.
//
// No credential and no model turn is involved: config.set is a gateway call
// against a disposable home, and the placeholder key only satisfies the
// gateway's precondition that some provider be configured. That key also keeps
// the live listing unreachable, which is the acceptance path this test pins.
func TestLiveModelSelectionNativeAnswers(t *testing.T) {
	executable := integrationHermesCLI(t)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	requireMeasuredHermesVersion(t, executable)

	scratch := durableTempDir(t)
	home, err := os.MkdirTemp(scratch, "acp-go-hermes-runtime-")
	if err != nil {
		t.Fatalf("create generation root: %v", err)
	}
	opts := ProcessOptions{
		ExecutablePath: executable,
		Home:           home,
		Cwd:            durableTempDir(t),
		ScratchParent:  scratch,
		Timeout:        120 * time.Second,
		Env: map[string]string{
			"NO_COLOR": "1",
			"PATH":     os.Getenv("PATH"),
			// The gateway refuses model methods until some inference provider is
			// configured. A placeholder satisfies that precondition; nothing here
			// authenticates, submits a turn, or reads the operator's Hermes home.
			"OPENAI_API_KEY": "acp-go-hermes-smoke-placeholder-not-a-credential",
		},
	}
	proc, err := Start(ctx, opts)
	if err != nil {
		t.Fatalf("start hermes serve: %v", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = proc.Close(closeCtx)
	}()

	created, err := proc.Client.CreateSession(ctx, map[string]any{
		jsonFieldCwd: durableTempDir(t), keyTitle: "acp-go-hermes model selection measurement",
	})
	if err != nil {
		t.Fatalf("session.create: %v", err)
	}
	defer func() {
		_ = proc.Client.CloseSession(ctx, created.SessionID)
		_ = proc.Client.DeleteSession(ctx, created.StoredSessionID)
	}()

	err = proc.Client.SetModel(ctx, created.SessionID, "missing-provider/missing-model")

	var rpcErr *RPCError
	if !errors.As(err, &rpcErr) {
		t.Fatalf("unknown provider answer = %v, want an RPCError", err)
	}
	if rpcErr.Code != 5001 || rpcErr.Message != measuredUnknownProviderRefusal {
		t.Fatalf("unknown provider refusal = %d %q, want %d %q\n"+
			"hermes %s answers this differently: re-measure and update every site this test's doc comment names",
			rpcErr.Code, rpcErr.Message, 5001, measuredUnknownProviderRefusal, measuredHermesVersion)
	}

	options, err := proc.Client.ModelOptions(ctx, created.SessionID)
	if err != nil {
		t.Fatalf("model.options: %v", err)
	}

	// An inference provider, not a preset registry: `moa` publishes exactly one
	// `default` entry and resolves names against its own preset list, so it
	// answers a made-up name the way a listing lookup does rather than the way a
	// provider does.
	index := slices.IndexFunc(options.Providers, func(provider Provider) bool { return len(provider.Models) > 1 })
	if index < 0 {
		t.Fatalf("no advertised provider publishes more than one model: %#v", options.Providers)
	}
	provider := options.Providers[index]

	unadvertised := "acp-go-hermes-unadvertised-probe-model"
	if slices.Contains(provider.Models, unadvertised) {
		t.Fatalf("provider %q advertises the probe model: %#v", provider.Slug, provider.Models)
	}
	if err := proc.Client.SetModel(ctx, created.SessionID, provider.Slug+"/"+unadvertised); err != nil {
		t.Fatalf("provider %q refused an unadvertised model: %v\n"+
			"hermes %s accepted it while the provider's live listing was unreachable",
			provider.Slug, err, measuredHermesVersion)
	}
}

func requireMeasuredHermesVersion(t *testing.T, executable string) {
	t.Helper()

	output, err := exec.Command(executable, "--version").CombinedOutput()
	if err != nil {
		t.Fatalf("hermes --version: %v\n%s", err, output)
	}

	version, ok := parseVersion(string(output))
	if !ok {
		t.Fatalf("hermes --version unparsed: %q", strings.TrimSpace(string(output)))
	}
	if version != measuredHermesVersion {
		t.Fatalf("local hermes is %s, every literal in this file was measured against %s: "+
			"re-measure this lane and carry the new answers to the sites this file's doc comments name",
			version, measuredHermesVersion)
	}
}
