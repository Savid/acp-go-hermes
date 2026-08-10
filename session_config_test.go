package hermesacp

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"

	"github.com/coder/acp-go-sdk"
)

func TestModelConfigOptionMetadataMapping(t *testing.T) {
	providers := nativehermes.ProvidersResponse{Providers: []nativehermes.ProviderInfo{{
		ID:   "openai",
		Name: "OpenAI",
		Models: map[string]nativehermes.ProviderModel{
			"gpt-test": {
				ID:   "gpt-test",
				Name: "GPT Test",
				Limit: map[string]any{
					"context": float64(1000),
					"output":  float64(200),
				},
				Reasoning:  true,
				ToolCall:   true,
				Modalities: nativehermes.ProviderModelModalities{Input: []string{"image", "pdf"}},
				Options: map[string]any{
					"reasoningEffort": map[string]any{"options": []any{"low", "medium"}},
				},
			},
		},
	}}}
	option := modelConfigOption(sessionSnapshot{}, providers)
	if option.Select == nil {
		t.Fatal("missing select option")
	}
	if option.Select.Id != configModel {
		t.Fatalf("id = %s", option.Select.Id)
	}
	if option.Select.Category == nil || *option.Select.Category != acp.SessionConfigOptionCategoryModel {
		t.Fatalf("category = %#v", option.Select.Category)
	}
	group := (*option.Select.Options.Grouped)[0]
	value := group.Options[0]
	if value.Value != "openai/gpt-test" {
		t.Fatalf("value = %s", value.Value)
	}
	meta, metaOK := value.Meta[hermesMetaKey].(map[string]any)
	if !metaOK || meta["contextWindow"] != 1000 || meta["maxOutputTokens"] != 200 {
		t.Fatalf("limits meta = %#v", value.Meta)
	}
	if got := meta["modelId"]; got != "openai/gpt-test" {
		t.Fatalf("modelId = %#v", got)
	}
	if _, exists := meta["capabilities"]; exists {
		t.Fatalf("capabilities metadata survived hard cutover: %#v", meta)
	}
	if got := meta["supportedEffortLevels"]; !containsStringAny(got, "low") || !containsStringAny(got, "medium") {
		t.Fatalf("effort meta = %#v", got)
	}
	if len(meta) != 4 {
		t.Fatalf("model metadata schema = %#v", meta)
	}
}

func TestSessionConfigBranchesAndValidation(t *testing.T) {
	ctx := context.Background()
	client := newFakeHermesClient()
	client.providers = nativehermes.ProvidersResponse{Providers: []nativehermes.ProviderInfo{
		{ID: "", Models: map[string]nativehermes.ProviderModel{"skip": {}}},
		{ID: "p", Models: map[string]nativehermes.ProviderModel{
			"m": {
				Limit:      map[string]any{"context": int(42), "output": json.Number("7")},
				Modalities: nativehermes.ProviderModelModalities{Input: []string{"audio", "video"}},
				Options:    map[string]any{"reasoningEffort": []any{"medium"}},
			},
		}},
	}}
	agent := newTestAgent()
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	sess := testSession(agent, client)
	agent.mu.Lock()
	agent.sessions[sess.id] = sess
	agent.mu.Unlock()

	if options := (&session{agent: agent}).configOptions(ctx); options != nil {
		t.Fatalf("nil client config options = %#v", options)
	}
	// A harness that enumerated nothing, for a session that has selected
	// nothing, leaves the config surface with nothing to publish rather than an
	// empty select a host would render as a broken picker.
	unselected := newSession(agent, "unselected", "/tmp/project", nil, nil, nativehermes.Session{ID: "native-unselected"}, newFakeHermesClient(), sessionMeta{}, idmapRecord{})
	if options := unselected.configOptions(ctx); options != nil {
		t.Fatalf("empty enumeration config options = %#v", options)
	}
	if _, err := agent.SetSessionConfigOption(ctx, acp.SetSessionConfigOptionRequest{}); err == nil {
		t.Fatal("missing value accepted")
	}
	if _, err := agent.SetSessionConfigOption(ctx, SetConfigOptionRequest("missing", configModel, "p/m")); err == nil {
		t.Fatal("unknown session config accepted")
	}
	if _, err := agent.SetSessionConfigOption(ctx, SetConfigOptionRequest(sess.id, configModel, "")); err == nil {
		t.Fatal("empty config value accepted")
	}
	if _, err := agent.SetSessionConfigOption(ctx, SetConfigOptionRequest(sess.id, "unknown", "x")); err == nil {
		t.Fatal("unknown config id accepted")
	}
	if _, err := agent.SetSessionConfigOption(ctx, SetConfigOptionRequest(sess.id, configModel, "missing/model")); err == nil {
		t.Fatal("unknown model accepted")
	}
	if _, err := agent.SetSessionConfigOption(ctx, SetConfigOptionRequest(sess.id, acp.SessionConfigId("mode"), "missing")); err == nil {
		t.Fatal("mode config option accepted")
	}
	if _, err := agent.SetSessionConfigOption(ctx, SetConfigOptionRequest(sess.id, configModel, "p/m")); err != nil {
		t.Fatalf("set model: %v", err)
	}
	if conn.updateCount() == 0 {
		t.Fatal("set model did not emit config update")
	}

	fallback := modelConfigOption(sessionSnapshot{providerID: "p", modelID: "m"}, nativehermes.ProvidersResponse{})
	if fallback.Select == nil || fallback.Select.Options.Ungrouped == nil || fallback.Select.CurrentValue != "p/m" {
		t.Fatalf("fallback model option = %#v", fallback)
	}
	if empty := modelConfigOption(sessionSnapshot{}, nativehermes.ProvidersResponse{}); empty.Select != nil {
		t.Fatalf("empty model option = %#v", empty)
	}
	efforts := supportedEfforts(client.providers.Providers[1].Models["m"])
	if len(efforts) != 1 || efforts[0] != "medium" {
		t.Fatalf("supportedEfforts = %#v", efforts)
	}
	efforts = supportedEfforts(nativehermes.ProviderModel{Options: map[string]any{
		"temperature":     []any{"ignored"},
		"reasoningEffort": []string{"low", "", "high"},
		"effortOptions":   map[string]any{"values": []any{"medium"}},
	}})
	if len(efforts) != 3 || efforts[0] != "high" || efforts[1] != "low" || efforts[2] != "medium" {
		t.Fatalf("normalized efforts = %#v", efforts)
	}
	if values := optionStringValues(map[string]any{"unknown": []any{"x"}}); values != nil {
		t.Fatalf("unknown option values = %#v", values)
	}
	if values := optionStringValues(42); values != nil {
		t.Fatalf("numeric option values = %#v", values)
	}
	if unstableConfigOptions(nil) != nil {
		t.Fatal("empty unstable config options returned non-nil")
	}
	if got := unstableConfigOptions([]acp.SessionConfigOption{{
		Select: &acp.SessionConfigOptionSelect{
			Type:         "select",
			Id:           "bad",
			Name:         "Bad",
			CurrentValue: "bad",
			Meta:         map[string]any{"bad": func() {}},
		},
	}}); len(got) != 0 {
		t.Fatalf("bad unstable config option was not skipped: %#v", got)
	}
}

// TestSetSessionConfigOptionReadsModelOptionsOnce pins the cost of one model
// selection at exactly one native enumeration. A host probes model support by
// selecting models under a short per-probe budget, and this call used to read
// the whole provider catalogue twice — once to validate the value and once to
// answer — for a mutation that happens entirely inside the wrapper.
func TestSetSessionConfigOptionReadsModelOptionsOnce(t *testing.T) {
	ctx := context.Background()
	client := newFakeHermesClient()
	client.providers = nativehermes.ProvidersResponse{Providers: []nativehermes.ProviderInfo{{
		ID:   "p",
		Name: "Provider",
		Models: map[string]nativehermes.ProviderModel{
			"one": {ID: "one", Name: "One"},
			"two": {ID: "two", Name: "Two"},
		},
	}}}

	agent := newTestAgent()
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)

	sess := testSession(agent, client)
	sess.providerID, sess.modelID = "p", "one"
	agent.mu.Lock()
	agent.sessions[sess.id] = sess
	agent.mu.Unlock()

	before := client.configProviderCallCount()

	response, err := agent.SetSessionConfigOption(ctx, SetConfigOptionRequest(sess.id, configModel, "p/two"))
	if err != nil {
		t.Fatalf("set model: %v", err)
	}

	if reads := client.configProviderCallCount() - before; reads != 1 {
		t.Fatalf("model.options reads = %d, want 1", reads)
	}

	if len(response.ConfigOptions) != 1 || response.ConfigOptions[0].Select.CurrentValue != "p/two" {
		t.Fatalf("response config options = %#v", response.ConfigOptions)
	}

	if sess.currentModel() != "p/two" {
		t.Fatalf("current model = %q, want p/two", sess.currentModel())
	}

	// A refused enumeration is still a rejected value rather than a silent
	// selection, and it too costs exactly one read.
	client.providersErr = errors.New("gateway refused")
	before = client.configProviderCallCount()

	if _, err := agent.SetSessionConfigOption(ctx, SetConfigOptionRequest(sess.id, configModel, "p/one")); err == nil {
		t.Fatal("a refused enumeration accepted a model selection")
	}

	if reads := client.configProviderCallCount() - before; reads != 1 {
		t.Fatalf("refused model.options reads = %d, want 1", reads)
	}

	if sess.currentModel() != "p/two" {
		t.Fatalf("a refused enumeration changed the model to %q", sess.currentModel())
	}
}

func TestHasConfigValueUngroupedAndMissing(t *testing.T) {
	ctx := context.Background()
	client := newFakeHermesClient()
	// Empty providers with a current model yields an ungrouped fallback option.
	agent := newTestAgent()
	sess := testSession(agent, client)
	sess.providerID = "openai"
	sess.modelID = "gpt-test"

	options := sess.configOptions(ctx)
	if !hasConfigValue(options, configModel, "openai/gpt-test") {
		t.Fatal("current ungrouped model value not found")
	}
	if hasConfigValue(options, configModel, "openai/other") {
		t.Fatal("absent ungrouped model value reported present")
	}
	if hasConfigValue(options, acp.SessionConfigId("mode"), "anything") {
		t.Fatal("non-model config id matched")
	}
}

func containsStringAny(value any, want string) bool {
	values, _ := value.([]string)
	for _, value := range values {
		if value == want {
			return true
		}
	}
	anyValues, _ := value.([]any)
	for _, value := range anyValues {
		if value == want {
			return true
		}
	}

	return false
}
