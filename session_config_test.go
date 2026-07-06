package hermesacp

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/coder/acp-go-sdk"
)

func TestModelConfigOptionMetadataMapping(t *testing.T) {
	providers := providersResponse{Providers: []providerInfo{{
		ID:   "openai",
		Name: "OpenAI",
		Models: map[string]providerModel{
			"gpt-test": {
				ID:   "gpt-test",
				Name: "GPT Test",
				Limit: map[string]any{
					"context": float64(1000),
					"output":  float64(200),
				},
				Reasoning:  true,
				ToolCall:   true,
				Modalities: providerModelModalities{Input: []string{"image", "pdf"}},
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
	if got := meta["capabilities"]; !containsStringAny(got, "tools") || !containsStringAny(got, "reasoning") ||
		!containsStringAny(got, "image") || !containsStringAny(got, "pdf") {
		t.Fatalf("capabilities meta = %#v", got)
	}
	if got := meta["supportedEffortLevels"]; !containsStringAny(got, "low") || !containsStringAny(got, "medium") {
		t.Fatalf("effort meta = %#v", got)
	}
}

func TestSessionConfigBranchesAndValidation(t *testing.T) {
	ctx := context.Background()
	client := newFakeHermesClient()
	client.providers = providersResponse{Providers: []providerInfo{
		{ID: "", Models: map[string]providerModel{"skip": {}}},
		{ID: "p", Models: map[string]providerModel{
			"m": {
				Limit:      map[string]any{"context": int(42), "output": json.Number("7")},
				Modalities: providerModelModalities{Input: []string{"audio", "video"}},
				Options:    map[string]any{"reasoningEffort": []any{"medium"}},
			},
		}},
	}}
	agent := NewAgent()
	conn := newRecordingAgentClient()
	agent.setAgentClient(conn)
	sess := testSession(agent, client)
	agent.mu.Lock()
	agent.sessions[sess.id] = sess
	agent.mu.Unlock()

	if options := (&session{agent: agent}).configOptions(ctx); options != nil {
		t.Fatalf("nil client config options = %#v", options)
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

	fallback := modelConfigOption(sessionSnapshot{providerID: "p", modelID: "m"}, providersResponse{})
	if fallback.Select == nil || fallback.Select.Options.Ungrouped == nil || fallback.Select.CurrentValue != "p/m" {
		t.Fatalf("fallback model option = %#v", fallback)
	}
	if empty := modelConfigOption(sessionSnapshot{}, providersResponse{}); empty.Select != nil {
		t.Fatalf("empty model option = %#v", empty)
	}
	efforts := supportedEfforts(client.providers.Providers[1].Models["m"])
	if len(efforts) != 1 || efforts[0] != "medium" {
		t.Fatalf("supportedEfforts = %#v", efforts)
	}
	efforts = supportedEfforts(providerModel{Options: map[string]any{
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

func TestHasConfigValueUngroupedAndMissing(t *testing.T) {
	ctx := context.Background()
	client := newFakeHermesClient()
	// Empty providers with a current model yields an ungrouped fallback option.
	agent := NewAgent()
	sess := testSession(agent, client)
	sess.providerID = "openai"
	sess.modelID = "gpt-test"

	if !sess.hasConfigValue(ctx, configModel, "openai/gpt-test") {
		t.Fatal("current ungrouped model value not found")
	}
	if sess.hasConfigValue(ctx, configModel, "openai/other") {
		t.Fatal("absent ungrouped model value reported present")
	}
	if sess.hasConfigValue(ctx, acp.SessionConfigId("mode"), "anything") {
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
