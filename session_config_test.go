package hermesacp

import (
	"context"
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
			"gpt-test": {ID: "gpt-test", Name: "GPT Test"},
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
	// The model catalogue answers an id and a name and nothing else, so the
	// published metadata is exactly the qualified model id.
	meta, metaOK := value.Meta[hermesMetaKey].(map[string]any)
	if !metaOK || len(meta) != 1 || meta["modelId"] != "openai/gpt-test" {
		t.Fatalf("model metadata schema = %#v", value.Meta)
	}
}

func TestModelSelectionQualifiesProviderExactlyOnce(t *testing.T) {
	for _, test := range []struct {
		provider string
		model    string
		want     string
	}{
		{provider: "xai-oauth", model: "grok-4.5", want: "xai-oauth/grok-4.5"},
		{provider: "openrouter", model: "x-ai/grok-4.5", want: "openrouter/x-ai/grok-4.5"},
		{provider: "nous", model: "nous/x-ai/grok-4.5", want: "nous/x-ai/grok-4.5"},
	} {
		if got := modelSelectionValue(test.provider, test.model); got != test.want {
			t.Fatalf("modelSelectionValue(%q, %q) = %q, want %q", test.provider, test.model, got, test.want)
		}
	}

	native := testNativeSession("native-qualified")
	native.Model.ProviderID = "xai-oauth"
	native.Model.ModelID = "xai-oauth/grok-4.5"
	sess := newSession(newTestAgent(), "qualified", t.TempDir(), nil, nil, native, newFakeHermesClient(), sessionMeta{}, idmapRecord{})
	if got := sess.currentModel(); got != "xai-oauth/grok-4.5" {
		t.Fatalf("current model duplicated provider: %q", got)
	}
	meta := sessionResponseMeta(sess.snapshot())
	hermesMeta, ok := meta[hermesMetaKey].(map[string]any)
	if !ok {
		t.Fatalf("Hermes meta type = %T", meta[hermesMetaKey])
	}
	if hermesMeta["modelId"] != "xai-oauth/grok-4.5" || hermesMeta["model"] != "xai-oauth/grok-4.5" {
		t.Fatalf("qualified response meta = %#v", hermesMeta)
	}
}

func TestSessionConfigBranchesAndValidation(t *testing.T) {
	ctx := context.Background()
	client := newFakeHermesClient()
	client.providers = nativehermes.ProvidersResponse{Providers: []nativehermes.ProviderInfo{
		{ID: "", Models: map[string]nativehermes.ProviderModel{"skip": {}}},
		{ID: "p", Models: map[string]nativehermes.ProviderModel{"m": {}}},
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
	// A request that carries no variant at all is missing "value", the field the
	// value-id variant is recognized by — not "type", which names the boolean
	// variant this agent refuses outright.
	_, noVariantErr := agent.SetSessionConfigOption(ctx, acp.SetSessionConfigOptionRequest{})
	requireUnsupportedField(t, noVariantErr, keyValue, "missing value")
	if _, err := agent.SetSessionConfigOption(ctx, SetConfigOptionRequest("missing", configModel, "p/m")); err == nil {
		t.Fatal("unknown session config accepted")
	}
	if _, err := agent.SetSessionConfigOption(ctx, SetConfigOptionRequest(sess.id, configModel, "")); err == nil {
		t.Fatal("empty config value accepted")
	}
	if _, err := agent.SetSessionConfigOption(ctx, SetConfigOptionRequest(sess.id, "unknown", "x")); err == nil {
		t.Fatal("unknown config id accepted")
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

// TestSetSessionConfigOptionReadsModelOptionsOnce pins the cost of publishing
// the post-mutation menu at exactly one native enumeration.
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
}

func TestUnknownModelValueTraversesEstablishmentMutationAndPrompt(t *testing.T) {
	ctx := t.Context()
	client := newFakeHermesClient()
	client.createSession = testNativeSession("native-unknown-model")
	client.providers = nativehermes.ProvidersResponse{Providers: []nativehermes.ProviderInfo{{
		ID: "provider", Models: map[string]nativehermes.ProviderModel{"menu-model": {ID: "menu-model"}},
	}}}
	var promptModel *nativehermes.ModelSelector
	client.sendMessage = func(_ context.Context, id string, req nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		promptModel = req.Model

		return nativehermes.NativeMessage{Info: nativehermes.NativeMessageInfo{
			ID: "assistant-unknown-model", SessionID: id, Role: "assistant", Finish: "stop",
		}}, nil
	}

	var establishmentModel string
	agent := newTestAgent(WithScratchDir(t.TempDir()), func(options *Options) {
		options.clientFactory = func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
			establishmentModel = start.DefaultModel
			var err error
			client.xdg, err = testGenerationXDG(start.ScratchParent)

			return client, err
		}
	})
	cwd := t.TempDir()
	establishmentValue := "provider/unlisted-at-establishment"
	created, err := agent.NewSession(ctx, NewSessionRequest(cwd, WithSessionHermesOptions(HermesOptions{
		Model: establishmentValue,
	})))
	if err != nil {
		t.Fatalf("establish unknown model: %v", err)
	}
	if establishmentModel != establishmentValue || len(created.ConfigOptions) != 1 ||
		created.ConfigOptions[0].Select.CurrentValue != acp.SessionConfigValueId(establishmentValue) {
		t.Fatalf("establishment model=%q options=%#v", establishmentModel, created.ConfigOptions)
	}

	mutationValue := "provider/unlisted-at-mutation"
	selected, err := agent.SetSessionConfigOption(ctx, SetModelRequest(created.SessionId, mutationValue))
	if err != nil {
		t.Fatalf("mutate unknown model: %v", err)
	}
	client.mu.Lock()
	setCalls := append([]fakeModelSelection(nil), client.setModelCalls...)
	client.mu.Unlock()
	if len(setCalls) != 1 || setCalls[0] != (fakeModelSelection{sessionID: "native-unknown-model", value: mutationValue}) {
		t.Fatalf("native model selections = %#v", setCalls)
	}
	if len(selected.ConfigOptions) != 1 || selected.ConfigOptions[0].Select.CurrentValue != acp.SessionConfigValueId(mutationValue) {
		t.Fatalf("selected options = %#v", selected.ConfigOptions)
	}

	if _, promptErr := agent.Prompt(ctx, TextPromptRequest(created.SessionId, "unknown-model-prompt", "continue")); promptErr != nil {
		t.Fatalf("prompt unknown model: %v", promptErr)
	}
	if promptModel == nil || promptModel.ProviderID != "provider" || promptModel.ModelID != "unlisted-at-mutation" {
		t.Fatalf("prompt model = %#v", promptModel)
	}
}

func TestUnknownModelValueTraversesActiveResume(t *testing.T) {
	client := newFakeHermesClient()
	client.providers = nativehermes.ProvidersResponse{Providers: []nativehermes.ProviderInfo{{
		ID: "provider", Models: map[string]nativehermes.ProviderModel{"menu-model": {ID: "menu-model"}},
	}}}
	var promptModel *nativehermes.ModelSelector
	client.sendMessage = func(_ context.Context, id string, req nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		promptModel = req.Model

		return nativehermes.NativeMessage{Info: nativehermes.NativeMessageInfo{
			ID: "assistant-resumed-model", SessionID: id, Role: "assistant", Finish: "stop",
		}}, nil
	}
	agent := newTestAgent()
	session := testSession(agent, client)
	agent.sessions[session.id] = session

	value := "provider/unlisted-at-active-resume"
	response, err := agent.ResumeSession(t.Context(), ResumeSessionRequest(session.id, session.cwd, WithSessionHermesOptions(HermesOptions{
		Model: value,
	})))
	if err != nil {
		t.Fatalf("resume unknown model: %v", err)
	}
	if len(response.ConfigOptions) != 1 || response.ConfigOptions[0].Select.CurrentValue != acp.SessionConfigValueId(value) {
		t.Fatalf("resumed options = %#v", response.ConfigOptions)
	}
	if _, promptErr := agent.Prompt(t.Context(), TextPromptRequest(session.id, "active-resumed-model-prompt", "continue")); promptErr != nil {
		t.Fatalf("prompt resumed unknown model: %v", promptErr)
	}
	if promptModel == nil || promptModel.ProviderID != "provider" || promptModel.ModelID != "unlisted-at-active-resume" {
		t.Fatalf("resumed prompt model = %#v", promptModel)
	}
}

func TestUnknownModelNativeRefusalPropagates(t *testing.T) {
	client := newFakeHermesClient()
	wantErr := errors.New("hermes json-rpc 5001: Unknown provider 'missing-provider'")
	client.setModelErr = wantErr
	agent := newTestAgent()
	session := testSession(agent, client)
	agent.sessions[session.id] = session

	value := "missing-provider/missing-model"
	if _, err := agent.SetSessionConfigOption(t.Context(), SetModelRequest(session.id, value)); !errors.Is(err, wantErr) {
		t.Fatalf("native unknown-model error = %v", err)
	}
	client.mu.Lock()
	setCalls := append([]fakeModelSelection(nil), client.setModelCalls...)
	client.mu.Unlock()
	if len(setCalls) != 1 || setCalls[0].value != value {
		t.Fatalf("native model selections = %#v", setCalls)
	}
	if reads := client.configProviderCallCount(); reads != 0 {
		t.Fatalf("model.options reads after native refusal = %d, want 0", reads)
	}
}

func TestModelSelectionSurvivesCatalogueReadFailure(t *testing.T) {
	client := newFakeHermesClient()
	client.providersErr = errors.New("model.options unavailable")
	agent := newTestAgent()
	session := testSession(agent, client)
	agent.sessions[session.id] = session

	value := "provider/unlisted-without-menu"
	response, err := agent.SetSessionConfigOption(t.Context(), SetModelRequest(session.id, value))
	if err != nil {
		t.Fatalf("set model while menu unavailable: %v", err)
	}
	if response.ConfigOptions != nil {
		t.Fatalf("unavailable menu = %#v", response.ConfigOptions)
	}
	if session.currentModel() != value {
		t.Fatalf("current model = %q, want %q", session.currentModel(), value)
	}
	client.mu.Lock()
	setCalls := append([]fakeModelSelection(nil), client.setModelCalls...)
	client.mu.Unlock()
	if len(setCalls) != 1 || setCalls[0].value != value {
		t.Fatalf("native model selections = %#v", setCalls)
	}
	if reads := client.configProviderCallCount(); reads != 1 {
		t.Fatalf("model.options reads = %d, want 1", reads)
	}
}

func TestSetSessionConfigNativeSetterFailure(t *testing.T) {
	client := newFakeHermesClient()
	client.providers = nativehermes.ProvidersResponse{Providers: []nativehermes.ProviderInfo{{
		ID: "provider", Models: map[string]nativehermes.ProviderModel{"model": {ID: "model"}},
	}}}
	wantErr := errors.New("set model")
	agent := newTestAgent()
	session := newSession(agent, "session-1", "/tmp/project", nil, nil, testNativeSession("native-1"), modelSetterTestServer{Server: client, err: wantErr}, sessionMeta{}, idmapRecord{
		SessionID: "session-1", NativeSessionID: "native-1", Format: SessionStoreFormat,
	})
	agent.sessions[session.id] = session
	if _, err := agent.SetSessionConfigOption(t.Context(), SetConfigOptionRequest(session.id, configModel, "provider/model")); !errors.Is(err, wantErr) {
		t.Fatalf("set model error=%v", err)
	}
}
