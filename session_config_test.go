package hermesacp

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strconv"
	"strings"
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
	option := modelConfigOption(sessionSnapshot{}, providers, nil)
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
	// published metadata is the qualified model id and the effort levels every
	// Hermes model takes.
	meta, metaOK := value.Meta[hermesMetaKey].(map[string]any)
	if !metaOK || len(meta) != 2 || meta["modelId"] != "openai/gpt-test" {
		t.Fatalf("model metadata schema = %#v", value.Meta)
	}
	if levels, _ := meta["supportedEffortLevels"].([]string); !slices.Equal(levels, hermesEffortLevels) {
		t.Fatalf("model effort levels = %#v", meta["supportedEffortLevels"])
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
	sess := newSession(newTestAgent(), "qualified", durableTempDir(t), nil, nil, native, newFakeHermesClient(), sessionMeta{}, idmapRecord{})
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
	sess := testSession(t, agent, client)
	agent.mu.Lock()
	agent.sessions[sess.id] = sess
	agent.mu.Unlock()

	if options := (&session{agent: agent}).configOptions(ctx); options != nil {
		t.Fatalf("nil client config options = %#v", options)
	}
	// A harness that enumerated nothing, for a session that has selected
	// nothing, leaves the config surface with nothing to publish rather than an
	// empty select a host would render as a broken picker.
	unselected := newSession(agent, "unselected", absTestPath("tmp", "project"), nil, nil, nativehermes.Session{ID: "native-unselected"}, newFakeHermesClient(), sessionMeta{}, idmapRecord{})
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

	fallback := modelConfigOption(sessionSnapshot{providerID: "p", modelID: "m"}, nativehermes.ProvidersResponse{}, nil)
	if fallback.Select == nil || fallback.Select.Options.Ungrouped == nil || fallback.Select.CurrentValue != "p/m" {
		t.Fatalf("fallback model option = %#v", fallback)
	}
	if empty := modelConfigOption(sessionSnapshot{}, nativehermes.ProvidersResponse{}, nil); empty.Select != nil {
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

	sess := testSession(t, agent, client)
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
	agent := newTestAgent(WithScratchDir(durableTempDir(t)), func(options *Options) {
		options.clientFactory = func(_ context.Context, start nativehermes.StartOptions) (nativehermes.Server, error) {
			establishmentModel = start.DefaultModel
			var err error
			client.xdg, err = testGenerationXDG(start.ScratchParent)

			return client, err
		}
	})
	cwd := durableTempDir(t)
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
	session := testSession(t, agent, client)
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

// TestMalformedModelValueRefusedByBothDoors pins the only refusal model
// selection still makes locally, and pins that both doors make it.
//
// The question is the value's shape and never which models exist: a value that
// is not provider-qualified cannot become a provider and a model, so no door
// can carry one. The active-resume door has to answer it itself — nothing that
// door accepts reaches Hermes, so a value it degraded would leave the next
// prompt running on the previously bound model while the config option echoed
// back the value the host asked for. The config door reads the same predicate
// before its native call, so the two answer alike and neither spends a gateway
// round trip on a string Hermes could not parse either.
func TestMalformedModelValueRefusedByBothDoors(t *testing.T) {
	client := newFakeHermesClient()
	client.sendMessage = func(_ context.Context, id string, req nativehermes.MessageRequest) (nativehermes.NativeMessage, error) {
		if req.Model == nil || req.Model.ProviderID != "provider" || req.Model.ModelID != "bound-model" {
			t.Errorf("prompt after refused selections carried model %#v", req.Model)
		}

		return nativehermes.NativeMessage{Info: nativehermes.NativeMessageInfo{
			ID: "assistant-bound-model", SessionID: id, Role: "assistant", Finish: "stop",
		}}, nil
	}
	agent := newTestAgent()
	session := testSession(t, agent, client)
	session.providerID, session.modelID = "provider", "bound-model"
	agent.sessions[session.id] = session

	for _, malformed := range []string{
		"bare-model", "provider/", "/model", "provider/two words", "provider/--flag", "--flag/model",
	} {
		_, err := agent.ResumeSession(t.Context(), ResumeSessionRequest(session.id, session.cwd,
			WithSessionHermesOptions(HermesOptions{Model: malformed})))
		requireUnsupportedField(t, err, hermesModelOptionPath, "active resume "+strconv.Quote(malformed))

		_, err = agent.SetSessionConfigOption(t.Context(), SetModelRequest(session.id, malformed))
		requireUnsupportedField(t, err, keyValue, "config door "+strconv.Quote(malformed))
	}

	// Neither door moved the session, and neither spent a native mutation on a
	// value it had already refused.
	if session.currentModel() != "provider/bound-model" {
		t.Fatalf("current model after refused selections = %q", session.currentModel())
	}
	client.mu.Lock()
	setCalls := append([]fakeModelSelection(nil), client.setModelCalls...)
	client.mu.Unlock()
	if len(setCalls) != 0 {
		t.Fatalf("native model selections after refusals = %#v", setCalls)
	}

	// The bound model is what the next prompt still names, checked inside the
	// native send above.
	if _, err := agent.Prompt(t.Context(), TextPromptRequest(session.id, "bound-model-prompt", "continue")); err != nil {
		t.Fatalf("prompt after refused selections: %v", err)
	}
}

func TestUnknownModelNativeRefusalIsSanitized(t *testing.T) {
	client := newFakeHermesClient()
	wantErr := &nativehermes.RPCError{
		Code:    5001,
		Message: "Unknown provider 'missing-provider'. Check 'hermes model' for available providers, or define it in config.yaml under 'providers:'.",
	}
	client.setModelErr = wantErr
	agent := newTestAgent()
	session := testSession(t, agent, client)
	agent.sessions[session.id] = session

	value := "missing-provider/missing-model"
	_, err := agent.SetSessionConfigOption(t.Context(), SetModelRequest(session.id, value))
	if !errors.Is(err, wantErr) {
		t.Fatalf("native refusal cause = %v, want %v", err, wantErr)
	}
	var requestErr *acp.RequestError
	if !errors.As(err, &requestErr) || requestErr.Code != -32602 {
		t.Fatalf("model refusal = %#v, want invalid params", err)
	}
	wantData := map[string]any{
		jsonFieldError: valHermesModelSelectionRefused,
		jsonFieldField: keyValue,
	}
	if !reflect.DeepEqual(requestErr.Data, wantData) {
		t.Fatalf("model refusal data = %#v, want %#v", requestErr.Data, wantData)
	}
	if strings.Contains(err.Error(), wantErr.Message) {
		t.Fatalf("model refusal leaked native text: %v", err)
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
	session := testSession(t, agent, client)
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
	session := newSession(agent, "session-1", absTestPath("tmp", "project"), nil, nil, testNativeSession("native-1"), modelSetterTestServer{Server: client, err: wantErr}, sessionMeta{}, idmapRecord{
		SessionID: "session-1", NativeSessionID: "native-1", Format: SessionStoreFormat,
	})
	agent.sessions[session.id] = session
	if _, err := agent.SetSessionConfigOption(t.Context(), SetConfigOptionRequest(session.id, configModel, "provider/model")); !errors.Is(err, wantErr) {
		t.Fatalf("set model error=%v", err)
	}
}
