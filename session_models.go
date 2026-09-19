package hermesacp

import (
	"context"
	"slices"
	"strings"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/wire"
)

const configModel acp.SessionConfigId = "model"
const configEffort acp.SessionConfigId = "effort"

func effortLevels() []string {
	return []string{"none", "minimal", "low", effortMedium, "high", "xhigh", "max", "ultra"}
}

func (s *session) refreshModels(ctx context.Context, rt *runtime) error {
	models, err := rt.client.ModelOptions(ctx, rt.liveID)
	if err != nil {
		return err
	}

	effort, err := rt.client.Reasoning(ctx, rt.liveID)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.models = models
	s.model = strings.Trim(models.Provider+"/"+models.Model, "/")
	s.effort = effort
	s.mu.Unlock()

	return nil
}

func (s *session) configOptions() []acp.SessionConfigOption {
	s.mu.Lock()
	defer s.mu.Unlock()

	rows := make([]wire.ModelRow, 0, len(s.models.Providers))
	for _, provider := range s.models.Providers {
		for _, model := range provider.Models {
			rows = append(rows, wire.ModelRow{ID: provider.Slug + "/" + model, Name: provider.Name + " / " + model})
		}
	}

	models := wire.ModelSelectOptions(vendor, s.model, rows, s.agent.options.ConfiguredModels)

	options := make([]acp.SessionConfigOption, 0, 2)
	if s.model != "" {
		options = append(options, acp.SessionConfigOption{Select: &acp.SessionConfigOptionSelect{Id: configModel, Name: "Model", Type: "select", Category: new(acp.SessionConfigOptionCategoryModel), CurrentValue: acp.SessionConfigValueId(s.model), Options: acp.SessionConfigSelectOptions{Ungrouped: &models}}})
	}

	efforts := make(acp.SessionConfigSelectOptionsUngrouped, 0, len(effortLevels()))
	for _, level := range effortLevels() {
		efforts = append(efforts, acp.SessionConfigSelectOption{Value: acp.SessionConfigValueId(level), Name: level})
	}

	if s.effort != "" {
		options = append(options, acp.SessionConfigOption{Select: &acp.SessionConfigOptionSelect{Id: configEffort, Name: "Effort", Type: "select", Category: new(acp.SessionConfigOptionCategoryThoughtLevel), CurrentValue: acp.SessionConfigValueId(s.effort), Options: acp.SessionConfigSelectOptions{Ungrouped: &efforts}}})
	}

	return options
}

func (s *session) setConfigOption(ctx context.Context, id acp.SessionConfigId, value string) ([]acp.SessionConfigOption, error) {
	if id != configModel && id != configEffort {
		return nil, wire.Unsupported("configId")
	}

	if id == configModel && validateOptionalModel(value) != nil || value == "" {
		return nil, wire.Unsupported(fieldValue)
	}

	if id == configEffort && !slices.Contains(effortLevels(), value) {
		return nil, wire.Unsupported(fieldValue)
	}

	if err := s.admissionError(); err != nil {
		return nil, err
	}

	release, err := s.acquireGate(limitSessionPrompt)
	if err != nil {
		return nil, err
	}
	defer release()

	s.mu.Lock()
	busy := s.cycle != nil
	s.mu.Unlock()

	if busy {
		return nil, wire.Backpressure(limitSessionPrompt)
	}

	rt, err := s.ensureRuntime(ctx)
	if err != nil {
		return nil, err
	}

	if id == configModel {
		err = rt.client.SetModel(ctx, rt.liveID, value)
	} else {
		_, err = rt.client.SetReasoning(ctx, rt.liveID, value)
	}

	if err != nil {
		return nil, wire.Unsupported(fieldValue)
	}

	if err := s.refreshModels(ctx, rt); err != nil {
		return nil, s.startFailure(ctx, err)
	}

	s.mu.Lock()
	s.options.Effort = s.effort
	s.mu.Unlock()

	if err := s.commitMirror(ctx, rt); err != nil {
		return nil, wire.InternalFailure(vendor, "")
	}

	options := s.configOptions()
	_ = s.emit(ctx, acp.SessionUpdate{ConfigOptionUpdate: &acp.SessionConfigOptionUpdate{ConfigOptions: options}})

	return options, nil
}
