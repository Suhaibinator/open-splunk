package common_persistence_models

import (
	"fmt"

	"github.com/Suhaibinator/go-common/pkg/genai/inference"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// ExecutionModelSelection rejects partially populated pins instead of reverting to routing.
func (r *ChatAgentRun) ExecutionModelSelection() (*inference.ModelSelection, error) {
	if r.RequestedInferenceModelID == nil && r.RequestedInferenceAPI == nil && r.ExecutionInferenceModelID == nil && r.ExecutionInferenceAPI == nil && r.ExecutionProviderID == nil && r.ExecutionProviderModelID == nil {
		return nil, nil
	}
	if r.RequestedInferenceModelID == nil || r.ExecutionInferenceModelID == nil || r.ExecutionInferenceAPI == nil || r.ExecutionProviderID == nil || r.ExecutionProviderModelID == nil || r.TriggeringUserID == nil || *r.TriggeringUserID == 0 {
		return nil, fmt.Errorf("incomplete execution model pin on run %d", r.ID)
	}
	requested := &inference.ModelSelection{InferenceModelID: *r.RequestedInferenceModelID, API: r.RequestedInferenceAPI}
	if err := requested.Validate(); err != nil {
		return nil, err
	}
	selected := &inference.ModelSelection{InferenceModelID: *r.ExecutionInferenceModelID, API: r.ExecutionInferenceAPI}
	if err := selected.Validate(); err != nil {
		return nil, err
	}
	if *r.RequestedInferenceModelID != selected.InferenceModelID || *selected.API == 0 || *r.ExecutionProviderID == "" || *r.ExecutionProviderModelID == "" {
		return nil, fmt.Errorf("invalid execution model pin on run %d", r.ID)
	}
	return selected.Clone(), nil
}

// ValidateExecutionModel compares catalog identity and the effective adapter before execution.
func (r *ChatAgentRun) ValidateExecutionModel(id uint64, provider, model string, api proto_models.InferenceAPI) error {
	selected, err := r.ExecutionModelSelection()
	if err != nil || selected == nil {
		return err
	}
	if selected.InferenceModelID != id || *selected.API != api || *r.ExecutionProviderID != provider || *r.ExecutionProviderModelID != model {
		return fmt.Errorf("execution model identity changed for run %d", r.ID)
	}
	return nil
}
