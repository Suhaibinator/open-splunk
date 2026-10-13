package genai

import "github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"

// DefaultModelResolver defines structured state used in the GenAI model resolution and runtime construction flow.
//
// Used in flow:
// - DefaultModelResolver is read or populated by backend components in this package to move requests through this stage.
type DefaultModelResolver struct{}

// ResolveModel accepts any normalized model definition and performs no runtime
// provisioning checks. Callers that need provider/runtime validation should
// override the catalog with ModelRuntimeFactory via service wiring.

// ResolveModel performs its package-specific work in the GenAI model resolution and runtime construction flow.
func (r *DefaultModelResolver) ResolveModel(model *common_service_models.InferenceModel) (*ResolvedModel, error) {
	return &ResolvedModel{}, nil
}
