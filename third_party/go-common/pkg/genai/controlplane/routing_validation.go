package controlplane

import (
	"context"
	"fmt"

	cpm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
)

func (registry *Registry) OrgModelChoice(operation Operation) (pb.InferenceOperationOrgModelChoice, bool) {
	registered, ok := registry.lookup(operation)
	return registered.config.OrgModelChoice, ok
}

// SessionRetarget returns the operation's application re-target gate.
func (registry *Registry) SessionRetarget(operation Operation) (pb.InferenceSessionRetarget, bool) {
	registered, ok := registry.lookup(operation)
	return registered.config.SessionRetarget, ok
}

func (registry *Registry) ValidateOperationModels(ctx context.Context, operation Operation, options []cpm.InferenceOperationModelOption, snapshot *cpm.InferenceRoutingSnapshot) error {
	gate, registered := registry.OrgModelChoice(operation)
	if !registered {
		return fmt.Errorf("operation %s is not registered", operation)
	}
	if snapshot == nil {
		return fmt.Errorf("routing snapshot is required")
	}
	var route *cpm.InferenceOperationRoute
	for position := range snapshot.Routes {
		if snapshot.Routes[position].Operation == int32(operation) {
			route = &snapshot.Routes[position]
			break
		}
	}
	if route == nil {
		return fmt.Errorf("operation %s has no default route", operation)
	}
	models := make(map[uint64]*cpm.InferenceModel, len(snapshot.Models))
	for position := range snapshot.Models {
		model := &snapshot.Models[position]
		models[model.ID] = model
	}
	seen := make(map[uint64]bool, len(options))
	for _, option := range options {
		if option.Operation != int32(operation) || option.InferenceModelID == 0 || seen[option.InferenceModelID] {
			return fmt.Errorf("invalid or duplicate operation model")
		}
		seen[option.InferenceModelID] = true
		if gate != pb.InferenceOperationOrgModelChoice_INFERENCE_OPERATION_ORG_MODEL_CHOICE_SITE_ADMIN_DECIDES && option.InferenceModelID != route.InferenceModelID {
			return fmt.Errorf("operation %s is locked by application", operation)
		}
		if option.PayerEligibility != pb.InferenceModelPayerEligibility_INFERENCE_MODEL_PAYER_ELIGIBILITY_PLATFORM_ONLY && option.PayerEligibility != pb.InferenceModelPayerEligibility_INFERENCE_MODEL_PAYER_ELIGIBILITY_PLATFORM_OR_CONNECTION && option.PayerEligibility != pb.InferenceModelPayerEligibility_INFERENCE_MODEL_PAYER_ELIGIBILITY_CONNECTION_ONLY {
			return fmt.Errorf("invalid payer eligibility")
		}
		if option.InferenceModelID == route.InferenceModelID && option.PayerEligibility == pb.InferenceModelPayerEligibility_INFERENCE_MODEL_PAYER_ELIGIBILITY_CONNECTION_ONLY {
			return fmt.Errorf("platform default must be platform payable")
		}
		model := models[option.InferenceModelID]
		if model == nil || !model.Active {
			return fmt.Errorf("model %d is unavailable", option.InferenceModelID)
		}
		if err := registry.ValidateModelRoute(ctx, model.ProviderID, model.ProviderModelID, route.Temperature, route.API); err != nil {
			return err
		}
	}
	if len(options) > 0 && !seen[route.InferenceModelID] {
		return fmt.Errorf("operation model list must include platform default")
	}
	return nil
}
