package common_sanitizer

import (
	"context"
	"fmt"
	"math"

	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// The inference routing sanitizers check request shape only: required fields, nonzero IDs,
// known enum values, and batch caps. Policy semantics (list caps, de-duplication, payer
// conflicts, session-action rules) are owned by the inference policy service.

func inferenceRoutingOrganizationID(organizationID uint64) error {
	if organizationID == 0 || organizationID > math.MaxInt64 {
		return ErrInvalidOrganizationID
	}
	return nil
}

func inferenceRoutingOperation(operation int32) error {
	if operation == 0 {
		return fmt.Errorf("operation must be nonzero: %w", ErrInvalidRequest)
	}
	return nil
}

func inferenceRoutingSessionAction(action proto_models.InferencePolicySessionAction, target *uint64) error {
	if _, found := proto_models.InferencePolicySessionAction_name[int32(action)]; !found {
		return fmt.Errorf("invalid session action: %w", ErrInvalidRequest)
	}
	if target != nil && *target == 0 {
		return fmt.Errorf("move target model must be nonzero: %w", ErrInvalidRequest)
	}
	return nil
}

func (h *Handler) SanitizeGetInferenceOperationModelsRequest(ctx context.Context, req *proto_models.GetInferenceOperationModelsRequest) (*proto_models.GetInferenceOperationModelsRequest, error) {
	const stage = "sanitize_get_inference_operation_models_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if err := inferenceRoutingOperation(req.Operation); err != nil {
		return nil, h.reject(ctx, stage, err, zap.Int32("operation", req.Operation))
	}
	return req, nil
}

func (h *Handler) SanitizeUpdateInferenceOperationModelsRequest(ctx context.Context, req *proto_models.UpdateInferenceOperationModelsRequest) (*proto_models.UpdateInferenceOperationModelsRequest, error) {
	const stage = "sanitize_update_inference_operation_models_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if err := inferenceRoutingOperation(req.Operation); err != nil {
		return nil, h.reject(ctx, stage, err, zap.Int32("operation", req.Operation))
	}
	if err := inferenceRoutingSessionAction(req.SessionAction, req.MoveToModelId); err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	for index, option := range req.Models {
		if option == nil || option.InferenceModelId == 0 {
			return nil, h.reject(ctx, stage, ErrInvalidRequest, zap.Int("model_index", index))
		}
		if _, valid := proto_models.InferenceModelPayerEligibility_name[int32(option.PayerEligibility)]; !valid || option.PayerEligibility == proto_models.InferenceModelPayerEligibility_INFERENCE_MODEL_PAYER_ELIGIBILITY_UNSPECIFIED {
			return nil, h.reject(ctx, stage, ErrInvalidRequest, zap.Int("model_index", index), zap.Int32("payer_eligibility", int32(option.PayerEligibility)))
		}
	}
	return req, nil
}

func (h *Handler) SanitizeGetOrganizationInferencePolicyRequest(ctx context.Context, req *proto_models.GetOrganizationInferencePolicyRequest) (*proto_models.GetOrganizationInferencePolicyRequest, error) {
	const stage = "sanitize_get_organization_inference_policy_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if err := inferenceRoutingOrganizationID(req.OrganizationId); err != nil {
		return nil, h.reject(ctx, stage, err, zap.Uint64("organization_id", req.OrganizationId))
	}
	return req, nil
}

func (h *Handler) SanitizeUpdateOrganizationInferencePolicyRequest(ctx context.Context, req *proto_models.UpdateOrganizationInferencePolicyRequest) (*proto_models.UpdateOrganizationInferencePolicyRequest, error) {
	const stage = "sanitize_update_organization_inference_policy_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if err := inferenceRoutingOrganizationID(req.OrganizationId); err != nil {
		return nil, h.reject(ctx, stage, err, zap.Uint64("organization_id", req.OrganizationId))
	}
	for index, providerID := range req.ProviderIds {
		if !config.ValidProviderID(providerID) {
			return nil, h.reject(ctx, stage, ErrInvalidRequest, zap.Int("provider_index", index), zap.Int("provider_id_length", len(providerID)))
		}
	}
	return req, nil
}

func (h *Handler) SanitizeUpdateOrganizationInferenceOperationPolicyRequest(ctx context.Context, req *proto_models.UpdateOrganizationInferenceOperationPolicyRequest) (*proto_models.UpdateOrganizationInferenceOperationPolicyRequest, error) {
	const stage = "sanitize_update_organization_inference_operation_policy_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if err := inferenceRoutingOrganizationID(req.OrganizationId); err != nil {
		return nil, h.reject(ctx, stage, err, zap.Uint64("organization_id", req.OrganizationId))
	}
	if err := inferenceRoutingOperation(req.Operation); err != nil {
		return nil, h.reject(ctx, stage, err, zap.Int32("operation", req.Operation))
	}
	if req.DefaultModelId != nil && *req.DefaultModelId == 0 {
		return nil, h.reject(ctx, stage, ErrInvalidRequest, zap.String("field", "default_model_id"))
	}
	if err := inferenceRoutingSessionAction(req.SessionAction, req.MoveToModelId); err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	for index, modelID := range req.InferenceModelIds {
		if modelID == 0 {
			return nil, h.reject(ctx, stage, ErrInvalidRequest, zap.Int("model_index", index))
		}
	}
	return req, nil
}

func (h *Handler) SanitizeListInferenceModelChoicesRequest(ctx context.Context, req *proto_models.ListInferenceModelChoicesRequest) (*proto_models.ListInferenceModelChoicesRequest, error) {
	const stage = "sanitize_list_inference_model_choices_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if err := inferenceRoutingOrganizationID(req.OrganizationId); err != nil {
		return nil, h.reject(ctx, stage, err, zap.Uint64("organization_id", req.OrganizationId))
	}
	if err := inferenceRoutingOperation(req.Operation); err != nil {
		return nil, h.reject(ctx, stage, err, zap.Int32("operation", req.Operation))
	}
	return req, nil
}

func inferenceRetargetSelection(modelID uint64, api *proto_models.InferenceAPI) error {
	if modelID == 0 {
		return fmt.Errorf("inference model is required: %w", ErrInvalidRequest)
	}
	if api != nil {
		if _, valid := proto_models.InferenceAPI_name[int32(*api)]; !valid {
			return fmt.Errorf("invalid inference API: %w", ErrInvalidRequest)
		}
	}
	return nil
}

func (h *Handler) SanitizeRetargetInferenceSessionRequest(ctx context.Context, req *proto_models.RetargetInferenceSessionRequest) (*proto_models.RetargetInferenceSessionRequest, error) {
	const stage = "sanitize_retarget_inference_session_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.AgentSessionId == 0 {
		return nil, h.reject(ctx, stage, ErrAgentSessionIDRequired)
	}
	if err := inferenceRetargetSelection(req.InferenceModelId, req.Api); err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	return req, nil
}

func (h *Handler) SanitizeRetargetInferenceSessionsRequest(ctx context.Context, req *proto_models.RetargetInferenceSessionsRequest) (*proto_models.RetargetInferenceSessionsRequest, error) {
	const stage = "sanitize_retarget_inference_sessions_request"
	policy := h.requestPolicy()
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if err := inferenceRoutingOrganizationID(req.OrganizationId); err != nil {
		return nil, h.reject(ctx, stage, err, zap.Uint64("organization_id", req.OrganizationId))
	}
	if len(req.Sessions) == 0 || len(req.Sessions) > policy.inference.retargetBatchSize {
		return nil, h.reject(ctx, stage, ErrInvalidRequest, zap.Int("session_count", len(req.Sessions)), zap.Int("max_sessions", policy.inference.retargetBatchSize))
	}
	if err := inferenceRetargetSelection(req.InferenceModelId, req.Api); err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	seen := make(map[uint64]struct{}, len(req.Sessions))
	for index, session := range req.Sessions {
		if session == nil || session.AgentSessionId == 0 {
			return nil, h.reject(ctx, stage, ErrAgentSessionIDRequired, zap.Int("session_index", index))
		}
		if _, duplicate := seen[session.AgentSessionId]; duplicate {
			return nil, h.reject(ctx, stage, ErrInvalidRequest, zap.Int("session_index", index))
		}
		seen[session.AgentSessionId] = struct{}{}
	}
	return req, nil
}
