package inference_policy

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	cpm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/go-common/pkg/genai/inference"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
)

// The request sanitizer checks only shape (nil entries, zero IDs, enum validity, size caps). The
// helpers below own the policy rules: per-view size limits, deduplication and ordering, payer
// conflicts between duplicates, provider ID syntax, and the session-action/move-target pairing.

func uniqueModelIDs(values []uint64, limit int) ([]uint64, error) {
	if len(values) > limit {
		return nil, ErrInvalidPolicy
	}
	seen := make(map[uint64]bool, len(values))
	result := make([]uint64, 0, len(values))
	for _, value := range values {
		if value == 0 || value > math.MaxInt64 {
			return nil, ErrInvalidPolicy
		}
		if !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	sort.Slice(result, func(left, right int) bool { return result[left] < result[right] })
	return result, nil
}

func operationOptions(operation int32, models []*pb.InferenceOperationModelOption, limit int) ([]cpm.InferenceOperationModelOption, []uint64, error) {
	if len(models) > limit {
		return nil, nil, ErrInvalidPolicy
	}
	seen := make(map[uint64]pb.InferenceModelPayerEligibility, len(models))
	options := make([]cpm.InferenceOperationModelOption, 0, len(models))
	ids := make([]uint64, 0, len(models))
	for _, model := range models {
		if model == nil || model.InferenceModelId == 0 || model.InferenceModelId > math.MaxInt64 {
			return nil, nil, ErrInvalidPolicy
		}
		if _, valid := pb.InferenceModelPayerEligibility_name[int32(model.PayerEligibility)]; !valid || model.PayerEligibility == pb.InferenceModelPayerEligibility_INFERENCE_MODEL_PAYER_ELIGIBILITY_UNSPECIFIED {
			return nil, nil, ErrInvalidPolicy
		}
		if previous, duplicate := seen[model.InferenceModelId]; duplicate {
			if previous != model.PayerEligibility {
				return nil, nil, ErrInvalidPolicy
			}
			continue
		}
		seen[model.InferenceModelId] = model.PayerEligibility
		options = append(options, cpm.InferenceOperationModelOption{Operation: operation, InferenceModelID: model.InferenceModelId, PayerEligibility: model.PayerEligibility})
		ids = append(ids, model.InferenceModelId)
	}
	sort.Slice(options, func(left, right int) bool { return options[left].InferenceModelID < options[right].InferenceModelID })
	return options, ids, nil
}

func providerOptions(organizationID uint64, values []string, limit int) ([]cpm.OrganizationInferenceProvider, error) {
	if len(values) > limit {
		return nil, ErrInvalidPolicy
	}
	seen := make(map[string]bool, len(values))
	result := make([]cpm.OrganizationInferenceProvider, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if !config.ValidProviderID(value) {
			return nil, ErrInvalidPolicy
		}
		if !seen[value] {
			seen[value] = true
			result = append(result, cpm.OrganizationInferenceProvider{OrganizationID: int64(organizationID), ProviderID: value})
		}
	}
	sort.Slice(result, func(left, right int) bool { return result[left].ProviderID < result[right].ProviderID })
	return result, nil
}

// validateAction is the sole enforcer of the move rules: MOVE needs a valid target and every other
// action rejects one.
func validateAction(action pb.InferencePolicySessionAction, target *uint64) error {
	switch action {
	case pb.InferencePolicySessionAction_INFERENCE_POLICY_SESSION_ACTION_UNSPECIFIED, pb.InferencePolicySessionAction_INFERENCE_POLICY_SESSION_ACTION_KEEP, pb.InferencePolicySessionAction_INFERENCE_POLICY_SESSION_ACTION_BLOCK:
		if target != nil {
			return ErrInvalidPolicy
		}
	case pb.InferencePolicySessionAction_INFERENCE_POLICY_SESSION_ACTION_MOVE:
		if target == nil || *target == 0 || *target > math.MaxInt64 {
			return ErrInvalidPolicy
		}
	default:
		return ErrInvalidPolicy
	}
	return nil
}

func (h *Handler) validateMove(ctx context.Context, organizationID, actor uint64, operation int32, target *uint64, snapshot *cpm.InferenceRoutingSnapshot) error {
	if target == nil {
		return nil
	}
	_, err := h.resolver.ResolveSnapshot(ctx, inference.GenerationRequest{OrganizationID: organizationID, ActorUserID: actor, Operation: operation, ModelSelection: &inference.ModelSelection{InferenceModelID: *target, SelectionSource: pb.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_RETARGET}}, snapshot)
	if err != nil {
		return policyModelError(organizationID, "invalid move target", err)
	}
	return nil
}

// policyModelError names an ancestor's provider allowlist when it is what rejects a model the
// organization saved, since editors cannot see ancestor allowlists; other failures stay
// ErrInvalidPolicy.
func policyModelError(organizationID uint64, problem string, err error) error {
	var blocked *inference.BlockError
	if organizationID != 0 && errors.As(err, &blocked) && blocked.Reason == pb.InferenceBlockReason_INFERENCE_BLOCK_REASON_PROVIDER_NOT_ALLOWED && blocked.OrganizationID != organizationID {
		return fmt.Errorf("%w: %s: model %d blocked by organization %d", ErrAncestorProviderBlocked, problem, blocked.InferenceModelID, blocked.OrganizationID)
	}
	return fmt.Errorf("%w: %s: %v", ErrInvalidPolicy, problem, err)
}

func (h *Handler) validateOrganizationModels(ctx context.Context, organizationID, actor uint64, operation int32, models []uint64, snapshot *cpm.InferenceRoutingSnapshot) error {
	request := inference.GenerationRequest{OrganizationID: organizationID, ActorUserID: actor, Operation: operation}
	if _, err := h.resolver.ResolveSnapshot(ctx, request, snapshot); err != nil {
		return policyModelError(organizationID, "invalid organization default", err)
	}
	for _, modelID := range models {
		request.ModelSelection = &inference.ModelSelection{InferenceModelID: modelID, SelectionSource: pb.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_RETARGET}
		if _, err := h.resolver.ResolveSnapshot(ctx, request, snapshot); err != nil {
			return policyModelError(organizationID, "invalid organization model", err)
		}
	}
	return nil
}

func effectiveModelIDs(snapshot *cpm.InferenceRoutingSnapshot, organizationID uint64, operation int32) map[uint64]bool {
	models := make(map[uint64]bool)
	count := 0
	for _, option := range snapshot.Options {
		if option.Operation == operation {
			count++
			if option.PayerEligibility != pb.InferenceModelPayerEligibility_INFERENCE_MODEL_PAYER_ELIGIBILITY_CONNECTION_ONLY {
				models[option.InferenceModelID] = true
			}
		}
	}
	if count <= 1 {
		clear(models)
		for _, route := range snapshot.Routes {
			if route.Operation == operation {
				models[route.InferenceModelID] = true
			}
		}
	}
	if organizationID != 0 && count > 1 {
		narrowed := make(map[uint64]bool)
		for _, option := range snapshot.OrganizationModelOptions {
			if uint64(option.OrganizationID) == organizationID && option.Operation == operation {
				narrowed[option.InferenceModelID] = true
			}
		}
		if len(narrowed) > 0 {
			for modelID := range models {
				if !narrowed[modelID] {
					delete(models, modelID)
				}
			}
		}
	}
	return models
}

func removedModelIDs(previous, proposed *cpm.InferenceRoutingSnapshot, organizationID uint64, operation int32) []uint64 {
	before, after := effectiveModelIDs(previous, organizationID, operation), effectiveModelIDs(proposed, organizationID, operation)
	removed := make([]uint64, 0)
	for modelID := range before {
		if !after[modelID] {
			removed = append(removed, modelID)
		}
	}
	sort.Slice(removed, func(left, right int) bool { return removed[left] < removed[right] })
	return removed
}

// actionNeeded reports whether a policy change strands sessions that action must touch: BLOCK or
// MOVE with removed models or closed member choice.
func actionNeeded(action pb.InferencePolicySessionAction, models []uint64, includeUserChoices bool) bool {
	if action == pb.InferencePolicySessionAction_INFERENCE_POLICY_SESSION_ACTION_KEEP || action == pb.InferencePolicySessionAction_INFERENCE_POLICY_SESSION_ACTION_UNSPECIFIED {
		return false
	}
	return len(models) > 0 || includeUserChoices
}

// actionReady fails before any write when the change needs a session action that is not wired.
func (h *Handler) actionReady(action pb.InferencePolicySessionAction, models []uint64, includeUserChoices bool) error {
	if actionNeeded(action, models, includeUserChoices) && h.sessionAction == nil {
		return ErrUnavailable
	}
	return nil
}

// applyAction hands the session action its own input; the caller does not reuse it.
func (h *Handler) applyAction(ctx context.Context, view config.ServiceView, input SessionActionInput) error {
	if !actionNeeded(input.Action, input.ModelIDs, input.IncludeUserChoices) {
		return nil
	}
	if h.sessionAction == nil {
		return ErrUnavailable
	}
	input.BatchSize = view.InferenceRetargetBatchSize()
	return h.sessionAction(ctx, view, input)
}

func memberChoice(snapshot *cpm.InferenceRoutingSnapshot, organizationID uint64, operation int32) bool {
	policy := operationPolicy(snapshot, organizationID, operation)
	return policy != nil && policy.MemberChoice
}
