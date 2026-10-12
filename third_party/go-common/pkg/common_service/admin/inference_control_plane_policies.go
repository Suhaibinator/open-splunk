// Admin control-plane model rate-limit policy administration: listing configured policies and
// applying validated policy edits (optional, all-or-none request/token buckets). The shared store
// interfaces, wiring, and permission helper live in inference_control_plane.go.

package admin

import (
	"context"
	stderrors "errors"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	"github.com/Suhaibinator/go-common/pkg/genai/controlplane"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ListInferenceRateLimitPolicies returns every configured model rate-limit policy.
func (h *AdminHandler[U]) ListInferenceRateLimitPolicies(ctx context.Context) (*proto_models.ListInferenceRateLimitPoliciesResponse, error) {
	store := h.inferenceModelPolicies
	if _, err := h.requireInferenceControlPlane(ctx, permission.PermissionInferenceViewing, "list inference rate-limit policies", store); err != nil {
		return nil, err
	}

	policies, err := store.ListModelRateLimitPolicies(ctx)
	if err != nil {
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "list_inference_rate_limit_policies"))
	}

	protoPolicies := make([]*proto_models.InferenceModelRateLimitPolicy, 0, len(policies))
	for i := range policies {
		protoPolicies = append(protoPolicies, inferenceRateLimitPolicyToProto(&policies[i]))
	}
	return &proto_models.ListInferenceRateLimitPoliciesResponse{Policies: protoPolicies}, nil
}

// UpdateInferenceRateLimitPolicy replaces one model's optional, all-or-none request and token
// buckets after validation. An absent bucket clears that limit. The in-process buckets rebuild
// automatically on the next execution that loads the policy (bucketsForScope rebuilds whenever the
// loaded policy differs from the one a bucket was built with), matching current behavior.
func (h *AdminHandler[U]) UpdateInferenceRateLimitPolicy(ctx context.Context, req *proto_models.UpdateInferenceRateLimitPolicyRequest) (*proto_models.UpdateInferenceRateLimitPolicyResponse, error) {
	store := h.inferenceModelPolicies
	adminUserID, err := h.requireInferenceControlPlane(ctx, permission.PermissionInferenceAdministration, "update inference rate-limit policy", store)
	if err != nil {
		return nil, err
	}
	if req == nil {
		return nil, errors.ErrInvalidRequest
	}

	model, err := store.GetModel(ctx, req.InferenceModelId)
	if err != nil {
		if stderrors.Is(err, common_persistence.ErrControlPlaneModelNotFound) {
			return nil, contextualAdminError(errors.ErrInferenceModelNotFound, err,
				zap.String(logkeys.Stage, "load_model_for_rate_limit_update"),
				zap.Uint64(appLogger.KeyInferenceModelID, req.InferenceModelId))
		}
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "load_model_for_rate_limit_update"),
			zap.Uint64(appLogger.KeyInferenceModelID, req.InferenceModelId))
	}

	kernelPolicy := controlplane.ModelRateLimitPolicy{}
	if req.RequestBucket != nil {
		kernelPolicy.HasRequestLimit = true
		kernelPolicy.RequestLimit = inferenceBucketFromProto(req.RequestBucket)
	}
	if req.TokenBucket != nil {
		kernelPolicy.HasTokenLimit = true
		kernelPolicy.TokenLimit = inferenceBucketFromProto(req.TokenBucket)
	}
	if err := controlplane.ValidateModelRateLimitPolicy(kernelPolicy); err != nil {
		return nil, contextualAdminWarn(errors.ErrInvalidRequest, err,
			zap.String(logkeys.Stage, "validate_inference_rate_limit_policy"),
			zap.Uint64(appLogger.KeyInferenceModelID, model.ID))
	}

	row := &common_persistence_models.InferenceModelRateLimitPolicy{
		InferenceModelID: model.ID,
		HasRequestLimit:  kernelPolicy.HasRequestLimit,
		HasTokenLimit:    kernelPolicy.HasTokenLimit,
	}
	if kernelPolicy.HasRequestLimit {
		row.RequestCapacity = int64(kernelPolicy.RequestLimit.Capacity)
		row.RequestRefillInterval = kernelPolicy.RequestLimit.RefillInterval
		row.RequestRefillUnits = int64(kernelPolicy.RequestLimit.RefillUnits)
		row.RequestWaitCap = kernelPolicy.RequestLimit.WaitCap
	}
	if kernelPolicy.HasTokenLimit {
		row.TokenCapacity = int64(kernelPolicy.TokenLimit.Capacity)
		row.TokenRefillInterval = kernelPolicy.TokenLimit.RefillInterval
		row.TokenRefillUnits = int64(kernelPolicy.TokenLimit.RefillUnits)
		row.TokenWaitCap = kernelPolicy.TokenLimit.WaitCap
	}
	if err := store.UpsertModelRateLimitPolicy(ctx, row); err != nil {
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "upsert_inference_rate_limit_policy"),
			zap.Uint64(appLogger.KeyInferenceModelID, model.ID))
	}

	stored, err := store.GetModelRateLimitPolicy(ctx, model.ID)
	if err != nil {
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "reload_inference_rate_limit_policy"),
			zap.Uint64(appLogger.KeyInferenceModelID, model.ID))
	}
	if stored == nil {
		return nil, appLogger.InvariantError("updated_inference_rate_limit_policy_is_reloadable", errors.ErrDatabaseError,
			zap.String(logkeys.Stage, "reload_inference_rate_limit_policy"),
			zap.Uint64(appLogger.KeyInferenceModelID, model.ID),
			zap.String(logkeys.Expected, "persisted rate-limit policy"),
			zap.String(logkeys.Actual, "nil"))
	}

	appLogger.For(ctx, h.logger, adminLoggerName).Info("Updated inference rate-limit policy",
		zap.Uint64(appLogger.KeyAdminUserID, adminUserID),
		zap.Uint64(appLogger.KeyInferenceModelID, model.ID))

	return &proto_models.UpdateInferenceRateLimitPolicyResponse{Policy: inferenceRateLimitPolicyToProto(stored)}, nil
}

// inferenceRateLimitPolicyToProto converts a policy row into its wire projection with optional,
// all-or-none buckets.
func inferenceRateLimitPolicyToProto(policy *common_persistence_models.InferenceModelRateLimitPolicy) *proto_models.InferenceModelRateLimitPolicy {
	protoPolicy := &proto_models.InferenceModelRateLimitPolicy{
		InferenceModelId: policy.InferenceModelID,
		CreatedAt:        timestamppb.New(policy.CreatedAt),
		UpdatedAt:        timestamppb.New(policy.UpdatedAt),
	}
	if policy.HasRequestLimit {
		protoPolicy.RequestBucket = &proto_models.InferenceRateLimitBucket{
			Capacity:       policy.RequestCapacity,
			RefillInterval: durationpb.New(policy.RequestRefillInterval),
			RefillUnits:    policy.RequestRefillUnits,
			WaitCap:        durationpb.New(policy.RequestWaitCap),
		}
	}
	if policy.HasTokenLimit {
		protoPolicy.TokenBucket = &proto_models.InferenceRateLimitBucket{
			Capacity:       policy.TokenCapacity,
			RefillInterval: durationpb.New(policy.TokenRefillInterval),
			RefillUnits:    policy.TokenRefillUnits,
			WaitCap:        durationpb.New(policy.TokenWaitCap),
		}
	}
	return protoPolicy
}

// inferenceBucketFromProto converts a wire bucket into the kernel policy shape for validation.
func inferenceBucketFromProto(bucket *proto_models.InferenceRateLimitBucket) controlplane.BucketRateLimitPolicy {
	return controlplane.BucketRateLimitPolicy{
		Capacity:       int(bucket.Capacity),
		RefillInterval: bucket.RefillInterval.AsDuration(),
		RefillUnits:    int(bucket.RefillUnits),
		WaitCap:        bucket.WaitCap.AsDuration(),
	}
}
