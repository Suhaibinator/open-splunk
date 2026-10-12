// This file implements admin orchestration for the AI control plane:
// route and rate-limit administration plus attempt/usage reads. Reads require
// permission.PermissionInferenceViewing and writes require permission.PermissionInferenceAdministration
// (endpoints are additionally gated by AdminAuthMiddlewareWithPermission; the checks here are
// defense in depth, matching the inference-model admin methods).
//
// Operation identity on the wire and in persistence is the importing application's numeric proto
// enum value. Symbolic and display names come from the code-owned registry.
//
// The endpoint methods are split across sibling files by concern: route administration
// (inference_control_plane_routes.go), rate-limit policy administration
// (inference_control_plane_policies.go), attempt-ledger reads (inference_control_plane_attempts.go),
// and usage aggregation (inference_control_plane_usage.go). This file holds the shared store
// interfaces, composition-time wiring, and the permission/availability gate they all use.
package admin

import (
	"context"

	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	"github.com/Suhaibinator/go-common/pkg/genai/controlplane"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// InferenceModelReader is the shared model lookup capability used by route and policy
// administration.
type InferenceModelReader interface {
	GetModel(ctx context.Context, modelID uint64) (*common_persistence_models.InferenceModel, error)
}

// InferenceRouteStore is the persistence surface used by route administration.
type InferenceRouteStore interface {
	InferenceModelReader
	ListRoutes(ctx context.Context) ([]common_persistence_models.InferenceOperationRoute, error)
	GetRoute(ctx context.Context, operation int32) (*common_persistence_models.InferenceOperationRoute, error)
	// UpsertRoute must assign config_version atomically in the write and refresh route from the
	// written row, so the caller sees this edit's exact version even under concurrent edits.
	UpsertRoute(ctx context.Context, route *common_persistence_models.InferenceOperationRoute) error
}

// InferenceModelPolicyStore is the persistence surface used by model-policy administration.
type InferenceModelPolicyStore interface {
	InferenceModelReader
	ListModelRateLimitPolicies(ctx context.Context) ([]common_persistence_models.InferenceModelRateLimitPolicy, error)
	GetModelRateLimitPolicy(ctx context.Context, modelID uint64) (*common_persistence_models.InferenceModelRateLimitPolicy, error)
	UpsertModelRateLimitPolicy(ctx context.Context, policy *common_persistence_models.InferenceModelRateLimitPolicy) error
}

// InferenceAttemptReadStore is the persistence surface used by attempt-ledger reads.
type InferenceAttemptReadStore interface {
	ListAttempts(ctx context.Context, filter common_persistence.AttemptFilter) ([]common_persistence_models.InferenceAttempt, int64, error)
	GetAttemptDetail(ctx context.Context, id uint64) (*common_persistence.InferenceAttemptDetail, error)
}

// InferenceUsageAggregationStore is the persistence surface used by usage aggregation.
type InferenceUsageAggregationStore interface {
	// AggregateAttempts returns the filtered set's totals and its provider/operation/status
	// breakdown from one consistent read.
	AggregateAttempts(ctx context.Context, filter common_persistence.AttemptFilter) (*common_persistence.AttemptSummary, []common_persistence.AttemptBreakdownRow, error)
}

// InferenceControlPlaneStore composes every admin capability at the application wiring boundary.
// Individual admin concerns depend on the narrower interfaces above. All writes are
// transaction-independent by store design (decision 13), so no transaction middleware wraps
// these endpoints.
type InferenceControlPlaneStore interface {
	InferenceRouteStore
	InferenceModelPolicyStore
	InferenceAttemptReadStore
	InferenceUsageAggregationStore
}

// Compile-time check that the concrete control-plane store satisfies the composed admin surface.
var _ InferenceControlPlaneStore = (*common_persistence.ControlPlaneStore)(nil)

// SetInferenceControlPlane wires the persistence store and the same code-owned operation registry
// used by execution. Until both are present, every control-plane admin method is unavailable.
func (h *AdminHandler[U]) SetInferenceControlPlane(store InferenceControlPlaneStore, registry *controlplane.Registry) {
	if h == nil {
		return
	}
	h.inferenceRoutes = store
	h.inferenceModelPolicies = store
	h.inferenceAttempts = store
	h.inferenceUsage = store
	h.inferenceRegistry = registry
}

// requireInferenceControlPlane checks the permission and then that the requested capability is
// wired.
func (h *AdminHandler[U]) requireInferenceControlPlane(
	ctx context.Context,
	operation permission.SitePermissionOperation,
	action string,
	capability any,
) (uint64, error) {
	adminUserID, _, err := h.requirePermission(ctx, operation, action)
	if err != nil {
		return 0, err
	}
	if capability == nil || h.inferenceRegistry == nil {
		appLogger.For(ctx, h.logger, adminLoggerName).Warn("Inference control plane not configured for admin operation",
			zap.String(appLogger.KeyAction, action))
		return 0, errors.ErrInferenceControlPlaneUnavailable
	}
	return uint64(adminUserID), nil
}
