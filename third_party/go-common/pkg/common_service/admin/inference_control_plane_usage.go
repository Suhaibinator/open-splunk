// Admin control-plane attempt-usage aggregation: rolls a filtered attempt set into totals plus the
// provider/operation/status breakdown. It reuses the filter conversion and operation lookup from
// inference_control_plane_attempts.go. The shared store interfaces, wiring, and permission helper
// live in inference_control_plane.go.

package admin

import (
	"context"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	"github.com/Suhaibinator/go-common/pkg/genai/controlplane"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// GetInferenceUsage aggregates the filtered attempt set into a rollup plus the
// provider/operation/status breakdown.
func (h *AdminHandler[U]) GetInferenceUsage(ctx context.Context, req *proto_models.GetInferenceUsageRequest) (*proto_models.GetInferenceUsageResponse, error) {
	store := h.inferenceUsage
	if _, err := h.requireInferenceControlPlane(ctx, permission.PermissionInferenceViewing, "get inference usage", store); err != nil {
		return nil, err
	}

	filter, matchesNothing, err := h.attemptFilterFromProto(req.GetFilter())
	if err != nil {
		return nil, err
	}
	if matchesNothing {
		return &proto_models.GetInferenceUsageResponse{Summary: &proto_models.InferenceUsageSummary{}}, nil
	}

	summary, breakdown, err := store.AggregateAttempts(ctx, filter)
	if err != nil {
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "aggregate_inference_attempts"))
	}

	protoRows := make([]*proto_models.InferenceUsageBreakdownRow, 0, len(breakdown))
	for _, row := range breakdown {
		protoRow := &proto_models.InferenceUsageBreakdownRow{
			ProviderId:   row.ProviderID,
			Operation:    row.Operation,
			Status:       row.Status,
			AttemptCount: row.AttemptCount,
			TotalTokens:  row.TotalTokens,
		}
		if operation, ok := h.inferenceRegistry.Lookup(controlplane.Operation(row.Operation)); ok {
			protoRow.SymbolicName = operation.SymbolicName
			protoRow.DisplayName = operation.DisplayName
		}
		protoRows = append(protoRows, protoRow)
	}

	return &proto_models.GetInferenceUsageResponse{
		Summary: &proto_models.InferenceUsageSummary{
			AttemptCount: summary.AttemptCount,
			SuccessCount: summary.SuccessCount,
			FailureCount: summary.FailureCount,
			InputTokens:  summary.InputTokens,
			OutputTokens: summary.OutputTokens,
			TotalTokens:  summary.TotalTokens,
		},
		Breakdown: protoRows,
	}, nil
}
