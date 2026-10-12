package common_persistence

import (
	"context"
	"database/sql"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// GetMaxChatInferenceStepIndex returns the greatest non-null step in a run's
// complete ledger, including failed calls and calls whose transcript was compacted.
// A nil result distinguishes an empty/all-null history from a real step zero.
func (h *CommonPersistenceHandler[U]) GetMaxChatInferenceStepIndex(ctx context.Context, runID uint64) (*uint32, error) {
	var result struct{ MaxStep sql.Null[uint32] }
	if err := h.getDBFromContext(ctx).Model(&common_persistence_models.ChatInferenceCall{}).
		Select("MAX(agent_step_index) AS max_step").Where("agent_run_id = ?", runID).
		Scan(&result).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "max_chat_inference_step_index"),
			zap.Uint64(appLogger.KeyAgentRunID, runID))
	}
	if !result.MaxStep.Valid {
		return nil, nil
	}
	step := result.MaxStep.V
	return &step, nil
}
