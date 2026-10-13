package common_persistence

import (
	"context"
	"errors"
	"fmt"

	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// AgentContextPersistence is an optional capability. Legacy adapters do not need it; explicit
// checkpoint mode fails configuration if the adapter cannot provide durable, fenced writes.
type AgentContextPersistence interface {
	TransactAgentContext(context.Context, uint64, func(context.Context, *models.ChatAgentContext) error) error
	GetAgentContextOperation(context.Context, string) (*models.ChatContextOperation, error)
	SaveAgentContextOperation(context.Context, *models.ChatContextOperation) error
}

// TransactAgentContext serializes a run and its channel/thread compaction scope. All context
// mutations, checkpoint writes and usage linkage in fn commit together. Never run inference in fn.
func (h *CommonPersistenceHandler[U]) TransactAgentContext(ctx context.Context, runID uint64, fn func(context.Context, *models.ChatAgentContext) error) error {
	return h.transaction(ctx, func(txCtx context.Context, tx *gorm.DB) error {
		if err := h.AssertChatAgentRunDriveLease(txCtx, runID); err != nil {
			return err
		}
		var run models.ChatAgentRun
		if err := tx.Where("id = ?", runID).Take(&run).Error; err != nil {
			return err
		}
		if run.AgentSessionID == nil || run.AgentSessionConfigVersion == nil {
			return ErrChatTranscriptRunMismatch
		}
		// Lock the scope independently of the session: multiple agents may share a transcript.
		if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", "agent-context:"+fmt.Sprintf("%d:%d", run.ChannelID, run.ParentMessageID)).Error; err != nil {
			return err
		}
		state := models.ChatAgentContext{AgentSessionID: *run.AgentSessionID, ConfigVersion: *run.AgentSessionConfigVersion}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&state).Error; err != nil {
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("agent_session_id = ? AND config_version = ?", state.AgentSessionID, state.ConfigVersion).Take(&state).Error; err != nil {
			return err
		}
		before := state
		if err := fn(txCtx, &state); err != nil {
			return err
		}
		// Compare persisted values without treating
		// an association pointer as a context-row change. Operation writes in fn
		// still commit when the context row itself needs no update.
		before.AgentSession = nil
		after := state
		after.AgentSession = nil
		if before == after {
			return nil
		}
		return tx.Save(&state).Error
	})
}

func (h *CommonPersistenceHandler[U]) GetAgentContextOperation(ctx context.Context, key string) (*models.ChatContextOperation, error) {
	var op models.ChatContextOperation
	err := h.DB(ctx).Where("key = ?", key).Take(&op).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &op, err
}
func (h *CommonPersistenceHandler[U]) SaveAgentContextOperation(ctx context.Context, op *models.ChatContextOperation) error {
	return h.DB(ctx).Save(op).Error
}

// RequestAgentContextCompaction is a service-authority request, not a checkpoint write. It can
// race an active drive safely: that drive observes the durable flag at its next step boundary.
func (h *CommonPersistenceHandler[U]) RequestAgentContextCompaction(ctx context.Context, runID uint64) error {
	return h.DB(ctx).Transaction(func(tx *gorm.DB) error {
		var run models.ChatAgentRun
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", runID).Take(&run).Error; err != nil {
			return err
		}
		if run.AgentSessionID == nil || run.AgentSessionConfigVersion == nil || (run.Status != proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING && run.Status != proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RETRYABLE && run.Status != proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_WAITING_FOR_APPROVAL) {
			return ErrChatTranscriptRunMismatch
		}
		state := models.ChatAgentContext{AgentSessionID: *run.AgentSessionID, ConfigVersion: *run.AgentSessionConfigVersion, ManualCompactionRunID: runID}
		return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "agent_session_id"}, {Name: "config_version"}}, DoUpdates: clause.AssignmentColumns([]string{"manual_compaction_run_id"})}).Create(&state).Error
	})
}
