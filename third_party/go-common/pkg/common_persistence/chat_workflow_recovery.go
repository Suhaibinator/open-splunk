package common_persistence

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// WorkflowAgentRunRecoveryPersistence is required by CommonPersistenceInterface;
// only workflow-owned drivers may use it.
// Managed conversations recover only an interrupted stream (RecoverInterruptedChatAgentRunStep)
// and otherwise fail an incomplete step closed.
type WorkflowAgentRunRecoveryPersistence interface {
	ResetIncompleteWorkflowAgentRunDrive(context.Context, uint64) (bool, error)
	ClaimWorkflowAgentRunDrive(context.Context, uint64, string, time.Duration) (bool, error)
	ListRecoverableWorkflowToolCalls(context.Context, uint64, time.Time, int) ([]*common_persistence_models.ChatMessage, error)
	ClaimWorkflowToolCall(context.Context, uint64, uint64, string, time.Time, time.Time) (bool, error)
}

// ResetIncompleteWorkflowAgentRunDrive discards only the unsealed model-step projection.
// Sealed tool calls/results, inference accounting, and run budgets are never reset. The caller
// must hold a live durable drive lease and validate its application workflow ownership first.
// A step with evidence of tool execution is not safe to discard and is rejected atomically.
// A message anchoring any agent session (including an archived one) is also protected:
// recovery returns ErrChatMessageAgentSessionRootPinned without removing any step artifacts.
func (h *CommonPersistenceHandler[U]) ResetIncompleteWorkflowAgentRunDrive(ctx context.Context, runID uint64) (bool, error) {
	if runID == 0 {
		return false, ErrChatAgentRunNotFound
	}
	changed := false
	err := h.getDBFromContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := h.assertChatAgentRunDriveLeaseWithDB(ctx, tx, runID); err != nil {
			return err
		}
		var run common_persistence_models.ChatAgentRun
		if err := tx.Where("id = ?", runID).Take(&run).Error; err != nil {
			return err
		}
		messages, err := discardUnsealedAgentProjection(tx, &run, nil)
		if err != nil {
			return err
		}
		if len(messages) == 0 {
			return nil
		}
		changed = true
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrChatAgentRunDriveLeaseLost) || errors.Is(err, ErrChatAgentRunStepSealInvalid) || errors.Is(err, ErrChatMessageAgentSessionRootPinned) {
			return false, err
		}
		return false, chatPersistenceDatabaseError(err)
	}
	return changed, nil
}

// ClaimWorkflowAgentRunDrive admits an explicitly authorized workflow owner even
// while its sealed tool step is waiting. The caller must validate application
// ownership first; scanners must continue using ClaimChatAgentRunDrive. A run whose
// session, or whose session's parent, is archived is retiring and never claimed.
func (h *CommonPersistenceHandler[U]) ClaimWorkflowAgentRunDrive(ctx context.Context, runID uint64, owner string, duration time.Duration) (bool, error) {
	return h.claimChatAgentRunDrive(ctx, runID, owner, duration, func(db *gorm.DB) *gorm.DB {
		return db.Where("status IN ?", append(DrivableChatAgentRunStatuses(), proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_WAITING_FOR_APPROVAL)).
			Where("NOT " + chatAgentRunRetiringOwnerDrivenSQL)
	})
}

// ListRecoverableWorkflowToolCalls lists only the requested run's sealed work
// under its live drive token. Scope before limiting so other runs cannot starve it.
func (h *CommonPersistenceHandler[U]) ListRecoverableWorkflowToolCalls(ctx context.Context, runID uint64, now time.Time, limit int) ([]*common_persistence_models.ChatMessage, error) {
	if limit <= 0 {
		limit = defaultTranscriptScanBatchSize
	}
	var calls []common_persistence_models.ChatMessageToolCall
	err := h.getDBFromContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := h.assertChatAgentRunDriveLeaseWithDB(ctx, tx, runID); err != nil {
			return err
		}
		query := applyRecoverableToolCallPredicate(tx.Model(&common_persistence_models.ChatMessageToolCall{}), now).Where("agent_run_id = ?", runID)
		var err error
		calls, err = listFairClaimableToolCallRows(query, limit)
		return err
	})
	if err != nil {
		return nil, workflowRecoveryError(err)
	}
	return h.hydrateToolCallMessages(ctx, calls)
}

// ClaimWorkflowToolCall locks and revalidates the drive token in the same
// transaction as the conditional tool claim. A stale list or foreign run cannot
// grant execution authority, and a competing drive takeover waits for this claim.
func (h *CommonPersistenceHandler[U]) ClaimWorkflowToolCall(ctx context.Context, runID, messageID uint64, owner string, expiresAt, now time.Time) (bool, error) {
	if strings.TrimSpace(owner) == "" {
		return false, errors.New("tool execution lease owner is required")
	}
	claimed := false
	err := h.getDBFromContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := h.assertChatAgentRunDriveLeaseWithDB(ctx, tx, runID); err != nil {
			return err
		}
		result := applyRecoverableToolCallPredicate(tx.Model(&common_persistence_models.ChatMessageToolCall{}), now).
			Where("agent_run_id = ? AND message_id = ?", runID, messageID).
			Select("execution_state", "execution_lease_owner", "execution_lease_expires_at", "execution_not_before", "execution_started_at", "execution_attempt_count", "updated_at").
			Updates(struct {
				ExecutionState          proto_models.ToolCallExecutionState `gorm:"column:execution_state"`
				ExecutionLeaseOwner     *string                             `gorm:"column:execution_lease_owner"`
				ExecutionLeaseExpiresAt *time.Time                          `gorm:"column:execution_lease_expires_at"`
				ExecutionNotBefore      *time.Time                          `gorm:"column:execution_not_before"`
				ExecutionStartedAt      *time.Time                          `gorm:"column:execution_started_at"`
				ExecutionAttemptCount   clause.Expr                         `gorm:"column:execution_attempt_count;type:bigint"`
				UpdatedAt               time.Time                           `gorm:"column:updated_at;autoUpdateTime:false"`
			}{
				ExecutionState:          proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTING,
				ExecutionLeaseOwner:     &owner,
				ExecutionLeaseExpiresAt: &expiresAt,
				ExecutionNotBefore:      nil,
				ExecutionStartedAt:      &now,
				ExecutionAttemptCount:   gorm.Expr(common_persistence_models.TableNameChatMessageToolCalls + ".execution_attempt_count + 1"),
				UpdatedAt:               now,
			})
		claimed = result.RowsAffected > 0
		return result.Error
	})
	return claimed, workflowRecoveryError(err)
}

func workflowRecoveryError(err error) error {
	if err == nil || errors.Is(err, ErrChatAgentRunDriveLeaseLost) {
		return err
	}
	return chatPersistenceDatabaseError(err)
}

// discardUnsealedAgentProjection shares the workflow cleanup guards with narrowly authorized
// stream recovery. A non-nil expectedStep permits only failed assistant stream rows.
// The caller holds the run lock and commits cleanup together with its recovery allowance.
func discardUnsealedAgentProjection(tx *gorm.DB, run *common_persistence_models.ChatAgentRun, expectedStep *uint32) ([]common_persistence_models.ChatMessage, error) {
	runID := run.ID
	// Transcript writes already lock run -> channel, so preserve that ordering here.
	// Binding only locks channel -> session (never the root's originating run).
	// Hold the same channel row lock used by binding through the pin check and
	// deletion. Include soft-deleted channels to preserve existing recovery behavior.
	var channel common_persistence_models.ChatChannel
	if err := tx.Unscoped().Clauses(clause.Locking{Strength: "UPDATE"}).
		Select("id").Where("id = ?", run.ChannelID).Take(&channel).Error; err != nil {
		return nil, err
	}
	query := tx.Model(&common_persistence_models.ChatMessage{}).Where("agent_run_id = ? AND agent_step_index IS NOT NULL", runID)
	if run.LastSealedStepIndex != nil {
		query = query.Where("agent_step_index > ?", *run.LastSealedStepIndex)
	}
	var messages []common_persistence_models.ChatMessage
	if err := query.Find(&messages).Error; err != nil {
		return nil, err
	}
	if len(messages) == 0 {
		return nil, nil
	}
	if expectedStep != nil {
		for _, message := range messages {
			if message.AgentStepIndex == nil || *message.AgentStepIndex != *expectedStep ||
				message.Kind != proto_models.MessageKind_MESSAGE_KIND_STANDARD ||
				message.StreamState != proto_models.MessageStreamState_MESSAGE_STREAM_STATE_FAILED {
				return nil, ErrChatAgentRunStepSealInvalid
			}
		}
	}
	ids := make([]uint64, 0, len(messages))
	for _, message := range messages {
		if message.Kind == proto_models.MessageKind_MESSAGE_KIND_TOOL_RESULT {
			return nil, ErrChatAgentRunStepSealInvalid
		}
		ids = append(ids, message.ID)
	}
	var pinned int64
	if err := tx.Model(&common_persistence_models.ChatAgentSession{}).
		Where("channel_id = ? AND parent_message_id IN ?", run.ChannelID, ids).
		Count(&pinned).Error; err != nil {
		return nil, err
	}
	if pinned != 0 {
		return nil, ErrChatMessageAgentSessionRootPinned
	}
	if expectedStep != nil {
		var sideRows int64
		if err := tx.Model(&common_persistence_models.ChatMessageToolCall{}).Where("message_id IN ?", ids).Count(&sideRows).Error; err != nil {
			return nil, err
		}
		if sideRows != 0 {
			return nil, ErrChatAgentRunStepSealInvalid
		}
	}
	var executed int64
	if err := tx.Model(&common_persistence_models.ChatMessageToolCall{}).
		Where("message_id IN ?", ids).
		Where("execution_attempt_count > 0 OR execution_started_at IS NOT NULL OR executed_at IS NOT NULL OR execution_lease_owner IS NOT NULL OR execution_state IN ?", []proto_models.ToolCallExecutionState{
			proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTING,
			proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTED,
		}).Count(&executed).Error; err != nil {
		return nil, err
	}
	if executed != 0 {
		return nil, ErrChatAgentRunStepSealInvalid
	}
	// Side rows have non-partial tool-call identity indexes and durable execution queues.
	// Remove them rather than leaving abandoned calls eligible for execution or conflicting
	// with a repeated provider call ID. Soft-deleted message text remains for diagnostics.
	if err := tx.Where("message_id IN ?", ids).Delete(&common_persistence_models.ChatMessageToolCall{}).Error; err != nil {
		return nil, err
	}
	if err := tx.Where("id IN ?", ids).Delete(&common_persistence_models.ChatMessage{}).Error; err != nil {
		return nil, err
	}
	return messages, nil
}
