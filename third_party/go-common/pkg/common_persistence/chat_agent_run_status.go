// Package common_persistence provides the persistence layer for the common API.
// This file holds the read side of the thread-scoped agent-run status model: the newest runs of
// each (channel, thread) location and the durable progress evidence of the runs still in flight.
// Nothing here is written by these reads, and no run activity is ever stored.
package common_persistence

import (
	"context"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/lib/pq"
	"go.uber.org/zap"
)

// ChatAgentRunActiveToolCall identifies one unresolved TOOL_CALL of a run. HiddenFromUsers lets the
// service keep a hidden row's id out of viewer-facing status.
type ChatAgentRunActiveToolCall struct {
	MessageID uint64
	Since     time.Time
	// LeaseExpiresAt is the execution lease deadline of an EXECUTING call; nil for a pending approval.
	LeaseExpiresAt  *time.Time
	HiddenFromUsers bool
}

// ChatAgentRunProgress is the durable progress evidence of one non-terminal run.
type ChatAgentRunProgress struct {
	// LastProgressAt is the newest transcript write tagged with the run, hidden rows included: the
	// time of hidden progress is not sensitive, only its content and identity are.
	LastProgressAt *time.Time
	// ExecutingTool is the longest-running TOOL_CALL currently claimed for execution.
	ExecutingTool *ChatAgentRunActiveToolCall
	// PendingApproval is the oldest TOOL_CALL still waiting for a human decision.
	PendingApproval *ChatAgentRunActiveToolCall
}

// ListLatestChatAgentRunsByThread returns, for each requested thread location of one channel, its
// newest perLocation runs (newest first within a location). parentMessageIDs uses the chat thread
// convention where 0 is the main timeline; locations without runs are simply absent.
func (h *CommonPersistenceHandler[U]) ListLatestChatAgentRunsByThread(
	ctx context.Context,
	channelID uint64,
	parentMessageIDs []uint64,
	perLocation int,
) ([]*common_persistence_models.ChatAgentRun, error) {
	if channelID == 0 || len(parentMessageIDs) == 0 || perLocation <= 0 {
		return nil, nil
	}
	// Zero is a real location here, so deduplicate without DeduplicateNonZero.
	seen := make(map[uint64]struct{}, len(parentMessageIDs))
	signedParentIDs := make(pq.Int64Array, 0, len(parentMessageIDs))
	for _, id := range parentMessageIDs {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		signedParentIDs = append(signedParentIDs, int64(id))
	}

	runsTable := common_persistence_models.TableNameChatAgentRuns
	var runs []*common_persistence_models.ChatAgentRun
	// One LATERAL probe per location walks idx_chat_agent_runs_channel_parent_run in index order and
	// stops after perLocation rows, so a long main timeline never sorts its whole run history.
	err := h.getDBFromContext(ctx).Raw(
		"SELECT latest.* FROM unnest(?::bigint[]) AS location(parent_message_id) "+
			"CROSS JOIN LATERAL (SELECT * FROM "+runsTable+" WHERE channel_id = ? AND parent_message_id = location.parent_message_id ORDER BY id DESC LIMIT ?) AS latest "+
			"ORDER BY latest.parent_message_id, latest.id DESC",
		signedParentIDs, channelID, perLocation,
	).Scan(&runs).Error
	if err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_latest_chat_agent_runs_by_thread"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Int(appLogger.KeyCount, len(signedParentIDs)))
	}
	return runs, nil
}

// GetChatAgentRunProgress returns durable progress evidence keyed by run id. Callers pass only
// non-terminal runs: a finished run's progress is its completed_at. Runs with no transcript rows
// and no unresolved tool calls are absent from the result.
func (h *CommonPersistenceHandler[U]) GetChatAgentRunProgress(
	ctx context.Context,
	runIDs []uint64,
) (map[uint64]*ChatAgentRunProgress, error) {
	runIDs = common_util.DeduplicateNonZero(runIDs)
	progress := make(map[uint64]*ChatAgentRunProgress, len(runIDs))
	if len(runIDs) == 0 {
		return progress, nil
	}
	entry := func(runID uint64) *ChatAgentRunProgress {
		p, ok := progress[runID]
		if !ok {
			p = &ChatAgentRunProgress{}
			progress[runID] = p
		}
		return p
	}
	db := h.getDBFromContext(ctx)
	messagesTable := common_persistence_models.TableNameChatMessages
	toolCallsTable := common_persistence_models.TableNameChatMessageToolCalls

	var newest []struct {
		AgentRunID     uint64
		LastProgressAt time.Time
	}
	if err := db.Table(messagesTable).
		Select("agent_run_id, MAX(created_at) AS last_progress_at").
		Where("agent_run_id IN ? AND deleted_at IS NULL", runIDs).
		Group("agent_run_id").
		Scan(&newest).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "get_chat_agent_run_progress_newest_message"),
			zap.Int(appLogger.KeyCount, len(runIDs)))
	}
	for _, row := range newest {
		at := row.LastProgressAt
		entry(row.AgentRunID).LastProgressAt = &at
	}

	var active []struct {
		AgentRunID              uint64
		MessageID               uint64
		ExecutionState          proto_models.ToolCallExecutionState
		ExecutionStartedAt      *time.Time
		ExecutionLeaseExpiresAt *time.Time
		CreatedAt               time.Time
		HiddenFromUsers         *bool
	}
	// idx_chat_message_tool_calls_call_unique (agent_run_id, tool_call_id) WHERE kind = TOOL_CALL
	// serves the run-id probe; a run has few unresolved calls, so the state filter stays in the heap.
	if err := db.Table(toolCallsTable+" AS tc").
		Select("tc.agent_run_id, tc.message_id, tc.execution_state, tc.execution_started_at, tc.execution_lease_expires_at, tc.created_at, m.hidden_from_users").
		Joins("JOIN "+messagesTable+" AS m ON m.id = tc.message_id").
		Where("tc.agent_run_id IN ? AND tc.kind = ?", runIDs, proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL).
		Where("m.deleted_at IS NULL").
		Where("tc.execution_state = ? OR (tc.execution_state = ? AND tc.approval_status = ?)",
			proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTING,
			proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_WAITING_FOR_APPROVAL,
			proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_PENDING).
		Order("tc.message_id ASC").
		Scan(&active).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "get_chat_agent_run_progress_active_tool_calls"),
			zap.Int(appLogger.KeyCount, len(runIDs)))
	}
	for _, row := range active {
		call := &ChatAgentRunActiveToolCall{
			MessageID:       row.MessageID,
			Since:           row.CreatedAt,
			HiddenFromUsers: row.HiddenFromUsers != nil && *row.HiddenFromUsers,
		}
		p := entry(row.AgentRunID)
		if row.ExecutionState == proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTING {
			if row.ExecutionStartedAt != nil {
				call.Since = *row.ExecutionStartedAt
			}
			call.LeaseExpiresAt = row.ExecutionLeaseExpiresAt
			if p.ExecutingTool == nil || call.Since.Before(p.ExecutingTool.Since) {
				p.ExecutingTool = call
			}
			continue
		}
		if p.PendingApproval == nil {
			p.PendingApproval = call
		}
	}
	return progress, nil
}
