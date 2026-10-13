package common_persistence

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// ChatAgentGraphDrivePersistence reads what an owner drives in one delegation graph.
type ChatAgentGraphDrivePersistence interface {
	// GetChatAgentGraphDriveState reads the graph rooted at rootSessionID (the root and its
	// delegated children) in one snapshot. ErrChatAgentSessionNotFound when the root is missing.
	GetChatAgentGraphDriveState(ctx context.Context, rootSessionID uint64) (*ChatAgentGraphDriveState, error)
	// RevokeChatAgentGraphDriveLeases clears every drive lease on the graph's runs, terminal runs
	// included. Its row locks wait for any transaction that already passed a run's token check, and
	// once it commits no token check on the graph's runs can pass, whatever the checking
	// transaction's start time.
	RevokeChatAgentGraphDriveLeases(ctx context.Context, rootSessionID uint64) error
}

// ChatAgentGraphDriveState is one snapshot of a delegation graph. The root's resume revision is
// read in the same snapshot as the runs, so an event committed after it advances the revision.
type ChatAgentGraphDriveState struct {
	Root models.ChatAgentSession
	// Children are the root's delegated children, archived ones included.
	Children []models.ChatAgentSession
	// RootRunID is the root's latest run, whatever its status.
	RootRunID uint64
	// Runs are the graph's nonterminal runs, by ascending id.
	Runs []ChatAgentGraphRun
	// Pool is the graph's budget pool, or nil when it has none.
	Pool *models.ChatAgentGraphBudget
}

// ChatAgentGraphRun is one nonterminal run of a delegation graph.
type ChatAgentGraphRun struct {
	ID, AgentSessionID  uint64
	Status              proto_models.ChatAgentRunStatus
	GenerationCount     uint64
	ToolCallCount       uint64
	LastSealedStepIndex *uint32
	// AwaitingApproval reports a tool call of the run that waits for an approval decision.
	AwaitingApproval bool
	// ToolWork reports calls of the run's sealed step that need no decision and are still to
	// execute: approved calls, and the non-approval calls of a mixed batch. A drive executes them
	// even while another call of the step awaits its decision.
	ToolWork bool
	// DueIn is how long until a drive may claim the run: its retry backoff, or the live lease of
	// another attempt, and for a run awaiting approval also until its tool work is recoverable.
	// Zero when it is claimable now.
	DueIn time.Duration
	// ToolWorkDueIn is how long until its tool work is recoverable: a call's live execution lease,
	// retry backoff, or fresh inline call's recovery delay. Zero when it is now or there is none.
	ToolWorkDueIn time.Duration
}

// Session returns the graph session sessionID, or nil when it is not in the graph.
func (s *ChatAgentGraphDriveState) Session(sessionID uint64) *models.ChatAgentSession {
	if s.Root.ID == sessionID {
		return &s.Root
	}
	for i := range s.Children {
		if s.Children[i].ID == sessionID {
			return &s.Children[i]
		}
	}
	return nil
}

// GetChatAgentGraphDriveState implements ChatAgentGraphDrivePersistence in one read-only
// REPEATABLE READ transaction, so every read shares the root's snapshot.
func (h *CommonPersistenceHandler[U]) GetChatAgentGraphDriveState(ctx context.Context, rootSessionID uint64) (*ChatAgentGraphDriveState, error) {
	if rootSessionID == 0 {
		return nil, ErrChatAgentSessionNotFound
	}
	state := &ChatAgentGraphDriveState{}
	err := h.getDBFromContext(ctx).Transaction(func(tx *gorm.DB) error {
		var sessions []models.ChatAgentSession
		if err := tx.Where("id = ? OR parent_agent_session_id = ?", rootSessionID, rootSessionID).Order("id").Find(&sessions).Error; err != nil {
			return err
		}
		ids := make([]uint64, 0, len(sessions))
		for _, session := range sessions {
			ids = append(ids, session.ID)
			if session.ID == rootSessionID {
				state.Root = session
			} else {
				state.Children = append(state.Children, session)
			}
		}
		if state.Root.ID == 0 || state.Root.IsDelegatedChild() {
			return ErrChatAgentSessionNotFound
		}
		runs := models.TableNameChatAgentRuns
		// A run is due when both its retry backoff and any other attempt's live lease have passed,
		// the claim's own conditions.
		dueAt := "GREATEST(COALESCE(" + runs + ".drive_not_before, CURRENT_TIMESTAMP), CASE WHEN " + runs +
			".drive_lease_owner IS NOT NULL AND " + runs + ".drive_lease_expires_at > CURRENT_TIMESTAMP THEN " + runs +
			".drive_lease_expires_at ELSE CURRENT_TIMESTAMP END)"
		// The earliest moment RecoverWorkflowAgentTools can execute a call of the sealed step.
		toolWorkAt := "(SELECT MIN(" + recoverableToolCallAtSQL("drive_work") + ") FROM " + models.TableNameChatMessageToolCalls + " drive_work JOIN " +
			models.TableNameChatMessages + " drive_work_messages ON drive_work_messages.id = drive_work.message_id WHERE drive_work.agent_run_id = " +
			runs + ".id AND drive_work.kind = ? AND " + runs + ".last_sealed_step_disposition = ? AND drive_work_messages.agent_step_index = " +
			runs + ".last_sealed_step_index)"
		var rows []struct {
			ChatAgentGraphRun
			DueInMs         int64
			ToolWorkDueInMs *int64
		}
		if err := tx.Model(&models.ChatAgentRun{}).
			Select(runs+".id, "+runs+".agent_session_id, "+runs+".status, "+runs+".generation_count, "+runs+".tool_call_count, "+runs+".last_sealed_step_index, "+
				"EXISTS (SELECT 1 FROM "+models.TableNameChatMessageToolCalls+" drive_calls WHERE drive_calls.agent_run_id = "+runs+".id AND drive_calls.execution_state = ?) AS awaiting_approval, "+
				"CEIL(EXTRACT(EPOCH FROM ("+dueAt+" - CURRENT_TIMESTAMP)) * 1000)::bigint AS due_in_ms, "+
				"CEIL(EXTRACT(EPOCH FROM ("+toolWorkAt+" - CURRENT_TIMESTAMP)) * 1000)::bigint AS tool_work_due_in_ms",
				proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_WAITING_FOR_APPROVAL,
				proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL, ChatAgentRunStepDispositionToolLoop).
			Where(runs+".agent_session_id IN ? AND "+runs+".status NOT IN ?", ids, TerminalChatAgentRunStatuses()).
			Order(runs + ".id").Scan(&rows).Error; err != nil {
			return err
		}
		for _, row := range rows {
			row.DueIn = time.Duration(max(row.DueInMs, 0)) * time.Millisecond
			if row.ToolWork = row.ToolWorkDueInMs != nil; row.ToolWork {
				row.ToolWorkDueIn = time.Duration(max(*row.ToolWorkDueInMs, 0)) * time.Millisecond
			}
			if row.AwaitingApproval {
				row.DueIn = max(row.DueIn, row.ToolWorkDueIn)
			}
			state.Runs = append(state.Runs, row.ChatAgentGraphRun)
		}
		if err := tx.Model(&models.ChatAgentRun{}).Select("COALESCE(MAX(id), 0)").
			Where("agent_session_id = ?", rootSessionID).Scan(&state.RootRunID).Error; err != nil {
			return err
		}
		var err error
		state.Pool, err = loadChatAgentGraphBudgetTx(tx, rootSessionID)
		return err
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		if errors.Is(err, ErrChatAgentSessionNotFound) {
			return nil, err
		}
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "get_chat_agent_graph_drive_state"),
			zap.Uint64(appLogger.KeyAgentSessionID, rootSessionID))
	}
	return state, nil
}

// RevokeChatAgentGraphDriveLeases implements ChatAgentGraphDrivePersistence in one statement.
func (h *CommonPersistenceHandler[U]) RevokeChatAgentGraphDriveLeases(ctx context.Context, rootSessionID uint64) error {
	err := h.getDBFromContext(ctx).Model(&models.ChatAgentRun{}).
		Where("drive_lease_owner IS NOT NULL AND agent_session_id IN (?)",
			h.getDBFromContext(ctx).Model(&models.ChatAgentSession{}).Select("id").Where("id = ? OR parent_agent_session_id = ?", rootSessionID, rootSessionID)).
		Updates(map[string]any{"drive_lease_owner": nil, "drive_lease_expires_at": nil, "updated_at": gorm.Expr("CURRENT_TIMESTAMP")}).Error
	if err != nil {
		return chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "revoke_chat_agent_graph_drive_leases"),
			zap.Uint64(appLogger.KeyAgentSessionID, rootSessionID))
	}
	return nil
}
