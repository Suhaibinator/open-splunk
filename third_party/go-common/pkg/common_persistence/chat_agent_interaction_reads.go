package common_persistence

import (
	"context"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// ChatAgentSessionInteractionState is what an agent session's interaction read needs.
type ChatAgentSessionInteractionState struct {
	Session *common_persistence_models.ChatAgentSession
	// ChatAgentSessionInputState is the session's active run, queued input count, and policy in force,
	// which is always read. The active run carries only its identity, location, status, triggering
	// user and captured config version.
	ChatAgentSessionInputState
}

// chatAgentSessionInteractionRow is one row of chatAgentSessionInteractionQuery.
type chatAgentSessionInteractionRow struct {
	Session                   common_persistence_models.ChatAgentSession `gorm:"embedded"`
	ActiveRunID               *uint64
	ActiveRunStatus           proto_models.ChatAgentRunStatus
	TriggeringUserID          *uint64
	AgentSessionConfigVersion *uint64
	Policy                    common_persistence_models.ChatAgentInteractionPolicy `gorm:"embedded"`
	PendingCount              uint32
}

// chatAgentSessionInteractionQuery reads, for the sessions (alias s) the caller's Where selects,
// each session with its active run (the one-active-run index makes that join yield at most one
// row), the policy in force (the active run's captured snapshot, else the session's active one)
// and its queued input count, in one query that takes no locks.
func chatAgentSessionInteractionQuery(db *gorm.DB) *gorm.DB {
	return db.Table(common_persistence_models.TableNameChatAgentSessions+" s").
		Select(`s.*, r.id AS active_run_id, r.status AS active_run_status, r.triggering_user_id,
			r.agent_session_config_version, p.input_mode, p.allow_user_stop,
			(SELECT COUNT(*) FROM `+common_persistence_models.TableNameChatAgentQueuedInputs+` q
				WHERE q.agent_session_id = s.id) AS pending_count`).
		Joins("LEFT JOIN "+common_persistence_models.TableNameChatAgentRuns+" r ON r.agent_session_id = s.id AND r.status IN ?", ActiveChatAgentRunStatuses()).
		Joins("LEFT JOIN " + common_persistence_models.TableNameChatAgentSessionConfigSnapshots +
			" p ON p.agent_session_id = s.id AND p.version = COALESCE(r.agent_session_config_version, s.active_config_version)")
}

// state converts a row of chatAgentSessionInteractionQuery. Returns
// ErrChatAgentSessionConfigNotFound when the session has no snapshot for the policy version.
func (row *chatAgentSessionInteractionRow) state() (*ChatAgentSessionInteractionState, error) {
	// input_mode is NOT NULL and never UNSPECIFIED, so a zero means the snapshot join missed.
	if row.Policy.InputMode == proto_models.ChatAgentInputMode_CHAT_AGENT_INPUT_MODE_UNSPECIFIED {
		return nil, ErrChatAgentSessionConfigNotFound
	}
	state := &ChatAgentSessionInteractionState{
		Session: &row.Session,
		Policy:  row.Policy, PendingCount: row.PendingCount,
	}
	if row.ActiveRunID != nil {
		state.ActiveRun = &common_persistence_models.ChatAgentRun{
			ID:                        *row.ActiveRunID,
			ChannelID:                 row.Session.ChannelID,
			ParentMessageID:           row.Session.ParentMessageID,
			AgentSessionID:            &row.Session.ID,
			AgentSessionConfigVersion: row.AgentSessionConfigVersion,
			Status:                    row.ActiveRunStatus,
			TriggeringUserID:          row.TriggeringUserID,
		}
	}
	return state, nil
}

// GetChatAgentSessionInteractionState reads an agent session's interaction state: the session, its
// active run, its queued input count, and the policy in force (chatAgentSessionInteractionQuery).
//
// Returns ErrChatAgentSessionNotFound when the session does not exist, and
// ErrChatAgentSessionConfigNotFound when it has no snapshot for the policy version.
func (h *CommonPersistenceHandler[U]) GetChatAgentSessionInteractionState(ctx context.Context, sessionID uint64) (*ChatAgentSessionInteractionState, error) {
	var rows []chatAgentSessionInteractionRow
	if err := chatAgentSessionInteractionQuery(h.getDBFromContext(ctx)).Where("s.id = ?", sessionID).Scan(&rows).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "get_chat_agent_session_interaction_state"),
			zap.Uint64(appLogger.KeyAgentSessionID, sessionID))
	}
	if len(rows) == 0 {
		return nil, ErrChatAgentSessionNotFound
	}
	return rows[0].state()
}

// ListChatAgentSessionInteractionStates is GetChatAgentSessionInteractionState for the sessions at
// several locations of one channel, in one query. Locations without a session are left out.
//
// Returns ErrChatAgentSessionConfigNotFound when a session has no snapshot for the policy version.
func (h *CommonPersistenceHandler[U]) ListChatAgentSessionInteractionStates(ctx context.Context, channelID uint64, parentMessageIDs []uint64) ([]*ChatAgentSessionInteractionState, error) {
	if len(parentMessageIDs) == 0 {
		return nil, nil
	}
	var rows []chatAgentSessionInteractionRow
	if err := chatAgentSessionInteractionQuery(h.getDBFromContext(ctx)).
		Where("s.channel_id = ? AND s.parent_message_id IN ?", channelID, parentMessageIDs).
		Order("s.id").Scan(&rows).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_chat_agent_session_interaction_states"),
			zap.Uint64(appLogger.KeyChannelID, channelID))
	}
	states := make([]*ChatAgentSessionInteractionState, len(rows))
	for i := range rows {
		state, err := rows[i].state()
		if err != nil {
			return nil, err
		}
		states[i] = state
	}
	return states, nil
}
