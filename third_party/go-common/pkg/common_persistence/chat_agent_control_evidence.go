package common_persistence

import (
	"context"
	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
)

// ChatAgentRunControlEvidence is fresh viewer evidence for an interaction projection.
type ChatAgentRunControlEvidence struct {
	HasAdmittedInput  bool
	ToolConfigPayload []byte
}

// GetChatAgentRunControlEvidence batches the viewer's admitted input and captured approval
// configuration for bounded status reads. Each run retains its own captured policy.
func (h *CommonPersistenceHandler[U]) GetChatAgentRunControlEvidence(ctx context.Context, ids []uint64, userID uint64) (map[uint64]ChatAgentRunControlEvidence, error) {
	out := make(map[uint64]ChatAgentRunControlEvidence, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	var rows []struct {
		ID                uint64
		HasAdmittedInput  bool
		ToolConfigPayload []byte
	}
	query := "SELECT r.id, s.tool_config_payload, EXISTS (SELECT 1 FROM " + pm.TableNameChatMessages + " m WHERE m.admitted_run_id = r.id AND m.sender_id = ?) AS has_admitted_input FROM " + pm.TableNameChatAgentRuns + " r LEFT JOIN " + pm.TableNameChatAgentSessionConfigSnapshots + " s ON s.agent_session_id = r.agent_session_id AND s.version = r.agent_session_config_version WHERE r.id IN ?"
	if err := h.getDBFromContext(ctx).Raw(query, userID, ids).Scan(&rows).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err)
	}
	for _, row := range rows {
		out[row.ID] = ChatAgentRunControlEvidence{HasAdmittedInput: row.HasAdmittedInput, ToolConfigPayload: row.ToolConfigPayload}
	}
	return out, nil
}
