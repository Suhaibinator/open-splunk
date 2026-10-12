package common_persistence

import (
	"context"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/lib/pq"
	"go.uber.org/zap"
)

// GetChatMessageReferencedAttachmentIDs includes soft-deleted messages. The result
// is a membership set.
func (h *CommonPersistenceHandler[U]) GetChatMessageReferencedAttachmentIDs(ctx context.Context, ids []uint64) (map[uint64]bool, error) {
	return h.chatReferencedAttachmentIDs(ctx, ids, common_persistence_models.TableNameChatMessages, "check_chat_message_attachment_references")
}

// GetChatHistoryReferencedAttachmentIDs includes all history, even when the
// parent message is soft-deleted. It reads through the ambient transaction so
// newly inserted edit history participates in the retention decision.
func (h *CommonPersistenceHandler[U]) GetChatHistoryReferencedAttachmentIDs(ctx context.Context, ids []uint64) (map[uint64]bool, error) {
	return h.chatReferencedAttachmentIDs(ctx, ids, common_persistence_models.TableNameChatMessageEditHistory, "check_chat_history_attachment_references")
}

func (h *CommonPersistenceHandler[U]) chatReferencedAttachmentIDs(ctx context.Context, ids []uint64, table, stage string) (map[uint64]bool, error) {
	ids = common_util.DeduplicateNonZero(ids)
	result := make(map[uint64]bool, len(ids))
	if len(ids) == 0 {
		return result, nil
	}
	signedIDs := make(pq.Int64Array, len(ids))
	for i, id := range ids {
		signedIDs[i] = int64(id)
	}
	var referenced []int64
	// Keep the original signed bigint ANY comparison and raw (unscoped) table
	// access. One array parameter avoids PostgreSQL's parameter count limit.
	err := h.getDBFromContext(ctx).Raw("SELECT candidate.id FROM unnest(?::bigint[]) AS candidate(id) WHERE EXISTS (SELECT 1 FROM "+table+" WHERE candidate.id = ANY(attachment_ids))", signedIDs).Scan(&referenced).Error
	if err != nil {
		return nil, chatPersistenceDatabaseError(err, zap.String(logkeys.Stage, stage))
	}
	for _, id := range referenced {
		result[uint64(id)] = true
	}
	return result, nil
}
