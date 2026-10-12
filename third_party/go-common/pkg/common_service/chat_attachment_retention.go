package common_service

import (
	"context"
	"fmt"
)

// Optional capability keeps custom persistence implementations compatible. The
// built-in persistence uses two set reads, regardless of the attachment count.
type chatAttachmentReferenceBatchReader interface {
	GetChatMessageReferencedAttachmentIDs(context.Context, []uint64) (map[uint64]bool, error)
	GetChatHistoryReferencedAttachmentIDs(context.Context, []uint64) (map[uint64]bool, error)
}

func (h *CommonServiceHandler[U]) removedChatAttachmentReferences(ctx context.Context, ids []uint64) (map[uint64]bool, map[uint64]bool, error) {
	if batch, ok := h.commonPersistenceHandler.(chatAttachmentReferenceBatchReader); ok {
		live, err := batch.GetChatMessageReferencedAttachmentIDs(ctx, ids)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to check live chat attachment references: %w", err)
		}
		// Read history even when all IDs have current references: a failed read must
		// still prevent every mutation, as it does in the singleton fallback.
		history, err := batch.GetChatHistoryReferencedAttachmentIDs(ctx, ids)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to check historical chat attachment references: %w", err)
		}
		return live, history, nil
	}
	live, history := make(map[uint64]bool, len(ids)), make(map[uint64]bool, len(ids))
	for _, id := range ids {
		var err error
		live[id], err = h.commonPersistenceHandler.IsAttachmentReferencedByChatMessages(ctx, id)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to check live chat attachment references: %w", err)
		}
		history[id], err = h.commonPersistenceHandler.IsAttachmentReferencedByChatMessageHistory(ctx, id)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to check historical chat attachment references: %w", err)
		}
	}
	return live, history, nil
}
