package common_service

import (
	"context"
	"fmt"
)

func (h *CommonServiceHandler[U]) removedChatAttachmentReferences(ctx context.Context, ids []uint64) (map[uint64]bool, map[uint64]bool, error) {
	live, err := h.commonPersistenceHandler.GetChatMessageReferencedAttachmentIDs(ctx, ids)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to check live chat attachment references: %w", err)
	}
	// Read history even when all IDs have current references: a failed read must
	// still prevent every mutation.
	history, err := h.commonPersistenceHandler.GetChatHistoryReferencedAttachmentIDs(ctx, ids)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to check historical chat attachment references: %w", err)
	}
	return live, history, nil
}
