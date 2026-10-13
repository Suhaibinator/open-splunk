package organization

import (
	"context"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
)

// SetChatRealtimeAccessInvalidator wires process-local access revocation at startup.
func (h *OrganizationHandler[U]) SetChatRealtimeAccessInvalidator(invalidator ports.ChatRealtimeAccessInvalidator) {
	h.realtimeInvalidator = invalidator
}

func (h *OrganizationHandler[U]) invalidateRealtimeAfterCommit(ctx context.Context, invalidation ports.ChatRealtimeAccessInvalidation) {
	ports.InvalidateChatRealtimeAfterCommit(ctx, h.realtimeInvalidator, invalidation)
}
