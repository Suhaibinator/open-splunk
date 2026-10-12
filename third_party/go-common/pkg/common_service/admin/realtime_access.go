package admin

import (
	"context"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
)

// SetChatRealtimeAccessInvalidator wires process-local access revocation at startup,
// including callers that use the admin handler directly instead of the facade.
func (h *AdminHandler[U]) SetChatRealtimeAccessInvalidator(invalidator ports.ChatRealtimeAccessInvalidator) {
	h.realtimeInvalidator = invalidator
}

func (h *AdminHandler[U]) invalidateRealtimeAfterCommit(ctx context.Context, change ports.ChatRealtimeAccessInvalidation) {
	if h.chatAccessInvalidator != nil {
		h.chatAccessInvalidator(ctx, change)
	}
	ports.InvalidateChatRealtimeAfterCommit(ctx, h.realtimeInvalidator, change)
}

// SetChatAccessCacheInvalidator wires HTTP access revocation independently of realtime.
func (h *AdminHandler[U]) SetChatAccessCacheInvalidator(invalidate func(context.Context, ports.ChatRealtimeAccessInvalidation)) {
	h.chatAccessInvalidator = invalidate
}
