package common_service

import (
	"context"
	"strconv"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	chatpkg "github.com/Suhaibinator/go-common/pkg/common_service/chat"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/rate_limiter"
)

// ForkChatChannel forks a chat channel for the context actor after the per-person fork rate
// limit (rate_limits.chat_fork_*, sampled by the reloading limiter on each check), since every
// fork copies up to chat_fork_max_copied_messages rows synchronously. Exceeding the limit returns
// ErrRateLimitExceeded before any fork work; a banned actor is rejected before the limit counts. See chat.Handler.ForkChannel.
func (h *CommonServiceHandler[U]) ForkChatChannel(ctx context.Context, input chatpkg.ForkChannelInput) (*common_service_models.ChatChannel, error) {
	if h == nil || h.chatHandler == nil {
		return nil, serviceErrors.ErrChatDisabled
	}
	actor, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found || actor == nil {
		return nil, serviceErrors.ErrUserNotFound
	}
	if (*actor).IsBanned() {
		return nil, serviceErrors.ErrUserBanned
	}
	if err := h.checkRateLimit(ctx, rate_limiter.ActionChatFork, strconv.FormatUint(uint64((*actor).GetID()), 10)); err != nil {
		return nil, err
	}
	return h.chatHandler.ForkChannel(ctx, input)
}
