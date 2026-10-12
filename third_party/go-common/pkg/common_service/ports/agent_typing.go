package ports

import (
	"context"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
)

// AgentTypingEmitter lets the agent runtime fan out typing indicators for a bot user to clients.
// Implementations should be best-effort and non-blocking.
type AgentTypingEmitter interface {
	StartTyping(ctx context.Context, botUserID common_service_models.UserIdType, channelID common_service_models.ChatChannelIdType) error
	StopTyping(ctx context.Context, botUserID common_service_models.UserIdType, channelID common_service_models.ChatChannelIdType) error
}
