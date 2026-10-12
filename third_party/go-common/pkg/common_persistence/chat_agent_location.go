package common_persistence

import (
	"context"
	"errors"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// withChatLocationTransaction retains channel locks through all checks and writes, including
// callers outside the service's ambient transaction. Never commit a caller-owned transaction.
func (h *CommonPersistenceHandler[U]) withChatLocationTransaction(ctx context.Context, fn func(context.Context, *gorm.DB) error) (result error) {
	defer func() {
		// Preserve unique-violation details so the session writer can classify its conflict.
		if result == nil || appLogger.IsInvariantError(result) || isUniqueViolation(result) {
			return
		}
		result = knownOrChatDatabaseError(result, "chat_location_transaction", ErrDatabaseError, ErrChatAgentSessionScopeInvalid, ErrChatChannelNotFound, ErrChatMessageNotFound, ErrChatAgentSessionNotFound, ErrChatAgentSessionRunActive, ErrChatAgentSessionSuspended, ErrChatMessageAgentSessionRootPinned, ErrChatChannelAgentSessionThreadsRequired, ErrChatChannelOrganizationAccessInvalid, ErrChatAvatarAttachmentInvalid, ErrChatAgentSessionLocationConflict, ErrChatAgentSessionTransactionRequired)
	}()

	if tx, ok := scontext.GetTransaction(ctx); ok && tx != nil {
		return fn(ctx, h.getDBFromContext(ctx))
	}
	return h.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Clone the carrier first: WithTransaction mutates it in place, and the caller must not
		// see this transaction after it finishes.
		txCtx := scontext.CopySRouterContext[uint64, U](ctx, ctx)
		return fn(scontext.WithTransaction[uint64, U](txCtx, &GormTransaction{Tx: tx}), tx)
	})
}

// LockChatChannelForAgentSession serializes location binding against root deletion, thread
// disabling, and organization changes. The caller must retain its transaction until commit.
func (h *CommonPersistenceHandler[U]) LockChatChannelForAgentSession(ctx context.Context, channelID uint64) (*common_persistence_models.ChatChannel, error) {
	if _, err := h.lockChatLocationChannel(ctx, channelID); err != nil {
		return nil, err
	}
	return h.GetChatChannelByID(ctx, channelID)
}

func (h *CommonPersistenceHandler[U]) lockChatLocationChannel(ctx context.Context, channelID uint64) (*common_persistence_models.ChatChannel, error) {
	if tx, ok := scontext.GetTransaction(ctx); !ok || tx == nil {
		return nil, ErrChatAgentSessionTransactionRequired
	}
	var channel common_persistence_models.ChatChannel
	err := h.getDBFromContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", channelID).First(&channel).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrChatChannelNotFound
	}
	if err != nil {
		return nil, chatPersistenceDatabaseError(err, zap.String(logkeys.Stage, "lock_chat_location_channel"), zap.Uint64(appLogger.KeyChannelID, channelID))
	}
	return &channel, nil
}

// GetChatAgentSessionInLocation never falls back to a sibling or main-timeline session.
func (h *CommonPersistenceHandler[U]) GetChatAgentSessionInLocation(ctx context.Context, channelID, parentMessageID uint64) (*common_persistence_models.ChatAgentSession, error) {
	if channelID == 0 {
		return nil, ErrChatAgentSessionNotFound
	}
	return h.getChatAgentSessionWhere(ctx, "get_chat_agent_session_in_location", nil, "channel_id = ? AND parent_message_id = ?", channelID, parentMessageID)
}

// validateAgentSessionLocation runs only while the channel row is locked. Archived sessions
// still constrain organization attribution and permanently retain their location/root.
func (h *CommonPersistenceHandler[U]) validateAgentSessionLocation(ctx context.Context, channel *common_persistence_models.ChatChannel, session *common_persistence_models.ChatAgentSession) error {
	if channel.IsArchived || session.OrganizationID <= 0 || (channel.OrganizationID != nil && *channel.OrganizationID != session.OrganizationID) {
		return ErrChatAgentSessionScopeInvalid
	}
	db := h.getDBFromContext(ctx)
	var incompatible int64
	if err := db.Model(&common_persistence_models.ChatAgentSession{}).Where("channel_id = ? AND (organization_id IS NULL OR organization_id <> ?)", channel.ID, session.OrganizationID).Count(&incompatible).Error; err != nil {
		return err
	}
	if incompatible > 0 {
		return ErrChatAgentSessionScopeInvalid
	}
	if session.ParentMessageID == 0 {
		return nil
	}
	if !channel.Features.Has(common_persistence_models.ChatChannelSupportThreads) {
		return ErrChatAgentSessionScopeInvalid
	}
	var root common_persistence_models.ChatMessage
	if err := db.Where("id = ? AND channel_id = ?", session.ParentMessageID, channel.ID).First(&root).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrChatAgentSessionScopeInvalid
		}
		return err
	}
	if (root.ParentMessageID != nil && *root.ParentMessageID != 0) ||
		(root.Kind != proto_models.MessageKind_MESSAGE_KIND_STANDARD) ||
		(root.HiddenFromUsers != nil && *root.HiddenFromUsers) {
		return ErrChatAgentSessionScopeInvalid
	}
	return nil
}
