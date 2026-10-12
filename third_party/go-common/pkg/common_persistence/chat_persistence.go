package common_persistence

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/lib/pq"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Chat-specific error variables for consistent error handling.
var (
	ErrChatChannelNotFound         = errors.New("chat channel not found")
	ErrChatMessageNotFound         = errors.New("chat message not found")
	ErrChatParticipantNotFound     = errors.New("chat participant not found")
	ErrChatAvatarAttachmentInvalid = errors.New("chat avatar attachment is invalid")
	ErrChatReactionNotFound        = errors.New("chat reaction not found")
	ErrChatReactionConflict        = errors.New("chat reaction conflicts with existing reaction")
	// ErrChatMessageTransactionRequired rejects a client message ID lock outside a transaction,
	// where the lock would be released before the send could write under it.
	ErrChatMessageTransactionRequired     = errors.New("chat message client ID lock requires a transaction")
	ErrChatParticipantTransactionRequired = errors.New("chat participant role lock requires a transaction")
	// ErrChatChannelOrganizationAccessInvalid rejects an org-derived access write that could
	// never grant what it says: a non-positive organization ID, an unknown role, or a read
	// threshold without an organization to evaluate it in.
	ErrChatChannelOrganizationAccessInvalid = errors.New("chat channel organization access is invalid")
	// ErrChatChannelPublicReadInvalid rejects a public read level the enum does not define.
	ErrChatChannelPublicReadInvalid = errors.New("chat channel public read level is invalid")
	// ErrChatChannelArchived rejects a write that the channel's archival freezes, found archived
	// under the write's own channel lock.
	ErrChatChannelArchived = errors.New("chat channel is archived")

	// Agentic transcript persistence errors.
	ErrChatAgentSessionLocationConflict       = errors.New("chat agent session location already occupied")
	ErrChatAgentSessionScopeInvalid           = errors.New("chat agent session scope is invalid")
	ErrChatMessageAgentSessionRootPinned      = errors.New("chat message is the root of an agent session")
	ErrChatChannelAgentSessionThreadsRequired = errors.New("chat channel has thread-bound agent sessions")
	ErrChatAgentSessionTransactionRequired    = errors.New("chat agent session channel lock requires a transaction")
	ErrChatAgentSessionNotFound               = errors.New("chat agent session not found")
	ErrChatAgentSessionConfigNotFound         = errors.New("chat agent session config snapshot not found")
	ErrChatAgentRunNotFound                   = errors.New("chat agent run not found")
	ErrChatAgentSessionRunActive              = errors.New("chat agent session already has an active run")
	ErrChatAgentSessionSuspended              = errors.New("chat agent session is suspended")
	ErrChatAgentRunDriveLeaseLost             = errors.New("chat agent run drive lease lost")
	ErrChatAgentRunStepSealInvalid            = errors.New("chat agent run step seal invalid")
	ErrChatMessageStreamStateConflict         = errors.New("chat message stream state conflict")
	ErrChatAgentSessionPromptInvalid          = errors.New("session system prompt must not be blank")
	ErrChatAgentSessionPromptFinalized        = errors.New("session system prompt already finalized or session missing")
	ErrChatAgentSessionVersionConflict        = errors.New("chat agent session config version conflict")
	ErrChatAgentSessionIdempotencyConflict    = errors.New("chat agent session create idempotency conflict")
	ErrChatAgentSessionScopedConflict         = errors.New("chat agent session scoped context conflict")

	// ErrChatToolResultOrphan means a TOOL_RESULT was recorded without an existing paired TOOL_CALL
	// for the same (agent_run_id, tool_call_id).
	ErrChatToolResultOrphan = errors.New("chat tool result has no paired tool call")
	// ErrChatToolCallConflict means a transcript tool row already exists for the same
	// (agent_run_id, tool_call_id, kind) with a different immutable payload.
	ErrChatToolCallConflict = errors.New("chat tool call payload conflict")
	// ErrChatTranscriptRunMismatch means a transcript write referenced a ChatAgentRun that does not
	// belong to the same channel/thread/bot context as the row being written (Decision 5 authority).
	// The internal writer validator rejects the write rather than persisting a cross-context row.
	ErrChatTranscriptRunMismatch = errors.New("chat transcript run/context mismatch")
	// ErrChatTranscriptPayloadInvalid means the transcript row's side-table payload metadata is not
	// self-consistent with its kind, payload_type, model_payload, or denormalized tool_call_id.
	ErrChatTranscriptPayloadInvalid = errors.New("chat transcript payload invalid")
	// ErrChatTranscriptBroadcastInvalid means a transcript write asked to broadcast a row that is
	// not a thread reply. Only a row with a thread parent can also appear on the main timeline.
	ErrChatTranscriptBroadcastInvalid = errors.New("chat transcript broadcast requires a thread parent")
	// ErrChatToolCallLeaseLost means a fenced execution attempted to commit a TOOL_RESULT without
	// still owning the paired TOOL_CALL's live execution lease. No result row is persisted.
	ErrChatToolCallLeaseLost = errors.New("chat tool call execution lease lost")
)

// chatPersistenceDatabaseError preserves the underlying database failure for
// errors.Is/errors.As inspection while adding event-specific context for the
// single owning log record at the service or HTTP boundary.
func chatPersistenceDatabaseError(cause error, fields ...zap.Field) error {
	return appLogger.WithErrorFields(errors.Join(ErrDatabaseError, cause), fields...)
}

// knownOrChatDatabaseError returns err unchanged when it matches a known sentinel and otherwise
// classifies it as a database error at stage.
func knownOrChatDatabaseError(err error, stage string, known ...error) error {
	if err == nil {
		return nil
	}
	for _, sentinel := range known {
		if errors.Is(err, sentinel) {
			return err
		}
	}
	return chatPersistenceDatabaseError(err, zap.String(logkeys.Stage, stage))
}

// publicChatAvatarScope is the single database predicate for attachments that
// may be exposed through a public chat channel. Keep the deleted_at predicate
// explicit: admin channel reads can be Unscoped to include deleted channels,
// but that must never make a deleted attachment publicly hydratable.
func publicChatAvatarScope(db *gorm.DB) *gorm.DB {
	attachmentTable := common_persistence_models.TableNameAttachments
	return db.Where(
		attachmentTable+".internal_only = ? AND "+
			attachmentTable+".upload_status = ? AND "+
			attachmentTable+".visibility = ? AND "+
			attachmentTable+".deleted_at IS NULL AND "+
			attachmentTable+".final_object_name <> ?",
		false,
		common_persistence_models.UploadStatusVetted,
		proto_models.AttachmentVisibility_ATTACHMENT_VISIBILITY_PUBLIC,
		"",
	).
		Where(attachmentTable+".object_type = ?", common_persistence_models.ObjectTypeProfilePicture).
		Where("LOWER(TRIM("+attachmentTable+".content_type)) IN ?", common_persistence_models.ProfilePictureAllowedMIMETypes())
}

func preloadPublicChatAvatar(db *gorm.DB) *gorm.DB {
	return db.Preload(common_persistence_models.PreloadAvatarAttachment, publicChatAvatarScope)
}

// sanitizeChatChannelAvatar removes a legacy or otherwise invalid foreign key
// when the guarded preload did not hydrate it. This prevents both the numeric
// attachment ID and its object path from crossing the persistence boundary.
func sanitizeChatChannelAvatar(channel *common_persistence_models.ChatChannel) {
	if channel != nil && channel.AvatarAttachmentID != nil && channel.AvatarAttachment == nil {
		channel.AvatarAttachmentID = nil
	}
}

// hydrateChatChannelAvatar loads the avatar of a channel row written with RETURNING, which
// fills only the channel's own columns, through the public avatar scope; an avatar the
// scope rejects is dropped from the row.
func hydrateChatChannelAvatar(db *gorm.DB, channel *common_persistence_models.ChatChannel) error {
	if channel.AvatarAttachmentID == nil {
		return nil
	}
	var avatar common_persistence_models.Attachment
	err := publicChatAvatarScope(db).
		Where(common_persistence_models.TableNameAttachments+".id = ?", *channel.AvatarAttachmentID).
		First(&avatar).Error
	switch {
	case err == nil:
		channel.AvatarAttachment = &avatar
	case errors.Is(err, gorm.ErrRecordNotFound):
		sanitizeChatChannelAvatar(channel)
	default:
		return chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "load_chat_channel_avatar_attachment"),
			zap.Uint64(appLogger.KeyChannelID, channel.ID),
			zap.Uint64(appLogger.KeyAttachmentID, *channel.AvatarAttachmentID))
	}
	return nil
}

func queryPublicChatAvatarForUpdate(db *gorm.DB, attachmentID uint64, ownerUserID *uint64) (*common_persistence_models.Attachment, error) {
	var avatar common_persistence_models.Attachment
	query := publicChatAvatarScope(db).
		Clauses(clause.Locking{Strength: "SHARE"}).
		Where(common_persistence_models.TableNameAttachments+".id = ?", attachmentID)
	if ownerUserID != nil {
		query = query.Where(common_persistence_models.TableNameAttachments+".user_id = ?", *ownerUserID)
	}
	if err := query.First(&avatar).Error; err != nil {
		return nil, err
	}
	return &avatar, nil
}

// loadPublicChatAvatarForUpdate is retained for admin persistence callers that do not carry the
// chat handler logger. Chat mutations use the receiver method below so genuine query failures keep
// their database cause in the persistence log.
func loadPublicChatAvatarForUpdate(db *gorm.DB, attachmentID uint64, ownerUserID *uint64) (*common_persistence_models.Attachment, error) {
	avatar, err := queryPublicChatAvatarForUpdate(db, attachmentID, ownerUserID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrChatAvatarAttachmentInvalid
	}
	if err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "validate_chat_avatar_attachment"),
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID))
	}
	return avatar, nil
}

func (h *CommonPersistenceHandler[U]) loadPublicChatAvatarForUpdate(ctx context.Context, db *gorm.DB, attachmentID uint64, ownerUserID *uint64) (*common_persistence_models.Attachment, error) {
	db = db.WithContext(ctx)
	avatar, err := queryPublicChatAvatarForUpdate(db, attachmentID, ownerUserID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrChatAvatarAttachmentInvalid
	}
	if err != nil {
		fields := []zap.Field{
			zap.String(logkeys.Stage, "validate_chat_avatar_attachment"),
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
		}
		if ownerUserID != nil {
			fields = append(fields, zap.Uint64(appLogger.KeyUserID, *ownerUserID))
		}
		return nil, chatPersistenceDatabaseError(err, fields...)
	}
	return avatar, nil
}

// withChatAvatarWriteTransaction reuses request transactions and otherwise
// owns a local transaction. The local transaction keeps the attachment's SHARE
// lock alive through the channel UPDATE and any readback.
func (h *CommonPersistenceHandler[U]) withChatAvatarWriteTransaction(
	ctx context.Context,
	fn func(context.Context, *gorm.DB) error,
) error {
	if transaction, ok := scontext.GetTransaction(ctx); ok && transaction != nil {
		return fn(ctx, transaction.GetDB())
	}

	tx := h.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return chatPersistenceDatabaseError(tx.Error,
			zap.String(logkeys.Stage, "begin_chat_avatar_update_transaction"))
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback().Error
		}
	}()
	// Clone the carrier first: WithTransaction mutates it in place, and the caller must not see
	// this transaction after it finishes.
	txCtx := scontext.CopySRouterContext[uint64, U](ctx, ctx)
	txCtx = scontext.WithTransaction[uint64, U](txCtx, &GormTransaction{Tx: tx})
	if err := fn(txCtx, tx); err != nil {
		return err
	}
	if err := tx.Commit().Error; err != nil {
		return chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "commit_chat_avatar_update_transaction"))
	}
	committed = true
	return nil
}

func normalizeChatChannelPreview(content string) string {
	preview := strings.TrimSpace(content)
	if len(preview) <= 255 {
		return preview
	}
	runes := []rune(preview)
	if len(runes) <= 255 {
		return preview
	}
	return string(runes[:255])
}

// chatPreviewMessageKinds are the message kinds eligible to drive a channel's
// last_message_at/last_message_preview/last_message_sender_id: explicitly STANDARD rows.
// Agent/system transcript kinds never own the preview.
var chatPreviewMessageKinds = []proto_models.MessageKind{
	proto_models.MessageKind_MESSAGE_KIND_STANDARD,
}

// refreshChatChannelLastMessage recomputes a channel's last_message_at/last_message_preview/
// last_message_sender_id from the newest visible preview-eligible message, clearing all three
// when none remain. It runs on the caller's db handle so the recompute commits atomically with
// the mutation that caused it.
func (h *CommonPersistenceHandler[U]) refreshChatChannelLastMessage(ctx context.Context, db *gorm.DB, channelID uint64) error {
	db = db.WithContext(ctx)
	var latest common_persistence_models.ChatMessage
	err := db.
		Where("channel_id = ?", channelID).
		Where("kind IN ?", chatPreviewMessageKinds).
		Where("hidden_from_users IS NOT TRUE").
		Order("id DESC").
		Take(&latest).Error

	updates := common_persistence_models.ChatChannel{}
	switch {
	case err == nil:
		preview := normalizeChatChannelPreview(latest.Content)
		updates.LastMessageAt = &latest.CreatedAt
		updates.LastMessagePreview = &preview
		updates.LastMessageSenderID = chatChannelLastMessageSender(latest.SenderID)
	case errors.Is(err, gorm.ErrRecordNotFound):
		// No visible message remains; every field is cleared.
	default:
		return chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "load_latest_chat_message_for_preview_refresh"),
			zap.Uint64(appLogger.KeyChannelID, channelID))
	}

	if err := db.Model(&common_persistence_models.ChatChannel{}).
		Where("id = ?", channelID).
		Select("last_message_at", "last_message_preview", "last_message_sender_id").Updates(&updates).Error; err != nil {
		return chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "refresh_chat_channel_last_message_fields"),
			zap.Uint64(appLogger.KeyChannelID, channelID))
	}
	return nil
}

// refreshChatChannelPreviewIfLatest rewrites the stored channel preview to the given content
// when messageID is the channel's newest visible preview-eligible message; edits to older
// messages leave the preview untouched. The sender is rewritten with it so the pair always
// describes the same message.
func (h *CommonPersistenceHandler[U]) refreshChatChannelPreviewIfLatest(ctx context.Context, db *gorm.DB, channelID uint64, messageID uint64, content string) error {
	db = db.WithContext(ctx)
	var latest []common_persistence_models.ChatMessage
	if err := db.Model(&common_persistence_models.ChatMessage{}).
		Select("id", "sender_id").
		Where("channel_id = ?", channelID).
		Where("kind IN ?", chatPreviewMessageKinds).
		Where("hidden_from_users IS NOT TRUE").
		Order("id DESC").
		Limit(1).
		Find(&latest).Error; err != nil {
		return chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "resolve_latest_chat_message_for_preview_refresh"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Uint64(appLogger.KeyMessageID, messageID))
	}
	if len(latest) == 0 || latest[0].ID != messageID {
		return nil
	}

	preview := normalizeChatChannelPreview(content)
	if err := db.Model(&common_persistence_models.ChatChannel{}).
		Where("id = ?", channelID).
		Select("last_message_preview", "last_message_sender_id").
		Updates(common_persistence_models.ChatChannel{LastMessagePreview: &preview, LastMessageSenderID: chatChannelLastMessageSender(latest[0].SenderID)}).Error; err != nil {
		return chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "update_chat_channel_preview_after_edit"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Uint64(appLogger.KeyMessageID, messageID))
	}
	return nil
}

func buildChatParticipantRejoinAssignments(now time.Time) clause.Set {
	return clause.Set{
		{Column: clause.Column{Name: "joined_at"}, Value: now},
		{Column: clause.Column{Name: "left_at"}, Value: (*time.Time)(nil)},
		{Column: clause.Column{Name: "notification_join_epoch"}, Value: gorm.Expr("EXCLUDED.notification_join_epoch")},
		{Column: clause.Column{Name: "notification_join_system_identifier"}, Value: gorm.Expr("EXCLUDED.notification_join_system_identifier")},
		{Column: clause.Column{Name: "notification_join_xid"}, Value: gorm.Expr("EXCLUDED.notification_join_xid")},
		{Column: clause.Column{Name: "role"}, Value: proto_models.ChannelRole_CHANNEL_ROLE_MEMBER},
		{Column: clause.Column{Name: "updated_at"}, Value: now},
	}
}

// buildChatParticipantRejoinOnConflict builds the upsert clause used when
// (re-)adding channel participants. The reactivation assignments (resetting
// role to MEMBER and refreshing joined_at) must only apply to rows that were
// previously soft-left (left_at IS NOT NULL). Without the WHERE guard, re-adding
// a participant who is still active — e.g. an admin re-inviting a list that
// includes the channel owner — would silently demote them to MEMBER and reset
// their join metadata.
func buildChatParticipantRejoinOnConflict(now time.Time) clause.OnConflict {
	return clause.OnConflict{
		Columns:   []clause.Column{{Name: "channel_id"}, {Name: "user_id"}},
		DoUpdates: buildChatParticipantRejoinAssignments(now),
		Where: clause.Where{Exprs: []clause.Expression{
			clause.Expr{SQL: common_persistence_models.TableNameChatChannelParticipants + ".left_at IS NOT NULL"},
		}},
	}
}

// ============================================================================
// CHAT: CHANNELS
// ============================================================================

// CreateChatChannel executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) CreateChatChannel(
	ctx context.Context,
	createdByUserID uint64,
	name string,
	description *string,
	contextID *uint64,
	contextDisplayName *string,
	contextEntityID *uint64,
	channelType proto_models.ChannelType,
	participantUserIDs []uint64,
	featureFlags common_persistence_models.ChatChannelFeatureFlags,
) (*common_persistence_models.ChatChannel, error) {
	db := h.getDBFromContext(ctx)

	channel := &common_persistence_models.ChatChannel{
		Name:               name,
		Description:        description,
		ContextID:          contextID,
		ContextDisplayName: contextDisplayName,
		ContextEntityID:    contextEntityID,
		ChannelType:        channelType,
		IsArchived:         false,
		CreatedByUserID:    createdByUserID,
		Features:           featureFlags.Features,
		ForkRules:          featureFlags.ForkRules,
	}

	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(channel).Error; err != nil {
			return chatPersistenceDatabaseError(err,
				zap.String(logkeys.Stage, "create_chat_channel"),
				zap.Uint64(appLogger.KeyUserID, createdByUserID),
				zap.Uint8(appLogger.KeyChannelType, uint8(channelType)))
		}

		// GORM replaces a false bool with its declared default on Create. Apply an
		// explicit opt-out inside the same transaction before exposing the channel.
		if featureFlags.UnreadNotificationsEnabled != nil && !*featureFlags.UnreadNotificationsEnabled {
			if err := tx.Model(channel).Update("unread_notifications_enabled", false).Error; err != nil {
				return chatPersistenceDatabaseError(err, zap.String(logkeys.Stage, "create_chat_channel_notification_setting"))
			}
			channel.UnreadNotificationsEnabled = false
		}

		unique := make(map[uint64]struct{}, len(participantUserIDs)+1)
		unique[createdByUserID] = struct{}{}
		for _, id := range participantUserIDs {
			unique[id] = struct{}{}
		}

		now := time.Now()
		participants := make([]common_persistence_models.ChatChannelParticipant, 0, len(unique))
		for id := range unique {
			role := proto_models.ChannelRole_CHANNEL_ROLE_MEMBER
			if id == createdByUserID {
				role = proto_models.ChannelRole_CHANNEL_ROLE_OWNER
			}
			participants = append(participants, common_persistence_models.ChatChannelParticipant{
				ChannelID: channel.ID,
				UserID:    id,
				Role:      role,
				JoinedAt:  now,
				LeftAt:    nil,
			})
		}

		if len(participants) > 0 {
			if err := h.createStampedChatParticipants(tx, participants, nil); err != nil {
				return chatPersistenceDatabaseError(err,
					zap.String(logkeys.Stage, "create_chat_channel_participants"),
					zap.Uint64(appLogger.KeyChannelID, channel.ID),
					zap.Int(appLogger.KeyParticipantCount, len(participants)))
			}
		}

		channel.Participants = participants
		return nil
	})
	if err != nil {
		return nil, err
	}

	// The created rows are the channel's complete state (a new channel has no
	// avatar attachment), so no read-back is needed. Participant User relations
	// are not hydrated; callers needing user details fetch them explicitly.
	return channel, nil
}

// GetChatChannelByID executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetChatChannelByID(ctx context.Context, channelID uint64) (*common_persistence_models.ChatChannel, error) {
	db := h.getDBFromContext(ctx)

	var channel common_persistence_models.ChatChannel
	err := preloadPublicChatAvatar(db).
		Preload(common_persistence_models.PreloadParticipants, "left_at IS NULL").
		Preload(common_persistence_models.PreloadParticipantsUser).
		Where("id = ?", channelID).
		First(&channel).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrChatChannelNotFound
		}
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "get_chat_channel"),
			zap.Uint64(appLogger.KeyChannelID, channelID))
	}
	sanitizeChatChannelAvatar(&channel)
	return &channel, nil
}

// GetChatChannelFeatures reads only a live channel's feature bitmask.
func (h *CommonPersistenceHandler[U]) GetChatChannelFeatures(ctx context.Context, channelID uint64) (common_persistence_models.ChatChannelFeatures, error) {
	var channel common_persistence_models.ChatChannel
	err := h.getDBFromContext(ctx).Select("features").Where("id = ?", channelID).First(&channel).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, ErrChatChannelNotFound
	}
	if err != nil {
		return 0, chatPersistenceDatabaseError(err, zap.String(logkeys.Stage, "get_chat_channel_features"), zap.Uint64(appLogger.KeyChannelID, channelID))
	}
	return channel.Features, nil
}

// ChatChannelAccess bundles the channel-scoped data required for permission
// checks: the channel itself (without the participant roster), the requesting
// user's active participant row (nil when they are not an active member), and
// the user IDs of active bot participants.
type ChatChannelAccess struct {
	Channel     *common_persistence_models.ChatChannel
	Participant *common_persistence_models.ChatChannelParticipant
	BotUserIDs  []uint64
}

// GetChatChannelAccess executes its operation in the persistence flow between service handlers and the database layer.
// It loads the data needed for channel permission checks without hydrating the
// full participant roster: the channel (with avatar attachment preloaded), the
// requesting user's active participant row, and the active bot participant user
// IDs. Unlike GetChatChannelByID this stays cheap on large channels because the
// membership lookup is a primary-key probe and the bot scan is filtered to bot
// rows in SQL.
func (h *CommonPersistenceHandler[U]) GetChatChannelAccess(ctx context.Context, channelID uint64, userID uint64) (*ChatChannelAccess, error) {
	db := h.getDBFromContext(ctx)

	var channel common_persistence_models.ChatChannel
	err := preloadPublicChatAvatar(db).
		Where("id = ?", channelID).
		First(&channel).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrChatChannelNotFound
		}
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "get_chat_channel_for_access_check"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Uint64(appLogger.KeyUserID, userID))
	}
	sanitizeChatChannelAvatar(&channel)

	access := &ChatChannelAccess{Channel: &channel}

	var participant common_persistence_models.ChatChannelParticipant
	err = db.
		Where("channel_id = ? AND user_id = ? AND left_at IS NULL", channelID, userID).
		First(&participant).Error
	switch {
	case err == nil:
		access.Participant = &participant
	case errors.Is(err, gorm.ErrRecordNotFound):
		// Not an active member; leave Participant nil so the service layer can
		// distinguish "channel exists but user is not a member".
	default:
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "get_chat_channel_membership"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Uint64(appLogger.KeyUserID, userID))
	}

	if err := db.
		Table(common_persistence_models.TableNameChatChannelParticipants+" p").
		Joins("JOIN "+common_persistence_models.TableNameUsers+" u ON u.id = p.user_id AND u.deleted_at IS NULL").
		Where("p.channel_id = ? AND p.left_at IS NULL AND u.is_bot = true", channelID).
		Pluck("p.user_id", &access.BotUserIDs).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_chat_channel_bot_participants"),
			zap.Uint64(appLogger.KeyChannelID, channelID))
	}

	return access, nil
}

// FindChatChannelByExactActiveMembers executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) FindChatChannelByExactActiveMembers(
	ctx context.Context,
	memberUserIDs []uint64,
) (*common_persistence_models.ChatChannel, error) {
	db := h.getDBFromContext(ctx)

	if len(memberUserIDs) == 0 {
		return nil, ErrChatChannelNotFound
	}

	// Exact active participant tuple match.
	// Strategy:
	// - Restrict to channels one of the members already belongs to (indexed on
	//   (user_id, left_at)), so the aggregate scans one user's channels instead
	//   of every active participant row in the system
	// - Filter participants to active rows
	// - Group by channel_id
	// - Ensure total active participant count equals len(memberUserIDs)
	// - Ensure all participant user_ids are within the provided set
	// This avoids leaking channels from cross-channel membership lookups.
	var channelID uint64
	q := db.
		Table(common_persistence_models.TableNameChatChannelParticipants+" p").
		Select("p.channel_id").
		Joins("JOIN "+common_persistence_models.TableNameChatChannels+" c ON c.id = p.channel_id AND c.deleted_at IS NULL").
		Where("p.channel_id IN (SELECT channel_id FROM "+common_persistence_models.TableNameChatChannelParticipants+" WHERE user_id = ? AND left_at IS NULL)", memberUserIDs[0]).
		Where("p.left_at IS NULL").
		Group("p.channel_id").
		Having("COUNT(*) = ?", len(memberUserIDs)).
		Having("SUM(CASE WHEN p.user_id IN ? THEN 1 ELSE 0 END) = ?", memberUserIDs, len(memberUserIDs)).
		Limit(1)

	if err := q.Scan(&channelID).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "find_chat_channel_by_member_tuple"),
			zap.Int(appLogger.KeyMemberCount, len(memberUserIDs)))
	}
	if channelID == 0 {
		return nil, ErrChatChannelNotFound
	}

	return h.GetChatChannelByID(ctx, channelID)
}

// AcquireChatChannelMemberTupleLock takes a transaction-scoped Postgres advisory lock keyed by a
// canonical member tuple. The lock is released automatically when the surrounding transaction
// commits or rolls back, so get-or-create for the same tuple is serialized across requests (and
// replicas) until the winner's channel is visible to the next Find.
// Parameters:
//   - ctx: context.Context carrying the request transaction the lock binds to.
//   - key: canonical member tuple key (sorted, comma-separated user IDs).
func (h *CommonPersistenceHandler[U]) AcquireChatChannelMemberTupleLock(ctx context.Context, key string) error {
	db := h.getDBFromContext(ctx)
	if err := db.Exec("SELECT pg_advisory_xact_lock(hashtext(?))", key).Error; err != nil {
		return chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "acquire_chat_member_tuple_lock"))
	}
	return nil
}

// ListChatChannelsForUser executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) ListChatChannelsForUser(
	ctx context.Context,
	userID uint64,
	pagination *proto_models.PaginationRequest,
	includeArchived bool,
) ([]*common_persistence_models.ChatChannel, *proto_models.PaginationResponse, error) {
	db := h.getDBFromContext(ctx)

	if err := requirePageLimit("list_chat_channels_for_user", pagination); err != nil {
		return nil, nil, err
	}
	page, limit := pagination.GetPage(), pagination.GetLimit()
	offset := common_util.PaginationOffset(page, limit)

	query := db.Model(&common_persistence_models.ChatChannel{}).
		Joins("JOIN "+common_persistence_models.TableNameChatChannelParticipants+" p ON p.channel_id = "+common_persistence_models.TableNameChatChannels+".id").
		Where("p.user_id = ? AND p.left_at IS NULL", userID)

	if !includeArchived {
		query = query.Where(common_persistence_models.TableNameChatChannels + ".is_archived = false")
	}

	includeTotalCount := common_util.ShouldIncludeTotalCount(pagination)
	var totalCount int64
	if includeTotalCount {
		if err := query.Count(&totalCount).Error; err != nil {
			return nil, nil, chatPersistenceDatabaseError(err,
				zap.String(logkeys.Stage, "count_chat_channels"),
				zap.Uint64(appLogger.KeyUserID, userID),
				zap.Bool(appLogger.KeyIncludeArchived, includeArchived))
		}
	}

	var channels []*common_persistence_models.ChatChannel
	if err := preloadPublicChatAvatar(query).
		Order(common_persistence_models.TableNameChatChannels + ".last_message_at DESC NULLS LAST, " + common_persistence_models.TableNameChatChannels + ".created_at DESC").
		Offset(offset).
		Limit(common_util.PaginationQueryLimit(limit, includeTotalCount)).
		Find(&channels).Error; err != nil {
		return nil, nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_chat_channels"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Uint32(appLogger.KeyPage, page),
			zap.Uint32(logkeys.Limit, limit),
			zap.Bool(appLogger.KeyIncludeArchived, includeArchived))
	}
	channels, hasNextPage := common_util.TrimPaginationProbe(channels, limit, includeTotalCount)
	for _, channel := range channels {
		sanitizeChatChannelAvatar(channel)
	}

	var paginationResponse *proto_models.PaginationResponse
	if includeTotalCount {
		paginationResponse = common_util.CreatePaginationResponse(totalCount, limit, page)
	} else {
		paginationResponse = common_util.CreatePaginationResponseWithoutTotalCount(limit, page, hasNextPage)
	}

	return channels, paginationResponse, nil
}

// UpdateChatChannel executes its operation in the persistence flow between service handlers and the database layer.
// Parameters:
//   - ctx: context.Context input used by UpdateChatChannel.
//   - channelID: uint64 input used by UpdateChatChannel.
//   - actorUserID: uint64 identifies the user who must own a requested avatar.
//   - name: *string input used by UpdateChatChannel.
//   - description: *string input used by UpdateChatChannel.
//   - avatarAttachmentID: *uint64 input used by UpdateChatChannel.
//   - isArchived: *bool input used by UpdateChatChannel.
//   - unreadNotificationsEnabled: *bool sets the unread notification policy; nil leaves it unchanged.
func (h *CommonPersistenceHandler[U]) UpdateChatChannel(
	ctx context.Context,
	channelID uint64,
	actorUserID uint64,
	name *string,
	description *string,
	avatarAttachmentID *uint64,
	isArchived *bool,
	unreadNotificationsEnabled *bool,
) (*common_persistence_models.ChatChannel, error) {
	if avatarAttachmentID == nil {
		return h.updateChatChannelWithDB(ctx, h.getDBFromContext(ctx), channelID, actorUserID, name, description, nil, isArchived, unreadNotificationsEnabled)
	}

	var channel *common_persistence_models.ChatChannel
	err := h.withChatAvatarWriteTransaction(ctx, func(txCtx context.Context, tx *gorm.DB) error {
		var updateErr error
		channel, updateErr = h.updateChatChannelWithDB(txCtx, tx, channelID, actorUserID, name, description, avatarAttachmentID, isArchived, unreadNotificationsEnabled)
		return updateErr
	})
	if err != nil {
		return nil, err
	}
	return channel, nil
}

func (h *CommonPersistenceHandler[U]) updateChatChannelWithDB(
	ctx context.Context,
	db *gorm.DB,
	channelID uint64,
	actorUserID uint64,
	name *string,
	description *string,
	avatarAttachmentID *uint64,
	isArchived *bool,
	unreadEnabled *bool,
) (*common_persistence_models.ChatChannel, error) {

	updates := common_persistence_models.ChatChannel{}
	var columns []string
	if unreadEnabled != nil {
		updates.UnreadNotificationsEnabled = *unreadEnabled
		columns = append(columns, "unread_notifications_enabled")
	}
	var requestedAvatar *common_persistence_models.Attachment
	if name != nil {
		updates.Name = *name
		columns = append(columns, "name")
	}
	if description != nil {
		updates.Description = description
		columns = append(columns, "description")
	}
	if avatarAttachmentID != nil {
		if *avatarAttachmentID == 0 {
			// A present zero value is the request-level clear signal.
			updates.AvatarAttachmentID = nil
			columns = append(columns, "avatar_attachment_id")
		} else {
			var err error
			requestedAvatar, err = h.loadPublicChatAvatarForUpdate(ctx, db, *avatarAttachmentID, &actorUserID)
			if err != nil {
				return nil, err
			}
			updates.AvatarAttachmentID = avatarAttachmentID
			columns = append(columns, "avatar_attachment_id")
		}
	}
	if isArchived != nil {
		updates.IsArchived = *isArchived
		columns = append(columns, "is_archived")
	}

	if len(columns) == 0 {
		return h.GetChatChannelByID(ctx, channelID)
	}

	// RETURNING captures the post-update row, avoiding a read-back that would
	// also hydrate the full participant roster.
	var channel common_persistence_models.ChatChannel
	result := db.Model(&channel).Clauses(clause.Returning{}).Where("id = ?", channelID).Select(columns).Updates(&updates)
	if result.Error != nil {
		return nil, chatPersistenceDatabaseError(result.Error,
			zap.String(logkeys.Stage, "update_chat_channel"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Uint64(appLogger.KeyUserID, actorUserID))
	}
	if result.RowsAffected == 0 {
		return nil, ErrChatChannelNotFound
	}

	if channel.AvatarAttachmentID != nil && requestedAvatar != nil && requestedAvatar.ID == *channel.AvatarAttachmentID {
		channel.AvatarAttachment = requestedAvatar
		return &channel, nil
	}
	if err := hydrateChatChannelAvatar(db, &channel); err != nil {
		return nil, err
	}
	return &channel, nil
}

// LeaveChatChannel executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) LeaveChatChannel(ctx context.Context, channelID uint64, userID uint64) error {
	db := h.getDBFromContext(ctx)

	now := time.Now()
	result := db.Model(&common_persistence_models.ChatChannelParticipant{}).
		Where("channel_id = ? AND user_id = ? AND left_at IS NULL", channelID, userID).
		Update("left_at", &now)
	if result.Error != nil {
		return chatPersistenceDatabaseError(result.Error,
			zap.String(logkeys.Stage, "leave_chat_channel"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Uint64(appLogger.KeyUserID, userID))
	}
	if result.RowsAffected == 0 {
		return ErrChatParticipantNotFound
	}
	return nil
}

// ============================================================================
// CHAT: PARTICIPANTS
// ============================================================================

// AddChatChannelParticipants executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AddChatChannelParticipants(ctx context.Context, channelID uint64, userIDs []uint64) ([]*common_persistence_models.ChatChannelParticipant, error) {
	db := h.getDBFromContext(ctx)

	now := time.Now()
	participants := make([]common_persistence_models.ChatChannelParticipant, 0, len(userIDs))
	for _, id := range userIDs {
		participants = append(participants, common_persistence_models.ChatChannelParticipant{
			ChannelID: channelID,
			UserID:    id,
			Role:      proto_models.ChannelRole_CHANNEL_ROLE_MEMBER,
			JoinedAt:  now,
			LeftAt:    nil,
		})
	}

	if len(participants) == 0 {
		return []*common_persistence_models.ChatChannelParticipant{}, nil
	}

	// Upsert to allow re-joining with fresh membership metadata. The WHERE guard
	// ensures active members (e.g. the channel owner) are not demoted when re-added.
	conflict := buildChatParticipantRejoinOnConflict(now)
	if err := h.createStampedChatParticipants(db, participants, &conflict); err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "add_chat_participants"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Int(appLogger.KeyParticipantCount, len(participants)))
	}

	// Return hydrated participants (active only).
	var out []*common_persistence_models.ChatChannelParticipant
	if err := db.
		Preload(common_persistence_models.PreloadUser).
		Where("channel_id = ? AND user_id IN ? AND left_at IS NULL", channelID, userIDs).
		Find(&out).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "load_added_chat_participants"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Int(appLogger.KeyParticipantCount, len(userIDs)))
	}
	return out, nil
}

// RemoveChatChannelParticipant executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) RemoveChatChannelParticipant(ctx context.Context, channelID uint64, userID uint64) error {
	return h.LeaveChatChannel(ctx, channelID, userID)
}

// UpdateChatChannelParticipantRole executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) UpdateChatChannelParticipantRole(ctx context.Context, channelID uint64, userID uint64, role proto_models.ChannelRole) (*common_persistence_models.ChatChannelParticipant, error) {
	db := h.getDBFromContext(ctx)

	result := db.Model(&common_persistence_models.ChatChannelParticipant{}).
		Where("channel_id = ? AND user_id = ? AND left_at IS NULL", channelID, userID).
		Update("role", role)
	if result.Error != nil {
		return nil, chatPersistenceDatabaseError(result.Error,
			zap.String(logkeys.Stage, "update_chat_participant_role"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Uint8(appLogger.KeyRole, uint8(role)))
	}
	if result.RowsAffected == 0 {
		return nil, ErrChatParticipantNotFound
	}

	var participant common_persistence_models.ChatChannelParticipant
	if err := db.Preload(common_persistence_models.PreloadUser).Where("channel_id = ? AND user_id = ?", channelID, userID).First(&participant).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "load_chat_participant_after_role_update"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Uint64(appLogger.KeyUserID, userID))
	}
	return &participant, nil
}

type chatParticipantRoleLockKey struct{}

// WithChatParticipantRoleLock requests a target-role row lock for participant management.
// The caller must provide an ambient transaction that also performs the subsequent mutation.
func WithChatParticipantRoleLock(ctx context.Context) context.Context {
	return context.WithValue(ctx, chatParticipantRoleLockKey{}, true)
}

// GetChatChannelParticipant returns an active participant with the same user
// hydration as roster reads, using the (channel_id, user_id) primary key.
func (h *CommonPersistenceHandler[U]) GetChatChannelParticipant(ctx context.Context, channelID uint64, userID uint64) (*common_persistence_models.ChatChannelParticipant, error) {
	var participant common_persistence_models.ChatChannelParticipant
	db := h.getDBFromContext(ctx)
	if lock, _ := ctx.Value(chatParticipantRoleLockKey{}).(bool); lock {
		if tx, exists := scontext.GetTransaction(ctx); !exists || tx == nil {
			return nil, ErrChatParticipantTransactionRequired
		}
		// Participant-management decisions retain the target role until their write commits.
		db = db.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	err := db.
		Preload(common_persistence_models.PreloadUser).
		Preload(common_persistence_models.PreloadUserProfilePictureAttachment).
		Where("channel_id = ? AND user_id = ? AND left_at IS NULL", channelID, userID).
		Take(&participant).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrChatParticipantNotFound
	}
	if err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "get_chat_channel_participant"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Uint64(appLogger.KeyUserID, userID))
	}
	return &participant, nil
}

// UpdateChatChannelParticipantPreferences changes only differing values, so
// repeated saves do not change UpdatedAt or emit duplicate preference events.
func (h *CommonPersistenceHandler[U]) UpdateChatChannelParticipantPreferences(
	ctx context.Context,
	channelID uint64,
	userID uint64,
	notificationMode *proto_models.ChannelNotificationMode,
	isPinned *bool,
) (*common_persistence_models.ChatChannelParticipant, bool, error) {
	db := h.getDBFromContext(ctx)
	updates := common_persistence_models.ChatChannelParticipant{}
	var columns []string
	var changedConditions []clause.Expression
	if notificationMode != nil {
		updates.NotificationMode = *notificationMode
		columns = append(columns, "notification_mode")
		changedConditions = append(changedConditions, clause.Neq{Column: "notification_mode", Value: *notificationMode})
	}
	if isPinned != nil {
		updates.IsPinned = *isPinned
		columns = append(columns, "is_pinned")
		changedConditions = append(changedConditions, clause.Neq{Column: "is_pinned", Value: *isPinned})
	}
	changed := false
	if len(columns) > 0 {
		result := db.Model(&common_persistence_models.ChatChannelParticipant{}).
			Where("channel_id = ? AND user_id = ? AND left_at IS NULL", channelID, userID).
			Where(clause.Or(changedConditions...)).Select(columns).Updates(&updates)
		if result.Error != nil {
			return nil, false, chatPersistenceDatabaseError(result.Error,
				zap.String(logkeys.Stage, "update_chat_participant_preferences"),
				zap.Uint64(appLogger.KeyChannelID, channelID),
				zap.Uint64(appLogger.KeyUserID, userID))
		}
		changed = result.RowsAffected > 0
	}
	participant, err := h.GetChatChannelParticipant(ctx, channelID, userID)
	return participant, changed, err
}

// ListChatChannelParticipants executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) ListChatChannelParticipants(
	ctx context.Context,
	channelID uint64,
	pagination *proto_models.PaginationRequest,
) ([]*common_persistence_models.ChatChannelParticipant, *proto_models.PaginationResponse, error) {
	db := h.getDBFromContext(ctx)

	if err := requirePageLimit("list_chat_channel_participants", pagination); err != nil {
		return nil, nil, err
	}
	page, limit := pagination.GetPage(), pagination.GetLimit()
	offset := common_util.PaginationOffset(page, limit)

	query := db.Model(&common_persistence_models.ChatChannelParticipant{}).
		Where("channel_id = ? AND left_at IS NULL", channelID)

	includeTotalCount := common_util.ShouldIncludeTotalCount(pagination)
	var totalCount int64
	if includeTotalCount {
		if err := query.Count(&totalCount).Error; err != nil {
			return nil, nil, chatPersistenceDatabaseError(err,
				zap.String(logkeys.Stage, "count_chat_participants"),
				zap.Uint64(appLogger.KeyChannelID, channelID))
		}
	}

	var participants []*common_persistence_models.ChatChannelParticipant
	if err := query.
		Preload(common_persistence_models.PreloadUser).
		Preload(common_persistence_models.PreloadUserProfilePictureAttachment).
		Order("joined_at ASC, user_id ASC").
		Offset(offset).
		Limit(common_util.PaginationQueryLimit(limit, includeTotalCount)).
		Find(&participants).Error; err != nil {
		return nil, nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_chat_participants"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Uint32(appLogger.KeyPage, page),
			zap.Uint32(logkeys.Limit, limit))
	}
	participants, hasNextPage := common_util.TrimPaginationProbe(participants, limit, includeTotalCount)

	var paginationResponse *proto_models.PaginationResponse
	if includeTotalCount {
		paginationResponse = common_util.CreatePaginationResponse(totalCount, limit, page)
	} else {
		paginationResponse = common_util.CreatePaginationResponseWithoutTotalCount(limit, page, hasNextPage)
	}
	return participants, paginationResponse, nil
}

// ListChatChannelParticipantsBatch executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) ListChatChannelParticipantsBatch(
	ctx context.Context,
	channelIDs []uint64,
	pagination *proto_models.PaginationRequest,
) ([]*common_persistence_models.ChatChannelParticipantsBatchItem, error) {
	if len(channelIDs) == 0 {
		return []*common_persistence_models.ChatChannelParticipantsBatchItem{}, nil
	}

	db := h.getDBFromContext(ctx)

	if err := requirePageLimit("list_chat_channel_participants_batch", pagination); err != nil {
		return nil, err
	}
	page, limit := pagination.GetPage(), pagination.GetLimit()
	offset := common_util.PaginationOffset(page, limit)

	normalizedChannelIDs := common_util.DeduplicateNonZero(channelIDs)
	if len(normalizedChannelIDs) == 0 {
		return []*common_persistence_models.ChatChannelParticipantsBatchItem{}, nil
	}

	includeTotalCount := common_util.ShouldIncludeTotalCount(pagination)
	countsByChannel := make(map[uint64]int64, len(normalizedChannelIDs))
	if includeTotalCount {
		// countRow carries grouped participant totals while
		// ListChatChannelParticipantsBatch assembles per-channel counts for batched responses.
		type countRow struct {
			ChannelID uint64 `gorm:"column:channel_id"`
			Count     int64  `gorm:"column:count"`
		}
		var countRows []countRow
		if err := db.Model(&common_persistence_models.ChatChannelParticipant{}).
			Select("channel_id, COUNT(*) AS count").
			Where("channel_id IN ? AND left_at IS NULL", normalizedChannelIDs).
			Group("channel_id").
			Scan(&countRows).Error; err != nil {
			return nil, chatPersistenceDatabaseError(err,
				zap.String(logkeys.Stage, "count_chat_participants_batch"),
				zap.Int(appLogger.KeyChannelCount, len(normalizedChannelIDs)))
		}
		for _, row := range countRows {
			countsByChannel[row.ChannelID] = row.Count
		}
	}

	participantTable := common_persistence_models.TableNameChatChannelParticipants
	rankedSubQuery := db.Table(participantTable).
		Select("channel_id, user_id, ROW_NUMBER() OVER (PARTITION BY channel_id ORDER BY joined_at ASC, user_id ASC) AS rn").
		Where("channel_id IN ? AND left_at IS NULL", normalizedChannelIDs)
	pagedSubQuery := db.Table("(?) AS ranked", rankedSubQuery).
		Select("channel_id, user_id, rn").
		Where("rn > ? AND rn <= ?", offset, offset+common_util.PaginationQueryLimit(limit, includeTotalCount))

	var participants []*common_persistence_models.ChatChannelParticipant
	if err := db.Model(&common_persistence_models.ChatChannelParticipant{}).
		Joins(
			"JOIN (?) paged ON paged.channel_id = "+participantTable+".channel_id AND paged.user_id = "+participantTable+".user_id",
			pagedSubQuery,
		).
		Preload(common_persistence_models.PreloadUser).
		Preload(common_persistence_models.PreloadUserProfilePictureAttachment).
		Order("paged.channel_id ASC, paged.rn ASC").
		Find(&participants).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_chat_participants_batch"),
			zap.Int(appLogger.KeyChannelCount, len(normalizedChannelIDs)),
			zap.Uint32(appLogger.KeyPage, page),
			zap.Uint32(logkeys.Limit, limit))
	}

	participantsByChannel := make(map[uint64][]*common_persistence_models.ChatChannelParticipant, len(normalizedChannelIDs))
	hasNextByChannel := make(map[uint64]bool, len(normalizedChannelIDs))
	for _, participant := range participants {
		if participant == nil {
			continue
		}
		channelParticipants := participantsByChannel[participant.ChannelID]
		if !includeTotalCount && uint32(len(channelParticipants)) >= limit {
			hasNextByChannel[participant.ChannelID] = true
			continue
		}
		participantsByChannel[participant.ChannelID] = append(channelParticipants, participant)
	}

	out := make([]*common_persistence_models.ChatChannelParticipantsBatchItem, 0, len(normalizedChannelIDs))
	for _, channelID := range normalizedChannelIDs {
		channelParticipants := participantsByChannel[channelID]
		if channelParticipants == nil {
			channelParticipants = []*common_persistence_models.ChatChannelParticipant{}
		}
		var paginationResponse *proto_models.PaginationResponse
		if includeTotalCount {
			paginationResponse = common_util.CreatePaginationResponse(countsByChannel[channelID], limit, page)
		} else {
			paginationResponse = common_util.CreatePaginationResponseWithoutTotalCount(limit, page, hasNextByChannel[channelID])
		}
		out = append(out, &common_persistence_models.ChatChannelParticipantsBatchItem{
			ChannelID:    channelID,
			Participants: channelParticipants,
			Pagination:   paginationResponse,
		})
	}

	return out, nil
}

// GetActiveChannelParticipantCount executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetActiveChannelParticipantCount(
	ctx context.Context,
	channelID uint64,
) (int64, error) {
	db := h.getDBFromContext(ctx)

	var count int64
	if err := db.Model(&common_persistence_models.ChatChannelParticipant{}).
		Where("channel_id = ? AND left_at IS NULL", channelID).
		Count(&count).Error; err != nil {
		return 0, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "count_active_channel_participants"),
			zap.Uint64(appLogger.KeyChannelID, channelID))
	}

	return count, nil
}

// GetActiveChannelParticipantCountByUserIDs executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetActiveChannelParticipantCountByUserIDs(
	ctx context.Context,
	channelID uint64,
	userIDs []uint64,
) (int64, error) {
	if len(userIDs) == 0 {
		return 0, nil
	}

	db := h.getDBFromContext(ctx)

	var count int64
	if err := db.Model(&common_persistence_models.ChatChannelParticipant{}).
		Where("channel_id = ? AND left_at IS NULL AND user_id IN ?", channelID, userIDs).
		Count(&count).Error; err != nil {
		return 0, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "count_active_channel_participants_by_user_ids"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Int(appLogger.KeyUserCount, len(userIDs)))
	}

	return count, nil
}

// GetActiveChannelParticipantUserIDs executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetActiveChannelParticipantUserIDs(
	ctx context.Context,
	channelID uint64,
) ([]uint64, error) {
	db := h.getDBFromContext(ctx)

	var userIDs []uint64
	err := db.Table(common_persistence_models.ChatChannelParticipant{}.TableName()+" AS p").
		Joins("JOIN "+common_persistence_models.User{}.TableName()+" AS u ON u.id = p.user_id").
		Where("p.channel_id = ? AND p.left_at IS NULL", channelID).
		Joins("JOIN "+common_persistence_models.ChatChannel{}.TableName()+" AS c ON c.id = p.channel_id AND c.deleted_at IS NULL").
		Where("u.banned = ? AND u.deleted_at IS NULL", false).
		Pluck("p.user_id", &userIDs).Error
	if err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "get_active_channel_participant_user_ids"),
			zap.Uint64(appLogger.KeyChannelID, channelID))
	}

	return userIDs, nil
}

// GetHiddenAuthorizedChannelParticipantUserIDs returns the active participants of a channel who are
// authorized to receive hidden_from_users transcript rows + tool-call resolution events: bot/service
// users and channel admins/owners. Regular members are excluded so a hidden row's MESSAGE_SENT or
// resolution event never fans out to them (Decision 7 split-recipient fanout). This is intentionally
// conservative — it never returns a regular member — so it can only under-deliver to a privileged
// session, never leak to a regular one.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - channelID: The channel whose privileged participants to resolve.
//
// Returns:
//   - result1: The hidden-authorized active participant user IDs.
//   - result2: ErrDatabaseError on failure.
func (h *CommonPersistenceHandler[U]) GetHiddenAuthorizedChannelParticipantUserIDs(
	ctx context.Context,
	channelID uint64,
) ([]uint64, error) {
	db := h.getDBFromContext(ctx)

	participantsTable := common_persistence_models.ChatChannelParticipant{}.TableName()
	usersTable := common_persistence_models.User{}.TableName()

	var userIDs []uint64
	err := db.Table(participantsTable+" AS p").
		Joins("JOIN "+usersTable+" AS u ON u.id = p.user_id").
		Where("p.channel_id = ? AND p.left_at IS NULL", channelID).
		Joins("JOIN "+common_persistence_models.ChatChannel{}.TableName()+" AS c ON c.id = p.channel_id AND c.deleted_at IS NULL").
		Where("u.banned = ? AND u.deleted_at IS NULL", false).
		Where("u.is_bot = ? OR p.role IN ?", true, []proto_models.ChannelRole{
			proto_models.ChannelRole_CHANNEL_ROLE_ADMIN,
			proto_models.ChannelRole_CHANNEL_ROLE_OWNER,
		}).
		Pluck("p.user_id", &userIDs).Error
	if err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "get_hidden_authorized_channel_participant_user_ids"),
			zap.Uint64(appLogger.KeyChannelID, channelID))
	}

	return userIDs, nil
}

// ============================================================================
// CHAT: MESSAGES
// ============================================================================

// SendChatMessage executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) SendChatMessage(
	ctx context.Context,
	in SendChatMessageInput,
) (*common_persistence_models.ChatMessage, error) {
	db := h.getDBFromContext(ctx)
	channelID, senderID, content := in.ChannelID, in.SenderID, in.Content
	embeds, mentions := in.Embeds, in.Mentions

	message := &common_persistence_models.ChatMessage{
		Kind:               proto_models.MessageKind_MESSAGE_KIND_STANDARD,
		Role:               in.Role,
		ChannelID:          channelID,
		SenderID:           senderID,
		Content:            content,
		ContentType:        in.ContentType,
		StreamState:        in.StreamState,
		ParentMessageID:    in.ParentMessageID,
		ReplyToMessageID:   in.ReplyToMessageID,
		BroadcastToChannel: in.BroadcastToChannel,
		AgentRunID:         in.AgentRunID,
		IsEdited:           false,
		OriginalContent:    nil,
	}
	if in.ClientMessageID != "" {
		clientMessageID := in.ClientMessageID
		message.ClientMessageID = &clientMessageID
	}

	err := db.Transaction(func(tx *gorm.DB) error {
		if err := insertChatMessageTx(tx, message, embeds, mentions); err != nil {
			return err
		}
		if err := touchChatChannelLastMessageTx(tx, channelID, senderID, content); err != nil {
			return chatPersistenceDatabaseError(err,
				zap.String(logkeys.Stage, "update_chat_channel_last_message_fields"),
				zap.Uint64(appLogger.KeyChannelID, channelID),
				zap.Uint64(appLogger.KeyMessageID, message.ID))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// The created rows already hold the full message state (a fresh message has
	// no reactions), so only the sender and mention users need hydrating —
	// avoiding a read-back with seven preloads.
	sortChatMessageEmbedsByPosition(message.Embeds)
	sortChatMessageMentionsByUser(message.Mentions)
	if err := h.hydrateChatMessageRelations(ctx, message, false); err != nil {
		return nil, err
	}
	return message, nil
}

// insertChatMessageTx writes a new message's row, embeds and mentions on tx, setting message.Embeds
// and message.Mentions to the rows written: the write every send shares, including queued agent
// input when delivery turns it into a message. The caller makes it the channel's last message.
func insertChatMessageTx(tx *gorm.DB, message *common_persistence_models.ChatMessage, embeds []CreateChatMessageEmbed, mentions []CreateChatMessageMention) error {
	if err := tx.Create(message).Error; err != nil {
		return chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "create_chat_message"),
			zap.Uint64(appLogger.KeyChannelID, message.ChannelID),
			zap.Uint64(appLogger.KeyUserID, message.SenderID))
	}
	if len(embeds) > 0 {
		rows := common_util.ApplyToEach(embeds, func(e CreateChatMessageEmbed) common_persistence_models.ChatMessageEmbed {
			return common_persistence_models.ChatMessageEmbed{
				MessageID:    message.ID,
				EmbedType:    e.EmbedType,
				EmbedData:    e.EmbedData,
				FallbackText: e.FallbackText,
				Position:     e.Position,
			}
		})
		if err := tx.Create(&rows).Error; err != nil {
			return chatPersistenceDatabaseError(err,
				zap.String(logkeys.Stage, "create_chat_message_embeds"),
				zap.Uint64(appLogger.KeyMessageID, message.ID),
				zap.Int(appLogger.KeyEmbedCount, len(rows)))
		}
		message.Embeds = rows
	}
	if len(mentions) > 0 {
		rows := common_util.ApplyToEach(mentions, func(mention CreateChatMessageMention) common_persistence_models.ChatMessageMention {
			return common_persistence_models.ChatMessageMention{
				MessageID: message.ID,
				UserID:    mention.UserID,
			}
		})
		if err := tx.Create(&rows).Error; err != nil {
			return chatPersistenceDatabaseError(err,
				zap.String(logkeys.Stage, "create_chat_message_mentions"),
				zap.Uint64(appLogger.KeyMessageID, message.ID),
				zap.Int(appLogger.KeyMentionCount, len(rows)))
		}
		message.Mentions = rows
	}
	return nil
}

// ChatClientMessageIDOwner is what holds one sender's client message ID in a channel: the input
// an earlier send queued for an agent session while it waits, else the message the send, or that
// input's delivery, wrote (soft-deleted ones included). Both are nil while the key is unused.
type ChatClientMessageIDOwner struct {
	QueuedInput *common_persistence_models.ChatAgentQueuedInput
	Message     *common_persistence_models.ChatMessage
}

// LockChatClientMessageID locks one sender's client message ID in a channel for the rest of the
// caller's transaction and returns what already holds it: a queued input read with its sender, or
// a message. The lock is a transaction-scoped advisory lock that a send takes before any of its
// row locks, so a concurrent send with the same key waits for this transaction and then finds what
// it wrote rather than violating idx_chat_messages_client_message_id or
// idx_chat_agent_queued_inputs_client_message_id. The queued input is read before the message:
// delivery moves the key from one to the other in one transaction, so a read that misses the input
// finds the message. It requires a transaction (ErrChatMessageTransactionRequired).
func (h *CommonPersistenceHandler[U]) LockChatClientMessageID(
	ctx context.Context,
	senderID uint64,
	channelID uint64,
	clientMessageID string,
) (ChatClientMessageIDOwner, error) {
	var owner ChatClientMessageIDOwner
	if tx, ok := scontext.GetTransaction(ctx); !ok || tx == nil {
		return owner, ErrChatMessageTransactionRequired
	}
	db := h.getDBFromContext(ctx)
	key := fmt.Sprintf("chat-client-message:%d:%d:%s", senderID, channelID, clientMessageID)
	fields := []zap.Field{
		zap.String(logkeys.Stage, "lock_chat_client_message_id"),
		zap.Uint64(appLogger.KeyChannelID, channelID),
		zap.Uint64(appLogger.KeyUserID, senderID),
	}
	if err := db.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", key).Error; err != nil {
		return owner, chatPersistenceDatabaseError(err, fields...)
	}
	var queued []*common_persistence_models.ChatAgentQueuedInput
	if err := preloadChatAgentQueuedInputSender(db).Where("sender_id = ? AND channel_id = ? AND client_message_id = ?", senderID, channelID, clientMessageID).
		Limit(1).Find(&queued).Error; err != nil {
		return owner, chatPersistenceDatabaseError(err, fields...)
	}
	if len(queued) > 0 {
		owner.QueuedInput = queued[0]
		return owner, nil
	}
	var ids []uint64
	if err := db.Unscoped().Model(&common_persistence_models.ChatMessage{}).
		Where("sender_id = ? AND channel_id = ? AND client_message_id = ?", senderID, channelID, clientMessageID).
		Limit(1).Pluck("id", &ids).Error; err != nil {
		return owner, chatPersistenceDatabaseError(err, fields...)
	}
	if len(ids) == 0 {
		return owner, nil
	}
	message, err := h.GetChatMessageByIDWithOptions(ctx, ids[0], ChatMessageReadOptions{IncludeDeleted: true})
	if err != nil {
		return owner, err
	}
	owner.Message = message
	return owner, nil
}

// SetChatChannelOrganizationAccess writes the org-derived access a channel owns: the organization
// it is associated with and the minimum effective organization role that may read it without
// participating. Both columns are always written together, so passing a nil organizationID with
// ORGANIZATION_ROLE_UNSPECIFIED clears the association. It runs on the caller's ambient
// transaction when there is one, so channel creation can set it atomically with CreateChatChannel.
//
// It returns ErrChatChannelOrganizationAccessInvalid for a non-positive organization ID, a role
// that is not a defined OrganizationRole, or a read threshold without an organization, and
// ErrChatChannelNotFound when no live channel has channelID. Whether the organization exists is
// not checked here: an unknown organization simply has no members to grant access to.
func (h *CommonPersistenceHandler[U]) SetChatChannelOrganizationAccess(ctx context.Context, channelID uint64, organizationID *int64, minRoleRead proto_models.OrganizationRole) error {
	if _, known := proto_models.OrganizationRole_name[int32(minRoleRead)]; !known ||
		(organizationID != nil && *organizationID <= 0) ||
		(organizationID == nil && minRoleRead != proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED) {
		return ErrChatChannelOrganizationAccessInvalid
	}
	return h.withChatLocationTransaction(ctx, func(txCtx context.Context, db *gorm.DB) error {
		if _, err := h.lockChatLocationChannel(txCtx, channelID); err != nil {
			return err
		}
		if organizationID != nil {
			var count int64
			if err := db.Model(&common_persistence_models.ChatAgentSession{}).Where("channel_id = ? AND (organization_id IS NULL OR organization_id <> ?)", channelID, *organizationID).Count(&count).Error; err != nil {
				return err
			}
			if count > 0 {
				return ErrChatChannelOrganizationAccessInvalid
			}
		}
		return h.setChatChannelOrganizationAccessInLocation(txCtx, channelID, organizationID, minRoleRead)
	})
}

func (h *CommonPersistenceHandler[U]) setChatChannelOrganizationAccessInLocation(
	ctx context.Context,
	channelID uint64,
	organizationID *int64,
	minRoleRead proto_models.OrganizationRole,
) error {
	if _, known := proto_models.OrganizationRole_name[int32(minRoleRead)]; !known ||
		(organizationID != nil && *organizationID <= 0) ||
		(organizationID == nil && minRoleRead != proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED) {
		return ErrChatChannelOrganizationAccessInvalid
	}
	db := h.getDBFromContext(ctx)

	// Selecting both columns makes nil organizations and the zero role explicit writes.
	result := db.Model(&common_persistence_models.ChatChannel{}).
		Where("id = ?", channelID).
		Select("organization_id", "org_min_role_read").Updates(&common_persistence_models.ChatChannel{
		OrganizationID: organizationID,
		OrgMinRoleRead: minRoleRead,
	})
	if result.Error != nil {
		return chatPersistenceDatabaseError(result.Error,
			zap.String(logkeys.Stage, "set_chat_channel_organization_access"),
			zap.Uint64(appLogger.KeyChannelID, channelID))
	}
	if result.RowsAffected == 0 {
		return ErrChatChannelNotFound
	}
	return nil
}

// SetChatChannelPublicRead writes a channel's public read level; see CommonPersistenceInterface.
func (h *CommonPersistenceHandler[U]) SetChatChannelPublicRead(ctx context.Context, channelID uint64, level proto_models.ChatChannelPublicRead) (*common_persistence_models.ChatChannel, error) {
	if _, known := proto_models.ChatChannelPublicRead_name[int32(level)]; !known {
		return nil, ErrChatChannelPublicReadInvalid
	}
	// Selecting the column makes the zero level (off) an explicit write; RETURNING fills the
	// whole fresh row.
	db := h.getDBFromContext(ctx)
	updated := &common_persistence_models.ChatChannel{PublicRead: level}
	result := db.Model(updated).Clauses(clause.Returning{}).
		Where("id = ?", channelID).
		Select("public_read", "updated_at").
		Updates(updated)
	if result.Error != nil {
		return nil, chatPersistenceDatabaseError(result.Error,
			zap.String(logkeys.Stage, "set_chat_channel_public_read"),
			zap.Uint64(appLogger.KeyChannelID, channelID))
	}
	if result.RowsAffected == 0 {
		return nil, ErrChatChannelNotFound
	}
	if err := hydrateChatChannelAvatar(db, updated); err != nil {
		return nil, err
	}
	return updated, nil
}

// CreateChatMessageEmbeds executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) CreateChatMessageEmbeds(
	ctx context.Context,
	messageID uint64,
	embeds []CreateChatMessageEmbed,
) error {
	if len(embeds) == 0 {
		return nil
	}
	db := h.getDBFromContext(ctx)

	rows := make([]common_persistence_models.ChatMessageEmbed, 0, len(embeds))
	for _, e := range embeds {
		rows = append(rows, common_persistence_models.ChatMessageEmbed{
			MessageID:    messageID,
			EmbedType:    e.EmbedType,
			EmbedData:    e.EmbedData,
			FallbackText: e.FallbackText,
			Position:     e.Position,
		})
	}
	if err := db.Create(&rows).Error; err != nil {
		return chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "create_chat_message_embeds"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.Int(appLogger.KeyEmbedCount, len(rows)))
	}
	return nil
}

// UpdateChatMessageAttachmentIDs executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) UpdateChatMessageAttachmentIDs(
	ctx context.Context,
	messageID uint64,
	attachmentIDs []uint64,
) (*common_persistence_models.ChatMessage, error) {
	db := h.getDBFromContext(ctx)
	pqIDs := common_util.ConvertUint64SliceToPqInt64Array(attachmentIDs)

	result := db.Model(&common_persistence_models.ChatMessage{}).
		Where("id = ?", messageID).
		Update("attachment_ids", pqIDs)
	if result.Error != nil {
		return nil, chatPersistenceDatabaseError(result.Error,
			zap.String(logkeys.Stage, "update_chat_message_attachment_ids"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.Int(appLogger.KeyAttachmentCount, len(attachmentIDs)))
	}
	if result.RowsAffected == 0 {
		return nil, ErrChatMessageNotFound
	}

	return h.GetChatMessageByID(ctx, messageID)
}

// chatMessageEmbedHistorySnapshot defines data used in the persistence flow between service handlers and the database layer.
// It is consumed by handlers and helpers that participate in this backend call path.
type chatMessageEmbedHistorySnapshot struct {
	EmbedType    uint32 `json:"embed_type"`
	EmbedData    []byte `json:"embed_data"`
	FallbackText string `json:"fallback_text"`
	Position     uint32 `json:"position"`
}

// chatMessageReactionOrder and chatMessageMentionOrder give the has-many preloads a stable order;
// without one the order follows the query plan and leaks to clients.
const (
	chatMessageReactionOrder = "created_at ASC, user_id ASC, emoji ASC"
	chatMessageMentionOrder  = "user_id ASC"
)

// preloadChatMessageAssociations preloads every message association except the ToolCall side
// row. Callers must follow the read with attachChatMessageToolCalls.
func preloadChatMessageAssociations(db *gorm.DB) *gorm.DB {
	return db.
		Preload(common_persistence_models.PreloadSender).
		// IncludeDeleted reads run Unscoped; keep deleted avatars out regardless.
		Preload(common_persistence_models.PreloadSenderProfilePictureAttachment, preloadLiveAttachment).
		Preload(common_persistence_models.PreloadEmbeds, func(tx *gorm.DB) *gorm.DB { return tx.Order("position ASC") }).
		Preload(common_persistence_models.PreloadReactions, func(tx *gorm.DB) *gorm.DB { return tx.Order(chatMessageReactionOrder) }).
		Preload(common_persistence_models.PreloadReactionsUser).
		Preload(common_persistence_models.PreloadMentions, func(tx *gorm.DB) *gorm.DB { return tx.Order(chatMessageMentionOrder) }).
		Preload(common_persistence_models.PreloadMentionsUser)
}

// preloadChatModelContextAssociations keeps model reads independent of display-only hydration.
// Tool side rows are attached separately; private model media is resolved after token selection.
func preloadChatModelContextAssociations(db *gorm.DB) *gorm.DB {
	return db.
		Preload(common_persistence_models.PreloadSender, func(tx *gorm.DB) *gorm.DB {
			return tx.Select("id", "username", "user_type", "is_bot")
		}).
		Preload(common_persistence_models.PreloadEmbeds, func(tx *gorm.DB) *gorm.DB { return tx.Order("position ASC") })
}

// chatMessageKindMayCarryToolCall reports whether a message of this kind can own a ToolCall side
// row. CreateTranscriptMessage is the only side-row writer, validateTranscriptSideRowPayload rejects
// HasToolCall for these kinds, and a message's kind never changes after insert.
func chatMessageKindMayCarryToolCall(kind proto_models.MessageKind) bool {
	switch kind {
	case proto_models.MessageKind_MESSAGE_KIND_SYSTEM_EVENT,
		proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL,
		proto_models.MessageKind_MESSAGE_KIND_TOOL_RESULT,
		proto_models.MessageKind_MESSAGE_KIND_COMPACTION_SUMMARY,
		proto_models.MessageKind_MESSAGE_KIND_PROVIDER_STATE:
		return true
	default:
		return false
	}
}

// attachChatMessageToolCalls loads the 1:1 ToolCall side rows (tool-call/approval/payload state)
// for messages whose kind can carry one, so pages of human messages skip the query entirely.
// Messages of other kinds keep a nil ToolCall, as a preload would leave them.
func (h *CommonPersistenceHandler[U]) attachChatMessageToolCalls(ctx context.Context, msgs []*common_persistence_models.ChatMessage) error {
	ids := make([]uint64, 0, len(msgs))
	for _, msg := range msgs {
		if chatMessageKindMayCarryToolCall(msg.Kind) {
			ids = append(ids, msg.ID)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	var calls []common_persistence_models.ChatMessageToolCall
	if err := h.getDBFromContext(ctx).Where("message_id IN ?", ids).Find(&calls).Error; err != nil {
		return chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "load_chat_message_tool_calls"),
			zap.Int(appLogger.KeyMessageCount, len(ids)))
	}
	byMessageID := make(map[uint64]*common_persistence_models.ChatMessageToolCall, len(calls))
	for i := range calls {
		byMessageID[calls[i].MessageID] = &calls[i]
	}
	for _, msg := range msgs {
		msg.ToolCall = byMessageID[msg.ID]
	}
	return nil
}

// sortChatMessageMentionsByUser orders mentions the way the read path does (user id ascending),
// so in-memory assembled messages match read-back order.
func sortChatMessageMentionsByUser(mentions []common_persistence_models.ChatMessageMention) {
	sort.SliceStable(mentions, func(i, j int) bool {
		return mentions[i].UserID < mentions[j].UserID
	})
}

// sortChatMessageEmbedsByPosition orders embeds the way the read path does
// (position ascending), so in-memory assembled messages match read-back order.
func sortChatMessageEmbedsByPosition(embeds []common_persistence_models.ChatMessageEmbed) {
	sort.SliceStable(embeds, func(i, j int) bool {
		return embeds[i].Position < embeds[j].Position
	})
}

// hydrateChatMessageRelations populates the relations of an in-memory
// assembled message — Sender (with profile picture), mention users, and
// optionally reactions — with targeted queries. Writes use this instead of a
// full GetChatMessageByID read-back (seven preloads) after commit.
// Parameters:
//   - ctx: context.Context input used by hydrateChatMessageRelations.
//   - msg: message whose relation fields are populated in place.
//   - includeReactions: when true, existing reactions (and their users) are loaded.
func (h *CommonPersistenceHandler[U]) hydrateChatMessageRelations(
	ctx context.Context,
	msg *common_persistence_models.ChatMessage,
	includeReactions bool,
) error {
	db := h.getDBFromContext(ctx)

	var sender common_persistence_models.User
	err := db.
		Preload(common_persistence_models.PreloadProfilePictureAttachment).
		Where("id = ?", msg.SenderID).
		First(&sender).Error
	switch {
	case err == nil:
		msg.Sender = sender
	case errors.Is(err, gorm.ErrRecordNotFound):
		// Sender deleted; leave the zero User (converter treats ID 0 as absent).
	default:
		return chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "load_chat_message_sender"),
			zap.Uint64(appLogger.KeyMessageID, msg.ID),
			zap.Uint64(appLogger.KeyUserID, msg.SenderID))
	}

	if len(msg.Mentions) > 0 {
		mentionUserIDs := common_util.ApplyToEach(msg.Mentions, func(mention common_persistence_models.ChatMessageMention) uint64 {
			return mention.UserID
		})
		var users []common_persistence_models.User
		if err := db.Where("id IN ?", mentionUserIDs).Find(&users).Error; err != nil {
			return chatPersistenceDatabaseError(err,
				zap.String(logkeys.Stage, "load_chat_message_mention_users"),
				zap.Uint64(appLogger.KeyMessageID, msg.ID),
				zap.Int(appLogger.KeyMentionCount, len(mentionUserIDs)))
		}
		usersByID := make(map[uint64]common_persistence_models.User, len(users))
		for _, u := range users {
			usersByID[u.ID] = u
		}
		for i := range msg.Mentions {
			if u, ok := usersByID[msg.Mentions[i].UserID]; ok {
				msg.Mentions[i].User = u
			}
		}
	}

	if includeReactions {
		if err := db.
			Preload(common_persistence_models.PreloadUser).
			Where("message_id = ?", msg.ID).
			Order(chatMessageReactionOrder).
			Find(&msg.Reactions).Error; err != nil {
			return chatPersistenceDatabaseError(err,
				zap.String(logkeys.Stage, "load_chat_message_reactions"),
				zap.Uint64(appLogger.KeyMessageID, msg.ID))
		}
	}

	return nil
}

// preloadChatMessageEditAssociations executes its operation in the persistence flow between service handlers and the database layer.
func preloadChatMessageEditAssociations(db *gorm.DB) *gorm.DB {
	return db.
		Preload(common_persistence_models.PreloadEmbeds, func(tx *gorm.DB) *gorm.DB { return tx.Order("position ASC") }).
		Preload(common_persistence_models.PreloadMentions)
}

// chatMessageVersionStartedAt executes its operation in the persistence flow between service handlers and the database layer.
func chatMessageVersionStartedAt(message *common_persistence_models.ChatMessage) time.Time {
	if message != nil && message.EditedAt != nil && !message.EditedAt.IsZero() {
		return *message.EditedAt
	}
	if message != nil {
		return message.CreatedAt
	}
	return time.Time{}
}

// marshalChatMessageEmbedsForHistory executes its operation in the persistence flow between service handlers and the database layer.
func marshalChatMessageEmbedsForHistory(embeds []common_persistence_models.ChatMessageEmbed) ([]byte, error) {
	if len(embeds) == 0 {
		return []byte("[]"), nil
	}

	snapshot := make([]chatMessageEmbedHistorySnapshot, 0, len(embeds))
	for _, embed := range embeds {
		snapshot = append(snapshot, chatMessageEmbedHistorySnapshot{
			EmbedType:    embed.EmbedType,
			EmbedData:    append([]byte(nil), embed.EmbedData...),
			FallbackText: embed.FallbackText,
			Position:     embed.Position,
		})
	}

	return json.Marshal(snapshot)
}

// buildChatMessageEditHistory executes its operation in the persistence flow between service handlers and the database layer.
func buildChatMessageEditHistory(
	message *common_persistence_models.ChatMessage,
	editedByUserID uint64,
	versionNumber uint32,
	versionEndedAt time.Time,
) (*common_persistence_models.ChatMessageEditHistory, error) {
	embedsJSON, err := marshalChatMessageEmbedsForHistory(message.Embeds)
	if err != nil {
		return nil, err
	}

	mentionUserIDs := make([]uint64, 0, len(message.Mentions))
	for _, mention := range message.Mentions {
		mentionUserIDs = append(mentionUserIDs, mention.UserID)
	}

	return &common_persistence_models.ChatMessageEditHistory{
		MessageID:        message.ID,
		VersionNumber:    versionNumber,
		EditedByUserID:   editedByUserID,
		VersionStartedAt: chatMessageVersionStartedAt(message),
		VersionEndedAt:   versionEndedAt,
		Content:          message.Content,
		ContentType:      message.ContentType,
		EmbedsJSON:       embedsJSON,
		AttachmentIDs:    append(pq.Int64Array(nil), message.AttachmentIDs...),
		MentionUserIDs:   common_util.ConvertUint64SliceToPqInt64Array(mentionUserIDs),
		CreatedAt:        versionEndedAt,
	}, nil
}

// GetChatMessageByID is the actor-default single-message read: hidden_from_users and soft-deleted
// rows are not found. Other visibility needs use GetChatMessageByIDWithOptions.
func (h *CommonPersistenceHandler[U]) GetChatMessageByID(ctx context.Context, messageID uint64) (*common_persistence_models.ChatMessage, error) {
	return h.GetChatMessageByIDWithOptions(ctx, messageID, ChatMessageReadOptions{
		VisibilityMode: ChatMessageVisibilityVisibleToActor,
	})
}

// ListChatMessageEditHistory executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) ListChatMessageEditHistory(
	ctx context.Context,
	messageID uint64,
	pagination *proto_models.PaginationRequest,
) ([]*common_persistence_models.ChatMessageEditHistory, *proto_models.PaginationResponse, error) {
	db := h.getDBFromContext(ctx)

	if err := requirePageLimit("list_chat_message_edit_history", pagination); err != nil {
		return nil, nil, err
	}
	page, limit := pagination.GetPage(), pagination.GetLimit()
	offset := common_util.PaginationOffset(page, limit)

	query := db.Model(&common_persistence_models.ChatMessageEditHistory{}).
		Where("message_id = ?", messageID)

	includeTotalCount := common_util.ShouldIncludeTotalCount(pagination)
	var totalCount int64
	if includeTotalCount {
		if err := query.Count(&totalCount).Error; err != nil {
			return nil, nil, chatPersistenceDatabaseError(err,
				zap.String(logkeys.Stage, "count_chat_message_edit_history"),
				zap.Uint64(appLogger.KeyMessageID, messageID))
		}
	}

	var rows []*common_persistence_models.ChatMessageEditHistory
	if err := query.
		Preload(common_persistence_models.PreloadEditedByUser).
		Preload(common_persistence_models.PreloadEditedByUserProfilePicture).
		Order("version_number DESC").
		Offset(offset).
		Limit(common_util.PaginationQueryLimit(limit, includeTotalCount)).
		Find(&rows).Error; err != nil {
		return nil, nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_chat_message_edit_history"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.Uint32(appLogger.KeyPage, page),
			zap.Uint32(logkeys.Limit, limit))
	}
	rows, hasNextPage := common_util.TrimPaginationProbe(rows, limit, includeTotalCount)

	if includeTotalCount {
		return rows, common_util.CreatePaginationResponse(totalCount, limit, page), nil
	}
	return rows, common_util.CreatePaginationResponseWithoutTotalCount(limit, page, hasNextPage), nil
}

// EditChatMessage executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) EditChatMessage(
	ctx context.Context,
	input EditChatMessageInput,
) (*common_persistence_models.ChatMessage, error) {
	db := h.getDBFromContext(ctx)

	now := time.Now()
	var edited common_persistence_models.ChatMessage
	err := db.Transaction(func(tx *gorm.DB) error {
		// Every edit locks its channel before its message, the order sends, deliveries and deletes
		// take, so an edit never holds the message while it waits on the channel to refresh the
		// preview below.
		var channel common_persistence_models.ChatChannel
		if err := tx.Clauses(clause.Locking{Strength: "NO KEY UPDATE"}).Select("id").
			Where("id = (?)", tx.Model(&common_persistence_models.ChatMessage{}).Select("channel_id").Where("id = ?", input.MessageID)).
			Take(&channel).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrChatMessageNotFound
			}
			return chatPersistenceDatabaseError(err,
				zap.String(logkeys.Stage, "lock_chat_channel_for_message_edit"),
				zap.Uint64(appLogger.KeyMessageID, input.MessageID))
		}
		// NO KEY UPDATE serializes edits of the message yet stays compatible with the FOR KEY SHARE
		// that inserting a reply or thread reply to it takes.
		var existing common_persistence_models.ChatMessage
		if err := preloadChatMessageEditAssociations(
			tx.Clauses(clause.Locking{Strength: "NO KEY UPDATE"}),
		).Where("id = ?", input.MessageID).First(&existing).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrChatMessageNotFound
			}
			return chatPersistenceDatabaseError(err,
				zap.String(logkeys.Stage, "load_chat_message_for_edit"),
				zap.Uint64(appLogger.KeyMessageID, input.MessageID),
				zap.Uint64(appLogger.KeyUserID, input.EditedByUserID))
		}
		if input.ExpectedStreamState != nil && existing.StreamState != *input.ExpectedStreamState {
			return ErrChatMessageStreamStateConflict
		}

		var maxVersion uint32
		if err := tx.Model(&common_persistence_models.ChatMessageEditHistory{}).
			Where("message_id = ?", input.MessageID).
			Select("COALESCE(MAX(version_number), 0)").
			Scan(&maxVersion).Error; err != nil {
			return chatPersistenceDatabaseError(err,
				zap.String(logkeys.Stage, "compute_chat_message_history_version"),
				zap.Uint64(appLogger.KeyMessageID, input.MessageID))
		}

		historyEntry, err := buildChatMessageEditHistory(&existing, input.EditedByUserID, maxVersion+1, now)
		if err != nil {
			return appLogger.InvariantError("chat_message_history_embeds_serializable", err,
				zap.String(logkeys.Stage, "build_chat_message_edit_history"),
				zap.Uint64(appLogger.KeyMessageID, input.MessageID),
				zap.String(logkeys.Expected, "message embeds serialize as JSON"),
				zap.String(logkeys.Actual, "serialization failed"))
		}
		if err := tx.Create(historyEntry).Error; err != nil {
			return chatPersistenceDatabaseError(err,
				zap.String(logkeys.Stage, "create_chat_message_edit_history"),
				zap.Uint64(appLogger.KeyMessageID, input.MessageID),
				zap.Uint32(appLogger.KeyVersion, maxVersion+1))
		}

		if existing.OriginalContent == nil {
			orig := existing.Content
			existing.OriginalContent = &orig
		}

		attachmentIDs := input.AttachmentIDs
		if attachmentIDs == nil {
			attachmentIDs = common_util.ConvertPqInt64ArrayToUint64Slice(existing.AttachmentIDs)
		}

		streamState := existing.StreamState
		if input.NewStreamState != nil {
			streamState = *input.NewStreamState
		}

		// RETURNING captures the post-update row so no read-back is needed.
		if err := tx.Model(&edited).Clauses(clause.Returning{}).Where("id = ?", input.MessageID).Select("content", "content_type", "stream_state", "is_edited", "edited_at", "original_content", "attachment_ids").Updates(&common_persistence_models.ChatMessage{
			Content:         input.NewContent,
			ContentType:     input.NewContentType,
			StreamState:     streamState,
			IsEdited:        true,
			EditedAt:        &now,
			OriginalContent: existing.OriginalContent,
			AttachmentIDs:   common_util.ConvertUint64SliceToPqInt64Array(attachmentIDs),
		}).Error; err != nil {
			return chatPersistenceDatabaseError(err,
				zap.String(logkeys.Stage, "update_chat_message_during_edit"),
				zap.Uint64(appLogger.KeyMessageID, input.MessageID))
		}

		// Replace embeds.
		if err := tx.Where("message_id = ?", input.MessageID).Delete(&common_persistence_models.ChatMessageEmbed{}).Error; err != nil {
			return chatPersistenceDatabaseError(err,
				zap.String(logkeys.Stage, "delete_chat_message_embeds_during_edit"),
				zap.Uint64(appLogger.KeyMessageID, input.MessageID))
		}
		edited.Embeds = nil
		if len(input.Embeds) > 0 {
			rows := make([]common_persistence_models.ChatMessageEmbed, 0, len(input.Embeds))
			for _, e := range input.Embeds {
				rows = append(rows, common_persistence_models.ChatMessageEmbed{
					MessageID:    input.MessageID,
					EmbedType:    e.EmbedType,
					EmbedData:    e.EmbedData,
					FallbackText: e.FallbackText,
					Position:     e.Position,
				})
			}
			if err := tx.Create(&rows).Error; err != nil {
				return chatPersistenceDatabaseError(err,
					zap.String(logkeys.Stage, "create_replacement_chat_message_embeds"),
					zap.Uint64(appLogger.KeyMessageID, input.MessageID),
					zap.Int(appLogger.KeyEmbedCount, len(rows)))
			}
			edited.Embeds = rows
		}

		// Replace mentions.
		if err := tx.Where("message_id = ?", input.MessageID).Delete(&common_persistence_models.ChatMessageMention{}).Error; err != nil {
			return chatPersistenceDatabaseError(err,
				zap.String(logkeys.Stage, "delete_chat_message_mentions_during_edit"),
				zap.Uint64(appLogger.KeyMessageID, input.MessageID))
		}
		edited.Mentions = nil
		if len(input.Mentions) > 0 {
			rows := make([]common_persistence_models.ChatMessageMention, 0, len(input.Mentions))
			for _, mention := range input.Mentions {
				rows = append(rows, common_persistence_models.ChatMessageMention{
					MessageID: input.MessageID,
					UserID:    mention.UserID,
				})
			}
			if err := tx.Create(&rows).Error; err != nil {
				return chatPersistenceDatabaseError(err,
					zap.String(logkeys.Stage, "create_replacement_chat_message_mentions"),
					zap.Uint64(appLogger.KeyMessageID, input.MessageID),
					zap.Int(appLogger.KeyMentionCount, len(rows)))
			}
			edited.Mentions = rows
		}

		// A stale preview would otherwise keep showing the pre-edit content in channel
		// lists when the edited message is the channel's newest visible one.
		if err := h.refreshChatChannelPreviewIfLatest(ctx, tx, existing.ChannelID, input.MessageID, input.NewContent); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	// Hydrate sender, mention users, and surviving reactions with targeted
	// queries instead of a full seven-preload read-back.
	sortChatMessageEmbedsByPosition(edited.Embeds)
	sortChatMessageMentionsByUser(edited.Mentions)
	if err := h.hydrateChatMessageRelations(ctx, &edited, true); err != nil {
		return nil, err
	}
	return &edited, nil
}

// DeleteChatMessage executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) DeleteChatMessage(ctx context.Context, channelID uint64, messageID uint64) error {
	return h.withChatLocationTransaction(ctx, func(txCtx context.Context, db *gorm.DB) error {
		// The channel lock serializes the delete against session creation, whose FOR UPDATE it
		// conflicts with. It is NO KEY UPDATE, as every channel lock taken before a last-message
		// refresh is: FOR UPDATE on this hot row deadlocks with concurrent inserts' foreign-key
		// checks (see the agent input lock order).
		if err := lockLiveChatChannelForAgentInputTx(db, channelID); err != nil {
			if errors.Is(err, ErrChatChannelNotFound) {
				return err
			}
			return chatPersistenceDatabaseError(err, zap.String(logkeys.Stage, "lock_chat_channel_for_delete"), zap.Uint64(appLogger.KeyChannelID, channelID))
		}
		var count int64
		if err := db.Model(&common_persistence_models.ChatAgentSession{}).Where("channel_id = ? AND parent_message_id = ?", channelID, messageID).Count(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			return ErrChatMessageAgentSessionRootPinned
		}
		return h.deleteChatMessageInLocation(txCtx, channelID, messageID)
	})
}

func (h *CommonPersistenceHandler[U]) deleteChatMessageInLocation(ctx context.Context, channelID uint64, messageID uint64) error {
	db := h.getDBFromContext(ctx)
	result := db.Where("channel_id = ?", channelID).Delete(&common_persistence_models.ChatMessage{}, messageID)
	if result.Error != nil {
		return chatPersistenceDatabaseError(result.Error,
			zap.String(logkeys.Stage, "delete_chat_message"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Uint64(appLogger.KeyMessageID, messageID))
	}
	if result.RowsAffected == 0 {
		return ErrChatMessageNotFound
	}
	// The deleted row may still be the stored channel preview; recompute it from the newest
	// surviving visible message so deleted content stops rendering in channel lists.
	return h.refreshChatChannelLastMessage(ctx, db, channelID)
}

// GetChatMessageReplyCounts executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetChatMessageReplyCounts(
	ctx context.Context,
	channelID uint64,
	messageIDs []uint64,
) (map[uint64]uint32, error) {
	result := make(map[uint64]uint32)
	if len(messageIDs) == 0 {
		return result, nil
	}

	db := h.getDBFromContext(ctx)

	// replyCount carries grouped reply totals while GetChatMessageReplyCounts maps
	// aggregate counts back to each parent message in the channel message flow.
	type replyCount struct {
		ParentMessageID uint64 `gorm:"column:parent_message_id"`
		Count           uint32 `gorm:"column:count"`
	}

	var counts []replyCount
	err := db.Model(&common_persistence_models.ChatMessage{}).
		Select("parent_message_id, COUNT(*) as count").
		Where("channel_id = ? AND parent_message_id IN ? AND deleted_at IS NULL AND hidden_from_users IS NOT TRUE AND kind <> ?",
			channelID, messageIDs, proto_models.MessageKind_MESSAGE_KIND_PROVIDER_STATE).
		Group("parent_message_id").
		Scan(&counts).Error
	if err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "get_chat_message_reply_counts"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Int(appLogger.KeyMessageCount, len(messageIDs)))
	}

	for _, c := range counts {
		result[c.ParentMessageID] = c.Count
	}

	return result, nil
}

// ============================================================================
// CHAT: REACTIONS
// ============================================================================

// AddChatMessageReaction executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AddChatMessageReaction(ctx context.Context, messageID uint64, userID uint64, emoji string) (*common_persistence_models.ChatMessageReaction, error) {
	db := h.getDBFromContext(ctx)

	reaction := &common_persistence_models.ChatMessageReaction{
		MessageID: messageID,
		UserID:    userID,
		Emoji:     emoji,
	}

	result := db.Clauses(clause.OnConflict{DoNothing: true}).Create(reaction)
	if result.Error != nil {
		return nil, chatPersistenceDatabaseError(result.Error,
			zap.String(logkeys.Stage, "add_chat_reaction"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyEmoji, emoji))
	}

	if result.RowsAffected == 0 {
		// Conflict: the reaction already existed, so the insert returned no row
		// and the existing one must be loaded.
		if err := db.Preload(common_persistence_models.PreloadUser).Where("message_id = ? AND user_id = ? AND emoji = ?", messageID, userID, emoji).First(reaction).Error; err != nil {
			return nil, chatPersistenceDatabaseError(err,
				zap.String(logkeys.Stage, "load_existing_chat_reaction_after_conflict"),
				zap.Uint64(appLogger.KeyMessageID, messageID),
				zap.Uint64(appLogger.KeyUserID, userID),
				zap.String(appLogger.KeyEmoji, emoji))
		}
		return reaction, nil
	}

	// Fresh insert: the row is already populated via RETURNING; only the user
	// relation needs hydrating.
	var user common_persistence_models.User
	if err := db.Where("id = ?", userID).First(&user).Error; err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, chatPersistenceDatabaseError(err,
				zap.String(logkeys.Stage, "load_chat_reaction_user"),
				zap.Uint64(appLogger.KeyMessageID, messageID),
				zap.Uint64(appLogger.KeyUserID, userID))
		}
	} else {
		reaction.User = user
	}

	return reaction, nil
}

// AddChatMessageReactionSingle adds a reaction while enforcing the one-reaction-per-user
// policy. The message row is locked FOR UPDATE on the caller's transaction so concurrent adds
// for the same message serialize; a same-emoji retry returns the existing row and a
// different-emoji reaction returns ErrChatReactionConflict.
func (h *CommonPersistenceHandler[U]) AddChatMessageReactionSingle(ctx context.Context, messageID uint64, userID uint64, emoji string) (*common_persistence_models.ChatMessageReaction, error) {
	db := h.getDBFromContext(ctx)

	// Serialize per message: the row lock is held until the request transaction commits, so a
	// concurrent add for the same user observes the winner's committed reaction below.
	var locked common_persistence_models.ChatMessage
	if err := db.Clauses(clause.Locking{Strength: "UPDATE"}).
		Select("id").
		Where("id = ?", messageID).
		First(&locked).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrChatMessageNotFound
		}
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "lock_chat_message_for_reaction"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.Uint64(appLogger.KeyUserID, userID))
	}

	var existing common_persistence_models.ChatMessageReaction
	err := db.Preload(common_persistence_models.PreloadUser).
		Where("message_id = ? AND user_id = ?", messageID, userID).
		First(&existing).Error
	switch {
	case err == nil:
		if existing.Emoji == emoji {
			// Idempotent retry of the same reaction.
			return &existing, nil
		}
		return nil, ErrChatReactionConflict
	case !errors.Is(err, gorm.ErrRecordNotFound):
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "load_existing_chat_reaction"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.Uint64(appLogger.KeyUserID, userID))
	}

	reaction := &common_persistence_models.ChatMessageReaction{
		MessageID: messageID,
		UserID:    userID,
		Emoji:     emoji,
	}
	if err := db.Create(reaction).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "add_single_chat_reaction"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyEmoji, emoji))
	}

	// Only the user relation needs hydrating for the response/event payload.
	var user common_persistence_models.User
	if err := db.Where("id = ?", userID).First(&user).Error; err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, chatPersistenceDatabaseError(err,
				zap.String(logkeys.Stage, "load_single_chat_reaction_user"),
				zap.Uint64(appLogger.KeyMessageID, messageID),
				zap.Uint64(appLogger.KeyUserID, userID))
		}
	} else {
		reaction.User = user
	}

	return reaction, nil
}

// RemoveChatMessageReaction executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) RemoveChatMessageReaction(ctx context.Context, messageID uint64, userID uint64, emoji string) error {
	db := h.getDBFromContext(ctx)

	result := db.Where("message_id = ? AND user_id = ? AND emoji = ?", messageID, userID, emoji).Delete(&common_persistence_models.ChatMessageReaction{})
	if result.Error != nil {
		return chatPersistenceDatabaseError(result.Error,
			zap.String(logkeys.Stage, "remove_chat_reaction"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyEmoji, emoji))
	}
	if result.RowsAffected == 0 {
		return ErrChatReactionNotFound
	}
	return nil
}

// ListChatMessageReactions executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) ListChatMessageReactions(
	ctx context.Context,
	messageID uint64,
	pagination *proto_models.PaginationRequest,
	emojiFilter *string,
) ([]*common_persistence_models.ChatMessageReaction, *proto_models.PaginationResponse, error) {
	db := h.getDBFromContext(ctx)

	if err := requirePageLimit("list_chat_message_reactions", pagination); err != nil {
		return nil, nil, err
	}
	page, limit := pagination.GetPage(), pagination.GetLimit()
	offset := common_util.PaginationOffset(page, limit)

	query := db.Model(&common_persistence_models.ChatMessageReaction{}).
		Where("message_id = ?", messageID)
	if emojiFilter != nil && strings.TrimSpace(*emojiFilter) != "" {
		query = query.Where("emoji = ?", strings.TrimSpace(*emojiFilter))
	}

	includeTotalCount := common_util.ShouldIncludeTotalCount(pagination)
	var totalCount int64
	if includeTotalCount {
		if err := query.Count(&totalCount).Error; err != nil {
			return nil, nil, chatPersistenceDatabaseError(err,
				zap.String(logkeys.Stage, "count_chat_reactions"),
				zap.Uint64(appLogger.KeyMessageID, messageID))
		}
	}

	var reactions []*common_persistence_models.ChatMessageReaction
	if err := query.
		Preload(common_persistence_models.PreloadUser).
		Order("created_at DESC").
		Offset(offset).
		Limit(common_util.PaginationQueryLimit(limit, includeTotalCount)).
		Find(&reactions).Error; err != nil {
		return nil, nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_chat_reactions"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.Uint32(appLogger.KeyPage, page),
			zap.Uint32(logkeys.Limit, limit))
	}
	reactions, hasNextPage := common_util.TrimPaginationProbe(reactions, limit, includeTotalCount)

	var paginationResponse *proto_models.PaginationResponse
	if includeTotalCount {
		paginationResponse = common_util.CreatePaginationResponse(totalCount, limit, page)
	} else {
		paginationResponse = common_util.CreatePaginationResponseWithoutTotalCount(limit, page, hasNextPage)
	}
	return reactions, paginationResponse, nil
}
