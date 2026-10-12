package common_persistence

import (
	"context"
	"errors"
	"fmt"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// ChatMessageVisibilityMode is the resolved visibility mode for a chat-message read.
//
// The service layer resolves the caller/request into one of these modes (after any
// permission checks) and passes it down through ChatMessageReadOptions. Persistence only
// applies the corresponding filter; it never decides whether an actor is allowed to see
// hidden rows (Decision 7).
type ChatMessageVisibilityMode uint8

const (
	// ChatMessageVisibilityVisibleToActor is the default for browser/user reads: rows with
	// hidden_from_users = TRUE are filtered out server-side.
	ChatMessageVisibilityVisibleToActor ChatMessageVisibilityMode = 1

	// ChatMessageVisibilityIncludeHiddenIfAuthorized is selected only when a read explicitly
	// requested include_hidden=true AND the service already verified authority. No
	// hidden filter is applied. Persistence trusts that the authority check happened upstream.
	ChatMessageVisibilityIncludeHiddenIfAuthorized ChatMessageVisibilityMode = 2

	// ChatMessageVisibilityServiceTranscript is internal-only (GetContextWindow, context
	// building, compaction, bot resume). It bypasses hidden_from_users filtering but stays
	// scoped to the channel/thread/run being reconstructed. Not exposed through public APIs.
	ChatMessageVisibilityServiceTranscript ChatMessageVisibilityMode = 3
)

// ChatMessageReadOptions carries the resolved read intent from the service layer down to
// persistence. Persistence applies the filters; it makes no authority decision.
type ChatMessageReadOptions struct {
	// VisibilityMode selects how hidden_from_users rows are treated. A zero value is normalized
	// to ChatMessageVisibilityVisibleToActor (the safe default) so a forgotten mode never leaks
	// hidden rows.
	VisibilityMode ChatMessageVisibilityMode

	// ChannelID, when non-zero, constrains the read to the channel snapshot that the service
	// authorized. It is used by public hidden get-by-ID reads to keep channel discovery and the
	// payload query fail-closed if the row changes between those operations.
	ChannelID uint64

	// IncludeDeleted, when true, reads soft-deleted rows (GORM Unscoped). Preserves the existing
	// includeDeleted semantics of the chat read methods.
	IncludeDeleted bool

	// ModelContextProjection hydrates only sender identity and ordered embeds for internal model
	// context. Reactions, mentions, and avatars are display data and are omitted.
	ModelContextProjection bool
}

// normalize returns the options with a zero VisibilityMode coerced to the safe default. This
// guarantees "forgot to set the mode" can never mean "show hidden rows."
func (o ChatMessageReadOptions) normalize() ChatMessageReadOptions {
	if o.VisibilityMode == 0 {
		o.VisibilityMode = ChatMessageVisibilityVisibleToActor
	}
	return o
}

// hidesHiddenRows reports whether this mode filters hidden_from_users rows server-side.
func (m ChatMessageVisibilityMode) hidesHiddenRows() bool {
	// Only the actor-facing default hides rows; both the authorized include-hidden mode and the
	// internal transcript mode read hidden rows.
	return m == ChatMessageVisibilityVisibleToActor
}

// applyChatMessageReadOptions applies the resolved read options to a chat_messages query: it
// scopes deleted-row visibility and applies the hidden_from_users filter for the actor-facing
// mode. exclude_from_ai_context is intentionally NOT filtered here — that is the LLM-context
// builder's concern, applied after fetch (Decision 4/7).
func applyChatMessageReadOptions(db *gorm.DB, opts ChatMessageReadOptions) *gorm.DB {
	opts = opts.normalize()
	if opts.IncludeDeleted {
		db = db.Unscoped()
	}
	if opts.VisibilityMode.hidesHiddenRows() {
		// NULL hidden_from_users (legacy / per-kind default unset) is treated as visible; only an
		// explicit TRUE hides a row.
		db = db.Where(fmt.Sprintf("%s.hidden_from_users IS NOT TRUE", common_persistence_models.TableNameChatMessages))
	}
	if opts.ChannelID != 0 {
		db = db.Where(fmt.Sprintf("%s.channel_id = ?", common_persistence_models.TableNameChatMessages), opts.ChannelID)
	}
	// Provider-state rows are internal protocol data, not privileged diagnostics. Even authorized
	// include-hidden reads must not return their attachment references or hashes.
	if opts.VisibilityMode != ChatMessageVisibilityServiceTranscript {
		db = db.Where(fmt.Sprintf("%s.kind <> ?", common_persistence_models.TableNameChatMessages), proto_models.MessageKind_MESSAGE_KIND_PROVIDER_STATE)
	}
	return db
}

// chatMainTimelinePredicate selects main-timeline rows: roots stored with NULL or 0 parents plus
// thread replies broadcast to the channel. Shared verbatim by the query scope and the partial
// index so the planner can match them.
const chatMainTimelinePredicate = "(parent_message_id IS NULL OR parent_message_id = 0 OR broadcast_to_channel)"

// applyChatMessageThreadScope applies the canonical channel + thread-scope predicate used by
// every chat-message timeline/thread query, reusing the exact 0<->NULL convention from
// ListChatMessagesWithOptions so a run's ParentMessageID (uint64, 0 = main timeline) never silently misses
// main-timeline rows where ChatMessage.parent_message_id is stored as NULL.
//
//   - parentMessageID == 0 -> main timeline: parent_message_id IS NULL OR = 0, plus thread
//     replies flagged broadcast_to_channel (one row shown in both its thread and the channel)
//   - parentMessageID != 0 -> thread: parent_message_id = ? OR id = ? (the root is included so
//     thread views show the message they hang off, matching ListChatMessagesWithOptions)
//
// The main-timeline string must stay byte-identical to the WHERE clause of the partial index
// idx_chat_messages_channel_main_timeline_id (ensurePerformanceIndexes): Postgres only uses a
// partial index when it can prove the query predicate implies the index predicate, and an
// identical expression is the case it proves reliably.
func applyChatMessageThreadScope(db *gorm.DB, parentMessageID uint64) *gorm.DB {
	if parentMessageID == 0 {
		return db.Where(chatMainTimelinePredicate)
	}
	return db.Where("(parent_message_id = ? OR id = ?)", parentMessageID, parentMessageID)
}

// Visibility-aware reads; the service resolves authority into ChatMessageReadOptions (resolveReadOptions).

// GetChatMessageChannelID resolves only the channel scope needed to authorize a public hidden
// get-by-ID. It deliberately does not load message content or associations, and normal GORM soft
// deletion still applies.
func (h *CommonPersistenceHandler[U]) GetChatMessageChannelID(ctx context.Context, messageID uint64) (uint64, error) {
	var row struct {
		ChannelID uint64 `gorm:"column:channel_id"`
	}
	result := h.getDBFromContext(ctx).
		Model(&common_persistence_models.ChatMessage{}).
		Select("channel_id").
		Where("id = ?", messageID).
		Take(&row)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return 0, ErrChatMessageNotFound
	}
	if result.Error != nil {
		return 0, chatPersistenceDatabaseError(result.Error,
			zap.String(logkeys.Stage, "resolve_chat_message_channel"),
			zap.Uint64(appLogger.KeyMessageID, messageID))
	}
	return row.ChannelID, nil
}

// GetChatMessageByIDWithOptions fetches a message by id, applying the resolved read options
// (hidden-row filter + deleted-row scope). Returns ErrChatMessageNotFound when the row is absent
// or filtered out by the visibility mode.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - messageID: The message identifier.
//   - opts: Resolved read options.
//
// Returns:
//   - result1: The message, or nil.
//   - result2: ErrChatMessageNotFound / ErrDatabaseError.
func (h *CommonPersistenceHandler[U]) GetChatMessageByIDWithOptions(
	ctx context.Context,
	messageID uint64,
	opts ChatMessageReadOptions,
) (*common_persistence_models.ChatMessage, error) {
	db := h.getDBFromContext(ctx)
	db = applyChatMessageReadOptions(db, opts)

	var msg common_persistence_models.ChatMessage
	err := preloadChatMessageAssociations(db).
		Where("id = ?", messageID).
		First(&msg).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrChatMessageNotFound
		}
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "get_chat_message"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.Uint8(appLogger.KeyVisibilityMode, uint8(opts.normalize().VisibilityMode)),
			zap.Bool(appLogger.KeyIncludeDeleted, opts.IncludeDeleted))
	}
	if err := h.attachChatMessageToolCalls(ctx, []*common_persistence_models.ChatMessage{&msg}); err != nil {
		return nil, err
	}
	return &msg, nil
}

// BatchGetChatMessagesWithOptions fetches multiple messages by id within a channel, applying the
// resolved read options. Hidden rows filtered by the visibility mode are reported as missing
// (consistent with the existing "not visible" missing-id semantics).
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - channelID: The channel identifier.
//   - messageIDs: The message identifiers.
//   - opts: Resolved read options.
//
// Returns:
//   - result1: The found messages.
//   - result2: The message IDs not found (or not visible under the mode).
//   - result3: ErrDatabaseError.
func (h *CommonPersistenceHandler[U]) BatchGetChatMessagesWithOptions(
	ctx context.Context,
	channelID uint64,
	messageIDs []uint64,
	opts ChatMessageReadOptions,
) ([]*common_persistence_models.ChatMessage, []uint64, error) {
	db := h.getDBFromContext(ctx)
	db = applyChatMessageReadOptions(db, opts)

	var messages []*common_persistence_models.ChatMessage
	if len(messageIDs) == 0 {
		return messages, []uint64{}, nil
	}

	if err := preloadChatMessageAssociations(db).
		Where("channel_id = ? AND id IN ?", channelID, messageIDs).
		Find(&messages).Error; err != nil {
		return nil, nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "batch_get_chat_messages"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Int(appLogger.KeyMessageCount, len(messageIDs)),
			zap.Uint8(appLogger.KeyVisibilityMode, uint8(opts.normalize().VisibilityMode)),
			zap.Bool(appLogger.KeyIncludeDeleted, opts.IncludeDeleted))
	}

	if err := h.attachChatMessageToolCalls(ctx, messages); err != nil {
		return nil, nil, err
	}

	found := make(map[uint64]struct{}, len(messages))
	for _, m := range messages {
		found[m.ID] = struct{}{}
	}
	missing := make([]uint64, 0)
	for _, id := range messageIDs {
		if _, ok := found[id]; !ok {
			missing = append(missing, id)
		}
	}
	return messages, missing, nil
}

// ListChatMessagesWithOptions lists messages for a channel/timeline with cursor semantics,
// applying the resolved read options (hidden-row filter + deleted-row scope). The visibility mode
// replaces the previous includeDeleted-only behavior; VisibleToActor additionally filters hidden
// rows server-side.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - channelID: The channel identifier.
//   - pagination: Cursor pagination (cursor is a message-id boundary).
//   - parentMessageID: Thread scope (0 = main timeline).
//   - opts: Resolved read options.
//   - sort: Optional sort direction.
//   - sortBy: Sort key (defaults to id).
//
// Returns:
//   - result1: The page of messages.
//   - result2: Pagination response.
//   - result3: ErrDatabaseError.
func (h *CommonPersistenceHandler[U]) ListChatMessagesWithOptions(
	ctx context.Context,
	channelID uint64,
	pagination *proto_models.PaginationRequest,
	parentMessageID uint64,
	opts ChatMessageReadOptions,
	sort *proto_models.SortRequest,
	sortBy proto_models.ListMessagesSortBy,
) ([]*common_persistence_models.ChatMessage, *proto_models.PaginationResponse, error) {
	db := h.getDBFromContext(ctx)
	db = applyChatMessageReadOptions(db, opts)

	if err := requirePageLimit("list_chat_messages", pagination); err != nil {
		return nil, nil, err
	}
	limit, cursor := pagination.GetLimit(), pagination.GetCursor()

	sortDesc := true
	if sort != nil {
		sortDesc = sort.SortDesc
	}
	if sortBy == proto_models.ListMessagesSortBy_LIST_MESSAGES_SORT_BY_UNSPECIFIED {
		sortBy = proto_models.ListMessagesSortBy_LIST_MESSAGES_SORT_BY_ID
	}

	query := db.Model(&common_persistence_models.ChatMessage{}).
		Where("channel_id = ?", channelID)
	query = applyChatMessageThreadScope(query, parentMessageID)

	if cursor != 0 {
		if sortDesc {
			query = query.Where("id < ?", cursor)
		} else {
			query = query.Where("id > ?", cursor)
		}
	}

	order := "id DESC"
	if !sortDesc {
		order = "id ASC"
	}

	var rows []common_persistence_models.ChatMessage
	if err := preloadChatMessageAssociations(query).
		Order(order).
		Limit(int(limit + 1)).
		Find(&rows).Error; err != nil {
		return nil, nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_chat_messages"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Uint64(appLogger.KeyParentMessageID, parentMessageID),
			zap.Uint64(appLogger.KeyCursor, cursor),
			zap.Uint32(logkeys.Limit, limit),
			zap.Uint8(appLogger.KeyVisibilityMode, uint8(opts.normalize().VisibilityMode)),
			zap.Bool(appLogger.KeyIncludeDeleted, opts.IncludeDeleted))
	}

	hasNext := false
	if uint32(len(rows)) > limit {
		hasNext = true
		rows = rows[:limit]
	}

	out := make([]*common_persistence_models.ChatMessage, 0, len(rows))
	for i := range rows {
		out = append(out, &rows[i])
	}
	if err := h.attachChatMessageToolCalls(ctx, out); err != nil {
		return nil, nil, err
	}

	var nextCursor uint64
	if hasNext && len(rows) > 0 {
		nextCursor = rows[len(rows)-1].ID
	}

	resp := &proto_models.PaginationResponse{
		CurrentPage:        0,
		TotalPages:         0,
		TotalItems:         0,
		ItemsPerPage:       limit,
		HasNextPage:        hasNext,
		HasPrevPage:        cursor != 0,
		NextCursor:         nextCursor,
		PrevCursor:         0,
		TotalCountIncluded: false,
	}
	return out, resp, nil
}
