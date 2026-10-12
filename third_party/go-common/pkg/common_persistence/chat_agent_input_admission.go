package common_persistence

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
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

// ============================================================================
// CHAT: AGENT INPUT QUEUE AND DELIVERY
// ============================================================================
//
// Human input that cannot reach an agent session when it is sent, because a run is active or the
// session is suspended, waits as a ChatAgentQueuedInput, not as a chat message. Delivery creates
// the message when the agent receives the input: at the start of an IMMEDIATE run's next step
// (ReserveChatAgentRunStepAndAdmit), all at once in one follow-up run when the session's active run
// ends (the terminal drain), at Resume, and when a send finds an idle session with input still
// queued (DrainChatAgentInput). Archiving the session or disabling its automatic runs posts the
// queue as ordinary messages. A delivered message gets a fresh ID, so the transcript and the model
// context both read the input where it was delivered, after the agent output that preceded it, and
// the context before it never changes.
//
// Lock order is run row, then channel row, then session rows, in every path. Every writer of a
// session's queue holds that session's lock: a send enqueues under the locks it took up front,
// edits and removals take the admission locks, and delivery reads and empties the queue under
// them, so a session's queue is exact while its lock is held. Every path that locks both a
// delegated child's session and its parent's holds the channel lock first (NO KEY UPDATE, or FOR
// UPDATE for a spawn), so their order does not matter: sends and settlement lock the child before
// the parent, Stop suspends the children after the parent. A send that may reach agent sessions
// takes all of its locks before it writes, in the order run (FOR KEY SHARE), channel, then each
// session it reaches, as input or as a delegated child's report, FOR UPDATE. It reads which
// sessions it reaches only after the channel lock, so a session created meanwhile (under the
// channel's FOR UPDATE) either committed before that read or waits for the send. Sends, queue
// edits and removals, step admission, the FINAL seal, the terminal drain, Stop and Resume all lock
// the channel FOR NO KEY UPDATE: the lock excludes the others and every last-message update until
// commit, yet stays compatible with concurrent message inserts, whose foreign keys take FOR KEY
// SHARE. It also queues behind the FOR UPDATE of a location change (session creation, thread and
// organization changes). Message deletion takes the same NO KEY UPDATE, since it refreshes the
// channel's last message. Every edit (EditChatMessage) takes the channel NO KEY UPDATE before its
// message, which it locks FOR NO KEY UPDATE, never FOR UPDATE: an edit never holds a message while
// it waits on the channel, and never blocks the foreign-key check of a reply to the message that a
// channel-locked send or delivery is inserting.
//
// Neither FOR KEY SHARE for the admission paths nor FOR UPDATE for sends would be safe on this hot
// row. While a last-message update is in flight, a queued FOR UPDATE locks the newer row version
// before it waits behind the older one, and a transaction that holds KEY SHARE on the older version
// then waits on the queued locker at its next foreign-key check. Every lock above is compatible
// with those checks, and each is taken once, before the transaction's other writes: a send never
// locks the channel again after it wrote (CreateChatAgentRunForSend), nor any session it did not
// lock up front. Settlement may update the channel's last message after it locked a session (a
// delegated child's notice, delivered input): it already holds the channel row.

// ErrChatAgentQueuedInputNotQueued reports that a queued input is no longer in its session's
// queue: delivery turned it into a message (or discarded it), or someone removed it. An edit or a
// removal that loses that race returns it, and the caller re-reads the queue.
var ErrChatAgentQueuedInputNotQueued = errors.New("chat agent input is no longer queued")

// ChatAgentSessionInputState is what routing one human message to one agent session needs, read
// under the locks LockChatAgentSessionsForSend takes.
type ChatAgentSessionInputState struct {
	// ActiveRun is the session's RUNNING, WAITING_FOR_APPROVAL or RETRYABLE run; nil when idle.
	ActiveRun *common_persistence_models.ChatAgentRun
	// PendingCount is how many inputs are queued for the session, held while it is suspended.
	PendingCount uint32
	// GraphPoolExhausted reports an idle session whose graph's budget pool is used up: it starts no
	// run, so input waits in its queue until SetChatAgentGraphBudget drains it.
	GraphPoolExhausted bool
	// GraphPooled reports an idle session whose graph has a budget pool: the run a message starts
	// there carries no library work ceilings, like the follow-ups persistence mints.
	GraphPooled bool
	// Policy governs a message that cannot start a run directly: the active run's captured policy,
	// or the session's active version while it is idle. It is read only when the session is busy,
	// suspended, or already has queued input, and is zero otherwise.
	Policy common_persistence_models.ChatAgentInteractionPolicy
}

// ChatAgentInputReceipt is what became of a human message for one agent session it reached: it
// started a run (RUN_STARTED), or it is queued input delivered into a run (ADMITTED).
type ChatAgentInputReceipt struct {
	AgentSessionID uint64
	State          proto_models.ChatAgentInputReceiptState
	AgentRunID     uint64
}

// ChatAgentSessionSendLock is one agent session a send locks. Input asks for its input state: the
// message reaches the session as input. A session that a delegated child's report only wakes, or
// the child itself, is locked without it.
type ChatAgentSessionSendLock struct {
	Session *common_persistence_models.ChatAgentSession
	Input   bool
}

// ChatAgentQueuedInputContent is the part of a queued input its sender may replace until delivery
// (EditChatAgentQueuedInput), which writes it as the message's content, mentions and attachments.
// The caller validated it exactly as a send's.
type ChatAgentQueuedInputContent struct {
	Content        string
	ContentType    proto_models.MessageContentType
	MentionUserIDs []uint64
	AttachmentIDs  []uint64
}

// NewChatAgentQueuedInput is one person's input to queue for an agent session (EnqueueChatAgentInput).
type NewChatAgentQueuedInput struct {
	// Session is the session the input waits for, as LockChatAgentSessionsForSend read it under its
	// lock; the queued input takes its location.
	Session          *common_persistence_models.ChatAgentSession
	SenderID         uint64
	ReplyToMessageID *uint64
	// ClientMessageID is the send's idempotency key; empty stores none. The caller looked it up with
	// LockChatClientMessageID first.
	ClientMessageID string
	ChatAgentQueuedInputContent
	// Embeds are fixed when the input is queued; an edit keeps them.
	Embeds []CreateChatMessageEmbed
}

// ChatAgentDeliveredInput is one queued input delivery turned into a message.
type ChatAgentDeliveredInput struct {
	QueuedInputID uint64
	MessageID     uint64
}

// ChatAgentQueuedInputDelivery is what one delivery of a session's queued input wrote, for the
// caller's post-commit events.
type ChatAgentQueuedInputDelivery struct {
	// Delivered pairs each queued input delivery turned into a message with that message, in queue
	// order, which is also message ID order.
	Delivered []ChatAgentDeliveredInput
	// Discarded counts the queued inputs delivery dropped instead, because their sender could no
	// longer send to the channel.
	Discarded int
}

// Changed reports whether the delivery took anything out of the queue.
func (d ChatAgentQueuedInputDelivery) Changed() bool {
	return len(d.Delivered) > 0 || d.Discarded > 0
}

// MessageIDs returns the messages delivery created, in queue order.
func (d ChatAgentQueuedInputDelivery) MessageIDs() []uint64 {
	return common_util.ApplyToEach(d.Delivered, func(delivered ChatAgentDeliveredInput) uint64 { return delivered.MessageID })
}

// MessageFor returns the message queuedInputID became, or zero when this delivery did not turn it
// into one (it discarded the input, or never read it).
func (d ChatAgentQueuedInputDelivery) MessageFor(queuedInputID uint64) uint64 {
	for _, delivered := range d.Delivered {
		if delivered.QueuedInputID == queuedInputID {
			return delivered.MessageID
		}
	}
	return 0
}

// ChatAgentMessageRecorder records a durable notification for delivered queued input or a
// delegation notice. txCtx carries the source transaction, so recording and the source message
// commit or roll back together. An error aborts the entire source operation.
type ChatAgentMessageRecorder func(txCtx context.Context, messageID uint64) error

// SetChatAgentMessageRecorder installs the recorder for persistence-owned agent messages at
// startup. The chat service supplies its notification sink; nil records nothing.
func (h *CommonPersistenceHandler[U]) SetChatAgentMessageRecorder(recorder ChatAgentMessageRecorder) {
	if recorder == nil {
		h.agentMessageRecorder.Store(nil)
		return
	}
	h.agentMessageRecorder.Store(&recorder)
}

func (h *CommonPersistenceHandler[U]) recordChatAgentMessage(ctx context.Context, messageID uint64) error {
	recorder := h.agentMessageRecorder.Load()
	if recorder == nil {
		return nil
	}
	// Without the source transaction in ctx the sink would miss the uncommitted message and
	// either record nothing or commit independently of a source operation that later rolls back.
	if ambient, ok := scontext.GetTransaction(ctx); !ok || ambient == nil {
		return appLogger.InvariantError("chat_agent_message_recorded_in_source_transaction",
			ErrChatAgentSessionTransactionRequired,
			zap.String(logkeys.Stage, "record_chat_agent_message"),
			zap.Uint64(appLogger.KeyMessageID, messageID))
	}
	return (*recorder)(ctx, messageID)
}

// queuedChatAgentInput is one queued input as delivery reads it: the row, and whether its sender
// may still send to the session's channel.
type queuedChatAgentInput struct {
	common_persistence_models.ChatAgentQueuedInput `gorm:"embedded"`
	Deliverable                                    bool
}

// LockChatChannelForSend takes the first locks of a send to a channel where agent sessions may take
// input, before it reads which sessions the message reaches or writes anything, in the lock order
// run -> channel: the run the message is tagged with (agentRunID, a delegated child's report; zero
// when untagged) FOR KEY SHARE, then the channel FOR NO KEY UPDATE. The channel lock queues behind
// a location change in flight (session creation holds the channel FOR UPDATE), so the sessions the
// caller reads after it are every session committed before the send's message. The caller keeps
// its transaction open until the message and its runs, or the queued input or wakes, are written,
// so the send is serialized against step admission, the FINAL seal, terminal drains, queue edits,
// and other such sends to the channel.
func (h *CommonPersistenceHandler[U]) LockChatChannelForSend(ctx context.Context, channelID, agentRunID uint64) error {
	if tx, ok := scontext.GetTransaction(ctx); !ok || tx == nil {
		return ErrChatAgentSessionTransactionRequired
	}
	db := h.getDBFromContext(ctx)
	if agentRunID != 0 {
		var run common_persistence_models.ChatAgentRun
		if err := db.Clauses(clause.Locking{Strength: "KEY SHARE"}).Select("id").Where("id = ?", agentRunID).Take(&run).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrChatAgentRunNotFound
			}
			return chatPersistenceDatabaseError(err,
				zap.String(logkeys.Stage, "lock_chat_agent_run_for_send"),
				zap.Uint64(appLogger.KeyAgentRunID, agentRunID))
		}
	}
	if err := lockLiveChatChannelForAgentInputTx(db, channelID); err != nil {
		if errors.Is(err, ErrChatChannelNotFound) {
			return err
		}
		return chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "lock_chat_channel_for_send"),
			zap.Uint64(appLogger.KeyChannelID, channelID))
	}
	return nil
}

// LockChatAgentSessionsForSend takes the last locks of a send that reaches agent sessions, under
// the locks LockChatChannelForSend took: each session the message reaches FOR UPDATE, in the given
// order, which puts thread sessions before the main timeline. Each session is re-read in place
// under its lock, and the input state of each Input lock is returned at the same index (zero for
// the others).
func (h *CommonPersistenceHandler[U]) LockChatAgentSessionsForSend(
	ctx context.Context,
	channelID uint64,
	locks []ChatAgentSessionSendLock,
) ([]ChatAgentSessionInputState, error) {
	if tx, ok := scontext.GetTransaction(ctx); !ok || tx == nil {
		return nil, ErrChatAgentSessionTransactionRequired
	}
	db := h.getDBFromContext(ctx)
	states := make([]ChatAgentSessionInputState, len(locks))
	for i, lock := range locks {
		session := lock.Session
		if session == nil || session.ChannelID != channelID {
			return nil, ErrChatAgentSessionScopeInvalid
		}
		if err := db.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", session.ID).Take(session).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil, ErrChatAgentSessionNotFound
			}
			return nil, chatPersistenceDatabaseError(err,
				zap.String(logkeys.Stage, "lock_chat_agent_session_for_input"),
				zap.Uint64(appLogger.KeyAgentSessionID, session.ID))
		}
		if !lock.Input {
			continue
		}
		state, err := chatAgentSessionInputStateTx(db, session)
		if err != nil {
			return nil, chatPersistenceDatabaseError(err,
				zap.String(logkeys.Stage, "read_chat_agent_session_input_state"),
				zap.Uint64(appLogger.KeyAgentSessionID, session.ID))
		}
		states[i] = state
	}
	return states, nil
}

// chatAgentSessionInputStateTx reads a session's input state under the send's locks. The policy in
// force is the active run's captured version, since that run is the one input would join, else the
// session's active version (as chatAgentSessionInteractionQuery decides it). It is left zero for an
// idle, unsuspended session with nothing queued and room in its graph's pool, where a message starts
// a run directly.
func chatAgentSessionInputStateTx(db *gorm.DB, session *common_persistence_models.ChatAgentSession) (ChatAgentSessionInputState, error) {
	var state ChatAgentSessionInputState
	var active []*common_persistence_models.ChatAgentRun
	if err := db.Where("agent_session_id = ? AND status IN ?", session.ID, ActiveChatAgentRunStatuses()).
		Limit(1).Find(&active).Error; err != nil {
		return state, err
	}
	var queued int64
	if err := db.Model(&common_persistence_models.ChatAgentQueuedInput{}).Where("agent_session_id = ?", session.ID).Count(&queued).Error; err != nil {
		return state, err
	}
	state.PendingCount = uint32(queued)
	version := session.ActiveConfigVersion
	if len(active) > 0 {
		state.ActiveRun = active[0]
		if captured := state.ActiveRun.AgentSessionConfigVersion; captured != nil {
			version = *captured
		}
	} else {
		pool, err := loadChatAgentGraphBudgetTx(db, session.GraphRootID())
		if err != nil {
			return state, err
		}
		state.GraphPooled, state.GraphPoolExhausted = pool != nil, graphBudgetExhaustedError(pool) != nil
		if !session.IsSuspended() && queued == 0 && !state.GraphPoolExhausted {
			return state, nil
		}
	}
	var err error
	if state.Policy, err = chatAgentInteractionPolicyTx(db, session.ID, version); err != nil {
		return state, err
	}
	return state, nil
}

// EnqueueChatAgentInput queues a person's input for an agent session at the session's location, in
// the send's transaction, while the caller still holds the locks LockChatChannelForSend and
// LockChatAgentSessionsForSend took (in.Session's among them), so the queue position is the order
// the sends locked the session in. No chat message is written: delivery creates it. The caller
// checked the session's queue against chat_agent_max_pending_inputs under the same locks.
func (h *CommonPersistenceHandler[U]) EnqueueChatAgentInput(ctx context.Context, in NewChatAgentQueuedInput) (*common_persistence_models.ChatAgentQueuedInput, error) {
	if tx, ok := scontext.GetTransaction(ctx); !ok || tx == nil {
		return nil, ErrChatAgentSessionTransactionRequired
	}
	session := in.Session
	if session == nil || session.ID == 0 {
		return nil, ErrChatAgentSessionNotFound
	}
	queued := &common_persistence_models.ChatAgentQueuedInput{
		AgentSessionID:   session.ID,
		ChannelID:        session.ChannelID,
		ParentMessageID:  session.ParentMessageID,
		SenderID:         in.SenderID,
		ReplyToMessageID: in.ReplyToMessageID,
	}
	if in.ClientMessageID != "" {
		clientMessageID := in.ClientMessageID
		queued.ClientMessageID = &clientMessageID
	}
	in.ChatAgentQueuedInputContent.applyTo(queued)
	queued.Embeds = make(common_persistence_models.ChatAgentQueuedInputEmbeds, len(in.Embeds))
	for i, embed := range in.Embeds {
		queued.Embeds[i] = common_persistence_models.ChatAgentQueuedInputEmbed(embed)
	}
	if err := h.getDBFromContext(ctx).Create(queued).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "enqueue_chat_agent_input"),
			zap.Uint64(appLogger.KeyAgentSessionID, session.ID),
			zap.Uint64(appLogger.KeySenderID, in.SenderID))
	}
	return queued, nil
}

// applyTo writes the content into a queued input row.
func (c ChatAgentQueuedInputContent) applyTo(queued *common_persistence_models.ChatAgentQueuedInput) {
	queued.Content, queued.ContentType = c.Content, c.ContentType
	queued.MentionUserIDs = common_util.ConvertUint64SliceToPqInt64Array(c.MentionUserIDs)
	queued.AttachmentIDs = common_util.ConvertUint64SliceToPqInt64Array(c.AttachmentIDs)
}

// GetChatAgentQueuedInput reads one queued input with its sender, or returns
// ErrChatAgentQueuedInputNotQueued when it is no longer queued.
func (h *CommonPersistenceHandler[U]) GetChatAgentQueuedInput(ctx context.Context, queuedInputID uint64) (*common_persistence_models.ChatAgentQueuedInput, error) {
	var queued common_persistence_models.ChatAgentQueuedInput
	if err := preloadChatAgentQueuedInputSender(h.getDBFromContext(ctx)).Where("id = ?", queuedInputID).Take(&queued).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrChatAgentQueuedInputNotQueued
		}
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "get_chat_agent_queued_input"),
			zap.Uint64(appLogger.KeyQueuedInputID, queuedInputID))
	}
	return &queued, nil
}

// ListChatAgentQueuedInputs pages an agent session's queue in queue order, each input with its
// sender, at most limit per page, keyed by queued input ID. Delivery and removal between pages only
// remove rows and never move a surviving one behind the cursor.
func (h *CommonPersistenceHandler[U]) ListChatAgentQueuedInputs(ctx context.Context, sessionID uint64, pagination *proto_models.PaginationRequest, limit int) ([]*common_persistence_models.ChatAgentQueuedInput, *proto_models.PaginationResponse, error) {
	if limit <= 0 {
		return nil, nil, appLogger.InvariantError("chat_agent_queued_inputs_limit_positive", ErrDatabaseError,
			zap.Uint64(appLogger.KeyAgentSessionID, sessionID), zap.Int(logkeys.Limit, limit))
	}
	var queued []*common_persistence_models.ChatAgentQueuedInput
	if err := preloadChatAgentQueuedInputSender(h.getDBFromContext(ctx)).
		Where("agent_session_id = ? AND id > ?", sessionID, pagination.GetCursor()).
		Order("id").Limit(common_util.QueryLimitWithProbe(limit)).Find(&queued).Error; err != nil {
		return nil, nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_chat_agent_queued_inputs"),
			zap.Uint64(appLogger.KeyAgentSessionID, sessionID))
	}
	page := &proto_models.PaginationResponse{
		ItemsPerPage: uint32(limit),
		HasNextPage:  len(queued) > limit,
		HasPrevPage:  pagination.GetCursor() != 0,
	}
	if page.HasNextPage {
		queued = queued[:limit]
		page.NextCursor = queued[limit-1].ID
	}
	return queued, page, nil
}

// preloadChatAgentQueuedInputSender loads a queued input's sender and avatar, as a message read
// loads its sender's.
func preloadChatAgentQueuedInputSender(db *gorm.DB) *gorm.DB {
	return db.Preload(common_persistence_models.PreloadSender).
		Preload(common_persistence_models.PreloadSenderProfilePictureAttachment, preloadLiveAttachment)
}

// SetChatAgentQueuedInputAttachmentIDs records the attachments a send created for the input it
// queued, keyed by the input (ObjectTypeChatQueuedInputAttachment), in the send's transaction and
// under the locks the send took, so delivery reads them with the input.
func (h *CommonPersistenceHandler[U]) SetChatAgentQueuedInputAttachmentIDs(ctx context.Context, queuedInputID uint64, attachmentIDs []uint64) error {
	if tx, ok := scontext.GetTransaction(ctx); !ok || tx == nil {
		return ErrChatAgentSessionTransactionRequired
	}
	result := h.getDBFromContext(ctx).Model(&common_persistence_models.ChatAgentQueuedInput{}).Where("id = ?", queuedInputID).
		Update("attachment_ids", common_util.ConvertUint64SliceToPqInt64Array(attachmentIDs))
	if result.Error != nil {
		return chatPersistenceDatabaseError(result.Error,
			zap.String(logkeys.Stage, "set_chat_agent_queued_input_attachment_ids"),
			zap.Uint64(appLogger.KeyQueuedInputID, queuedInputID))
	}
	if result.RowsAffected == 0 {
		return ErrChatAgentQueuedInputNotQueued
	}
	return nil
}

// EditChatAgentQueuedInput replaces a queued input's content, mentions and attachments under the
// admission locks, so the edit either lands before delivery reads the input or finds it gone, and
// returns the edited input; its embeds are kept. Uploads of the input that the new content no
// longer lists are abandoned (abandonChatAgentQueuedInputUploadsTx). It returns
// ErrChatAgentQueuedInputNotQueued when delivery or a removal took the input first, and
// ErrChatChannelArchived when the channel is archived under the locks.
func (h *CommonPersistenceHandler[U]) EditChatAgentQueuedInput(ctx context.Context, queuedInputID uint64, content ChatAgentQueuedInputContent) (*common_persistence_models.ChatAgentQueuedInput, error) {
	edited := &common_persistence_models.ChatAgentQueuedInput{}
	content.applyTo(edited)
	err := h.withQueuedChatAgentInputLocked(ctx, queuedInputID, func(tx *gorm.DB) (int64, error) {
		// Select writes the empty values (no text, no attachments) too; updated_at follows.
		result := tx.Model(edited).Clauses(clause.Returning{}).Where("id = ?", queuedInputID).
			Select("content", "content_type", "mention_user_ids", "attachment_ids").Updates(edited)
		if result.Error != nil || result.RowsAffected == 0 {
			return result.RowsAffected, result.Error
		}
		return result.RowsAffected, abandonChatAgentQueuedInputUploadsTx(tx, []uint64{queuedInputID}, edited.AttachmentIDs)
	})
	if err != nil {
		return nil, chatAgentQueuedInputError(err, "edit_chat_agent_queued_input", queuedInputID)
	}
	return edited, nil
}

// RemoveChatAgentQueuedInput takes a queued input out of its session's queue under the admission
// locks, abandons its pending uploads (abandonChatAgentQueuedInputUploadsTx), and returns it as it
// was, or returns ErrChatAgentQueuedInputNotQueued when delivery or another removal took it first,
// and ErrChatChannelArchived when the channel is archived under the locks.
func (h *CommonPersistenceHandler[U]) RemoveChatAgentQueuedInput(ctx context.Context, queuedInputID uint64) (*common_persistence_models.ChatAgentQueuedInput, error) {
	removed := &common_persistence_models.ChatAgentQueuedInput{}
	err := h.withQueuedChatAgentInputLocked(ctx, queuedInputID, func(tx *gorm.DB) (int64, error) {
		result := tx.Clauses(clause.Returning{}).Where("id = ?", queuedInputID).Delete(removed)
		if result.Error != nil || result.RowsAffected == 0 {
			return result.RowsAffected, result.Error
		}
		return result.RowsAffected, abandonChatAgentQueuedInputUploadsTx(tx, []uint64{queuedInputID}, nil)
	})
	if err != nil {
		return nil, chatAgentQueuedInputError(err, "remove_chat_agent_queued_input", queuedInputID)
	}
	return removed, nil
}

// LockChatAgentQueuedInput takes a queued input's admission locks (channel, then session) in the
// caller's transaction and reads the input as it stands under them, so what the transaction then
// writes builds on content no concurrent edit, removal or delivery changes before it commits. It
// returns ErrChatAgentQueuedInputNotQueued when the input left the queue, and ErrChatChannelArchived
// when the channel is archived under the locks. Requires a transaction.
func (h *CommonPersistenceHandler[U]) LockChatAgentQueuedInput(ctx context.Context, queuedInputID uint64) (*common_persistence_models.ChatAgentQueuedInput, error) {
	if tx, ok := scontext.GetTransaction(ctx); !ok || tx == nil {
		return nil, ErrChatAgentSessionTransactionRequired
	}
	db := h.getDBFromContext(ctx)
	var queued common_persistence_models.ChatAgentQueuedInput
	err := lockQueuedChatAgentInputTx(db, queuedInputID)
	if err == nil {
		err = db.Where("id = ?", queuedInputID).Take(&queued).Error
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		err = ErrChatAgentQueuedInputNotQueued
	}
	if err != nil {
		return nil, chatAgentQueuedInputError(err, "lock_chat_agent_queued_input", queuedInputID)
	}
	return &queued, nil
}

// withQueuedChatAgentInputLocked runs write, one statement on the queued input, in a transaction
// that first takes its session's admission locks (lockQueuedChatAgentInputTx). A write that
// affects no row means the input left the queue.
func (h *CommonPersistenceHandler[U]) withQueuedChatAgentInputLocked(ctx context.Context, queuedInputID uint64, write func(tx *gorm.DB) (int64, error)) error {
	return h.getDBFromContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockQueuedChatAgentInputTx(tx, queuedInputID); err != nil {
			return err
		}
		written, err := write(tx)
		if err != nil {
			return err
		}
		if written == 0 {
			return ErrChatAgentQueuedInputNotQueued
		}
		return nil
	})
}

// lockQueuedChatAgentInputTx takes the admission locks (channel, then session) of a queued input's
// session, or returns ErrChatAgentQueuedInputNotQueued when the input or its session is gone, and
// ErrChatChannelArchived when the channel, as read under its lock, is archived (archival freezes
// its queues). The input may still leave the queue before the locks are granted; a caller reads or
// writes it after.
func lockQueuedChatAgentInputTx(tx *gorm.DB, queuedInputID uint64) error {
	var location common_persistence_models.ChatAgentQueuedInput
	if err := tx.Select("agent_session_id", "channel_id").Where("id = ?", queuedInputID).Take(&location).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrChatAgentQueuedInputNotQueued
		}
		return err
	}
	session, archived, err := lockChatAgentSessionForAdmissionTx(tx, location.ChannelID, location.AgentSessionID)
	if err != nil {
		return err
	}
	if session == nil {
		return ErrChatAgentQueuedInputNotQueued
	}
	if archived {
		return ErrChatChannelArchived
	}
	return nil
}

// chatAgentQueuedInputError maps a queued-input write's error, keeping ErrChatAgentQueuedInputNotQueued
// and ErrChatChannelArchived.
func chatAgentQueuedInputError(err error, stage string, queuedInputID uint64) error {
	if errors.Is(err, ErrChatAgentQueuedInputNotQueued) || errors.Is(err, ErrChatChannelArchived) {
		return err
	}
	return chatPersistenceDatabaseError(err, zap.String(logkeys.Stage, stage), zap.Uint64(appLogger.KeyQueuedInputID, queuedInputID))
}

// DrainChatAgentInput delivers every input queued for an agent session into one new run when the
// session can take one (live, automatic, not suspended, in an unarchived channel, with no active
// run), and returns that run and the delivery; otherwise the run is nil and the input stays
// queued. A send uses it after it queued its input behind input already waiting in an idle
// session, so its input reaches the agent in order. It requires a transaction and takes the
// admission locks.
func (h *CommonPersistenceHandler[U]) DrainChatAgentInput(ctx context.Context, sessionID uint64) (*common_persistence_models.ChatAgentRun, ChatAgentQueuedInputDelivery, error) {
	if tx, ok := scontext.GetTransaction(ctx); !ok || tx == nil {
		return nil, ChatAgentQueuedInputDelivery{}, ErrChatAgentSessionTransactionRequired
	}
	db := h.getDBFromContext(ctx)
	fields := []zap.Field{zap.String(logkeys.Stage, "drain_chat_agent_input"), zap.Uint64(appLogger.KeyAgentSessionID, sessionID)}
	session, err := loadChatAgentSessionTx(db, sessionID)
	if err != nil {
		return nil, ChatAgentQueuedInputDelivery{}, chatPersistenceDatabaseError(err, fields...)
	}
	if session == nil {
		return nil, ChatAgentQueuedInputDelivery{}, ErrChatAgentSessionNotFound
	}
	run, delivery, err := h.drainChatAgentInputTx(ctx, db, session, 0)
	if err != nil {
		return nil, ChatAgentQueuedInputDelivery{}, chatPersistenceDatabaseError(err, fields...)
	}
	return run, delivery, nil
}

// GetChatAgentInputReceipts reports what became of a human message for every agent session it
// reached: RUN_STARTED for each run it triggered, and ADMITTED for the run its queued input was
// delivered into (a drained run's trigger is also delivered input, so it reports ADMITTED once). A
// run-tagged report that woke its parent reports nothing. Soft-deleted messages are included, for
// an idempotent replay of their send.
func (h *CommonPersistenceHandler[U]) GetChatAgentInputReceipts(ctx context.Context, messageID uint64) ([]ChatAgentInputReceipt, error) {
	var rows []struct {
		AgentSessionID uint64
		AgentRunID     uint64
		Admitted       bool
	}
	if err := h.getDBFromContext(ctx).Raw(`SELECT r.agent_session_id, r.id AS agent_run_id, (r.id = m.admitted_run_id) IS TRUE AS admitted
		FROM `+common_persistence_models.TableNameChatMessages+` m
		JOIN `+common_persistence_models.TableNameChatAgentRuns+` r ON r.id = m.admitted_run_id
			OR (r.triggering_message_id = m.id AND m.agent_run_id IS NULL AND m.kind IN @kinds AND m.role IN @roles)
		WHERE m.id = @id AND r.agent_session_id IS NOT NULL
		ORDER BY r.agent_session_id, r.id`,
		sql.Named("id", messageID), sql.Named("kinds", chatHumanInputKinds), sql.Named("roles", chatHumanInputRoles)).Scan(&rows).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "get_chat_agent_input_receipts"),
			zap.Uint64(appLogger.KeyMessageID, messageID))
	}
	receipts := make([]ChatAgentInputReceipt, len(rows))
	for i, row := range rows {
		// Decided in Go: the driver binds proto enums as text, which a SQL CASE cannot compare.
		state := proto_models.ChatAgentInputReceiptState_CHAT_AGENT_INPUT_RECEIPT_STATE_RUN_STARTED
		if row.Admitted {
			state = proto_models.ChatAgentInputReceiptState_CHAT_AGENT_INPUT_RECEIPT_STATE_ADMITTED
		}
		receipts[i] = ChatAgentInputReceipt{AgentSessionID: row.AgentSessionID, State: state, AgentRunID: row.AgentRunID}
	}
	return receipts, nil
}

// chatHumanInputKinds and chatHumanInputRoles are the kinds and roles of a human-authored row
// (legacy rows leave them unspecified).
var (
	chatHumanInputKinds = []proto_models.MessageKind{proto_models.MessageKind_MESSAGE_KIND_STANDARD}
	chatHumanInputRoles = []proto_models.MessageRole{proto_models.MessageRole_MESSAGE_ROLE_USER}
)

// ListChatAgentRunAdmittedMessageIDs lists, in delivery order, the messages queued input became
// when it was delivered into runID, up to throughMessageID, soft-deleted ones included. The
// partial index idx_chat_messages_admitted_run serves it.
func (h *CommonPersistenceHandler[U]) ListChatAgentRunAdmittedMessageIDs(ctx context.Context, runID, throughMessageID uint64) ([]uint64, error) {
	var ids []uint64
	if err := h.getDBFromContext(ctx).Unscoped().Model(&common_persistence_models.ChatMessage{}).
		Where("admitted_run_id = ? AND id <= ?", runID, throughMessageID).Order("id").Pluck("id", &ids).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_chat_agent_run_admitted_message_ids"),
			zap.Uint64(appLogger.KeyAgentRunID, runID))
	}
	return ids, nil
}

// ReserveChatAgentRunStepAndAdmit reserves one model step's generation for the run named by the
// context's drive lease. With admit, which the runtime sets when the run's captured input mode is
// IMMEDIATE, the same transaction also delivers every input queued for the run's session into the
// run, unless the session is suspended: each becomes a chat message the step's context reads.
// Only the step loop calls it, before it builds the step's context; compaction reserves through
// ReserveChatAgentRunBudget and never admits. A step admits only when its reservation leaves a
// generation under maxGenerations (chatAgentStepCanAdmit) and in its graph's pool
// (chatAgentGraphPoolHasGenerations), so a compaction that the delivered input may trigger cannot
// stop the run with that input unanswered. A false result reserved and delivered nothing, and
// input that is not delivered waits for the terminal drain; so does a refusal by the run's graph
// budget pool, returned as a *GraphBudgetExhaustedError. The delivery names the created messages,
// for the caller to announce after commit.
func (h *CommonPersistenceHandler[U]) ReserveChatAgentRunStepAndAdmit(
	ctx context.Context,
	runID uint64,
	maxGenerations uint64,
	maxToolCalls uint64,
	admit bool,
) (bool, ChatAgentQueuedInputDelivery, error) {
	reserved := false
	var delivery ChatAgentQueuedInputDelivery
	var waits []time.Duration
	err := h.transaction(ctx, func(txCtx context.Context, tx *gorm.DB) error {
		run, err := h.lockLeasedChatAgentRunTx(ctx, tx, runID, "id", "channel_id", "agent_session_id", "generation_count")
		if err != nil {
			return err
		}
		if reserved, err = reserveChatAgentRunBudgetTx(tx, runID, 1, 0, maxGenerations, maxToolCalls); err != nil || !reserved {
			return err
		}
		sessionID := common_util.Deref(run.AgentSessionID)
		if !admit || sessionID == 0 || !chatAgentStepCanAdmit(run.GenerationCount, maxGenerations) {
			return nil
		}
		// Nothing is usually queued, so an unlocked probe spares the step the admission locks.
		// Input committed after it is delivered by the next step, steered for by the final seal, or
		// drained when the run ends, each of which reads it under those locks. Input whose sender
		// lost access is left for a delivery or the terminal drain to discard.
		if waiting, err := chatAgentInputWaitsTx(tx, sessionID); err != nil || !waiting {
			return err
		}
		// A pooled run's limits are unbounded, so its graph's pool must keep that generation too.
		// The step's charge already holds the pool row, which the admission locks follow.
		if pool, err := loadChatAgentRunGraphBudgetTx(tx, runID); err != nil || !chatAgentGraphPoolHasGenerations(pool, 1) {
			return err
		}
		session, _, err := lockChatAgentSessionForAdmissionTx(tx, run.ChannelID, sessionID)
		if err != nil || session == nil || session.IsSuspended() {
			return err
		}
		queued, err := queuedChatAgentInputsTx(tx, session.ID)
		if err != nil || len(queued) == 0 {
			return err
		}
		waits = deliveryWaits(queued)
		delivery, err = h.deliverChatAgentQueuedInputsTx(txCtx, tx, queued, runID)
		return err
	})
	if err != nil {
		if errors.Is(err, ErrChatAgentRunDriveLeaseLost) || errors.Is(err, ErrGraphBudgetExhausted) {
			h.recordGraphBudgetRefusal(err)
			return false, ChatAgentQueuedInputDelivery{}, err
		}
		return false, ChatAgentQueuedInputDelivery{}, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "reserve_chat_agent_run_step_and_admit"),
			zap.Uint64(appLogger.KeyAgentRunID, runID))
	}
	h.GetMetricsHandler().RecordChatAgentInputStepAdmission(waits)
	return reserved, delivery, nil
}

// chatAgentStepCanAdmit reports whether a model step reserved when the run had spent
// generationCount generations may admit queued input: the step's own reservation must still leave
// one under maxGenerations for the compaction that the delivered input can trigger. Otherwise that
// compaction's reservation would stop the run with the input delivered but unanswered, where input
// left queued gets a follow-up run with a fresh budget from the terminal drain. A step that needs
// more than one compaction summary can still exhaust the budget; that input is covered by the
// documented limit on input delivered into a step whose generation then fails.
func chatAgentStepCanAdmit(generationCount, maxGenerations uint64) bool {
	return generationCount+1 < maxGenerations
}

// chatAgentGraphPoolHasGenerations reports whether a graph's pool, nil when the graph has none,
// has generations left to charge: chatAgentStepCanAdmit's spare generation, applied to the pool.
// Only generations count; the pool's other limits refuse the work itself when it is charged.
func chatAgentGraphPoolHasGenerations(pool *common_persistence_models.ChatAgentGraphBudget, generations uint64) bool {
	return pool == nil || uint64(pool.UsedGenerations)+generations <= uint64(pool.MaxGenerations)
}

// chatAgentQueuedInputDeliverable is the SQL predicate of a queued input q whose sender may still
// send to the session's channel: a participant who has not left, with an account that is neither
// banned nor deleted. Delivery turns such an input into a message and discards any other.
const chatAgentQueuedInputDeliverable = `EXISTS (SELECT 1 FROM ` + common_persistence_models.TableNameChatChannelParticipants + ` p
		WHERE p.channel_id = q.channel_id AND p.user_id = q.sender_id AND p.left_at IS NULL)
	AND EXISTS (SELECT 1 FROM ` + common_persistence_models.TableNameUsers + ` u
		WHERE u.id = q.sender_id AND NOT u.banned AND u.deleted_at IS NULL)`

// chatAgentInputWaitsTx reports whether a session's queue holds input delivery would turn into a
// message (chatAgentQueuedInputDeliverable).
func chatAgentInputWaitsTx(tx *gorm.DB, sessionID uint64) (bool, error) {
	var waiting bool
	err := tx.Raw("SELECT EXISTS (SELECT 1 FROM "+common_persistence_models.TableNameChatAgentQueuedInputs+
		" q WHERE q.agent_session_id = ? AND "+chatAgentQueuedInputDeliverable+")", sessionID).Scan(&waiting).Error
	return waiting, err
}

// lockChatChannelForAgentInputTx locks a channel FOR NO KEY UPDATE, the one channel lock every
// path that sends, queues, delivers, edits or deletes takes (see the section comment), and returns
// its id, is_archived and deleted_at. A soft-deleted channel's row is locked as well, so the lock
// order still holds for the sessions it keeps; each caller decides what a deleted channel means
// to it. A missing channel returns gorm.ErrRecordNotFound.
func lockChatChannelForAgentInputTx(tx *gorm.DB, channelID uint64) (common_persistence_models.ChatChannel, error) {
	var channel common_persistence_models.ChatChannel
	err := tx.Unscoped().Clauses(clause.Locking{Strength: "NO KEY UPDATE"}).Select("id", "is_archived", "deleted_at").
		Where("id = ?", channelID).Take(&channel).Error
	return channel, err
}

// lockLiveChatChannelForAgentInputTx takes lockChatChannelForAgentInputTx's lock for a path that
// writes into the channel: a missing or soft-deleted channel returns ErrChatChannelNotFound.
func lockLiveChatChannelForAgentInputTx(tx *gorm.DB, channelID uint64) error {
	channel, err := lockChatChannelForAgentInputTx(tx, channelID)
	if errors.Is(err, gorm.ErrRecordNotFound) || (err == nil && channel.DeletedAt.Valid) {
		return ErrChatChannelNotFound
	}
	return err
}

// lockChatAgentSessionForAdmissionTx takes the admission locks: the channel
// (lockChatChannelForAgentInputTx), then the session FOR UPDATE. It returns the locked session and
// whether the channel is archived. A missing or soft-deleted channel, where nothing is admitted,
// or a missing session returns a nil session.
func lockChatAgentSessionForAdmissionTx(tx *gorm.DB, channelID, sessionID uint64) (*common_persistence_models.ChatAgentSession, bool, error) {
	channel, err := lockChatChannelForAgentInputTx(tx, channelID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if channel.DeletedAt.Valid {
		return nil, false, nil
	}
	session, err := lockChatAgentSessionRowTx(tx, sessionID)
	return session, channel.IsArchived, err
}

// lockChatAgentSessionRowTx locks a session row FOR UPDATE and reads it; a missing session returns
// nil. The caller holds the session's channel lock.
func lockChatAgentSessionRowTx(tx *gorm.DB, sessionID uint64) (*common_persistence_models.ChatAgentSession, error) {
	var session common_persistence_models.ChatAgentSession
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", sessionID).Take(&session).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &session, nil
}

// queuedChatAgentInputsTx reads a session's queue in order, each input with whether delivery may
// turn it into a message (chatAgentQueuedInputDeliverable). The caller holds the session lock,
// which every writer of the queue takes, so the list stays exact until commit.
func queuedChatAgentInputsTx(tx *gorm.DB, sessionID uint64) ([]queuedChatAgentInput, error) {
	var queued []queuedChatAgentInput
	err := tx.Table(common_persistence_models.TableNameChatAgentQueuedInputs+" q").
		Select("q.*, "+chatAgentQueuedInputDeliverable+" AS deliverable").
		Where("q.agent_session_id = ?", sessionID).Order("q.id").Scan(&queued).Error
	return queued, err
}

// deliveryWaits returns how long each deliverable queued input has waited as of now.
func deliveryWaits(queued []queuedChatAgentInput) []time.Duration {
	now := time.Now()
	waits := make([]time.Duration, 0, len(queued))
	for i := range queued {
		if queued[i].Deliverable {
			waits = append(waits, now.Sub(queued[i].CreatedAt))
		}
	}
	return waits
}

// lastDeliverable returns the newest queued input delivery will turn into a message, or nil.
func lastDeliverable(queued []queuedChatAgentInput) *queuedChatAgentInput {
	for i := len(queued) - 1; i >= 0; i-- {
		if queued[i].Deliverable {
			return &queued[i]
		}
	}
	return nil
}

// deliverChatAgentQueuedInputsTx empties a session's queue, read by queuedChatAgentInputsTx under
// the caller's admission locks (channel, then session), into chat messages in queue order. Each
// message is written as the send would have written it (insertChatMessageTx) at the session's
// location, as its sender, with the input's content, reply target, mentions, embeds, attachments
// and client message ID, and with runID as its AdmittedRunID (zero posts an ordinary message, for
// a session that will run no more). A message lists only the attachments its input lists and
// still owns, and those move to it; the newest message becomes the channel's last message, and
// every input leaves the queue. An input whose sender can no longer send to the channel is
// discarded instead, with its pending uploads (abandonChatAgentQueuedInputUploadsTx). Each complete
// message's notification is then recorded (SetChatAgentMessageRecorder) in the same
// transaction, which ctx must carry, with post-commit hook support for the notification's realtime
// publication: a persistence-owned transaction opens it with transaction and passes its txCtx down.
func (h *CommonPersistenceHandler[U]) deliverChatAgentQueuedInputsTx(ctx context.Context, tx *gorm.DB, queued []queuedChatAgentInput, runID uint64) (ChatAgentQueuedInputDelivery, error) {
	var delivery ChatAgentQueuedInputDelivery
	var admittedRunID *uint64
	if runID != 0 {
		admittedRunID = &runID
	}
	owners, err := chatAgentQueuedInputAttachmentOwnersTx(tx, queued)
	if err != nil {
		return delivery, err
	}
	taken := make([]uint64, len(queued))
	var discarded []uint64
	// movedAttachments lists each delivered attachment, at the same index as the message it joins.
	var movedAttachments, toMessages pq.Int64Array
	var last *common_persistence_models.ChatMessage
	for i := range queued {
		input := &queued[i].ChatAgentQueuedInput
		taken[i] = input.ID
		if !queued[i].Deliverable {
			discarded = append(discarded, input.ID)
			appLogger.For(ctx, h.logger, loggerName).Info("queued agent input discarded: its sender can no longer send to the channel",
				zap.String(logkeys.Stage, "deliver_chat_agent_queued_input"),
				zap.Uint64(appLogger.KeyAgentSessionID, input.AgentSessionID),
				zap.Uint64(appLogger.KeyQueuedInputID, input.ID),
				zap.Uint64(appLogger.KeySenderID, input.SenderID))
			continue
		}
		var owned pq.Int64Array
		for _, attachmentID := range input.AttachmentIDs {
			if owners[attachmentID] == input.ID {
				owned = append(owned, attachmentID)
			}
		}
		message := &common_persistence_models.ChatMessage{
			ChannelID:        input.ChannelID,
			SenderID:         input.SenderID,
			Content:          input.Content,
			ContentType:      input.ContentType,
			StreamState:      proto_models.MessageStreamState_MESSAGE_STREAM_STATE_COMPLETE,
			ReplyToMessageID: input.ReplyToMessageID,
			AttachmentIDs:    owned,
			ClientMessageID:  input.ClientMessageID,
			AdmittedRunID:    admittedRunID,
		}
		if input.ParentMessageID != 0 {
			message.ParentMessageID = &input.ParentMessageID
		}
		embeds := make([]CreateChatMessageEmbed, len(input.Embeds))
		for j, embed := range input.Embeds {
			embeds[j] = CreateChatMessageEmbed(embed)
		}
		mentions := make([]CreateChatMessageMention, len(input.MentionUserIDs))
		for j, userID := range input.MentionUserIDs {
			mentions[j] = CreateChatMessageMention{UserID: uint64(userID)}
		}
		if err := insertChatMessageTx(tx, message, embeds, mentions); err != nil {
			return delivery, err
		}
		delivery.Delivered = append(delivery.Delivered, ChatAgentDeliveredInput{QueuedInputID: input.ID, MessageID: message.ID})
		for _, attachmentID := range owned {
			movedAttachments = append(movedAttachments, attachmentID)
			toMessages = append(toMessages, int64(message.ID))
		}
		last = message
	}
	delivery.Discarded = len(discarded)
	if last != nil {
		if err := touchChatChannelLastMessageTx(tx, last.ChannelID, last.SenderID, last.Content); err != nil {
			return delivery, err
		}
	}
	if len(movedAttachments) > 0 {
		if err := tx.Exec("UPDATE "+common_persistence_models.TableNameAttachments+" AS a"+
			" SET object_type = ?, entity_id = moved.message_id::text, updated_at = ?"+
			" FROM unnest(?::bigint[], ?::bigint[]) AS moved(attachment_id, message_id) WHERE a.id = moved.attachment_id",
			common_persistence_models.ObjectTypeChatAttachment, time.Now(), movedAttachments, toMessages).Error; err != nil {
			return delivery, err
		}
	}
	if err := abandonChatAgentQueuedInputUploadsTx(tx, discarded, nil); err != nil {
		return delivery, err
	}
	result := tx.Where("id IN ?", taken).Delete(&common_persistence_models.ChatAgentQueuedInput{})
	if result.Error != nil {
		return delivery, result.Error
	}
	if result.RowsAffected != int64(len(taken)) {
		return delivery, appLogger.InvariantError("chat_agent_queued_input_delivered_once",
			errors.Join(ErrDatabaseError, errors.New("queued input changed under the session lock")),
			zap.String(logkeys.Stage, "deliver_chat_agent_queued_input"),
			zap.Uint64(appLogger.KeyAgentRunID, runID),
			zap.Int(logkeys.Expected, len(taken)),
			zap.Int64(logkeys.Actual, result.RowsAffected))
	}
	// Recorded once each message is complete, as a send records its own.
	for _, delivered := range delivery.Delivered {
		if err := h.recordChatAgentMessage(ctx, delivered.MessageID); err != nil {
			return delivery, err
		}
	}
	return delivery, nil
}

// chatAgentQueuedInputAttachmentOwnersTx maps each attachment a deliverable input lists to the
// queued input that owns it, in one read. A listed attachment the map leaves out, or maps to
// another input, belongs to something else and never joins the message. Only delivery, under the
// session lock, moves a queued input's attachments, and lifecycle writes never change an owner, so
// the map holds until commit.
func chatAgentQueuedInputAttachmentOwnersTx(tx *gorm.DB, queued []queuedChatAgentInput) (map[int64]uint64, error) {
	var listed []int64
	for i := range queued {
		if queued[i].Deliverable {
			listed = append(listed, queued[i].AttachmentIDs...)
		}
	}
	if len(listed) == 0 {
		return nil, nil
	}
	var rows []struct {
		ID       int64
		EntityID string
	}
	if err := tx.Model(&common_persistence_models.Attachment{}).Select("id", "entity_id").
		Where("id IN ? AND object_type = ? AND internal_only = ?", listed, common_persistence_models.ObjectTypeChatQueuedInputAttachment, false).
		Scan(&rows).Error; err != nil {
		return nil, err
	}
	owners := make(map[int64]uint64, len(rows))
	for _, row := range rows {
		if owner, err := strconv.ParseUint(row.EntityID, 10, 64); err == nil {
			owners[row.ID] = owner
		}
	}
	return owners, nil
}

// abandonChatAgentQueuedInputUploadsTx rejects the uploads still pending for the given queued
// inputs, except the attachments listed in keep: a removed input, an input delivery discards, and
// attachments an edit drops have no message to join. Their stored objects are kept. A lifecycle
// worker that read one as pending then fails its compare-and-swap, so none is processed later.
func abandonChatAgentQueuedInputUploadsTx(tx *gorm.DB, queuedInputIDs []uint64, keep pq.Int64Array) error {
	if len(queuedInputIDs) == 0 {
		return nil
	}
	entityIDs := common_util.ApplyToEach(queuedInputIDs, func(id uint64) string { return strconv.FormatUint(id, 10) })
	query := tx.Model(&common_persistence_models.Attachment{}).
		Where("object_type = ? AND entity_id IN ? AND internal_only = ? AND upload_status IN ?",
			common_persistence_models.ObjectTypeChatQueuedInputAttachment, entityIDs, false,
			[]common_persistence_models.UploadStatus{common_persistence_models.UploadStatusPendingUpload, common_persistence_models.UploadStatusPendingProcessing})
	if len(keep) > 0 {
		query = query.Where("id NOT IN ?", []int64(keep))
	}
	now := time.Now()
	return query.Select("upload_status", "validation_details", "processing_end_time").Updates(&common_persistence_models.Attachment{
		UploadStatus:      common_persistence_models.UploadStatusRejected,
		ValidationDetails: "Abandoned: its queued input no longer lists it",
		ProcessingEndTime: &now,
	}).Error
}

// drainChatAgentInputTx delivers every input queued for session into one new follow-up run when the
// session can take one: live, automatic, not suspended, in a channel that is not archived, and
// with no active run. drainedFromRunID is the run whose ending drains the input, recorded as the
// follow-up's InputDrainedFromRunID (zero records none). The follow-up is triggered by, and acts
// for the sender of, the newest delivered message. The admission locks (channel, then session)
// are taken before the queue is read, so a concurrent send either queued its input before the read
// or routes it after this transaction. Input that cannot be drained stays queued: Resume drains a
// suspended session, and archiving or disabling automatic runs posts the queue. A queue whose
// senders all lost access is discarded without starting a run. It returns the new run, or nil when
// none started, and the delivery.
func (h *CommonPersistenceHandler[U]) drainChatAgentInputTx(
	ctx context.Context,
	tx *gorm.DB,
	session *common_persistence_models.ChatAgentSession,
	drainedFromRunID uint64,
) (*common_persistence_models.ChatAgentRun, ChatAgentQueuedInputDelivery, error) {
	locked, archived, err := lockChatAgentSessionForAdmissionTx(tx, session.ChannelID, session.ID)
	if err != nil || locked == nil {
		return nil, ChatAgentQueuedInputDelivery{}, err
	}
	return h.drainLockedChatAgentInputTx(ctx, tx, locked, archived, drainedFromRunID)
}

// drainLockedChatAgentInputTx is drainChatAgentInputTx for a caller that already holds the
// admission locks (lockChatAgentSessionForAdmissionTx): locked is the session it read under them
// and archived the channel's state.
func (h *CommonPersistenceHandler[U]) drainLockedChatAgentInputTx(
	ctx context.Context,
	tx *gorm.DB,
	locked *common_persistence_models.ChatAgentSession,
	archived bool,
	drainedFromRunID uint64,
) (*common_persistence_models.ChatAgentRun, ChatAgentQueuedInputDelivery, error) {
	var none ChatAgentQueuedInputDelivery
	if archived || locked.IsSuspended() || !locked.IsLive() ||
		locked.AutoRunPolicy != common_persistence_models.ChatAgentSessionAutoRunPolicyAutomatic {
		return nil, none, nil
	}
	queued, err := queuedChatAgentInputsTx(tx, locked.ID)
	if err != nil || len(queued) == 0 {
		return nil, none, err
	}
	if active, err := sessionHasActiveChatAgentRunTx(tx, locked.ID, 0); err != nil || active {
		// An active run admits the input at its next step or drains it at its own ending.
		return nil, none, err
	}
	newest := lastDeliverable(queued)
	if newest == nil {
		delivery, err := h.deliverChatAgentQueuedInputsTx(ctx, tx, queued, 0)
		return nil, delivery, err
	}
	// The run is created first, so its messages can name it; its trigger, the newest of them, is
	// set once they exist.
	pool, err := loadChatAgentGraphBudgetTx(tx, locked.GraphRootID())
	if err != nil || graphBudgetExhaustedError(pool) != nil {
		// A used-up pool admits no run: the input stays queued until SetChatAgentGraphBudget
		// gives the pool room and drains it.
		return nil, none, err
	}
	run := h.newChatAgentFollowUpRun(locked, nil, newest.SenderID, pool != nil)
	if drainedFromRunID != 0 {
		run.InputDrainedFromRunID = &drainedFromRunID
	}
	if err := tx.Create(run).Error; err != nil {
		return nil, none, err
	}
	waits := deliveryWaits(queued)
	delivery, err := h.deliverChatAgentQueuedInputsTx(ctx, tx, queued, run.ID)
	if err != nil {
		return nil, none, err
	}
	trigger := delivery.Delivered[len(delivery.Delivered)-1].MessageID
	if err := tx.Model(run).Update("triggering_message_id", trigger).Error; err != nil {
		return nil, none, err
	}
	run.TriggeringMessageID = &trigger
	// Recorded as the drain is written: the caller owns the transaction, so a caller that then
	// fails and rolls back is still counted.
	h.GetMetricsHandler().RecordChatAgentInputDrain(waits)
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("queued agent input drained into one follow-up run",
			zap.Uint64(appLogger.KeyAgentSessionID, locked.ID),
			zap.Uint64(appLogger.KeyAgentRunID, run.ID),
			zap.Int(appLogger.KeyCount, len(delivery.Delivered)))
	}
	return run, delivery, nil
}

// lockChatAgentSessionByIDTx takes the admission locks of a session known by ID: it reads the
// session's channel, then locks the channel (lockChatChannelForAgentInputTx) and re-reads the
// session under its own lock. Unlike lockChatAgentSessionForAdmissionTx it returns the session of
// a soft-deleted channel too, with channelDeleted set: archiving, disabling automatic runs and
// Resume still apply to it, but nothing is written into the channel (postChatAgentQueuedInputsTx
// discards the queue, and the drains Resume runs deliver nothing). A missing session returns
// ErrChatAgentSessionNotFound.
func lockChatAgentSessionByIDTx(tx *gorm.DB, sessionID uint64) (session *common_persistence_models.ChatAgentSession, channelDeleted bool, err error) {
	if session, err = loadChatAgentSessionTx(tx, sessionID); err != nil {
		return nil, false, err
	}
	if session == nil {
		return nil, false, ErrChatAgentSessionNotFound
	}
	if err := lockLiveChatChannelForAgentInputTx(tx, session.ChannelID); err != nil {
		if !errors.Is(err, ErrChatChannelNotFound) {
			return nil, false, err
		}
		channelDeleted = true
	}
	if session, err = lockChatAgentSessionRowTx(tx, sessionID); err != nil {
		return nil, false, err
	}
	if session == nil {
		return nil, false, ErrChatAgentSessionNotFound
	}
	return session, channelDeleted, nil
}

// postChatAgentQueuedInputsTx posts a session's queue as ordinary messages (no run) for a session
// that will not run by itself again: archived, or with automatic runs disabled, so the application
// starts every run. In a soft-deleted channel, which nobody reads, the queue is discarded with its
// pending uploads instead. The caller holds the session's admission locks (lockChatAgentSessionByIDTx).
func (h *CommonPersistenceHandler[U]) postChatAgentQueuedInputsTx(ctx context.Context, tx *gorm.DB, sessionID uint64, channelDeleted bool) (ChatAgentQueuedInputDelivery, error) {
	if channelDeleted {
		return h.discardChatAgentQueuedInputsTx(ctx, tx, sessionID)
	}
	queued, err := queuedChatAgentInputsTx(tx, sessionID)
	if err != nil || len(queued) == 0 {
		return ChatAgentQueuedInputDelivery{}, err
	}
	return h.deliverChatAgentQueuedInputsTx(ctx, tx, queued, 0)
}

// discardChatAgentQueuedInputsTx empties a session's queue without writing any message, and
// abandons the inputs' pending uploads (abandonChatAgentQueuedInputUploadsTx). The caller holds the
// session lock.
func (h *CommonPersistenceHandler[U]) discardChatAgentQueuedInputsTx(ctx context.Context, tx *gorm.DB, sessionID uint64) (ChatAgentQueuedInputDelivery, error) {
	var discarded []uint64
	if err := tx.Raw("DELETE FROM "+common_persistence_models.TableNameChatAgentQueuedInputs+
		" WHERE agent_session_id = ? RETURNING id", sessionID).Scan(&discarded).Error; err != nil || len(discarded) == 0 {
		return ChatAgentQueuedInputDelivery{}, err
	}
	appLogger.For(ctx, h.logger, loggerName).Info("queued agent input discarded: its channel was deleted",
		zap.String(logkeys.Stage, "discard_chat_agent_queued_inputs"),
		zap.Uint64(appLogger.KeyAgentSessionID, sessionID),
		zap.Int(appLogger.KeyCount, len(discarded)))
	return ChatAgentQueuedInputDelivery{Discarded: len(discarded)}, abandonChatAgentQueuedInputUploadsTx(tx, discarded, nil)
}

// sessionHasActiveChatAgentRunTx reports whether a session has an active run other than
// exceptRunID (zero excludes none). Every automatic start path asks it under the session lock, so
// the one-active-run index is only a backstop that never aborts the caller's transaction.
func sessionHasActiveChatAgentRunTx(tx *gorm.DB, sessionID, exceptRunID uint64) (bool, error) {
	var active bool
	err := tx.Raw("SELECT EXISTS (SELECT 1 FROM "+common_persistence_models.TableNameChatAgentRuns+
		" WHERE agent_session_id = ? AND id <> ? AND status IN ?)", sessionID, exceptRunID, ActiveChatAgentRunStatuses()).
		Scan(&active).Error
	return active, err
}

// steerChatAgentRunFinalAnswerTx decides whether a FINAL seal is recorded as Steered: the caller
// allows it (steer), the run's next step can admit under maxGenerations (chatAgentStepCanAdmit)
// and its graph's pool (chatAgentGraphPoolHasGenerations), and input that delivery would turn into
// a message is queued for the run's unsuspended session.
// A run that could not take the admitting step completes instead, and its terminal drain hands
// the input to a follow-up. When steering is possible it takes the admission locks (channel, then
// session), so input a send queued while the run was active is either seen here or routed after
// the run ends; the locked session is returned for the terminal settlement to reuse.
func steerChatAgentRunFinalAnswerTx(tx *gorm.DB, run *common_persistence_models.ChatAgentRun, maxGenerations uint64, steer bool) (*common_persistence_models.ChatAgentSession, bool, error) {
	sessionID := common_util.Deref(run.AgentSessionID)
	if !steer || sessionID == 0 || !chatAgentStepCanAdmit(run.GenerationCount, maxGenerations) {
		return nil, false, nil
	}
	session, _, err := lockChatAgentSessionForAdmissionTx(tx, run.ChannelID, sessionID)
	if err != nil || session == nil || session.IsSuspended() {
		return session, false, err
	}
	if waiting, err := chatAgentInputWaitsTx(tx, sessionID); err != nil || !waiting {
		return session, false, err
	}
	// The graph's pool must pay for that step and the compaction its input can trigger. The read
	// takes no lock, so following the admission locks keeps the run -> pool -> channel order.
	pool, err := loadChatAgentRunGraphBudgetTx(tx, run.ID)
	return session, err == nil && chatAgentGraphPoolHasGenerations(pool, 2), err
}
