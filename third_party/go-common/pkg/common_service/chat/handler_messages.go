package chat

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// newPostCommitMessageRefresher returns a closure that reloads and hydrates a
// message at most once after commit, so multiple post-commit hooks (event
// publisher, bot dispatcher) share a single read-back instead of each
// re-fetching the message with all preloads.
//
// Parameters:
// - `messageID`: the message to reload (0 skips the reload).
// - `fallback`: the message to use when reloading fails or is skipped.
// - `logContext`: short label naming the consumer, used in warning logs.
//
// Returns:
// - `result1`: a function yielding the refreshed (or fallback) message.
func (h *Handler[U]) newPostCommitMessageRefresher(
	messageID uint64,
	fallback *common_service_models.ChatMessage,
	logContext string,
) func(context.Context) *common_service_models.ChatMessage {
	var once sync.Once
	result := fallback
	return func(hookCtx context.Context) *common_service_models.ChatMessage {
		once.Do(func() {
			if messageID != 0 {
				if refreshed, err := h.persistenceHandler.GetChatMessageByID(hookCtx, messageID); err == nil {
					result = h.converter.PersistenceChatMessageToService(refreshed)
				} else if h.logger != nil {
					appLogger.For(hookCtx, h.logger, chatLoggerName).Warn("Failed to reload chat message",
						zap.String(appLogger.KeyConsumer, logContext),
						zap.Uint64(appLogger.KeyMessageID, messageID),
						zap.Error(err))
				}
			}
			if hydrateErr := h.hydrateMessageAttachments(hookCtx, result); hydrateErr != nil && h.logger != nil {
				appLogger.For(hookCtx, h.logger, chatLoggerName).Warn("Failed to hydrate chat message attachments",
					zap.String(appLogger.KeyConsumer, logContext),
					zap.Uint64(appLogger.KeyMessageID, messageID),
					zap.Error(hydrateErr))
			}
		})
		return result
	}
}

// requireThreadRoot loads rootID and requires it to anchor a thread in channel: a live,
// visible STANDARD main-timeline row of that channel. A missing or deleted root, and one that
// cannot anchor a thread (another channel, a reply, a transcript row, a hidden message), is
// ErrChatInvalidThread; only a failed lookup maps to another error.
func (h *Handler[U]) requireThreadRoot(ctx context.Context, channel common_service_models.ChatChannelLike, rootID uint64) (*common_persistence_models.ChatMessage, error) {
	channelID := uint64(channel.GetID())
	root, err := h.persistenceHandler.GetChatMessageByID(ctx, rootID)
	if err != nil {
		if errors.Is(err, persistence.ErrChatMessageNotFound) {
			return nil, serviceErrors.ErrChatInvalidThread
		}
		return nil, mapChatMessagePersistenceError(err,
			zap.String(logkeys.Stage, "thread_root_lookup"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Uint64(appLogger.KeyParentMessageID, rootID))
	}
	if root.ChannelID != channelID || !isNilOrZero(root.ParentMessageID) || requireStandardMutableMessage(root) != nil {
		return nil, serviceErrors.ErrChatInvalidThread
	}
	return root, nil
}

// validateThreadAndReplyReferences executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) validateThreadAndReplyReferences(
	ctx context.Context,
	channel common_service_models.ChatChannelLike,
	parentMessageID uint64,
	replyToMessageID uint64,
	broadcastToChannel bool,
) error {
	if channel == nil {
		return serviceErrors.ErrInternalError
	}
	// Checked before the feature gates so a main-timeline broadcast reports the malformed request
	// rather than a disabled feature, independent of the channel's thread setting.
	if broadcastToChannel && parentMessageID == 0 {
		return serviceErrors.ErrChatInvalidBroadcast
	}
	if parentMessageID != 0 && !channel.GetSupportThreads() {
		return serviceErrors.ErrChatFeatureDisabled
	}
	if replyToMessageID != 0 && !channel.GetSupportReplies() {
		return serviceErrors.ErrChatFeatureDisabled
	}

	channelID := uint64(channel.GetID())

	var threadRoot *common_service_models.ChatMessage
	if parentMessageID != 0 {
		parent, err := h.requireThreadRoot(ctx, channel, parentMessageID)
		if err != nil {
			return err
		}
		threadRoot = h.converter.PersistenceChatMessageToService(parent)
	}

	if replyToMessageID != 0 {
		replyTo, err := h.persistenceHandler.GetChatMessageByID(ctx, replyToMessageID)
		if err != nil {
			if errors.Is(err, persistence.ErrChatMessageNotFound) {
				return serviceErrors.ErrChatInvalidReply
			}
			return mapChatMessagePersistenceError(err,
				zap.String(logkeys.Stage, "validate_reply_message_lookup"),
				zap.Uint64(appLogger.KeyChannelID, channelID),
				zap.Uint64(appLogger.KeyParentMessageID, parentMessageID),
				zap.Uint64(appLogger.KeyReplyToMessageID, replyToMessageID))
		}
		if replyTo.ChannelID != channelID {
			return serviceErrors.ErrChatInvalidReply
		}
		// Replies cannot target a non-STANDARD transcript row or a hidden message.
		if err := requireStandardMutableMessage(replyTo); err != nil {
			return serviceErrors.ErrChatInvalidReply
		}
		if parentMessageID == 0 {
			if !isNilOrZero(replyTo.ParentMessageID) {
				return serviceErrors.ErrChatInvalidReply
			}
		} else {
			if replyTo.ID == parentMessageID {
				return nil
			}
			if threadRoot == nil || replyTo.ParentMessageID == nil || *replyTo.ParentMessageID != parentMessageID {
				return serviceErrors.ErrChatInvalidReply
			}
		}
	}

	return nil
}

// stripMarkdownCode executes part of the chat package flow and provides shared helper behavior.
func stripMarkdownCode(content string) string {
	var b strings.Builder
	b.Grow(len(content))
	i := 0
	n := len(content)
	for i < n {
		// Fenced code blocks: ``` ... ```
		if i+2 < n && content[i] == '`' && content[i+1] == '`' && content[i+2] == '`' {
			end := strings.Index(content[i+3:], "```")
			if end >= 0 {
				i += 3 + end + 3
			} else {
				// Unclosed fence — skip the rest
				break
			}
			continue
		}
		// Inline code spans: ` ... `
		if content[i] == '`' {
			end := strings.IndexByte(content[i+1:], '`')
			if end >= 0 {
				i += 1 + end + 1
			} else {
				// Unclosed backtick — keep the rest as-is
				b.WriteString(content[i:])
				break
			}
			continue
		}
		b.WriteByte(content[i])
		i++
	}
	return b.String()
}

// parseMentionUserIDs executes part of the chat package flow and provides shared helper behavior.
func parseMentionUserIDs(content string, contentType proto_models.MessageContentType) []uint64 {
	if len(content) < 4 {
		return nil
	}

	// Strip code blocks and inline code spans so mentions inside them are
	// not extracted, matching the frontend rendering behaviour.
	// Only applies to markdown content — plain text has no code block syntax.
	if contentType == proto_models.MessageContentType_MESSAGE_CONTENT_TYPE_MARKDOWN {
		content = stripMarkdownCode(content)
	}

	seen := make(map[uint64]struct{})
	out := make([]uint64, 0)

	for i := 0; i+3 < len(content); i++ {
		if content[i] != '<' || content[i+1] != '@' {
			continue
		}
		start := i + 2
		if start >= len(content) || content[start] < '0' || content[start] > '9' {
			continue
		}
		j := start
		for j < len(content) && content[j] >= '0' && content[j] <= '9' {
			j++
		}
		if j >= len(content) || content[j] != '>' {
			continue
		}
		id, err := strconv.ParseUint(content[start:j], 10, 64)
		if err != nil || id == 0 {
			continue
		}
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			out = append(out, id)
		}
		i = j
	}

	return out
}

// validateMessageMentions executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) validateMessageMentions(
	ctx context.Context,
	channelID uint64,
	content string,
	contentType proto_models.MessageContentType,
) ([]uint64, error) {
	mentionUserIDs := parseMentionUserIDs(content, contentType)
	if len(mentionUserIDs) == 0 {
		return nil, nil
	}

	if len(mentionUserIDs) > h.policy().ChatMaxMentionsPerMessage() {
		return nil, serviceErrors.ErrChatMentionLimitExceeded
	}

	count, err := h.persistenceHandler.GetActiveChannelParticipantCountByUserIDs(ctx, channelID, mentionUserIDs)
	if err != nil {
		return nil, mapChatMessagePersistenceError(err,
			zap.String(logkeys.Stage, "validate_message_mentions"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Int(appLogger.KeyMentionCount, len(mentionUserIDs)))
	}
	if int(count) != len(mentionUserIDs) {
		return nil, serviceErrors.ErrChatInvalidMention
	}

	return mentionUserIDs, nil
}

// validateMessageEmbeds executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) validateMessageEmbeds(
	channel common_service_models.ChatChannelLike,
	embeds []persistence.CreateChatMessageEmbed,
) error {
	if len(embeds) == 0 {
		return nil
	}
	if channel == nil {
		return serviceErrors.ErrInternalError
	}
	if !channel.GetSupportEmbeds() {
		return serviceErrors.ErrChatFeatureDisabled
	}
	limits := h.policy()
	if limits.ChatMaxEmbedsPerMessage() > 0 && len(embeds) > limits.ChatMaxEmbedsPerMessage() {
		return serviceErrors.ErrInvalidRequest
	}

	for _, embed := range embeds {
		if embed.EmbedType == 0 {
			return serviceErrors.ErrInvalidRequest
		}
		if limits.ChatMaxEmbedDataBytes() > 0 && len(embed.EmbedData) > limits.ChatMaxEmbedDataBytes() {
			return serviceErrors.ErrInvalidRequest
		}
		if embed.EmbedType <= embedTypeNativeMax && !channel.GetAllowNativeEmbeds() {
			return serviceErrors.ErrChatFeatureDisabled
		}
		if embed.EmbedType >= embedTypeCustomMin && !channel.GetAllowCustomEmbeds() {
			return serviceErrors.ErrChatFeatureDisabled
		}
	}

	return nil
}

// normalizeBotMessageContentType executes part of the chat package flow and provides shared helper behavior.
func normalizeBotMessageContentType(content string, contentType proto_models.MessageContentType) (proto_models.MessageContentType, error) {
	switch contentType {
	case proto_models.MessageContentType_MESSAGE_CONTENT_TYPE_UNSPECIFIED:
		return common_util.DetectMessageContentType(content), nil
	case proto_models.MessageContentType_MESSAGE_CONTENT_TYPE_TEXT:
		return proto_models.MessageContentType_MESSAGE_CONTENT_TYPE_TEXT, nil
	case proto_models.MessageContentType_MESSAGE_CONTENT_TYPE_MARKDOWN:
		return proto_models.MessageContentType_MESSAGE_CONTENT_TYPE_MARKDOWN, nil
	default:
		return proto_models.MessageContentType_MESSAGE_CONTENT_TYPE_UNSPECIFIED, serviceErrors.ErrInvalidRequest
	}
}

// SendMessageInput is the human send request. ParentMessageID 0 targets the main timeline;
// BroadcastToChannel additionally surfaces a thread reply there and is rejected on the main timeline.
type SendMessageInput struct {
	ChannelID          uint64
	Content            string
	ParentMessageID    uint64
	ReplyToMessageID   uint64
	BroadcastToChannel bool
	// AgentRunID tags the row with the sending bot's run (a delegated child's report_to_parent).
	// It is a Go-only field: the run must be in the same channel and thread location and owned by
	// the sending bot, or the send is rejected with ErrInvalidRequest.
	AgentRunID *uint64
	Embeds     []persistence.CreateChatMessageEmbed
	// ClientMessageID is the sender's idempotency key for the send. Repeating a key the sender
	// already used in the channel writes nothing and returns the message the first send wrote;
	// empty sends without one.
	ClientMessageID string
}

// SendBotMessageInput extends SendMessageInput with the bot-selected content type
// (UNSPECIFIED auto-detects from the content).
type SendBotMessageInput struct {
	SendMessageInput
	ContentType proto_models.MessageContentType
}

// SendMessage sends a plain-text message as the context actor. When the agent session the message
// reaches is busy or suspended, the input is queued for it instead and the message is nil:
// delivery writes the message when the agent receives the input. SendMessageWithAttachments
// reports the queued input.
func (h *Handler[U]) SendMessage(
	ctx context.Context,
	input SendMessageInput,
) (*common_service_models.ChatMessage, error) {
	sent, err := h.sendText(ctx, input, sendOptions{})
	if err != nil {
		return nil, err
	}
	return sent.message, nil
}

// sendText is the plain-text send behind SendMessage and SendMessageWithAttachments.
func (h *Handler[U]) sendText(ctx context.Context, input SendMessageInput, opts sendOptions) (*sentMessage, error) {
	return h.sendMessageWithContentType(
		ctx,
		input,
		proto_models.MessageContentType_MESSAGE_CONTENT_TYPE_TEXT,
		proto_models.MessageStreamState_MESSAGE_STREAM_STATE_COMPLETE,
		proto_models.MessageRole_MESSAGE_ROLE_UNSPECIFIED,
		opts,
	)
}

// SendBotMessage sends a message whose content type the bot chooses. Like SendMessage, it returns a
// nil message when the input is queued for a busy or suspended agent session; a message tagged with
// an agent run (AgentRunID) is that agent's output and is never queued.
func (h *Handler[U]) SendBotMessage(
	ctx context.Context,
	input SendBotMessageInput,
) (*common_service_models.ChatMessage, error) {
	normalizedContentType, err := normalizeBotMessageContentType(input.Content, input.ContentType)
	if err != nil {
		return nil, err
	}

	sent, err := h.sendMessageWithContentType(
		ctx,
		input.SendMessageInput,
		normalizedContentType,
		proto_models.MessageStreamState_MESSAGE_STREAM_STATE_COMPLETE,
		proto_models.MessageRole_MESSAGE_ROLE_UNSPECIFIED,
		sendOptions{},
	)
	if err != nil {
		return nil, err
	}
	return sent.message, nil
}

// sendOptions carries private per-send behavior that is not part of the message itself.
type sendOptions struct {
	// suppressAgentAutoRun skips the follow-up auto-run trigger. CreateAgentChatSession's initial
	// message sets it: that message already mints the first run transactionally, so the post-commit
	// auto-trigger must not create a second run for the same message.
	suppressAgentAutoRun  bool
	suppressNotifications bool
	// attachments are the send's new uploads, created as the sender's for what the send writes (its
	// message, or the input it queues) before either is announced or delivered. A new send with
	// attachments is refused before anything is locked or written unless both the handler and the
	// channel support them; a replay re-issues the uploads the first send created instead.
	attachments []*proto_models.AttachmentMetadata
}

// sentMessage is the outcome of one send: the message it wrote, or the input it queued for a busy
// or suspended agent session instead (exactly one is set), the receipts of the agent sessions it
// reached as input, and the upload instructions of its attachments.
type sentMessage struct {
	message     *common_service_models.ChatMessage
	queuedInput *common_service_models.ChatAgentQueuedInput
	receipts    []*proto_models.AgentInputReceipt
	uploads     []*proto_models.AttachmentUploadInfo
}

// sendMessageWithContentType validates, authorizes, and writes one message in the caller's
// transaction, then routes it to the agent sessions it reaches. A repeated client message ID
// returns the earlier send instead. The agent sessions are locked and each one's route decided
// before anything is written, so a refusal (a busy BLOCKED session, a full queue) writes nothing,
// and a send that must wait for a busy or suspended session is queued for it instead of written
// as a message.
func (h *Handler[U]) sendMessageWithContentType(
	ctx context.Context,
	input SendMessageInput,
	contentType proto_models.MessageContentType,
	streamState proto_models.MessageStreamState,
	role proto_models.MessageRole,
	opts sendOptions,
) (*sentMessage, error) {
	channelID := input.ChannelID
	content := input.Content
	parentMessageID := input.ParentMessageID
	replyToMessageID := input.ReplyToMessageID
	embeds := input.Embeds
	limits := h.policy()
	if !limits.EnableChat() {
		return nil, serviceErrors.ErrChatDisabled
	}
	// The hot key limit applies to new sends only (below); a replay needs a key the store can hold.
	if strings.ContainsRune(input.ClientMessageID, '\x00') || len(input.ClientMessageID) > config.MaxChatMaxClientMessageIDLength {
		return nil, serviceErrors.ErrInvalidRequest
	}

	actor, actorID, err := h.getActor(ctx)
	if err != nil {
		return nil, err
	}
	if role == proto_models.MessageRole_MESSAGE_ROLE_UNSPECIFIED {
		role = proto_models.MessageRole_MESSAGE_ROLE_USER
		if actor.IsBot() {
			role = proto_models.MessageRole_MESSAGE_ROLE_ASSISTANT
		}
	}
	if err := h.requireTransaction(ctx); err != nil {
		return nil, err
	}
	channel, err := h.requireChannelMember(ctx, channelID, actorID)
	if err != nil {
		return nil, err
	}
	// A retry returns the earlier send whatever changed since it committed: its thread or reply
	// target, its mentions, the hot limits, or the application's view of the send. Only
	// membership is checked again here; over HTTP the request sanitizer has already checked the
	// retry against the current hot limits.
	if input.ClientMessageID != "" {
		if replay, err := h.replaySentMessage(ctx, actor, channel, input.ClientMessageID, opts.attachments); err != nil || replay != nil {
			return replay, err
		}
	}
	if len(opts.attachments) > 0 && (!h.SupportsAttachments() || !channel.GetSupportAttachments()) {
		return nil, serviceErrors.ErrChatFeatureDisabled
	}
	if limits.ChatMaxClientMessageIDLength() > 0 && len(input.ClientMessageID) > limits.ChatMaxClientMessageIDLength() {
		return nil, serviceErrors.ErrInvalidRequest
	}
	if limits.ChatMaxMessageLength() > 0 && len(content) > limits.ChatMaxMessageLength() {
		return nil, serviceErrors.ErrInvalidRequest
	}
	// A send bounds its embeds and attachments together.
	if limits.ChatMaxEmbedsPerMessage() > 0 && len(embeds)+len(opts.attachments) > limits.ChatMaxEmbedsPerMessage() {
		return nil, serviceErrors.ErrInvalidRequest
	}
	if err := h.validateThreadAndReplyReferences(ctx, channel, parentMessageID, replyToMessageID, input.BroadcastToChannel); err != nil {
		return nil, err
	}
	var agentRunPtr *uint64
	if !isNilOrZero(input.AgentRunID) {
		if err := h.validateSendAgentRun(ctx, *input.AgentRunID, channelID, parentMessageID, actorID); err != nil {
			return nil, err
		}
		runID := *input.AgentRunID
		agentRunPtr = &runID
	}
	mentionUserIDs, err := h.validateMessageMentions(ctx, channelID, content, contentType)
	if err != nil {
		return nil, err
	}
	if err := h.validateMessageEmbeds(channel, embeds); err != nil {
		return nil, err
	}

	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation:              ports.ChatPermissionSendMessage,
		Actor:                  actor,
		Channel:                channel,
		ParentMessageID:        parentMessageID,
		ReplyToMessageID:       replyToMessageID,
		BroadcastToChannel:     input.BroadcastToChannel,
		ProposedMessageContent: content,
		ProposedEmbedCount:     len(embeds),
		ProposedMentionUserIDs: mentionUserIDs,
		ProposedMentionCount:   len(mentionUserIDs),
	}); err != nil {
		return nil, err
	}

	// The initial message of a session and a delegated thread root mint their runs themselves, so
	// they reach no session here.
	var targets []*agentInputTarget
	if !opts.suppressAgentAutoRun {
		if targets, err = h.lockAgentInputTargets(ctx, channelID, parentMessageID, input.BroadcastToChannel, actorID, common_util.Deref(agentRunPtr)); err != nil {
			return nil, err
		}
		if targets, err = h.routeAgentInput(targets, agentRunPtr != nil, input.BroadcastToChannel); err != nil {
			return nil, err
		}
	}

	var parentPtr *uint64
	if parentMessageID != 0 {
		parentPtr = &parentMessageID
	}
	var replyPtr *uint64
	if replyToMessageID != 0 {
		replyPtr = &replyToMessageID
	}
	if target := waitingAgentInputTarget(targets); target != nil {
		return h.queueAgentInput(ctx, actor, channel, target, persistence.NewChatAgentQueuedInput{
			SenderID:         actorID,
			ReplyToMessageID: replyPtr,
			ClientMessageID:  input.ClientMessageID,
			Content:          content,
			ContentType:      contentType,
			MentionUserIDs:   mentionUserIDs,
			Embeds:           embeds,
		}, opts.attachments)
	}

	created, createErr := h.persistenceHandler.SendChatMessage(ctx, persistence.SendChatMessageInput{
		ChannelID:          channelID,
		SenderID:           actorID,
		Content:            content,
		ContentType:        contentType,
		StreamState:        streamState,
		ParentMessageID:    parentPtr,
		ReplyToMessageID:   replyPtr,
		BroadcastToChannel: input.BroadcastToChannel,
		AgentRunID:         agentRunPtr,
		Embeds:             embeds,
		Mentions: common_util.ApplyToEach(mentionUserIDs, func(userId uint64) persistence.CreateChatMessageMention {
			return persistence.CreateChatMessageMention{UserID: userId}
		}),
		Role:            role,
		ClientMessageID: input.ClientMessageID,
	})
	if createErr != nil {
		switch {
		case errors.Is(createErr, persistence.ErrDatabaseError):
			return nil, serviceErrors.ErrDatabaseError
		default:
			return nil, serviceErrors.ErrInternalError
		}
	}
	sent := &sentMessage{}
	if len(opts.attachments) > 0 {
		var attachmentIDs []uint64
		if attachmentIDs, sent.uploads, err = h.createSendAttachmentUploads(ctx, actorID, messageAttachmentOwner(created.ID), opts.attachments); err != nil {
			return nil, err
		}
		if len(attachmentIDs) > 0 {
			if created, err = h.persistenceHandler.UpdateChatMessageAttachmentIDs(ctx, created.ID, attachmentIDs); err != nil {
				return nil, mapChatMessagePersistenceError(err)
			}
		}
	}

	if h.notificationSink != nil && !opts.suppressNotifications {
		if err := h.notificationSink.ChatMessageRecorded(ctx, created.ID); err != nil {
			return nil, err
		}
	}

	// Sending updates the channel's last_message fields, so the cached channel
	// entry must be refreshed (membership entries stay valid).
	h.invalidateChannelAccessCache(ctx, channelID)

	createdMessage := h.converter.PersistenceChatMessageToService(created)
	refreshMessage := h.newPostCommitMessageRefresher(created.ID, createdMessage, "send event")
	if h.eventPublisher != nil && createdMessage != nil {
		h.publishChatEventAfterCommit(ctx, func(publishCtx context.Context) *ports.ChatEvent {
			return &ports.ChatEvent{
				Type:        proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_SENT,
				OccurredAt:  time.Now(),
				ActorUserID: common_service_models.UserIdType(actorID),
				Message:     refreshMessage(publishCtx),
			}
		})
	}
	// The sessions the message reaches receive it in this transaction, under the locks taken above:
	// it starts a run or, as a delegated child's report, wakes the parent. Runs are driven after
	// commit.
	sent.message = createdMessage
	if createdMessage != nil {
		if sent.receipts, err = h.deliverAgentInput(ctx, actorID, createdMessage, targets); err != nil {
			return nil, err
		}
	}
	return sent, nil
}

// EditMessage executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) EditMessage(
	ctx context.Context,
	messageID uint64,
	content string,
	embeds []persistence.CreateChatMessageEmbed,
	attachmentIDs []uint64,
) (*common_service_models.ChatMessage, error) {
	return h.editMessageWithContentType(
		ctx,
		messageID,
		content,
		proto_models.MessageContentType_MESSAGE_CONTENT_TYPE_TEXT,
		nil,
		embeds,
		attachmentIDs,
	)
}

// EditBotMessage executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) EditBotMessage(
	ctx context.Context,
	messageID uint64,
	content string,
	contentType proto_models.MessageContentType,
	embeds []persistence.CreateChatMessageEmbed,
	attachmentIDs []uint64,
) (*common_service_models.ChatMessage, error) {
	normalizedContentType, err := normalizeBotMessageContentType(content, contentType)
	if err != nil {
		return nil, err
	}

	return h.editMessageWithContentType(
		ctx,
		messageID,
		content,
		normalizedContentType,
		nil,
		embeds,
		attachmentIDs,
	)
}

// EditBotMessageWithStreamState updates a bot-authored message and applies an explicit terminal stream state.
func (h *Handler[U]) EditBotMessageWithStreamState(
	ctx context.Context,
	messageID uint64,
	content string,
	contentType proto_models.MessageContentType,
	streamState proto_models.MessageStreamState,
	embeds []persistence.CreateChatMessageEmbed,
	attachmentIDs []uint64,
) (*common_service_models.ChatMessage, error) {
	normalizedContentType, err := normalizeBotMessageContentType(content, contentType)
	if err != nil {
		return nil, err
	}

	return h.editMessageWithContentType(
		ctx,
		messageID,
		content,
		normalizedContentType,
		&streamState,
		embeds,
		attachmentIDs,
	)
}

// editMessageWithContentType executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) editMessageWithContentType(
	ctx context.Context,
	messageID uint64,
	content string,
	contentType proto_models.MessageContentType,
	streamState *proto_models.MessageStreamState,
	embeds []persistence.CreateChatMessageEmbed,
	attachmentIDs []uint64,
) (*common_service_models.ChatMessage, error) {
	limits := h.policy()
	if !limits.EnableChat() {
		return nil, serviceErrors.ErrChatDisabled
	}
	if limits.ChatMaxMessageLength() > 0 && len(content) > limits.ChatMaxMessageLength() {
		return nil, serviceErrors.ErrInvalidRequest
	}

	actor, actorID, err := h.getActor(ctx)
	if err != nil {
		return nil, err
	}
	if err := h.requireTransaction(ctx); err != nil {
		return nil, err
	}

	existing, getErr := h.persistenceHandler.GetChatMessageByID(ctx, messageID)
	if getErr != nil {
		return nil, mapChatMessagePersistenceError(getErr)
	}

	if err := requireStandardMutableMessage(existing); err != nil {
		return nil, err
	}

	channel, err := h.requireChannelMember(ctx, existing.ChannelID, actorID)
	if err != nil {
		return nil, err
	}
	if !channel.GetSupportMessageEdits() {
		return nil, serviceErrors.ErrChatFeatureDisabled
	}
	if existing.SenderID != actorID {
		return nil, serviceErrors.ErrInsufficientPermissions
	}

	messageForAuth := h.converter.PersistenceChatMessageToService(existing)
	mentionUserIDs, err := h.validateMessageMentions(ctx, existing.ChannelID, content, contentType)
	if err != nil {
		return nil, err
	}
	if err := h.validateMessageEmbeds(channel, embeds); err != nil {
		return nil, err
	}
	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation:              ports.ChatPermissionEditMessage,
		Actor:                  actor,
		Channel:                channel,
		Message:                messageForAuth,
		ProposedMessageContent: content,
		ProposedEmbedCount:     len(embeds),
		ProposedMentionUserIDs: mentionUserIDs,
		ProposedMentionCount:   len(mentionUserIDs),
	}); err != nil {
		return nil, err
	}

	updated, updateErr := h.persistenceHandler.EditChatMessage(ctx, persistence.EditChatMessageInput{
		MessageID:      messageID,
		EditedByUserID: actorID,
		NewContent:     content,
		NewContentType: contentType,
		NewStreamState: streamState,
		Embeds:         embeds,
		Mentions: common_util.ApplyToEach(mentionUserIDs, func(userId uint64) persistence.CreateChatMessageMention {
			return persistence.CreateChatMessageMention{UserID: userId}
		}),
		AttachmentIDs: attachmentIDs,
	})
	if updateErr != nil {
		switch {
		case errors.Is(updateErr, persistence.ErrChatMessageNotFound):
			return nil, serviceErrors.ErrChatMessageNotFound
		case errors.Is(updateErr, persistence.ErrDatabaseError):
			return nil, serviceErrors.ErrDatabaseError
		default:
			return nil, serviceErrors.ErrInternalError
		}
	}

	// Editing the latest message can rewrite the channel's last_message
	// preview, so the cached channel entry must be refreshed.
	h.invalidateChannelAccessCache(ctx, updated.ChannelID)

	updatedMessage := h.converter.PersistenceChatMessageToService(updated)
	messageID = updated.ID
	refreshMessage := h.newPostCommitMessageRefresher(messageID, updatedMessage, "edit event")
	if h.eventPublisher != nil && updatedMessage != nil {
		h.publishChatEventAfterCommit(ctx, func(publishCtx context.Context) *ports.ChatEvent {
			return &ports.ChatEvent{
				Type:        proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_EDITED,
				OccurredAt:  time.Now(),
				ActorUserID: common_service_models.UserIdType(actorID),
				Message:     refreshMessage(publishCtx),
			}
		})
	}
	return updatedMessage, nil
}

// DeleteMessage executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) DeleteMessage(ctx context.Context, messageID uint64) error {
	if h.chatDisabled() {
		return serviceErrors.ErrChatDisabled
	}

	actor, actorID, err := h.getActor(ctx)
	if err != nil {
		return err
	}
	if err := h.requireTransaction(ctx); err != nil {
		return err
	}

	existing, getErr := h.persistenceHandler.GetChatMessageByID(ctx, messageID)
	if getErr != nil {
		if errors.Is(getErr, persistence.ErrChatMessageNotFound) {
			return serviceErrors.ErrChatMessageNotFound
		}
		return mapChatMessagePersistenceError(getErr,
			zap.String(logkeys.Stage, "delete_message_lookup"),
			zap.Uint64(appLogger.KeyActorUserID, actorID),
			zap.Uint64(appLogger.KeyMessageID, messageID))
	}

	if err := requireStandardMutableMessage(existing); err != nil {
		return err
	}

	channel, err := h.requireChannelMember(ctx, existing.ChannelID, actorID)
	if err != nil {
		return err
	}
	if !channel.GetSupportMessageDeletes() {
		return serviceErrors.ErrChatFeatureDisabled
	}
	if existing.SenderID != actorID {
		return serviceErrors.ErrInsufficientPermissions
	}

	deletedMessage := h.converter.PersistenceChatMessageToService(existing)
	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation: ports.ChatPermissionDeleteMessage,
		Actor:     actor,
		Channel:   channel,
		Message:   deletedMessage,
	}); err != nil {
		return err
	}
	if delErr := h.persistenceHandler.DeleteChatMessage(ctx, existing.ChannelID, messageID); delErr != nil {
		if errors.Is(delErr, persistence.ErrChatMessageNotFound) {
			return serviceErrors.ErrChatMessageNotFound
		}
		return mapChatMessagePersistenceError(delErr,
			zap.String(logkeys.Stage, "delete_message"),
			zap.Uint64(appLogger.KeyActorUserID, actorID),
			zap.Uint64(appLogger.KeyChannelID, existing.ChannelID),
			zap.Uint64(appLogger.KeyMessageID, messageID))
	}

	h.invalidateChannelAccessCache(ctx, existing.ChannelID)

	if h.eventPublisher != nil && deletedMessage != nil {
		h.publishChatEventAfterCommit(ctx, func(context.Context) *ports.ChatEvent {
			return &ports.ChatEvent{
				Type:        proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_DELETED,
				OccurredAt:  time.Now(),
				ActorUserID: common_service_models.UserIdType(actorID),
				Message:     deletedMessage,
			}
		})
	}

	return nil
}

// GetMessage executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) GetMessage(ctx context.Context, messageID uint64, includeHidden bool) (*common_service_models.ChatMessage, error) {
	if h.chatDisabled() {
		return nil, serviceErrors.ErrChatDisabled
	}

	actor, _, err := h.getActor(ctx)
	if err != nil {
		return nil, err
	}

	var channel *common_service_models.ChatChannel
	if includeHidden {
		// Preserve the hidden-read fail-closed behavior before even the metadata lookup when there
		// is no authority source.
		if !h.hasHiddenReadAuthority(ctx, actor) {
			return nil, serviceErrors.ErrInsufficientPermissions
		}
		channelID, resolveErr := h.persistenceHandler.GetChatMessageChannelID(ctx, messageID)
		if resolveErr != nil {
			switch {
			case errors.Is(resolveErr, persistence.ErrChatMessageNotFound):
				// Without a channel there is no scope against which to authorize the hidden read.
				// Match the denied result so hidden message IDs do not become an existence oracle.
				return nil, serviceErrors.ErrInsufficientPermissions
			case errors.Is(resolveErr, persistence.ErrDatabaseError):
				return nil, serviceErrors.ErrDatabaseError
			default:
				return nil, serviceErrors.ErrInternalError
			}
		}
		channel, _, _, err = h.requireChannelReadAccess(ctx, channelID, actor, true)
		if err != nil {
			// Membership and channel-not-found outcomes (archive state never fails
			// the read gate) are only reached here because the hidden row exists.
			// Collapsing them into the same denial as "no such message" keeps
			// include_hidden from acting as an existence/state oracle for actors who
			// hold no hidden-read grant. A failed organization role lookup is
			// collapsed too. Infrastructure failures keep their own mapping.
			if errors.Is(err, serviceErrors.ErrDatabaseError) || errors.Is(err, serviceErrors.ErrInternalError) {
				return nil, err
			}
			return nil, serviceErrors.ErrInsufficientPermissions
		}
	}

	readOpts, err := h.resolveReadOptions(ctx, actor, channel, includeHidden)
	if err != nil {
		return nil, err
	}

	msg, getErr := h.persistenceHandler.GetChatMessageByIDWithOptions(ctx, messageID, readOpts)
	if getErr != nil {
		return nil, mapChatMessagePersistenceError(getErr)
	}
	if channel != nil && msg.ChannelID != channel.ID {
		// Custom persistence implementations may predate ChatMessageReadOptions.ChannelID and
		// ignore that additive scope. Never return a row from a channel other than the one
		// checked, and deny it the same way every other hidden-path failure is denied.
		return nil, serviceErrors.ErrInsufficientPermissions
	}

	if channel == nil {
		channel, err = h.requireChannelRead(ctx, msg.ChannelID, actor)
		if err != nil {
			return nil, err
		}
	}

	out := h.converter.PersistenceChatMessageToService(msg)
	if err := h.hydrateMessageAttachments(ctx, out); err != nil {
		return nil, err
	}
	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation: ports.ChatPermissionGetMessage,
		Actor:     actor,
		Channel:   channel,
		Message:   out,
	}); err != nil {
		return nil, err
	}

	return out, nil
}

// ListMessageEditHistory executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) ListMessageEditHistory(
	ctx context.Context,
	messageID uint64,
	pagination *proto_models.PaginationRequest,
) ([]*common_service_models.ChatMessageEditHistoryEntry, *proto_models.PaginationResponse, error) {
	if h.chatDisabled() {
		return nil, nil, serviceErrors.ErrChatDisabled
	}

	actor, _, err := h.getActor(ctx)
	if err != nil {
		return nil, nil, err
	}

	msg, getErr := h.persistenceHandler.GetChatMessageByID(ctx, messageID)
	if getErr != nil {
		return nil, nil, mapChatMessagePersistenceError(getErr)
	}

	channel, err := h.requireChannelRead(ctx, msg.ChannelID, actor)
	if err != nil {
		return nil, nil, err
	}
	if !channel.GetSupportMessageEdits() {
		return nil, nil, serviceErrors.ErrChatFeatureDisabled
	}

	messageForAuth := h.converter.PersistenceChatMessageToService(msg)
	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation: ports.ChatPermissionListMessageEditHistory,
		Actor:     actor,
		Channel:   channel,
		Message:   messageForAuth,
	}); err != nil {
		return nil, nil, err
	}

	rows, page, listErr := h.persistenceHandler.ListChatMessageEditHistory(ctx, messageID, pagination)
	if listErr != nil {
		switch {
		case errors.Is(listErr, persistence.ErrDatabaseError):
			return nil, nil, serviceErrors.ErrDatabaseError
		default:
			return nil, nil, serviceErrors.ErrInternalError
		}
	}

	out := common_util.ApplyToEach(rows, func(row *common_persistence_models.ChatMessageEditHistory) *common_service_models.ChatMessageEditHistoryEntry {
		return h.converter.PersistenceChatMessageEditHistoryToService(row)
	})
	return out, page, nil
}

// BatchGetMessages executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) BatchGetMessages(
	ctx context.Context,
	channelID uint64,
	messageIDs []uint64,
	includeHidden bool,
) ([]*common_service_models.ChatMessage, []uint64, error) {
	if h.chatDisabled() {
		return nil, nil, serviceErrors.ErrChatDisabled
	}

	actor, _, err := h.getActor(ctx)
	if err != nil {
		return nil, nil, err
	}
	channel, _, _, err := h.requireChannelReadAccess(ctx, channelID, actor, includeHidden)
	if err != nil {
		return nil, nil, err
	}

	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation:  ports.ChatPermissionBatchGetMessages,
		Actor:      actor,
		Channel:    channel,
		MessageIDs: messageIDs,
	}); err != nil {
		return nil, nil, err
	}

	readOpts, err := h.resolveReadOptions(ctx, actor, channel, includeHidden)
	if err != nil {
		return nil, nil, err
	}

	rows, missing, listErr := h.persistenceHandler.BatchGetChatMessagesWithOptions(ctx, channelID, messageIDs, readOpts)
	if listErr != nil {
		switch {
		case errors.Is(listErr, persistence.ErrDatabaseError):
			return nil, nil, serviceErrors.ErrDatabaseError
		default:
			return nil, nil, serviceErrors.ErrInternalError
		}
	}

	out := common_util.ApplyToEach(rows, func(m *common_persistence_models.ChatMessage) *common_service_models.ChatMessage {
		return h.converter.PersistenceChatMessageToService(m)
	})
	if err := h.hydrateMessagesAttachments(ctx, out); err != nil {
		return nil, nil, err
	}
	return out, missing, nil
}

// ListMessages executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) ListMessages(
	ctx context.Context,
	channelID uint64,
	pagination *proto_models.PaginationRequest,
	parentMessageID uint64,
	includeDeleted bool,
	sort *proto_models.SortRequest,
	sortBy proto_models.ListMessagesSortBy,
	includeHidden bool,
) ([]*common_service_models.ChatMessage, *proto_models.PaginationResponse, error) {
	if h.chatDisabled() {
		return nil, nil, serviceErrors.ErrChatDisabled
	}

	actor, actorID, err := h.getActor(ctx)
	if err != nil {
		return nil, nil, err
	}
	channel, _, _, err := h.requireChannelReadAccess(ctx, channelID, actor, includeHidden || includeDeleted)
	if err != nil {
		return nil, nil, err
	}

	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation:       ports.ChatPermissionListMessages,
		Actor:           actor,
		Channel:         channel,
		ParentMessageID: parentMessageID,
		IncludeDeleted:  includeDeleted,
	}); err != nil {
		return nil, nil, err
	}

	readOpts, err := h.resolveReadOptions(ctx, actor, channel, includeHidden)
	if err != nil {
		return nil, nil, err
	}
	readOpts.IncludeDeleted = includeDeleted
	return h.listChannelMessages(ctx, channel, pagination, parentMessageID, readOpts, sort, sortBy, actorID)
}

// listChannelMessages is the message listing shared by ListMessages and ListPublicMessages once the
// caller has passed its read gate and resolved readOpts: it checks the thread root, runs the query,
// enriches main-timeline rows with reply counts and hydrates attachments. actorID only labels logs
// (0 for an anonymous reader).
func (h *Handler[U]) listChannelMessages(
	ctx context.Context,
	channel *common_service_models.ChatChannel,
	pagination *proto_models.PaginationRequest,
	parentMessageID uint64,
	readOpts persistence.ChatMessageReadOptions,
	sort *proto_models.SortRequest,
	sortBy proto_models.ListMessagesSortBy,
	actorID uint64,
) ([]*common_service_models.ChatMessage, *proto_models.PaginationResponse, error) {
	channelID := uint64(channel.ID)
	if parentMessageID != 0 {
		if !channel.GetSupportThreads() {
			return nil, nil, serviceErrors.ErrChatFeatureDisabled
		}
		if _, err := h.requireThreadRoot(ctx, channel, parentMessageID); err != nil {
			return nil, nil, err
		}
	}

	rows, pageResp, listErr := h.persistenceHandler.ListChatMessagesWithOptions(ctx, channelID, pagination, parentMessageID, readOpts, sort, sortBy)
	if listErr != nil {
		return nil, nil, mapChatMessagePersistenceError(listErr,
			zap.String(logkeys.Stage, "list_messages"),
			zap.Uint64(appLogger.KeyActorUserID, actorID),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Uint64(appLogger.KeyParentMessageID, parentMessageID))
	}

	// Enrich messages with reply counts when threads are enabled and viewing main timeline
	if channel.GetSupportThreads() && parentMessageID == 0 && len(rows) > 0 {
		messageIDs := make([]uint64, len(rows))
		for i, m := range rows {
			messageIDs[i] = m.ID
		}
		replyCounts, countErr := h.persistenceHandler.GetChatMessageReplyCounts(ctx, channelID, messageIDs)
		if countErr == nil {
			for _, m := range rows {
				if count, ok := replyCounts[m.ID]; ok {
					m.ReplyCount = count
				}
			}
		} else {
			appLogger.LogOperationWarn(appLogger.For(ctx, h.logger, chatLoggerName), "Failed to enrich chat messages with reply counts", "list_messages_reply_counts", countErr,
				zap.Uint64(appLogger.KeyActorUserID, actorID),
				zap.Uint64(appLogger.KeyChannelID, channelID),
				zap.Int(appLogger.KeyMessageCount, len(messageIDs)))
		}
		// Non-fatal: if count query fails, messages are still returned without reply counts
	}

	out := common_util.ApplyToEach(rows, func(m *common_persistence_models.ChatMessage) *common_service_models.ChatMessage {
		return h.converter.PersistenceChatMessageToService(m)
	})
	if err := h.hydrateMessagesAttachments(ctx, out); err != nil {
		return nil, nil, err
	}
	return out, pageResp, nil
}
