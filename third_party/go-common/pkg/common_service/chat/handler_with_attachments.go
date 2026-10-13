package chat

import (
	"context"
	"errors"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// ChatAttachmentOwner is what a chat upload belongs to: a message (ObjectTypeChatAttachment), or,
// until the agent receives it, a queued agent input (ObjectTypeChatQueuedInputAttachment), whose
// delivery moves its uploads to the message it becomes. ID is the owner's ID.
type ChatAttachmentOwner struct {
	ObjectType string
	ID         uint64
}

// messageAttachmentOwner names a chat message as the owner of its uploads.
func messageAttachmentOwner(messageID uint64) ChatAttachmentOwner {
	return ChatAttachmentOwner{ObjectType: common_persistence_models.ObjectTypeChatAttachment, ID: messageID}
}

// queuedInputAttachmentOwner names a queued agent input as the owner of its uploads.
func queuedInputAttachmentOwner(queuedInputID uint64) ChatAttachmentOwner {
	return ChatAttachmentOwner{ObjectType: common_persistence_models.ObjectTypeChatQueuedInputAttachment, ID: queuedInputID}
}

type AttachmentCoordinator interface {
	CreateChatAttachmentUploads(
		ctx context.Context,
		userID uint64,
		owner ChatAttachmentOwner,
		attachments []*proto_models.AttachmentMetadata,
	) ([]uint64, []*proto_models.AttachmentUploadInfo, error)
	PlanChatAttachmentReplacement(
		ctx context.Context,
		userID uint64,
		owner ChatAttachmentOwner,
		attachments []*proto_models.AttachmentMetadata,
		existingAttachmentIDs []uint64,
	) ([]uint64, []uint64, []*proto_models.AttachmentUploadInfo, error)
	BuildOrderedChatAttachmentIDs(
		ctx context.Context,
		requestAttachments []*proto_models.AttachmentMetadata,
		existingAttachmentIDs []uint64,
		newAttachmentIDs []uint64,
	) ([]uint64, error)
	// ReissueChatAttachmentUploads returns fresh upload instructions for a replayed send's
	// attachments (attachmentIDs) the sender has not uploaded yet: those of the replayed request's
	// files (requestAttachments), matched by content hash.
	ReissueChatAttachmentUploads(
		ctx context.Context,
		userID uint64,
		attachmentIDs []uint64,
		requestAttachments []*proto_models.AttachmentMetadata,
	) ([]*proto_models.AttachmentUploadInfo, error)
	FinalizeRemovedChatAttachments(ctx context.Context, attachmentIDs []uint64) error
}

// SendMessageWithAttachmentsInput is a person's send with its new uploads. The uploads are created
// as the sender's, for whatever the send writes.
type SendMessageWithAttachmentsInput struct {
	ChannelID          uint64
	Content            string
	ParentMessageID    uint64
	ReplyToMessageID   uint64
	BroadcastToChannel bool
	Embeds             []*proto_models.CreateMessageEmbedRequest
	Attachments        []*proto_models.AttachmentMetadata
	// ClientMessageID is the sender's idempotency key; see SendMessageInput.ClientMessageID.
	ClientMessageID string
}

// SendMessageWithAttachmentsResult is the outcome of a send: exactly one of Message and QueuedInput
// is set.
type SendMessageWithAttachmentsResult struct {
	// Message is the message the send wrote, or, when it joined input already queued for an idle
	// agent session, the message its input was delivered as.
	Message *common_service_models.ChatMessage
	// QueuedInput is the input the send queued for a busy or suspended agent session instead of
	// writing a message; delivery writes the message when the agent receives it.
	QueuedInput *common_service_models.ChatAgentQueuedInput
	// NewAttachments are the upload instructions for the send's attachments, owned by the message
	// or the queued input. A replayed send returns fresh instructions for the attachments that were
	// not uploaded yet.
	NewAttachments []*proto_models.AttachmentUploadInfo
	// AgentInputReceipts report, per agent session the send reached as input, whether it started a
	// run (RUN_STARTED), was queued (PENDING, or HELD while the session is suspended), or was
	// delivered into a run (ADMITTED).
	AgentInputReceipts []*proto_models.AgentInputReceipt
}

// EditMessageWithAttachmentsInput defines the chat package data shape used in the backend request and persistence flow.
// It is used to carry state between handlers, service orchestration, and storage integrations in this package.
type EditMessageWithAttachmentsInput struct {
	ActorUserID        uint64
	MessageID          uint64
	Content            string
	Embeds             []*proto_models.CreateMessageEmbedRequest
	ReplaceEmbeds      bool
	Attachments        []*proto_models.AttachmentMetadata
	ReplaceAttachments bool
}

// EditMessageWithAttachmentsResult defines the chat package data shape used in the backend request and persistence flow.
// It is used to carry state between handlers, service orchestration, and storage integrations in this package.
type EditMessageWithAttachmentsResult struct {
	Message        *common_service_models.ChatMessage
	NewAttachments []*proto_models.AttachmentUploadInfo
}

// createChatMessageEmbedsFromProto executes part of the chat package flow and provides shared helper behavior.
func createChatMessageEmbedsFromProto(reqEmbeds []*proto_models.CreateMessageEmbedRequest) []persistence.CreateChatMessageEmbed {
	return common_util.ApplyToEachFiltered(
		reqEmbeds,
		func(e *proto_models.CreateMessageEmbedRequest) persistence.CreateChatMessageEmbed {
			return persistence.CreateChatMessageEmbed{
				EmbedType:    e.EmbedType,
				EmbedData:    e.EmbedData,
				FallbackText: e.FallbackText,
				Position:     e.Position,
			}
		},
		func(e *proto_models.CreateMessageEmbedRequest) bool {
			return e != nil
		},
		nil,
	)
}

// buildEditMessageEmbeds executes part of the chat package flow and provides shared helper behavior.
func buildEditMessageEmbeds(
	reqEmbeds []*proto_models.CreateMessageEmbedRequest,
	existingEmbeds []common_persistence_models.ChatMessageEmbed,
	replaceEmbeds bool,
) []persistence.CreateChatMessageEmbed {
	if !replaceEmbeds {
		out := common_util.ApplyToEach(existingEmbeds, func(embed common_persistence_models.ChatMessageEmbed) persistence.CreateChatMessageEmbed {
			return persistence.CreateChatMessageEmbed{
				EmbedType:    embed.EmbedType,
				EmbedData:    embed.EmbedData,
				FallbackText: embed.FallbackText,
				Position:     embed.Position,
			}
		})
		for i := range out {
			out[i].Position = uint32(i)
		}
		return out
	}

	out := createChatMessageEmbedsFromProto(reqEmbeds)
	for i := range out {
		out[i].Position = uint32(i)
	}
	return out
}

// SendMessageWithAttachments sends a plain-text message with its new uploads as the context actor.
// The send writes the message, or queues the input for a busy or suspended agent session, and the
// uploads belong to whichever it wrote; the result's attachments and the queued input's sender are
// hydrated for the response.
func (h *Handler[U]) SendMessageWithAttachments(
	ctx context.Context,
	input SendMessageWithAttachmentsInput,
) (*SendMessageWithAttachmentsResult, error) {
	sent, err := h.sendText(ctx, SendMessageInput{
		ChannelID:          input.ChannelID,
		Content:            input.Content,
		ParentMessageID:    input.ParentMessageID,
		ReplyToMessageID:   input.ReplyToMessageID,
		BroadcastToChannel: input.BroadcastToChannel,
		Embeds:             createChatMessageEmbedsFromProto(input.Embeds),
		ClientMessageID:    input.ClientMessageID,
	}, sendOptions{attachments: input.Attachments})
	if err != nil {
		return nil, err
	}
	if err := h.hydrateMessageAttachments(ctx, sent.message); err != nil {
		return nil, err
	}
	return &SendMessageWithAttachmentsResult{
		Message:            sent.message,
		QueuedInput:        sent.queuedInput,
		NewAttachments:     sent.uploads,
		AgentInputReceipts: sent.receipts,
	}, nil
}

// createSendAttachmentUploads creates a new send's uploads as the sender's, owned by owner (the
// message it wrote or the input it queued), and returns their IDs in request order with their
// upload instructions.
func (h *Handler[U]) createSendAttachmentUploads(
	ctx context.Context,
	senderID uint64,
	owner ChatAttachmentOwner,
	attachments []*proto_models.AttachmentMetadata,
) ([]uint64, []*proto_models.AttachmentUploadInfo, error) {
	coordinator, err := h.requireAttachmentCoordinator()
	if err != nil {
		return nil, nil, err
	}
	newAttachmentIDs, uploads, err := coordinator.CreateChatAttachmentUploads(ctx, senderID, owner, attachments)
	if err != nil || len(newAttachmentIDs) == 0 {
		return nil, uploads, err
	}
	attachmentIDs, err := coordinator.BuildOrderedChatAttachmentIDs(ctx, attachments, nil, newAttachmentIDs)
	if err != nil {
		return nil, nil, err
	}
	return attachmentIDs, uploads, nil
}

// reissueSentAttachmentUploads returns fresh upload instructions for a replayed send's attachments
// (attachmentIDs, of the message it wrote or the input it queued) that were not uploaded yet, so a
// client that lost the first response can still upload them.
func (h *Handler[U]) reissueSentAttachmentUploads(
	ctx context.Context,
	senderID uint64,
	attachmentIDs []uint64,
	requested []*proto_models.AttachmentMetadata,
) ([]*proto_models.AttachmentUploadInfo, error) {
	if len(attachmentIDs) == 0 {
		return nil, nil
	}
	coordinator, err := h.requireAttachmentCoordinator()
	if err != nil {
		return nil, err
	}
	return coordinator.ReissueChatAttachmentUploads(ctx, senderID, attachmentIDs, requested)
}

// EditMessageWithAttachments executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) EditMessageWithAttachments(
	ctx context.Context,
	input EditMessageWithAttachmentsInput,
) (*EditMessageWithAttachmentsResult, error) {
	if len(input.Attachments) > 0 {
		if !h.SupportsAttachments() {
			return nil, serviceErrors.ErrChatFeatureDisabled
		}
	}

	existing, err := h.persistenceHandler.GetChatMessageByID(ctx, input.MessageID)
	if err != nil {
		return nil, mapChatMessagePersistenceError(err)
	}

	if len(input.Attachments) > 0 {
		channel, err := h.GetChannel(ctx, existing.ChannelID)
		if err != nil {
			return nil, err
		}
		if channel == nil || !channel.GetSupportAttachments() {
			return nil, serviceErrors.ErrChatFeatureDisabled
		}
	}

	existingAttachmentIDs := common_util.ConvertPqInt64ArrayToUint64Slice(existing.AttachmentIDs)
	embeds := buildEditMessageEmbeds(input.Embeds, existing.Embeds, input.ReplaceEmbeds)
	finalAttachmentIDs := append([]uint64(nil), existingAttachmentIDs...)
	var removedAttachmentIDs []uint64
	var newAttachments []*proto_models.AttachmentUploadInfo

	if input.ReplaceAttachments {
		coordinator, err := h.requireAttachmentCoordinator()
		if err != nil {
			return nil, err
		}

		actorUserID, err := h.resolveAttachmentActorID(ctx, input.ActorUserID)
		if err != nil {
			return nil, err
		}

		newAttachmentIDs, attachmentsToRemove, uploadInfos, err := coordinator.PlanChatAttachmentReplacement(
			ctx,
			actorUserID,
			messageAttachmentOwner(input.MessageID),
			input.Attachments,
			existingAttachmentIDs,
		)
		if err != nil {
			return nil, err
		}

		attachmentIDs, err := coordinator.BuildOrderedChatAttachmentIDs(ctx, input.Attachments, existingAttachmentIDs, newAttachmentIDs)
		if err != nil {
			return nil, err
		}

		finalAttachmentIDs = attachmentIDs
		removedAttachmentIDs = attachmentsToRemove
		newAttachments = uploadInfos
	} else if len(input.Attachments) > 0 {
		coordinator, err := h.requireAttachmentCoordinator()
		if err != nil {
			return nil, err
		}

		actorUserID, err := h.resolveAttachmentActorID(ctx, input.ActorUserID)
		if err != nil {
			return nil, err
		}

		newAttachmentIDs, uploadInfos, err := coordinator.CreateChatAttachmentUploads(ctx, actorUserID, messageAttachmentOwner(input.MessageID), input.Attachments)
		if err != nil {
			return nil, err
		}

		finalAttachmentIDs = append(finalAttachmentIDs, newAttachmentIDs...)
		newAttachments = uploadInfos
	}

	msg, err := h.EditMessage(
		ctx,
		input.MessageID,
		input.Content,
		embeds,
		finalAttachmentIDs,
	)
	if err != nil {
		return nil, err
	}

	if len(removedAttachmentIDs) > 0 {
		coordinator, err := h.requireAttachmentCoordinator()
		if err != nil {
			return nil, err
		}
		if err := coordinator.FinalizeRemovedChatAttachments(ctx, removedAttachmentIDs); err != nil {
			return nil, err
		}
	}

	if err := h.hydrateMessageAttachments(ctx, msg); err != nil {
		return nil, err
	}
	return &EditMessageWithAttachmentsResult{
		Message:        msg,
		NewAttachments: newAttachments,
	}, nil
}

// requireAttachmentCoordinator executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) requireAttachmentCoordinator() (AttachmentCoordinator, error) {
	if h.attachmentCoordinator == nil {
		return nil, serviceErrors.ErrInternalError
	}
	return h.attachmentCoordinator, nil
}

// resolveAttachmentActorID executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) resolveAttachmentActorID(ctx context.Context, actorUserID uint64) (uint64, error) {
	if actorUserID != 0 {
		return actorUserID, nil
	}

	_, resolvedActorID, err := h.getActor(ctx)
	if err != nil {
		return 0, err
	}
	return resolvedActorID, nil
}

func mapChatMessagePersistenceError(err error, fields ...zap.Field) error {
	var domainErr error
	switch {
	case errors.Is(err, persistence.ErrChatMessageAgentSessionRootPinned):
		domainErr = serviceErrors.ErrChatMessageAgentSessionRootPinned
	case errors.Is(err, persistence.ErrChatMessageNotFound):
		domainErr = serviceErrors.ErrChatMessageNotFound
	case errors.Is(err, persistence.ErrChatChannelNotFound):
		domainErr = serviceErrors.ErrChatChannelNotFound
	case errors.Is(err, persistence.ErrChatAgentSessionNotFound):
		domainErr = serviceErrors.ErrChatAgentSessionNotFound
	case errors.Is(err, persistence.ErrChatAgentRunNotFound):
		domainErr = serviceErrors.ErrChatAgentRunNotFound
	case errors.Is(err, persistence.ErrChatForkTooLarge):
		domainErr = serviceErrors.ErrChatForkTooLarge
	case errors.Is(err, persistence.ErrChatForkPointInvalid):
		domainErr = serviceErrors.ErrChatForkPointInvalid
	case errors.Is(err, persistence.ErrChatForkRequestKeyConflict):
		domainErr = serviceErrors.ErrChatForkRequestKeyConflict
	case errors.Is(err, persistence.ErrDatabaseError):
		domainErr = serviceErrors.ErrDatabaseError
	default:
		domainErr = serviceErrors.ErrInternalError
	}
	return appLogger.WithErrorCause(domainErr, err, fields...)
}
