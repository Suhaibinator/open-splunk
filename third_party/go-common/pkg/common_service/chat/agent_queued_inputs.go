package chat

import (
	"context"
	"errors"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// EditAgentQueuedInputInput rewords a queued agent input before the agent receives it: Content
// replaces its text, and Attachments its whole attachment list (one left out is removed). Its
// embeds and reply target are kept.
type EditAgentQueuedInputInput struct {
	QueuedInputID uint64
	Content       string
	Attachments   []*proto_models.AttachmentMetadata
}

// EditAgentQueuedInputResult is the edited queued input and the upload instructions of the
// attachments the edit added.
type EditAgentQueuedInputResult struct {
	QueuedInput    *common_service_models.ChatAgentQueuedInput
	NewAttachments []*proto_models.AttachmentUploadInfo
}

// ListAgentQueuedInputs pages an agent session's queue, oldest first and independently of
// transcript pages, for the composer's queue tray. Anyone who may list the session's messages may
// read it (the channel read gate and the ChatPermissionListMessages veto), read-only organization
// readers included, and sees everyone's queued input, each with whether they may edit or remove
// it. A page holds at most chat_agent_max_pending_inputs entries (hot KMS).
func (h *Handler[U]) ListAgentQueuedInputs(ctx context.Context, sessionID uint64, pagination *proto_models.PaginationRequest) ([]*common_service_models.ChatAgentQueuedInput, *proto_models.PaginationResponse, error) {
	if h.chatDisabled() {
		return nil, nil, serviceErrors.ErrChatDisabled
	}
	if sessionID == 0 {
		return nil, nil, serviceErrors.ErrInvalidRequest
	}
	actor, _, err := h.getActor(ctx)
	if err != nil {
		return nil, nil, err
	}
	session, err := h.persistenceHandler.GetChatAgentSessionByID(ctx, sessionID)
	if err != nil {
		if errors.Is(err, persistence.ErrChatAgentSessionNotFound) {
			return nil, nil, serviceErrors.ErrChatAgentSessionNotFound
		}
		return nil, nil, serviceErrors.ErrDatabaseError
	}
	channel, role, participant, err := h.requireChannelReadAccess(ctx, session.ChannelID, actor, false)
	if err != nil {
		return nil, nil, err
	}
	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation:       ports.ChatPermissionListMessages,
		Actor:           actor,
		Channel:         channel,
		ParentMessageID: session.ParentMessageID,
		AgentSessionID:  session.ID,
	}); err != nil {
		return nil, nil, err
	}
	limit := h.agentMaxPendingInputs()
	if requested := int(pagination.GetLimit()); requested > 0 && requested < limit {
		limit = requested
	}
	queued, page, err := h.persistenceHandler.ListChatAgentQueuedInputs(ctx, sessionID, pagination, limit)
	if err != nil {
		return nil, nil, serviceErrors.ErrDatabaseError
	}
	canChange := participant && !channel.IsArchived
	viewer := agentQueuedInputViewer[U]{
		actor:     actor,
		channel:   channel,
		canChange: canChange,
		isAdmin:   canChange && role >= proto_models.ChannelRole_CHANNEL_ROLE_ADMIN,
	}
	views, err := h.agentQueuedInputViews(ctx, viewer, queued...)
	if err != nil {
		return nil, nil, err
	}
	return views, page, nil
}

// EditAgentQueuedInput rewords input the context actor queued, before the agent receives it. Only
// its sender may edit it, while still taking part in the channel and it is not archived; the edit
// is validated as a send would be (length, mentions, attachments the channel takes, counted with
// the input's kept embeds) and the application's Authorize callback may veto it
// (ChatPermissionEditAgentQueuedInput). The new attachments are created for the queued input, and
// its pending uploads the edit drops are abandoned. The edit serializes with delivery: once the
// agent received the input it fails with ErrChatAgentQueuedInputNotQueued, and the caller's
// transaction rolls back everything it wrote. Requires a transaction.
func (h *Handler[U]) EditAgentQueuedInput(ctx context.Context, input EditAgentQueuedInputInput) (*EditAgentQueuedInputResult, error) {
	if h.chatDisabled() {
		return nil, serviceErrors.ErrChatDisabled
	}
	actor, actorID, err := h.getActor(ctx)
	if err != nil {
		return nil, err
	}
	if err := h.requireTransaction(ctx); err != nil {
		return nil, err
	}
	fields := []zap.Field{
		zap.String(logkeys.Stage, "edit_agent_queued_input"),
		zap.Uint64(appLogger.KeyQueuedInputID, input.QueuedInputID),
		zap.Uint64(appLogger.KeyUserID, actorID),
	}
	queued, err := h.persistenceHandler.GetChatAgentQueuedInput(ctx, input.QueuedInputID)
	if err != nil {
		return nil, mapAgentQueuedInputError(err, fields...)
	}
	channel, err := h.requireChannelMember(ctx, queued.ChannelID, common_service_models.UserIdType(actorID))
	if err != nil {
		return nil, err
	}
	if queued.SenderID != actorID {
		return nil, serviceErrors.ErrInsufficientPermissions
	}
	limits := h.policy()
	if limits.ChatMaxMessageLength() > 0 && len(input.Content) > limits.ChatMaxMessageLength() {
		return nil, serviceErrors.ErrInvalidRequest
	}
	if input.Content == "" && len(input.Attachments) == 0 && len(queued.Embeds) == 0 {
		return nil, serviceErrors.ErrInvalidRequest
	}
	// A send bounds its embeds and attachments together; the queued input's embeds are kept.
	if limits.ChatMaxEmbedsPerMessage() > 0 && len(queued.Embeds)+len(input.Attachments) > limits.ChatMaxEmbedsPerMessage() {
		return nil, serviceErrors.ErrInvalidRequest
	}
	if len(input.Attachments) > 0 && (!h.SupportsAttachments() || !channel.GetSupportAttachments()) {
		return nil, serviceErrors.ErrChatFeatureDisabled
	}
	mentionUserIDs, err := h.validateMessageMentions(ctx, queued.ChannelID, input.Content, queued.ContentType)
	if err != nil {
		return nil, err
	}
	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation:              ports.ChatPermissionEditAgentQueuedInput,
		Actor:                  actor,
		Channel:                channel,
		ParentMessageID:        queued.ParentMessageID,
		AgentSessionID:         queued.AgentSessionID,
		AgentQueuedInputID:     queued.ID,
		ProposedMessageContent: input.Content,
		ProposedEmbedCount:     len(queued.Embeds),
		ProposedMentionUserIDs: mentionUserIDs,
		ProposedMentionCount:   len(mentionUserIDs),
	}); err != nil {
		return nil, err
	}
	var attachmentIDs []uint64
	var uploads []*proto_models.AttachmentUploadInfo
	if len(input.Attachments) > 0 {
		// The kept attachments are planned from the input as it stands under its admission locks,
		// held until commit, so a concurrent edit cannot abandon an upload this edit keeps.
		var current *common_persistence_models.ChatAgentQueuedInput
		if current, err = h.persistenceHandler.LockChatAgentQueuedInput(ctx, queued.ID); err != nil {
			return nil, mapAgentQueuedInputError(err, fields...)
		}
		if attachmentIDs, uploads, err = h.replaceAgentQueuedInputAttachments(ctx, current, input.Attachments); err != nil {
			return nil, err
		}
	}
	edited, err := h.persistenceHandler.EditChatAgentQueuedInput(ctx, queued.ID, persistence.ChatAgentQueuedInputContent{
		Content:        input.Content,
		ContentType:    queued.ContentType,
		MentionUserIDs: mentionUserIDs,
		AttachmentIDs:  attachmentIDs,
	})
	if err != nil {
		return nil, mapAgentQueuedInputError(err, fields...)
	}
	h.PublishAgentSessionInteractionAfterCommit(ctx, queued.AgentSessionID, queued.ChannelID, queued.ParentMessageID)
	edited.Sender = queued.Sender
	views, err := h.agentQueuedInputViews(ctx, senderQueuedInputViewer(actor, channel), edited)
	if err != nil {
		return nil, err
	}
	return &EditAgentQueuedInputResult{QueuedInput: views[0], NewAttachments: uploads}, nil
}

// replaceAgentQueuedInputAttachments plans the attachment list an edit asks for: it keeps the
// queued input's attachments the request lists again (matched by the content hash the queue
// reports, which for a pending upload is the one its sender declared), creates the
// sender's uploads for the new ones, owned by the queued input, and returns the list in request
// order with the new uploads' instructions.
func (h *Handler[U]) replaceAgentQueuedInputAttachments(
	ctx context.Context,
	queued *common_persistence_models.ChatAgentQueuedInput,
	requested []*proto_models.AttachmentMetadata,
) ([]uint64, []*proto_models.AttachmentUploadInfo, error) {
	coordinator, err := h.requireAttachmentCoordinator()
	if err != nil {
		return nil, nil, err
	}
	existing := common_util.ConvertPqInt64ArrayToUint64Slice(queued.AttachmentIDs)
	newAttachmentIDs, _, uploads, err := coordinator.PlanChatAttachmentReplacement(ctx, queued.SenderID, queuedInputAttachmentOwner(queued.ID), requested, existing)
	if err != nil {
		return nil, nil, err
	}
	attachmentIDs, err := coordinator.BuildOrderedChatAttachmentIDs(ctx, requested, existing, newAttachmentIDs)
	if err != nil {
		return nil, nil, err
	}
	return attachmentIDs, uploads, nil
}

// RemoveAgentQueuedInput withdraws queued input before the agent receives it. Its sender and the
// channel's admins may remove it while they take part in the channel and it is not archived, and
// the application's Authorize callback may veto it (ChatPermissionRemoveAgentQueuedInput). Its
// pending uploads are abandoned; stored objects are kept. The removal serializes with delivery:
// once the agent received the input it fails with ErrChatAgentQueuedInputNotQueued. Requires a
// transaction.
func (h *Handler[U]) RemoveAgentQueuedInput(ctx context.Context, queuedInputID uint64) error {
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
	fields := []zap.Field{
		zap.String(logkeys.Stage, "remove_agent_queued_input"),
		zap.Uint64(appLogger.KeyQueuedInputID, queuedInputID),
		zap.Uint64(appLogger.KeyUserID, actorID),
	}
	queued, err := h.persistenceHandler.GetChatAgentQueuedInput(ctx, queuedInputID)
	if err != nil {
		return mapAgentQueuedInputError(err, fields...)
	}
	var channel *common_service_models.ChatChannel
	if queued.SenderID == actorID {
		channel, err = h.requireChannelMember(ctx, queued.ChannelID, common_service_models.UserIdType(actorID))
	} else {
		channel, err = h.requireChannelAdmin(ctx, queued.ChannelID, common_service_models.UserIdType(actorID), false)
	}
	if err != nil {
		return err
	}
	if err := h.authorizeAgentQueuedInput(ctx, actor, channel, ports.ChatPermissionRemoveAgentQueuedInput, queued); err != nil {
		return err
	}
	if _, err := h.persistenceHandler.RemoveChatAgentQueuedInput(ctx, queued.ID); err != nil {
		return mapAgentQueuedInputError(err, fields...)
	}
	h.PublishAgentSessionInteractionAfterCommit(ctx, queued.AgentSessionID, queued.ChannelID, queued.ParentMessageID)
	return nil
}

// agentQueuedInputViewer is who reads queued input in channel, which decides what they may do
// with it. Only a viewer who takes part in the channel, while it is not archived (canChange),
// changes queued input: they edit and remove their own, and a channel admin (isAdmin, which
// implies canChange) removes anyone's, each unless the application's Authorize callback denies it.
type agentQueuedInputViewer[U common_service_models.UserLike] struct {
	actor     U
	channel   *common_service_models.ChatChannel
	canChange bool
	isAdmin   bool
}

// senderQueuedInputViewer is the sender of input a send queued or an edit changed in channel,
// which that send or edit found them taking part in.
func senderQueuedInputViewer[U common_service_models.UserLike](actor U, channel *common_service_models.ChatChannel) agentQueuedInputViewer[U] {
	return agentQueuedInputViewer[U]{actor: actor, channel: channel, canChange: true}
}

// agentQueuedInputViews converts queued input rows, read with their senders, for viewer, with their
// attachments hydrated. Each flag is decided as its operation decides, the application's Authorize
// callback included (asked with no edit or removal following, and no proposed content).
func (h *Handler[U]) agentQueuedInputViews(ctx context.Context, viewer agentQueuedInputViewer[U], rows ...*common_persistence_models.ChatAgentQueuedInput) ([]*common_service_models.ChatAgentQueuedInput, error) {
	viewerID := uint64(viewer.actor.GetID())
	views := make([]*common_service_models.ChatAgentQueuedInput, len(rows))
	for i, row := range rows {
		view := h.converter.PersistenceChatAgentQueuedInputToService(row)
		own := viewer.canChange && row.SenderID == viewerID
		view.ViewerCanEdit = own &&
			h.authorizeAgentQueuedInput(ctx, viewer.actor, viewer.channel, ports.ChatPermissionEditAgentQueuedInput, row) == nil
		view.ViewerCanRemove = (own || viewer.isAdmin) &&
			h.authorizeAgentQueuedInput(ctx, viewer.actor, viewer.channel, ports.ChatPermissionRemoveAgentQueuedInput, row) == nil
		views[i] = view
	}
	if err := h.hydrateQueuedInputsAttachments(ctx, views...); err != nil {
		return nil, err
	}
	return views, nil
}

// authorizeAgentQueuedInput asks the application's Authorize callback whether actor may edit or
// remove (operation) queued in channel, with no proposed content: for a removal, and for the
// viewer flags of both.
func (h *Handler[U]) authorizeAgentQueuedInput(
	ctx context.Context,
	actor U,
	channel *common_service_models.ChatChannel,
	operation ports.ChatPermissionOperation,
	queued *common_persistence_models.ChatAgentQueuedInput,
) error {
	request := ports.ChatPermissionRequest[U]{
		Operation:          operation,
		Actor:              actor,
		Channel:            channel,
		ParentMessageID:    queued.ParentMessageID,
		AgentSessionID:     queued.AgentSessionID,
		AgentQueuedInputID: queued.ID,
	}
	if operation == ports.ChatPermissionRemoveAgentQueuedInput {
		senderID := common_service_models.UserIdType(queued.SenderID)
		request.TargetUserID = &senderID
	}
	return h.authorize(ctx, request)
}

// hydrateQueuedInputsAttachments fills the attachments of queued inputs from their attachment IDs,
// as hydrateMessagesAttachments does for messages; nil inputs are skipped.
func (h *Handler[U]) hydrateQueuedInputsAttachments(ctx context.Context, inputs ...*common_service_models.ChatAgentQueuedInput) error {
	withAttachments := make([]*common_service_models.ChatAgentQueuedInput, 0, len(inputs))
	for _, input := range inputs {
		if input != nil && len(input.AttachmentIDs) > 0 {
			withAttachments = append(withAttachments, input)
		}
	}
	operationalConfig := h.policy()
	if len(withAttachments) == 0 || h.attachmentHydrator == nil || !operationalConfig.ChatSupportAttachments() {
		return nil
	}
	if err := h.attachmentHydrator.HydrateChatAgentQueuedInputsAttachments(ctx, withAttachments); err != nil {
		if errors.Is(err, persistence.ErrDatabaseError) {
			return serviceErrors.ErrDatabaseError
		}
		return serviceErrors.ErrInternalError
	}
	return nil
}

// mapAgentQueuedInputError maps a persistence error of a queued-input operation: input that left
// the queue is ErrChatAgentQueuedInputNotQueued, a channel archived since the access check
// ErrChatChannelArchived, anything else a database error.
func mapAgentQueuedInputError(err error, fields ...zap.Field) error {
	switch {
	case errors.Is(err, persistence.ErrChatAgentQueuedInputNotQueued):
		return serviceErrors.ErrChatAgentQueuedInputNotQueued
	case errors.Is(err, persistence.ErrChatChannelArchived):
		return serviceErrors.ErrChatChannelArchived
	}
	return appLogger.WithErrorCause(serviceErrors.ErrDatabaseError, err, fields...)
}
