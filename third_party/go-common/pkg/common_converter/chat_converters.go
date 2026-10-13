package common_converter

import (
	"context"
	json "encoding/json/v2"
	"slices"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	proto_models "github.com/Suhaibinator/go-common/pkg/proto_models"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// timestampOrNil executes this function as part of the backend conversion flow between persistence, service, and transport models.
func timestampOrNil(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// timestampPtrOrNil executes this function as part of the backend conversion flow between persistence, service, and transport models.
func timestampPtrOrNil(t *time.Time) *timestamppb.Timestamp {
	if t == nil || t.IsZero() {
		return nil
	}
	return timestamppb.New(*t)
}

// ownedPtr returns a pointer to a copy of *p, or nil. The chat service projections built below
// own every pointer and byte slice they expose: the persistence row they come from may be shared
// (the channel access cache serves one row to every request, a notification batch serves one row
// to every recipient), while the projection reaches application permission callbacks and event
// sinks that may write through it. Copying here, once per conversion, keeps such a write from
// changing what a later request, recipient, or authorization decision reads.
func ownedPtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// PersistenceChatChannelToService projects a persistence channel row onto the service model. The
// result shares no memory with pChannel (see ownedPtr).
//
// Parameters:
//   - pChannel: Chat channel input used to scope this operation.
func (h *ConverterHandler[U]) PersistenceChatChannelToService(pChannel *common_persistence_models.ChatChannel) *common_service_models.ChatChannel {
	if pChannel == nil {
		return nil
	}

	var avatarAttachmentID *uint64
	avatarPath := ""
	if pChannel.AvatarAttachmentID != nil &&
		pChannel.AvatarAttachment != nil &&
		pChannel.AvatarAttachment.ID == *pChannel.AvatarAttachmentID &&
		pChannel.AvatarAttachment.IsValidPublicAvatar() {
		avatarAttachmentID = ownedPtr(pChannel.AvatarAttachmentID)
		avatarPath = pChannel.AvatarAttachment.FinalObjectName
	}

	var deletedAt *time.Time
	if pChannel.DeletedAt.Valid {
		t := pChannel.DeletedAt.Time
		deletedAt = &t
	}

	// The threshold is only meaningful with an organization to evaluate it in, so a channel
	// without one maps to nil regardless of the stored OrgMinRoleRead.
	var organizationAccess *common_service_models.ChatChannelOrganizationAccess
	if pChannel.OrganizationID != nil {
		organizationAccess = &common_service_models.ChatChannelOrganizationAccess{
			OrganizationID: common_service_models.OrganizationIdType(*pChannel.OrganizationID),
			MinRoleRead:    pChannel.OrgMinRoleRead,
		}
	}

	return &common_service_models.ChatChannel{
		ID:                         pChannel.ID,
		Name:                       pChannel.Name,
		Description:                ownedPtr(pChannel.Description),
		ContextID:                  ownedPtr(pChannel.ContextID),
		ContextDisplayName:         ownedPtr(pChannel.ContextDisplayName),
		ContextEntityID:            ownedPtr(pChannel.ContextEntityID),
		AvatarAttachmentID:         avatarAttachmentID,
		AvatarUrl:                  avatarPath,
		ChannelType:                pChannel.ChannelType,
		IsArchived:                 pChannel.IsArchived,
		CreatedByUserID:            pChannel.CreatedByUserID,
		LastMessageAt:              ownedPtr(pChannel.LastMessageAt),
		LastMessagePreview:         ownedPtr(pChannel.LastMessagePreview),
		CreatedAt:                  pChannel.CreatedAt,
		UpdatedAt:                  pChannel.UpdatedAt,
		DeletedAt:                  deletedAt,
		UnreadNotificationsEnabled: pChannel.UnreadNotificationsEnabled,
		Features:                   pChannel.Features,
		ForkRules:                  pChannel.ForkRules,
		ForkedFromChannelID:        ownedPtr(pChannel.ForkedFromChannelID),
		ForkPointMessageID:         ownedPtr(pChannel.ForkPointMessageID),
		OrganizationAccess:         organizationAccess,
		PublicRead:                 pChannel.PublicRead,
	}
}

// ServiceChatChannelToProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ServiceChatChannelToProto(channel common_service_models.ChatChannelLike) *proto_models.Channel {
	if channel == nil {
		return nil
	}

	avatarURL := common_service_models.ComposePublicAttachmentURL(channel.GetAvatarUrl(), h.publicBaseURL(), h.publicBucket)
	rules := channel.GetForkRules()

	description := ""
	if channel.GetDescription() != nil {
		description = *channel.GetDescription()
	}

	avatarAttachmentID := uint64(0)
	if channel.GetAvatarAttachmentID() != nil {
		avatarAttachmentID = *channel.GetAvatarAttachmentID()
	}

	lastMessagePreview := ""
	if channel.GetLastMessagePreview() != nil {
		lastMessagePreview = *channel.GetLastMessagePreview()
	}

	return &proto_models.Channel{
		Id:                            channel.GetID(),
		Name:                          channel.GetName(),
		Description:                   description,
		AvatarAttachmentId:            avatarAttachmentID,
		AvatarUrl:                     avatarURL,
		ChannelType:                   channel.GetChannelType(),
		IsArchived:                    channel.GetIsArchived(),
		CreatedByUserId:               channel.GetCreatedByUserID(),
		LastMessageAt:                 timestampPtrOrNil(channel.GetLastMessageAt()),
		LastMessagePreview:            lastMessagePreview,
		CreatedAt:                     timestampOrNil(channel.GetCreatedAt()),
		UpdatedAt:                     timestampOrNil(channel.GetUpdatedAt()),
		DeletedAt:                     timestampPtrOrNil(channel.GetDeletedAt()),
		UnreadNotificationsEnabled:    channel.GetUnreadNotificationsEnabled(),
		SupportReplies:                channel.GetSupportReplies(),
		SupportThreads:                channel.GetSupportThreads(),
		SupportMessageEdits:           channel.GetSupportMessageEdits(),
		SupportMessageDeletes:         channel.GetSupportMessageDeletes(),
		SupportMessageReactions:       channel.GetSupportMessageReactions(),
		AllowMultipleReactionsPerUser: channel.GetAllowMultipleReactionsPerUser(),
		SupportEmbeds:                 channel.GetSupportEmbeds(),
		SupportAttachments:            channel.GetSupportAttachments(),
		AllowNativeEmbeds:             channel.GetAllowNativeEmbeds(),
		AllowCustomEmbeds:             channel.GetAllowCustomEmbeds(),
		SupportInferenceUsage:         channel.GetSupportInferenceUsage(),
		ContextId:                     channel.GetContextID(),
		ContextDisplayName:            channel.GetContextDisplayName(),
		ContextEntityId:               channel.GetContextEntityID(),
		PublicRead:                    channel.GetPublicRead(),
		ForkRules:                     &proto_models.ChannelForkRules{WhoMayFork: rules.WhoMayFork, MemberPool: rules.MemberPool, OrgAccess: rules.OrgAccess, Context: rules.Context},
		ForkedFromChannelId:           channel.GetForkedFromChannelID(),
		ForkPointMessageId:            channel.GetForkPointMessageID(),
	}
}

// ChannelForkRulesFromProto maps wire fork rules onto the stored shape; nil disables forking.
// Undefined enum values pass through for the service to reject.
func ChannelForkRulesFromProto(rules *proto_models.ChannelForkRules) common_persistence_models.ChatChannelForkRules {
	return common_persistence_models.ChatChannelForkRules{
		WhoMayFork: rules.GetWhoMayFork(),
		MemberPool: rules.GetMemberPool(),
		OrgAccess:  rules.GetOrgAccess(),
		Context:    rules.GetContext(),
	}
}

// PersistenceChatChannelParticipantToService executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) PersistenceChatChannelParticipantToService(
	pParticipant *common_persistence_models.ChatChannelParticipant,
) *common_service_models.ChatChannelParticipant {
	if pParticipant == nil {
		return nil
	}

	var user *common_service_models.User
	if pParticipant.User.ID != 0 {
		user = h.ConvertPersistenceToUser(&pParticipant.User)
	}

	return &common_service_models.ChatChannelParticipant{
		ChannelID:         pParticipant.ChannelID,
		UserID:            pParticipant.UserID,
		Role:              pParticipant.Role,
		NotificationMode:  pParticipant.NotificationMode,
		IsPinned:          pParticipant.IsPinned,
		LastReadMessageID: ownedPtr(pParticipant.LastReadMessageID),
		JoinedAt:          pParticipant.JoinedAt,
		UpdatedAt:         pParticipant.UpdatedAt,
		LeftAt:            ownedPtr(pParticipant.LeftAt),
		User:              user,
	}
}

// ServiceChatChannelParticipantToProto converts a participant for viewerID.
// Personal preferences (notification mode, pin) are serialized only when
// viewerID is the participant; pass 0 when no viewer owns the row.
func (h *ConverterHandler[U]) ServiceChatChannelParticipantToProto(
	participant common_service_models.ChatChannelParticipantLike,
	viewerID common_service_models.UserIdType,
) *proto_models.ChannelParticipant {
	if participant == nil {
		return nil
	}
	identifiers := h.publicIdentifierTypes()

	lastReadMessageID := uint64(0)
	if participant.GetLastReadMessageID() != nil {
		lastReadMessageID = *participant.GetLastReadMessageID()
	}

	out := &proto_models.ChannelParticipant{
		ChannelId:         participant.GetChannelID(),
		UserId:            participant.GetUserID(),
		Role:              participant.GetRole(),
		LastReadMessageId: lastReadMessageID,
		JoinedAt:          timestampOrNil(participant.GetJoinedAt()),
		UpdatedAt:         timestampOrNil(participant.GetUpdatedAt()),
		LeftAt:            timestampPtrOrNil(participant.GetLeftAt()),
		User:              h.convertUserLikeToPublicProtoWithIdentifiers(participant.GetUser(), identifiers),
	}
	// Personal preferences are visible only to their owner. Internal service
	// models stay intact for authorization and notification delivery.
	if viewerID != 0 && viewerID == participant.GetUserID() {
		out.NotificationMode = participant.GetNotificationMode()
		out.IsPinned = participant.GetIsPinned()
	}
	return out
}

// PersistenceChatMessageToService projects a persistence message row onto the service model. The
// result shares no memory with pMessage (see ownedPtr).
func (h *ConverterHandler[U]) PersistenceChatMessageToService(pMessage *common_persistence_models.ChatMessage) *common_service_models.ChatMessage {
	if pMessage == nil {
		return nil
	}

	var sender *common_service_models.User
	if pMessage.Sender.ID != 0 {
		sender = h.ConvertPersistenceToUser(&pMessage.Sender)
	}

	var deletedAt *time.Time
	if pMessage.DeletedAt.Valid {
		t := pMessage.DeletedAt.Time
		deletedAt = &t
	}

	return &common_service_models.ChatMessage{
		ID:                 pMessage.ID,
		ChannelID:          pMessage.ChannelID,
		SenderID:           pMessage.SenderID,
		Content:            pMessage.Content,
		ContentType:        pMessage.ContentType,
		StreamState:        pMessage.StreamState,
		ParentMessageID:    ownedPtr(pMessage.ParentMessageID),
		ReplyToMessageID:   ownedPtr(pMessage.ReplyToMessageID),
		BroadcastToChannel: pMessage.BroadcastToChannel,
		IsEdited:           pMessage.IsEdited,
		EditedAt:           ownedPtr(pMessage.EditedAt),
		OriginalContent:    ownedPtr(pMessage.OriginalContent),
		CreatedAt:          pMessage.CreatedAt,
		UpdatedAt:          pMessage.UpdatedAt,
		DeletedAt:          deletedAt,
		Sender:             sender,
		Embeds: common_util.ApplyToEach(pMessage.Embeds, func(pEmbed common_persistence_models.ChatMessageEmbed) *common_service_models.ChatMessageEmbed {
			return h.PersistenceChatMessageEmbedToService(&pEmbed)
		}),
		AttachmentIDs: common_util.ConvertPqInt64ArrayToUint64Slice(pMessage.AttachmentIDs),
		Reactions: common_util.ApplyToEach(pMessage.Reactions, func(pReaction common_persistence_models.ChatMessageReaction) *common_service_models.ChatMessageReaction {
			return h.PersistenceChatMessageReactionToService(&pReaction)
		}),
		Mentions: common_util.ApplyToEach(pMessage.Mentions, func(pMention common_persistence_models.ChatMessageMention) *common_service_models.ChatMessageMention {
			return h.PersistenceChatMessageMentionToService(&pMention)
		}),
		ReplyCount: pMessage.ReplyCount,

		// Agentic transcript fields: inline on the message row, plus the structured tool-call
		// payload/approval/execution reassembled from the 1:1 ChatMessageToolCall association.
		Kind:                 pMessage.Kind,
		Role:                 pMessage.Role,
		ExcludeFromAIContext: ownedPtr(pMessage.ExcludeFromAIContext),
		HiddenFromUsers:      ownedPtr(pMessage.HiddenFromUsers),
		AgentRunID:           ownedPtr(pMessage.AgentRunID),
		AgentStepIndex:       ownedPtr(pMessage.AgentStepIndex),
		AgentNoteSubtype:     pMessage.AgentNoteSubtype,
		ToolCall:             persistenceChatMessageToolCallToService(pMessage.ToolCall),
	}
}

// persistenceChatMessageToolCallToService reassembles the structured tool-call payload + approval +
// execution state from the persistence 1:1 ChatMessageToolCall side row. Returns nil when absent
// (STANDARD / non-tool-call messages).
func persistenceChatMessageToolCallToService(tc *common_persistence_models.ChatMessageToolCall) *common_service_models.ChatMessageToolCall {
	if tc == nil {
		return nil
	}
	return &common_service_models.ChatMessageToolCall{
		ToolCallID:              ownedPtr(tc.ToolCallID),
		ModelPayload:            slices.Clone(tc.ModelPayload),
		PayloadType:             tc.PayloadType,
		ApprovalStatus:          tc.ApprovalStatus,
		TriggeringUserID:        ownedPtr(tc.TriggeringUserID),
		ResolvedByUserID:        ownedPtr(tc.ResolvedByUserID),
		ResolvedAt:              ownedPtr(tc.ResolvedAt),
		ResolutionReason:        ownedPtr(tc.ResolutionReason),
		ApprovalExpiresAt:       ownedPtr(tc.ApprovalExpiresAt),
		ExecutionState:          tc.ExecutionState,
		ExecutionAttemptCount:   tc.ExecutionAttemptCount,
		ExecutionLeaseOwner:     ownedPtr(tc.ExecutionLeaseOwner),
		ExecutionLeaseExpiresAt: ownedPtr(tc.ExecutionLeaseExpiresAt),
		ExecutionStartedAt:      ownedPtr(tc.ExecutionStartedAt),
		ExecutedAt:              ownedPtr(tc.ExecutedAt),
		LastExecutionError:      ownedPtr(tc.LastExecutionError),
	}
}

// ServiceChatMessageToProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ServiceChatMessageToProto(ctx context.Context, message common_service_models.ChatMessageLike) *proto_models.Message {
	if common_util.IsNil(message) {
		return nil
	}
	return h.serviceChatMessageToProto(ctx, message, h.publicIdentifierTypes())
}

func (h *ConverterHandler[U]) serviceChatMessageToProto(
	ctx context.Context,
	message common_service_models.ChatMessageLike,
	identifiers publicIdentifierTypes,
) *proto_models.Message {
	parentMessageID := uint64(0)
	if message.GetParentMessageID() != nil {
		parentMessageID = *message.GetParentMessageID()
	}

	replyToMessageID := uint64(0)
	if message.GetReplyToMessageID() != nil {
		replyToMessageID = *message.GetReplyToMessageID()
	}

	embeds := make([]*proto_models.MessageEmbed, 0, len(message.GetEmbeds()))
	for _, e := range message.GetEmbeds() {
		if e == nil {
			continue
		}
		embeds = append(embeds, h.ServiceChatMessageEmbedToProto(e))
	}

	reactions := make([]*proto_models.MessageReaction, 0, len(message.GetReactions()))
	for _, r := range message.GetReactions() {
		if r == nil {
			continue
		}
		reactions = append(reactions, h.serviceChatMessageReactionToProto(ctx, r, identifiers))
	}

	mentions := make([]*proto_models.MessageMention, 0, len(message.GetMentions()))
	for _, m := range message.GetMentions() {
		if m == nil {
			continue
		}
		mentions = append(mentions, h.serviceChatMessageMentionToProto(ctx, m, identifiers))
	}

	out := &proto_models.Message{
		Id:              message.GetID(),
		ChannelId:       message.GetChannelID(),
		SenderId:        message.GetSenderID(),
		Content:         message.GetContent(),
		ContentType:     message.GetContentType(),
		StreamState:     message.GetStreamState(),
		ParentMessageId: parentMessageID,
		IsEdited:        message.GetIsEdited(),
		EditedAt:        timestampPtrOrNil(message.GetEditedAt()),
		OriginalContent: func() string {
			if message.GetOriginalContent() == nil {
				return ""
			}
			return *message.GetOriginalContent()
		}(),
		CreatedAt:          timestampOrNil(message.GetCreatedAt()),
		UpdatedAt:          timestampOrNil(message.GetUpdatedAt()),
		DeletedAt:          timestampPtrOrNil(message.GetDeletedAt()),
		Sender:             h.convertUserLikeToPublicProtoWithIdentifiers(message.GetSender(), identifiers),
		Embeds:             embeds,
		AttachmentIds:      message.GetAttachmentIDs(),
		Attachments:        message.GetAttachments(),
		Reactions:          reactions,
		Mentions:           mentions,
		ReplyToMessageId:   replyToMessageID,
		BroadcastToChannel: message.GetBroadcastToChannel(),
		ReplyCount:         message.GetReplyCount(),

		// Agentic transcript fields that live inline on the message row.
		Kind:                 message.GetKind(),
		Role:                 message.GetRole(),
		ExcludeFromAiContext: message.GetExcludeFromAIContext(),
		HiddenFromUsers:      message.GetHiddenFromUsers(),
		AgentRunId:           message.GetAgentRunID(),
		AgentStepIndex:       message.GetAgentStepIndex(),
		AgentNoteSubtype:     message.GetAgentNoteSubtype(),
	}

	// Flatten the structured tool-call payload + approval/execution state onto the proto Message.
	// The wire shape is flat (these fields live on Message), sourced from the nested ToolCall (which
	// mirrors the persistence 1:1 side row).
	applyToolCallToProtoMessage(out, message.GetToolCall())
	return out
}

// applyToolCallToProtoMessage flattens a service ToolCall onto the proto Message. It is a no-op for
// nil (STANDARD / non-tool-call rows leave the tool-call fields at their zero/absent values).
//
// Operational execution internals — the worker lease (owner/expiry/started-at), the attempt
// counter, and last_execution_error — are deliberately NOT copied: this converter feeds every
// user-facing read (ListMessages / BatchGetMessages / WS fanout, gated on channel membership only),
// and those columns expose worker identity and internal error text. Clients get the fields that
// drive UI state: approval fields, execution_state, and executed_at. The proto fields remain
// reserved for a future bot/admin-scoped read path.
func applyToolCallToProtoMessage(out *proto_models.Message, tc *common_service_models.ChatMessageToolCall) {
	if out == nil || tc == nil {
		return
	}
	out.ToolCallId = tc.ToolCallID
	out.ModelPayload = userVisibleModelPayload(tc)
	out.PayloadType = tc.PayloadType
	out.ApprovalStatus = tc.ApprovalStatus
	out.TriggeringUserId = common_util.Deref(tc.TriggeringUserID)
	out.ResolvedByUserId = common_util.Deref(tc.ResolvedByUserID)
	out.ResolvedAt = timestampPtrOrNil(tc.ResolvedAt)
	out.ResolutionReason = common_util.Deref(tc.ResolutionReason)
	out.ApprovalExpiresAt = timestampPtrOrNil(tc.ApprovalExpiresAt)
	out.ExecutionState = tc.ExecutionState
	out.ExecutedAt = timestampPtrOrNil(tc.ExecutedAt)
}

// userVisibleModelPayload removes model-only state before the transcript payload is copied to
// channel clients: opaque provider replay tokens on tool calls, and the verbatim protected context
// a compaction checkpoint carries. The persisted side row remains unchanged for model
// reconstruction.
func userVisibleModelPayload(tc *common_service_models.ChatMessageToolCall) []byte {
	if tc == nil || len(tc.ModelPayload) == 0 {
		return nil
	}
	var redact func(*proto_models.MessageModelPayload) bool
	switch tc.PayloadType {
	case proto_models.MessageModelPayloadType_MESSAGE_MODEL_PAYLOAD_TYPE_TOOL_CALL:
		redact = func(envelope *proto_models.MessageModelPayload) bool {
			call := envelope.GetToolCall()
			if call == nil {
				return false
			}
			call.ThoughtSignature = nil
			call.ProviderCallId = nil
			return true
		}
	case proto_models.MessageModelPayloadType_MESSAGE_MODEL_PAYLOAD_TYPE_COMPACTION:
		redact = func(envelope *proto_models.MessageModelPayload) bool {
			compaction := envelope.GetCompaction()
			if compaction == nil {
				return false
			}
			// A visible checkpoint must not republish a hidden workflow-state note, nor keep
			// serving a user message after it was edited or deleted. The ids stay; they name rows
			// the reader is already authorized (or not) to load.
			compaction.WorkflowState = ""
			compaction.RetainedUserContent = ""
			return true
		}
	default:
		return tc.ModelPayload
	}

	var envelope proto_models.MessageModelPayload
	if err := proto.Unmarshal(tc.ModelPayload, &envelope); err != nil || !redact(&envelope) {
		// Fail closed: a malformed payload must not expose bytes that may contain
		// provider-private continuation state or hidden context.
		return nil
	}
	redacted, err := proto.MarshalOptions{Deterministic: true}.Marshal(&envelope)
	if err != nil {
		return nil
	}
	return redacted
}

// chatMessageEditHistoryEmbedSnapshot defines the data structure used in the backend conversion flow between persistence, service, and transport models.
// It carries state across layer boundaries handled by this package.
type chatMessageEditHistoryEmbedSnapshot struct {
	EmbedType    uint32 `json:"embed_type"`
	EmbedData    []byte `json:"embed_data"`
	FallbackText string `json:"fallback_text"`
	Position     uint32 `json:"position"`
}

// PersistenceChatMessageEditHistoryToService executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) PersistenceChatMessageEditHistoryToService(
	pHistory *common_persistence_models.ChatMessageEditHistory,
) *common_service_models.ChatMessageEditHistoryEntry {
	if pHistory == nil {
		return nil
	}

	var editedByUser *common_service_models.User
	if pHistory.EditedByUser != nil && pHistory.EditedByUser.ID != 0 {
		editedByUser = h.ConvertPersistenceToUser(pHistory.EditedByUser)
	}

	var embedSnapshots []chatMessageEditHistoryEmbedSnapshot
	if len(pHistory.EmbedsJSON) > 0 {
		if err := json.Unmarshal(pHistory.EmbedsJSON, &embedSnapshots); err != nil {
			embedSnapshots = nil
		}
	}

	embeds := make([]*common_service_models.ChatMessageEmbed, 0, len(embedSnapshots))
	for _, snapshot := range embedSnapshots {
		embeds = append(embeds, &common_service_models.ChatMessageEmbed{
			MessageID:    pHistory.MessageID,
			EmbedType:    snapshot.EmbedType,
			EmbedData:    snapshot.EmbedData,
			FallbackText: snapshot.FallbackText,
			Position:     snapshot.Position,
		})
	}

	return &common_service_models.ChatMessageEditHistoryEntry{
		ID:               pHistory.ID,
		MessageID:        pHistory.MessageID,
		VersionNumber:    pHistory.VersionNumber,
		EditedByUserID:   pHistory.EditedByUserID,
		VersionStartedAt: pHistory.VersionStartedAt,
		VersionEndedAt:   pHistory.VersionEndedAt,
		Content:          pHistory.Content,
		ContentType:      pHistory.ContentType,
		Embeds:           embeds,
		AttachmentIDs:    common_util.ConvertPqInt64ArrayToUint64Slice(pHistory.AttachmentIDs),
		MentionUserIDs:   common_util.ConvertPqInt64ArrayToUint64Slice(pHistory.MentionUserIDs),
		CreatedAt:        pHistory.CreatedAt,
		EditedByUser:     editedByUser,
	}
}

// ServiceChatMessageEditHistoryToProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ServiceChatMessageEditHistoryToProto(
	ctx context.Context,
	entry common_service_models.ChatMessageEditHistoryEntryLike,
) *proto_models.ChatMessageEditHistoryEntry {
	if entry == nil {
		return nil
	}
	return h.serviceChatMessageEditHistoryToProto(ctx, entry, h.publicIdentifierTypes())
}

func (h *ConverterHandler[U]) serviceChatMessageEditHistoryToProto(
	ctx context.Context,
	entry common_service_models.ChatMessageEditHistoryEntryLike,
	identifiers publicIdentifierTypes,
) *proto_models.ChatMessageEditHistoryEntry {
	if entry == nil {
		return nil
	}

	embeds := make([]*proto_models.MessageEmbed, 0, len(entry.GetEmbeds()))
	for _, embed := range entry.GetEmbeds() {
		if embed == nil {
			continue
		}
		embeds = append(embeds, h.ServiceChatMessageEmbedToProto(embed))
	}

	return &proto_models.ChatMessageEditHistoryEntry{
		Id:               entry.GetID(),
		MessageId:        entry.GetMessageID(),
		VersionNumber:    entry.GetVersionNumber(),
		EditedByUserId:   entry.GetEditedByUserID(),
		VersionStartedAt: timestampOrNil(entry.GetVersionStartedAt()),
		VersionEndedAt:   timestampOrNil(entry.GetVersionEndedAt()),
		Content:          entry.GetContent(),
		ContentType:      entry.GetContentType(),
		Embeds:           embeds,
		AttachmentIds:    entry.GetAttachmentIDs(),
		MentionUserIds:   entry.GetMentionUserIDs(),
		CreatedAt:        timestampOrNil(entry.GetCreatedAt()),
		EditedByUser:     h.convertUserLikeToPublicProtoWithIdentifiers(entry.GetEditedByUser(), identifiers),
	}
}

// PersistenceChatMessageEmbedToService executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) PersistenceChatMessageEmbedToService(pEmbed *common_persistence_models.ChatMessageEmbed) *common_service_models.ChatMessageEmbed {
	if pEmbed == nil {
		return nil
	}
	return &common_service_models.ChatMessageEmbed{
		ID:           pEmbed.ID,
		MessageID:    pEmbed.MessageID,
		EmbedType:    pEmbed.EmbedType,
		EmbedData:    slices.Clone(pEmbed.EmbedData),
		FallbackText: pEmbed.FallbackText,
		Position:     pEmbed.Position,
		CreatedAt:    pEmbed.CreatedAt,
	}
}

// ServiceChatMessageEmbedToProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ServiceChatMessageEmbedToProto(embed common_service_models.ChatMessageEmbedLike) *proto_models.MessageEmbed {
	if embed == nil {
		return nil
	}
	return &proto_models.MessageEmbed{
		Id:           embed.GetID(),
		MessageId:    embed.GetMessageID(),
		EmbedType:    embed.GetEmbedType(),
		EmbedData:    embed.GetEmbedData(),
		FallbackText: embed.GetFallbackText(),
		Position:     embed.GetPosition(),
		CreatedAt:    timestampOrNil(embed.GetCreatedAt()),
	}
}

// PersistenceChatMessageReactionToService executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) PersistenceChatMessageReactionToService(pReaction *common_persistence_models.ChatMessageReaction) *common_service_models.ChatMessageReaction {
	if pReaction == nil {
		return nil
	}

	var user *common_service_models.User
	if pReaction.User.ID != 0 {
		user = h.ConvertPersistenceToUser(&pReaction.User)
	}

	return &common_service_models.ChatMessageReaction{
		MessageID: pReaction.MessageID,
		UserID:    pReaction.UserID,
		Emoji:     pReaction.Emoji,
		CreatedAt: pReaction.CreatedAt,
		User:      user,
	}
}

// ServiceChatMessageReactionToProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ServiceChatMessageReactionToProto(
	ctx context.Context,
	reaction common_service_models.ChatMessageReactionLike,
) *proto_models.MessageReaction {
	if reaction == nil {
		return nil
	}
	return h.serviceChatMessageReactionToProto(ctx, reaction, h.publicIdentifierTypes())
}

func (h *ConverterHandler[U]) serviceChatMessageReactionToProto(
	ctx context.Context,
	reaction common_service_models.ChatMessageReactionLike,
	identifiers publicIdentifierTypes,
) *proto_models.MessageReaction {
	if reaction == nil {
		return nil
	}
	user := reaction.GetUser()
	var protoUser *proto_models.PublicUser
	if user != nil {
		protoUser = h.convertUserLikeToPublicProtoWithIdentifiers(user, identifiers)
	}
	return &proto_models.MessageReaction{
		MessageId: reaction.GetMessageID(),
		UserId:    reaction.GetUserID(),
		Emoji:     reaction.GetEmoji(),
		CreatedAt: timestampOrNil(reaction.GetCreatedAt()),
		User:      protoUser,
	}
}

// PersistenceChatMessageMentionToService executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) PersistenceChatMessageMentionToService(pMention *common_persistence_models.ChatMessageMention) *common_service_models.ChatMessageMention {
	if pMention == nil {
		return nil
	}

	var user *common_service_models.User
	if pMention.User.ID != 0 {
		user = h.ConvertPersistenceToUser(&pMention.User)
	}

	return &common_service_models.ChatMessageMention{
		MessageID: pMention.MessageID,
		UserID:    pMention.UserID,
		CreatedAt: pMention.CreatedAt,
		User:      user,
	}
}

// ServiceChatMessageMentionToProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ServiceChatMessageMentionToProto(
	ctx context.Context,
	mention common_service_models.ChatMessageMentionLike,
) *proto_models.MessageMention {
	if mention == nil {
		return nil
	}
	return h.serviceChatMessageMentionToProto(ctx, mention, h.publicIdentifierTypes())
}

func (h *ConverterHandler[U]) serviceChatMessageMentionToProto(
	ctx context.Context,
	mention common_service_models.ChatMessageMentionLike,
	identifiers publicIdentifierTypes,
) *proto_models.MessageMention {
	if mention == nil {
		return nil
	}
	user := mention.GetUser()
	var protoUser *proto_models.PublicUser
	if user != nil {
		protoUser = h.convertUserLikeToPublicProtoWithIdentifiers(user, identifiers)
	}
	return &proto_models.MessageMention{
		MessageId: mention.GetMessageID(),
		UserId:    mention.GetUserID(),
		CreatedAt: timestampOrNil(mention.GetCreatedAt()),
		User:      protoUser,
	}
}
