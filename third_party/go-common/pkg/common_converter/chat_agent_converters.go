package common_converter

import (
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// PersistenceChatAgentSessionToService maps a persistence ChatAgentSession to its service-layer
// view. It returns nil for a nil input.
//
// Parameters:
//   - pSession: the persistence agent-session row to convert.
//
// Returns:
//   - result: the service-layer ChatAgentSession, or nil.
func (h *ConverterHandler[U]) PersistenceChatAgentSessionToService(
	pSession *common_persistence_models.ChatAgentSession,
) *common_service_models.ChatAgentSession {
	if pSession == nil {
		return nil
	}
	return &common_service_models.ChatAgentSession{
		InferenceOperation:                 pSession.InferenceOperation,
		ExecutionInferenceModelID:          ownedPtr(pSession.ExecutionInferenceModelID),
		ExecutionInferenceAPI:              ownedPtr(pSession.ExecutionInferenceAPI),
		ExecutionProviderID:                ownedPtr(pSession.ExecutionProviderID),
		ExecutionProviderModelID:           ownedPtr(pSession.ExecutionProviderModelID),
		SelectionSource:                    pSession.SelectionSource,
		SelectingUserID:                    ownedPtr(pSession.SelectingUserID),
		SiteOverrideUserID:                 ownedPtr(pSession.SiteOverrideUserID),
		MemberChoiceSelection:              pSession.MemberChoiceSelection,
		OperationListVersion:               pSession.OperationListVersion,
		OrganizationPolicyVersion:          pSession.OrganizationPolicyVersion,
		OrganizationOperationPolicyVersion: pSession.OrganizationOperationPolicyVersion,
		InferencePinVersion:                pSession.InferencePinVersion,
		InferenceBlockedReason:             pSession.InferenceBlockedReason,
		InferenceBlockedAt:                 ownedPtr(pSession.InferenceBlockedAt),

		ID:                   pSession.ID,
		OrganizationID:       common_service_models.OrganizationIdType(pSession.OrganizationID),
		ChannelID:            pSession.ChannelID,
		ParentMessageID:      pSession.ParentMessageID,
		AgentType:            pSession.AgentType,
		BotUserID:            pSession.BotUserID,
		UserID:               pSession.UserID,
		CreatedByUserID:      pSession.CreatedByUserID,
		InferenceActorUserID: cloneUint64Ptr(pSession.InferenceActorUserID),
		ActiveConfigVersion:  pSession.ActiveConfigVersion,
		Status:               common_service_models.ChatAgentSessionStatus(pSession.Status),
		AutoRunPolicy:        common_service_models.ChatAgentSessionAutoRunPolicy(pSession.AutoRunPolicy),
		ContextID:            pSession.ContextID,
		ContextEntityID:      pSession.ContextEntityID,
		Metadata:             decodeAgentSessionMetadata(pSession.MetadataJSON),
		ParentAgentSessionID: cloneUint64Ptr(pSession.ParentAgentSessionID),
		SpawnedByAgentRunID:  cloneUint64Ptr(pSession.SpawnedByAgentRunID),
		CreatedAt:            pSession.CreatedAt,
		UpdatedAt:            pSession.UpdatedAt,
	}
}

// cloneUint64Ptr copies an optional id so the service view never aliases the persistence row.
func cloneUint64Ptr(v *uint64) *uint64 {
	if v == nil {
		return nil
	}
	out := *v
	return &out
}

// ServiceChatAgentSessionStatusToProto maps the service session status to its wire enum. Sessions
// never store the zero value, so it and unknown values map to UNSPECIFIED, which is not live.
func (h *ConverterHandler[U]) ServiceChatAgentSessionStatusToProto(
	status common_service_models.ChatAgentSessionStatus,
) proto_models.ChatAgentSessionStatus {
	switch common_persistence_models.ChatAgentSessionStatus(status) {
	case common_persistence_models.ChatAgentSessionStatusActive:
		return proto_models.ChatAgentSessionStatus_CHAT_AGENT_SESSION_STATUS_ACTIVE
	case common_persistence_models.ChatAgentSessionStatusArchived:
		return proto_models.ChatAgentSessionStatus_CHAT_AGENT_SESSION_STATUS_ARCHIVED
	default:
		return proto_models.ChatAgentSessionStatus_CHAT_AGENT_SESSION_STATUS_UNSPECIFIED
	}
}

// ServiceChatAgentSessionToSummaryProto maps a service session to its read-only wire summary. The
// caller resolves displayName from the agent registry; an empty value falls back to the agent
// type so a session whose type is no longer registered still has a label. It returns nil for a
// nil input.
func (h *ConverterHandler[U]) ServiceChatAgentSessionToSummaryProto(
	session *common_service_models.ChatAgentSession,
	displayName string,
) *proto_models.AgentSessionSummary {
	if session == nil {
		return nil
	}
	if displayName == "" {
		displayName = session.AgentType
	}
	return &proto_models.AgentSessionSummary{
		InferenceOperation:        session.InferenceOperation,
		ExecutionInferenceModelId: ownedPtr(session.ExecutionInferenceModelID),
		ExecutionInferenceApi:     ownedPtr(session.ExecutionInferenceAPI),
		ExecutionProviderId:       ownedPtr(session.ExecutionProviderID),
		ExecutionProviderModelId:  ownedPtr(session.ExecutionProviderModelID),
		SelectionSource:           session.SelectionSource,
		InferencePinVersion:       session.InferencePinVersion,
		InferenceBlockedReason:    session.InferenceBlockedReason,
		InferenceBlockedAt:        timestampPtrOrNil(session.InferenceBlockedAt),

		AgentSessionId:       session.ID,
		OrganizationId:       uint64(session.OrganizationID),
		ChannelId:            session.ChannelID,
		ParentMessageId:      session.ParentMessageID,
		AgentType:            session.AgentType,
		AgentDisplayName:     displayName,
		BotUserId:            session.BotUserID,
		Status:               h.ServiceChatAgentSessionStatusToProto(session.Status),
		ParentAgentSessionId: cloneUint64Ptr(session.ParentAgentSessionID),
		SpawnedByAgentRunId:  cloneUint64Ptr(session.SpawnedByAgentRunID),
		CreatedAt:            timestamppb.New(session.CreatedAt),
		UpdatedAt:            timestamppb.New(session.UpdatedAt),
	}
}

func decodeAgentSessionMetadata(raw []byte) map[string]string {
	metadata, _ := common_service_models.DecodeAgentSessionMetadata(raw)
	return metadata
}

// PersistenceChatAgentRunToService maps a persistence ChatAgentRun to its service-layer view. It
// returns nil for a nil input. Persistence-only Genkit correlation IDs and MetadataJSON are
// intentionally omitted: they are operational/debugging data, not part of the service-layer run
// contract.
//
// Parameters:
//   - pRun: the persistence agent-run row to convert.
//
// Returns:
//   - result: the service-layer ChatAgentRun, or nil.
func (h *ConverterHandler[U]) PersistenceChatAgentRunToService(
	pRun *common_persistence_models.ChatAgentRun,
) *common_service_models.ChatAgentRun {
	if pRun == nil {
		return nil
	}
	return &common_service_models.ChatAgentRun{
		RequestedInferenceModelID: pRun.RequestedInferenceModelID,
		RequestedInferenceAPI:     pRun.RequestedInferenceAPI,
		ExecutionInferenceModelID: pRun.ExecutionInferenceModelID,
		ExecutionInferenceAPI:     pRun.ExecutionInferenceAPI,
		ExecutionProviderID:       pRun.ExecutionProviderID,
		ExecutionProviderModelID:  pRun.ExecutionProviderModelID,

		ID:                        pRun.ID,
		OrganizationID:            common_service_models.OrganizationIdType(pRun.OrganizationID),
		ChannelID:                 pRun.ChannelID,
		ParentMessageID:           pRun.ParentMessageID,
		AgentSessionID:            pRun.AgentSessionID,
		AgentSessionConfigVersion: pRun.AgentSessionConfigVersion,
		BotUserID:                 pRun.BotUserID,
		TriggeringUserID:          pRun.TriggeringUserID,
		TriggeringMessageID:       pRun.TriggeringMessageID,
		Status:                    pRun.Status,
		StatusReason:              pRun.StatusReason,
		StatusReasonCode:          common_util.Deref(pRun.StatusReasonCode),
		ExternalRunID:             pRun.ExternalRunID,
		MaxActiveDurationMs:       pRun.MaxActiveDurationMs,
		MaxGenerations:            pRun.MaxGenerations,
		MaxToolCalls:              pRun.MaxToolCalls,
		ActiveDurationMs:          pRun.ActiveDurationMs,
		GenerationCount:           pRun.GenerationCount,
		ToolCallCount:             pRun.ToolCallCount,
		StartedAt:                 pRun.StartedAt,
		CompletedAt:               pRun.CompletedAt,
		CreatedAt:                 pRun.CreatedAt,
		UpdatedAt:                 pRun.UpdatedAt,
	}
}

// PersistenceChatAgentQueuedInputToService maps a queued agent input row to its service-layer view,
// with its sender when the row was read with it. The viewer flags and hydrated attachments are left
// for the service, which decides them for the reader. It returns nil for a nil input.
func (h *ConverterHandler[U]) PersistenceChatAgentQueuedInputToService(
	pQueued *common_persistence_models.ChatAgentQueuedInput,
) *common_service_models.ChatAgentQueuedInput {
	if pQueued == nil {
		return nil
	}
	var sender *common_service_models.User
	if pQueued.Sender != nil && pQueued.Sender.ID != 0 {
		sender = h.ConvertPersistenceToUser(pQueued.Sender)
	}
	return &common_service_models.ChatAgentQueuedInput{
		ID:               pQueued.ID,
		AgentSessionID:   pQueued.AgentSessionID,
		ChannelID:        pQueued.ChannelID,
		ParentMessageID:  pQueued.ParentMessageID,
		SenderID:         common_service_models.UserIdType(pQueued.SenderID),
		Sender:           sender,
		Content:          pQueued.Content,
		ContentType:      pQueued.ContentType,
		ReplyToMessageID: cloneUint64Ptr(pQueued.ReplyToMessageID),
		AttachmentIDs:    common_util.ConvertPqInt64ArrayToUint64Slice(pQueued.AttachmentIDs),
		EmbedCount:       len(pQueued.Embeds),
		CreatedAt:        pQueued.CreatedAt,
		UpdatedAt:        pQueued.UpdatedAt,
	}
}

// ServiceChatAgentQueuedInputToProto maps a queued agent input to its wire form; the sender is
// projected like a message sender's. It returns nil for a nil input.
func (h *ConverterHandler[U]) ServiceChatAgentQueuedInputToProto(
	queued *common_service_models.ChatAgentQueuedInput,
) *proto_models.AgentQueuedInput {
	if queued == nil {
		return nil
	}
	var sender *proto_models.PublicUser
	if queued.Sender != nil {
		sender = h.ConvertUserLikeToPublicProto(queued.Sender)
	}
	return &proto_models.AgentQueuedInput{
		Id:               queued.ID,
		AgentSessionId:   queued.AgentSessionID,
		ChannelId:        queued.ChannelID,
		ParentMessageId:  queued.ParentMessageID,
		SenderId:         uint64(queued.SenderID),
		Sender:           sender,
		Content:          queued.Content,
		ContentType:      queued.ContentType,
		ReplyToMessageId: common_util.Deref(queued.ReplyToMessageID),
		AttachmentIds:    queued.AttachmentIDs,
		Attachments:      queued.Attachments,
		EmbedCount:       uint32(queued.EmbedCount),
		CreatedAt:        timestampOrNil(queued.CreatedAt),
		UpdatedAt:        timestampOrNil(queued.UpdatedAt),
		ViewerCanEdit:    queued.ViewerCanEdit,
		ViewerCanRemove:  queued.ViewerCanRemove,
	}
}
