package common_converter

import (
	"context"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/config"
	proto_models "github.com/Suhaibinator/go-common/pkg/proto_models"
)

// ConverterInterface defines the interface for entity conversion operations.
type ConverterInterface interface {
	// User conversions
	ConvertPersistenceToUser(pUser *common_persistence_models.User) *common_service_models.User
	ConvertUserToPersistence(user common_service_models.UserLike) *common_persistence_models.User
	ConvertUserLikeToProto(ctx context.Context, user common_service_models.UserLike) *proto_models.User
	ConvertUserLikeToPublicProto(user common_service_models.UserLike) *proto_models.PublicUser
	ConvertUserLikeToPublicProtoWithPermissions(user common_service_models.UserLike, permissions *config.UserAttributePermissions) *proto_models.PublicUser
	ConvertProtoToUser(user *proto_models.User) (*common_service_models.User, error)
	ConvertAdminUserDetailsToProto(ctx context.Context, domain *common_service_models.AdminUserDetails) *proto_models.AdminUserDetails
	PersistenceUsernameChangeHistoryToService(pEntry *common_persistence_models.UsernameChangeHistory) *common_service_models.AdminUsernameChangeHistoryEntry
	ServiceUsernameChangeHistoryToProto(ctx context.Context, domain *common_service_models.AdminUsernameChangeHistoryEntry) *proto_models.AdminUsernameChangeHistoryEntry
	PersistenceAdminAttachmentToService(pAttachment *common_persistence_models.Attachment) *common_service_models.AdminAttachmentDetails
	ServiceAdminAttachmentToProto(ctx context.Context, domain *common_service_models.AdminAttachmentDetails) *proto_models.AdminAttachmentDetails
	ServiceAdminChatChannelDetailsToProto(ctx context.Context, details *common_service_models.AdminChatChannelDetails) *proto_models.AdminChannelDetails
	ConvertAdminSessionTokenToProto(ctx context.Context, domain *common_service_models.AdminSessionToken) *proto_models.AdminSessionToken
	SearchPublicUsersParamsFromProto(req *proto_models.SearchPublicUsersRequest) *common_service_models.SearchPublicUsersParams
	SearchPublicUsersResultToProto(result *common_service_models.SearchPublicUsersResult) *proto_models.SearchPublicUsersResponse

	// Permission-based filtering conversions
	ConvertUserLikeToProtoWithPermissions(user common_service_models.UserLike, permissions *config.UserAttributePermissions) *proto_models.User
	FilterProtoUserWithPermissions(user *proto_models.User, permissions *config.UserAttributePermissions) *proto_models.User
	FilterOrganizationMemberDetailsWithPermissions(memberDetails *proto_models.OrganizationMemberDetails, permissions *config.UserAttributePermissions) *proto_models.OrganizationMemberDetails
	ConvertUserSettingsToProto(settings *common_service_models.UserSettings) *proto_models.UserSettings

	// Organization conversions
	PersistenceOrganizationToService(pOrg *common_persistence_models.Organization) *common_service_models.Organization
	ServiceOrganizationToPersistence(org common_service_models.OrganizationLike) *common_persistence_models.Organization
	ServiceOrganizationToProto(org common_service_models.OrganizationLike) *proto_models.Organization
	ProtoOrganizationToService(org *proto_models.Organization) (*common_service_models.Organization, error)
	PersistenceOrganizationEmailDomainToService(domain *common_persistence_models.OrganizationEmailDomain) *common_service_models.OrganizationEmailDomain
	ServiceOrganizationEmailDomainToProto(domain common_service_models.OrganizationEmailDomainLike) *proto_models.OrganizationEmailDomain

	// UserOrganization conversions
	PersistenceUserOrganizationToService(pUserOrg *common_persistence_models.UserOrganization) *common_service_models.UserOrganization
	PersistenceUserOrganizationToServiceMemberDetails(pUserOrg *common_persistence_models.UserOrganization) *common_service_models.OrganizationMemberDetails
	ServiceUserOrganizationToProto(userOrg common_service_models.UserOrganizationLike) *proto_models.OrganizationUser
	ServiceUserOrganizationResponseToOrganizationWithUserRole(userOrgResp *common_service_models.UserOrganizationResponse) *proto_models.OrganizationWithUserRole
	ServiceOrganizationMemberDetailsToProto(member *common_service_models.OrganizationMemberDetails) *proto_models.OrganizationMemberDetails
	ServiceOrganizationMemberDetailsToFilteredProto(member *common_service_models.OrganizationMemberDetails, permissions *config.UserAttributePermissions) *proto_models.OrganizationMemberDetails

	// OrganizationInvitation conversions
	PersistenceOrganizationInvitationToService(pInvite *common_persistence_models.OrganizationInvitation) *common_service_models.OrganizationInvitation
	ServiceOrganizationInvitationToProto(invite *common_service_models.OrganizationInvitation, org *common_service_models.Organization) *proto_models.OrganizationInvitation

	// UserAuthMethods conversions
	PersistenceUserAuthMethodsToService(pUserAuthMethods *common_persistence_models.UserAuthMethods) *common_service_models.UserAuthMethods

	// OAuthLogin conversions
	PersistenceOAuthLoginToService(pOAuthLogin *common_persistence_models.OAuthLogin) *common_service_models.OAuthLogin
	PersistenceOAuthLoginsToService(pOAuthLogins []common_persistence_models.OAuthLogin) []common_service_models.OAuthLogin

	// Chat conversions
	PersistenceChatAgentSessionToService(pSession *common_persistence_models.ChatAgentSession) *common_service_models.ChatAgentSession
	PersistenceChatAgentRunToService(pRun *common_persistence_models.ChatAgentRun) *common_service_models.ChatAgentRun
	ServiceChatAgentSessionStatusToProto(status common_service_models.ChatAgentSessionStatus) proto_models.ChatAgentSessionStatus
	ServiceChatAgentSessionToSummaryProto(session *common_service_models.ChatAgentSession, displayName string) *proto_models.AgentSessionSummary
	PersistenceChatAgentQueuedInputToService(pQueued *common_persistence_models.ChatAgentQueuedInput) *common_service_models.ChatAgentQueuedInput
	ServiceChatAgentQueuedInputToProto(queued *common_service_models.ChatAgentQueuedInput) *proto_models.AgentQueuedInput
	PersistenceChatInferenceTotalsToProto(channelID uint64, totals *common_persistence_models.ChatInferenceTotals) *proto_models.ChatInferenceTotals
	PersistenceMessageInferenceAggregateToProto(agg *persistence.MessageInferenceAggregate) *proto_models.MessageInferenceSummary
	PersistenceInferenceUsageBreakdownToProto(b *persistence.InferenceUsageBreakdown) *proto_models.AgentSessionUsageBreakdown
	PersistenceChatInferenceCallToProto(call *common_persistence_models.ChatInferenceCall) *proto_models.ChatInferenceCall
	PersistenceChatChannelToService(pChannel *common_persistence_models.ChatChannel) *common_service_models.ChatChannel
	ServiceChatChannelToProto(channel common_service_models.ChatChannelLike) *proto_models.Channel
	PersistenceChatChannelParticipantToService(pParticipant *common_persistence_models.ChatChannelParticipant) *common_service_models.ChatChannelParticipant
	ServiceChatChannelParticipantToProto(participant common_service_models.ChatChannelParticipantLike, viewerID common_service_models.UserIdType) *proto_models.ChannelParticipant
	PersistenceChatMessageToService(pMessage *common_persistence_models.ChatMessage) *common_service_models.ChatMessage
	ServiceChatMessageToProto(ctx context.Context, message common_service_models.ChatMessageLike) *proto_models.Message
	PersistenceChatMessageEditHistoryToService(pHistory *common_persistence_models.ChatMessageEditHistory) *common_service_models.ChatMessageEditHistoryEntry
	ServiceChatMessageEditHistoryToProto(ctx context.Context, entry common_service_models.ChatMessageEditHistoryEntryLike) *proto_models.ChatMessageEditHistoryEntry
	PersistenceChatMessageEmbedToService(pEmbed *common_persistence_models.ChatMessageEmbed) *common_service_models.ChatMessageEmbed
	ServiceChatMessageEmbedToProto(embed common_service_models.ChatMessageEmbedLike) *proto_models.MessageEmbed
	PersistenceChatMessageReactionToService(pReaction *common_persistence_models.ChatMessageReaction) *common_service_models.ChatMessageReaction
	ServiceChatMessageReactionToProto(ctx context.Context, reaction common_service_models.ChatMessageReactionLike) *proto_models.MessageReaction

	// Chat realtime event conversions.
	//
	// The result is chat's *payload* message, not a wire frame: the realtime publisher serializes it
	// into RealtimeEvent.payload and stamps the topic, event type, and timestamp on the envelope. The
	// converter therefore never sees or names a topic.
	ServiceChatEventToWebSocketProto(ctx context.Context, event *ports.ChatEvent) *proto_models.ChatWebSocketEvent

	// Career conversions
	PersistenceCareerApplicationToProto(application *common_persistence_models.CareerApplication) *proto_models.CareerApplication
	PersistenceCareerApplicationToApplicantProto(application *common_persistence_models.CareerApplication) *proto_models.ApplicantCareerApplication
	PersistenceCareerApplicationToAdminProto(application *common_persistence_models.CareerApplication) *proto_models.AdminCareerApplication

	// InferenceModel conversions
	PersistenceInferenceModelToService(model *common_persistence_models.InferenceModel) *common_service_models.InferenceModel
	ServiceInferenceModelToProto(model *common_service_models.InferenceModel) *proto_models.InferenceModel
	ServiceInferenceModelToAdminProto(model *common_service_models.InferenceModel) *proto_models.AdminInferenceModel
}

// Compile-time check to ensure ConverterHandler implements ConverterInterface
var _ ConverterInterface = (*ConverterHandler[common_service_models.User])(nil)
