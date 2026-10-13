package common_api

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	"github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/router"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_converter"
	chatservice "github.com/Suhaibinator/go-common/pkg/common_service/chat"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// Tool-call resolution rate limit (Decision 6). ResolveToolCall is an authenticated, mutating route
// that drives the agent run state machine (approve -> durable work item; deny -> synthetic result +
// resume/complete), so it is rate-limited per resolver to bound abuse and accidental retry storms.
//
// The limit is enforced per authenticated user via a custom key extractor rather than StrategyUser:
// SRouter strips the UserIDFromUser/UserIDToString closures when converting a route-level
// RateLimitConfig[any, any] to the concrete [T, U], so StrategyUser is unusable on a route override.
// StrategyCustom's KeyExtractor survives that conversion and lets us key on the resolver's user ID
// (falling back to client IP for the unauthenticated edge that AuthRequired should already prevent).
const (
	resolveToolCallRateLimitBucket = "chat_resolve_tool_call"
	resolveToolCallRateLimit       = 30
	resolveToolCallRateLimitWindow = time.Minute
)

// resolveToolCallRateLimitKey keys the resolve-tool-call rate limit on the authenticated user, with a
// client-IP / RemoteAddr fallback so a missing user (which AuthRequired should prevent) still gets a
// bounded, non-empty bucket rather than sharing one global key.
func resolveToolCallRateLimitKey(r *http.Request) (string, error) {
	if userID, ok := scontext.GetUserID[common_service_models.UserIdType](r.Context()); ok && userID != 0 {
		return "user:" + strconv.FormatUint(uint64(userID), 10), nil
	}
	if ip, ok := scontext.GetClientIP(r.Context()); ok && ip != "" {
		return "ip:" + ip, nil
	}
	return "ip:" + r.RemoteAddr, nil
}

// RegisterChatRoutes RegisterChatRoutes registers API routes and wires this capability into the backend request flow during handler setup.
func (h *CommonAPIHandler[U]) RegisterChatRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	group = h.featureGroup(group, config.APIView.EnableChat)

	// Get transaction middleware for write operations. These chat mutation routes are
	// unary request/response endpoints, so they use the default commit-before-flush
	// middleware; their post-commit fanout hooks run synchronously after commit. Chat no
	// longer owns a socket of its own: it is a consumer of the realtime transport, whose
	// route is registered separately by RegisterRealtimeRoutes.
	transactionMiddleware := h.GetTransactionMiddleware()

	// Register route groups
	h.registerChannelRoutes(group, transactionMiddleware)
	h.registerParticipantRoutes(group, transactionMiddleware)
	h.registerMessageRoutes(group, transactionMiddleware)
	h.registerReactionRoutes(group, transactionMiddleware)
	h.registerPublicReadRoutes(group, transactionMiddleware)

	h.logger.Named(commonAPILoggerName).Debug("Chat routes registered to route group")
}

// ===================================================================
// Channel Route Registration
// ===================================================================

// registerChannelRoutes registerChannelRoutes supports the backend request flow by performing this package's local processing step.
func (h *CommonAPIHandler[U]) registerChannelRoutes(
	group *router.RouteGroup[common_service_models.UserIdType, U],
	txMiddleware common.Middleware,
) {
	// POST /chat/channels - Create channel
	createChannelRoute := router.RouteConfig[*proto_models.CreateChannelRequest, *proto_models.CreateChannelResponse]{
		Path:        "/chat/channels",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.CreateChannelRequest, *proto_models.CreateChannelResponse](),
		Handler:     h.CreateChannel,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeCreateChannelRequest,
		Middlewares: []common.Middleware{txMiddleware},
	}
	group.Route(createChannelRoute)

	// POST /chat/channels/by_members/unique - Get-or-create unique channel for exact member tuple
	uniqueByMembersRoute := router.RouteConfig[*proto_models.GetOrCreateChannelByMembersUniqueRequest, *proto_models.GetOrCreateChannelByMembersUniqueResponse]{
		Path:        "/chat/channels/by_members/unique",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.GetOrCreateChannelByMembersUniqueRequest, *proto_models.GetOrCreateChannelByMembersUniqueResponse](),
		Handler:     h.GetOrCreateChannelByMembersUnique,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeGetOrCreateChannelByMembersUniqueRequest,
		Middlewares: []common.Middleware{txMiddleware},
	}
	group.Route(uniqueByMembersRoute)

	// POST /chat/channels/fork - Fork a channel at a message into a new channel (per-person
	// rate-limited by the service)
	forkChannelRoute := router.RouteConfig[*proto_models.ForkChannelRequest, *proto_models.ForkChannelResponse]{
		Path:        "/chat/channels/fork",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.ForkChannelRequest, *proto_models.ForkChannelResponse](),
		Handler:     h.ForkChannel,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeForkChannelRequest,
		Middlewares: []common.Middleware{txMiddleware},
	}
	group.Route(forkChannelRoute)

	// POST /chat/channels/get - Get channel
	getChannelRoute := router.RouteConfig[*proto_models.GetChannelRequest, *proto_models.GetChannelResponse]{
		Path:       "/chat/channels/get",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.GetChannelRequest, *proto_models.GetChannelResponse](),
		Handler:    h.GetChannel,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeGetChannelRequest,
	}
	group.Route(getChannelRoute)

	// PUT /chat/channels/update - Update channel
	updateChannelRoute := router.RouteConfig[*proto_models.UpdateChannelRequest, *proto_models.UpdateChannelResponse]{
		Path:        "/chat/channels/update",
		Methods:     []router.HttpMethod{http.MethodPut, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.UpdateChannelRequest, *proto_models.UpdateChannelResponse](),
		Handler:     h.UpdateChannel,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeUpdateChannelRequest,
		Middlewares: []common.Middleware{txMiddleware},
	}
	group.Route(updateChannelRoute)

	// POST /chat/channels/list - List channels
	listChannelsRoute := router.RouteConfig[*proto_models.ListChannelsRequest, *proto_models.ListChannelsResponse]{
		Path:       "/chat/channels/list",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.ListChannelsRequest, *proto_models.ListChannelsResponse](),
		Handler:    h.ListChannels,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeListChannelsRequest,
	}
	group.Route(listChannelsRoute)

	// POST /chat/channels/leave - Leave channel
	leaveChannelRoute := router.RouteConfig[*proto_models.LeaveChannelRequest, *proto_models.LeaveChannelResponse]{
		Path:        "/chat/channels/leave",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.LeaveChannelRequest, *proto_models.LeaveChannelResponse](),
		Handler:     h.LeaveChannel,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeLeaveChannelRequest,
		Middlewares: []common.Middleware{txMiddleware},
	}
	group.Route(leaveChannelRoute)
}

// ===================================================================
// Participant Route Registration
// ===================================================================

// registerParticipantRoutes registerParticipantRoutes supports the backend request flow by performing this package's local processing step.
func (h *CommonAPIHandler[U]) registerParticipantRoutes(
	group *router.RouteGroup[common_service_models.UserIdType, U],
	txMiddleware common.Middleware,
) {
	// POST /chat/channels/participants/add - Add participants
	addParticipantsRoute := router.RouteConfig[*proto_models.AddParticipantsRequest, *proto_models.AddParticipantsResponse]{
		Path:        "/chat/channels/participants/add",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.AddParticipantsRequest, *proto_models.AddParticipantsResponse](),
		Handler:     h.AddChannelParticipants,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAddParticipantsRequest,
		Middlewares: []common.Middleware{txMiddleware},
	}
	group.Route(addParticipantsRoute)

	// DELETE /chat/channels/participants/remove - Remove participant
	removeParticipantRoute := router.RouteConfig[*proto_models.RemoveParticipantRequest, *proto_models.RemoveParticipantResponse]{
		Path:        "/chat/channels/participants/remove",
		Methods:     []router.HttpMethod{http.MethodDelete, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.RemoveParticipantRequest, *proto_models.RemoveParticipantResponse](),
		Handler:     h.RemoveChannelParticipant,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeRemoveParticipantRequest,
		Middlewares: []common.Middleware{txMiddleware},
	}
	group.Route(removeParticipantRoute)

	// PUT /chat/channels/participants/role - Update participant role
	updateRoleRoute := router.RouteConfig[*proto_models.UpdateParticipantRoleRequest, *proto_models.UpdateParticipantRoleResponse]{
		Path:        "/chat/channels/participants/role",
		Methods:     []router.HttpMethod{http.MethodPut, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.UpdateParticipantRoleRequest, *proto_models.UpdateParticipantRoleResponse](),
		Handler:     h.UpdateParticipantRole,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeUpdateParticipantRoleRequest,
		Middlewares: []common.Middleware{txMiddleware},
	}
	group.Route(updateRoleRoute)

	// PUT /chat/channels/participants/preferences - Update participant preferences
	updatePrefsRoute := router.RouteConfig[*proto_models.UpdateParticipantPreferencesRequest, *proto_models.UpdateParticipantPreferencesResponse]{
		Path:        "/chat/channels/participants/preferences",
		Methods:     []router.HttpMethod{http.MethodPut, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.UpdateParticipantPreferencesRequest, *proto_models.UpdateParticipantPreferencesResponse](),
		Handler:     h.UpdateParticipantPreferences,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeUpdateParticipantPreferencesRequest,
		Middlewares: []common.Middleware{txMiddleware},
	}
	group.Route(updatePrefsRoute)

	// POST /chat/channels/participants/list - List participants
	listParticipantsRoute := router.RouteConfig[*proto_models.ListParticipantsRequest, *proto_models.ListParticipantsResponse]{
		Path:       "/chat/channels/participants/list",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.ListParticipantsRequest, *proto_models.ListParticipantsResponse](),
		Handler:    h.ListChannelParticipants,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeListParticipantsRequest,
	}
	group.Route(listParticipantsRoute)

	// POST /chat/channels/participants/list_batch - List participants for multiple channels
	listParticipantsBatchRoute := router.RouteConfig[*proto_models.ListParticipantsBatchRequest, *proto_models.ListParticipantsBatchResponse]{
		Path:       "/chat/channels/participants/list_batch",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.ListParticipantsBatchRequest, *proto_models.ListParticipantsBatchResponse](),
		Handler:    h.ListChannelParticipantsBatch,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeListParticipantsBatchRequest,
	}
	group.Route(listParticipantsBatchRoute)
}

// ===================================================================
// Message Route Registration
// ===================================================================

// registerMessageRoutes registerMessageRoutes supports the backend request flow by performing this package's local processing step.
func (h *CommonAPIHandler[U]) registerMessageRoutes(
	group *router.RouteGroup[common_service_models.UserIdType, U],
	txMiddleware common.Middleware,
) {
	// POST /chat/messages/send - Send message
	sendMessageRoute := router.RouteConfig[*proto_models.SendMessageRequest, *proto_models.SendMessageResponse]{
		Path:        "/chat/messages/send",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.SendMessageRequest, *proto_models.SendMessageResponse](),
		Handler:     h.SendMessage,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeSendMessageRequest,
		Middlewares: []common.Middleware{txMiddleware},
	}
	group.Route(sendMessageRoute)

	// PUT /chat/messages/edit - Edit message
	editMessageRoute := router.RouteConfig[*proto_models.EditMessageRequest, *proto_models.EditMessageResponse]{
		Path:        "/chat/messages/edit",
		Methods:     []router.HttpMethod{http.MethodPut, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.EditMessageRequest, *proto_models.EditMessageResponse](),
		Handler:     h.EditMessage,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeEditMessageRequest,
		Middlewares: []common.Middleware{txMiddleware},
	}
	group.Route(editMessageRoute)

	// DELETE /chat/messages/delete - Delete message
	deleteMessageRoute := router.RouteConfig[*proto_models.DeleteMessageRequest, *proto_models.DeleteMessageResponse]{
		Path:        "/chat/messages/delete",
		Methods:     []router.HttpMethod{http.MethodDelete, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.DeleteMessageRequest, *proto_models.DeleteMessageResponse](),
		Handler:     h.DeleteMessage,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeDeleteMessageRequest,
		Middlewares: []common.Middleware{txMiddleware},
	}
	group.Route(deleteMessageRoute)

	// POST /chat/messages/get - Get message
	getMessageRoute := router.RouteConfig[*proto_models.GetMessageRequest, *proto_models.GetMessageResponse]{
		Path:       "/chat/messages/get",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.GetMessageRequest, *proto_models.GetMessageResponse](),
		Handler:    h.GetMessage,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeGetMessageRequest,
	}
	group.Route(getMessageRoute)

	// POST /chat/messages/list - List messages
	listMessagesRoute := router.RouteConfig[*proto_models.ListMessagesRequest, *proto_models.ListMessagesResponse]{
		Path:       "/chat/messages/list",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.ListMessagesRequest, *proto_models.ListMessagesResponse](),
		Handler:    h.ListMessages,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeListMessagesRequest,
	}
	group.Route(listMessagesRoute)

	// POST /chat/messages/edit_history - List message edit history
	listMessageEditHistoryRoute := router.RouteConfig[*proto_models.ListMessageEditHistoryRequest, *proto_models.ListMessageEditHistoryResponse]{
		Path:       "/chat/messages/edit_history",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.ListMessageEditHistoryRequest, *proto_models.ListMessageEditHistoryResponse](),
		Handler:    h.ListMessageEditHistory,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeListMessageEditHistoryRequest,
	}
	group.Route(listMessageEditHistoryRoute)

	// POST /chat/messages/batch_get - Batch get messages
	batchGetRoute := router.RouteConfig[*proto_models.BatchGetMessagesRequest, *proto_models.BatchGetMessagesResponse]{
		Path:       "/chat/messages/batch_get",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.BatchGetMessagesRequest, *proto_models.BatchGetMessagesResponse](),
		Handler:    h.BatchGetMessages,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeBatchGetMessagesRequest,
	}
	group.Route(batchGetRoute)

	// POST /chat/usage/channel_totals - Aggregate inference totals for a channel
	channelTotalsRoute := router.RouteConfig[*proto_models.GetChatInferenceTotalsRequest, *proto_models.GetChatInferenceTotalsResponse]{
		Path:       "/chat/usage/channel_totals",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.GetChatInferenceTotalsRequest, *proto_models.GetChatInferenceTotalsResponse](),
		Handler:    h.GetChatInferenceTotals,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeGetChatInferenceTotalsRequest,
	}
	group.Route(channelTotalsRoute)

	// POST /chat/usage/agent_session - Inference usage scoped to an agent session
	agentSessionUsageRoute := router.RouteConfig[*proto_models.GetAgentSessionUsageRequest, *proto_models.GetAgentSessionUsageResponse]{
		Path:       "/chat/usage/agent_session",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.GetAgentSessionUsageRequest, *proto_models.GetAgentSessionUsageResponse](),
		Handler:    h.GetAgentSessionUsage,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeGetAgentSessionUsageRequest,
	}
	group.Route(agentSessionUsageRoute)

	// POST /chat/agent_runs/inference_calls - Append-only inference-call ledger for one agent run
	runInferenceCallsRoute := router.RouteConfig[*proto_models.ListAgentRunInferenceCallsRequest, *proto_models.ListAgentRunInferenceCallsResponse]{
		Path:       "/chat/agent_runs/inference_calls",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.ListAgentRunInferenceCallsRequest, *proto_models.ListAgentRunInferenceCallsResponse](),
		Handler:    h.ListAgentRunInferenceCalls,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeListAgentRunInferenceCallsRequest,
	}
	group.Route(runInferenceCallsRoute)

	runStatusRoute := router.RouteConfig[*proto_models.GetAgentRunStatusRequest, *proto_models.GetAgentRunStatusResponse]{
		Path:       "/chat/agent_runs/status",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.GetAgentRunStatusRequest, *proto_models.GetAgentRunStatusResponse](),
		Handler:    h.GetAgentRunStatus,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeGetAgentRunStatusRequest,
	}
	group.Route(runStatusRoute)

	// POST /chat/agent_sessions/list - Agent sessions bound to one channel, delegated children included
	listChannelAgentSessionsRoute := router.RouteConfig[*proto_models.ListChannelAgentSessionsRequest, *proto_models.ListChannelAgentSessionsResponse]{
		Path:       "/chat/agent_sessions/list",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.ListChannelAgentSessionsRequest, *proto_models.ListChannelAgentSessionsResponse](),
		Handler:    h.ListChannelAgentSessions,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeListChannelAgentSessionsRequest,
	}
	group.Route(listChannelAgentSessionsRoute)

	// POST /chat/agent_queued_inputs/list - One agent session's queued input, oldest first, for the
	// composer's queue tray
	listAgentQueuedInputsRoute := router.RouteConfig[*proto_models.ListAgentQueuedInputsRequest, *proto_models.ListAgentQueuedInputsResponse]{
		Path:       "/chat/agent_queued_inputs/list",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.ListAgentQueuedInputsRequest, *proto_models.ListAgentQueuedInputsResponse](),
		Handler:    h.ListAgentQueuedInputs,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeListAgentQueuedInputsRequest,
	}
	group.Route(listAgentQueuedInputsRoute)

	// PUT /chat/agent_queued_inputs/edit - Reword queued input before the agent receives it
	editAgentQueuedInputRoute := router.RouteConfig[*proto_models.EditAgentQueuedInputRequest, *proto_models.EditAgentQueuedInputResponse]{
		Path:        "/chat/agent_queued_inputs/edit",
		Methods:     []router.HttpMethod{http.MethodPut, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.EditAgentQueuedInputRequest, *proto_models.EditAgentQueuedInputResponse](),
		Handler:     h.EditAgentQueuedInput,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeEditAgentQueuedInputRequest,
		Middlewares: []common.Middleware{txMiddleware},
	}
	group.Route(editAgentQueuedInputRoute)

	// DELETE /chat/agent_queued_inputs/remove - Withdraw queued input before the agent receives it
	removeAgentQueuedInputRoute := router.RouteConfig[*proto_models.RemoveAgentQueuedInputRequest, *proto_models.RemoveAgentQueuedInputResponse]{
		Path:        "/chat/agent_queued_inputs/remove",
		Methods:     []router.HttpMethod{http.MethodDelete, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.RemoveAgentQueuedInputRequest, *proto_models.RemoveAgentQueuedInputResponse](),
		Handler:     h.RemoveAgentQueuedInput,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeRemoveAgentQueuedInputRequest,
		Middlewares: []common.Middleware{txMiddleware},
	}
	group.Route(removeAgentQueuedInputRoute)

	// POST /chat/messages/inference_summary - Derived inference-usage summary for one message
	messageInferenceSummaryRoute := router.RouteConfig[*proto_models.GetMessageInferenceSummaryRequest, *proto_models.GetMessageInferenceSummaryResponse]{
		Path:       "/chat/messages/inference_summary",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.GetMessageInferenceSummaryRequest, *proto_models.GetMessageInferenceSummaryResponse](),
		Handler:    h.GetMessageInferenceSummary,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeGetMessageInferenceSummaryRequest,
	}
	group.Route(messageInferenceSummaryRoute)

	// POST /chat/messages/batch_inference_summaries - Batched derived inference summaries for a page
	batchInferenceSummariesRoute := router.RouteConfig[*proto_models.BatchGetMessageInferenceSummariesRequest, *proto_models.BatchGetMessageInferenceSummariesResponse]{
		Path:       "/chat/messages/batch_inference_summaries",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.BatchGetMessageInferenceSummariesRequest, *proto_models.BatchGetMessageInferenceSummariesResponse](),
		Handler:    h.BatchGetMessageInferenceSummaries,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeBatchGetMessageInferenceSummariesRequest,
	}
	group.Route(batchInferenceSummariesRoute)

	// POST /chat/messages/resolve_tool_call - Approve/deny a TOOL_CALL awaiting confirmation (mutating,
	// per-resolver rate-limited).
	resolveToolCallRoute := router.RouteConfig[*proto_models.ResolveToolCallRequest, *proto_models.ResolveToolCallResponse]{
		Path:        "/chat/messages/resolve_tool_call",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.ResolveToolCallRequest, *proto_models.ResolveToolCallResponse](),
		Handler:     h.ResolveToolCall,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeResolveToolCallRequest,
		Middlewares: []common.Middleware{txMiddleware},
		Overrides: common.RouteOverrides{
			RateLimit: &common.RateLimitConfig[any, any]{
				BucketName:   resolveToolCallRateLimitBucket,
				Limit:        resolveToolCallRateLimit,
				Window:       resolveToolCallRateLimitWindow,
				Strategy:     common.StrategyCustom,
				KeyExtractor: resolveToolCallRateLimitKey,
			},
		},
	}
	group.Route(resolveToolCallRoute)

	// POST /chat/agent_runs/stop - Stop an agent run and suspend its session (mutating; the service
	// rate-limits it together with Resume). Stop owns its transaction: it cancels the run's local
	// drive after commit, so it takes no transaction middleware.
	stopAgentRunRoute := router.RouteConfig[*proto_models.StopAgentRunRequest, *proto_models.StopAgentRunResponse]{
		Path:       "/chat/agent_runs/stop",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.StopAgentRunRequest, *proto_models.StopAgentRunResponse](),
		Handler:    h.StopAgentRun,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeStopAgentRunRequest,
	}
	group.Route(stopAgentRunRoute)

	// POST /chat/agent_sessions/resume - Resume a stopped agent session (mutating; the service
	// rate-limits it together with Stop; owns its transaction and drives the run it starts after
	// commit).
	resumeAgentSessionRoute := router.RouteConfig[*proto_models.ResumeAgentSessionRequest, *proto_models.ResumeAgentSessionResponse]{
		Path:       "/chat/agent_sessions/resume",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.ResumeAgentSessionRequest, *proto_models.ResumeAgentSessionResponse](),
		Handler:    h.ResumeAgentSession,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeResumeAgentSessionRequest,
	}
	group.Route(resumeAgentSessionRoute)
}

// ===================================================================
// Reaction Route Registration
// ===================================================================

// registerReactionRoutes registerReactionRoutes supports the backend request flow by performing this package's local processing step.
func (h *CommonAPIHandler[U]) registerReactionRoutes(
	group *router.RouteGroup[common_service_models.UserIdType, U],
	txMiddleware common.Middleware,
) {
	// POST /chat/messages/reactions/add - Add reaction
	addReactionRoute := router.RouteConfig[*proto_models.AddReactionRequest, *proto_models.AddReactionResponse]{
		Path:        "/chat/messages/reactions/add",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.AddReactionRequest, *proto_models.AddReactionResponse](),
		Handler:     h.AddReaction,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAddReactionRequest,
		Middlewares: []common.Middleware{txMiddleware},
	}
	group.Route(addReactionRoute)

	// DELETE /chat/messages/reactions/remove - Remove reaction
	removeReactionRoute := router.RouteConfig[*proto_models.RemoveReactionRequest, *proto_models.RemoveReactionResponse]{
		Path:        "/chat/messages/reactions/remove",
		Methods:     []router.HttpMethod{http.MethodDelete, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.RemoveReactionRequest, *proto_models.RemoveReactionResponse](),
		Handler:     h.RemoveReaction,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeRemoveReactionRequest,
		Middlewares: []common.Middleware{txMiddleware},
	}
	group.Route(removeReactionRoute)

	// POST /chat/messages/reactions/list - List reactions
	listReactionsRoute := router.RouteConfig[*proto_models.ListReactionsRequest, *proto_models.ListReactionsResponse]{
		Path:       "/chat/messages/reactions/list",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.ListReactionsRequest, *proto_models.ListReactionsResponse](),
		Handler:    h.ListReactions,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeListReactionsRequest,
	}
	group.Route(listReactionsRoute)
}

// ===================================================================
// Channel Handlers
// ===================================================================

// CreateChannel CreateChannel handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) CreateChannel(
	r *http.Request,
	req *proto_models.CreateChannelRequest,
) (*proto_models.CreateChannelResponse, error) {
	_, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	channel, err := h.serviceHandler.GetChatHandler().CreateChannel(
		r.Context(),
		req.Name,
		req.Description,
		req.ContextId,
		req.ContextDisplayName,
		req.ContextEntityId,
		req.ChannelType,
		req.ParticipantUserIds,
		req.UnreadNotificationsEnabled,
		common_converter.ChannelForkRulesFromProto(req.ForkRules),
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "create_chat_channel"),
			zap.String(appLogger.KeyName, req.Name),
			zap.Stringer(appLogger.KeyType, req.ChannelType),
			zap.Int(appLogger.KeyCount, len(req.ParticipantUserIds)))
	}

	return &proto_models.CreateChannelResponse{
		Channel: h.serviceHandler.GetConverter().ServiceChatChannelToProto(channel),
	}, nil
}

// ForkChannel forks a channel at a message into a new channel the caller owns; see
// chat.Handler.ForkChannel.
func (h *CommonAPIHandler[U]) ForkChannel(r *http.Request, req *proto_models.ForkChannelRequest) (*proto_models.ForkChannelResponse, error) {
	if _, found := scontext.GetUserID[common_service_models.UserIdType](r.Context()); !found {
		return nil, ErrUnauthorized
	}
	channel, err := h.serviceHandler.ForkChatChannel(r.Context(), chatservice.ForkChannelInput{
		SourceChannelID:    req.ChannelId,
		ForkPointMessageID: req.ForkPointMessageId,
		Name:               req.Name,
		MemberUserIDs:      req.MemberUserIds,
		KeepOrgAccess:      req.KeepOrgAccess,
		KeepContext:        req.KeepContext,
		ClientRequestID:    req.ClientRequestId,
	})
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "fork_chat_channel"),
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
			zap.Uint64(appLogger.KeyMessageID, req.ForkPointMessageId),
			zap.Int(appLogger.KeyCount, len(req.MemberUserIds)))
	}
	return &proto_models.ForkChannelResponse{
		Channel: h.serviceHandler.GetConverter().ServiceChatChannelToProto(channel),
	}, nil
}

// GetOrCreateChannelByMembersUnique GetOrCreateChannelByMembersUnique handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) GetOrCreateChannelByMembersUnique(
	r *http.Request,
	req *proto_models.GetOrCreateChannelByMembersUniqueRequest,
) (*proto_models.GetOrCreateChannelByMembersUniqueResponse, error) {
	_, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	resp, err := h.serviceHandler.GetChatHandler().GetOrCreateChannelByMembersUnique(
		r.Context(),
		req.MemberUserIds,
		req.ContextId,
		req.ContextDisplayName,
		req.ContextEntityId,
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "get_or_create_chat_channel_by_members"),
			zap.Int(appLogger.KeyCount, len(req.MemberUserIds)))
	}

	return &proto_models.GetOrCreateChannelByMembersUniqueResponse{
		Channel: h.serviceHandler.GetConverter().ServiceChatChannelToProto(resp.Channel),
		Created: resp.Created,
	}, nil
}

// GetChannel GetChannel handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) GetChannel(
	r *http.Request,
	req *proto_models.GetChannelRequest,
) (*proto_models.GetChannelResponse, error) {
	_, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	channel, err := h.serviceHandler.GetChatHandler().GetChannel(r.Context(), req.ChannelId)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "get_chat_channel"),
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId))
	}

	return &proto_models.GetChannelResponse{
		Channel: h.serviceHandler.GetConverter().ServiceChatChannelToProto(channel),
	}, nil
}

// UpdateChannel UpdateChannel handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) UpdateChannel(
	r *http.Request,
	req *proto_models.UpdateChannelRequest,
) (*proto_models.UpdateChannelResponse, error) {
	_, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	channel, err := h.serviceHandler.GetChatHandler().UpdateChannel(
		r.Context(),
		req.ChannelId,
		req.Name,
		req.Description,
		req.AvatarAttachmentId,
		req.IsArchived,
		req.UnreadNotificationsEnabled,
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "update_chat_channel"),
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId))
	}

	return &proto_models.UpdateChannelResponse{
		Channel: h.serviceHandler.GetConverter().ServiceChatChannelToProto(channel),
	}, nil
}

// ListChannels ListChannels handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) ListChannels(
	r *http.Request,
	req *proto_models.ListChannelsRequest,
) (*proto_models.ListChannelsResponse, error) {
	_, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	rows, page, err := h.serviceHandler.GetChatHandler().ListChannels(r.Context(), req.Pagination, req.IncludeArchived)
	if err != nil {
		return nil, MapServiceErrorWithFields(err, zap.String(logkeys.Operation, "list_chat_channels"))
	}

	channels := common_util.ApplyToEach(rows, func(ch *common_service_models.ChatChannel) *proto_models.Channel {
		return h.serviceHandler.GetConverter().ServiceChatChannelToProto(ch)
	})

	return &proto_models.ListChannelsResponse{
		Channels:   channels,
		Pagination: page,
	}, nil
}

// LeaveChannel LeaveChannel supports the backend request flow by performing this package's local processing step.
func (h *CommonAPIHandler[U]) LeaveChannel(
	r *http.Request,
	req *proto_models.LeaveChannelRequest,
) (*proto_models.LeaveChannelResponse, error) {
	_, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	if err := h.serviceHandler.GetChatHandler().LeaveChannel(r.Context(), req.ChannelId); err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "leave_chat_channel"),
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId))
	}

	return &proto_models.LeaveChannelResponse{}, nil
}

// ===================================================================
// Participant Handlers
// ===================================================================

// AddChannelParticipants AddChannelParticipants supports the backend request flow by performing this package's local processing step.
func (h *CommonAPIHandler[U]) AddChannelParticipants(
	r *http.Request,
	req *proto_models.AddParticipantsRequest,
) (*proto_models.AddParticipantsResponse, error) {
	_, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	rows, err := h.serviceHandler.GetChatHandler().AddChannelParticipants(r.Context(), req.ChannelId, req.UserIds)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "add_chat_channel_participants"),
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
			zap.Int(appLogger.KeyCount, len(req.UserIds)))
	}

	viewerID, _ := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	converter := h.serviceHandler.GetConverter()
	out := common_util.ApplyToEach(rows, func(p *common_service_models.ChatChannelParticipant) *proto_models.ChannelParticipant {
		return converter.ServiceChatChannelParticipantToProto(p, viewerID)
	})

	return &proto_models.AddParticipantsResponse{AddedParticipants: out}, nil
}

// RemoveChannelParticipant RemoveChannelParticipant handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) RemoveChannelParticipant(
	r *http.Request,
	req *proto_models.RemoveParticipantRequest,
) (*proto_models.RemoveParticipantResponse, error) {
	_, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	if err := h.serviceHandler.GetChatHandler().RemoveChannelParticipant(r.Context(), req.ChannelId, req.UserId); err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "remove_chat_channel_participant"),
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
			zap.Uint64(appLogger.KeyTargetUserID, req.UserId))
	}

	return &proto_models.RemoveParticipantResponse{}, nil
}

// UpdateParticipantRole UpdateParticipantRole handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) UpdateParticipantRole(
	r *http.Request,
	req *proto_models.UpdateParticipantRoleRequest,
) (*proto_models.UpdateParticipantRoleResponse, error) {
	_, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	participant, err := h.serviceHandler.GetChatHandler().UpdateChannelParticipantRole(r.Context(), req.ChannelId, req.UserId, req.Role)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "update_chat_channel_participant_role"),
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
			zap.Uint64(appLogger.KeyTargetUserID, req.UserId),
			zap.Stringer(appLogger.KeyNewRole, req.Role))
	}

	viewerID, _ := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	return &proto_models.UpdateParticipantRoleResponse{
		Participant: h.serviceHandler.GetConverter().ServiceChatChannelParticipantToProto(participant, viewerID),
	}, nil
}

// UpdateParticipantPreferences UpdateParticipantPreferences handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) UpdateParticipantPreferences(
	r *http.Request,
	req *proto_models.UpdateParticipantPreferencesRequest,
) (*proto_models.UpdateParticipantPreferencesResponse, error) {
	_, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	participant, err := h.serviceHandler.GetChatHandler().UpdateChannelParticipantPreferences(r.Context(), req.ChannelId, req.NotificationMode, req.IsPinned)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "update_chat_channel_participant_preferences"),
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId))
	}

	viewerID, _ := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	return &proto_models.UpdateParticipantPreferencesResponse{
		Participant: h.serviceHandler.GetConverter().ServiceChatChannelParticipantToProto(participant, viewerID),
	}, nil
}

// ListChannelParticipants ListChannelParticipants handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) ListChannelParticipants(
	r *http.Request,
	req *proto_models.ListParticipantsRequest,
) (*proto_models.ListParticipantsResponse, error) {
	viewerID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	rows, page, current, err := h.serviceHandler.GetChatHandler().ListChannelParticipants(r.Context(), req.ChannelId, req.Pagination)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "list_chat_channel_participants"),
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId))
	}

	converter := h.serviceHandler.GetConverter()
	out := common_util.ApplyToEach(rows, func(p *common_service_models.ChatChannelParticipant) *proto_models.ChannelParticipant {
		return converter.ServiceChatChannelParticipantToProto(p, viewerID)
	})

	var currentParticipant *proto_models.ChannelParticipant
	if current != nil {
		currentParticipant = converter.ServiceChatChannelParticipantToProto(current, viewerID)
	}
	return &proto_models.ListParticipantsResponse{Participants: out, Pagination: page, CurrentParticipant: currentParticipant}, nil
}

// ListChannelParticipantsBatch ListChannelParticipantsBatch handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) ListChannelParticipantsBatch(
	r *http.Request,
	req *proto_models.ListParticipantsBatchRequest,
) (*proto_models.ListParticipantsBatchResponse, error) {
	_, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	rows, err := h.serviceHandler.GetChatHandler().ListChannelParticipantsBatch(r.Context(), req.ChannelIds, req.Pagination)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "list_chat_channel_participants_batch"),
			zap.Int(appLogger.KeyCount, len(req.ChannelIds)))
	}

	viewerID, _ := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	converter := h.serviceHandler.GetConverter()
	items := common_util.ApplyToEach(rows, func(item *common_service_models.ChatChannelParticipantsBatchItem) *proto_models.ChannelParticipantsBatchItem {
		participants := common_util.ApplyToEach(item.Participants, func(p *common_service_models.ChatChannelParticipant) *proto_models.ChannelParticipant {
			return converter.ServiceChatChannelParticipantToProto(p, viewerID)
		})
		return &proto_models.ChannelParticipantsBatchItem{
			ChannelId:    uint64(item.ChannelID),
			Participants: participants,
			Pagination:   item.Pagination,
		}
	})

	return &proto_models.ListParticipantsBatchResponse{Items: items}, nil
}

// ===================================================================
// Message Handlers
// ===================================================================

// SendMessage SendMessage supports the backend request flow by performing this package's local processing step.
func (h *CommonAPIHandler[U]) SendMessage(
	r *http.Request,
	req *proto_models.SendMessageRequest,
) (*proto_models.SendMessageResponse, error) {
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	result, err := h.serviceHandler.GetChatHandler().SendMessageWithAttachments(
		r.Context(),
		chatservice.SendMessageWithAttachmentsInput{
			ChannelID:          req.ChannelId,
			Content:            req.Content,
			ParentMessageID:    req.ParentMessageId,
			ReplyToMessageID:   req.ReplyToMessageId,
			BroadcastToChannel: req.BroadcastToChannel,
			ClientMessageID:    req.ClientMessageId,
			Embeds:             req.Embeds,
			Attachments:        req.Attachments,
		},
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "send_chat_message"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(userID)),
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
			zap.Int(appLogger.KeyAttachmentCount, len(req.Attachments)))
	}

	converter := h.serviceHandler.GetConverter()
	response := &proto_models.SendMessageResponse{
		NewAttachments:     result.NewAttachments,
		AgentInputReceipts: result.AgentInputReceipts,
		QueuedInput:        converter.ServiceChatAgentQueuedInputToProto(result.QueuedInput),
	}
	// A queued send has no message yet. Converted only when present: the converter takes an
	// interface, which a nil *ChatMessage would not leave nil.
	if result.Message != nil {
		response.Message = converter.ServiceChatMessageToProto(r.Context(), result.Message)
	}
	return response, nil
}

// EditMessage EditMessage supports the backend request flow by performing this package's local processing step.
func (h *CommonAPIHandler[U]) EditMessage(
	r *http.Request,
	req *proto_models.EditMessageRequest,
) (*proto_models.EditMessageResponse, error) {
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	result, err := h.serviceHandler.GetChatHandler().EditMessageWithAttachments(
		r.Context(),
		chatservice.EditMessageWithAttachmentsInput{
			ActorUserID:        uint64(userID),
			MessageID:          req.MessageId,
			Content:            req.Content,
			Embeds:             req.Embeds,
			ReplaceEmbeds:      req.ReplaceEmbeds,
			Attachments:        req.Attachments,
			ReplaceAttachments: req.ReplaceAttachments,
		},
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "edit_chat_message"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(userID)),
			zap.Uint64(appLogger.KeyMessageID, req.MessageId),
			zap.Int(appLogger.KeyAttachmentCount, len(req.Attachments)))
	}

	return &proto_models.EditMessageResponse{
		Message:        h.serviceHandler.GetConverter().ServiceChatMessageToProto(r.Context(), result.Message),
		NewAttachments: result.NewAttachments,
	}, nil
}

// DeleteMessage DeleteMessage handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) DeleteMessage(
	r *http.Request,
	req *proto_models.DeleteMessageRequest,
) (*proto_models.DeleteMessageResponse, error) {
	_, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	if err := h.serviceHandler.GetChatHandler().DeleteMessage(r.Context(), req.MessageId); err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "delete_chat_message"),
			zap.Uint64(appLogger.KeyMessageID, req.MessageId))
	}
	return &proto_models.DeleteMessageResponse{}, nil
}

// GetMessage GetMessage handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) GetMessage(
	r *http.Request,
	req *proto_models.GetMessageRequest,
) (*proto_models.GetMessageResponse, error) {
	_, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	msg, err := h.serviceHandler.GetChatHandler().GetMessage(r.Context(), req.MessageId, req.IncludeHidden)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "get_chat_message"),
			zap.Uint64(appLogger.KeyMessageID, req.MessageId))
	}
	return &proto_models.GetMessageResponse{
		Message: h.serviceHandler.GetConverter().ServiceChatMessageToProto(r.Context(), msg),
	}, nil
}

// ListMessages ListMessages handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) ListMessages(
	r *http.Request,
	req *proto_models.ListMessagesRequest,
) (*proto_models.ListMessagesResponse, error) {
	_, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	rows, page, err := h.serviceHandler.GetChatHandler().ListMessages(
		r.Context(),
		req.ChannelId,
		req.Pagination,
		req.ParentMessageId,
		req.IncludeDeleted,
		req.Sort,
		req.SortBy,
		req.IncludeHidden,
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "list_chat_messages"),
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId))
	}

	ctx := r.Context()
	converter := h.serviceHandler.GetConverter()
	out := common_util.ApplyToEach(rows, func(m *common_service_models.ChatMessage) *proto_models.Message {
		return converter.ServiceChatMessageToProto(ctx, m)
	})
	return &proto_models.ListMessagesResponse{Messages: out, Pagination: page}, nil
}

// ListMessageEditHistory ListMessageEditHistory handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) ListMessageEditHistory(
	r *http.Request,
	req *proto_models.ListMessageEditHistoryRequest,
) (*proto_models.ListMessageEditHistoryResponse, error) {
	_, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	rows, page, err := h.serviceHandler.GetChatHandler().ListMessageEditHistory(r.Context(), req.MessageId, req.Pagination)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "list_chat_message_edit_history"),
			zap.Uint64(appLogger.KeyMessageID, req.MessageId))
	}

	ctx := r.Context()
	converter := h.serviceHandler.GetConverter()
	out := common_util.ApplyToEach(rows, func(entry *common_service_models.ChatMessageEditHistoryEntry) *proto_models.ChatMessageEditHistoryEntry {
		return converter.ServiceChatMessageEditHistoryToProto(ctx, entry)
	})
	return &proto_models.ListMessageEditHistoryResponse{Entries: out, Pagination: page}, nil
}

// BatchGetMessages BatchGetMessages supports the backend request flow by performing this package's local processing step.
func (h *CommonAPIHandler[U]) BatchGetMessages(
	r *http.Request,
	req *proto_models.BatchGetMessagesRequest,
) (*proto_models.BatchGetMessagesResponse, error) {
	_, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	rows, missing, err := h.serviceHandler.GetChatHandler().BatchGetMessages(r.Context(), req.ChannelId, req.MessageIds, req.IncludeHidden)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "batch_get_chat_messages"),
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
			zap.Int(appLogger.KeyMessageCount, len(req.MessageIds)))
	}

	ctx := r.Context()
	converter := h.serviceHandler.GetConverter()
	out := common_util.ApplyToEach(rows, func(m *common_service_models.ChatMessage) *proto_models.Message {
		return converter.ServiceChatMessageToProto(ctx, m)
	})

	return &proto_models.BatchGetMessagesResponse{
		Messages:          out,
		MissingMessageIds: missing,
	}, nil
}

// ===================================================================
// Usage + Tool-Call Resolution Handlers
// ===================================================================

// GetChatInferenceTotals returns the aggregate inference totals for a channel.
func (h *CommonAPIHandler[U]) GetChatInferenceTotals(r *http.Request, req *proto_models.GetChatInferenceTotalsRequest) (*proto_models.GetChatInferenceTotalsResponse, error) {
	if _, found := scontext.GetUserID[common_service_models.UserIdType](r.Context()); !found {
		return nil, ErrUnauthorized
	}
	totals, err := h.serviceHandler.GetChatHandler().GetChatInferenceTotals(r.Context(), req.ChannelId)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "get_chat_inference_totals"),
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId))
	}
	return &proto_models.GetChatInferenceTotalsResponse{Totals: totals}, nil
}

// GetAgentSessionUsage returns session-scoped usage totals plus breakdowns.
func (h *CommonAPIHandler[U]) GetAgentSessionUsage(r *http.Request, req *proto_models.GetAgentSessionUsageRequest) (*proto_models.GetAgentSessionUsageResponse, error) {
	if _, found := scontext.GetUserID[common_service_models.UserIdType](r.Context()); !found {
		return nil, ErrUnauthorized
	}
	usage, byRunPage, byModelPage, err := h.serviceHandler.GetChatHandler().GetAgentSessionUsage(
		r.Context(),
		req.AgentSessionId,
		req.ByRunPagination,
		req.ByModelPagination,
		req.IncludeChildren,
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "get_agent_session_usage"),
			zap.Uint64(appLogger.KeySessionID, req.AgentSessionId))
	}
	return &proto_models.GetAgentSessionUsageResponse{
		Usage:             usage,
		ByRunPagination:   byRunPage,
		ByModelPagination: byModelPage,
	}, nil
}

// ListAgentRunInferenceCalls returns the paginated inference-call ledger for one run.
func (h *CommonAPIHandler[U]) ListAgentRunInferenceCalls(r *http.Request, req *proto_models.ListAgentRunInferenceCallsRequest) (*proto_models.ListAgentRunInferenceCallsResponse, error) {
	if _, found := scontext.GetUserID[common_service_models.UserIdType](r.Context()); !found {
		return nil, ErrUnauthorized
	}
	calls, page, err := h.serviceHandler.GetChatHandler().ListAgentRunInferenceCalls(r.Context(), req.AgentRunId, req.Pagination)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "list_agent_run_inference_calls"),
			zap.Uint64(appLogger.KeyAgentRunID, req.AgentRunId))
	}
	return &proto_models.ListAgentRunInferenceCallsResponse{Calls: calls, Pagination: page}, nil
}

// GetAgentRunStatus returns the authoritative agent-run status for thread locations of one channel.
func (h *CommonAPIHandler[U]) GetAgentRunStatus(r *http.Request, req *proto_models.GetAgentRunStatusRequest) (*proto_models.GetAgentRunStatusResponse, error) {
	if _, found := scontext.GetUserID[common_service_models.UserIdType](r.Context()); !found {
		return nil, ErrUnauthorized
	}
	resp, err := h.serviceHandler.GetChatHandler().GetAgentRunStatus(r.Context(), req.ChannelId, req.ParentMessageIds, req.HistoryLimit, req.IncludeDelegatedThreads)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "get_agent_run_status"),
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId))
	}
	return resp, nil
}

// ListChannelAgentSessions returns the agent sessions bound to one channel, including delegated
// child sessions, for any channel reader.
func (h *CommonAPIHandler[U]) ListChannelAgentSessions(r *http.Request, req *proto_models.ListChannelAgentSessionsRequest) (*proto_models.ListChannelAgentSessionsResponse, error) {
	if _, found := scontext.GetUserID[common_service_models.UserIdType](r.Context()); !found {
		return nil, ErrUnauthorized
	}
	resp, err := h.serviceHandler.GetChatHandler().ListChannelAgentSessions(r.Context(), req.ChannelId)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "list_channel_agent_sessions"),
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId))
	}
	return resp, nil
}

// GetMessageInferenceSummary returns the derived per-message usage summary.
func (h *CommonAPIHandler[U]) GetMessageInferenceSummary(r *http.Request, req *proto_models.GetMessageInferenceSummaryRequest) (*proto_models.GetMessageInferenceSummaryResponse, error) {
	if _, found := scontext.GetUserID[common_service_models.UserIdType](r.Context()); !found {
		return nil, ErrUnauthorized
	}
	summary, err := h.serviceHandler.GetChatHandler().GetMessageInferenceSummary(r.Context(), req.MessageId)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "get_chat_message_inference_summary"),
			zap.Uint64(appLogger.KeyMessageID, req.MessageId))
	}
	return &proto_models.GetMessageInferenceSummaryResponse{Summary: summary}, nil
}

// BatchGetMessageInferenceSummaries returns derived per-message usage summaries for a transcript page.
func (h *CommonAPIHandler[U]) BatchGetMessageInferenceSummaries(r *http.Request, req *proto_models.BatchGetMessageInferenceSummariesRequest) (*proto_models.BatchGetMessageInferenceSummariesResponse, error) {
	if _, found := scontext.GetUserID[common_service_models.UserIdType](r.Context()); !found {
		return nil, ErrUnauthorized
	}
	summaries, err := h.serviceHandler.GetChatHandler().BatchGetMessageInferenceSummaries(r.Context(), req.ChannelId, req.MessageIds)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "batch_get_chat_message_inference_summaries"),
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
			zap.Int(appLogger.KeyMessageCount, len(req.MessageIds)))
	}
	return &proto_models.BatchGetMessageInferenceSummariesResponse{Summaries: summaries}, nil
}

// ResolveToolCall approves or denies a TOOL_CALL awaiting human-in-the-loop confirmation. The
// resolution state machine is implemented in the service/approval layer.
func (h *CommonAPIHandler[U]) ResolveToolCall(r *http.Request, req *proto_models.ResolveToolCallRequest) (*proto_models.ResolveToolCallResponse, error) {
	if _, found := scontext.GetUserID[common_service_models.UserIdType](r.Context()); !found {
		return nil, ErrUnauthorized
	}
	msg, err := h.serviceHandler.GetChatHandler().ResolveToolCall(r.Context(), req.MessageId, req.Approve, req.Reason)
	// A late approval expires the row inside this request's transaction. Preserve the typed service
	// outcome for non-HTTP callers, but return the committed EXPIRED message here: surfacing an HTTP
	// error would make the transaction middleware roll back the correctness transition.
	if err != nil && (!errors.Is(err, serviceErrors.ErrToolCallApprovalExpired) || msg == nil) {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "resolve_tool_call"),
			zap.Uint64(appLogger.KeyMessageID, req.MessageId),
			zap.Bool(appLogger.KeyApprove, req.Approve),
			zap.String(logkeys.Reason, req.GetReason()))
	}
	return &proto_models.ResolveToolCallResponse{
		Message: h.serviceHandler.GetConverter().ServiceChatMessageToProto(r.Context(), msg),
	}, nil
}

// StopAgentRun stops an agent run and suspends its session until Resume.
func (h *CommonAPIHandler[U]) StopAgentRun(r *http.Request, req *proto_models.StopAgentRunRequest) (*proto_models.StopAgentRunResponse, error) {
	if _, found := scontext.GetUserID[common_service_models.UserIdType](r.Context()); !found {
		return nil, ErrUnauthorized
	}
	resp, err := h.serviceHandler.StopChatAgentRun(r.Context(), req.AgentRunId)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "stop_agent_run"),
			zap.Uint64(appLogger.KeyAgentRunID, req.AgentRunId))
	}
	return resp, nil
}

// ResumeAgentSession lifts the suspension a Stop left on an agent session.
func (h *CommonAPIHandler[U]) ResumeAgentSession(r *http.Request, req *proto_models.ResumeAgentSessionRequest) (*proto_models.ResumeAgentSessionResponse, error) {
	if _, found := scontext.GetUserID[common_service_models.UserIdType](r.Context()); !found {
		return nil, ErrUnauthorized
	}
	resp, err := h.serviceHandler.ResumeChatAgentSession(r.Context(), req.AgentSessionId)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "resume_agent_session"),
			zap.Uint64(appLogger.KeyAgentSessionID, req.AgentSessionId))
	}
	return resp, nil
}

// ListAgentQueuedInputs pages one agent session's queued input, oldest first.
func (h *CommonAPIHandler[U]) ListAgentQueuedInputs(r *http.Request, req *proto_models.ListAgentQueuedInputsRequest) (*proto_models.ListAgentQueuedInputsResponse, error) {
	if _, found := scontext.GetUserID[common_service_models.UserIdType](r.Context()); !found {
		return nil, ErrUnauthorized
	}
	queued, page, err := h.serviceHandler.GetChatHandler().ListAgentQueuedInputs(r.Context(), req.AgentSessionId, req.Pagination)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "list_agent_queued_inputs"),
			zap.Uint64(appLogger.KeyAgentSessionID, req.AgentSessionId))
	}
	return &proto_models.ListAgentQueuedInputsResponse{
		QueuedInputs: common_util.ApplyToEach(queued, h.serviceHandler.GetConverter().ServiceChatAgentQueuedInputToProto),
		Pagination:   page,
	}, nil
}

// EditAgentQueuedInput rewords queued input the caller sent, before the agent receives it.
func (h *CommonAPIHandler[U]) EditAgentQueuedInput(r *http.Request, req *proto_models.EditAgentQueuedInputRequest) (*proto_models.EditAgentQueuedInputResponse, error) {
	if _, found := scontext.GetUserID[common_service_models.UserIdType](r.Context()); !found {
		return nil, ErrUnauthorized
	}
	result, err := h.serviceHandler.GetChatHandler().EditAgentQueuedInput(r.Context(), chatservice.EditAgentQueuedInputInput{
		QueuedInputID: req.QueuedInputId,
		Content:       req.Content,
		Attachments:   req.Attachments,
	})
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "edit_agent_queued_input"),
			zap.Uint64(appLogger.KeyQueuedInputID, req.QueuedInputId),
			zap.Int(appLogger.KeyAttachmentCount, len(req.Attachments)))
	}
	return &proto_models.EditAgentQueuedInputResponse{
		QueuedInput:    h.serviceHandler.GetConverter().ServiceChatAgentQueuedInputToProto(result.QueuedInput),
		NewAttachments: result.NewAttachments,
	}, nil
}

// RemoveAgentQueuedInput withdraws queued input before the agent receives it.
func (h *CommonAPIHandler[U]) RemoveAgentQueuedInput(r *http.Request, req *proto_models.RemoveAgentQueuedInputRequest) (*proto_models.RemoveAgentQueuedInputResponse, error) {
	if _, found := scontext.GetUserID[common_service_models.UserIdType](r.Context()); !found {
		return nil, ErrUnauthorized
	}
	if err := h.serviceHandler.GetChatHandler().RemoveAgentQueuedInput(r.Context(), req.QueuedInputId); err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "remove_agent_queued_input"),
			zap.Uint64(appLogger.KeyQueuedInputID, req.QueuedInputId))
	}
	return &proto_models.RemoveAgentQueuedInputResponse{}, nil
}

// ===================================================================
// Reaction Handlers
// ===================================================================

// AddReaction AddReaction supports the backend request flow by performing this package's local processing step.
func (h *CommonAPIHandler[U]) AddReaction(
	r *http.Request,
	req *proto_models.AddReactionRequest,
) (*proto_models.AddReactionResponse, error) {
	_, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	reaction, err := h.serviceHandler.GetChatHandler().AddReaction(r.Context(), req.MessageId, req.Emoji)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "add_chat_reaction"),
			zap.Uint64(appLogger.KeyMessageID, req.MessageId),
			zap.String(appLogger.KeyEmoji, req.Emoji))
	}

	return &proto_models.AddReactionResponse{
		Reaction: h.serviceHandler.GetConverter().ServiceChatMessageReactionToProto(r.Context(), reaction),
	}, nil
}

// RemoveReaction RemoveReaction handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) RemoveReaction(
	r *http.Request,
	req *proto_models.RemoveReactionRequest,
) (*proto_models.RemoveReactionResponse, error) {
	_, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	if err := h.serviceHandler.GetChatHandler().RemoveReaction(r.Context(), req.MessageId, req.Emoji); err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "remove_chat_reaction"),
			zap.Uint64(appLogger.KeyMessageID, req.MessageId),
			zap.String(appLogger.KeyEmoji, req.Emoji))
	}
	return &proto_models.RemoveReactionResponse{}, nil
}

// ListReactions ListReactions handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) ListReactions(
	r *http.Request,
	req *proto_models.ListReactionsRequest,
) (*proto_models.ListReactionsResponse, error) {
	_, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	rows, page, err := h.serviceHandler.GetChatHandler().ListReactions(r.Context(), req.MessageId, req.Pagination, req.EmojiFilter)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "list_chat_reactions"),
			zap.Uint64(appLogger.KeyMessageID, req.MessageId))
	}

	ctx := r.Context()
	converter := h.serviceHandler.GetConverter()
	out := common_util.ApplyToEach(rows, func(reaction *common_service_models.ChatMessageReaction) *proto_models.MessageReaction {
		return converter.ServiceChatMessageReactionToProto(ctx, reaction)
	})
	return &proto_models.ListReactionsResponse{Reactions: out, Pagination: page}, nil
}
