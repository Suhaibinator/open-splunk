package chat

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_converter"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	common_persistence_models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	commonai "github.com/Suhaibinator/go-common/pkg/common_service/ai"
	attachmentpkg "github.com/Suhaibinator/go-common/pkg/common_service/attachment"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/jellydator/ttlcache/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const (
	chatLoggerName                     = "common_service.chat"
	toolApprovalExpiryLoggerName       = "common_service.chat.tool_approval_expiry"
	toolApprovalWorkBackstopLoggerName = "common_service.chat.tool_approval_work_backstop"

	embedTypeNativeMax = 999
	embedTypeCustomMin = 1000
)

type HandlerInterface[U common_service_models.UserLike] interface {
	// Channels
	// CreateChannel stores forkRules on the new channel; they cannot be changed afterwards.
	CreateChannel(ctx context.Context, name string, description string, contextID *uint64, contextDisplayName *string, contextEntityID *uint64, channelType proto_models.ChannelType, participantUserIDs []uint64, unreadNotificationsEnabled *bool, forkRules common_persistence_models.ChatChannelForkRules) (*common_service_models.ChatChannel, error)
	// CreateWorkflowChannel is the server-side create of a workflow transcript channel with exact
	// feature flags and, optionally, organization read access the actor need not be a member for.
	CreateWorkflowChannel(ctx context.Context, input CreateWorkflowChannelInput) (*common_service_models.ChatChannel, error)
	// ForkChannel copies a channel's history up to a message into a new channel the actor owns.
	ForkChannel(ctx context.Context, input ForkChannelInput) (*common_service_models.ChatChannel, error)
	GetChannel(ctx context.Context, channelID uint64) (*common_service_models.ChatChannel, error)
	UpdateChannel(ctx context.Context, channelID uint64, name *string, description *string, avatarAttachmentID *uint64, isArchived *bool, unreadNotificationsEnabled *bool) (*common_service_models.ChatChannel, error)
	// SetChannelOrganizationAccess sets or clears (nil) the channel-owned organization access that
	// lets qualifying members of an organization read the channel without being participants.
	SetChannelOrganizationAccess(ctx context.Context, channelID uint64, access *common_service_models.ChatChannelOrganizationAccess) error
	// SetChannelPublicRead sets who beyond participants may read a channel (owner only) and
	// returns the updated channel.
	SetChannelPublicRead(ctx context.Context, channelID uint64, level proto_models.ChatChannelPublicRead) (*common_service_models.ChatChannel, error)
	// GetPublicChannel and ListPublicMessages are the actor-less reads of an ANYONE channel.
	// Callers outside the service facade skip its per-IP rate limit.
	GetPublicChannel(ctx context.Context, channelID uint64) (*common_service_models.ChatChannel, error)
	ListPublicMessages(ctx context.Context, channelID uint64, pagination *proto_models.PaginationRequest, parentMessageID uint64, sort *proto_models.SortRequest, sortBy proto_models.ListMessagesSortBy) ([]*common_service_models.ChatMessage, *proto_models.PaginationResponse, error)
	ListChannels(ctx context.Context, pagination *proto_models.PaginationRequest, includeArchived bool) ([]*common_service_models.ChatChannel, *proto_models.PaginationResponse, error)
	LeaveChannel(ctx context.Context, channelID uint64) error

	// Participants
	AddChannelParticipants(ctx context.Context, channelID uint64, userIDs []uint64) ([]*common_service_models.ChatChannelParticipant, error)
	RemoveChannelParticipant(ctx context.Context, channelID uint64, userID common_service_models.UserIdType) error
	UpdateChannelParticipantRole(ctx context.Context, channelID uint64, userID common_service_models.UserIdType, role proto_models.ChannelRole) (*common_service_models.ChatChannelParticipant, error)
	UpdateChannelParticipantPreferences(ctx context.Context, channelID uint64, notificationMode *proto_models.ChannelNotificationMode, isPinned *bool) (*common_service_models.ChatChannelParticipant, error)
	ListChannelParticipants(ctx context.Context, channelID uint64, pagination *proto_models.PaginationRequest) ([]*common_service_models.ChatChannelParticipant, *proto_models.PaginationResponse, *common_service_models.ChatChannelParticipant, error)
	ListChannelParticipantsBatch(ctx context.Context, channelIDs []uint64, pagination *proto_models.PaginationRequest) ([]*common_service_models.ChatChannelParticipantsBatchItem, error)

	GetOrCreateChannelByMembersUnique(ctx context.Context, memberUserIDs []uint64, contextID *uint64, contextDisplayName *string, contextEntityID *uint64) (*GetOrCreateUniqueChannelResponse, error)
	SendMessage(
		ctx context.Context,
		input SendMessageInput,
	) (*common_service_models.ChatMessage, error)
	SendMessageWithAttachments(
		ctx context.Context,
		input SendMessageWithAttachmentsInput,
	) (*SendMessageWithAttachmentsResult, error)
	SendBotMessage(
		ctx context.Context,
		input SendBotMessageInput,
	) (*common_service_models.ChatMessage, error)
	EditMessage(
		ctx context.Context,
		messageID uint64,
		content string,
		embeds []persistence.CreateChatMessageEmbed,
		attachmentIDs []uint64,
	) (*common_service_models.ChatMessage, error)
	EditMessageWithAttachments(
		ctx context.Context,
		input EditMessageWithAttachmentsInput,
	) (*EditMessageWithAttachmentsResult, error)
	EditBotMessage(
		ctx context.Context,
		messageID uint64,
		content string,
		contentType proto_models.MessageContentType,
		embeds []persistence.CreateChatMessageEmbed,
		attachmentIDs []uint64,
	) (*common_service_models.ChatMessage, error)
	EditBotMessageWithStreamState(
		ctx context.Context,
		messageID uint64,
		content string,
		contentType proto_models.MessageContentType,
		streamState proto_models.MessageStreamState,
		embeds []persistence.CreateChatMessageEmbed,
		attachmentIDs []uint64,
	) (*common_service_models.ChatMessage, error)
	DeleteMessage(ctx context.Context, messageID uint64) error

	// Queued agent input: what people sent while an agent session was busy or suspended, which
	// becomes a message only when the agent receives it.
	ListAgentQueuedInputs(ctx context.Context, sessionID uint64, pagination *proto_models.PaginationRequest) ([]*common_service_models.ChatAgentQueuedInput, *proto_models.PaginationResponse, error)
	EditAgentQueuedInput(ctx context.Context, input EditAgentQueuedInputInput) (*EditAgentQueuedInputResult, error)
	RemoveAgentQueuedInput(ctx context.Context, queuedInputID uint64) error

	GetMessage(ctx context.Context, messageID uint64, includeHidden bool) (*common_service_models.ChatMessage, error)
	ListMessageEditHistory(
		ctx context.Context,
		messageID uint64,
		pagination *proto_models.PaginationRequest,
	) ([]*common_service_models.ChatMessageEditHistoryEntry, *proto_models.PaginationResponse, error)
	BatchGetMessages(ctx context.Context, channelID uint64, messageIDs []uint64, includeHidden bool) ([]*common_service_models.ChatMessage, []uint64, error)
	ListMessages(
		ctx context.Context,
		channelID uint64,
		pagination *proto_models.PaginationRequest,
		parentMessageID uint64,
		includeDeleted bool,
		sort *proto_models.SortRequest,
		sortBy proto_models.ListMessagesSortBy,
		includeHidden bool,
	) ([]*common_service_models.ChatMessage, *proto_models.PaginationResponse, error)
	AddReaction(ctx context.Context, messageID uint64, emoji string) (*common_service_models.ChatMessageReaction, error)
	RemoveReaction(ctx context.Context, messageID uint64, emoji string) error
	ListReactions(
		ctx context.Context,
		messageID uint64,
		pagination *proto_models.PaginationRequest,
		emojiFilter *string,
	) ([]*common_service_models.ChatMessageReaction, *proto_models.PaginationResponse, error)
	SupportsAttachments() bool

	// Agent sessions (server-side Go service contract; no public HTTP/proto API in v1).
	CreateAgentChatSession(ctx context.Context, input CreateAgentChatSessionInput) (*CreateAgentChatSessionResult, error)
	CreateAgentChatSessionInLocation(ctx context.Context, input CreateAgentChatSessionInLocationInput) (*CreateAgentChatSessionResult, error)
	RetargetAgentChatSession(context.Context, RetargetAgentChatSessionInput) (*common_service_models.ChatAgentSession, error)
	BulkRetargetAgentChatSessions(context.Context, BulkRetargetAgentChatSessionsInput) ([]*common_service_models.ChatAgentSession, error)
	GetAgentChatSessionInLocation(ctx context.Context, input GetAgentChatSessionInLocationInput) (*GetAgentChatSessionResult, error)
	GetAgentChatSession(ctx context.Context, input GetAgentChatSessionInput) (*GetAgentChatSessionResult, error)
	UpdateAgentChatSessionConfig(ctx context.Context, input UpdateAgentChatSessionConfigInput) (*UpdateAgentChatSessionConfigResult, error)

	// Usage reads (public HTTP; return zeros, not errors, when no ledger rows exist).
	GetChatInferenceTotals(ctx context.Context, channelID uint64) (*proto_models.ChatInferenceTotals, error)
	// GetAgentSessionUsage rolls usage up over the session; includeChildren additionally folds a
	// delegation parent's children into the totals and fills the by_session axis.
	GetAgentSessionUsage(ctx context.Context, agentSessionID uint64, byRunPagination *proto_models.PaginationRequest, byModelPagination *proto_models.PaginationRequest, includeChildren bool) (*proto_models.AgentSessionUsage, *proto_models.PaginationResponse, *proto_models.PaginationResponse, error)
	ListAgentRunInferenceCalls(ctx context.Context, agentRunID uint64, pagination *proto_models.PaginationRequest) ([]*proto_models.ChatInferenceCall, *proto_models.PaginationResponse, error)
	// GetAgentRunStatus reports run status per thread location; includeDelegatedThreads appends the
	// channel's delegated child threads to the requested locations.
	GetAgentRunStatus(ctx context.Context, channelID uint64, parentMessageIDs []uint64, historyLimit uint32, includeDelegatedThreads bool) (*proto_models.GetAgentRunStatusResponse, error)
	// ListChannelAgentSessions lists the agent sessions bound to one channel (main timeline first,
	// then threads), including delegated children, for any channel reader.
	ListChannelAgentSessions(ctx context.Context, channelID uint64) (*proto_models.ListChannelAgentSessionsResponse, error)
	GetMessageInferenceSummary(ctx context.Context, messageID uint64) (*proto_models.MessageInferenceSummary, error)
	BatchGetMessageInferenceSummaries(ctx context.Context, channelID uint64, messageIDs []uint64) ([]*proto_models.MessageInferenceSummary, error)

	// ResolveToolCall approves/denies a pending TOOL_CALL. The approval state machine is owned by
	// the approval lane; a late approval returns the committed EXPIRED message together with
	// ErrToolCallApprovalExpired so non-HTTP callers can distinguish the outcome.
	ResolveToolCall(ctx context.Context, messageID uint64, approve bool, reason *string) (*common_service_models.ChatMessage, error)

	// PostSystemEvent writes a MESSAGE_KIND_SYSTEM_EVENT transcript row through the internal
	// transcript writer (NOT the public send path). Apps call it from their own backend code (for
	// example join/leave/rename hooks) with service authority — it is excluded from AI context and
	// visible to users by default, and never bumps unread/preview (Decision 5 / per-kind defaults).
	PostSystemEvent(ctx context.Context, input PostSystemEventInput) (*common_service_models.ChatMessage, error)

	// WriteCompactionSummary persists a MESSAGE_KIND_COMPACTION_SUMMARY checkpoint plus its
	// purpose=COMPACTION ChatInferenceCall ledger row, validating the watermark on write (Decision 3).
	// go-common provides this primitive; the app/runtime triggers it.
	WriteCompactionSummary(ctx context.Context, input CompactionSummaryInput) (*common_service_models.ChatMessage, error)

	// Wiring setters. The service handler drives these directly through the interface (no
	// concrete-type assertion), so decorating implementations must forward every one below —
	// embedding *Handler does so automatically.

	// SetEventPublisher wires the post-commit chat event publisher.
	SetEventPublisher(publisher ports.ChatEventPublisher)
	// SetPermissionCallbacks wires the optional chat permission hooks.
	SetPermissionCallbacks(cfg *ports.ChatPermissionCallbackConfig[U])
	// SetAgentRegistries wires the agent-type and agent-tool registries used to validate agent
	// chat sessions.
	SetAgentRegistries(agents ports.AgentRegistry, tools ports.AgentToolRegistry)
	// SetToolCallExecutor wires the pure tool executor (runtime lane) used by the approved-work
	// scanner to run claimed TOOL_CALLs.
	SetToolCallExecutor(executor ports.ToolCallExecutor)
	// SetInternalAttachmentStore wires private storage for provider-facing tool media.
	SetInternalAttachmentStore(store ports.InternalAttachmentStore)

	// Agentic tool-approval lane background tasks (Decision 6). The service handler starts these
	// after wiring and stops them on shutdown; both are idempotent/no-op when nothing is configured.
	StartApprovalBackgroundTasks()
	StopApprovalBackgroundTasks()
}

type AttachmentHydrator interface {
	HydrateChatMessagesAttachments(ctx context.Context, messages []*common_service_models.ChatMessage) error
	HydrateChatAgentQueuedInputsAttachments(ctx context.Context, inputs []*common_service_models.ChatAgentQueuedInput) error
}

// GetOrCreateUniqueChannelResponse defines the chat package data shape used in the backend request and persistence flow.
// It is used to carry state between handlers, service orchestration, and storage integrations in this package.
type GetOrCreateUniqueChannelResponse struct {
	Channel *common_service_models.ChatChannel
	Created bool
}

// Handler defines the chat package data shape used in the backend request and persistence flow.
// It is used to carry state between handlers, service orchestration, and storage integrations in this package.
type Handler[U common_service_models.UserLike] struct {
	selectionPlanner      commonai.GenerationPlanner
	overrideAuthorizer    ports.InferenceModelOverrideAuthorizer
	retargetGates         ports.InferenceSessionRetargetGates
	persistenceHandler    persistence.CommonPersistenceInterface
	converter             common_converter.ConverterInterface
	configProvider        func() PolicyView
	logger                *zap.Logger
	eventPublisher        ports.ChatEventPublisher
	realtimeInvalidator   ports.ChatRealtimeAccessInvalidator
	notificationSink      ports.ChatNotificationSink
	permissionCallbacks   *ports.ChatPermissionCallbackConfig[U]
	attachmentHydrator    AttachmentHydrator
	attachmentCoordinator AttachmentCoordinator

	// agentRegistry / agentToolRegistry resolve registered agent types and tools
	// when creating/updating agent chat sessions. Nil until wired by the service
	// handler; agent-session methods reject when unset.
	agentRegistry     ports.AgentRegistry
	agentToolRegistry ports.AgentToolRegistry

	// permissionHandler authorizes site-admin tool-call resolution (Decision 6). Nil until wired by
	// the service handler after the permission handler is constructed; when nil, only the triggering
	// user and per-session channel admins can resolve.
	permissionHandler permission.PermissionHandlerInterface

	// organizationRoleResolver resolves a user's effective (lineage-aware) role in a channel's
	// owning organization for channel-owned org access. Nil until wired by the service handler;
	// when nil, org-derived read access never grants and SetChannelOrganizationAccess cannot
	// enable it.
	organizationRoleResolver ports.OrganizationRoleResolver

	// toolCallExecutor runs an approved/claimed TOOL_CALL. It is the pure persistence-free seam owned
	// by the runtime lane (E4). Nil until the runtime wires it; when nil, the approved-work scanner is
	// a no-op (approved rows stay claimable until an executor is available), so the approval/expiry
	// state machine still works standalone.
	toolCallExecutor ports.ToolCallExecutor
	// internalAttachmentStore externalizes binary approved-tool results before transcript limits
	// are applied. It is never exposed through public attachment APIs.
	internalAttachmentStore ports.InternalAttachmentStore

	// agentRunDriver drives a ChatAgentRun through the transcript-native Genkit continuation loop. It
	// is the runtime lane (E4) entrypoint CommonServiceHandler.RunAgentTurn, injected via
	// SetAgentRunDriver. The chat handler calls it at every production trigger site: after a fresh run
	// is created (initial message or a follow-up human message in an agent channel) and after an
	// approval/deny/expiry/approved-execution resume. Nil until wired; when nil, agent runs are never
	// driven (the feature is inert), but the transcript/approval state machine still works standalone.
	agentRunDriver func(ctx context.Context, runID uint64)
	// agentRunObserver is shared with the runtime lane so approval/recovery workers notify the app
	// when they win a durable lifecycle transition on this replica.
	agentRunObserver ports.AgentRunObserver

	// agentRunsCtx cancels detached run drives during service shutdown. The mutex closes the
	// WaitGroup Add/Wait race by refusing new drives before StopAgentRuns begins waiting.
	agentRunsMu       sync.Mutex
	agentRunsCtx      context.Context
	agentRunsCancel   context.CancelFunc
	agentRunsWg       sync.WaitGroup
	agentRunsStopping bool
	approvedRunsMu    sync.Mutex
	approvedRuns      map[uint64]struct{}

	// channelAccessCache caches per-channel access data (channel row without
	// roster + active bot user IDs); membershipCache caches per-(channel,user)
	// roles. Both are nil when RoleCacheCapacity is 0 (caching disabled).
	accessCacheMu       sync.RWMutex
	accessCacheTTL      time.Duration
	accessCacheCapacity uint64
	channelAccessCache  *ttlcache.Cache[uint64, channelAccessEntry]
	membershipCache     *ttlcache.Cache[channelMembershipKey, proto_models.ChannelRole]

	// Agentic tool-approval lane (Decision 6) background tasks: the pending-approval expiry sweep and
	// the approved-work backstop scanner. Started by the service handler after wiring; stopped via
	// StopApprovalBackgroundTasks. Nil ctx means not started.
	approvalTasksCtx    context.Context
	approvalTasksCancel context.CancelFunc
	approvalTasksWg     sync.WaitGroup
	// Owned approval transactions drain their post-commit hooks into detached runners. Admission is
	// serialized with shutdown so StopApprovalBackgroundTasks cannot race WaitGroup.Add.
	approvalPostCommitMu       sync.Mutex
	approvalPostCommitWg       sync.WaitGroup
	approvalPostCommitStopping bool
	// approvalExpirySweepMu serializes manual/background sweeps and protects the cross-tick cursor.
	// Keeping the cursor across bounded ticks prevents a permanently failing head page from starving
	// deeper overdue rows; reaching the tail clears it so failed rows are retried from the head.
	approvalExpirySweepMu      sync.Mutex
	approvalExpiryCursor       *persistence.ToolCallExpiryCursor
	approvalExpiryRetryBacklog bool

	// The global approved-work backstop dispatches through one handler-wide bounded pool. Scanner
	// ticks never wait for arbitrary tools, while a hung tool can occupy only one slot. Its limits
	// are the chat_tool_work_* keys (toolWorkPolicy); the runner override is nil in production and
	// exists as a deterministic test seam.
	toolWorkMu            sync.Mutex
	toolWorkCtx           context.Context
	toolWorkCancel        context.CancelFunc
	toolWorkScanSemaphore chan struct{}
	toolWorkScanWg        sync.WaitGroup
	toolWorkSemaphore     chan struct{}
	toolWorkOutstanding   chan struct{}
	toolWorkCompletion    chan struct{}
	toolWorkWg            sync.WaitGroup
	toolWorkStopping      bool
	toolWorkStopDone      chan struct{}
	toolWorkRunner        func(context.Context, *common_persistence_models.ChatMessage)

	// toolRunWorkCancels tracks approved/recovery tool executions by owning run on this replica.
	// The public run-cancellation control plane calls CancelAgentRunWork after winning the durable
	// status transition so cooperative executors stop promptly; persistence fencing remains the
	// authority for executors that return after cancellation or ignore their context.
	toolRunWorkMu      sync.Mutex
	toolRunWorkSeq     uint64
	toolRunWorkCancels map[uint64]map[uint64]context.CancelFunc
}

var _ HandlerInterface[common_service_models.User] = (*Handler[common_service_models.User])(nil)

func NewHandler[U common_service_models.UserLike](
	persistenceHandler persistence.CommonPersistenceInterface,
	converter common_converter.ConverterInterface,
	provider func() PolicyView,
	logger *zap.Logger,
) *Handler[U] {
	h := &Handler[U]{
		persistenceHandler: persistenceHandler,
		converter:          converter,
		configProvider:     provider,
		logger:             logger,
	}
	h.refreshAccessCaches(h.policy())
	return h
}

// AgentRunBudgetCeilings samples the work-budget ceilings from the hot chat configuration. Every
// run the chat service mints is stamped with them, and the composition root installs this method as
// the persistence ChatAgentRunBudgetCeilingsProvider for the runs persistence mints itself.
func (h *Handler[U]) AgentRunBudgetCeilings() persistence.ChatAgentRunBudgetCeilings {
	v := h.policy()
	return persistence.ChatAgentRunBudgetCeilings{MaxActiveDuration: v.ChatAgentRunMaxActiveDuration(), MaxGenerations: uint64(v.ChatAgentRunMaxGenerations()), MaxToolCalls: uint64(v.ChatAgentRunMaxToolCalls())}
}

func (h *Handler[U]) policy() PolicyView {
	if h.configProvider == nil {
		return defaultPolicy()
	}
	return h.configProvider()
}

func (h *Handler[U]) chatDisabled() bool { return !h.policy().EnableChat() }

// SetEventPublisher executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) SetEventPublisher(publisher ports.ChatEventPublisher) {
	h.eventPublisher = publisher
}

// publishChatEventAfterCommit hands the event built by newEvent to the chat event publisher once the
// ambient transaction commits, and immediately when ctx carries no transaction (decision D8 — the
// publish used to be dropped in that case). Every chat producer that publishes durable state goes
// through here; mid-stream ephemera (token deltas, typing) deliberately do not.
//
// Publishing is best-effort: a failure on the immediate path is logged, never returned into the
// caller's business flow. On the deferred path the error reaches the transaction's hook runner.
//
// Best-effort does not mean uninteresting. A failure here means committed chat state that no
// connected client was told about — a message that will not appear until something else triggers a
// refetch — and its causes are all server-side (a payload go-common could not serialize, a hub that
// is shutting down). It is logged at Error so it is visible without turning on Debug; the realtime
// publisher counts the same failure by cause.
//
// Parameters:
// - `ctx`: propagates request-scoped deadlines, cancellation, trace metadata, and the transaction.
// - `newEvent`: builds the event at publish time; a nil result skips the publish.
func (h *Handler[U]) publishChatEventAfterCommit(ctx context.Context, newEvent func(ctx context.Context) *ports.ChatEvent) {
	if h.eventPublisher == nil || newEvent == nil {
		return
	}
	if err := ports.PublishChatEventAfterCommit(ctx, h.eventPublisher, newEvent); err != nil && h.logger != nil {
		appLogger.For(ctx, h.logger, chatLoggerName).Error("failed to publish chat event",
			zap.String(logkeys.Stage, "publish_chat_event_after_commit"),
			zap.Error(err))
	}
}

// SupportsAttachments executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) SupportsAttachments() bool { return h.policy().EnableAttachments() }

// SetPermissionCallbacks executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) SetPermissionCallbacks(cfg *ports.ChatPermissionCallbackConfig[U]) {
	h.permissionCallbacks = cfg
}

// SetAgentRegistries wires the agent-type and agent-tool registries used to
// validate agent chat sessions.
func (h *Handler[U]) SetAgentRegistries(agents ports.AgentRegistry, tools ports.AgentToolRegistry) {
	h.agentRegistry = agents
	h.agentToolRegistry = tools
}

// SetInferenceSessionRetargetGates wires the operation registry's re-target gates reported on
// session summaries. Unset, every summary reports UNSPECIFIED (treated as NEVER).
func (h *Handler[U]) SetInferenceSessionRetargetGates(gates ports.InferenceSessionRetargetGates) {
	h.retargetGates = gates
}

// SetPermissionHandler wires the permission handler used to authorize site-admin tool-call
// resolution (Decision 6). Called by the service handler after the permission handler is built.
func (h *Handler[U]) SetPermissionHandler(handler permission.PermissionHandlerInterface) {
	h.permissionHandler = handler
}

// SetOrganizationRoleResolver wires the effective organization role lookup behind channel-owned
// org access (see SetChannelOrganizationAccess). Called by the service handler during wiring,
// before serving begins. A nil resolver is valid: org-derived read access then never grants, and
// enabling org access on a channel is rejected.
func (h *Handler[U]) SetOrganizationRoleResolver(resolver ports.OrganizationRoleResolver) {
	h.organizationRoleResolver = resolver
}

// SetToolCallExecutor wires the pure tool executor (runtime lane) used by the approved-work scanner
// to run claimed TOOL_CALLs. When unset the scanner is a no-op and approved rows remain claimable.
func (h *Handler[U]) SetToolCallExecutor(executor ports.ToolCallExecutor) {
	h.toolCallExecutor = executor
}

// SetInternalAttachmentStore wires the private attachment capability used by approved tool calls.
func (h *Handler[U]) SetInternalAttachmentStore(store ports.InternalAttachmentStore) {
	h.internalAttachmentStore = store
}

// SetAgentRunDriver injects the runtime lane's agent-run driver (CommonServiceHandler.RunAgentTurn).
// The chat handler invokes it at every production trigger site to drive a ChatAgentRun through the
// transcript-native Genkit continuation loop: after a fresh run is created (initial or follow-up
// human message in an agent channel) and after an approval/deny/expiry/approved-execution resume.
// It satisfies the agentRunDriverSink interface the service handler asserts at wiring time.
//
// Parameters:
// - `driver`: supplies the run driver invoked with a detached context and the run id to drive.
func (h *Handler[U]) SetAgentRunDriver(driver func(ctx context.Context, runID uint64)) {
	h.agentRunDriver = driver
}

// SetAgentRunObserver injects the app-owned post-commit lifecycle observer used by approval and
// recovery-worker transitions, including RETRYABLE and terminal states. It intentionally remains
// a capability method rather than expanding HandlerInterface.
func (h *Handler[U]) SetAgentRunObserver(observer ports.AgentRunObserver) {
	h.agentRunObserver = observer
}

// CancelAgentRunWork cooperatively cancels every approved/recovery tool execution for runID on
// this replica. It is intentionally a narrow capability method rather than part of HandlerInterface;
// CommonServiceHandler discovers it when first-class run cancellation is available.
func (h *Handler[U]) CancelAgentRunWork(runID uint64) bool {
	if h == nil || runID == 0 {
		return false
	}
	h.toolRunWorkMu.Lock()
	registered := h.toolRunWorkCancels[runID]
	delete(h.toolRunWorkCancels, runID)
	cancels := make([]context.CancelFunc, 0, len(registered))
	for _, cancel := range registered {
		cancels = append(cancels, cancel)
	}
	h.toolRunWorkMu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	return len(cancels) > 0
}

// SetAttachmentHydrator executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) SetAttachmentHydrator(hydrator AttachmentHydrator) {
	h.attachmentHydrator = hydrator
}

// SetAttachmentCoordinator executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) SetAttachmentCoordinator(coordinator AttachmentCoordinator) {
	h.attachmentCoordinator = coordinator
}

// hydrateMessageAttachments executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) hydrateMessageAttachments(ctx context.Context, message *common_service_models.ChatMessage) error {
	operationalConfig := h.policy()

	if message == nil {
		return nil
	}
	if len(message.AttachmentIDs) > 0 && h.attachmentHydrator != nil && operationalConfig.ChatSupportAttachments() {
		if err := h.attachmentHydrator.HydrateChatMessagesAttachments(ctx, []*common_service_models.ChatMessage{message}); err != nil {
			if errors.Is(err, persistence.ErrDatabaseError) {
				return serviceErrors.ErrDatabaseError
			}
			return serviceErrors.ErrInternalError
		}
	}
	h.hydrateToolResultMediaForDisplay(ctx, []*common_service_models.ChatMessage{message})
	return nil
}

// hydrateMessagesAttachments executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) hydrateMessagesAttachments(ctx context.Context, messages []*common_service_models.ChatMessage) error {
	operationalConfig := h.policy()

	if len(messages) == 0 {
		return nil
	}
	if h.attachmentHydrator != nil && operationalConfig.ChatSupportAttachments() {
		if err := h.attachmentHydrator.HydrateChatMessagesAttachments(ctx, messages); err != nil {
			if errors.Is(err, persistence.ErrDatabaseError) {
				return serviceErrors.ErrDatabaseError
			}
			return serviceErrors.ErrInternalError
		}
	}
	h.hydrateToolResultMediaForDisplay(ctx, messages)
	return nil
}

// hydrateToolResultMediaForDisplay projects private attachment-backed tool media into
// browser-consumable sources. Every source is resolved through the internal attachment store,
// which validates the bot owner and deterministic run/call/part binding. Resolution failures are
// omitted from the projection so a stale or spoofed reference cannot expose another object or
// make the surrounding chat unreadable.
func (h *Handler[U]) hydrateToolResultMediaForDisplay(ctx context.Context, messages []*common_service_models.ChatMessage) {
	if h.internalAttachmentStore == nil {
		return
	}
	for _, message := range messages {
		if message == nil || message.Kind != proto_models.MessageKind_MESSAGE_KIND_TOOL_RESULT ||
			message.ToolCall == nil || message.AgentRunID == nil || message.ToolCall.ToolCallID == nil ||
			message.ToolCall.PayloadType != proto_models.MessageModelPayloadType_MESSAGE_MODEL_PAYLOAD_TYPE_TOOL_RESULT {
			continue
		}
		var envelope proto_models.MessageModelPayload
		if err := proto.Unmarshal(message.ToolCall.ModelPayload, &envelope); err != nil {
			continue
		}
		result := envelope.GetToolResult()
		if result == nil || result.GetToolCallId() == "" || result.GetToolCallId() != *message.ToolCall.ToolCallID {
			continue
		}
		projected := proto.Clone(&envelope).(*proto_models.MessageModelPayload)
		projectedResult := projected.GetToolResult()
		changed := false
		for partIndex, part := range projectedResult.GetContentParts() {
			media := part.GetMedia()
			if media == nil || media.GetAttachmentId() == "" || media.GetSizeBytes() == 0 {
				continue
			}
			attachmentID, err := strconv.ParseUint(strings.TrimSpace(media.GetAttachmentId()), 10, 64)
			if err != nil || attachmentID == 0 {
				continue
			}
			resolved, err := attachmentpkg.ResolveToolMediaDisplayReference(
				ctx, h.internalAttachmentStore, attachmentID, uint64(message.SenderID), *message.AgentRunID,
				result.GetToolCallId(), partIndex, media.GetContentType(), media.GetName(), media.GetSizeBytes(),
			)
			if err != nil || resolved == nil || resolved.Source == "" || resolved.SizeBytes != media.GetSizeBytes() {
				continue
			}
			media.Source = &proto_models.MediaPart_Url{Url: resolved.Source}
			changed = true
		}
		if !changed {
			continue
		}
		payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(projected)
		if err != nil {
			continue
		}
		toolCallView := *message.ToolCall
		toolCallView.ModelPayload = payload
		message.ToolCall = &toolCallView
	}
}

// requireTransaction executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) requireTransaction(ctx context.Context) error {
	tx, ok := scontext.GetTransaction(ctx)
	if !ok || tx == nil {
		return serviceErrors.ErrTransactionNotFound
	}
	return nil
}

// getActor executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) getActor(ctx context.Context) (U, uint64, error) {
	requestingUser, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found || requestingUser == nil {
		var zero U
		return zero, 0, serviceErrors.ErrUserNotFound
	}
	return *requestingUser, uint64((*requestingUser).GetID()), nil
}

// isNilOrZero executes part of the chat package flow and provides shared helper behavior.
func isNilOrZero(id *uint64) bool {
	return id == nil || *id == 0
}
