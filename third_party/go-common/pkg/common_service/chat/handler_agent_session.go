package chat

import (
	"cmp"
	"context"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_json"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	commonai "github.com/Suhaibinator/go-common/pkg/common_service/ai"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/genai/inference"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// CreateMessageEmbedInput is the local input shape for an initial-message embed on agent-session
// creation.
type CreateMessageEmbedInput struct {
	EmbedType    uint32
	EmbedData    []byte
	FallbackText string
	Position     uint32
}

// AgentSessionOptions is the shared session identity, configuration and initial
// content used when creating a new channel or binding to an existing location.
// Channel creation, location and request-actor properties belong to the enclosing input.
type AgentSessionOptions struct {
	// PromptCustomization applies once at creation; nil uses the registered default.
	// The importing application authorizes custom instructions before calling this service.
	PromptCustomization *ports.AgentPromptCustomization

	ModelSelection *inference.ModelSelection
	// OrganizationID is the positive organization that owns the session and all model usage it
	// produces. The caller must authorize this scope; it is never inferred from ActorUserID.
	OrganizationID common_service_models.OrganizationIdType
	AgentType      string
	// UserID is the effective chat user; nil or zero defaults to the actor. Supplying another user
	// requires the consuming app to establish service authority to act for that user before calling.
	UserID *uint64
	// InferenceActorUserID names the human accountable for a workflow-owned session's inference.
	// Routing authorizes, revalidates and attributes model calls to this user instead of each run's
	// triggering user; the user does not join the channel, and tools still run as the triggering
	// user. It requires a workflow-owned agent and no ModelSelection, rejects bots and banned users,
	// is fixed at creation and is not re-applied to a reused session. Like UserID, the consuming
	// app must establish authority to act for this user before calling.
	InferenceActorUserID *uint64
	Metadata             map[string]string
	ToolSelection        ports.AgentSessionToolSelection
	Compaction           *ports.AgentCompactionRules
	ReasoningPolicy      *ports.AgentReasoningPolicy
	// AutoRunPolicy controls whether later human main-timeline messages automatically create runs.
	// Its zero value preserves the historical automatic behavior.
	AutoRunPolicy common_service_models.ChatAgentSessionAutoRunPolicy
	// MaxModelSteps overrides AgentDefinition.MaxModelSteps for this session. nil uses the agent
	// default; a non-nil zero requests no session limit, still bounded by the hot application ceiling.
	MaxModelSteps *uint32
	// InputMode and AllowUserStop override the fields of AgentDefinition.DefaultInteractionPolicy
	// for this session; nil uses the agent default. An unspecified or undefined mode is rejected
	// with ErrInvalidRequest.
	InputMode     *proto_models.ChatAgentInputMode
	AllowUserStop *bool
	// InitialRunBudget optionally overrides the durable budget captured on the initial run of a
	// workflow-owned agent. The override is partial: a non-zero field lowers the corresponding
	// global chat ceiling, and a zero field inherits it. nil applies the defaults unchanged.
	//
	// The request is rejected with ErrInvalidRequest when the agent is not workflow-owned (managed
	// sessions cannot override budgets), when MaxActiveDuration is negative or positive but shorter
	// than the one-millisecond persisted resolution, or when the budget is non-nil with every field
	// zero (an override that overrides nothing is treated as a caller bug, not as nil).
	//
	// The resolved limits are persisted on the run at creation, and resuming does not reset them.
	// The override participates in the create idempotency fingerprint exactly as given, so a
	// replay must repeat the same override.
	InitialRunBudget *ports.AgentRunBudget
	// DelegationOverride narrows a delegating agent's registered delegation for this session (see
	// ports.DelegationOverride). nil keeps the registered delegation. An override on an agent that
	// does not delegate, or one that widens, is rejected with ErrInvalidRequest. It participates in
	// the create idempotency fingerprint when set.
	DelegationOverride *ports.DelegationOverride
	InitialUserMessage *string
	InitialEmbeds      []CreateMessageEmbedInput
	IdempotencyKey     string
}

// CreateAgentChatSessionInput is the server-side input for creating an agent chat session. It is a
// Go service contract, not a public HTTP/proto API.
type CreateAgentChatSessionInput struct {
	AgentSessionOptions
	ActorUserID      uint64
	initialModelPlan *commonai.GenerationPlan
	routingAuthority ports.InferenceRoutingAuthority
	// metadataLimits replaces the default metadata size limits; a delegated child's come from the
	// hot chat_agent_delegation_metadata_* keys.
	metadataLimits agentSessionMetadataLimits
	// UnreadNotificationsEnabled overrides the new channel default (off when auto-run is disabled).
	// Reused channels retain their current setting.
	UnreadNotificationsEnabled *bool
	// OrganizationReadMinRole lets members of OrganizationID read the session's transcript without
	// being channel participants. Set it to the lowest organization role that may read: for
	// example OrganizationRole_ORGANIZATION_ROLE_MODERATOR_TIER_0 means "staff at MODERATOR_TIER_0
	// and above in the run's organization may read this transcript". Roles are effective roles, so
	// when organization role inheritance is enabled a qualifying role held in an ancestor
	// organization counts too, and losing the role revokes access as soon as the organization role
	// cache is invalidated.
	//
	// The zero value (UNSPECIFIED) takes the org_min_role_read_agent_transcript entry at
	// OrganizationID's hierarchy depth when the channel is created; when that entry is UNSPECIFIED
	// too, or no role resolver is wired, only participants can read. The grant is read-only and survives archive; readers are not added to the roster, do
	// not see the channel in ListChannels and receive no realtime events. It is stored on the
	// session's channel at creation (see SetChannelOrganizationAccess to change it later) and is
	// not re-applied when an existing session is reused or replayed. An undefined enum value, or a
	// non-zero value while organizations are not enabled (no role resolver is wired), is rejected
	// with ErrInvalidRequest. It participates in the create idempotency fingerprint exactly as
	// given (the configured default never does), and only when non-zero.
	OrganizationReadMinRole proto_models.OrganizationRole
	// ForkRules are stored on the session's new channel and cannot be changed afterwards. The zero
	// value disables forking. Like OrganizationReadMinRole they are not re-applied to a reused
	// channel, and they participate in the create idempotency fingerprint only when non-zero.
	ForkRules                    common_persistence_models.ChatChannelForkRules
	AdditionalParticipantUserIDs []uint64
	ContextID                    *uint64
	ContextDisplayName           *string
	ContextEntityID              *uint64
	GetOrCreateByContext         bool
}

// CreateAgentChatSessionResult is the result of creating (or getting) an agent chat session.
type CreateAgentChatSessionResult struct {
	Channel        *common_service_models.ChatChannel
	AgentSession   *common_service_models.ChatAgentSession
	InitialMessage *common_service_models.ChatMessage
	InitialRun     *common_service_models.ChatAgentRun
	// Reused is true when an existing session was returned instead of a freshly
	// created one: an idempotent-replay match on the create idempotency key, or a
	// GetOrCreateByContext reuse of the default scoped session (including the
	// reload after a scoped create race). It is false for a fresh create.
	Reused bool
}

const (
	maxAgentSessionMetadataKeys        = 32
	maxAgentSessionMetadataKeyBytes    = 128
	maxAgentSessionMetadataValueBytes  = 1024
	maxAgentSessionMetadataEncodedSize = 8 * 1024
	// maxAgentSessionCreateIdempotencyKeyBytes is the create_idempotency_key column width.
	maxAgentSessionCreateIdempotencyKeyBytes = 255
)

// agentSessionMetadataLimits bounds one metadata value and the encoded metadata. Zero fields use the
// session defaults (maxAgentSessionMetadataValueBytes, maxAgentSessionMetadataEncodedSize).
type agentSessionMetadataLimits struct {
	valueBytes int
	totalBytes int
}

func validAgentSessionAutoRunPolicy(policy common_service_models.ChatAgentSessionAutoRunPolicy) bool {
	return policy == common_service_models.ChatAgentSessionAutoRunPolicyAutomatic ||
		policy == common_service_models.ChatAgentSessionAutoRunPolicyDisabled
}

// GetAgentChatSessionInput is the input for fetching an agent chat session.
type GetAgentChatSessionInput struct {
	ActorUserID    uint64
	AgentSessionID uint64
}

// GetAgentChatSessionResult is the result of fetching an agent chat session.
type GetAgentChatSessionResult struct {
	AgentSession *common_service_models.ChatAgentSession
	Channel      *common_service_models.ChatChannel
}

// UpdateAgentChatSessionConfigInput is the input for updating an agent chat session's tool and/or
// compaction config. It produces a new append-only config snapshot.
type UpdateAgentChatSessionConfigInput struct {
	ActorUserID     uint64
	AgentSessionID  uint64
	ToolSelection   *ports.AgentSessionToolSelection
	Compaction      *ports.AgentCompactionRules
	ReasoningPolicy *ports.AgentReasoningPolicy
	// AutoRunPolicy changes whether later eligible human messages automatically create runs. nil
	// preserves the current policy. Like the other fields, it participates in the optimistic
	// config-version update so concurrent session configuration writes cannot silently overwrite it.
	AutoRunPolicy *common_service_models.ChatAgentSessionAutoRunPolicy
	// MaxModelSteps writes a new captured budget for future runs. nil preserves the current value;
	// a non-nil zero explicitly selects unlimited model generations.
	MaxModelSteps *uint32
	// InputMode and AllowUserStop change the session's interaction policy for runs started from the
	// new version on; nil preserves the current value. A run in flight keeps the policy it captured.
	// An unspecified or undefined mode is rejected with ErrInvalidRequest.
	InputMode     *proto_models.ChatAgentInputMode
	AllowUserStop *bool
	// DelegationOverride replaces the session's delegation override for runs started from the new
	// version on: a zero override removes it, and nil preserves the current one. It may only narrow
	// the registered delegation; a widening override, supplied or preserved, is rejected with
	// ErrInvalidRequest.
	DelegationOverride     *ports.DelegationOverride
	ExpectedCurrentVersion *uint64
}

// UpdateAgentChatSessionConfigResult is the result of updating a session config.
type UpdateAgentChatSessionConfigResult struct {
	AgentSession     *common_service_models.ChatAgentSession
	NewConfigVersion uint64
}

// CreateAgentChatSession creates a channel-bound agent chat session for a registered agent type. It
// is a server-side Go service method; the consuming app validates its own domain permissions and
// calls this directly. There is no public HTTP/proto API for agent-session lifecycle in v1.
// When UserID differs from the context actor, app-level service authority for acting on behalf of
// that effective user is a caller precondition; go-common still authorizes channel creation and
// scopes every reuse path to both the actor and effective user's channel membership. The caller
// must likewise establish the actor/effective user's authority in OrganizationID before calling;
// go-common requires and durably scopes the ID but this chat package has no organization policy
// callbacks with which to infer or authorize it.
//
// The whole operation runs in the caller-provided transaction: channel creation, the session row,
// the version-1 config snapshot, the optional initial message, and the first run all commit or roll
// back together, so a failed initial message never leaves an orphan session.
func (h *Handler[U]) CreateAgentChatSession(
	ctx context.Context,
	input CreateAgentChatSessionInput,
) (*CreateAgentChatSessionResult, error) {
	if h.chatDisabled() {
		return nil, serviceErrors.ErrChatDisabled
	}
	if input.OrganizationID <= 0 {
		return nil, serviceErrors.ErrInvalidRequest
	}
	if input.OrganizationReadMinRole != proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED &&
		!validOrganizationReadMinRole(input.OrganizationReadMinRole) {
		return nil, fmt.Errorf("%w: OrganizationReadMinRole must be a defined organization role", serviceErrors.ErrInvalidRequest)
	}
	if input.OrganizationReadMinRole != proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED && h.organizationRoleResolver == nil {
		// Without a resolver the access could never grant a read; fail loudly instead of silently.
		return nil, fmt.Errorf("%w: OrganizationReadMinRole requires organizations to be enabled", serviceErrors.ErrInvalidRequest)
	}
	if !input.ForkRules.Valid() {
		return nil, fmt.Errorf("%w: ForkRules must hold defined values", serviceErrors.ErrInvalidRequest)
	}
	if h.agentRegistry == nil || h.agentToolRegistry == nil {
		return nil, serviceErrors.ErrInternalError
	}
	if err := h.requireTransaction(ctx); err != nil {
		return nil, err
	}

	actor, actorID, err := h.resolveAgentSessionActor(ctx, input.ActorUserID)
	if err != nil {
		return nil, err
	}

	prepared, err := h.prepareAgentSessionCreate(input, actorID)
	if err != nil {
		return nil, err
	}
	if !prepared.agent.CanBeRoot() {
		return nil, fmt.Errorf("%w: agent type %q exists only as a delegation child", serviceErrors.ErrInvalidRequest, prepared.agent.Type)
	}
	input = prepared.input
	input.routingAuthority, err = h.captureInferenceAuthority()
	if err != nil {
		return nil, err
	}
	agent, tools := prepared.agent, prepared.tools
	metadataJSON := prepared.metadataJSON
	reasoningPolicy, maxModelSteps := prepared.reasoningPolicy, prepared.maxModelSteps
	effectiveUserID, hasInitialContent := prepared.effectiveUserID, prepared.hasInitialContent

	if err := h.prepareInitialModelSelection(ctx, &input, actorID); err != nil {
		return nil, err
	}
	participantIDs := buildAgentChannelParticipants(agent.BotUserID, input.AdditionalParticipantUserIDs, effectiveUserID, actorID)
	fingerprint := ""
	if hasInitialContent {
		fingerprint, err = fingerprintAgentSessionCreate(actorID, effectiveUserID, input, participantIDs, tools, reasoningPolicy, maxModelSteps)
		if err != nil {
			return nil, serviceErrors.ErrInternalError
		}
		existing, getErr := h.persistenceHandler.GetChatAgentSessionByCreateIdempotencyKey(
			ctx, int64(input.OrganizationID), actorID, effectiveUserID, input.AgentType, input.IdempotencyKey,
		)
		if getErr == nil && existing != nil {
			return h.replayAgentSessionCreate(ctx, actor, existing, fingerprint)
		}
		if getErr != nil && !errors.Is(getErr, persistence.ErrChatAgentSessionNotFound) {
			return nil, serviceErrors.ErrDatabaseError
		}
	}

	scopedContextID := uint64(0)
	scopedContextEntityID := uint64(0)

	// GetOrCreateByContext: return the existing default session for the exact scoped key only
	// when its channel is still usable by both the actor and the effective chat user.
	if input.GetOrCreateByContext {
		if input.ContextID == nil || *input.ContextID == 0 || input.ContextEntityID == nil || *input.ContextEntityID == 0 {
			return nil, serviceErrors.ErrInvalidRequest
		}
		scopedContextID = *input.ContextID
		scopedContextEntityID = *input.ContextEntityID
		existing, getErr := h.persistenceHandler.GetChatAgentSessionByScopedKey(
			ctx, int64(input.OrganizationID), input.AgentType, effectiveUserID, scopedContextID, scopedContextEntityID,
		)
		if getErr == nil && existing != nil {
			result, reusable, reuseErr := h.reuseScopedAgentSession(
				ctx, actor, actorID, effectiveUserID, existing, input.AutoRunPolicy, input.PromptCustomization,
			)
			if reuseErr != nil {
				return nil, reuseErr
			}
			if reusable {
				return result, nil
			}
			posted, archiveErr := h.persistenceHandler.ArchiveChatAgentSession(ctx, existing.ID)
			if archiveErr != nil {
				return nil, serviceErrors.ErrDatabaseError
			}
			h.PublishDeliveredAgentInputAfterCommit(ctx, existing.ID, existing.ChannelID, existing.ParentMessageID, posted.MessageIDs())
		}
		if getErr != nil && !errors.Is(getErr, persistence.ErrChatAgentSessionNotFound) {
			return nil, serviceErrors.ErrDatabaseError
		}
		// Not found: fall through to create the default session for this scoped key.
	}
	if err := h.resolveInitialModelSelection(ctx, &input, agent.Operation); err != nil {
		return nil, err
	}

	var rollbackScopedCreate func() error
	if input.GetOrCreateByContext {
		rollbackScopedCreate, err = h.startScopedAgentSessionCreateSavepoint(ctx)
		if err != nil {
			return nil, err
		}
	}

	// Create the channel with the effective user + agent bot + any additional participants.
	channel, err := h.createAgentChannel(ctx, actor, actorID, agent, input, participantIDs)
	if err != nil {
		return nil, err
	}

	// Write the session row.
	session := &common_persistence_models.ChatAgentSession{
		OrganizationID:       int64(input.OrganizationID),
		ChannelID:            channel.ID,
		AgentType:            input.AgentType,
		BotUserID:            uint64(agent.BotUserID),
		UserID:               effectiveUserID,
		CreatedByUserID:      actorID,
		InferenceActorUserID: input.InferenceActorUserID,
		ActiveConfigVersion:  1,
		Status:               common_persistence_models.ChatAgentSessionStatusActive,
		AutoRunPolicy:        common_persistence_models.ChatAgentSessionAutoRunPolicy(input.AutoRunPolicy),
		IsDefaultScoped:      input.GetOrCreateByContext,
		ContextID:            input.ContextID,
		ContextEntityID:      input.ContextEntityID,
		MetadataJSON:         metadataJSON,
	}
	if hasInitialContent {
		session.CreateIdempotencyKey = &input.IdempotencyKey
		session.CreateIdempotencyFingerprint = &fingerprint
	}
	session.ChatAgentSessionInferencePin = SessionInferencePin(input.initialModelPlan, actorID)
	if createErr := h.persistenceHandler.CreateChatAgentSession(ctx, session); createErr != nil {
		if errors.Is(createErr, persistence.ErrChatAgentSessionIdempotencyConflict) {
			return nil, serviceErrors.ErrChatAgentSessionIdempotencyConflict
		}
		if input.GetOrCreateByContext && errors.Is(createErr, persistence.ErrChatAgentSessionScopedConflict) {
			if rollbackScopedCreate != nil {
				if rollbackErr := rollbackScopedCreate(); rollbackErr != nil {
					return nil, serviceErrors.ErrDatabaseError
				}
			}
			return h.reloadScopedAgentSessionAfterCreateRace(
				ctx,
				actor,
				actorID,
				effectiveUserID,
				input.OrganizationID,
				input.AgentType,
				scopedContextID,
				scopedContextEntityID,
				input.AutoRunPolicy,
				input.PromptCustomization,
			)
		}
		return nil, serviceErrors.ErrDatabaseError
	}

	return h.finishAgentSessionCreate(ctx, channel, session, actorID, input, prepared)
}

func (h *Handler[U]) finishAgentSessionCreate(
	ctx context.Context, channel *common_persistence_models.ChatChannel,
	session *common_persistence_models.ChatAgentSession, actorID uint64,
	input CreateAgentChatSessionInput, prepared *preparedAgentSessionCreate,
) (*CreateAgentChatSessionResult, error) {
	agent, tools := prepared.agent, prepared.tools
	if err := h.finalizeAgentSessionPrompt(ctx, session, agent, input.PromptCustomization, input.Metadata); err != nil {
		return nil, err
	}
	reasoningPolicy, maxModelSteps := prepared.reasoningPolicy, prepared.maxModelSteps
	hasInitialContent := prepared.hasInitialContent
	// Write version-1 config snapshot: normalized rows are authoritative for the resolved tools;
	// the blob records the caller's selection intent + the agent's session-level approval policy.
	toolPayload, err := marshalToolSelectionPayload(input.ToolSelection, agent.ApprovalPolicy, reasoningPolicy, maxModelSteps)
	if err != nil {
		return nil, serviceErrors.ErrInternalError
	}
	if err := h.writeConfigSnapshot(ctx, session.ID, 1, actorID, tools, toolPayload, *input.Compaction, prepared.interactionPolicy, input.DelegationOverride); err != nil {
		return nil, err
	}

	result := &CreateAgentChatSessionResult{
		Channel:      h.converter.PersistenceChatChannelToService(channel),
		AgentSession: h.converter.PersistenceChatAgentSessionToService(session),
	}

	// Optional initial user message + first run.
	if hasInitialContent {
		message, run, err := h.writeInitialMessageAndRun(ctx, channel.ID, session, uint64(agent.BotUserID), input)
		if err != nil {
			return nil, err
		}
		initialMessageID := uint64(message.ID)
		initialRunID := uint64(run.ID)
		if err := h.persistenceHandler.SetChatAgentSessionInitialArtifacts(ctx, session.ID, initialMessageID, initialRunID); err != nil {
			return nil, serviceErrors.ErrDatabaseError
		}
		session.InitialMessageID = &initialMessageID
		session.InitialRunID = &initialRunID
		result.AgentSession = h.converter.PersistenceChatAgentSessionToService(session)
		result.InitialMessage = message
		result.InitialRun = run
	}

	return result, nil
}

// GetAgentChatSession fetches an agent chat session and its channel.
func (h *Handler[U]) GetAgentChatSession(
	ctx context.Context,
	input GetAgentChatSessionInput,
) (*GetAgentChatSessionResult, error) {
	if h.chatDisabled() {
		return nil, serviceErrors.ErrChatDisabled
	}
	if input.AgentSessionID == 0 {
		return nil, serviceErrors.ErrInvalidRequest
	}

	actor, _, err := h.resolveAgentSessionActor(ctx, input.ActorUserID)
	if err != nil {
		return nil, err
	}

	session, err := h.persistenceHandler.GetChatAgentSessionByID(ctx, input.AgentSessionID)
	if err != nil {
		if errors.Is(err, persistence.ErrChatAgentSessionNotFound) {
			return nil, serviceErrors.ErrChatAgentSessionNotFound
		}
		return nil, serviceErrors.ErrDatabaseError
	}

	channel, err := h.readChannelAuthorized(ctx, actor, session.ChannelID)
	if err != nil {
		return nil, err
	}

	return &GetAgentChatSessionResult{
		AgentSession: h.converter.PersistenceChatAgentSessionToService(session),
		Channel:      channel,
	}, nil
}

// UpdateAgentChatSessionConfig writes a new append-only config snapshot for a session and advances
// its active config version via optimistic concurrency. Updates affect future runs only; they do
// not rewrite history or change an in-flight run, which uses its captured config version.
func (h *Handler[U]) UpdateAgentChatSessionConfig(
	ctx context.Context,
	input UpdateAgentChatSessionConfigInput,
) (*UpdateAgentChatSessionConfigResult, error) {
	if h.chatDisabled() {
		return nil, serviceErrors.ErrChatDisabled
	}
	if h.agentRegistry == nil || h.agentToolRegistry == nil {
		return nil, serviceErrors.ErrInternalError
	}
	if input.AgentSessionID == 0 {
		return nil, serviceErrors.ErrInvalidRequest
	}
	// At least one field must change.
	if input.ToolSelection == nil && input.Compaction == nil && input.ReasoningPolicy == nil &&
		input.AutoRunPolicy == nil && input.MaxModelSteps == nil && input.InputMode == nil && input.AllowUserStop == nil &&
		input.DelegationOverride == nil {
		return nil, serviceErrors.ErrInvalidRequest
	}
	if input.AutoRunPolicy != nil && !validAgentSessionAutoRunPolicy(*input.AutoRunPolicy) {
		return nil, serviceErrors.ErrInvalidRequest
	}
	if input.InputMode != nil && !ports.ValidAgentInputMode(*input.InputMode) {
		return nil, serviceErrors.ErrInvalidRequest
	}
	if err := h.requireTransaction(ctx); err != nil {
		return nil, err
	}

	actor, actorID, err := h.resolveAgentSessionActor(ctx, input.ActorUserID)
	if err != nil {
		return nil, err
	}

	session, err := h.persistenceHandler.GetChatAgentSessionByID(ctx, input.AgentSessionID)
	if err != nil {
		if errors.Is(err, persistence.ErrChatAgentSessionNotFound) {
			return nil, serviceErrors.ErrChatAgentSessionNotFound
		}
		return nil, serviceErrors.ErrDatabaseError
	}

	channel, err := h.requireChannelMember(ctx, session.ChannelID, actor.GetID())
	if err != nil {
		return nil, err
	}
	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation: ports.ChatPermissionUpdateChannel,
		Actor:     actor,
		Channel:   channel,
	}); err != nil {
		return nil, err
	}

	agent, ok := h.agentRegistry.GetAgentType(session.AgentType)
	if !ok {
		return nil, serviceErrors.ErrInvalidRequest
	}

	// Load the current active snapshot so omitted fields are preserved into a full,
	// self-contained new snapshot.
	curSnapshot, curTools, err := h.persistenceHandler.GetChatAgentSessionConfigSnapshot(ctx, session.ID, session.ActiveConfigVersion)
	if err != nil {
		return nil, serviceErrors.ErrDatabaseError
	}

	// Resolve the effective tools and the selection-intent blob: a supplied selection is re-resolved
	// (rows) and re-serialized (intent blob); an omitted selection preserves the current snapshot's
	// normalized rows AND its prior intent blob, so nothing drifts.
	var tools []resolvedToolConfig
	var toolPayload []byte
	reasoningPolicy, err := DecodeReasoningPolicy(curSnapshot.ToolConfigPayload)
	if err != nil {
		return nil, serviceErrors.ErrInternalError
	}
	if input.ReasoningPolicy != nil {
		reasoningPolicy = *input.ReasoningPolicy
	}
	maxModelSteps, err := DecodeMaxModelSteps(curSnapshot.ToolConfigPayload, agent.MaxModelSteps)
	if err != nil {
		return nil, serviceErrors.ErrInternalError
	}
	if input.MaxModelSteps != nil {
		maxModelSteps = *input.MaxModelSteps
	}
	if input.ToolSelection != nil {
		tools, err = h.resolveToolSelection(agent, *input.ToolSelection, h.agentToolRegistry)
		if err != nil {
			return nil, err
		}
		toolPayload, err = marshalToolSelectionPayload(*input.ToolSelection, agent.ApprovalPolicy, reasoningPolicy, maxModelSteps)
		if err != nil {
			return nil, serviceErrors.ErrInternalError
		}
	} else {
		tools = toolConfigsFromNormalizedRows(curTools)
		toolPayload = curSnapshot.ToolConfigPayload
		if input.ReasoningPolicy != nil {
			toolPayload, err = replaceReasoningPolicyInToolSelectionPayload(toolPayload, reasoningPolicy)
			if err != nil {
				return nil, serviceErrors.ErrInternalError
			}
		}
		if input.MaxModelSteps != nil {
			toolPayload, err = replaceMaxModelStepsInToolSelectionPayload(toolPayload, maxModelSteps)
			if err != nil {
				return nil, serviceErrors.ErrInternalError
			}
		}
	}

	// Resolve the effective compaction: supplied rules, or the current snapshot's rules.
	var compaction ports.AgentCompactionRules
	if input.Compaction != nil {
		compaction = *input.Compaction
	} else {
		compaction, err = ports.DecodeCompactionPayload(curSnapshot.CompactionPayload)
		if err != nil {
			return nil, serviceErrors.ErrInternalError
		}
	}

	if err := ports.ValidateCompactionRules(compaction); err != nil {
		return nil, serviceErrors.ErrInvalidRequest
	}
	// Resolve the effective interaction policy: each supplied override, else the current value.
	interaction := ports.ResolveInteractionPolicy(ports.AgentInteractionPolicy(curSnapshot.ChatAgentInteractionPolicy), input.InputMode, input.AllowUserStop)
	// Resolve the delegation override: the supplied one, else the current snapshot's. Either is
	// validated against the current registration, so a carried override the registration has since
	// narrowed below is rejected rather than copied forward; a zero override clears it.
	delegationOverride := input.DelegationOverride
	if delegationOverride == nil {
		if delegationOverride, err = ports.DecodeDelegationOverride(curSnapshot.DelegationOverridePayload); err != nil {
			return nil, serviceErrors.ErrInternalError
		}
	}
	if delegationOverride != nil {
		if err := validateDelegationOverride(agent, delegationOverride); err != nil {
			return nil, err
		}
	}

	// Optimistic concurrency: require the expected current version when supplied; otherwise use the
	// session's current active version so the CAS still guards a concurrent writer.
	expected := session.ActiveConfigVersion
	if input.ExpectedCurrentVersion != nil {
		expected = *input.ExpectedCurrentVersion
	}
	newVersion := expected + 1

	// Disabling automatic runs posts the session's queued input, which locks the channel before the
	// session, so it goes before the version bump writes the session row.
	if input.AutoRunPolicy != nil {
		if err := h.persistAgentSessionAutoRunPolicy(ctx, session, *input.AutoRunPolicy); err != nil {
			return nil, err
		}
	}
	if err := h.persistenceHandler.BumpChatAgentSessionActiveConfigVersion(ctx, session.ID, expected, newVersion); err != nil {
		if errors.Is(err, persistence.ErrChatAgentSessionVersionConflict) {
			return nil, serviceErrors.ErrChatAgentSessionVersionConflict
		}
		return nil, serviceErrors.ErrDatabaseError
	}

	if err := h.writeConfigSnapshot(ctx, session.ID, newVersion, actorID, tools, toolPayload, compaction, interaction, delegationOverride); err != nil {
		return nil, err
	}

	// A changed automatic-run policy announced the interaction with the input it posted.
	if input.AutoRunPolicy == nil && (input.InputMode != nil || input.AllowUserStop != nil) {
		h.PublishAgentSessionInteractionAfterCommit(ctx, session.ID, session.ChannelID, session.ParentMessageID)
	}
	session.ActiveConfigVersion = newVersion
	return &UpdateAgentChatSessionConfigResult{
		AgentSession:     h.converter.PersistenceChatAgentSessionToService(session),
		NewConfigVersion: newVersion,
	}, nil
}

// writeConfigSnapshot persists one append-only config snapshot version plus its normalized
// tool-config rows. The normalized rows (built from resolvedTools) are AUTHORITATIVE for per-tool
// enable/approval/renderer_key; toolPayload is the caller's selection-intent blob (provenance only).
// interaction is the already-resolved effective policy stored in the snapshot's typed columns.
func (h *Handler[U]) writeConfigSnapshot(
	ctx context.Context,
	sessionID uint64,
	version uint64,
	actorID uint64,
	resolvedTools []resolvedToolConfig,
	toolPayload []byte,
	compaction ports.AgentCompactionRules,
	interaction ports.AgentInteractionPolicy,
	delegationOverride *ports.DelegationOverride,
) error {
	compactionPayload, err := ports.EncodeCompactionPayload(compaction)
	if err != nil {
		return serviceErrors.ErrInternalError
	}
	delegationPayload, err := ports.EncodeDelegationOverride(delegationOverride)
	if err != nil {
		return serviceErrors.ErrInternalError
	}
	snapshot := &common_persistence_models.ChatAgentSessionConfigSnapshot{
		AgentSessionID:             sessionID,
		Version:                    version,
		ToolConfigPayload:          toolPayload,
		CompactionPayload:          compactionPayload,
		DelegationOverridePayload:  delegationPayload,
		ChatAgentInteractionPolicy: common_persistence_models.ChatAgentInteractionPolicy(interaction),
		CreatedByUserID:            actorID,
	}
	rows := toNormalizedToolConfigRows(resolvedTools)
	if err := h.persistenceHandler.InsertChatAgentSessionConfigSnapshot(ctx, snapshot, rows); err != nil {
		return serviceErrors.ErrDatabaseError
	}
	return nil
}

// writeInitialMessageAndRun writes the initial STANDARD user message through the normal send path
// with an explicit USER role (even for a bot actor), and creates the first ChatAgentRun pinning
// the session's active config version.
func (h *Handler[U]) writeInitialMessageAndRun(
	ctx context.Context,
	channelID uint64,
	session *common_persistence_models.ChatAgentSession,
	botUserID uint64,
	input CreateAgentChatSessionInput,
) (*common_service_models.ChatMessage, *common_service_models.ChatAgentRun, error) {
	content := ""
	if input.InitialUserMessage != nil {
		content = *input.InitialUserMessage
	}
	embeds := make([]persistence.CreateChatMessageEmbed, 0, len(input.InitialEmbeds))
	for _, e := range input.InitialEmbeds {
		embeds = append(embeds, persistence.CreateChatMessageEmbed{
			EmbedType:    e.EmbedType,
			EmbedData:    e.EmbedData,
			FallbackText: e.FallbackText,
			Position:     e.Position,
		})
	}

	// Suppress the generic send path's follow-up auto-run trigger: this initial message mints its
	// first run transactionally right below, so the post-commit auto-trigger must not create a second
	// run for the same message.
	sent, err := h.sendMessageWithContentType(
		ctx,
		SendMessageInput{
			ChannelID:       channelID,
			Content:         content,
			ParentMessageID: session.ParentMessageID,
			Embeds:          embeds,
		},
		proto_models.MessageContentType_MESSAGE_CONTENT_TYPE_TEXT,
		proto_models.MessageStreamState_MESSAGE_STREAM_STATE_COMPLETE,
		proto_models.MessageRole_MESSAGE_ROLE_USER,
		sendOptions{suppressAgentAutoRun: true},
	)
	if err != nil {
		return nil, nil, err
	}
	message := sent.message

	// Mint the first run, capturing the session's active config version atomically.
	triggeringUserID := session.UserID
	triggeringMessageID := uint64(message.ID)
	run := &common_persistence_models.ChatAgentRun{
		OrganizationID:      session.OrganizationID,
		ChannelID:           channelID,
		ParentMessageID:     session.ParentMessageID,
		AgentSessionID:      &session.ID,
		BotUserID:           botUserID,
		TriggeringUserID:    &triggeringUserID,
		TriggeringMessageID: &triggeringMessageID,
		Status:              proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING,
	}
	if input.ModelSelection != nil {
		plan := input.initialModelPlan
		if plan == nil {
			return nil, nil, serviceErrors.ErrAgentOperationRouteUnresolved
		}
		run.RequestedInferenceModelID = new(input.ModelSelection.InferenceModelID)
		run.RequestedInferenceAPI = input.ModelSelection.Clone().API
		run.ExecutionInferenceModelID = new(plan.InferenceModelID)
		run.ExecutionInferenceAPI = new(plan.API)
		run.ExecutionProviderID = new(plan.ProviderID)
		run.ExecutionProviderModelID = new(plan.ProviderModelID)
	}
	h.AgentRunBudgetCeilings().ApplyTo(run)
	applyInitialRunBudgetOverride(run, input.InitialRunBudget)
	if err := h.persistenceHandler.CreateChatAgentRunCapturingConfig(ctx, run); err != nil {
		if errors.Is(err, persistence.ErrChatAgentSessionRunActive) || errors.Is(err, persistence.ErrChatAgentSessionSuspended) {
			return nil, nil, serviceErrors.ErrChatAgentSessionBusy
		}
		return nil, nil, serviceErrors.ErrDatabaseError
	}

	// Drive the fresh run through the agent loop post-commit (after the session + run + message are
	// durable). No-op when the runtime driver is not wired.
	h.startAgentRunAfterCommit(ctx, session.AgentType, run.ID, run.ChannelID, run.ParentMessageID)

	return message, h.converter.PersistenceChatAgentRunToService(run), nil
}

// resolveAgentSessionActor requires a real context actor and treats an explicit ActorUserID only as
// an assertion about that actor. Agent-session lifecycle calls are server-side, but they must not be
// able to impersonate an arbitrary actor by passing an id in the input struct.
func (h *Handler[U]) resolveAgentSessionActor(ctx context.Context, actorUserID uint64) (U, uint64, error) {
	actor, resolved, err := h.getActor(ctx)
	if err != nil {
		var zero U
		return zero, 0, err
	}
	if actorUserID != 0 && actorUserID != resolved {
		var zero U
		return zero, 0, serviceErrors.ErrInsufficientPermissions
	}
	return actor, resolved, nil
}

// buildAgentChannelParticipants assembles the channel participant list (excluding the actor, who is
// implicitly the creator): the agent bot user plus any additional participants and the effective
// chat user when distinct from the actor.
func buildAgentChannelParticipants(botUserID common_service_models.UserIdType, additional []uint64, effectiveUserID, actorID uint64) []uint64 {
	seen := make(map[uint64]struct{})
	out := make([]uint64, 0, len(additional)+2)
	add := func(id uint64) {
		if id == 0 || id == actorID {
			return
		}
		if _, dup := seen[id]; dup {
			return
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	add(uint64(botUserID))
	add(effectiveUserID)
	for _, id := range additional {
		add(id)
	}
	return out
}

func sanitizeAgentSessionMetadata(input map[string]string, limits agentSessionMetadataLimits) (map[string]string, []byte, error) {
	if len(input) == 0 {
		return nil, nil, nil
	}
	valueBytes := cmp.Or(limits.valueBytes, maxAgentSessionMetadataValueBytes)
	totalBytes := cmp.Or(limits.totalBytes, maxAgentSessionMetadataEncodedSize)
	if len(input) > maxAgentSessionMetadataKeys {
		return nil, nil, errors.New("too many metadata keys")
	}
	metadata := make(map[string]string, len(input))
	for rawKey, value := range input {
		if !utf8.ValidString(rawKey) || !utf8.ValidString(value) {
			return nil, nil, errors.New("metadata must be valid utf-8")
		}
		key := strings.TrimSpace(rawKey)
		if key == "" {
			return nil, nil, errors.New("metadata key must not be empty")
		}
		if len(key) > maxAgentSessionMetadataKeyBytes {
			return nil, nil, errors.New("metadata key too large")
		}
		if len(value) > valueBytes {
			return nil, nil, errors.New("metadata value too large")
		}
		if _, exists := metadata[key]; exists {
			return nil, nil, errors.New("duplicate metadata key")
		}
		metadata[key] = value
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return nil, nil, err
	}
	if len(encoded) > totalBytes {
		return nil, nil, errors.New("metadata payload too large")
	}
	return common_util.CloneMapOrNil(metadata), encoded, nil
}

// createAgentChannel creates the group channel that hosts the agent session.
func (h *Handler[U]) createAgentChannel(
	ctx context.Context,
	actor U,
	actorID uint64,
	agent ports.AgentDefinition,
	input CreateAgentChatSessionInput,
	participantIDs []uint64,
) (*common_persistence_models.ChatChannel, error) {
	if maxParticipants := h.policy().ChatMaxParticipantsPerGroup(); maxParticipants > 0 && len(participantIDs)+1 > maxParticipants {
		return nil, serviceErrors.ErrInvalidRequest
	}
	unreadEnabled := input.AutoRunPolicy != common_service_models.ChatAgentSessionAutoRunPolicyDisabled
	if input.UnreadNotificationsEnabled != nil {
		unreadEnabled = *input.UnreadNotificationsEnabled
	}
	name := strings.TrimSpace(agent.DisplayName)
	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation:                          ports.ChatPermissionCreateChannel,
		Actor:                              actor,
		ChannelType:                        proto_models.ChannelType_CHANNEL_TYPE_GROUP,
		ParticipantUserIDs:                 participantIDs,
		ProposedUnreadNotificationsEnabled: &unreadEnabled,
		ProposedChannelName:                &name,
		ProposedContextID:                  input.ContextID,
		ProposedContextDisplayName:         input.ContextDisplayName,
		ProposedContextEntityID:            input.ContextEntityID,
	}); err != nil {
		return nil, err
	}
	features, err := h.newChannelFeatures(ctx, ports.ChatChannelCreation[U]{
		Operation:          ports.ChatPermissionCreateChannel,
		Actor:              actor,
		ChannelType:        proto_models.ChannelType_CHANNEL_TYPE_GROUP,
		ParticipantUserIDs: participantIDs,
		ContextID:          input.ContextID,
		ContextDisplayName: input.ContextDisplayName,
		ContextEntityID:    input.ContextEntityID,
		AgentType:          agent.Type,
	})
	if err != nil {
		return nil, err
	}
	flags := common_persistence_models.ChatChannelFeatureFlags{
		UnreadNotificationsEnabled: &unreadEnabled,
		Features:                   features,
		ForkRules:                  input.ForkRules,
	}
	created, err := h.persistenceHandler.CreateChatChannel(
		ctx,
		actorID,
		name,
		nil,
		input.ContextID,
		input.ContextDisplayName,
		input.ContextEntityID,
		proto_models.ChannelType_CHANNEL_TYPE_GROUP,
		participantIDs,
		flags,
	)
	if err != nil {
		if errors.Is(err, persistence.ErrDatabaseError) {
			return nil, serviceErrors.ErrDatabaseError
		}
		return nil, serviceErrors.ErrInternalError
	}
	// The default applies only to the channel created here, never to the fingerprint, so a
	// configuration change cannot turn an idempotent replay into a conflict.
	readMinRole, err := h.organizationReadMinRoleOrDefault(ctx, h.policy().OrgMinRoleReadAgentTranscript(), input.OrganizationID, input.OrganizationReadMinRole)
	if err != nil {
		return nil, err
	}
	if readMinRole != proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED {
		// The channel owns its org-derived read access. It is written in the same transaction as
		// the channel, so a failed session create never leaves a readable orphan.
		if err := h.grantNewChannelOrganizationRead(ctx, created, input.OrganizationID, readMinRole); err != nil {
			return nil, err
		}
	}
	return created, nil
}

func (h *Handler[U]) readChannelAuthorized(
	ctx context.Context,
	actor U,
	channelID uint64,
) (*common_service_models.ChatChannel, error) {
	channel, err := h.requireChannelRead(ctx, channelID, actor)
	if err != nil {
		return nil, err
	}
	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation: ports.ChatPermissionGetChannel,
		Actor:     actor,
		Channel:   channel,
	}); err != nil {
		return nil, err
	}
	return channel, nil
}

// persistAgentSessionAutoRunPolicy reconciles the mutable session-row policy. Callers already
// require an ambient transaction, so a config version/snapshot update and this row update commit or
// roll back together. Disabling automatic runs posts the session's queued input as messages, which
// are announced after commit with the session's interaction change.
func (h *Handler[U]) persistAgentSessionAutoRunPolicy(
	ctx context.Context,
	session *common_persistence_models.ChatAgentSession,
	requested common_service_models.ChatAgentSessionAutoRunPolicy,
) error {
	if session == nil || !validAgentSessionAutoRunPolicy(requested) {
		return serviceErrors.ErrInvalidRequest
	}
	persisted := common_persistence_models.ChatAgentSessionAutoRunPolicy(requested)
	posted, err := h.persistenceHandler.UpdateChatAgentSessionAutoRunPolicy(ctx, session.ID, persisted)
	if err != nil {
		if errors.Is(err, persistence.ErrChatAgentSessionNotFound) {
			return serviceErrors.ErrChatAgentSessionNotFound
		}
		return serviceErrors.ErrDatabaseError
	}
	session.AutoRunPolicy = persisted
	h.PublishDeliveredAgentInputAfterCommit(ctx, session.ID, session.ChannelID, session.ParentMessageID, posted.MessageIDs())
	return nil
}

func (h *Handler[U]) reuseScopedAgentSession(
	ctx context.Context,
	actor U,
	actorID uint64,
	effectiveUserID uint64,
	session *common_persistence_models.ChatAgentSession,
	requestedAutoRunPolicy common_service_models.ChatAgentSessionAutoRunPolicy,
	requestedPrompt *ports.AgentPromptCustomization,
) (*CreateAgentChatSessionResult, bool, error) {
	if session == nil {
		return nil, false, nil
	}
	// Reuse needs a channel the actor can still use, which the read gate does not establish: it
	// admits archived channels and organization readers. Require live participation instead, so an
	// archived or left channel stays a stale session that is retired and recreated.
	channel, err := h.requireChannelMember(ctx, session.ChannelID, common_service_models.UserIdType(actorID))
	if err != nil {
		if isStaleScopedAgentSessionError(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation: ports.ChatPermissionGetChannel,
		Actor:     actor,
		Channel:   channel,
	}); err != nil {
		return nil, false, err
	}
	if effectiveUserID != actorID {
		if _, err := h.requireChannelMember(ctx, session.ChannelID, common_service_models.UserIdType(effectiveUserID)); err != nil {
			if isStaleScopedAgentSessionError(err) {
				return nil, false, nil
			}
			return nil, false, err
		}
	}
	if !matchesAgentPromptCustomization(session.PromptCustomizationJSON, requestedPrompt) {
		return nil, false, serviceErrors.ErrChatAgentSessionIdempotencyConflict
	}
	// Create's value-typed zero is both the historical default and an omitted field. Do not let an
	// old caller accidentally re-enable a reusable Disabled session. Disabled is non-zero and may
	// be applied here; explicit re-enabling uses UpdateAgentChatSessionConfig's pointer field. A
	// session already Disabled has nothing to change.
	if requestedAutoRunPolicy == common_service_models.ChatAgentSessionAutoRunPolicyDisabled &&
		session.AutoRunPolicy != common_persistence_models.ChatAgentSessionAutoRunPolicyDisabled {
		if err := h.persistAgentSessionAutoRunPolicy(ctx, session, requestedAutoRunPolicy); err != nil {
			return nil, false, err
		}
	}
	return &CreateAgentChatSessionResult{
		Channel:      channel,
		AgentSession: h.converter.PersistenceChatAgentSessionToService(session),
		Reused:       true,
	}, true, nil
}

func isStaleScopedAgentSessionError(err error) bool {
	return errors.Is(err, serviceErrors.ErrChatNotMember) ||
		errors.Is(err, serviceErrors.ErrChatChannelArchived) ||
		errors.Is(err, serviceErrors.ErrChatChannelNotFound)
}

func (h *Handler[U]) startScopedAgentSessionCreateSavepoint(ctx context.Context) (func() error, error) {
	tx, ok := scontext.GetTransaction(ctx)
	if !ok || tx == nil || tx.GetDB() == nil {
		return nil, nil
	}
	const savepoint = "chat_agent_session_create"
	if err := tx.SavePoint(savepoint); err != nil {
		return nil, serviceErrors.ErrDatabaseError
	}
	return func() error {
		return tx.RollbackTo(savepoint)
	}, nil
}

func (h *Handler[U]) reloadScopedAgentSessionAfterCreateRace(
	ctx context.Context,
	actor U,
	actorID uint64,
	effectiveUserID uint64,
	organizationID common_service_models.OrganizationIdType,
	agentType string,
	contextID uint64,
	contextEntityID uint64,
	requestedAutoRunPolicy common_service_models.ChatAgentSessionAutoRunPolicy,
	requestedPrompt *ports.AgentPromptCustomization,
) (*CreateAgentChatSessionResult, error) {
	existing, err := h.persistenceHandler.GetChatAgentSessionByScopedKey(ctx, int64(organizationID), agentType, effectiveUserID, contextID, contextEntityID)
	if err != nil {
		return nil, serviceErrors.ErrDatabaseError
	}
	result, reusable, err := h.reuseScopedAgentSession(
		ctx, actor, actorID, effectiveUserID, existing, requestedAutoRunPolicy, requestedPrompt,
	)
	if err != nil {
		return nil, err
	}
	if !reusable {
		return nil, serviceErrors.ErrDatabaseError
	}
	return result, nil
}

func (h *Handler[U]) replayAgentSessionCreate(
	ctx context.Context,
	actor U,
	session *common_persistence_models.ChatAgentSession,
	fingerprint string,
) (*CreateAgentChatSessionResult, error) {
	if session.CreateIdempotencyFingerprint == nil || *session.CreateIdempotencyFingerprint != fingerprint {
		return nil, serviceErrors.ErrChatAgentSessionIdempotencyConflict
	}
	return h.agentSessionCreateResult(ctx, actor, session)
}

// agentSessionCreateResult is the replay result of an already created session: its channel, read as
// actor, and its initial message and run.
func (h *Handler[U]) agentSessionCreateResult(
	ctx context.Context,
	actor U,
	session *common_persistence_models.ChatAgentSession,
) (*CreateAgentChatSessionResult, error) {
	channel, err := h.readChannelAuthorized(ctx, actor, session.ChannelID)
	if err != nil {
		return nil, err
	}
	result := &CreateAgentChatSessionResult{
		Channel:      channel,
		AgentSession: h.converter.PersistenceChatAgentSessionToService(session),
		Reused:       true,
	}
	if session.InitialMessageID != nil && *session.InitialMessageID != 0 {
		msg, err := h.persistenceHandler.GetChatMessageByIDWithOptions(ctx, *session.InitialMessageID, persistence.ChatMessageReadOptions{
			VisibilityMode: persistence.ChatMessageVisibilityServiceTranscript,
		})
		if err != nil {
			return nil, mapChatMessagePersistenceError(err)
		}
		result.InitialMessage = h.converter.PersistenceChatMessageToService(msg)
	}
	if session.InitialRunID != nil && *session.InitialRunID != 0 {
		run, err := h.persistenceHandler.GetChatAgentRunByID(ctx, *session.InitialRunID)
		if err != nil {
			if errors.Is(err, persistence.ErrChatAgentRunNotFound) {
				return nil, serviceErrors.ErrChatAgentRunNotFound
			}
			return nil, serviceErrors.ErrDatabaseError
		}
		result.InitialRun = h.converter.PersistenceChatAgentRunToService(run)
	}
	return result, nil
}

type agentSessionCreateFingerprint struct {
	UnreadNotificationsEnabled *bool                                               `json:"unread_notifications_enabled,omitempty"`
	PromptCustomization        *ports.AgentPromptCustomization                     `json:"prompt_customization,omitempty"`
	ModelSelection             *inference.ModelSelection                           `json:"model_selection,omitempty"`
	InitialRunBudget           *agentSessionFingerprintBudget                      `json:"initial_run_budget,omitempty"`
	OrganizationID             common_service_models.OrganizationIdType            `json:"organization_id"`
	ActorUserID                uint64                                              `json:"actor_user_id"`
	UserID                     uint64                                              `json:"user_id"`
	AgentType                  string                                              `json:"agent_type"`
	ParticipantUserID          []uint64                                            `json:"participant_user_ids"`
	ContextID                  *uint64                                             `json:"context_id,omitempty"`
	ContextDisplay             *string                                             `json:"context_display_name,omitempty"`
	ContextEntityID            *uint64                                             `json:"context_entity_id,omitempty"`
	Metadata                   map[string]string                                   `json:"metadata,omitempty"`
	GetOrCreate                bool                                                `json:"get_or_create_by_context"`
	ToolSelection              agentSessionFingerprintChoice                       `json:"tool_selection"`
	Tools                      []agentSessionFingerprintTool                       `json:"tools"`
	Compaction                 ports.AgentCompactionRules                          `json:"compaction"`
	ReasoningPolicy            ports.AgentReasoningPolicy                          `json:"reasoning_policy"`
	MaxModelSteps              uint32                                              `json:"max_model_steps"`
	AutoRunPolicy              common_service_models.ChatAgentSessionAutoRunPolicy `json:"auto_run_policy,omitempty"`
	InitialMessage             string                                              `json:"initial_message"`
	InitialEmbeds              []agentSessionFingerprintEmbed                      `json:"initial_embeds"`
	// OrganizationReadMinRole is a pointer so an unset (UNSPECIFIED) role is omitted under every
	// JSON omitempty dialect, keeping fingerprints of sessions created before the field unchanged.
	OrganizationReadMinRole *int32 `json:"organization_read_min_role,omitempty"`
	// InferenceActorUserID is omitted when unset, keeping earlier fingerprints unchanged.
	InferenceActorUserID *uint64 `json:"inference_actor_user_id,omitempty"`
	// InputMode and AllowUserStop are the caller's overrides, omitted when unset for the same reason.
	InputMode     *int32 `json:"input_mode,omitempty"`
	AllowUserStop *bool  `json:"allow_user_stop,omitempty"`
	// DelegationOverride is omitted when unset for the same reason.
	DelegationOverride *ports.DelegationOverride `json:"delegation_override,omitempty"`
	// ForkRules is omitted when zero for the same reason.
	ForkRules *common_persistence_models.ChatChannelForkRules `json:"fork_rules,omitempty"`
}

type agentSessionFingerprintChoice struct {
	Mode  uint8                                  `json:"mode"`
	Tools []agentSessionFingerprintRequestedTool `json:"tools,omitempty"`
}

type agentSessionFingerprintRequestedTool struct {
	ToolName         string `json:"tool_name"`
	Enabled          bool   `json:"enabled"`
	RequiresApproval *bool  `json:"requires_approval,omitempty"`
}

type agentSessionFingerprintTool struct {
	ToolName         string `json:"tool_name"`
	Enabled          bool   `json:"enabled"`
	RequiresApproval bool   `json:"requires_approval"`
	RendererKey      string `json:"renderer_key,omitempty"`
}

type agentSessionFingerprintEmbed struct {
	EmbedType    uint32 `json:"embed_type"`
	EmbedData    []byte `json:"embed_data"`
	FallbackText string `json:"fallback_text"`
	Position     uint32 `json:"position"`
}

func fingerprintAgentSessionCreate(
	actorID uint64,
	effectiveUserID uint64,
	input CreateAgentChatSessionInput,
	participantIDs []uint64,
	tools []resolvedToolConfig,
	reasoningPolicy ports.AgentReasoningPolicy,
	maxModelSteps ...uint32,
) (string, error) {
	effectiveMaxModelSteps := uint32(0)
	if len(maxModelSteps) > 0 {
		effectiveMaxModelSteps = maxModelSteps[0]
	}
	initialMessage := ""
	if input.InitialUserMessage != nil {
		initialMessage = *input.InitialUserMessage
	}
	fpTools := make([]agentSessionFingerprintTool, 0, len(tools))
	for _, tool := range tools {
		fpTools = append(fpTools, agentSessionFingerprintTool(tool))
	}
	slices.SortFunc(fpTools, func(a, b agentSessionFingerprintTool) int {
		return strings.Compare(a.ToolName, b.ToolName)
	})
	requestedTools := make([]agentSessionFingerprintRequestedTool, 0, len(input.ToolSelection.Tools))
	for _, tool := range input.ToolSelection.Tools {
		requestedTools = append(requestedTools, agentSessionFingerprintRequestedTool{
			ToolName:         tool.ToolName,
			Enabled:          tool.Enabled,
			RequiresApproval: tool.RequiresApproval,
		})
	}
	slices.SortFunc(requestedTools, func(a, b agentSessionFingerprintRequestedTool) int {
		return strings.Compare(a.ToolName, b.ToolName)
	})
	embeds := make([]agentSessionFingerprintEmbed, 0, len(input.InitialEmbeds))
	for _, embed := range input.InitialEmbeds {
		embeds = append(embeds, agentSessionFingerprintEmbed{
			EmbedType:    embed.EmbedType,
			EmbedData:    append([]byte(nil), embed.EmbedData...),
			FallbackText: embed.FallbackText,
			Position:     embed.Position,
		})
	}
	canonical := agentSessionCreateFingerprint{
		UnreadNotificationsEnabled: input.UnreadNotificationsEnabled,
		PromptCustomization:        input.PromptCustomization,
		ModelSelection:             input.ModelSelection,
		InitialRunBudget:           fingerprintWorkflowBudget(input.InitialRunBudget),
		OrganizationID:             input.OrganizationID,
		ActorUserID:                actorID,
		UserID:                     effectiveUserID,
		AgentType:                  input.AgentType,
		ParticipantUserID:          canonicalizeMemberTuple(actorID, participantIDs),
		ContextID:                  input.ContextID,
		ContextDisplay:             input.ContextDisplayName,
		ContextEntityID:            input.ContextEntityID,
		Metadata:                   input.Metadata,
		GetOrCreate:                input.GetOrCreateByContext,
		ToolSelection: agentSessionFingerprintChoice{
			Mode:  uint8(input.ToolSelection.Mode),
			Tools: requestedTools,
		},
		Tools:                fpTools,
		Compaction:           *input.Compaction,
		ReasoningPolicy:      reasoningPolicy,
		MaxModelSteps:        effectiveMaxModelSteps,
		AutoRunPolicy:        input.AutoRunPolicy,
		InitialMessage:       initialMessage,
		InitialEmbeds:        embeds,
		InferenceActorUserID: input.InferenceActorUserID,
	}
	if input.OrganizationReadMinRole != proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED {
		role := int32(input.OrganizationReadMinRole)
		canonical.OrganizationReadMinRole = &role
	}
	if input.InputMode != nil {
		mode := int32(*input.InputMode)
		canonical.InputMode = &mode
	}
	canonical.AllowUserStop = input.AllowUserStop
	if input.ForkRules != (common_persistence_models.ChatChannelForkRules{}) {
		canonical.ForkRules = &input.ForkRules
	}
	if input.DelegationOverride != nil && !input.DelegationOverride.IsZero() {
		canonical.DelegationOverride = input.DelegationOverride
	}
	return canonicalSHA256(common_json.MarshalCanonical(canonical))
}

// toolConfigsFromNormalizedRows converts persisted normalized tool-config rows back into resolved
// configs (used when preserving the current tool config across an update).
func toolConfigsFromNormalizedRows(rows []common_persistence_models.ChatAgentSessionToolConfig) []resolvedToolConfig {
	out := make([]resolvedToolConfig, 0, len(rows))
	for _, r := range rows {
		rendererKey := ""
		if r.RendererKey != nil {
			rendererKey = *r.RendererKey
		}
		out = append(out, resolvedToolConfig{
			ToolName:         r.ToolName,
			Enabled:          r.Enabled,
			RequiresApproval: r.RequiresApproval,
			RendererKey:      rendererKey,
		})
	}
	return out
}

// validateWorkflowInitialBudget checks CreateAgentChatSessionInput.InitialRunBudget before any
// write. Both failures wrap ErrInvalidRequest so the API layer keeps mapping them to 400, with
// distinct messages for the two caller mistakes: overriding a non-workflow agent, and limits
// that cannot be persisted as a meaningful override.
func validateWorkflowInitialBudget(workflowOwned bool, b *ports.AgentRunBudget) error {
	if b == nil {
		return nil
	}
	if !workflowOwned {
		return fmt.Errorf("%w: InitialRunBudget requires a workflow-owned agent", serviceErrors.ErrInvalidRequest)
	}
	if b.MaxActiveDuration < 0 || (b.MaxActiveDuration > 0 && b.MaxActiveDuration < time.Millisecond) {
		return fmt.Errorf("%w: InitialRunBudget.MaxActiveDuration must be zero (inherit) or at least one millisecond", serviceErrors.ErrInvalidRequest)
	}
	if b.MaxActiveDuration == 0 && b.MaxGenerations == 0 && b.MaxToolCalls == 0 {
		return fmt.Errorf("%w: InitialRunBudget must override at least one limit; pass nil to inherit every default", serviceErrors.ErrInvalidRequest)
	}
	return nil
}

// applyInitialRunBudgetOverride layers a validated partial override onto the defaults already
// captured by AgentRunBudgetCeilings().ApplyTo: non-zero fields narrow the ceilings, zero fields inherit them.
func applyInitialRunBudgetOverride(run *common_persistence_models.ChatAgentRun, b *ports.AgentRunBudget) {
	if b == nil {
		return
	}
	if b.MaxActiveDuration > 0 {
		run.MaxActiveDurationMs = min(run.MaxActiveDurationMs, uint64(b.MaxActiveDuration.Milliseconds()))
	}
	if b.MaxGenerations > 0 {
		run.MaxGenerations = min(run.MaxGenerations, b.MaxGenerations)
	}
	if b.MaxToolCalls > 0 {
		run.MaxToolCalls = min(run.MaxToolCalls, b.MaxToolCalls)
	}
}

type agentSessionFingerprintBudget struct {
	MaxActiveDurationMs uint64 `json:"max_active_duration_ms"`
	MaxGenerations      uint64 `json:"max_generations"`
	MaxToolCalls        uint64 `json:"max_tool_calls"`
}

func fingerprintWorkflowBudget(b *ports.AgentRunBudget) *agentSessionFingerprintBudget {
	if b == nil {
		return nil
	}
	return &agentSessionFingerprintBudget{MaxActiveDurationMs: uint64(b.MaxActiveDuration.Milliseconds()), MaxGenerations: b.MaxGenerations, MaxToolCalls: b.MaxToolCalls}
}

type preparedAgentSessionCreate struct {
	input             CreateAgentChatSessionInput
	agent             ports.AgentDefinition
	tools             []resolvedToolConfig
	metadataJSON      []byte
	reasoningPolicy   ports.AgentReasoningPolicy
	interactionPolicy ports.AgentInteractionPolicy
	maxModelSteps     uint32
	effectiveUserID   uint64
	hasInitialContent bool
}

// validateDelegationOverride rejects an override that agent's registered delegation does not admit.
func validateDelegationOverride(agent ports.AgentDefinition, override *ports.DelegationOverride) error {
	if err := ports.ValidateDelegationOverride(agent.Delegation, *override); err != nil {
		return fmt.Errorf("%w: %w", serviceErrors.ErrInvalidRequest, err)
	}
	return nil
}

// prepareAgentSessionCreate is shared by new-channel and existing-location creation.
func (h *Handler[U]) prepareAgentSessionCreate(input CreateAgentChatSessionInput, actorID uint64) (*preparedAgentSessionCreate, error) {
	if err := validateAgentPromptCustomization(input.PromptCustomization); err != nil {
		return nil, err
	}
	if input.PromptCustomization != nil {
		copied := *input.PromptCustomization
		input.PromptCustomization = &copied
	}

	agent, ok := h.agentRegistry.GetAgentType(input.AgentType)
	if !ok {
		return nil, serviceErrors.ErrInvalidRequest
	}

	if err := validateWorkflowInitialBudget(agent.IsWorkflowOwned(), input.InitialRunBudget); err != nil {
		return nil, err
	}
	if input.InferenceActorUserID != nil && (!agent.IsWorkflowOwned() || *input.InferenceActorUserID == 0 || input.ModelSelection != nil) {
		return nil, fmt.Errorf("%w: InferenceActorUserID requires a workflow-owned agent, a nonzero user and no ModelSelection", serviceErrors.ErrInvalidRequest)
	}

	// Compaction is required; nil is rejected. Enabled=false is the explicit no-compaction path.
	if input.Compaction == nil || ports.ValidateCompactionRules(*input.Compaction) != nil {
		return nil, serviceErrors.ErrInvalidRequest
	}
	if !validAgentSessionAutoRunPolicy(input.AutoRunPolicy) {
		return nil, serviceErrors.ErrInvalidRequest
	}
	if input.InputMode != nil && !ports.ValidAgentInputMode(*input.InputMode) {
		return nil, serviceErrors.ErrInvalidRequest
	}
	if input.DelegationOverride != nil {
		if err := validateDelegationOverride(agent, input.DelegationOverride); err != nil {
			return nil, err
		}
		copied := *input.DelegationOverride
		copied.ChildAgentTypes = slices.Clone(copied.ChildAgentTypes)
		input.DelegationOverride = &copied
	}

	metadata, metadataJSON, err := sanitizeAgentSessionMetadata(input.Metadata, input.metadataLimits)
	if err != nil {
		return nil, serviceErrors.ErrInvalidRequest
	}
	input.Metadata = metadata

	// Resolve tools before any write so validation failures don't create a channel.
	tools, err := h.resolveToolSelection(agent, input.ToolSelection, h.agentToolRegistry)
	if err != nil {
		return nil, err
	}
	reasoningPolicy := agent.ReasoningPolicy
	if input.ReasoningPolicy != nil {
		reasoningPolicy = *input.ReasoningPolicy
	}
	maxModelSteps := agent.MaxModelSteps
	if input.MaxModelSteps != nil {
		maxModelSteps = *input.MaxModelSteps
	}
	interactionPolicy := ports.ResolveInteractionPolicy(agent.DefaultInteractionPolicy, input.InputMode, input.AllowUserStop)

	// Effective chat user: UserID, or the actor when nil.
	effectiveUserID := actorID
	if input.UserID != nil && *input.UserID != 0 {
		effectiveUserID = *input.UserID
	}

	hasInitialContent := (input.InitialUserMessage != nil && *input.InitialUserMessage != "") || len(input.InitialEmbeds) > 0
	if hasInitialContent && input.IdempotencyKey == "" {
		return nil, serviceErrors.ErrInvalidRequest
	}
	if len(input.IdempotencyKey) > maxAgentSessionCreateIdempotencyKeyBytes {
		return nil, serviceErrors.ErrInvalidRequest
	}

	return &preparedAgentSessionCreate{input: input, agent: agent, tools: tools, metadataJSON: metadataJSON,
		reasoningPolicy: reasoningPolicy, interactionPolicy: interactionPolicy, maxModelSteps: maxModelSteps,
		effectiveUserID: effectiveUserID, hasInitialContent: hasInitialContent}, nil
}
