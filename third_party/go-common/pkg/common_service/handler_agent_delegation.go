package common_service

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_context"
	common_persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	common_persistence_models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/agentkit"
	botpkg "github.com/Suhaibinator/go-common/pkg/common_service/bot"
	chatpkg "github.com/Suhaibinator/go-common/pkg/common_service/chat"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// delegationToolErrorCode is the model-visible error discriminant of the library delegation tools.
// The wire codes are a model-facing contract: the tool descriptions and the parent's prompt state
// name them, so they evolve additively only.
type delegationToolErrorCode int32

const (
	delegationErrorUnspecified delegationToolErrorCode = iota
	// delegationErrorNotADelegationParent: the calling run's session cannot spawn (not a
	// main-timeline session of a delegating agent, itself a child, or archived).
	delegationErrorNotADelegationParent
	// delegationErrorChildTypeNotAllowed: the parent does not declare the child type, or the child
	// type is not spawnable (unregistered, workflow-owned, delegating, shared bot).
	delegationErrorChildTypeNotAllowed
	// delegationErrorMaxActiveChildrenReached: the parent's active-children limit is in force.
	delegationErrorMaxActiveChildrenReached
	// delegationErrorChildNotFound: no child with that session id belongs to the calling parent.
	delegationErrorChildNotFound
	// delegationErrorNotADelegatedChild: report_to_parent was called from a run that has no parent.
	delegationErrorNotADelegatedChild
	// delegationErrorThreadUnavailable: the delegation thread can no longer be posted to (its root
	// was deleted or no longer anchors a thread). Retrying the same call cannot succeed.
	delegationErrorThreadUnavailable
	// delegationErrorMaxChildRunsReached: the child has used chat_agent_delegation_max_runs_per_child
	// runs, so a continue send cannot start another.
	delegationErrorMaxChildRunsReached
	// delegationErrorChildUnavailable: the child cannot receive a parent message (archived, its
	// channel or thread is unavailable, or a continue send to a child that does not auto-run).
	// Retrying the same call cannot succeed.
	delegationErrorChildUnavailable
	// delegationErrorGraphBudgetExhausted: the graph's budget pool is used up, so no child can be
	// spawned or messaged until the application raises it.
	delegationErrorGraphBudgetExhausted
	// delegationErrorMaxChildrenReached: the parent has created as many sub-agents as its delegation
	// allows over its lifetime (MaxChildrenPerSession).
	delegationErrorMaxChildrenReached
)

func (c delegationToolErrorCode) String() string {
	switch c {
	case delegationErrorMaxChildRunsReached:
		return "max_child_runs_reached"
	case delegationErrorNotADelegationParent:
		return "not_a_delegation_parent"
	case delegationErrorChildTypeNotAllowed:
		return "child_type_not_allowed"
	case delegationErrorMaxActiveChildrenReached:
		return "max_active_children_reached"
	case delegationErrorChildNotFound:
		return "child_not_found"
	case delegationErrorNotADelegatedChild:
		return "not_a_delegated_child"
	case delegationErrorThreadUnavailable:
		return "thread_unavailable"
	case delegationErrorChildUnavailable:
		return "child_unavailable"
	case delegationErrorGraphBudgetExhausted:
		return "graph_budget_exhausted"
	case delegationErrorMaxChildrenReached:
		return "max_children_reached"
	default:
		return "unspecified"
	}
}

// errDelegationToolsUnavailable is the terminal cause when a delegation tool runs in a handler
// without the chat service or persistence it needs; retrying the same process cannot fix that.
var errDelegationToolsUnavailable = errors.New("delegation tools require the chat service and persistence")

// DelegatedChildCancellation reports what CancelDelegatedChild did.
type DelegatedChildCancellation struct {
	// CancelledRunIDs are the child runs this call moved to CANCELLED.
	CancelledRunIDs []uint64
	// Archived is true when this call archived the child session; false when it already was.
	Archived bool
}

// Tool argument and result shapes. Results are JSON for the model; the field names are part of the
// tool contract described to it and change additively only.
type spawnSubagentArgs struct {
	AgentType     string  `json:"agent_type"`
	Title         string  `json:"title"`
	Instructions  string  `json:"instructions"`
	MaxModelSteps *uint32 `json:"max_model_steps,omitzero"`
	// Scope is the child's scope, checked by its type's ports.ChildScopePolicy.
	Scope jsontext.Value `json:"scope,omitzero"`
}

type spawnSubagentResult struct {
	ChildSessionID uint64 `json:"child_session_id"`
	ThreadRootID   uint64 `json:"thread_root_id"`
	AgentRunID     uint64 `json:"agent_run_id"`
	AgentType      string `json:"agent_type"`
	Replayed       bool   `json:"replayed"`
}

type reportToParentArgs struct {
	Content string `json:"content"`
}

type reportToParentResult struct {
	MessageID    uint64 `json:"message_id"`
	ThreadRootID uint64 `json:"thread_root_id"`
	Replayed     bool   `json:"replayed"`
}

type cancelSubagentArgs struct {
	ChildSessionID uint64 `json:"child_session_id"`
	Reason         string `json:"reason,omitzero"`
}

type cancelSubagentResult struct {
	ChildSessionID  uint64   `json:"child_session_id"`
	CancelledRunIDs []uint64 `json:"cancelled_run_ids"`
	// Archived reports the child's state after the call, not whether this call archived it
	// (DelegatedChildCancellation.Archived): a replayed or repeated cancel must read the same to the
	// model, and a successful retire always leaves the child archived.
	Archived bool `json:"archived"`
}

type subagentStatusArgs struct {
	ChildSessionID      uint64 `json:"child_session_id,omitzero"`
	IncludeCompleted    bool   `json:"include_completed,omitzero"`
	AfterChildSessionID uint64 `json:"after_child_session_id,omitzero"`
	Limit               int    `json:"limit,omitzero"`
}

type subagentStatusRun struct {
	AgentRunID       uint64     `json:"agent_run_id"`
	Status           string     `json:"status"`
	StatusReason     string     `json:"status_reason,omitzero"`
	StatusReasonCode string     `json:"status_reason_code,omitzero"`
	StartedAt        time.Time  `json:"started_at"`
	CompletedAt      *time.Time `json:"completed_at,omitzero"`
}

type subagentStatusReport struct {
	MessageID uint64    `json:"message_id"`
	Excerpt   string    `json:"excerpt"`
	At        time.Time `json:"at"`
}

type subagentStatusChild struct {
	// RunCount is the child's lifetime run count, measured against the result's max_runs_per_child.
	RunCount       uint64                `json:"run_count"`
	ChildSessionID uint64                `json:"child_session_id"`
	ThreadRootID   uint64                `json:"thread_root_id"`
	AgentType      string                `json:"agent_type"`
	SessionStatus  string                `json:"session_status"`
	Run            *subagentStatusRun    `json:"run"`
	LastReport     *subagentStatusReport `json:"last_report"`
	// PendingMessageID is the newest parent message a continue send recorded while the child was
	// running (the child's PendingWakeMessageID). The running child sees it at its next model step;
	// if the run ends without having consumed it, the drain mints a follow-up run or discards it
	// (see LastDiscardedMessage). Omitted when none.
	PendingMessageID uint64 `json:"pending_message_id,omitzero"`
	// LastDiscardedMessage is the newest parent message the child's drain discarded, with why.
	LastDiscardedMessage *subagentStatusDiscardedMessage `json:"last_discarded_message,omitzero"`
}

type subagentStatusDiscardedMessage struct {
	MessageID uint64 `json:"message_id"`
	// Reason is the snake_case discard reason (ChatAgentMessageDiscardReason.String), for example
	// run_failed, child_archived, or max_child_runs_reached.
	Reason string `json:"reason"`
}

type subagentStatusResult struct {
	Children []subagentStatusChild `json:"children"`
	// Omitted is a lower bound on children left out. HasMore and the ID cursor
	// allow explicit pagination without counting lifetime history.
	Omitted                 uint32 `json:"omitted,omitzero"`
	HasMore                 bool   `json:"has_more"`
	NextAfterChildSessionID uint64 `json:"next_after_child_session_id,omitzero"`
	PendingWakes            uint32 `json:"pending_wakes"`
	// MaxRunsPerChild is the effective chat_agent_delegation_max_runs_per_child (hot KMS, sampled
	// once per call): the lifetime run cap every child's run_count is measured against.
	MaxRunsPerChild int `json:"max_runs_per_child"`
}

const maxSubagentStatusReportExcerptRunes = 400

// delegationToolExecutors binds the library delegation tools to this handler. initializeBotDispatcher
// registers the definitions bot.NewDelegationToolDefinitions builds from them, so applications
// never define these tools themselves.
func (h *CommonServiceHandler[U]) delegationToolExecutors() botpkg.DelegationToolExecutors {
	return botpkg.DelegationToolExecutors{
		Read:   h.executeReadSubagentMessages,
		Send:   h.executeSendSubagentMessage,
		Spawn:  h.executeSpawnSubagent,
		Status: h.executeSubagentStatus,
		Cancel: h.executeCancelSubagent,
		Report: h.executeReportToParent,
	}
}

// delegationRunContext is the calling run resolved to its session and agent definition.
type delegationRunContext struct {
	run     *common_persistence_models.ChatAgentRun
	session *common_persistence_models.ChatAgentSession
	agent   ports.AgentDefinition
	known   bool
}

func (rc *delegationRunContext) isDelegationParent() bool {
	return rc.session.CanDelegate() && rc.known && rc.agent.IsDelegating()
}

// ownsDelegatedChild reports whether child is a direct child of the calling parent in the parent's
// own channel. Both halves are checked: a child row naming this parent from another channel is not
// one the parent may read, steer, cancel, or inspect.
func (rc *delegationRunContext) ownsDelegatedChild(child *common_persistence_models.ChatAgentSession) bool {
	return child != nil && child.ParentAgentSessionID != nil && *child.ParentAgentSessionID == rc.session.ID && child.ChannelID == rc.session.ChannelID
}

// loadDelegationParent resolves the tool call's run and requires it to belong to a delegation
// parent. A calling agent that cannot delegate gets the not_a_delegation_parent result with the
// given text; a failed load is returned as the error.
func (h *CommonServiceHandler[U]) loadDelegationParent(ctx context.Context, invocation ports.AgentToolInvocation, refusal string) (*delegationRunContext, *ports.AgentToolResult, error) {
	rc, err := h.loadDelegationRun(ctx, invocation.AgentRunID)
	if err != nil {
		return nil, nil, err
	}
	if !rc.isDelegationParent() {
		return nil, delegationError(invocation, delegationErrorNotADelegationParent, refusal), nil
	}
	// The run's delegation may narrow the registered one: a parent that no longer spawns does not
	// steer or cancel its children either.
	if ports.IsDelegationParentToolName(invocation.ToolName) {
		delegation, err := chatpkg.RunDelegation(ctx, h.commonPersistenceHandler, rc.agent, rc.run)
		if err != nil {
			return nil, nil, err
		}
		if !slices.Contains(ports.DelegationParentToolNames(delegation.Creation), invocation.ToolName) {
			return nil, delegationError(invocation, delegationErrorNotADelegationParent, "only the application manages this conversation's sub-agents"), nil
		}
	}
	return rc, nil, nil
}

// loadOwnedChild is the one ownership check every parent tool applies to a named child: the child
// must exist and be owned by the calling parent (ownsDelegatedChild). Anything else is the
// child_not_found result, so a parent cannot probe for sessions it does not own.
func (h *CommonServiceHandler[U]) loadOwnedChild(ctx context.Context, invocation ports.AgentToolInvocation, rc *delegationRunContext, childID uint64) (*common_persistence_models.ChatAgentSession, *ports.AgentToolResult, error) {
	child, err := h.commonPersistenceHandler.GetChatAgentSessionByID(ctx, childID)
	if errors.Is(err, common_persistence.ErrChatAgentSessionNotFound) || err == nil && !rc.ownsDelegatedChild(child) {
		return nil, childNotFound(invocation, childID), nil
	}
	if err != nil {
		return nil, nil, err
	}
	return child, nil, nil
}

// loadDelegationRun resolves the tool call's run. A run or session that no longer exists is a
// terminal failure: the durable identity the tool would act for is gone, so a retry cannot help.
func (h *CommonServiceHandler[U]) loadDelegationRun(ctx context.Context, runID uint64) (*delegationRunContext, error) {
	if h.commonPersistenceHandler == nil || h.chatHandler == nil || h.agentRegistry == nil {
		return nil, &ports.AgentToolExecutionError{Cause: errDelegationToolsUnavailable}
	}
	if runID == 0 {
		return nil, &ports.AgentToolExecutionError{Cause: fmt.Errorf("delegation tool invoked without a run")}
	}
	run, err := h.commonPersistenceHandler.GetChatAgentRunByID(ctx, runID)
	if err != nil {
		if errors.Is(err, common_persistence.ErrChatAgentRunNotFound) {
			return nil, &ports.AgentToolExecutionError{Cause: fmt.Errorf("delegation tool run %d: %w", runID, err)}
		}
		return nil, err
	}
	if run == nil || common_util.Deref(run.AgentSessionID) == 0 {
		return nil, &ports.AgentToolExecutionError{Cause: fmt.Errorf("delegation tool run %d has no agent session", runID)}
	}
	session, err := h.commonPersistenceHandler.GetChatAgentSessionByID(ctx, *run.AgentSessionID)
	if err != nil {
		if errors.Is(err, common_persistence.ErrChatAgentSessionNotFound) {
			return nil, &ports.AgentToolExecutionError{Cause: fmt.Errorf("delegation tool session %d: %w", *run.AgentSessionID, err)}
		}
		return nil, err
	}
	if session == nil {
		return nil, &ports.AgentToolExecutionError{Cause: fmt.Errorf("delegation tool session %d not found", *run.AgentSessionID)}
	}
	agent, known := h.agentRegistry.GetAgentType(session.AgentType)
	return &delegationRunContext{run: run, session: session, agent: agent, known: known}, nil
}

// withDelegationTransaction runs fn as the given bot inside a transaction this handler owns. The
// tool executes inside the calling run's drive: its ambient request transaction and destination
// drive lease are stripped before the bot's own transaction begins (a retained writer lease would
// fence the child's rows as stale work of the wrong run). The original run/owner are retained
// separately solely to fence admission of new side effects. The bot is attached
// as the actor because the chat service authorizes sends and spawns against the context actor, and
// the human who triggered the parent run must not lend the tool their authority. Post-commit hooks
// (the child's drive, realtime publishes) run detached after commit, from a context that carries
// neither the tool invocation nor any actor (delegationPostCommitContext, which strips the bot).
func (h *CommonServiceHandler[U]) withDelegationTransaction(ctx context.Context, botUserID uint64, fn func(txCtx context.Context) error) error {
	if h.commonPersistenceHandler == nil {
		return &ports.AgentToolExecutionError{Cause: errDelegationToolsUnavailable}
	}
	actorCtx, err := h.delegationActorContext(ctx, botUserID)
	if err != nil {
		return err
	}
	return h.runOwnedTransaction(actorCtx, delegationPostCommitContext[U](actorCtx), "delegation_tool_post_commit", fn)
}

// delegationActorContext is ctx without the calling drive's request transaction and lease, acting
// as the given bot: the identity a delegation tool uses toward the chat service.
func (h *CommonServiceHandler[U]) delegationActorContext(ctx context.Context, botUserID uint64) (context.Context, error) {
	if lease, ok := common_context.GetChatAgentRunDriveLease(ctx); ok {
		ctx = common_context.WithChatAgentDelegationOrigin(ctx, lease.RunID, lease.Owner)
	}
	return agentkit.NewActorContext(h.GetUserByID, 0, 0).Attach(common_persistence.WithoutRequestTransaction[U](ctx), common_service_models.UserIdType(botUserID))
}

// delegationPostCommitContext is the base of a delegation tool's post-commit hooks. Those hooks start
// other work (the spawned child's drive, a woken parent run, realtime fan-out) that must not inherit
// the calling tool's identity: application callbacks reached from a child's drive read
// ports.AgentToolInvocationFromContext and the actor, and would otherwise attribute the whole child
// drive to the parent's tool call and bot. Trace metadata is kept. WithoutState drops every go-common
// value, including the drive lease; ctx comes from delegationActorContext, which already cleared the
// request transaction and lease, so nothing the hooks need is lost here.
func delegationPostCommitContext[U common_service_models.UserLike](ctx context.Context) context.Context {
	ctx = common_context.WithoutState(ctx)
	return agentkit.ContextWithoutActor[U](ctx)
}

// delegationToolFailure maps a chat-service error to the executor contract. Retrying a tool call
// replays the same stored arguments, so only a failure that can change on retry may stay retryable:
//   - a domain error the model can act on (a bad mention in the text, a child type it may not
//     spawn, the thread it reports to being gone, a delegation cap, a child that cannot receive
//     messages) becomes an is_error result it can correct;
//   - an authorization, idempotency, configuration, or missing-entity refusal is terminal, since a
//     retry would refuse again;
//   - anything else, including database errors, is left retryable.
func delegationToolFailure(invocation ports.AgentToolInvocation, err error) (*ports.AgentToolResult, error) {
	switch {
	case errors.Is(err, serviceErrors.ErrChatAgentDelegationParentInvalid):
		return delegationError(invocation, delegationErrorNotADelegationParent, err.Error()), nil
	case errors.Is(err, serviceErrors.ErrChatAgentDelegationNotAllowed), errors.Is(err, serviceErrors.ErrChatAgentDelegationChildInvalid):
		return delegationError(invocation, delegationErrorChildTypeNotAllowed, err.Error()), nil
	case errors.Is(err, serviceErrors.ErrChatAgentDelegationChildrenExhausted):
		return delegationError(invocation, delegationErrorMaxChildrenReached,
			"this agent has started all the sub-agents it may; reuse an existing one with send_subagent_message"), nil
	case errors.Is(err, serviceErrors.ErrChatAgentDelegationLimitExceeded):
		if invocation.ToolName == ports.DelegationToolSend {
			return delegationError(invocation, delegationErrorMaxActiveChildrenReached,
				"the maximum number of sub-agents are already working; wait for one to finish, or use mode queue_only"), nil
		}
		return delegationError(invocation, delegationErrorMaxActiveChildrenReached, err.Error()), nil
	case errors.Is(err, serviceErrors.ErrGraphBudgetExhausted):
		return delegationError(invocation, delegationErrorGraphBudgetExhausted,
			"the work budget shared by you and your sub-agents is used up; finish with what you have"), nil
	case errors.Is(err, serviceErrors.ErrChatAgentChildRunLimit):
		return delegationError(invocation, delegationErrorMaxChildRunsReached,
			"this sub-agent has used all of its runs (max_runs_per_child); use mode queue_only, or spawn a new sub-agent"), nil
	case errors.Is(err, serviceErrors.ErrChatAgentChildUnavailable):
		return delegationError(invocation, delegationErrorChildUnavailable,
			"this sub-agent cannot receive the message: it is archived, its thread or channel is unavailable, or (for mode continue) it does not run automatically"), nil
	case errors.Is(err, serviceErrors.ErrChatAgentSessionNotFound):
		return delegationError(invocation, delegationErrorChildNotFound, err.Error()), nil
	case errors.Is(err, serviceErrors.ErrChatInvalidThread),
		errors.Is(err, serviceErrors.ErrChatInvalidReply),
		errors.Is(err, serviceErrors.ErrChatInvalidBroadcast):
		return delegationError(invocation, delegationErrorThreadUnavailable, err.Error()), nil
	case errors.Is(err, serviceErrors.ErrInvalidRequest),
		errors.Is(err, serviceErrors.ErrChatInvalidMention),
		errors.Is(err, serviceErrors.ErrChatMentionLimitExceeded):
		return delegationError(invocation, agentkit.GenericErrorCodeInvalidArguments, err.Error()), nil
	case errors.Is(err, serviceErrors.ErrInsufficientPermissions),
		errors.Is(err, common_persistence.ErrChatAgentRunDriveLeaseLost),
		errors.Is(err, serviceErrors.ErrChatAgentSessionIdempotencyConflict),
		errors.Is(err, serviceErrors.ErrChatAgentSessionLocationConflict),
		errors.Is(err, serviceErrors.ErrChatAgentRunNotFound),
		errors.Is(err, serviceErrors.ErrChatChannelNotFound),
		errors.Is(err, serviceErrors.ErrChatMessageNotFound),
		errors.Is(err, serviceErrors.ErrChatDisabled),
		errors.Is(err, serviceErrors.ErrChatFeatureDisabled),
		errors.Is(err, serviceErrors.ErrChatChannelArchived),
		errors.Is(err, serviceErrors.ErrChatNotMember),
		errors.Is(err, serviceErrors.ErrChatAgentSessionScopeInvalid),
		errors.Is(err, serviceErrors.ErrChatAgentMessageInvalid),
		errors.Is(err, serviceErrors.ErrChatAgentMessagePageInvalid),
		errors.Is(err, serviceErrors.ErrTransactionNotFound),
		errors.Is(err, serviceErrors.ErrUserNotFound),
		errors.Is(err, serviceErrors.ErrInternalError),
		errors.Is(err, agentkit.ErrActorUnavailable):
		return nil, &ports.AgentToolExecutionError{Cause: err}
	default:
		return nil, err
	}
}

// delegationError is the is_error result of a delegation tool call: a model-visible code and message.
func delegationError(invocation ports.AgentToolInvocation, code agentkit.ErrorCode, message string) *ports.AgentToolResult {
	return agentkit.ErrorResult(invocation.ToolName, invocation.RendererKey, code, message)
}

// childNotFound refuses a child session id that is not one of the calling parent's children.
func childNotFound(invocation ports.AgentToolInvocation, childSessionID uint64) *ports.AgentToolResult {
	return delegationError(invocation, delegationErrorChildNotFound,
		fmt.Sprintf("no sub-agent with session id %d belongs to this conversation", childSessionID))
}

// delegationCancelledByParent is the cancellation a parent's cancel of one child records: the
// child's notice is posted, and the parent is not woken for a stop it asked for.
var delegationCancelledByParent = ports.AgentRunCancellation{
	Reason: proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_CANCELLED_BY_APPLICATION,
	Code:   ports.AgentRunCancellationCodeDelegationCancelledByParent,
}

// delegationParentArchived is the cancellation a parent's archive cascades to its children.
var delegationParentArchived = ports.AgentRunCancellation{
	Reason: proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_PARENT_WORKFLOW_TERMINATED,
	Code:   ports.AgentRunCancellationCodeDelegationParentArchived,
}

// delegationChildArchived is the cancellation of a delegated child archived directly.
var delegationChildArchived = ports.AgentRunCancellation{
	Reason: proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_CANCELLED_BY_APPLICATION,
}

// delegationParentStopped is the cancellation of a child whose parent a person stopped: the child
// posts its notice, and its suspended parent is not woken for it.
var delegationParentStopped = ports.AgentRunCancellation{
	Reason: proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_STOPPED_BY_USER,
	Code:   ports.AgentRunCancellationCodeDelegationParentStopped,
}

// executeSpawnSubagent is spawn_subagent. The chat service checks the parent session and the
// registry pair rules under the channel lock; its refusals reach the model through
// delegationToolFailure (a child type the parent does not declare lists the declared ones).
func (h *CommonServiceHandler[U]) executeSpawnSubagent(ctx context.Context, invocation ports.AgentToolInvocation) (*ports.AgentToolResult, error) {
	args, err := agentkit.DecodeToolArgs[spawnSubagentArgs](invocation.ArgumentsJSON)
	if err != nil {
		return delegationError(invocation, agentkit.GenericErrorCodeInvalidArguments, err.Error()), nil
	}
	if strings.TrimSpace(args.AgentType) == "" || strings.TrimSpace(args.Title) == "" || strings.TrimSpace(args.Instructions) == "" {
		return delegationError(invocation, agentkit.GenericErrorCodeInvalidArguments, "agent_type, title, and instructions are required"), nil
	}
	rc, err := h.loadDelegationRun(ctx, invocation.AgentRunID)
	if err != nil {
		return nil, err
	}
	var spawned *chatpkg.CreateAgentChatSessionResult
	err = h.withDelegationTransaction(ctx, rc.session.BotUserID, func(txCtx context.Context) error {
		result, err := h.chatHandler.SpawnDelegatedAgentSession(txCtx, chatpkg.SpawnDelegatedAgentSessionInput{
			ParentAgentSessionID: rc.session.ID,
			ParentAgentRunID:     rc.run.ID,
			AgentType:            args.AgentType,
			Title:                args.Title,
			Instructions:         args.Instructions,
			MaxModelSteps:        args.MaxModelSteps,
			IdempotencyKey:       delegationSpawnIdempotencyKey(invocation),
			Scope:                args.Scope,
		})
		if err != nil {
			return err
		}
		spawned = result
		return nil
	})
	if err != nil {
		return delegationToolFailure(invocation, err)
	}
	if spawned == nil || spawned.AgentSession == nil {
		return nil, &ports.AgentToolExecutionError{Cause: errors.New("spawn returned no child session")}
	}
	payload := spawnSubagentResult{
		ChildSessionID: spawned.AgentSession.ID,
		ThreadRootID:   spawned.AgentSession.ParentMessageID,
		AgentType:      spawned.AgentSession.AgentType,
		Replayed:       spawned.Reused,
	}
	if spawned.InitialRun != nil {
		payload.AgentRunID = spawned.InitialRun.ID
	}
	return agentkit.SuccessResult(payload)
}

// delegationSpawnIdempotencyKey is the replay key of one spawn_subagent call. The invocation's own
// key is the provider tool-call id for app-executor tools, which is only unique within a run (some
// providers number calls sequentially), while spawn keys live in a per-parent namespace that spans
// every run of the parent: a colliding key would replay an old, finished child or fail a fresh spawn
// as an idempotency conflict. The persisted TOOL_CALL row id is globally unique and stable across
// retries of the same claimed call; without one, the run id scopes the provider key.
func delegationSpawnIdempotencyKey(invocation ports.AgentToolInvocation) string {
	if invocation.ToolCallMessageID != 0 {
		return fmt.Sprintf("tool-call-message-%d", invocation.ToolCallMessageID)
	}
	return fmt.Sprintf("run-%d:%s", invocation.AgentRunID, invocation.IdempotencyKey)
}

// executeReportToParent is report_to_parent: one broadcast thread reply as the child bot, tagged with
// the child run so the parent's follow-up trigger recognizes it as a delegation wake. A retried tool
// call finds the report its earlier attempt already posted instead of posting it twice.
func (h *CommonServiceHandler[U]) executeReportToParent(ctx context.Context, invocation ports.AgentToolInvocation) (*ports.AgentToolResult, error) {
	args, err := agentkit.DecodeToolArgs[reportToParentArgs](invocation.ArgumentsJSON)
	if err != nil {
		return delegationError(invocation, agentkit.GenericErrorCodeInvalidArguments, err.Error()), nil
	}
	if strings.TrimSpace(args.Content) == "" {
		return delegationError(invocation, agentkit.GenericErrorCodeInvalidArguments, "content is required"), nil
	}
	rc, err := h.loadDelegationRun(ctx, invocation.AgentRunID)
	if err != nil {
		return nil, err
	}
	if !rc.session.IsDelegatedChild() {
		return delegationError(invocation, delegationErrorNotADelegatedChild,
			"this conversation was not delegated by another agent; answer here instead"), nil
	}
	// Only a durable tool-call id can anchor the replay lookup: with none, any earlier report of
	// the run would be mistaken for this call's.
	if invocation.ToolCallMessageID != 0 {
		existing, err := h.commonPersistenceHandler.GetLatestBroadcastReplyByRun(ctx, rc.run.ID, invocation.ToolCallMessageID)
		switch {
		case err == nil && existing != nil:
			return agentkit.SuccessResult(reportToParentResult{MessageID: existing.ID, ThreadRootID: rc.session.ParentMessageID, Replayed: true})
		case err != nil && !errors.Is(err, common_persistence.ErrChatMessageNotFound):
			return nil, err
		}
	}
	var sent *common_service_models.ChatMessage
	replayed := false
	err = h.withDelegationTransaction(ctx, rc.session.BotUserID, func(txCtx context.Context) error {
		runID := rc.run.ID
		locked, err := common_persistence.LockChatAgentDelegationOrigin(txCtx, h.commonPersistenceHandler, runID)
		if err != nil {
			return err
		}
		if invocation.ToolCallMessageID != 0 {
			existing, replayErr := h.commonPersistenceHandler.GetLatestBroadcastReplyByRun(txCtx, runID, invocation.ToolCallMessageID)
			if replayErr == nil && existing != nil {
				sent = &common_service_models.ChatMessage{ID: common_service_models.ChatMessageIdType(existing.ID)}
				replayed = true
				return nil
			}
			if replayErr != nil && !errors.Is(replayErr, common_persistence.ErrChatMessageNotFound) {
				return replayErr
			}
		}
		if err := common_persistence.ValidateChatAgentDelegationOrigin(txCtx, h.commonPersistenceHandler, locked, rc.session.ID); err != nil {
			return err
		}
		message, err := h.chatHandler.SendBotMessage(txCtx, chatpkg.SendBotMessageInput{
			ChannelID:          rc.run.ChannelID,
			Content:            args.Content,
			ParentMessageID:    rc.session.ParentMessageID,
			BroadcastToChannel: true,
			AgentRunID:         &runID,
		})
		if err != nil {
			return err
		}
		sent = message
		return nil
	})
	if err != nil {
		return delegationToolFailure(invocation, err)
	}
	if sent == nil {
		return nil, &ports.AgentToolExecutionError{Cause: errors.New("report send returned no message")}
	}
	return agentkit.SuccessResult(reportToParentResult{MessageID: uint64(sent.ID), ThreadRootID: rc.session.ParentMessageID, Replayed: replayed})
}

// executeCancelSubagent is cancel_subagent. Ownership is checked against the calling parent (its
// direct child, in its channel) so one parent cannot stop another parent's child, then the child is
// retired through retireDelegatedChild with the delegation_cancelled_by_parent code, the same path
// CancelDelegatedChild takes after its application-side lookup. It is idempotent: a finished or
// archived child reports success with nothing cancelled.
func (h *CommonServiceHandler[U]) executeCancelSubagent(ctx context.Context, invocation ports.AgentToolInvocation) (*ports.AgentToolResult, error) {
	args, err := agentkit.DecodeToolArgs[cancelSubagentArgs](invocation.ArgumentsJSON)
	if err != nil {
		return delegationError(invocation, agentkit.GenericErrorCodeInvalidArguments, err.Error()), nil
	}
	if args.ChildSessionID == 0 {
		return delegationError(invocation, agentkit.GenericErrorCodeInvalidArguments, "child_session_id is required"), nil
	}
	rc, refused, err := h.loadDelegationParent(ctx, invocation, "this agent has no sub-agents to cancel")
	if refused != nil || err != nil {
		return refused, err
	}
	child, refused, err := h.loadOwnedChild(ctx, invocation, rc, args.ChildSessionID)
	if refused != nil || err != nil {
		return refused, err
	}
	if reason := strings.TrimSpace(args.Reason); reason != "" {
		appLogger.For(ctx, h.logger, agentRuntimeLoggerName).Info("parent agent cancelled a delegated child",
			zap.String(logkeys.Stage, "cancel_subagent"),
			zap.Uint64(appLogger.KeyAgentSessionID, child.ID),
			zap.Uint64(appLogger.KeyAgentRunID, rc.run.ID),
			zap.Int(appLogger.KeyLength, len(reason)))
	}
	owner := ""
	if lease, ok := common_context.GetChatAgentRunDriveLease(ctx); ok {
		if lease.RunID != rc.run.ID {
			return delegationToolFailure(invocation, common_persistence.ErrChatAgentRunDriveLeaseLost)
		}
		owner = lease.Owner
	}
	ctx = common_context.WithChatAgentDelegationOrigin(ctx, rc.run.ID, owner)
	ctx = common_persistence.WithoutRequestTransaction[U](ctx)
	outcome, err := h.retireDelegatedChild(ctx, child, delegationCancelledByParent)
	if err != nil {
		return delegationToolFailure(invocation, err)
	}
	return agentkit.SuccessResult(cancelSubagentResult{
		ChildSessionID:  child.ID,
		CancelledRunIDs: outcome.CancelledRunIDs,
		Archived:        true,
	})
}

// executeSubagentStatus is subagent_status: the parent's children with their latest run and last
// report, optionally narrowed to one child. It reads without a transaction or actor: the calling
// run's own session is the only thing it reports on.
func (h *CommonServiceHandler[U]) executeSubagentStatus(ctx context.Context, invocation ports.AgentToolInvocation) (*ports.AgentToolResult, error) {
	args, err := agentkit.DecodeToolArgs[subagentStatusArgs](invocation.ArgumentsJSON)
	if err != nil {
		return delegationError(invocation, agentkit.GenericErrorCodeInvalidArguments, err.Error()), nil
	}
	if args.Limit < 0 || (args.ChildSessionID != 0 && (args.IncludeCompleted || args.AfterChildSessionID != 0 || args.Limit != 0)) {
		return delegationError(invocation, agentkit.GenericErrorCodeInvalidArguments, "use child_session_id alone, or list with include_completed, after_child_session_id and a positive limit"), nil
	}
	rc, refused, err := h.loadDelegationParent(ctx, invocation, "this agent has no sub-agents")
	if refused != nil || err != nil {
		return refused, err
	}
	readCtx := common_persistence.WithoutRequestTransaction[U](ctx)
	if args.ChildSessionID != 0 {
		session, refused, err := h.loadOwnedChild(readCtx, invocation, rc, args.ChildSessionID)
		if refused != nil || err != nil {
			return refused, err
		}
		child, err := h.chatHandler.DescribeDelegatedChild(readCtx, session)
		if err != nil {
			return delegationToolFailure(invocation, err)
		}
		result := subagentStatusResult{
			Children:     []subagentStatusChild{subagentStatusEntry(child)},
			PendingWakes: rc.session.PendingWakeCount,
		}
		return h.subagentStatusWithCounts(readCtx, result)
	}
	if args.IncludeCompleted || args.AfterChildSessionID != 0 || args.Limit != 0 {
		page, err := h.chatHandler.DelegatedChildrenForSession(readCtx, rc.session, chatpkg.DelegatedChildrenPageRequest{
			IncludeCompleted: args.IncludeCompleted, AfterChildSessionID: args.AfterChildSessionID, Limit: args.Limit,
		})
		if err != nil {
			return delegationToolFailure(invocation, err)
		}
		result := subagentStatusResult{PendingWakes: rc.session.PendingWakeCount, HasMore: page.HasMore, NextAfterChildSessionID: page.NextAfterChildSessionID}
		for _, child := range page.Children {
			result.Children = append(result.Children, subagentStatusEntry(child))
		}
		if page.HasMore {
			result.Omitted = 1
		}
		return h.subagentStatusWithCounts(readCtx, result)
	}
	// Routine status lists only active work; completed history requires an explicit page.
	state, err := h.chatHandler.DelegationStateForSession(readCtx, rc.session.ID)
	if err != nil {
		return delegationToolFailure(invocation, err)
	}
	// A nil Children encodes as [] under encoding/json/v2, so an empty listing needs no seed slice.
	var result subagentStatusResult
	if state != nil {
		result.PendingWakes = state.PendingWakeCount
		for _, child := range state.Children {
			if child == nil || child.Session == nil {
				continue
			}
			result.Children = append(result.Children, subagentStatusEntry(child))
		}
		if total := int(state.TotalChildren); total > len(result.Children) {
			result.Omitted = uint32(total - len(result.Children))
			result.HasMore = true
			if len(result.Children) > 0 {
				result.NextAfterChildSessionID = result.Children[len(result.Children)-1].ChildSessionID
			}
		}
	}
	return h.subagentStatusWithCounts(readCtx, result)
}

func subagentStatusEntry(child *chatpkg.DelegatedChild) subagentStatusChild {
	entry := subagentStatusChild{
		ChildSessionID:   child.Session.ID,
		ThreadRootID:     child.Session.ParentMessageID,
		AgentType:        child.Session.AgentType,
		SessionStatus:    "active",
		PendingMessageID: child.PendingMessageID,
	}
	if child.IsArchived() {
		entry.SessionStatus = "archived"
	}
	if run := child.LatestRun; run != nil {
		entry.Run = &subagentStatusRun{
			AgentRunID:       run.ID,
			Status:           child.RunStatusName(),
			StatusReason:     child.RunReasonName(),
			StatusReasonCode: run.StatusReasonCode,
			StartedAt:        run.StartedAt,
			CompletedAt:      run.CompletedAt,
		}
	}
	if report := child.LastReport; report != nil {
		entry.LastReport = &subagentStatusReport{
			MessageID: uint64(report.ID),
			Excerpt:   child.ReportExcerpt(maxSubagentStatusReportExcerptRunes),
			At:        report.CreatedAt,
		}
	}
	return entry
}

// ListDelegatedChildren returns one page of a delegation parent's active children, oldest first, each with
// its latest run and last report. The context actor must be able to read the parent's channel. A
// page holds at most chat_agent_delegated_children_page_max children (hot KMS); pass the returned
// NextAfterChildSessionID back as AfterChildSessionID while HasMore is set. IncludeCompleted opts
// into lifetime history.
func (h *CommonServiceHandler[U]) ListDelegatedChildren(ctx context.Context, parentAgentSessionID uint64, page chatpkg.DelegatedChildrenPageRequest) (*chatpkg.DelegatedChildrenPage, error) {
	if h == nil || h.chatHandler == nil {
		return nil, serviceErrors.ErrChatDisabled
	}
	return h.chatHandler.ListDelegatedChildren(ctx, parentAgentSessionID, page)
}

// CreateDelegatedChildren creates a batch of delegated children of a parent whose delegation allows
// application creation (see chat.Handler.CreateDelegatedChildren for replay, wake groups, scope and
// caps). It requires the caller's transaction with post-commit hooks, and acts as the parent's bot:
// the threads' roots and first messages are the parent bot's, as an agent spawn's are. Ownership is
// fenced at the root only: the application fences its claim on the graph in the same transaction.
// TriggeringUserID defaults to the parent's user, the human the root was created for.
func (h *CommonServiceHandler[U]) CreateDelegatedChildren(ctx context.Context, in chatpkg.CreateDelegatedChildrenInput) ([]chatpkg.DelegatedChildHandle, error) {
	if h == nil || h.chatHandler == nil || h.commonPersistenceHandler == nil {
		return nil, serviceErrors.ErrChatDisabled
	}
	botCtx, err := h.parentBotContext(ctx, in.ParentAgentSessionID)
	if err != nil {
		return nil, err
	}
	return h.chatHandler.CreateDelegatedChildren(botCtx, in)
}

// ReplaceDelegatedChild replaces a delegated child of a parent whose delegation allows application
// creation (see chat.Handler.ReplaceDelegatedChild for replay, slot transfer and caps). It requires
// the caller's transaction with post-commit hooks, and acts as the parent's bot. The old child is
// archived in that transaction; once it commits, a post-commit hook cancels the old child's runs
// with the parent-cancel code, which never wakes the parent. A crash before the hook leaves an
// archived child with active runs, which the recovery sweep's retirement side pass cancels.
func (h *CommonServiceHandler[U]) ReplaceDelegatedChild(ctx context.Context, in chatpkg.ReplaceDelegatedChildInput) (chatpkg.DelegatedChildHandle, error) {
	if h == nil || h.chatHandler == nil || h.commonPersistenceHandler == nil {
		return chatpkg.DelegatedChildHandle{}, serviceErrors.ErrChatDisabled
	}
	old, err := h.loadAgentSessionForControl(ctx, in.OldChildSessionID)
	if err != nil {
		return chatpkg.DelegatedChildHandle{}, err
	}
	if !old.IsDelegatedChild() {
		return chatpkg.DelegatedChildHandle{}, serviceErrors.ErrChatAgentSessionNotDelegated
	}
	botCtx, err := h.parentBotContext(ctx, *old.ParentAgentSessionID)
	if err != nil {
		return chatpkg.DelegatedChildHandle{}, err
	}
	handle, err := h.chatHandler.ReplaceDelegatedChild(botCtx, in)
	if err != nil {
		return chatpkg.DelegatedChildHandle{}, err
	}
	// Cancellation cannot join the caller's transaction: it takes the run row before the channel
	// row, the reverse of the creation's order. The archive above is its durable intent.
	retired := *old
	retired.Status = common_persistence_models.ChatAgentSessionStatusArchived
	common_persistence.AddPostCommitHookFromContext(ctx, func(hookCtx context.Context) error {
		h.runOwnedPostCommitHooks(hookCtx, []common_persistence.PostCommitHook{func(cancelCtx context.Context) error {
			_, err := h.retireDelegatedChild(cancelCtx, &retired, delegationCancelledByParent)
			return err
		}}, "replace_delegated_child")
		return nil
	})
	return handle, nil
}

// ResumeParent continues a delegation parent with a message from the application (see
// chat.Handler.ResumeParent). It requires the caller's transaction with post-commit hooks, and acts
// as the parent's bot. It returns the minted parent run (zero when coalesced), whether the resume
// coalesced into the parent's pending wake, and whether it replayed a committed resume.
func (h *CommonServiceHandler[U]) ResumeParent(ctx context.Context, in chatpkg.ResumeParentInput) (runID uint64, coalesced, replayed bool, err error) {
	if h == nil || h.chatHandler == nil || h.commonPersistenceHandler == nil {
		return 0, false, false, serviceErrors.ErrChatDisabled
	}
	botCtx, err := h.parentBotContext(ctx, in.ParentAgentSessionID)
	if err != nil {
		return 0, false, false, err
	}
	return h.chatHandler.ResumeParent(botCtx, in)
}

// parentBotContext attaches a delegation parent's bot as the context actor: the application's
// delegation calls post and write as the parent's bot, as the parent's own tools do.
func (h *CommonServiceHandler[U]) parentBotContext(ctx context.Context, parentAgentSessionID uint64) (context.Context, error) {
	parent, err := h.loadAgentSessionForControl(ctx, parentAgentSessionID)
	if err != nil {
		return nil, err
	}
	return agentkit.NewActorContext(h.GetUserByID, 0, 0).Attach(ctx, common_service_models.UserIdType(parent.BotUserID))
}

// CancelDelegatedChild stops one delegated child on the parent's behalf: its active runs are
// cancelled with CANCELLED_BY_APPLICATION and the delegation_cancelled_by_parent code, which posts
// the child's notice without waking the parent (the parent asked), and the child session is
// archived. It is idempotent: a child that already finished or was archived returns with nothing
// cancelled. ErrChatAgentSessionNotDelegated when the session is not a delegated child.
func (h *CommonServiceHandler[U]) CancelDelegatedChild(ctx context.Context, childAgentSessionID uint64) (*DelegatedChildCancellation, error) {
	if h == nil || h.commonPersistenceHandler == nil {
		return nil, serviceErrors.ErrChatAgentSessionNotFound
	}
	controlCtx := common_persistence.WithoutRequestTransaction[U](ctx)
	child, err := h.loadAgentSessionForControl(controlCtx, childAgentSessionID)
	if err != nil {
		return nil, err
	}
	if !child.IsDelegatedChild() {
		return nil, serviceErrors.ErrChatAgentSessionNotDelegated
	}
	return h.retireDelegatedChild(controlCtx, child, delegationCancelledByParent)
}

// retireDelegatedChildren pages unfinished retirement work. Every child in a
// page is archived before its active runs are read together, closing admission
// before cancellation. Fully retired history never incurs per-child run reads.
func (h *CommonServiceHandler[U]) retireDelegatedChildren(ctx context.Context, parentAgentSessionID uint64) error {
	if h == nil || h.commonPersistenceHandler == nil || parentAgentSessionID == 0 {
		return serviceErrors.ErrChatAgentSessionNotFound
	}
	controlCtx := common_persistence.WithoutRequestTransaction[U](ctx)
	pageSize := config.DefaultChatAgentRunRecoveryScanPageSize
	if h.agentRuntime != nil {
		pageSize = h.agentRuntime.recoveryPolicy().scanPageSize
	}
	var afterID uint64
	for {
		children, err := h.commonPersistenceHandler.ListDelegatedChatAgentSessionsNeedingRetirementByParent(controlCtx, parentAgentSessionID, afterID, pageSize)
		if err != nil {
			return serviceErrors.ErrDatabaseError
		}
		if len(children) == 0 {
			return nil
		}
		ids := make([]uint64, 0, len(children))
		for _, child := range children {
			if _, err := h.archiveDelegatedChild(controlCtx, child); err != nil {
				return err
			}
			ids = append(ids, child.ID)
			afterID = child.ID
		}
		active, err := h.commonPersistenceHandler.ListActiveChatAgentRunsBySessions(controlCtx, ids)
		if err != nil {
			return serviceErrors.ErrDatabaseError
		}
		if _, err := h.cancelDelegatedRuns(controlCtx, active, delegationParentArchived); err != nil {
			return err
		}
		if len(children) < pageSize {
			return nil
		}
	}
}

// ArchiveChatAgentSession retires a session. A delegation parent is archived first, so a child that
// is cancelled afterwards settles against an inactive parent and skips the wake, then its children
// are retired with the delegation_parent_archived code. A delegated child archived directly is
// retired the same way with a plain CANCELLED_BY_APPLICATION cancel: its in-flight runs stop, each
// posts the child's notice, and the parent is never woken for an archived child. Idempotent;
// ErrChatAgentSessionNotFound when the session does not exist.
func (h *CommonServiceHandler[U]) ArchiveChatAgentSession(ctx context.Context, sessionID uint64) error {
	if h == nil || h.commonPersistenceHandler == nil {
		return serviceErrors.ErrChatAgentSessionNotFound
	}
	controlCtx := common_persistence.WithoutRequestTransaction[U](ctx)
	session, err := h.loadAgentSessionForControl(controlCtx, sessionID)
	if err != nil {
		return err
	}
	if session.IsDelegatedChild() {
		_, err := h.retireDelegatedChild(controlCtx, session, delegationChildArchived)
		return err
	}
	if session.Status != common_persistence_models.ChatAgentSessionStatusArchived {
		posted, err := h.commonPersistenceHandler.ArchiveChatAgentSession(controlCtx, sessionID)
		if err != nil {
			return mapAgentSessionControlError(err)
		}
		h.publishAgentSessionArchived(controlCtx, session, posted)
	}
	if !session.IsMainTimeline() {
		return nil
	}
	return h.retireDelegatedChildren(controlCtx, sessionID)
}

// publishAgentSessionArchived tells the session's readers its interaction changed: it no longer
// takes input, and the archive posted whatever input was queued, as the messages it announces. It
// then starts the parent run the archive minted by releasing the child's wake group.
func (h *CommonServiceHandler[U]) publishAgentSessionArchived(ctx context.Context, session *common_persistence_models.ChatAgentSession, archive common_persistence.ChatAgentSessionArchive) {
	if h.chatHandler != nil {
		h.chatHandler.PublishDeliveredAgentInputAfterCommit(ctx, session.ID, session.ChannelID, session.ParentMessageID, archive.MessageIDs())
		if archive.ParentWakeRun != nil {
			h.chatHandler.StartMintedAgentRunAfterCommit(ctx, archive.ParentWakeRun)
		}
	}
}

// retireDelegatedChild archives the child first, then cancels its active runs one transaction each
// (persistence settles the notice and wake decision inside the run's own terminal write). Archiving
// first closes the window in which a human reply could mint a new child run between the listing
// and the archive: once the child is archived no new run starts at its thread, every run that
// already exists is in the listing, and settlement never wakes the parent for an archived child.
// The archive is its own statement, so it never holds the session row together with a run row.
// The archive itself is the durable intent: recovery cancels runs it left active after a crash or
// a failed subsequent database operation, and no run of a retiring child can be drive-claimed.
func (h *CommonServiceHandler[U]) retireDelegatedChild(ctx context.Context, child *common_persistence_models.ChatAgentSession, cancellation ports.AgentRunCancellation) (*DelegatedChildCancellation, error) {
	archived, err := h.archiveDelegatedChild(ctx, child)
	if err != nil {
		return nil, err
	}
	active, err := h.commonPersistenceHandler.ListActiveChatAgentRunsBySession(ctx, child.ID)
	if err != nil {
		return nil, serviceErrors.ErrDatabaseError
	}
	cancelled, err := h.cancelDelegatedRuns(ctx, active, cancellation)
	if err != nil {
		return nil, err
	}
	return &DelegatedChildCancellation{Archived: archived, CancelledRunIDs: cancelled}, nil
}

func (h *CommonServiceHandler[U]) archiveDelegatedChild(ctx context.Context, child *common_persistence_models.ChatAgentSession) (bool, error) {
	if child.Status != common_persistence_models.ChatAgentSessionStatusArchived {
		if posted, err := h.commonPersistenceHandler.ArchiveChatAgentSession(ctx, child.ID); err != nil {
			if !errors.Is(err, common_persistence.ErrChatAgentSessionNotFound) {
				return false, mapAgentSessionControlError(err)
			}
		} else {
			h.publishAgentSessionArchived(common_context.ClearChatAgentDelegationOrigin(ctx), child, posted)
			return true, nil
		}
	}
	return false, nil
}

func (h *CommonServiceHandler[U]) cancelDelegatedRuns(ctx context.Context, active []*common_persistence_models.ChatAgentRun, cancellation ports.AgentRunCancellation) ([]uint64, error) {
	var cancelledIDs []uint64
	for _, run := range active {
		if run == nil {
			continue
		}
		cancelled, err := h.CancelChatAgentRun(ctx, run.ID, cancellation)
		if err != nil && !errors.Is(err, serviceErrors.ErrChatAgentRunNotFound) {
			return nil, err
		}
		if cancelled {
			cancelledIDs = append(cancelledIDs, run.ID)
		}
	}
	return cancelledIDs, nil
}

func (h *CommonServiceHandler[U]) loadAgentSessionForControl(ctx context.Context, sessionID uint64) (*common_persistence_models.ChatAgentSession, error) {
	if sessionID == 0 {
		return nil, serviceErrors.ErrChatAgentSessionNotFound
	}
	session, err := h.commonPersistenceHandler.GetChatAgentSessionByID(ctx, sessionID)
	if err != nil {
		return nil, mapAgentSessionControlError(err)
	}
	if session == nil {
		return nil, serviceErrors.ErrChatAgentSessionNotFound
	}
	return session, nil
}

func mapAgentSessionControlError(err error) error {
	if errors.Is(err, common_persistence.ErrChatAgentRunDriveLeaseLost) {
		return err
	}
	if errors.Is(err, common_persistence.ErrChatAgentSessionNotFound) {
		return serviceErrors.ErrChatAgentSessionNotFound
	}
	return serviceErrors.ErrDatabaseError
}
