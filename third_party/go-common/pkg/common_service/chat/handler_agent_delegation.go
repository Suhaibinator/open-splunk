package chat

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	sm "github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

const (
	// delegationIdempotencyKeyPrefix namespaces the spawn tool's idempotency key so it can never
	// collide with an application's own CreateAgentChatSession keys for the same creator.
	delegationIdempotencyKeyPrefix = "delegation:"
	// delegationTitleMetadataKey carries the spawn title on the child session for readers that
	// have the session but not its thread root.
	delegationTitleMetadataKey = "delegation.title"
)

// SpawnDelegatedAgentSessionInput is the service-authority spawn a delegating agent's tool call
// performs. It is a Go service contract for the library's spawn_subagent executor, not a public
// HTTP/proto API. The context actor must be the parent session's bot, and an ambient transaction
// with post-commit hooks is required: the root, session, config snapshot, instructions row, and
// first run commit together, and the drive fires after commit.
type SpawnDelegatedAgentSessionInput struct {
	// ParentAgentSessionID is the main-timeline session doing the delegating.
	ParentAgentSessionID uint64
	// ParentAgentRunID is the parent run whose tool call spawns the child. It is recorded on the
	// child so wake runs can act for the human behind it.
	ParentAgentRunID uint64
	// AgentType is the child agent type; it must be declared in the parent's Delegation.
	AgentType string
	// Title becomes the thread root on the main timeline, posted as the parent bot.
	Title string
	// Instructions is the child's initial user message, posted in the thread as the parent bot.
	Instructions string
	// MaxModelSteps overrides the child agent's default; nil keeps it. It can only lower a nonzero
	// configured cap (the child's MaxModelSteps is a cost guard the parent cannot lift): zero is
	// rejected, and a larger value is clamped to the child's cap.
	MaxModelSteps *uint32
	// IdempotencyKey is the tool call's key. A replay returns the existing child.
	IdempotencyKey string
	// Scope is the tool call's optional scope argument, checked by the child type's
	// ports.ChildScopePolicy. Nil when the call supplied none.
	Scope jsontext.Value
}

// DelegatedChild is one child of a delegation parent with the state a status reader needs.
type DelegatedChild struct {
	Session *sm.ChatAgentSession
	// LatestRun is the child's newest run at its thread, or nil when it has none.
	LatestRun *sm.ChatAgentRun
	// LastReport is the newest visible run-tagged broadcast row of the child's thread (a report,
	// the closing message, or a notice), or nil when there is none.
	LastReport *sm.ChatMessage
	// PendingMessageID is the child's pending wake: the newest parent message a continue send
	// recorded while the child had an active run, which the run's terminal drain will deliver or
	// discard. Zero when nothing is pending.
	PendingMessageID uint64
}

// DelegatedChildrenPageRequest selects one page of a delegation parent's children, oldest first.
type DelegatedChildrenPageRequest struct {
	// IncludeCompleted opts into lifetime history, including idle and archived children.
	// By default only children with active runs are returned.
	IncludeCompleted bool
	// AfterChildSessionID is the previous page's NextAfterChildSessionID; zero starts at the oldest
	// child.
	AfterChildSessionID uint64
	// Limit is the page size. Zero or negative, or a value above
	// chat_agent_delegated_children_page_max (hot KMS, sampled once per page), takes that maximum.
	Limit int
}

// DelegatedChildrenPage is one page of a delegation parent's children, oldest first.
type DelegatedChildrenPage struct {
	Children []*DelegatedChild
	// HasMore reports that the parent had further children when the page was read.
	HasMore bool
	// NextAfterChildSessionID is the cursor for the next page: the session ID of the page's last
	// child when HasMore, zero otherwise.
	NextAfterChildSessionID uint64
}

// DelegationState is what a parent's live prompt state and status tooling need about its children.
type DelegationState struct {
	// Children are at most chat_agent_delegation_state_max_children of the parent's
	// children with active runs, oldest first. Empty for non-parents.
	Children []*DelegatedChild
	// TotalChildren is a lower bound on active children, including a probe when more exist.
	// Use the paginated child API with IncludeCompleted for lifetime history.
	TotalChildren uint32
	// PendingWakeCount counts delegation wakes deferred because the parent already had an active
	// run; the active run's terminal write drains them into one follow-up run.
	PendingWakeCount uint32
	// ReportExcerptRunes is the child report excerpt length FormatDelegationStateBlock renders,
	// sampled from chat_agent_delegation_report_excerpt_runes by the read that built this state.
	ReportExcerptRunes int
}

// SpawnDelegatedAgentSession creates a delegated child agent session in a new thread of the parent's
// channel: the thread root on the main timeline (as the parent bot), the child session linked to the
// parent, its version-1 config snapshot, the instructions as the child's first user message, and
// the child's first run, all in the ambient transaction. It is the AGENT origin of
// createDelegatedChildren: the actor must be the parent's bot, the spawning run is fenced, the child
// acts for that run's human, and the spawn permission hook decides. The idempotency key is checked
// before any cap, so a retried tool call replays the existing child instead of spawning a second one.
func (h *Handler[U]) SpawnDelegatedAgentSession(ctx context.Context, in SpawnDelegatedAgentSessionInput) (*CreateAgentChatSessionResult, error) {
	if h.chatDisabled() {
		return nil, serviceErrors.ErrChatDisabled
	}
	if in.ParentAgentSessionID == 0 || in.ParentAgentRunID == 0 || in.IdempotencyKey == "" ||
		(in.MaxModelSteps != nil && *in.MaxModelSteps == 0) {
		return nil, serviceErrors.ErrInvalidRequest
	}
	child, err := newDelegatedChildRequest(delegationIdempotencyKeyPrefix+in.IdempotencyKey, in.AgentType, in.Title, in.Instructions, in.Scope)
	if err != nil {
		return nil, err
	}
	child.maxModelSteps = in.MaxModelSteps
	if err := h.requireAgentCreateContext(ctx); err != nil {
		return nil, err
	}
	actor, actorID, err := h.getActor(ctx)
	if err != nil {
		return nil, err
	}
	parent, err := delegationParent(h.persistenceHandler.GetChatAgentSessionByID(ctx, in.ParentAgentSessionID))
	if err != nil {
		return nil, err
	}
	if parent.BotUserID != actorID {
		return nil, serviceErrors.ErrInsufficientPermissions
	}
	parentRun, err := persistence.LockChatAgentDelegationOrigin(ctx, h.persistenceHandler, in.ParentAgentRunID)
	if err != nil {
		if errors.Is(err, persistence.ErrChatAgentRunNotFound) {
			return nil, serviceErrors.ErrChatAgentRunNotFound
		}
		return nil, serviceErrors.ErrDatabaseError
	}
	if common_util.Deref(parentRun.AgentSessionID) != parent.ID {
		return nil, serviceErrors.ErrChatAgentDelegationParentInvalid
	}
	results, err := h.createDelegatedChildren(ctx, delegatedCreation[U]{
		origin: proto_models.DelegationCreationOrigin_DELEGATION_CREATION_ORIGIN_AGENT,
		actor:  actor, actorID: actorID, parent: parent, parentRun: parentRun,
		effectiveUserID: parent.UserID,
		children:        []delegatedChildRequest{child},
		limits:          sampleDelegatedChildLimits(h.policy()),
	})
	if err != nil {
		return nil, err
	}
	return results[0], nil
}

// RunDelegation returns the delegation run works under: agent's registered delegation narrowed by
// the session override captured in the config version the run pinned, so an override change applies
// from the session's next run. It is nil when agent does not delegate.
func RunDelegation(ctx context.Context, p persistence.CommonPersistenceInterface, agent ports.AgentDefinition, run *pm.ChatAgentRun) (*ports.AgentDelegation, error) {
	if run.AgentSessionID == nil || run.AgentSessionConfigVersion == nil {
		return agent.SessionDelegation(nil)
	}
	return SessionConfigDelegation(ctx, p, agent, *run.AgentSessionID, *run.AgentSessionConfigVersion)
}

// SessionConfigDelegation is agent's registered delegation narrowed by the override captured in one
// config version of the session; nil when agent does not delegate.
func SessionConfigDelegation(ctx context.Context, p persistence.CommonPersistenceInterface, agent ports.AgentDefinition, sessionID, configVersion uint64) (*ports.AgentDelegation, error) {
	if !agent.IsDelegating() {
		return nil, nil
	}
	snapshot, _, err := p.GetChatAgentSessionConfigSnapshot(ctx, sessionID, configVersion)
	if err != nil {
		return nil, err
	}
	return agent.SessionDelegation(snapshot.DelegationOverridePayload)
}

// ValidateWorkflowGraphOwner fences check's run through its graph root's OwnerValidator
// (ports.ValidateGraphOwner), reading a child run's root session for its agent type. def is the
// definition of check.SessionID's agent.
func ValidateWorkflowGraphOwner(ctx context.Context, p persistence.CommonPersistenceInterface, registry ports.AgentRegistry, def ports.AgentDefinition, check ports.OwnerCheck) error {
	rootType := ""
	if check.RootSessionID != check.SessionID {
		root, err := p.GetChatAgentSessionByID(ctx, check.RootSessionID)
		if err != nil {
			return err
		}
		rootType = root.AgentType
	}
	return ports.ValidateGraphOwner(ctx, registry, def, rootType, check)
}

// validateDelegationParent applies the parent-side rules that hold whichever way the row was loaded:
// a main-timeline session, not itself a delegated child, still active, and not suspended by a
// person's Stop. The locked re-read makes the last airtight: the parent row it holds FOR SHARE
// conflicts with the Stop's session lock, so a Stop that commits first is seen here, and one that
// waits for the spawn finds the new child working and cancels it.
func validateDelegationParent(parent *pm.ChatAgentSession) error {
	if parent == nil {
		return serviceErrors.ErrChatAgentSessionNotFound
	}
	if !parent.CanDelegate() || parent.IsSuspended() {
		return serviceErrors.ErrChatAgentDelegationParentInvalid
	}
	return nil
}

// clampDelegatedMaxModelSteps applies a parent's requested step cap to a child. A child that
// declares a nonzero MaxModelSteps keeps it as the ceiling; the parent (possibly steered by
// injected content) may only lower it. Without a requested value the child's default applies.
func clampDelegatedMaxModelSteps(requested *uint32, childCap uint32) *uint32 {
	if requested == nil {
		return nil
	}
	value := *requested
	if childCap > 0 && value > childCap {
		value = childCap
	}
	return &value
}

// ListDelegatedChildren returns one page of the parent's active children, oldest first, each with its
// latest run and last report. The actor must be able to read the parent's channel. Pages are keyed
// on the child session ID, so a child spawned while a caller pages through lands on a later page and
// never shifts or repeats an earlier one. IncludeCompleted opts into lifetime history.
func (h *Handler[U]) ListDelegatedChildren(ctx context.Context, parentSessionID uint64, page DelegatedChildrenPageRequest) (*DelegatedChildrenPage, error) {
	if h.chatDisabled() {
		return nil, serviceErrors.ErrChatDisabled
	}
	if parentSessionID == 0 {
		return nil, serviceErrors.ErrInvalidRequest
	}
	actor, _, err := h.getActor(ctx)
	if err != nil {
		return nil, err
	}
	parent, err := h.persistenceHandler.GetChatAgentSessionByID(ctx, parentSessionID)
	if err != nil {
		return nil, mapAgentSessionLocationError(err)
	}
	if _, err := h.readChannelAuthorized(ctx, actor, parent.ChannelID); err != nil {
		return nil, err
	}
	return h.DelegatedChildrenForSession(ctx, parent, page)
}

// DelegatedChildrenForSession is the internal page read for an already authorized
// or tool-owned parent. Callers establish authority before invoking it.
func (h *Handler[U]) DelegatedChildrenForSession(ctx context.Context, parent *pm.ChatAgentSession, page DelegatedChildrenPageRequest) (*DelegatedChildrenPage, error) {
	if parent == nil {
		return nil, serviceErrors.ErrChatAgentSessionNotFound
	}
	limit := h.delegatedChildrenPageLimit(page.Limit)
	// One row past the page tells whether another page exists without a count query.
	var sessions []*pm.ChatAgentSession
	var err error
	if page.IncludeCompleted {
		sessions, err = h.persistenceHandler.ListChatAgentSessionsByParentAfterID(ctx, parent.ID, page.AfterChildSessionID, limit+1)
	} else {
		sessions, err = h.persistenceHandler.ListActiveChatAgentSessionsByParentAfterID(ctx, parent.ID, page.AfterChildSessionID, limit+1)
	}
	if err != nil {
		return nil, mapAgentSessionLocationError(err)
	}
	out := &DelegatedChildrenPage{}
	if len(sessions) > limit {
		sessions = sessions[:limit]
		out.HasMore = true
		out.NextAfterChildSessionID = sessions[limit-1].ID
	}
	if out.Children, err = h.buildDelegatedChildren(ctx, parent.ChannelID, sessions); err != nil {
		return nil, err
	}
	return out, nil
}

// DelegationStateForSession loads the live delegation state of one session for prompt state and
// tooling. It carries no authority check: callers are service-internal (the runtime's live-state
// provider and the library tool executors, which authorize on their own). A session that is not a
// main-timeline session returns an empty state. The read is bounded: at most
// chat_agent_delegation_state_max_children active children (hot KMS, sampled
// once per read), never fewer than the session type's registered active-children cap, plus a
// one-row continuation probe. Completed history is opt-in.
func (h *Handler[U]) DelegationStateForSession(ctx context.Context, sessionID uint64) (*DelegationState, error) {
	if sessionID == 0 {
		return nil, serviceErrors.ErrInvalidRequest
	}
	session, err := h.persistenceHandler.GetChatAgentSessionByID(ctx, sessionID)
	if err != nil {
		return nil, mapAgentSessionLocationError(err)
	}
	if !session.IsMainTimeline() {
		return &DelegationState{}, nil
	}
	cfg := h.policy()
	maxChildren := cfg.ChatAgentDelegationStateMaxChildren()
	// Validation holds the read to the registration bounds a candidate states, but the registry
	// captured its bounds at startup, so a reload that lowers both can fall below a registered cap.
	// Admission holds the parent to that cap, so reading that many keeps every active child visible.
	if agent, ok := h.agentRegistry.GetAgentType(session.AgentType); ok {
		maxChildren = max(maxChildren, int(agent.Delegation.EffectiveMaxActiveChildren()))
	}
	sessions, err := h.persistenceHandler.ListActiveChatAgentSessionsByParentAfterID(ctx, session.ID, 0, maxChildren+1)
	if err != nil {
		return nil, mapAgentSessionLocationError(err)
	}
	total := uint32(len(sessions))
	if len(sessions) > maxChildren {
		sessions = sessions[:maxChildren]
	}
	children, err := h.buildDelegatedChildren(ctx, session.ChannelID, sessions)
	if err != nil {
		return nil, err
	}
	return &DelegationState{
		Children:           children,
		TotalChildren:      max(total, uint32(len(children))),
		PendingWakeCount:   session.PendingWakeCount,
		ReportExcerptRunes: cfg.ChatAgentDelegationReportExcerptRunes(),
	}, nil
}

// DescribeDelegatedChild joins one child session, already loaded and owned by the caller's parent,
// with its latest run and last report. Like DelegationStateForSession it carries no authority or
// ownership check: the library tool executors check that the child is theirs before calling it.
func (h *Handler[U]) DescribeDelegatedChild(ctx context.Context, child *pm.ChatAgentSession) (*DelegatedChild, error) {
	if child == nil {
		return nil, serviceErrors.ErrChatAgentSessionNotFound
	}
	children, err := h.buildDelegatedChildren(ctx, child.ChannelID, []*pm.ChatAgentSession{child})
	if err != nil {
		return nil, err
	}
	return children[0], nil
}

// buildDelegatedChildren joins child sessions with the newest run and newest broadcast row of each
// child's thread, keeping the sessions' order. Children live in the parent's channel, one per
// thread root.
func (h *Handler[U]) buildDelegatedChildren(ctx context.Context, channelID uint64, sessions []*pm.ChatAgentSession) ([]*DelegatedChild, error) {
	if len(sessions) == 0 {
		return nil, nil
	}
	roots := make([]uint64, 0, len(sessions))
	for _, child := range sessions {
		if child != nil && child.ParentMessageID != 0 {
			roots = append(roots, child.ParentMessageID)
		}
	}
	runs, err := h.persistenceHandler.ListLatestChatAgentRunsByThread(ctx, channelID, roots, 1)
	if err != nil {
		return nil, serviceErrors.ErrDatabaseError
	}
	latestByRoot := make(map[uint64]*pm.ChatAgentRun, len(roots))
	for _, run := range runs {
		// Runs arrive newest first within a location, so the first one seen is the latest.
		if run != nil {
			if _, seen := latestByRoot[run.ParentMessageID]; !seen {
				latestByRoot[run.ParentMessageID] = run
			}
		}
	}
	reports, err := h.persistenceHandler.ListLatestDelegationReportsByThread(ctx, channelID, roots)
	if err != nil {
		return nil, serviceErrors.ErrDatabaseError
	}
	out := make([]*DelegatedChild, 0, len(sessions))
	for _, child := range sessions {
		if child == nil {
			continue
		}
		entry := &DelegatedChild{Session: h.converter.PersistenceChatAgentSessionToService(child), PendingMessageID: common_util.Deref(child.PendingWakeMessageID)}
		if run := latestByRoot[child.ParentMessageID]; run != nil {
			entry.LatestRun = h.converter.PersistenceChatAgentRunToService(run)
		}
		if report := reports[child.ParentMessageID]; report != nil {
			entry.LastReport = h.converter.PersistenceChatMessageToService(report)
		}
		out = append(out, entry)
	}
	return out, nil
}

// SettleAgentRunAfterCommit finishes a durable terminal run transition after commit. Persistence
// already did the transactional part in the terminal write (the drained follow-up that the
// session's queued input was delivered into as messages, a delegated child's notice row and
// broadcast flip, the parent's wake run or pending-wake record, a parent's drained wake); what
// remains is post-commit: the realtime MESSAGE_SENT for each message a drain delivered and for a
// child's notice, then the drive of every run the ending minted. Durable notifications for
// delivered messages and delegation notices were recorded in the terminal write. It runs after
// every terminal transition app-wide, which costs one run read and one lookup of the runs the ending minted, plus
// one lookup of a drained run's messages. Immediate post-commit work is best-effort and idempotent:
// recovery claims un-driven RUNNING runs; async notification workers drain committed emissions.
func (h *Handler[U]) SettleAgentRunAfterCommit(ctx context.Context, runID uint64) {
	if h == nil || runID == 0 {
		return
	}
	ctx = persistence.WithoutRequestTransaction[U](context.WithoutCancel(ctx))
	run, err := h.persistenceHandler.GetChatAgentRunByID(ctx, runID)
	if err != nil {
		h.logSettleSkip(ctx, "load_settled_run", runID, err)
		return
	}
	if !persistence.IsTerminalChatAgentRunStatus(run.Status) || isNilOrZero(run.AgentSessionID) {
		return
	}
	started, err := h.persistenceHandler.ListChatAgentRunsStartedBy(ctx, runID)
	if err != nil {
		h.logSettleSkip(ctx, "load_runs_started_by_ending", runID, err)
	}
	// The session hears of its queue change once, with the input the ending drained, before the run
	// that input starts. A person's Stop announced the change itself, and drains nothing. Every
	// other ending publishes, deliberately even when it drained nothing: a drain whose inputs were
	// all discarded starts no run and leaves no other trace to publish on.
	var drained []uint64
	for _, next := range started {
		if next != nil && next.InputDrainedFromRunID != nil {
			drained = h.drainedAgentInput(ctx, next)
		}
	}
	if len(drained) > 0 || !isPersonStop(run) {
		h.PublishDeliveredAgentInputAfterCommit(ctx, *run.AgentSessionID, run.ChannelID, run.ParentMessageID, drained)
	}
	h.publishDelegationNoticeAfterCommit(ctx, run)
	for _, next := range started {
		h.StartMintedAgentRunAfterCommit(ctx, next)
	}
}

// StartMintedAgentRunAfterCommit publishes and drives a RUNNING run persistence minted on its own
// (a delegation wake or a drained follow-up), after the ambient transaction commits or now without
// one. A run of an owner-driven session is minted but left for its owner.
func (h *Handler[U]) StartMintedAgentRunAfterCommit(ctx context.Context, run *pm.ChatAgentRun) {
	if h == nil || run == nil || run.Status != proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING {
		return
	}
	h.publishAgentRunStartedAfterCommit(ctx, run.ID, run.ChannelID, run.ParentMessageID)
	if !h.leaveRunForOwner(ctx, "settle_started_run", run) {
		h.scheduleAgentRunDrive(ctx, run.ID)
	}
}

// drainedAgentInput lists the messages the session's queued input became when the ending drained
// it into run, the follow-up run of the same session, found by the run they were delivered into.
// The drain made its newest message the run's trigger; input the run's own steps took later was
// announced by those steps.
func (h *Handler[U]) drainedAgentInput(ctx context.Context, run *pm.ChatAgentRun) []uint64 {
	messageIDs, err := h.persistenceHandler.ListChatAgentRunAdmittedMessageIDs(ctx, run.ID, common_util.Deref(run.TriggeringMessageID))
	if err != nil {
		h.logSettleSkip(ctx, "load_drained_agent_input", run.ID, err)
		return nil
	}
	return messageIDs
}

func (h *Handler[U]) logSettleSkip(ctx context.Context, stage string, runID uint64, err error) {
	appLogger.For(ctx, h.logger, chatLoggerName).Warn("agent run settlement skipped",
		zap.String(logkeys.Stage, stage),
		zap.String(logkeys.Fallback, "skip_agent_run_settlement"),
		zap.Uint64(appLogger.KeyAgentRunID, runID),
		zap.Error(err))
}

// publishDelegationNoticeAfterCommit publishes the notice a delegated child's ending
// posted; only a delegated child's run carries one.
//
// Only the notice is published here. A normal ending's closing message was broadcast by the
// transcript writer when it was written, and the writer already published its full row, so a
// second MESSAGE_SENT would only duplicate the fan-out. (A custom writer that skipped the flag gets
// it set durably by settlement; clients pick that row up on their next read.)
func (h *Handler[U]) publishDelegationNoticeAfterCommit(ctx context.Context, run *pm.ChatAgentRun) {
	noticeID := common_util.Deref(run.DelegationNoticeMessageID)
	if noticeID == 0 {
		return
	}
	row, err := h.persistenceHandler.GetChatMessageByID(ctx, noticeID)
	switch {
	case err == nil && row != nil:
		h.publishSettledRowSent(ctx, row)
	case err != nil && !errors.Is(err, persistence.ErrChatMessageNotFound):
		h.logSettleSkip(ctx, "load_delegation_notice_row", run.ID, err)
	}
}

// PublishAgentRunNoticeAfterCommit publishes MESSAGE_SENT for the generic SYSTEM_EVENT notice a
// terminal write inserted (TerminalizeChatAgentRunWithNotice, FailChatAgentRunWithNotice), after
// the ambient transaction commits, or immediately without one. The notice is written inside the
// terminal transaction, so nothing else announces it. A delegated child's notice is a STANDARD row
// that SettleAgentRunAfterCommit publishes; this skips it rather than publishing it twice.
func (h *Handler[U]) PublishAgentRunNoticeAfterCommit(ctx context.Context, messageID uint64) {
	if h == nil || messageID == 0 || h.eventPublisher == nil {
		return
	}
	publish := func(publishCtx context.Context) {
		publishCtx = persistence.WithoutRequestTransaction[U](context.WithoutCancel(publishCtx))
		row, err := h.persistenceHandler.GetChatMessageByID(publishCtx, messageID)
		if err != nil || row == nil {
			appLogger.For(publishCtx, h.logger, chatLoggerName).Warn("failed to load agent run notice for realtime publication",
				zap.String(logkeys.Stage, "publish_agent_run_notice"),
				zap.String(logkeys.Fallback, "skip_realtime_publication"),
				zap.Uint64(appLogger.KeyMessageID, messageID),
				zap.Error(err))
			return
		}
		if row.Kind != proto_models.MessageKind_MESSAGE_KIND_SYSTEM_EVENT {
			return
		}
		h.publishSettledRowSent(publishCtx, row)
	}
	if persistence.AddPostCommitHookFromContext(ctx, func(hookCtx context.Context) error {
		publish(hookCtx)
		return nil
	}) {
		return
	}
	publish(ctx)
}

// publishSettledRowSent publishes one full-row MESSAGE_SENT for a row a terminal write inserted
// (a delegation notice or a generic run notice). It goes through the shared after-commit path, which publishes immediately when ctx
// carries no transaction, as it does not here.
func (h *Handler[U]) publishSettledRowSent(ctx context.Context, row *pm.ChatMessage) {
	if h.eventPublisher == nil || row == nil {
		return
	}
	message := h.converter.PersistenceChatMessageToService(row)
	if message == nil {
		return
	}
	if err := h.hydrateMessageAttachments(ctx, message); err != nil {
		appLogger.For(ctx, h.logger, chatLoggerName).Warn("failed to hydrate settled notice row",
			zap.String(logkeys.Stage, "publish_settled_row_sent"),
			zap.Uint64(appLogger.KeyMessageID, row.ID),
			zap.Error(err))
	}
	h.publishMessageSent(ctx, message)
}

// publishMessageSent publishes a full-row MESSAGE_SENT, from its sender, for a message something
// other than its send wrote (a run notice, delivered agent input), after the ambient transaction
// commits or now without one.
func (h *Handler[U]) publishMessageSent(ctx context.Context, message *sm.ChatMessage) {
	if message == nil {
		return
	}
	h.publishChatEventAfterCommit(ctx, func(context.Context) *ports.ChatEvent {
		return &ports.ChatEvent{
			Type:        proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_SENT,
			OccurredAt:  time.Now(),
			ActorUserID: message.SenderID,
			Message:     message,
		}
	})
}

// wakeDelegationParent is the report branch of a send: a broadcast from the bot of one of parent's
// delegated children (a report_to_parent row, or any other broadcast the child bot sends) wakes the
// parent through the persistence coalescer instead of minting an unconditional run, so with an
// active parent run it is recorded as a pending wake. A minted run is driven after commit. Failures
// are logged; the report itself is written either way.
func (h *Handler[U]) wakeDelegationParent(ctx context.Context, child, parent *pm.ChatAgentSession, message *sm.ChatMessage) {
	result, err := h.persistenceHandler.CreateOrCoalesceParentWakeRun(ctx, persistence.ChatAgentParentWakeInput{
		ChildAgentSessionID: child.ID,
		TriggerMessageID:    uint64(message.ID),
	})
	if err != nil {
		appLogger.For(ctx, h.logger, chatLoggerName).Error("failed to wake delegation parent",
			zap.String(logkeys.Stage, "create_or_coalesce_parent_wake_run"),
			zap.Uint64(appLogger.KeyChannelID, parent.ChannelID),
			zap.Uint64(appLogger.KeyAgentSessionID, parent.ID),
			zap.Uint64(appLogger.KeyMessageID, uint64(message.ID)),
			zap.Error(err))
		return
	}
	switch result.State {
	case proto_models.DelegationWakeState_DELEGATION_WAKE_STATE_RUN_CREATED:
		// A wake run is created at its parent session's location.
		h.startAgentRunAfterCommit(ctx, parent.AgentType, result.RunID, parent.ChannelID, parent.ParentMessageID)
	default:
		// Deferred to the child's wake group, coalesced into the active parent run, or skipped.
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, chatLoggerName).Debug("delegation wake not run",
				zap.Uint64(appLogger.KeyAgentSessionID, parent.ID),
				zap.Uint64(appLogger.KeyMessageID, uint64(message.ID)),
				zap.Stringer(appLogger.KeyOutcome, result.State),
				zap.Uint32(appLogger.KeyCount, result.PendingWakeCount),
				zap.String(logkeys.Reason, result.SkipReason))
		}
	}
}

// validateSendAgentRun checks that a run-tagged send belongs to the run: same channel, same thread
// location, and sent by the run's own bot. Anything else could tag a stranger's message as a run's
// report.
func (h *Handler[U]) validateSendAgentRun(ctx context.Context, runID, channelID, parentMessageID, senderID uint64) error {
	run, err := h.persistenceHandler.GetChatAgentRunByID(ctx, runID)
	if err != nil {
		if errors.Is(err, persistence.ErrChatAgentRunNotFound) {
			return fmt.Errorf("%w: agent run %d not found", serviceErrors.ErrInvalidRequest, runID)
		}
		return serviceErrors.ErrDatabaseError
	}
	if run.ChannelID != channelID || run.ParentMessageID != parentMessageID || run.BotUserID != senderID {
		return fmt.Errorf("%w: agent run does not belong to this sender and location", serviceErrors.ErrInvalidRequest)
	}
	return nil
}

// delegatedChildrenPageLimit resolves a ListDelegatedChildren page size against
// chat_agent_delegated_children_page_max, sampled once per page: an unset or oversized request
// takes the maximum.
func (h *Handler[U]) delegatedChildrenPageLimit(requested int) int {
	pageMax := h.policy().ChatAgentDelegatedChildrenPageMax()
	if requested <= 0 {
		return pageMax
	}
	return min(requested, pageMax)
}
