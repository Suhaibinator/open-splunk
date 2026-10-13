package common_persistence

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_metrics"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/lib/pq"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Library-owned delegation tool names. Their arguments and results name chat IDs, which a fork
// remaps onto its copies.
const (
	// DelegationToolSpawn starts a delegated child agent session in a new thread.
	DelegationToolSpawn = "spawn_subagent"
	DelegationToolRead  = "read_subagent_messages"
	DelegationToolSend  = "send_subagent_message"
	// DelegationToolStatus reads the parent's delegated children and their latest runs.
	DelegationToolStatus = "subagent_status"
	// DelegationToolCancel cancels a delegated child's active run and archives the child.
	DelegationToolCancel = "cancel_subagent"
	// DelegationToolReport posts a progress report from a child to the main timeline.
	DelegationToolReport = "report_to_parent"
)

var delegationToolNames = []string{DelegationToolSpawn, DelegationToolRead, DelegationToolSend, DelegationToolStatus, DelegationToolCancel, DelegationToolReport}

// IsDelegationToolName reports whether name is one of the library-owned delegation tools.
func IsDelegationToolName(name string) bool { return slices.Contains(delegationToolNames, name) }

// Explicit timestamps preserve the clock chosen by each delegation transition.
type chatMessageBroadcastUpdate struct {
	BroadcastToChannel bool
	UpdatedAt          time.Time `gorm:"autoUpdateTime:false"`
}

type chatAgentDelegationRunUpdate struct {
	DelegationNoticeMessageID *uint64
	DelegationWakeState       proto_models.DelegationWakeState
	UpdatedAt                 clause.Expr `gorm:"type:timestamptz;autoUpdateTime:false"`
}

type chatAgentPendingWakeClearUpdate struct {
	PendingWakeMessageID *uint64
	PendingWakeCount     uint32
	UpdatedAt            time.Time `gorm:"autoUpdateTime:false"`
}

// Cancellation codes recorded on delegated child runs. ports re-exports them as
// AgentRunCancellationCodeDelegation* for the service layer.
const (
	// DelegationCancelledByParentCode marks a child run its parent cancelled. The parent asked for
	// the stop, so settlement posts the notice but never wakes the parent for it.
	DelegationCancelledByParentCode = "delegation_cancelled_by_parent"
	// DelegationParentArchivedCode marks a child run retired because its parent session was archived.
	DelegationParentArchivedCode = "delegation_parent_archived"
	// DelegationParentStoppedCode marks a child run cancelled, with reason STOPPED_BY_USER, because
	// a person stopped its parent session. The parent is suspended and not waiting for the child,
	// so settlement posts the notice but never wakes the parent, not even through a pending wake.
	DelegationParentStoppedCode = "delegation_parent_stopped"
)

// Generic visible lifecycle events written atomically with a terminal transition. Internal causes
// remain in service logs; these texts are the whole user-facing story.
const (
	// ChatAgentRunStepLimitNoticeContent is the SYSTEM_EVENT of a MODEL_STEP_LIMIT_EXCEEDED stop.
	ChatAgentRunStepLimitNoticeContent = "The assistant stopped because this task reached its step limit. Send another message to continue."
	// ChatAgentRunBudgetNoticeContent is the SYSTEM_EVENT of a cumulative work-budget stop.
	ChatAgentRunBudgetNoticeContent = "The assistant stopped because this task reached its work limit. Send another message to continue."
)

// Delegation notices are posted as the child bot's own visible message, in the thread and on the
// main timeline, so they read in the first person.
const (
	chatAgentDelegationNoticeCancelledByParent  = "I stopped this task because the lead agent cancelled it."
	chatAgentDelegationNoticeParentArchived     = "I stopped this task because the lead conversation was archived."
	chatAgentDelegationNoticeCancelled          = "I stopped this task because it was cancelled."
	chatAgentDelegationNoticeStoppedByUser      = "I stopped this task because someone stopped it from the chat."
	chatAgentDelegationNoticeParentStopped      = "I stopped this task because someone stopped the lead agent."
	chatAgentDelegationNoticeStepLimit          = "I stopped before finishing: this task reached its step limit."
	chatAgentDelegationNoticeBudget             = "I stopped before finishing: this task reached its work limit."
	chatAgentDelegationNoticeApprovalNotGranted = "I stopped before finishing: a required approval was not granted."
	chatAgentDelegationNoticeFailed             = "I stopped before finishing: I hit an error."
	chatAgentDelegationNoticeNoReply            = "I finished this task without a final reply."
)

// chatAgentRunStop classifies why a run ended without a normal answer; the two notice tables below
// turn it into visible text.
type chatAgentRunStop int

const (
	chatAgentRunStopFailed chatAgentRunStop = iota
	chatAgentRunStopStepLimit
	chatAgentRunStopBudget
	chatAgentRunStopApprovalNotGranted
	chatAgentRunStopCancelledByParent
	chatAgentRunStopParentArchived
	chatAgentRunStopCancelled
	chatAgentRunStopStoppedByUser
	chatAgentRunStopParentStopped
	chatAgentRunStopNoReply
)

// classifyChatAgentRunStop classifies a terminal run from the status, reason, and reason code
// recorded on it. A COMPLETED run with an unspecified reason is classified as ending without a
// reply: only a completion that produced no closing message is ever asked about.
func classifyChatAgentRunStop(status proto_models.ChatAgentRunStatus, reason proto_models.ChatAgentRunStatusReason, reasonCode string) chatAgentRunStop {
	switch status {
	case proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_CANCELLED:
		switch {
		case reasonCode == DelegationCancelledByParentCode:
			return chatAgentRunStopCancelledByParent
		case reasonCode == DelegationParentArchivedCode,
			reason == proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_PARENT_WORKFLOW_TERMINATED:
			return chatAgentRunStopParentArchived
		case chatAgentRunStoppedWithParent(reason, reasonCode):
			return chatAgentRunStopParentStopped
		case reason == proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_STOPPED_BY_USER:
			return chatAgentRunStopStoppedByUser
		}
		return chatAgentRunStopCancelled
	case proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_MODEL_STEP_LIMIT_EXCEEDED:
		return chatAgentRunStopStepLimit
	}
	switch reason {
	case proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_MODEL_STEP_LIMIT:
		return chatAgentRunStopStepLimit
	case proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_BUDGET_ACTIVE_DURATION,
		proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_BUDGET_GENERATIONS,
		proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_BUDGET_TOOL_CALLS:
		return chatAgentRunStopBudget
	case proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_APPROVAL_NOT_GRANTED:
		return chatAgentRunStopApprovalNotGranted
	}
	if status == proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_COMPLETED {
		return chatAgentRunStopNoReply
	}
	return chatAgentRunStopFailed
}

// chatAgentRunNoticeTexts are the generic SYSTEM_EVENT texts; a stop without an entry uses
// ChatAgentRunFailureNoticeContent.
var chatAgentRunNoticeTexts = map[chatAgentRunStop]string{
	chatAgentRunStopStepLimit: ChatAgentRunStepLimitNoticeContent,
	chatAgentRunStopBudget:    ChatAgentRunBudgetNoticeContent,
}

// chatAgentDelegationNoticeTexts are a delegated child's first-person notices, one per stop.
var chatAgentDelegationNoticeTexts = map[chatAgentRunStop]string{
	chatAgentRunStopFailed:             chatAgentDelegationNoticeFailed,
	chatAgentRunStopStepLimit:          chatAgentDelegationNoticeStepLimit,
	chatAgentRunStopBudget:             chatAgentDelegationNoticeBudget,
	chatAgentRunStopApprovalNotGranted: chatAgentDelegationNoticeApprovalNotGranted,
	chatAgentRunStopCancelledByParent:  chatAgentDelegationNoticeCancelledByParent,
	chatAgentRunStopParentArchived:     chatAgentDelegationNoticeParentArchived,
	chatAgentRunStopCancelled:          chatAgentDelegationNoticeCancelled,
	chatAgentRunStopStoppedByUser:      chatAgentDelegationNoticeStoppedByUser,
	chatAgentRunStopParentStopped:      chatAgentDelegationNoticeParentStopped,
	chatAgentRunStopNoReply:            chatAgentDelegationNoticeNoReply,
}

// chatAgentRunStoppedWithParent reports whether a cancelled child run was stopped because a person
// stopped its parent. Applications cannot record STOPPED_BY_USER, so the code alone cannot make a
// run look like one.
func chatAgentRunStoppedWithParent(reason proto_models.ChatAgentRunStatusReason, reasonCode string) bool {
	return reason == proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_STOPPED_BY_USER &&
		reasonCode == DelegationParentStoppedCode
}

// ChatAgentRunNoticeContent is the generic SYSTEM_EVENT text a non-delegated run's terminal stop
// posts (TerminalizeChatAgentRunWithNotice): the step-limit or work-limit text, otherwise the
// generic failure text.
func ChatAgentRunNoticeContent(status proto_models.ChatAgentRunStatus, reason proto_models.ChatAgentRunStatusReason) string {
	if text, ok := chatAgentRunNoticeTexts[classifyChatAgentRunStop(status, reason, "")]; ok {
		return text
	}
	return ChatAgentRunFailureNoticeContent
}

// chatAgentDelegationNoticeContent is the visible text a delegated child run posts when it ends
// without a normal answer. status, reason, and reasonCode are the values recorded on the run.
func chatAgentDelegationNoticeContent(status proto_models.ChatAgentRunStatus, reason proto_models.ChatAgentRunStatusReason, reasonCode string) string {
	return chatAgentDelegationNoticeTexts[classifyChatAgentRunStop(status, reason, reasonCode)]
}

// ChatAgentRunBudgetCeilings are the work-budget ceilings stamped on runs that persistence mints on
// its own (delegation wake and drained follow-up runs). They mirror the service's AgentRunBudget.
type ChatAgentRunBudgetCeilings struct {
	MaxActiveDuration time.Duration
	MaxGenerations    uint64
	MaxToolCalls      uint64
}

// ChatAgentRunBudgetCeilingsProvider samples the current ceilings once per minted run, so a hot
// configuration change reaches persistence-minted runs exactly like service-minted ones.
type ChatAgentRunBudgetCeilingsProvider func() ChatAgentRunBudgetCeilings

// SetChatAgentRunBudgetCeilingsProvider installs the ceilings source. Persistence has no view of
// the chat configuration group, so the service wires this at startup; without it, minted runs carry
// zero ceilings, which the runtime reads as "use the current service ceilings".
func (h *CommonPersistenceHandler[U]) SetChatAgentRunBudgetCeilingsProvider(provider ChatAgentRunBudgetCeilingsProvider) {
	if provider == nil {
		h.agentRunBudgetCeilings.Store(nil)
		return
	}
	h.agentRunBudgetCeilings.Store(&provider)
}

// ChatAgentWorkflowTypesProvider lists the registered workflow-owned agent types, whose runs only
// their application's owner drive may drive. The chat service installs it at startup; it is sampled
// per use, so a type registered later is seen by the next wake or recovery scan.
type ChatAgentWorkflowTypesProvider func() []string

// SetChatAgentWorkflowTypesProvider installs the workflow-type source; nil uninstalls it, after which
// no type is owner-driven.
func (h *CommonPersistenceHandler[U]) SetChatAgentWorkflowTypesProvider(provider ChatAgentWorkflowTypesProvider) {
	if provider == nil {
		h.workflowAgentTypes.Store(nil)
		return
	}
	h.workflowAgentTypes.Store(&provider)
}

// chatAgentWorkflowTypes samples the installed workflow-type provider; empty without one. Never nil:
// pq.Array binds a nil slice as SQL NULL, and agent_type = ANY(NULL) is NULL rather than false.
func (h *CommonPersistenceHandler[U]) chatAgentWorkflowTypes() []string {
	if p := h.workflowAgentTypes.Load(); p != nil && *p != nil {
		if types := (*p)(); types != nil {
			return types
		}
	}
	return []string{}
}

// ApplyTo stamps the ceilings on a run being minted.
func (c ChatAgentRunBudgetCeilings) ApplyTo(run *common_persistence_models.ChatAgentRun) {
	if run == nil {
		return
	}
	run.MaxActiveDurationMs = uint64(max(c.MaxActiveDuration, 0).Milliseconds())
	run.MaxGenerations = c.MaxGenerations
	run.MaxToolCalls = c.MaxToolCalls
}

func (h *CommonPersistenceHandler[U]) chatAgentRunBudgetCeilings() ChatAgentRunBudgetCeilings {
	provider := h.agentRunBudgetCeilings.Load()
	if provider == nil || *provider == nil {
		return ChatAgentRunBudgetCeilings{}
	}
	return (*provider)()
}

// ChatAgentParentWakeInput asks persistence to wake, or coalesce a wake for, the parent of a
// delegated child session because of one trigger row on the main timeline (a report the child
// broadcast). Terminal endings settle through the run's own terminal write instead.
type ChatAgentParentWakeInput struct {
	ChildAgentSessionID uint64
	TriggerMessageID    uint64
}

// ChatAgentSessionWakeResult reports what a wake request did.
type ChatAgentSessionWakeResult struct {
	// State is RunCreated when a parent run was minted, Coalesced when the parent already had an
	// active run and the trigger was recorded on its pending fields, DeferredToGroup when another
	// member of the child's wake group is still working, and None when a guard or the child's
	// WAKE_POLICY_NEVER skipped the wake (SkipReason says which).
	State proto_models.DelegationWakeState
	// RunID is the minted parent run when State is RunCreated.
	RunID uint64
	// PendingWakeCount is the parent's deferred-wake count after coalescing.
	PendingWakeCount uint32
	// SkipReason names the failed guard when State is None.
	SkipReason string
}

// ActiveChatAgentRunStatuses are the non-terminal run statuses: RUNNING, WAITING_FOR_APPROVAL and
// RETRYABLE. Each call returns a fresh slice.
func ActiveChatAgentRunStatuses() []proto_models.ChatAgentRunStatus {
	return []proto_models.ChatAgentRunStatus{
		proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING,
		proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_WAITING_FOR_APPROVAL,
		proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RETRYABLE,
	}
}

// standardChatMessageKinds are the kinds that render as ordinary chat text. Rows written through
// SendChatMessage records the explicit STANDARD kind.
var standardChatMessageKinds = []proto_models.MessageKind{
	proto_models.MessageKind_MESSAGE_KIND_STANDARD,
}

// ============================================================================
// CHAT: DELEGATION READS
// ============================================================================

// ListChatAgentSessionsByParent returns every session spawned by one parent, archived children
// included, oldest first.
func (h *CommonPersistenceHandler[U]) ListChatAgentSessionsByParent(ctx context.Context, parentAgentSessionID uint64) ([]*common_persistence_models.ChatAgentSession, error) {
	if parentAgentSessionID == 0 {
		return nil, nil
	}
	var sessions []*common_persistence_models.ChatAgentSession
	if err := h.getDBFromContext(ctx).
		Where("parent_agent_session_id = ?", parentAgentSessionID).
		Order("id ASC").
		Find(&sessions).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_chat_agent_sessions_by_parent"),
			zap.Uint64(appLogger.KeyAgentSessionID, parentAgentSessionID))
	}
	return sessions, nil
}

// ListChatAgentSessionsByParentAfterID returns at most limit of one parent's children with an id
// above afterSessionID, archived children included, oldest first. It is the keyset page behind
// ListDelegatedChildren: ids are the stable order, so a child spawned between pages lands on a later
// page instead of shifting earlier ones. A non-positive limit returns no rows.
func (h *CommonPersistenceHandler[U]) ListChatAgentSessionsByParentAfterID(ctx context.Context, parentAgentSessionID, afterSessionID uint64, limit int) ([]*common_persistence_models.ChatAgentSession, error) {
	if parentAgentSessionID == 0 || limit <= 0 {
		return nil, nil
	}
	var sessions []*common_persistence_models.ChatAgentSession
	if err := h.getDBFromContext(ctx).
		Where("parent_agent_session_id = ? AND id > ?", parentAgentSessionID, afterSessionID).
		Order("id ASC").
		Limit(limit).
		Find(&sessions).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_chat_agent_sessions_by_parent_after_id"),
			zap.Uint64(appLogger.KeyAgentSessionID, parentAgentSessionID),
			zap.Uint64(appLogger.KeyCursor, afterSessionID),
			zap.Int(logkeys.Limit, limit))
	}
	return sessions, nil
}

// ListActiveChatAgentSessionsByParentAfterID pages unarchived children with an active run, the
// children MaxActiveChildren counts: an archived child whose run awaits its post-commit
// cancellation must not crowd a live replacement out of a bounded read.
func (h *CommonPersistenceHandler[U]) ListActiveChatAgentSessionsByParentAfterID(ctx context.Context, parentSessionID, afterSessionID uint64, limit int) ([]*common_persistence_models.ChatAgentSession, error) {
	if parentSessionID == 0 || limit <= 0 {
		return nil, nil
	}
	sessions := common_persistence_models.TableNameChatAgentSessions
	var children []*common_persistence_models.ChatAgentSession
	if err := h.getDBFromContext(ctx).
		Select(sessions+".*").
		Joins("JOIN "+common_persistence_models.TableNameChatAgentRuns+" active_run ON active_run.agent_session_id = "+sessions+".id").
		Where(sessions+".parent_agent_session_id = ? AND "+sessions+".id > ? AND "+sessions+".status = ?", parentSessionID, afterSessionID, common_persistence_models.ChatAgentSessionStatusActive).
		Where("active_run.status IN ?", ActiveChatAgentRunStatuses()).
		Order(sessions + ".id ASC").Limit(limit).Find(&children).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err, zap.String(logkeys.Stage, "list_active_delegated_children"), zap.Uint64(appLogger.KeyAgentSessionID, parentSessionID))
	}
	return children, nil
}

// ListRecentChatAgentSessionsByParent returns at most limit of one parent's children, archived ones
// included, and a lower bound on the parent's child count. Children with a RUNNING, WAITING_FOR_APPROVAL, or
// RETRYABLE run come first (a parent may have at most 32 of them, so a status reader never loses a
// working child to the cap), then the rest newest first. It is the bounded read behind a parent's
// live prompt state and status tool: a long-lived parent accumulates finished children it no
// longer acts on. Historical totals are deliberately not counted on this hot read. A non-positive
// limit returns no rows and zero; the returned count includes a one-row probe when more exist.
func (h *CommonPersistenceHandler[U]) ListRecentChatAgentSessionsByParent(ctx context.Context, parentAgentSessionID uint64, limit int) ([]*common_persistence_models.ChatAgentSession, uint32, error) {
	if parentAgentSessionID == 0 {
		return nil, 0, nil
	}
	bounded, err := h.boundedDelegatedSessions(ctx, common_persistence_models.TableNameChatAgentSessions+".parent_agent_session_id = ?", parentAgentSessionID, limit, common_persistence_models.TableNameChatAgentSessions+".*")
	if err != nil {
		return nil, 0, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_recent_chat_agent_sessions_by_parent"),
			zap.Uint64(appLogger.KeyAgentSessionID, parentAgentSessionID),
			zap.Int(logkeys.Limit, limit))
	}
	total := uint32(len(bounded.sessions))
	if bounded.more {
		total++
	}
	return bounded.sessions, total, nil
}

// DelegatedThreadRoots is the bounded, active-first read of one channel's delegated child threads.
type DelegatedThreadRoots struct {
	// Roots lists the thread roots of children with an active run first, newest first, then the
	// newest of the rest.
	Roots []uint64
	// Active is how many leading Roots hold a child with an active run.
	Active int
	// MoreActive reports children with an active run beyond the limit, none of them in Roots.
	MoreActive bool
}

// ListDelegatedThreadRootsByChannel returns the thread roots of at most limit delegated child
// sessions in one channel, children with an active run first and then newest first, and how many
// of them hold an active run. It is the bounded read behind the delegated run-status append: a
// channel that has spawned many children over its lifetime must still report the ones that are
// running now, and say when it could not.
func (h *CommonPersistenceHandler[U]) ListDelegatedThreadRootsByChannel(ctx context.Context, channelID uint64, limit int) (DelegatedThreadRoots, error) {
	if channelID == 0 || limit <= 0 {
		return DelegatedThreadRoots{}, nil
	}
	table := common_persistence_models.TableNameChatAgentSessions
	bounded, err := h.boundedDelegatedSessions(ctx, table+".channel_id = ? AND "+table+".parent_agent_session_id IS NOT NULL AND "+table+".parent_message_id <> 0", channelID, limit, table+".id, "+table+".parent_message_id")
	if err != nil {
		return DelegatedThreadRoots{}, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_delegated_thread_roots_by_channel"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Int(logkeys.Limit, limit))
	}
	return DelegatedThreadRoots{
		Roots:      common_util.ApplyToEach(bounded.sessions, func(session *common_persistence_models.ChatAgentSession) uint64 { return session.ParentMessageID }),
		Active:     bounded.active,
		MoreActive: bounded.moreActive,
	}, nil
}

// delegatedSessionOrder is shared by both bounded active-first projections.
func delegatedSessionOrder() clause.OrderBy {
	return clause.OrderBy{Expression: clause.Expr{SQL: common_persistence_models.TableNameChatAgentSessions + ".id DESC"}}
}

// boundedDelegatedChildren is one bounded active-first read: the first `active` sessions are
// children with an active run, the rest the newest others. more reports any child past the limit,
// moreActive only one with an active run.
type boundedDelegatedChildren struct {
	sessions   []*common_persistence_models.ChatAgentSession
	active     int
	more       bool
	moreActive bool
}

// boundedDelegatedSessions selects active children through the partial active-run index before
// probing recent history through the session parent/channel index. No lifetime COUNT or computed
// sort over historical children is required, and an old active child cannot disappear behind new
// terminal children. Both reads are bounded by the requested cap plus one probe row.
func (h *CommonPersistenceHandler[U]) boundedDelegatedSessions(ctx context.Context, filter string, key uint64, limit int, columns string) (boundedDelegatedChildren, error) {
	if limit <= 0 {
		return boundedDelegatedChildren{}, nil
	}
	table := common_persistence_models.TableNameChatAgentSessions
	runs := common_persistence_models.TableNameChatAgentRuns
	db := h.getDBFromContext(ctx)
	base := func() *gorm.DB {
		return db.Model(&common_persistence_models.ChatAgentSession{}).Select(columns).Where(filter, key)
	}
	var active []*common_persistence_models.ChatAgentSession
	if err := base().Joins("JOIN "+runs+" active_run ON active_run.agent_session_id = "+table+".id").
		Where("active_run.status IN ?", ActiveChatAgentRunStatuses()).
		Order(delegatedSessionOrder()).Limit(limit + 1).Find(&active).Error; err != nil {
		return boundedDelegatedChildren{}, err
	}
	if len(active) > limit {
		return boundedDelegatedChildren{sessions: active[:limit], active: limit, more: true, moreActive: true}, nil
	}
	ids := common_util.ApplyToEach(active, func(s *common_persistence_models.ChatAgentSession) uint64 { return s.ID })
	query := base().Order(delegatedSessionOrder()).Limit(limit - len(active) + 1)
	if len(ids) > 0 {
		query = query.Where(table+".id NOT IN ?", ids)
	}
	var recent []*common_persistence_models.ChatAgentSession
	if err := query.Find(&recent).Error; err != nil {
		return boundedDelegatedChildren{}, err
	}
	all := append(active, recent...)
	more := len(all) > limit
	if more {
		all = all[:limit]
	}
	return boundedDelegatedChildren{sessions: all, active: len(active), more: more}, nil
}

// ListDelegatedThreadRootsAmong returns the roots, among the given thread roots of one channel,
// that hold a delegated child session, in no particular order. Sessions are unique per location
// and the parent link is immutable, so the answer for a root never changes once it has a session.
// The main timeline (0) is never a delegated location and is ignored.
func (h *CommonPersistenceHandler[U]) ListDelegatedThreadRootsAmong(ctx context.Context, channelID uint64, roots []uint64) ([]uint64, error) {
	candidates := make([]uint64, 0, len(roots))
	for _, root := range roots {
		if root != 0 {
			candidates = append(candidates, root)
		}
	}
	if channelID == 0 || len(candidates) == 0 {
		return nil, nil
	}
	var delegated []uint64
	if err := h.getDBFromContext(ctx).Model(&common_persistence_models.ChatAgentSession{}).
		Where("channel_id = ? AND parent_agent_session_id IS NOT NULL AND parent_message_id IN ?", channelID, candidates).
		Pluck("parent_message_id", &delegated).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_delegated_thread_roots_among"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Int(logkeys.Limit, len(candidates)))
	}
	return delegated, nil
}

// CountRunningDelegatedChildren counts the parent's active children that still have a non-terminal
// run. Children stay ACTIVE after they answer so humans can keep talking in the thread, which is
// why the limit counts runs rather than sessions.
func (h *CommonPersistenceHandler[U]) CountRunningDelegatedChildren(ctx context.Context, parentAgentSessionID uint64) (uint32, error) {
	if parentAgentSessionID == 0 {
		return 0, nil
	}
	count, err := countRunningDelegatedChildrenTx(h.getDBFromContext(ctx), parentAgentSessionID)
	if err != nil {
		return 0, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "count_running_delegated_children"),
			zap.Uint64(appLogger.KeyAgentSessionID, parentAgentSessionID))
	}
	if count < 0 {
		count = 0
	}
	return uint32(count), nil
}

// CountActiveDelegatedChildrenByDriver counts, across all parents, the unarchived children with a
// nonterminal run (the children MaxActiveChildren counts), split by driver: owner-driven children are
// of a workflow-owned agent type (the installed workflow-types provider), interactive ones are not.
func (h *CommonPersistenceHandler[U]) CountActiveDelegatedChildrenByDriver(ctx context.Context) (interactive, ownerDriven uint64, err error) {
	sessions, runs := common_persistence_models.TableNameChatAgentSessions, common_persistence_models.TableNameChatAgentRuns
	var counts struct{ Interactive, OwnerDriven uint64 }
	workflowTypes := pq.Array(h.chatAgentWorkflowTypes())
	err = h.getDBFromContext(ctx).Raw(`SELECT COUNT(*) FILTER (WHERE NOT child.agent_type = ANY(?)) AS interactive,
		COUNT(*) FILTER (WHERE child.agent_type = ANY(?)) AS owner_driven
		FROM `+sessions+` child WHERE child.parent_agent_session_id IS NOT NULL AND child.status = ?
		AND EXISTS (SELECT 1 FROM `+runs+` active_run WHERE active_run.agent_session_id = child.id AND active_run.status IN ?)`,
		workflowTypes, workflowTypes, common_persistence_models.ChatAgentSessionStatusActive,
		ActiveChatAgentRunStatuses()).Scan(&counts).Error
	if err != nil {
		return 0, 0, chatPersistenceDatabaseError(err, zap.String(logkeys.Stage, "count_active_delegated_children_by_driver"))
	}
	return counts.Interactive, counts.OwnerDriven, nil
}

// CountDelegatedChildren counts every child the parent has created, archived ones included: the
// lifetime count AgentDelegation.MaxChildrenPerSession bounds.
func (h *CommonPersistenceHandler[U]) CountDelegatedChildren(ctx context.Context, parentAgentSessionID uint64) (uint32, error) {
	if parentAgentSessionID == 0 {
		return 0, nil
	}
	var count int64
	if err := h.getDBFromContext(ctx).Model(&common_persistence_models.ChatAgentSession{}).
		Where("parent_agent_session_id = ?", parentAgentSessionID).Count(&count).Error; err != nil {
		return 0, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "count_delegated_children"),
			zap.Uint64(appLogger.KeyAgentSessionID, parentAgentSessionID))
	}
	return uint32(count), nil
}

// GetChatAgentSessionByParentCreateIdempotencyKey finds the parent's child created under key, archived
// or not, through the per-parent idempotency index (idx_chat_agent_sessions_parent_create_idempotency).
// ErrChatAgentSessionNotFound when there is none.
func (h *CommonPersistenceHandler[U]) GetChatAgentSessionByParentCreateIdempotencyKey(ctx context.Context, parentAgentSessionID uint64, key string) (*common_persistence_models.ChatAgentSession, error) {
	if parentAgentSessionID == 0 || key == "" {
		return nil, ErrChatAgentSessionNotFound
	}
	return h.getChatAgentSessionWhere(ctx, "get_chat_agent_session_by_parent_create_idempotency_key",
		[]zap.Field{zap.Uint64(appLogger.KeyAgentSessionID, parentAgentSessionID)},
		"parent_agent_session_id = ? AND create_idempotency_key = ?", parentAgentSessionID, key)
}

// GetChatAgentSessionReplacing finds the child that replaced childSessionID (its
// replaces_child_session_id), archived or not. ErrChatAgentSessionNotFound when there is none.
func (h *CommonPersistenceHandler[U]) GetChatAgentSessionReplacing(ctx context.Context, childSessionID uint64) (*common_persistence_models.ChatAgentSession, error) {
	if childSessionID == 0 {
		return nil, ErrChatAgentSessionNotFound
	}
	return h.getChatAgentSessionWhere(ctx, "get_chat_agent_session_replacing",
		[]zap.Field{zap.Uint64(appLogger.KeyAgentSessionID, childSessionID)},
		"replaces_child_session_id = ?", childSessionID)
}

func countRunningDelegatedChildrenTx(tx *gorm.DB, parentAgentSessionID uint64) (int64, error) {
	sessions := common_persistence_models.TableNameChatAgentSessions
	runs := common_persistence_models.TableNameChatAgentRuns
	var count int64
	err := tx.Table(sessions+" AS child").
		Where("child.parent_agent_session_id = ? AND child.status = ?", parentAgentSessionID, common_persistence_models.ChatAgentSessionStatusActive).
		Where("EXISTS (SELECT 1 FROM "+runs+" active_run WHERE active_run.agent_session_id = child.id AND active_run.status IN ?)", ActiveChatAgentRunStatuses()).
		Count(&count).Error
	return count, err
}

// ListActiveChatAgentRunsBySession returns the session's RUNNING, WAITING_FOR_APPROVAL, and
// RETRYABLE runs, oldest first, without locking them. Delegated-child retirement uses it to find
// runs to cancel. Wake coalescing does not use it: createOrCoalesceSessionWakeTx counts active
// runs itself while holding the parent session lock.
func (h *CommonPersistenceHandler[U]) ListActiveChatAgentRunsBySession(ctx context.Context, agentSessionID uint64) ([]*common_persistence_models.ChatAgentRun, error) {
	if agentSessionID == 0 {
		return nil, nil
	}
	var runs []*common_persistence_models.ChatAgentRun
	if err := h.getDBFromContext(ctx).
		Where("agent_session_id = ? AND status IN ?", agentSessionID, ActiveChatAgentRunStatuses()).
		Order("id ASC").
		Find(&runs).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_active_chat_agent_runs_by_session"),
			zap.Uint64(appLogger.KeyAgentSessionID, agentSessionID))
	}
	return runs, nil
}

// ListActiveChatAgentRunsBySessions finds active runs for one bounded page of
// sessions. Callers retire every session in the page before reading its runs.
func (h *CommonPersistenceHandler[U]) ListActiveChatAgentRunsBySessions(ctx context.Context, sessionIDs []uint64) ([]*common_persistence_models.ChatAgentRun, error) {
	if len(sessionIDs) == 0 {
		return nil, nil
	}
	var runs []*common_persistence_models.ChatAgentRun
	if err := h.getDBFromContext(ctx).
		Where("agent_session_id IN ? AND status IN ?", sessionIDs, ActiveChatAgentRunStatuses()).
		Order("id ASC").Find(&runs).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err, zap.String(logkeys.Stage, "list_active_chat_agent_runs_by_sessions"))
	}
	return runs, nil
}

// GetLatestBroadcastReplyByRun returns the newest visible broadcast text row tagged with the run
// and created after afterMessageID. report_to_parent uses it to find a report an earlier attempt of
// the same tool call already posted. ErrChatMessageNotFound when there is none.
func (h *CommonPersistenceHandler[U]) GetLatestBroadcastReplyByRun(ctx context.Context, runID uint64, afterMessageID uint64) (*common_persistence_models.ChatMessage, error) {
	if runID == 0 {
		return nil, ErrChatMessageNotFound
	}
	var message common_persistence_models.ChatMessage
	err := h.getDBFromContext(ctx).
		Where("agent_run_id = ? AND id > ? AND broadcast_to_channel = ? AND kind IN ? AND deleted_at IS NULL", runID, afterMessageID, true, standardChatMessageKinds).
		Order("id DESC").
		First(&message).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrChatMessageNotFound
		}
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "get_latest_broadcast_reply_by_run"),
			zap.Uint64(appLogger.KeyAgentRunID, runID),
			zap.Uint64(appLogger.KeyMessageID, afterMessageID))
	}
	return &message, nil
}

// ListLatestDelegationReportsByThread returns, per thread root, the newest visible run-tagged
// broadcast text row of that thread: the child's latest report, closing message, or notice. Roots
// without one are absent.
func (h *CommonPersistenceHandler[U]) ListLatestDelegationReportsByThread(ctx context.Context, channelID uint64, rootMessageIDs []uint64) (map[uint64]*common_persistence_models.ChatMessage, error) {
	out := make(map[uint64]*common_persistence_models.ChatMessage)
	rootMessageIDs = common_util.DeduplicateNonZero(rootMessageIDs)
	if channelID == 0 || len(rootMessageIDs) == 0 {
		return out, nil
	}
	roots := make(pq.Int64Array, 0, len(rootMessageIDs))
	for _, id := range rootMessageIDs {
		roots = append(roots, int64(id))
	}
	messages := common_persistence_models.TableNameChatMessages
	var rows []*common_persistence_models.ChatMessage
	// One LATERAL probe per root keeps a chatty thread from sorting its whole history.
	err := h.getDBFromContext(ctx).Raw(
		"SELECT latest.* FROM unnest(?::bigint[]) AS thread(root_id) "+
			"CROSS JOIN LATERAL (SELECT * FROM "+messages+" WHERE channel_id = ? AND parent_message_id = thread.root_id "+
			"AND broadcast_to_channel AND kind = ? AND agent_run_id IS NOT NULL AND deleted_at IS NULL AND hidden_from_users IS NOT TRUE "+
			"ORDER BY id DESC LIMIT 1) AS latest",
		roots, channelID,
		proto_models.MessageKind_MESSAGE_KIND_STANDARD,
	).Scan(&rows).Error
	if err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_latest_delegation_reports_by_thread"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Int(appLogger.KeyCount, len(roots)))
	}
	for _, row := range rows {
		if row == nil || row.ParentMessageID == nil {
			continue
		}
		out[*row.ParentMessageID] = row
	}
	return out, nil
}

// ListChatAgentRunsStartedBy returns the runs one run's ending minted, oldest first: the follow-up
// that received its session's queued human input (InputDrainedFromRunID), and a delegation wake
// (WokenByAgentRunID), either of a delegated child's parent or of the ending run's own deferred
// wakes. Both links are uniquely indexed, so there are at most two.
func (h *CommonPersistenceHandler[U]) ListChatAgentRunsStartedBy(ctx context.Context, runID uint64) ([]*common_persistence_models.ChatAgentRun, error) {
	if runID == 0 {
		return nil, nil
	}
	var runs []*common_persistence_models.ChatAgentRun
	if err := h.getDBFromContext(ctx).
		Where("input_drained_from_run_id = ? OR woken_by_agent_run_id = ?", runID, runID).
		Order("id").Find(&runs).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_chat_agent_runs_started_by"),
			zap.Uint64(appLogger.KeyAgentRunID, runID))
	}
	return runs, nil
}

// MarkChatMessageBroadcastToChannel also shows an existing thread row on the main timeline. It is
// how a streamed closing message gets its flag after the seal: stream frames cannot carry it. A
// row that is not a thread reply is rejected with ErrChatTranscriptBroadcastInvalid; already
// broadcast rows are a no-op.
func (h *CommonPersistenceHandler[U]) MarkChatMessageBroadcastToChannel(ctx context.Context, messageID uint64) error {
	if messageID == 0 {
		return ErrChatMessageNotFound
	}
	db := h.getDBFromContext(ctx)
	result := db.Model(&common_persistence_models.ChatMessage{}).
		Where("id = ? AND parent_message_id IS NOT NULL AND parent_message_id <> 0 AND deleted_at IS NULL", messageID).
		Select("broadcast_to_channel", "updated_at").
		Updates(chatMessageBroadcastUpdate{BroadcastToChannel: true, UpdatedAt: time.Now()})
	if result.Error != nil {
		return chatPersistenceDatabaseError(result.Error,
			zap.String(logkeys.Stage, "mark_chat_message_broadcast_to_channel"),
			zap.Uint64(appLogger.KeyMessageID, messageID))
	}
	if result.RowsAffected > 0 {
		return nil
	}
	var existing int64
	if err := db.Model(&common_persistence_models.ChatMessage{}).Where("id = ?", messageID).Count(&existing).Error; err != nil {
		return chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "mark_chat_message_broadcast_to_channel"),
			zap.Uint64(appLogger.KeyMessageID, messageID))
	}
	if existing == 0 {
		return ErrChatMessageNotFound
	}
	return ErrChatTranscriptBroadcastInvalid
}

// ListChatAgentSessionsByChannel returns the channel's sessions, the main-timeline session first
// and then thread sessions newest first, capped at limit. The caller samples
// chat_agent_sessions_by_channel_max per listing; a channel has one main-timeline session and one
// per delegated or bound thread, so the cap is a safety net, not a page. When the cap applies, the oldest thread sessions are the ones dropped: the newest are the likeliest to still
// be working and to be on screen.
func (h *CommonPersistenceHandler[U]) ListChatAgentSessionsByChannel(ctx context.Context, channelID uint64, limit int) ([]*common_persistence_models.ChatAgentSession, error) {
	if limit < 1 || limit > config.MaxChatAgentSessionsByChannel {
		return nil, fmt.Errorf("list chat agent sessions by channel: limit %d outside 1..%d", limit, config.MaxChatAgentSessionsByChannel)
	}
	if channelID == 0 {
		return nil, nil
	}
	var sessions []*common_persistence_models.ChatAgentSession
	if err := h.getDBFromContext(ctx).
		Where("channel_id = ?", channelID).
		Order(clause.OrderBy{Expression: clause.Expr{SQL: "(parent_message_id = 0) DESC, id DESC"}}).
		Limit(limit).
		Find(&sessions).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_chat_agent_sessions_by_channel"),
			zap.Uint64(appLogger.KeyChannelID, channelID))
	}
	return sessions, nil
}

// ============================================================================
// CHAT: DELEGATION SETTLEMENT
// ============================================================================

// CreateOrCoalesceParentWakeRun wakes the parent of a delegated child for one trigger row the child
// broadcast to the main timeline (a report_to_parent message). The parent session is locked for the
// decision: with an active parent run the trigger is recorded as a pending wake that the active
// run's terminal write drains; otherwise a parent run is minted with the trigger. Guard failures
// return State None with a SkipReason instead of an error; an archived child is one (child_not_live),
// as it is for settlement. The child session must be a delegated child
// (ErrChatAgentSessionScopeInvalid otherwise).
func (h *CommonPersistenceHandler[U]) CreateOrCoalesceParentWakeRun(ctx context.Context, in ChatAgentParentWakeInput) (*ChatAgentSessionWakeResult, error) {
	if in.ChildAgentSessionID == 0 {
		return nil, ErrChatAgentSessionNotFound
	}
	if in.TriggerMessageID == 0 {
		return nil, ErrChatMessageNotFound
	}
	var result *ChatAgentSessionWakeResult
	err := h.getDBFromContext(ctx).Transaction(func(tx *gorm.DB) error {
		// FOR SHARE, as in settlement: an archive of the child (a plain UPDATE of this row) either
		// committed before this read, which sees it, or waits for the report's transaction. A report
		// that lands after its child was retired therefore never wakes the parent. The report path
		// has already written the channel row, so the order stays channel row -> child session ->
		// parent session.
		var child common_persistence_models.ChatAgentSession
		if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).Where("id = ?", in.ChildAgentSessionID).Take(&child).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrChatAgentSessionNotFound
			}
			return err
		}
		if !child.IsDelegatedChild() {
			return ErrChatAgentSessionScopeInvalid
		}
		if !child.IsLive() {
			result = &ChatAgentSessionWakeResult{SkipReason: "child_not_live"}
			return nil
		}
		var err error
		result, err = h.wakeDelegationParentTx(ctx, tx, &child, 0, in.TriggerMessageID)
		return err
	})
	if err != nil {
		if errors.Is(err, ErrChatAgentSessionNotFound) || errors.Is(err, ErrChatAgentSessionScopeInvalid) {
			return nil, err
		}
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "create_or_coalesce_parent_wake_run"),
			zap.Uint64(appLogger.KeyAgentSessionID, in.ChildAgentSessionID),
			zap.Uint64(appLogger.KeyMessageID, in.TriggerMessageID))
	}
	return result, nil
}

// settleAgentRunTerminalTx is the single settlement hook behind every terminal write. It runs
// inside the writer's transaction after the run row already carries its terminal status (the caller
// holds the run row lock, which fixes the lock order run row -> channel row -> session row). The
// run's session delivers the human input queued behind this run into one follow-up run, in
// main-timeline and thread sessions alike (a suspended session, including a child stopped with its
// parent, holds it). A delegated child run first posts its notice and wakes its parent, so the
// notice precedes the delivered input in its thread and in the follow-up's context. A
// main-timeline run that drained no human input then drains the wake its session deferred while it
// was active; a deferred wake stays pending behind a human follow-up and drains when that run ends.
// Guard failures are logged and skipped. Database errors and failed notification recording for
// delivered input or delegation notices (SetChatAgentMessageRecorder) propagate and roll the
// entire transition back, with any input still queued.
func (h *CommonPersistenceHandler[U]) settleAgentRunTerminalTx(ctx context.Context, tx *gorm.DB, runID uint64) error {
	_, err := h.settleAgentRunTerminalWithSessionTx(ctx, tx, runID, nil)
	return err
}

// settleAgentRunTerminalWithSessionTx is settleAgentRunTerminalTx for a caller that may already
// hold the run's session (nil loads it). It returns the notice a delegated child's settlement
// posted (zero for every other run, and for a child whose ending was a normal answer).
func (h *CommonPersistenceHandler[U]) settleAgentRunTerminalWithSessionTx(ctx context.Context, tx *gorm.DB, runID uint64, session *common_persistence_models.ChatAgentSession) (uint64, error) {
	if tx == nil || runID == 0 {
		return 0, nil
	}
	var run common_persistence_models.ChatAgentRun
	if err := tx.Where("id = ?", runID).Take(&run).Error; err != nil {
		return 0, err
	}
	return h.settleLoadedAgentRunTerminalTx(ctx, tx, &run, session)
}

// settleLoadedAgentRunTerminalTx reuses the caller's locked, SQL-synchronized run.
func (h *CommonPersistenceHandler[U]) settleLoadedAgentRunTerminalTx(ctx context.Context, tx *gorm.DB, loaded *common_persistence_models.ChatAgentRun, session *common_persistence_models.ChatAgentSession) (uint64, error) {
	if tx == nil || loaded == nil || loaded.ID == 0 {
		return 0, nil
	}
	run := *loaded
	runID := run.ID
	if !IsTerminalChatAgentRunStatus(run.Status) {
		appLogger.LogInvariantViolation(appLogger.For(ctx, h.logger, loggerName), "chat_agent_run_settled_after_terminal_write", nil,
			zap.String(logkeys.Stage, "settle_agent_run_terminal"),
			zap.Uint64(appLogger.KeyAgentRunID, runID),
			zap.String(logkeys.Expected, "terminal status"),
			zap.Stringer(logkeys.Actual, run.Status),
			zap.String(logkeys.Fallback, "skip_delegation_settlement"))
		return 0, nil
	}
	if run.AgentSessionID == nil || *run.AgentSessionID == 0 {
		return 0, nil
	}
	if session == nil || session.ID != *run.AgentSessionID {
		loaded, err := loadChatAgentSessionTx(tx, *run.AgentSessionID)
		if err != nil || loaded == nil {
			return 0, err
		}
		session = loaded
	}
	// The post-commit settlement announces the drained run's messages, found by AdmittedRunID.
	if session.IsDelegatedChild() {
		origin, agentType, status := session.CreationOrigin, session.AgentType, run.Status
		h.recordAfterCommit(ctx, func(metrics *common_metrics.CommonMetricsHandler) {
			metrics.RecordChatAgentChildRunTerminal(origin, slices.Contains(h.chatAgentWorkflowTypes(), agentType), agentType, status)
		})
		// The admission locks come first so the notice's channel write and the wake's session
		// reads keep the order channel -> session; the notice is then written before the input,
		// so the follow-up reads the person's input after it.
		locked, archived, err := lockChatAgentSessionForAdmissionTx(tx, session.ChannelID, session.ID)
		if err != nil {
			return 0, err
		}
		// The child's own follow-up is decided first: when this ending still owes answers to
		// steering messages, the follow-up's ending is the parent's report, not this one.
		followUp, err := h.drainPendingChildWakeTx(ctx, tx, &run, session)
		if err != nil {
			return 0, err
		}
		if err := h.settleDelegatedChildRunTx(ctx, tx, &run, session, followUp); err != nil {
			return 0, err
		}
		if locked != nil {
			if _, _, err := h.drainLockedChatAgentInputTx(ctx, tx, locked, archived, run.ID); err != nil {
				return 0, err
			}
		}
		return common_util.Deref(run.DelegationNoticeMessageID), nil
	}
	drained, _, err := h.drainChatAgentInputTx(ctx, tx, session, run.ID)
	if err != nil {
		return 0, err
	}
	if session.IsMainTimeline() && drained == nil {
		_, err := h.drainPendingParentWakeTx(ctx, tx, session.ID, &run)
		return 0, err
	}
	return 0, nil
}

// loadChatAgentSessionTx reads one session inside tx; a missing session is (nil, nil).
func loadChatAgentSessionTx(tx *gorm.DB, sessionID uint64) (*common_persistence_models.ChatAgentSession, error) {
	var session common_persistence_models.ChatAgentSession
	if err := tx.Where("id = ?", sessionID).Take(&session).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &session, nil
}

// settleDelegatedChildRunTx settles one delegated child run's ending: a normal answer is the wake
// trigger (its broadcast flag is set if the writer skipped it); every other ending posts one
// visible broadcast notice as the child bot, which becomes the trigger. A parent-requested cancel
// posts the notice but does not wake the parent, and neither does any ending of an archived child:
// whoever archived it (the parent through cancel_subagent, a parent archive, or the application)
// no longer wants its work, so the parent must not pay for a run to read about it. A run schema
// setup superseded is not an ending of the child's work: the child's newer run carries on and
// settles the real ending, so this one posts nothing and wakes no one. The run row's notice id and
// wake state make a repeated call a no-op.
func (h *CommonPersistenceHandler[U]) settleDelegatedChildRunTx(ctx context.Context, tx *gorm.DB, run *common_persistence_models.ChatAgentRun, child *common_persistence_models.ChatAgentSession, followUp *common_persistence_models.ChatAgentRun) error {
	if run.DelegationWakeState != proto_models.DelegationWakeState_DELEGATION_WAKE_STATE_UNSPECIFIED || run.DelegationNoticeMessageID != nil {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("delegated child run already settled",
				zap.Uint64(appLogger.KeyAgentRunID, run.ID))
		}
		return nil
	}
	reasonCode := common_util.Deref(run.StatusReasonCode)
	normal := run.Status == proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_COMPLETED &&
		run.StatusReason == proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_UNSPECIFIED
	var trigger uint64
	if normal {
		closing, err := delegationClosingReplyTx(tx, run.ID, child.ParentMessageID)
		switch {
		case err == nil:
			trigger = closing.ID
			if !closing.BroadcastToChannel {
				if err := tx.Model(&common_persistence_models.ChatMessage{}).Where("id = ?", closing.ID).
					Select("broadcast_to_channel", "updated_at").
					Updates(chatMessageBroadcastUpdate{BroadcastToChannel: true, UpdatedAt: time.Now()}).Error; err != nil {
					return err
				}
			}
		case errors.Is(err, gorm.ErrRecordNotFound):
			// A custom writer completed the run without any assistant text: humans and the parent
			// still need to hear that the child stopped.
			normal = false
		default:
			return err
		}
	}
	if !normal {
		noticeID, err := h.insertDelegationNoticeTx(tx, run, child, chatAgentDelegationNoticeContent(run.Status, run.StatusReason, reasonCode))
		if err != nil {
			return err
		}
		if err := tx.Model(&common_persistence_models.ChatAgentRun{}).Where("id = ?", run.ID).
			Select("delegation_notice_message_id", "updated_at").
			Updates(chatAgentDelegationRunUpdate{DelegationNoticeMessageID: &noticeID, UpdatedAt: gorm.Expr("CURRENT_TIMESTAMP")}).Error; err != nil {
			return err
		}
		run.DelegationNoticeMessageID = &noticeID
		if err := h.recordChatAgentMessage(ctx, noticeID); err != nil {
			return err
		}
		trigger = noticeID
	}
	if reasonCode == DelegationCancelledByParentCode || chatAgentRunStoppedWithParent(run.StatusReason, reasonCode) {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("delegated child run ended by its parent; parent not woken",
				zap.Uint64(appLogger.KeyAgentRunID, run.ID),
				zap.String(appLogger.KeyCode, reasonCode))
		}
		return nil
	}
	if followUp != nil {
		if err := tx.Model(&common_persistence_models.ChatAgentRun{}).Where("id = ?", run.ID).
			Select("delegation_wake_state", "updated_at").
			Updates(chatAgentDelegationRunUpdate{DelegationWakeState: proto_models.DelegationWakeState_DELEGATION_WAKE_STATE_SUPERSEDED_BY_FOLLOW_UP, UpdatedAt: gorm.Expr("CURRENT_TIMESTAMP")}).Error; err != nil {
			return err
		}
		run.DelegationWakeState = proto_models.DelegationWakeState_DELEGATION_WAKE_STATE_SUPERSEDED_BY_FOLLOW_UP
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("delegated child run superseded by its follow-up; parent not woken",
				zap.Uint64(appLogger.KeyAgentRunID, run.ID),
				zap.Uint64(appLogger.KeyAgentSessionID, child.ID),
				zap.Uint64("follow_up_agent_run_id", followUp.ID))
		}
		return nil
	}
	// Re-read the child for the wake decision: settlement already holds the child row FOR UPDATE
	// (the admission locks), so an archive of the child either committed before that lock, and this
	// read sees it, or waits for this transaction; an archived child is never mistaken for an
	// active one. FOR SHARE adds a lock only when the channel was missing or soft-deleted and the
	// admission lock skipped the session row.
	if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).Where("id = ?", child.ID).Take(child).Error; err != nil {
		return err
	}
	if !child.IsLive() {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("non-live delegated child run ended; parent not woken",
				zap.Uint64(appLogger.KeyAgentRunID, run.ID),
				zap.Uint64(appLogger.KeyAgentSessionID, child.ID))
		}
		return nil
	}
	outcome, err := h.wakeDelegationParentTx(ctx, tx, child, run.ID, trigger)
	if err != nil {
		return err
	}
	if outcome.State == proto_models.DelegationWakeState_DELEGATION_WAKE_STATE_UNSPECIFIED {
		return nil
	}
	if err := tx.Model(&common_persistence_models.ChatAgentRun{}).Where("id = ?", run.ID).
		Select("delegation_wake_state", "updated_at").
		Updates(chatAgentDelegationRunUpdate{DelegationWakeState: outcome.State, UpdatedAt: gorm.Expr("CURRENT_TIMESTAMP")}).Error; err != nil {
		return err
	}
	run.DelegationWakeState = outcome.State
	return nil
}

// delegationClosingReplyTx reads a delegated child run's closing turn in the child's thread
// (threadRootID): its newest complete assistant text, the trigger of a normal ending. Reports and
// notices are run-tagged but step-less by design; requiring a step index keeps a run that completed
// without assistant text from re-using its last report as the closing turn, which would wake the
// parent twice for the same row. gorm.ErrRecordNotFound when the run wrote none.
func delegationClosingReplyTx(tx *gorm.DB, runID, threadRootID uint64) (common_persistence_models.ChatMessage, error) {
	var closing common_persistence_models.ChatMessage
	err := tx.Select("id", "broadcast_to_channel").
		Where("agent_run_id = ? AND kind IN ? AND COALESCE(parent_message_id, 0) = ? AND stream_state IN ? AND agent_step_index IS NOT NULL AND deleted_at IS NULL AND hidden_from_users IS NOT TRUE",
			runID, standardChatMessageKinds, threadRootID,
			[]proto_models.MessageStreamState{proto_models.MessageStreamState_MESSAGE_STREAM_STATE_UNSPECIFIED, proto_models.MessageStreamState_MESSAGE_STREAM_STATE_COMPLETE}).
		Order("id DESC").Take(&closing).Error
	return closing, err
}

// insertDelegationNoticeTx writes the child bot's visible broadcast notice in the child's thread.
// It is an ordinary assistant message (not excluded from AI context) so the parent's next turn
// reads it, and it carries the run id but no step index so the assistant-turn unique index stays
// out of the way.
func (h *CommonPersistenceHandler[U]) insertDelegationNoticeTx(tx *gorm.DB, run *common_persistence_models.ChatAgentRun, child *common_persistence_models.ChatAgentSession, content string) (uint64, error) {
	exclude, hidden := false, false
	runID := run.ID
	parent := child.ParentMessageID
	notice := &common_persistence_models.ChatMessage{
		ChannelID:            child.ChannelID,
		SenderID:             child.BotUserID,
		Content:              content,
		ContentType:          proto_models.MessageContentType_MESSAGE_CONTENT_TYPE_TEXT,
		StreamState:          proto_models.MessageStreamState_MESSAGE_STREAM_STATE_COMPLETE,
		ParentMessageID:      &parent,
		BroadcastToChannel:   true,
		Kind:                 proto_models.MessageKind_MESSAGE_KIND_STANDARD,
		Role:                 proto_models.MessageRole_MESSAGE_ROLE_ASSISTANT,
		ExcludeFromAIContext: &exclude,
		HiddenFromUsers:      &hidden,
		AgentRunID:           &runID,
	}
	if err := insertAgentRunNoticeTx(tx, notice); err != nil {
		return 0, err
	}
	return notice.ID, nil
}

// insertAgentRunNoticeTx writes a visible run notice (a generic SYSTEM_EVENT or a delegated child's
// broadcast notice) and makes it the channel's last message.
func insertAgentRunNoticeTx(tx *gorm.DB, notice *common_persistence_models.ChatMessage) error {
	if err := tx.Create(notice).Error; err != nil {
		return err
	}
	return touchChatChannelLastMessageTx(tx, notice.ChannelID, notice.SenderID, notice.Content)
}

// touchChatChannelLastMessageTx records a new visible message as the channel's last message: its
// time, its normalized preview and its sender.
func touchChatChannelLastMessageTx(tx *gorm.DB, channelID, senderID uint64, content string) error {
	now := time.Now()
	preview := normalizeChatChannelPreview(content)
	return tx.Model(&common_persistence_models.ChatChannel{}).
		Where("id = ?", channelID).
		Select("last_message_at", "last_message_preview", "last_message_sender_id").
		Updates(common_persistence_models.ChatChannel{LastMessageAt: &now, LastMessagePreview: &preview, LastMessageSenderID: chatChannelLastMessageSender(senderID)}).Error
}

// chatChannelLastMessageSender maps a message sender onto the nullable channel
// column; a zero sender is stored as NULL.
func chatChannelLastMessageSender(senderID uint64) *uint64 {
	if senderID == 0 {
		return nil
	}
	return &senderID
}

// wakeDelegationParentTx locks the parent session, applies the child's wake policy
// (delegationWakeDecisionTx) and the wake guards, then creates or coalesces the wake. childRunID is
// recorded as WokenByAgentRunID on a minted run (zero for report wakes, which may repeat per child
// run). A drained follow-up records the parent run whose ending drained it the same way.
func (h *CommonPersistenceHandler[U]) wakeDelegationParentTx(ctx context.Context, tx *gorm.DB, child *common_persistence_models.ChatAgentSession, childRunID uint64, triggerMessageID uint64) (*ChatAgentSessionWakeResult, error) {
	parentID := common_util.Deref(child.ParentAgentSessionID)
	skip := func(reason string) *ChatAgentSessionWakeResult {
		appLogger.For(ctx, h.logger, loggerName).Warn("delegation parent wake skipped",
			zap.String(logkeys.Stage, "wake_delegation_parent"),
			zap.String(logkeys.Reason, reason),
			zap.Uint64(appLogger.KeyAgentSessionID, parentID),
			zap.Uint64(appLogger.KeyAgentRunID, childRunID),
			zap.Uint64(appLogger.KeyMessageID, triggerMessageID))
		return &ChatAgentSessionWakeResult{SkipReason: reason}
	}
	var parent common_persistence_models.ChatAgentSession
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", parentID).Take(&parent).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return skip("parent_session_missing"), nil
		}
		return nil, err
	}
	group := common_util.Deref(child.WakeGroup)
	if held, err := delegationWakeDecisionTx(tx, child); err != nil || held != nil {
		if held != nil && appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("delegation parent wake held by the child's wake policy",
				zap.Uint64(appLogger.KeyAgentSessionID, parentID),
				zap.Uint64(appLogger.KeyAgentRunID, childRunID),
				zap.Uint64(appLogger.KeyMessageID, triggerMessageID),
				zap.Stringer(logkeys.Status, held.State),
				zap.String(logkeys.Reason, held.SkipReason))
		}
		if err == nil && childRunID != 0 && held.State == proto_models.DelegationWakeState_DELEGATION_WAKE_STATE_DEFERRED_TO_GROUP {
			err = setDeferredWakeGroupTx(tx, &parent, group, true)
		}
		return held, err
	}
	if childRunID != 0 && child.WakePolicy == proto_models.WakePolicy_WAKE_POLICY_WITH_SIBLINGS && group != "" {
		// The last member's ending released its group; a report (no run) does not complete it.
		if err := setDeferredWakeGroupTx(tx, &parent, group, false); err != nil {
			return nil, err
		}
		h.recordAfterCommit(ctx, func(metrics *common_metrics.CommonMetricsHandler) { metrics.RecordChatAgentWakeGroupCompletion() })
	}
	if reason, err := h.parentWakeDecisionTx(tx, &parent, child); err != nil || reason != "" {
		if err != nil {
			return nil, err
		}
		return skip(reason), nil
	}
	var triggerCount int64
	if err := tx.Model(&common_persistence_models.ChatMessage{}).
		Where("id = ? AND channel_id = ? AND deleted_at IS NULL", triggerMessageID, parent.ChannelID).
		Count(&triggerCount).Error; err != nil {
		return nil, err
	}
	if triggerCount == 0 {
		return skip("trigger_message_missing"), nil
	}
	human, err := h.delegationWakeHumanTx(tx, child, &parent)
	if err != nil {
		return nil, err
	}
	return h.createOrCoalesceSessionWakeTx(tx, &parent, triggerMessageID, human, childRunID)
}

// delegationWakeDecisionTx applies child's wake policy to one of its endings or reports. It returns
// nil when the parent may be woken (the guards still apply), or the result that holds the wake:
//   - WAKE_POLICY_NEVER never wakes the parent, reports included;
//   - WAKE_POLICY_WITH_SIBLINGS defers (DEFERRED_TO_GROUP) while another member of the child's
//     (parent, wake_group) is unarchived and has a nonterminal run. Undriven, approval-waiting and
//     inference-blocked members hold the group open; archived members, whose runs may still be
//     being cancelled, do not. Settlement records the deferral on the ending run; a report writes
//     nothing, so the run's own ending still settles. Archiving the last member that held the
//     group open releases its deferred endings instead (releaseArchivedWakeGroupTx).
//
// The caller holds the parent row FOR UPDATE, which every ending and report of the group takes
// before deciding, so of the members that end together exactly one, the last, sees no other
// nonterminal member and wakes the parent. UNSPECIFIED (an agent spawn) and IMMEDIATE wake.
func delegationWakeDecisionTx(tx *gorm.DB, child *common_persistence_models.ChatAgentSession) (*ChatAgentSessionWakeResult, error) {
	if child.WakePolicy == proto_models.WakePolicy_WAKE_POLICY_NEVER {
		return &ChatAgentSessionWakeResult{SkipReason: "wake_policy_never"}, nil
	}
	group := common_util.Deref(child.WakeGroup)
	if child.WakePolicy != proto_models.WakePolicy_WAKE_POLICY_WITH_SIBLINGS || group == "" {
		return nil, nil
	}
	sessions := common_persistence_models.TableNameChatAgentSessions
	var pending bool
	if err := tx.Raw(`SELECT EXISTS (SELECT 1 FROM `+sessions+` member
		WHERE member.parent_agent_session_id = ? AND member.wake_group = ? AND member.id <> ? AND member.status = ?
		AND EXISTS (SELECT 1 FROM `+common_persistence_models.TableNameChatAgentRuns+` run WHERE run.agent_session_id = member.id AND run.status IN ?))`,
		common_util.Deref(child.ParentAgentSessionID), group, child.ID, common_persistence_models.ChatAgentSessionStatusActive,
		ActiveChatAgentRunStatuses()).Scan(&pending).Error; err != nil {
		return nil, err
	}
	if pending {
		return &ChatAgentSessionWakeResult{State: proto_models.DelegationWakeState_DELEGATION_WAKE_STATE_DEFERRED_TO_GROUP}, nil
	}
	return nil, nil
}

// setDeferredWakeGroupTx adds group to (deferred) or removes it from the parent's
// deferred_wake_groups. The caller holds the parent row FOR UPDATE, which every deferral and
// release of the group takes before deciding, so the marker is exact without locking any run row.
func setDeferredWakeGroupTx(tx *gorm.DB, parent *common_persistence_models.ChatAgentSession, group string, deferred bool) error {
	if group == "" || slices.Contains(parent.DeferredWakeGroups, group) == deferred {
		return nil
	}
	value := gorm.Expr("array_remove(deferred_wake_groups, ?::text)", group)
	if deferred {
		value = gorm.Expr("array_append(deferred_wake_groups, ?::text)", group)
	}
	return tx.Model(&common_persistence_models.ChatAgentSession{}).Where("id = ?", parent.ID).UpdateColumn("deferred_wake_groups", value).Error
}

// releaseArchivedWakeGroupTx runs in the archive of a WAKE_POLICY_WITH_SIBLINGS child, after its
// status write, under the archive's channel and child locks. The archived child's endings never
// wake the parent, so when it was the last member holding open a group with an undelivered
// deferral (the parent's deferred_wake_groups), the group's newest deferred ending wakes the parent
// instead, through wakeDelegationParentTx as if that ending had come last: the same guards,
// coalescing with an active parent run, and release of the group's marker, which makes a repeated
// archive a no-op. It decides under the parent row lock, after the channel and child locks, as
// settlement and reports do, and locks no run row. An inactive parent (the parent archive cascade
// archives the parent first) is never woken. A minted run advances the graph's resume revision, so
// a parked owner-driven graph is notified. It returns the parent run it minted, for the caller to
// start after commit.
func (h *CommonPersistenceHandler[U]) releaseArchivedWakeGroupTx(ctx context.Context, tx *gorm.DB, archived *common_persistence_models.ChatAgentSession) (*common_persistence_models.ChatAgentRun, error) {
	group := common_util.Deref(archived.WakeGroup)
	if !archived.IsDelegatedChild() || archived.WakePolicy != proto_models.WakePolicy_WAKE_POLICY_WITH_SIBLINGS || group == "" {
		return nil, nil
	}
	var parent common_persistence_models.ChatAgentSession
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Select("id", "status", "deferred_wake_groups").Where("id = ?", *archived.ParentAgentSessionID).Take(&parent).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if parent.Status != common_persistence_models.ChatAgentSessionStatusActive || !slices.Contains(parent.DeferredWakeGroups, group) {
		return nil, nil
	}
	if held, err := delegationWakeDecisionTx(tx, archived); err != nil || held != nil {
		return nil, err
	}
	var deferred common_persistence_models.ChatAgentRun
	if err := tx.Where(`delegation_wake_state = ? AND agent_session_id IN (SELECT id FROM `+common_persistence_models.TableNameChatAgentSessions+`
		WHERE parent_agent_session_id = ? AND wake_group = ?)`,
		proto_models.DelegationWakeState_DELEGATION_WAKE_STATE_DEFERRED_TO_GROUP, parent.ID, group).
		Order("id DESC").Take(&deferred).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	member, err := loadChatAgentSessionTx(tx, common_util.Deref(deferred.AgentSessionID))
	if err != nil || member == nil {
		return nil, err
	}
	// The deferred ending's own trigger: its notice, or the closing turn of a normal ending.
	trigger := common_util.Deref(deferred.DelegationNoticeMessageID)
	if trigger == 0 {
		closing, err := delegationClosingReplyTx(tx, deferred.ID, member.ParentMessageID)
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
		trigger = closing.ID
	}
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("archive of the last working wake-group member releases the group",
			zap.Uint64(appLogger.KeyAgentSessionID, archived.ID),
			zap.Uint64(appLogger.KeyAgentRunID, deferred.ID),
			zap.Uint64(appLogger.KeyMessageID, trigger))
	}
	outcome, err := h.wakeDelegationParentTx(ctx, tx, member, deferred.ID, trigger)
	if err != nil || outcome.State != proto_models.DelegationWakeState_DELEGATION_WAKE_STATE_RUN_CREATED {
		return nil, err
	}
	if err := h.advanceChatAgentGraphResumeRevisionTx(ctx, tx, []uint64{parent.ID}, proto_models.GraphEventKind_GRAPH_EVENT_KIND_WAKE_GROUP_RELEASED); err != nil {
		return nil, err
	}
	var minted common_persistence_models.ChatAgentRun
	if err := tx.Where("id = ?", outcome.RunID).Take(&minted).Error; err != nil {
		return nil, err
	}
	return &minted, nil
}

// parentWakeDecisionTx returns the failed guard name, or "" when parent may be woken: the session
// guards, then (for a child wake, child non-nil) the cross-session checks, then the channel's
// archive state. Child wakes and pending-wake drains share it.
func (h *CommonPersistenceHandler[U]) parentWakeDecisionTx(tx *gorm.DB, parent, child *common_persistence_models.ChatAgentSession) (string, error) {
	if reason := delegationWakeGuard(parent, slices.Contains(h.chatAgentWorkflowTypes(), parent.AgentType)); reason != "" {
		return reason, nil
	}
	if child != nil {
		if parent.ChannelID != child.ChannelID {
			return "parent_channel_mismatch", nil
		}
		if parent.BotUserID == child.BotUserID {
			return "shared_bot_user", nil
		}
	}
	archived, err := chatChannelArchivedTx(tx, parent.ChannelID)
	if err != nil {
		return "", err
	}
	if archived {
		return "channel_archived", nil
	}
	return "", nil
}

// delegationWakeGuard returns the failed session-only guard name, or "" when the session may be
// woken. An inference block does not fail it: like Stop, a block defers the wake, and the
// retarget that clears the block drains it. An owner-driven session (its agent type is
// workflow-owned) is woken whatever its auto-run policy: workflow roots run with auto-run
// disabled, and the wake run is left for the owner's drive.
func delegationWakeGuard(session *common_persistence_models.ChatAgentSession, ownerDriven bool) string {
	if !session.IsMainTimeline() {
		return "parent_not_main_timeline"
	}
	if session.Status != common_persistence_models.ChatAgentSessionStatusActive {
		return "parent_not_active"
	}
	if !ownerDriven && session.AutoRunPolicy != common_persistence_models.ChatAgentSessionAutoRunPolicyAutomatic {
		return "parent_auto_run_not_automatic"
	}
	return ""
}

func chatChannelArchivedTx(tx *gorm.DB, channelID uint64) (bool, error) {
	var channel common_persistence_models.ChatChannel
	if err := tx.Select("id", "is_archived").Where("id = ?", channelID).Take(&channel).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return true, nil
		}
		return false, err
	}
	return channel.IsArchived, nil
}

// delegationWakeHumanTx picks the human a parent wake run acts for. A fork clone wakes for its own
// human, the forker: its copied spawning run still names the source conversation's human, who may
// not belong to the fork. Otherwise it is the person behind the run that spawned the child, or for a
// child the application created, the human it was created for (its effective user), falling back
// to the parent session's effective user. It is never a bot of the pair, so approvals raised by the
// wake run reach a person and actor attachment resolves a human.
func (h *CommonPersistenceHandler[U]) delegationWakeHumanTx(tx *gorm.DB, child, parent *common_persistence_models.ChatAgentSession) (uint64, error) {
	ownHuman := child.UserID != 0 && child.UserID != child.BotUserID && child.UserID != parent.BotUserID
	if ownHuman && child.CreationOrigin == proto_models.DelegationCreationOrigin_DELEGATION_CREATION_ORIGIN_FORK {
		return child.UserID, nil
	}
	if spawnRunID := common_util.Deref(child.SpawnedByAgentRunID); spawnRunID != 0 {
		var spawn common_persistence_models.ChatAgentRun
		err := tx.Select("id", "triggering_user_id").Where("id = ?", spawnRunID).Take(&spawn).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, err
		}
		if human := common_util.Deref(spawn.TriggeringUserID); human != 0 && human != child.BotUserID && human != parent.BotUserID {
			return human, nil
		}
	} else if ownHuman && child.CreationOrigin == proto_models.DelegationCreationOrigin_DELEGATION_CREATION_ORIGIN_APP {
		return child.UserID, nil
	}
	return parent.UserID, nil
}

// createOrCoalesceSessionWakeTx is the coalescer. The caller holds the destination session row FOR
// UPDATE, so the active-run check and the write it decides are one atomic step: concurrent wakes
// serialize on the session row and cannot both mint a run. A suspended or inference-blocked parent
// defers the wake exactly like a busy one; Resume or the unblocking retarget drains it. So does a
// parent whose graph budget pool is used up, until SetChatAgentGraphBudget raises it: the wake runs
// inside a child's settlement, so it is never an error.
func (h *CommonPersistenceHandler[U]) createOrCoalesceSessionWakeTx(tx *gorm.DB, parent *common_persistence_models.ChatAgentSession, triggerMessageID uint64, humanUserID uint64, wokenByRunID uint64) (*ChatAgentSessionWakeResult, error) {
	pool, err := loadChatAgentGraphBudgetTx(tx, parent.GraphRootID())
	if err != nil {
		return nil, err
	}
	deferred := parent.IsSuspended() || parent.IsInferenceBlocked() || graphBudgetExhaustedError(pool) != nil
	if !deferred {
		if deferred, err = sessionHasActiveChatAgentRunTx(tx, parent.ID, 0); err != nil {
			return nil, err
		}
	}
	if deferred {
		if err := coalesceSessionWakeTx(tx, parent.ID, triggerMessageID); err != nil {
			return nil, err
		}
		return &ChatAgentSessionWakeResult{
			State:            proto_models.DelegationWakeState_DELEGATION_WAKE_STATE_COALESCED,
			PendingWakeCount: parent.PendingWakeCount + 1,
		}, nil
	}
	run := h.newSessionWakeRun(parent, triggerMessageID, humanUserID, wokenByRunID, pool != nil)
	if err := tx.Create(run).Error; err != nil {
		return nil, err
	}
	return &ChatAgentSessionWakeResult{
		State: proto_models.DelegationWakeState_DELEGATION_WAKE_STATE_RUN_CREATED,
		RunID: run.ID,
	}, nil
}

// newChatAgentFollowUpRun builds a RUNNING run that persistence mints on its own (a delegation
// wake or a drained follow-up) at the session's location, pinned to its current config version
// (the caller holds the session lock, so the version cannot move underneath it). A nil trigger is
// set by the caller once its message exists. A run of a graph with a budget pool (pooled) carries
// no work ceilings: the pool bounds it, and its session's or definition's step limit still applies.
func (h *CommonPersistenceHandler[U]) newChatAgentFollowUpRun(session *common_persistence_models.ChatAgentSession, triggerMessageID *uint64, humanUserID uint64, pooled bool) *common_persistence_models.ChatAgentRun {
	version := session.ActiveConfigVersion
	sessionID := session.ID
	run := &common_persistence_models.ChatAgentRun{
		OrganizationID:            session.OrganizationID,
		ChannelID:                 session.ChannelID,
		ParentMessageID:           session.ParentMessageID,
		AgentSessionID:            &sessionID,
		AgentSessionConfigVersion: &version,
		BotUserID:                 session.BotUserID,
		TriggeringUserID:          &humanUserID,
		TriggeringMessageID:       triggerMessageID,
		Status:                    proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING,
		StartedAt:                 time.Now(),
	}
	if !pooled {
		h.chatAgentRunBudgetCeilings().ApplyTo(run)
	}
	return run
}

// drainPendingParentWakeTx runs at every terminal write of a main-timeline session's run that
// drained no human input (a human follow-up drains the deferred wake at its own ending), and at
// Resume (endingRun nil). The caller's ending run is locked and SQL-synchronized, including the
// just-sealed consumption watermark. A normally completed run whose sealed generations represented
// the pending trigger already answered it, so the pending fields are cleared without a follow-up; a
// failed or cancelled run keeps the wake, since its updates may never have been answered.
// Otherwise, if the session is not suspended and has no other active run, it mints one follow-up
// for the newest deferred trigger and clears the pending fields. That single run sees every
// deferred report because the main-timeline context window already includes all broadcast rows. A
// suspended session keeps its pending wakes for Resume, and an inference-blocked one for the
// retarget that clears the block. A failed guard clears the pending fields and logs.
func (h *CommonPersistenceHandler[U]) drainPendingParentWakeTx(ctx context.Context, tx *gorm.DB, sessionID uint64, endingRun *common_persistence_models.ChatAgentRun) (*common_persistence_models.ChatAgentRun, error) {
	var session common_persistence_models.ChatAgentSession
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", sessionID).Take(&session).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	pending := common_util.Deref(session.PendingWakeMessageID)
	if pending == 0 {
		return nil, nil
	}
	var endingRunID uint64
	if endingRun != nil {
		endingRunID = endingRun.ID
		if endingRun.Status == proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_COMPLETED &&
			endingRun.StatusReason == proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_UNSPECIFIED &&
			pending <= endingRun.ConsumedPendingMessageID {
			return nil, clearPendingWakeTx(tx, session.ID)
		}
	}
	if session.IsSuspended() || session.IsInferenceBlocked() {
		return nil, nil
	}
	pool, err := loadChatAgentGraphBudgetTx(tx, session.GraphRootID())
	if err != nil || graphBudgetExhaustedError(pool) != nil {
		// A used-up pool keeps the wakes for SetChatAgentGraphBudget to drain.
		return nil, err
	}
	if active, err := sessionHasActiveChatAgentRunTx(tx, session.ID, endingRunID); err != nil || active {
		// Another run of this session still owes the drain; it happens at that run's own end.
		return nil, err
	}
	clearPending := func() error {
		return clearPendingWakeTx(tx, session.ID)
	}
	skipReason, err := h.parentWakeDecisionTx(tx, &session, nil)
	if err != nil {
		return nil, err
	}
	if skipReason != "" {
		appLogger.For(ctx, h.logger, loggerName).Warn("pending delegation wake dropped",
			zap.String(logkeys.Stage, "drain_pending_parent_wake"),
			zap.String(logkeys.Reason, skipReason),
			zap.Uint64(appLogger.KeyAgentSessionID, session.ID),
			zap.Uint64(appLogger.KeyAgentRunID, endingRunID),
			zap.Uint64(appLogger.KeyMessageID, pending),
			zap.Uint32(appLogger.KeyCount, session.PendingWakeCount))
		return nil, clearPending()
	}
	// The drained run records the run whose ending minted it, like a child-woken run does, so the
	// post-commit settlement drives exactly this run and no other RUNNING run of the session. Resume
	// drives the run it minted itself and records none (endingRunID zero): the run it would name,
	// the stopped one, may already have woken this session, and the pair is unique.
	follow := h.newSessionWakeRun(&session, pending, session.UserID, endingRunID, pool != nil)
	if err := tx.Create(follow).Error; err != nil {
		return nil, err
	}
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("pending delegation wakes drained into one follow-up run",
			zap.Uint64(appLogger.KeyAgentSessionID, session.ID),
			zap.Uint64(appLogger.KeyAgentRunID, follow.ID),
			zap.Uint32(appLogger.KeyCount, session.PendingWakeCount))
	}
	return follow, clearPending()
}

// newSessionWakeRun adds the wake provenance to the shared follow-up constructor.
func (h *CommonPersistenceHandler[U]) newSessionWakeRun(session *common_persistence_models.ChatAgentSession, triggerMessageID, humanUserID, wokenByRunID uint64, pooled bool) *common_persistence_models.ChatAgentRun {
	run := h.newChatAgentFollowUpRun(session, &triggerMessageID, humanUserID, pooled)
	if wokenByRunID != 0 {
		run.WokenByAgentRunID = &wokenByRunID
	}
	return run
}

func coalesceSessionWakeTx(tx *gorm.DB, sessionID, triggerMessageID uint64) error {
	return tx.Model(&common_persistence_models.ChatAgentSession{}).Where("id = ?", sessionID).
		Select("pending_wake_message_id", "pending_wake_count", "updated_at").
		Updates(struct {
			PendingWakeMessageID clause.Expr `gorm:"type:bigint"`
			PendingWakeCount     clause.Expr `gorm:"type:integer"`
			UpdatedAt            time.Time   `gorm:"autoUpdateTime:false"`
		}{
			PendingWakeMessageID: gorm.Expr("GREATEST(COALESCE(pending_wake_message_id, 0), ?)", triggerMessageID),
			PendingWakeCount:     gorm.Expr("pending_wake_count + 1"),
			UpdatedAt:            time.Now(),
		}).Error
}

func countSessionRunsTx(tx *gorm.DB, sessionID, excludeRunID uint64) (total, active int64, err error) {
	var counts struct{ Total, Active int64 }
	err = tx.Model(&common_persistence_models.ChatAgentRun{}).
		Select("COUNT(*) AS total, COUNT(*) FILTER (WHERE id <> ? AND status IN ?) AS active", excludeRunID, ActiveChatAgentRunStatuses()).
		Where("agent_session_id = ?", sessionID).
		Scan(&counts).Error
	return counts.Total, counts.Active, err
}

func lockPendingWakeTx(tx *gorm.DB, session *common_persistence_models.ChatAgentSession, runID uint64) (pendingWake, error) {
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", session.ID).Take(session).Error; err != nil {
		return pendingWake{}, err
	}
	pending := pendingWake{MessageID: common_util.Deref(session.PendingWakeMessageID)}
	if pending.MessageID == 0 {
		return pending, nil
	}
	var err error
	pending.TotalRuns, pending.OtherActive, err = countSessionRunsTx(tx, session.ID, runID)
	return pending, err
}

func clearPendingWakeTx(tx *gorm.DB, sessionID uint64) error {
	return tx.Model(&common_persistence_models.ChatAgentSession{}).Where("id = ?", sessionID).
		Select("pending_wake_message_id", "pending_wake_count", "updated_at").
		Updates(chatAgentPendingWakeClearUpdate{UpdatedAt: time.Now()}).Error
}

func (h *CommonPersistenceHandler[U]) GetChatAgentRunWokenBy(ctx context.Context, runID, sessionID uint64) (*common_persistence_models.ChatAgentRun, error) {
	if runID == 0 || sessionID == 0 {
		return nil, ErrChatAgentRunNotFound
	}
	var run common_persistence_models.ChatAgentRun
	err := h.getDBFromContext(ctx).Where("woken_by_agent_run_id = ? AND agent_session_id = ?", runID, sessionID).Take(&run).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrChatAgentRunNotFound
		}
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "get_chat_agent_run_woken_by"),
			zap.Uint64(appLogger.KeyAgentRunID, runID),
			zap.Uint64(appLogger.KeyAgentSessionID, sessionID))
	}
	return &run, nil
}

type pendingWake struct {
	MessageID   uint64
	TotalRuns   int64
	OtherActive int64
}
