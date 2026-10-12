package common_persistence

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrChatAgentChildRunLimit         = errors.New("max_child_runs_reached")
	ErrChatAgentChildConcurrencyLimit = errors.New("max_active_children_reached")
	ErrChatAgentChildUnavailable      = errors.New("delegated child is unavailable")
	// ErrChatAgentMessageInvalid means the send input itself is malformed (a service bug).
	ErrChatAgentMessageInvalid = errors.New("delegated agent message input is invalid")
	// ErrChatAgentMessageScopeInvalid means the writer produced a message outside the child's thread.
	ErrChatAgentMessageScopeInvalid = errors.New("delegated agent message is outside the child's thread")
	ErrChatAgentMessagePageInvalid  = errors.New("delegated agent message page is invalid")
	// ErrChatAgentParentUnavailable means a parent cannot be resumed: it is archived, not a
	// main-timeline session, interactive with automatic runs disabled, or in an archived channel.
	ErrChatAgentParentUnavailable = errors.New("delegation parent is unavailable")
)

// childMessageReceiptKind is the kind of every receipt a parent's send writes; the child-message
// reads and drains consider only these, never an application's PARENT_RESUME receipts.
const childMessageReceiptKind = proto_models.ChatAgentMessageReceiptKind_CHAT_AGENT_MESSAGE_RECEIPT_KIND_CHILD_MESSAGE

// ChatAgentChildWakeLimits are the caps a child wake is admitted against: the per-child run cap
// and the parent's active-children cap. A zero MaxActiveChildren means the parent's agent type
// does not delegate (or is not registered), so no child of it may be woken.
type ChatAgentChildWakeLimits struct{ MaxRuns, MaxActiveChildren int }

// ChatAgentChildWakeLimitsProvider samples the caps for one parent: its agent type and the
// delegation override payload of its active config version (nil when it has none). The chat
// service installs it at startup; sends and terminal drains both read it, so there is one source
// of truth.
type ChatAgentChildWakeLimitsProvider func(parentAgentType string, delegationOverride []byte) ChatAgentChildWakeLimits

// ChatAgentMessagingPersistence is the bounded, service-authorized parent-to-child messaging seam.
type ChatAgentMessagingPersistence interface {
	// SetChatAgentChildWakeLimitsProvider installs the caps source; nil uninstalls it, after which
	// every cap reads as zero and no child can be woken.
	SetChatAgentChildWakeLimitsProvider(ChatAgentChildWakeLimitsProvider)
	// SendDelegatedAgentMessage records one parent send inside the caller's transaction. write
	// persists the message row and returns its id; it runs only after replay, gate, and admission
	// checks pass. The bool reports a replay of an earlier committed send with the same key.
	SendDelegatedAgentMessage(context.Context, ChatAgentChildMessageInput, func() (uint64, error)) (*pm.ChatAgentMessageReceipt, bool, error)
	// ReadDelegatedAgentMessages pages one child thread's visible conversation rows.
	ReadDelegatedAgentMessages(context.Context, ChatAgentMessagePageInput) (*ChatAgentMessagePage, error)
	// CountChatAgentSessionRuns counts every run (any status) per session.
	CountChatAgentSessionRuns(context.Context, []uint64) (map[uint64]uint64, error)
	// GetChatAgentSessionPendingWakeMessageID returns a session's pending wake trigger (zero when
	// nothing is pending), or ErrChatAgentSessionNotFound.
	GetChatAgentSessionPendingWakeMessageID(ctx context.Context, sessionID uint64) (uint64, error)
	// ListUnconsumedChildMessageIDs lists outstanding continue receipts through the supplied
	// transcript boundary. Deleted/excluded messages still owe explicit context proof.
	ListUnconsumedChildMessageIDs(ctx context.Context, sessionID, throughID uint64) ([]uint64, error)
	// LatestDiscardedChildMessages returns, per child session, the newest receipt a drain marked
	// Discarded. Children without one are absent.
	LatestDiscardedChildMessages(ctx context.Context, childSessionIDs []uint64) (map[uint64]*pm.ChatAgentMessageReceipt, error)
	// ResumeChatAgentParent records one application resume of a delegation parent inside the
	// caller's transaction: an app_resume note on the parent's timeline, a minted or coalesced
	// parent run, and a PARENT_RESUME receipt. The bool reports a replay of an earlier committed
	// resume with the same key.
	ResumeChatAgentParent(context.Context, ChatAgentParentResumeInput) (*pm.ChatAgentMessageReceipt, bool, error)
}

// ChatAgentParentResumeInput identifies one application resume of a delegation parent. Content is
// the note the parent's next run reads. MaxContentLength, when positive, bounds a new resume's
// Content in bytes; a replay is not held to it.
type ChatAgentParentResumeInput struct {
	ParentSessionID            uint64
	InvocationKey, Fingerprint string
	Content                    string
	MaxContentLength           int
}

// ChatAgentChildMessageInput identifies one parent send. Caps come from the installed provider.
type ChatAgentChildMessageInput struct {
	ParentSessionID, ChildSessionID, OriginRunID uint64
	InvocationKey, Fingerprint                   string
	Mode                                         pm.ChatAgentMessageMode
}

func (h *CommonPersistenceHandler[U]) SetChatAgentChildWakeLimitsProvider(p ChatAgentChildWakeLimitsProvider) {
	if p == nil {
		h.childWakeLimits.Store(nil)
	} else {
		h.childWakeLimits.Store(&p)
	}
}

// childWakeLimitsTx samples the installed provider for parent: its agent type and the delegation
// override of its active config version, so a session-narrowed MaxActiveChildren applies to child
// wakes as it does to creation. Without a provider every cap is zero, like the budget ceilings: a
// zero MaxActiveChildren is a data outcome (the parent does not delegate), while a zero MaxRuns
// cannot happen under the KMS floors and is an error.
func (h *CommonPersistenceHandler[U]) childWakeLimitsTx(tx *gorm.DB, parent *pm.ChatAgentSession) (ChatAgentChildWakeLimits, error) {
	var limits ChatAgentChildWakeLimits
	if p := h.childWakeLimits.Load(); p != nil && *p != nil {
		var snapshot pm.ChatAgentSessionConfigSnapshot
		if err := tx.Select("delegation_override_payload").Where("agent_session_id = ? AND version = ?", parent.ID, parent.ActiveConfigVersion).
			Limit(1).Find(&snapshot).Error; err != nil {
			return limits, err
		}
		limits = (*p)(parent.AgentType, snapshot.DelegationOverridePayload)
	}
	if limits.MaxRuns <= 0 {
		return limits, fmt.Errorf("delegated child wake limits: max runs per child is %d for parent agent type %q", limits.MaxRuns, parent.AgentType)
	}
	return limits, nil
}

// takeChatAgentSessionTx takes query's session into dst, mapping a missing row to
// ErrChatAgentSessionNotFound.
func takeChatAgentSessionTx(dst *pm.ChatAgentSession, query *gorm.DB) error {
	err := query.Take(dst).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrChatAgentSessionNotFound
	}
	return err
}

// SendDelegatedAgentMessage runs inside the service-owned transaction. The channel lock serializes
// child sends, spawning, and terminal drains before any new message ID is allocated.
func (h *CommonPersistenceHandler[U]) SendDelegatedAgentMessage(ctx context.Context, in ChatAgentChildMessageInput, write func() (uint64, error)) (*pm.ChatAgentMessageReceipt, bool, error) {
	if in.InvocationKey == "" || in.Fingerprint == "" || write == nil || (in.Mode != pm.ChatAgentMessageModeContinue && in.Mode != pm.ChatAgentMessageModeQueueOnly) {
		return nil, false, ErrChatAgentMessageInvalid
	}
	tx := h.getDBFromContext(ctx)
	origin, originErr := LockChatAgentDelegationOrigin(ctx, h, in.OriginRunID)
	if originErr != nil && !errors.Is(originErr, ErrChatAgentRunNotFound) {
		return nil, false, originErr
	}
	var parent, child pm.ChatAgentSession
	if err := takeChatAgentSessionTx(&parent, tx.Select("channel_id").Where("id = ?", in.ParentSessionID)); err != nil {
		return nil, false, err
	}
	channel, err := h.lockChatLocationChannel(ctx, parent.ChannelID)
	if err != nil {
		return nil, false, err
	}
	if err := takeChatAgentSessionTx(&parent, tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", in.ParentSessionID)); err != nil {
		return nil, false, err
	}
	if err := takeChatAgentSessionTx(&child, tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND parent_agent_session_id = ?", in.ChildSessionID, parent.ID)); err != nil {
		return nil, false, err
	}
	// Replay precedes lifecycle/budget checks: a committed send remains the same outcome after retirement.
	var receipt pm.ChatAgentMessageReceipt
	err = tx.Where("invocation_key = ?", in.InvocationKey).Take(&receipt).Error
	if err == nil {
		if receipt.Kind != childMessageReceiptKind || receipt.Fingerprint != in.Fingerprint || receipt.ChildSessionID != child.ID || receipt.OriginRunID != in.OriginRunID {
			return nil, false, ErrChatAgentSessionIdempotencyConflict
		}
		return &receipt, true, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, err
	}
	if originErr != nil {
		return nil, false, originErr
	}
	if err := ValidateChatAgentDelegationOrigin(ctx, h, origin, parent.ID); err != nil {
		return nil, false, err
	}
	continueMode := in.Mode == pm.ChatAgentMessageModeContinue
	if childMessageGate(channel, &parent, &child, continueMode) != pm.ChatAgentMessageDiscardReasonNone {
		return nil, false, ErrChatAgentChildUnavailable
	}
	pool, err := loadChatAgentGraphBudgetTx(tx, parent.ID)
	if err == nil {
		err = graphBudgetExhaustedError(pool)
	}
	if err != nil {
		return nil, false, err
	}
	// Only a continue send needs the child's run counts: an active run coalesces the message into
	// the pending wake, otherwise a run is admitted against the caps and minted.
	var active int64
	if continueMode {
		var total int64
		total, active, err = countSessionRunsTx(tx, child.ID, 0)
		if err != nil {
			return nil, false, err
		}
		if active == 0 {
			limits, err := h.childWakeLimitsTx(tx, &parent)
			if err != nil {
				return nil, false, err
			}
			if limits.MaxActiveChildren <= 0 {
				return nil, false, ErrChatAgentChildUnavailable
			}
			if err := childWakeAdmissionTx(tx, total, parent.ID, limits); err != nil {
				return nil, false, err
			}
		}
	}
	id, err := write()
	if err != nil {
		return nil, false, err
	}
	// The writer is service code; the receipt and wake trigger must still be a row in the child's
	// own thread, or the child would be woken for a message it cannot see.
	var scoped int64
	if err := tx.Model(&pm.ChatMessage{}).Where("id = ? AND channel_id = ? AND parent_message_id = ? AND deleted_at IS NULL", id, child.ChannelID, child.ParentMessageID).Count(&scoped).Error; err != nil {
		return nil, false, err
	}
	if scoped == 0 {
		return nil, false, ErrChatAgentMessageScopeInvalid
	}
	receipt = pm.ChatAgentMessageReceipt{Kind: childMessageReceiptKind, InvocationKey: in.InvocationKey, Fingerprint: in.Fingerprint, OriginRunID: in.OriginRunID, ChildSessionID: child.ID, MessageID: id, Disposition: pm.ChatAgentMessageDispositionQueued}
	switch {
	case !continueMode:
	case active > 0:
		if err := coalesceSessionWakeTx(tx, child.ID, id); err != nil {
			return nil, false, err
		}
		receipt.Disposition = pm.ChatAgentMessageDispositionCoalesced
	default:
		human, err := h.delegationWakeHumanTx(tx, &child, &parent)
		if err != nil {
			return nil, false, err
		}
		run := h.newSessionWakeRun(&child, id, human, 0, pool != nil)
		if err := tx.Create(run).Error; err != nil {
			return nil, false, err
		}
		receipt.CreatedRunID = run.ID
		receipt.Disposition = pm.ChatAgentMessageDispositionStarted
	}
	if err := tx.Create(&receipt).Error; err != nil {
		return nil, false, err
	}
	return &receipt, false, nil
}

// ResumeChatAgentParent follows SendDelegatedAgentMessage's sequence, pointed at the parent: under
// the channel and parent locks a committed resume with the same key replays (the same key with
// another fingerprint conflicts); then the parent must pass the wake guards and its graph budget
// pool must have room (ErrGraphBudgetExhausted); then the application's note is written, a parent
// run is minted or coalesced through the wake coalescer (a busy, suspended or inference-blocked
// parent defers it), and the receipt records the outcome. An owner-driven parent's graph advances
// its resume revision, as the last write. The note is an AGENT_NOTE with subtype app_resume and no
// run: every later run of the parent reads it as a message from the application.
func (h *CommonPersistenceHandler[U]) ResumeChatAgentParent(ctx context.Context, in ChatAgentParentResumeInput) (*pm.ChatAgentMessageReceipt, bool, error) {
	if in.ParentSessionID == 0 || in.InvocationKey == "" || in.Fingerprint == "" || in.Content == "" {
		return nil, false, ErrChatAgentMessageInvalid
	}
	tx := h.getDBFromContext(ctx)
	var parent pm.ChatAgentSession
	if err := takeChatAgentSessionTx(&parent, tx.Select("channel_id").Where("id = ?", in.ParentSessionID)); err != nil {
		return nil, false, err
	}
	if _, err := h.lockChatLocationChannel(ctx, parent.ChannelID); err != nil {
		return nil, false, err
	}
	if err := takeChatAgentSessionTx(&parent, tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", in.ParentSessionID)); err != nil {
		return nil, false, err
	}
	var receipt pm.ChatAgentMessageReceipt
	err := tx.Where("invocation_key = ?", in.InvocationKey).Take(&receipt).Error
	if err == nil {
		if receipt.Kind != proto_models.ChatAgentMessageReceiptKind_CHAT_AGENT_MESSAGE_RECEIPT_KIND_PARENT_RESUME ||
			receipt.Fingerprint != in.Fingerprint || receipt.ChildSessionID != parent.ID {
			return nil, false, ErrChatAgentSessionIdempotencyConflict
		}
		return &receipt, true, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, err
	}
	if in.MaxContentLength > 0 && len(in.Content) > in.MaxContentLength {
		return nil, false, fmt.Errorf("%w: content exceeds %d bytes", ErrChatAgentMessageInvalid, in.MaxContentLength)
	}
	if reason, err := h.parentWakeDecisionTx(tx, &parent, nil); err != nil || reason != "" {
		if err != nil {
			return nil, false, err
		}
		return nil, false, fmt.Errorf("%w: %s", ErrChatAgentParentUnavailable, reason)
	}
	pool, err := loadChatAgentGraphBudgetTx(tx, parent.GraphRootID())
	if err == nil {
		err = graphBudgetExhaustedError(pool)
	}
	if err != nil {
		return nil, false, err
	}
	// The note is visible: channel members see the application's message (clients render the
	// app_resume subtype), and it stays in the parent's AI context.
	exclude, hidden := false, false
	note := &pm.ChatMessage{
		ChannelID: parent.ChannelID, SenderID: parent.BotUserID, Content: in.Content,
		ContentType: proto_models.MessageContentType_MESSAGE_CONTENT_TYPE_TEXT,
		StreamState: proto_models.MessageStreamState_MESSAGE_STREAM_STATE_COMPLETE,
		Kind:        proto_models.MessageKind_MESSAGE_KIND_AGENT_NOTE, Role: proto_models.MessageRole_MESSAGE_ROLE_USER,
		ExcludeFromAIContext: &exclude, HiddenFromUsers: &hidden, AgentNoteSubtype: common_service_models.AgentNoteSubtypeAppResume,
	}
	// The wake guards admitted a main-timeline parent, so the note has no thread.
	if err := tx.Create(note).Error; err != nil {
		return nil, false, err
	}
	wake, err := h.createOrCoalesceSessionWakeTx(tx, &parent, note.ID, parent.UserID, 0)
	if err != nil {
		return nil, false, err
	}
	receipt = pm.ChatAgentMessageReceipt{
		Kind: proto_models.ChatAgentMessageReceiptKind_CHAT_AGENT_MESSAGE_RECEIPT_KIND_PARENT_RESUME, InvocationKey: in.InvocationKey,
		Fingerprint: in.Fingerprint, ChildSessionID: parent.ID, MessageID: note.ID, Disposition: pm.ChatAgentMessageDispositionCoalesced,
	}
	if wake.State == proto_models.DelegationWakeState_DELEGATION_WAKE_STATE_RUN_CREATED {
		receipt.Disposition, receipt.CreatedRunID = pm.ChatAgentMessageDispositionStarted, wake.RunID
	}
	if err := tx.Create(&receipt).Error; err != nil {
		return nil, false, err
	}
	if slices.Contains(h.chatAgentWorkflowTypes(), parent.AgentType) {
		if err := h.advanceChatAgentGraphResumeRevisionTx(ctx, tx, []uint64{parent.ID}, proto_models.GraphEventKind_GRAPH_EVENT_KIND_PARENT_RESUME); err != nil {
			return nil, false, err
		}
	}
	return &receipt, false, nil
}

// childMessageGate is the one gate a parent's message must pass to reach a child, shared by sends
// and the terminal drain. It returns the discard reason that blocks delivery, or None. requireAutoRun
// adds the child's auto-run policy: a continue send and the drain mint runs and need it; a
// queue_only send only leaves a row in the thread and does not. The drain deliberately does not
// apply the parent-wake guards (the parent's own auto-run policy, the shared-bot check): those
// decide whether the parent may be woken, not whether the child may work.
func childMessageGate(channel *pm.ChatChannel, parent, child *pm.ChatAgentSession, requireAutoRun bool) pm.ChatAgentMessageDiscardReason {
	if channel == nil || channel.IsArchived || !channel.Features.Has(pm.ChatChannelSupportThreads) {
		return pm.ChatAgentMessageDiscardReasonChannelUnavailable
	}
	if !parent.IsMainTimeline() || !parent.IsLive() || parent.IsSuspended() || child.ChannelID != parent.ChannelID {
		return pm.ChatAgentMessageDiscardReasonParentUnavailable
	}
	if !child.IsLive() {
		return pm.ChatAgentMessageDiscardReasonChildArchived
	}
	if requireAutoRun && (child.IsSuspended() || child.AutoRunPolicy != pm.ChatAgentSessionAutoRunPolicyAutomatic) {
		return pm.ChatAgentMessageDiscardReasonChildNotAutomatic
	}
	return pm.ChatAgentMessageDiscardReasonNone
}

// childWakeAdmissionTx applies the caps before a child run is minted for a parent's message.
// childRuns is the child's run count of any status, which the caller already read.
func childWakeAdmissionTx(tx *gorm.DB, childRuns int64, parentID uint64, limits ChatAgentChildWakeLimits) error {
	if childRuns >= int64(limits.MaxRuns) {
		return ErrChatAgentChildRunLimit
	}
	running, err := countRunningDelegatedChildrenTx(tx, parentID)
	if err != nil {
		return err
	}
	if running >= int64(limits.MaxActiveChildren) {
		return ErrChatAgentChildConcurrencyLimit
	}
	return nil
}

// drainPendingChildWakeTx runs at every terminal write of a delegated child's run, before the
// parent-wake decision, and returns the follow-up run it minted (nil when it minted none). The
// caller holds the channel lock. A normal
// completion that left steering messages unconsumed mints one follow-up run for the newest of
// them, exactly like a parent's drained wake; the caller then withholds the parent wake so the
// follow-up's own ending is the parent's one report for the chain. While another run of the child
// is still active the pending wake is retained for that run's ending, whatever this one's outcome.
// Every other outcome that abandons steering is a discard: the receipts it orphans are marked
// even when an idle-started run or an already-drained follow-up has no pending wake. Child sends
// differ from parent notifications: failure and cancellation discard wake intent rather than
// deferring it. A normal completion in a graph whose budget pool is used up retains the pending
// wake and its receipts instead, for SetChatAgentGraphBudget to drain once the pool has room.
func (h *CommonPersistenceHandler[U]) drainPendingChildWakeTx(ctx context.Context, tx *gorm.DB, run *pm.ChatAgentRun, child *pm.ChatAgentSession) (*pm.ChatAgentRun, error) {
	pending, err := lockPendingWakeTx(tx, child, run.ID)
	if err != nil {
		return nil, err
	}
	if pending.MessageID != 0 && pending.MessageID <= run.ConsumedPendingMessageID {
		// A sealed generation already represented the newest steering message.
		return nil, clearPendingWakeTx(tx, child.ID)
	}
	if pending.MessageID == 0 && run.Status == proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_COMPLETED && run.StatusReason == proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_UNSPECIFIED {
		return nil, nil
	}
	if pending.MessageID == 0 {
		// The shared coalescer skips run counts when there is no pending wake. Receipt ownership
		// still matters: another active run can answer an idle-started or drained instruction.
		_, pending.OtherActive, err = countSessionRunsTx(tx, child.ID, run.ID)
		if err != nil {
			return nil, err
		}
	}
	if pending.OtherActive != 0 {
		// Another run of the child still owes the drain; it happens at that run's own end.
		return nil, nil
	}
	discard := func(reason pm.ChatAgentMessageDiscardReason) error {
		// Session wake fields only describe a future run. Once a run starts they are cleared,
		// but its receipt still owes an answer. With no active run left, all unanswered continue
		// receipts are abandoned, including intent deferred by an earlier concurrent ending.
		var lastMessageID uint64
		if err := tx.Model(&pm.ChatAgentMessageReceipt{}).
			Select("COALESCE(MAX(message_id), 0)").
			Where("child_session_id = ? AND kind = ? AND disposition IN ?", child.ID, childMessageReceiptKind,
				[]pm.ChatAgentMessageDisposition{pm.ChatAgentMessageDispositionStarted, pm.ChatAgentMessageDispositionCoalesced}).
			Scan(&lastMessageID).Error; err != nil {
			return err
		}
		marked, err := h.markChildMessagesDiscardedTx(tx, run, child.ID, lastMessageID, reason)
		if err != nil {
			return err
		}
		if pending.MessageID != 0 || marked != 0 {
			appLogger.For(ctx, h.logger, loggerName).Warn("pending child wake discarded",
				zap.String(logkeys.Stage, "drain_pending_child_wake"),
				zap.Stringer(logkeys.Reason, reason),
				zap.Uint64(appLogger.KeyAgentSessionID, child.ID),
				zap.Uint64(appLogger.KeyAgentRunID, run.ID),
				zap.Uint64(appLogger.KeyMessageID, max(pending.MessageID, lastMessageID)),
				zap.Int64(appLogger.KeyCount, marked))
		}
		if pending.MessageID == 0 {
			return nil
		}
		return clearPendingWakeTx(tx, child.ID)
	}
	switch {
	case run.Status == proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_FAILED:
		return nil, discard(pm.ChatAgentMessageDiscardReasonRunFailed)
	case run.Status == proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_CANCELLED:
		return nil, discard(pm.ChatAgentMessageDiscardReasonRunCancelled)
	case run.Status != proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_COMPLETED || run.StatusReason != proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_UNSPECIFIED:
		return nil, discard(pm.ChatAgentMessageDiscardReasonRunStopped)
	}
	var parent pm.ChatAgentSession
	if err := tx.Where("id = ?", common_util.Deref(child.ParentAgentSessionID)).Take(&parent).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, discard(pm.ChatAgentMessageDiscardReasonParentUnavailable)
		}
		return nil, err
	}
	var channel *pm.ChatChannel
	var loaded pm.ChatChannel
	if err := tx.Select("id", "is_archived", "features").Where("id = ?", child.ChannelID).Take(&loaded).Error; err == nil {
		channel = &loaded
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	if reason := childMessageGate(channel, &parent, child, true); reason != pm.ChatAgentMessageDiscardReasonNone {
		return nil, discard(reason)
	}
	limits, err := h.childWakeLimitsTx(tx, &parent)
	if err != nil {
		return nil, err
	}
	if limits.MaxActiveChildren <= 0 {
		return nil, discard(pm.ChatAgentMessageDiscardReasonParentUnavailable)
	}
	if err := childWakeAdmissionTx(tx, pending.TotalRuns, parent.ID, limits); err != nil {
		switch {
		case errors.Is(err, ErrChatAgentChildRunLimit):
			return nil, discard(pm.ChatAgentMessageDiscardReasonRunLimit)
		case errors.Is(err, ErrChatAgentChildConcurrencyLimit):
			return nil, discard(pm.ChatAgentMessageDiscardReasonConcurrencyLimit)
		}
		return nil, err
	}
	human, err := h.delegationWakeHumanTx(tx, child, &parent)
	if err != nil {
		return nil, err
	}
	pool, err := loadChatAgentGraphBudgetTx(tx, parent.ID)
	if err != nil || graphBudgetExhaustedError(pool) != nil {
		return nil, err
	}
	// No other run of the child is active (checked above under the child's lock), so the wake is
	// minted directly rather than through the coalescer.
	follow := h.newSessionWakeRun(child, pending.MessageID, human, run.ID, pool != nil)
	if err := tx.Create(follow).Error; err != nil {
		return nil, err
	}
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("pending child wake drained into one follow-up run",
			zap.Uint64(appLogger.KeyAgentSessionID, child.ID),
			zap.Uint64(appLogger.KeyAgentRunID, follow.ID),
			zap.Uint64(appLogger.KeyMessageID, pending.MessageID))
	}
	return follow, clearPendingWakeTx(tx, child.ID)
}

// markChildMessagesDiscardedTx moves the Started and Coalesced receipts whose answers are now
// owed to nobody to Discarded and returns how many it marked. Those are the receipts newer than
// everything a generation has already answered: every child run's consumed watermark, and the
// trigger of every run that completed normally (run itself included when it did: its trigger was
// answered even though its pending wake is being dropped). Consumed steering remains answered
// even when that run later fails or defers settlement to another active run. A follow-up run owes
// the messages its predecessor drained, which are older than its own trigger. Queued
// receipts never held wake intent and are left alone.
func (h *CommonPersistenceHandler[U]) markChildMessagesDiscardedTx(tx *gorm.DB, run *pm.ChatAgentRun, childID, pending uint64, reason pm.ChatAgentMessageDiscardReason) (int64, error) {
	result := tx.Model(&pm.ChatAgentMessageReceipt{}).
		Where("child_session_id = ? AND kind = ? AND disposition IN ? AND message_id <= ? AND message_id > GREATEST(?, "+answeredChildMessagesSQL+")",
			childID, childMessageReceiptKind, []pm.ChatAgentMessageDisposition{pm.ChatAgentMessageDispositionStarted, pm.ChatAgentMessageDispositionCoalesced}, pending,
			run.ConsumedPendingMessageID,
			proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_COMPLETED, proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_UNSPECIFIED, childID).
		Select("disposition", "discard_reason").
		Updates(pm.ChatAgentMessageReceipt{Disposition: pm.ChatAgentMessageDispositionDiscarded, DiscardReason: reason})
	return result.RowsAffected, result.Error
}

// Shared by context requirements and terminal settlement: sealed consumption survives failed
// runs, while a normally completed historical trigger is also considered answered.
const answeredChildMessagesSQL = "(SELECT COALESCE(MAX(GREATEST(consumed_pending_message_id, CASE WHEN status = ? AND status_reason = ? THEN COALESCE(triggering_message_id, 0) ELSE 0 END)), 0) FROM " + pm.TableNameChatAgentRuns + " WHERE agent_session_id = ?)"

func (h *CommonPersistenceHandler[U]) ListUnconsumedChildMessageIDs(ctx context.Context, sessionID, throughID uint64) ([]uint64, error) {
	if sessionID == 0 {
		return nil, ErrChatAgentSessionNotFound
	}
	if throughID == 0 {
		return nil, nil
	}
	var ids []uint64
	err := h.getDBFromContext(ctx).Model(&pm.ChatAgentMessageReceipt{}).
		Where("child_session_id = ? AND kind = ? AND disposition IN ? AND message_id <= ? AND message_id > "+answeredChildMessagesSQL,
			sessionID, childMessageReceiptKind, []pm.ChatAgentMessageDisposition{pm.ChatAgentMessageDispositionStarted, pm.ChatAgentMessageDispositionCoalesced}, throughID,
			proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_COMPLETED, proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_UNSPECIFIED, sessionID).
		Order("message_id ASC").Pluck("message_id", &ids).Error
	if err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_unconsumed_child_messages"),
			zap.Uint64(appLogger.KeyAgentSessionID, sessionID))
	}
	return ids, nil
}

// deleteChildMessageReceiptsTx removes every receipt addressed to a child of parentSessionID, and
// the session's own PARENT_RESUME receipts: only the archived parent's runs could replay either.
func deleteChildMessageReceiptsTx(tx *gorm.DB, parentSessionID uint64) error {
	return tx.Where("child_session_id IN (SELECT id FROM "+pm.TableNameChatAgentSessions+" WHERE parent_agent_session_id = ?) OR (child_session_id = ? AND kind = ?)",
		parentSessionID, parentSessionID, proto_models.ChatAgentMessageReceiptKind_CHAT_AGENT_MESSAGE_RECEIPT_KIND_PARENT_RESUME).
		Delete(&pm.ChatAgentMessageReceipt{}).Error
}

func (h *CommonPersistenceHandler[U]) GetChatAgentSessionPendingWakeMessageID(ctx context.Context, sessionID uint64) (uint64, error) {
	if sessionID == 0 {
		return 0, ErrChatAgentSessionNotFound
	}
	var row struct{ PendingWakeMessageID *uint64 }
	err := h.getDBFromContext(ctx).Model(&pm.ChatAgentSession{}).Select("pending_wake_message_id").Where("id = ?", sessionID).Take(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, ErrChatAgentSessionNotFound
		}
		return 0, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "get_chat_agent_session_pending_wake"),
			zap.Uint64(appLogger.KeyAgentSessionID, sessionID))
	}
	return common_util.Deref(row.PendingWakeMessageID), nil
}

func (h *CommonPersistenceHandler[U]) CountChatAgentSessionRuns(ctx context.Context, ids []uint64) (map[uint64]uint64, error) {
	out := map[uint64]uint64{}
	ids = common_util.DeduplicateNonZero(ids)
	if len(ids) == 0 {
		return out, nil
	}
	var rows []struct {
		AgentSessionID uint64
		Count          uint64
	}
	if err := h.getDBFromContext(ctx).Model(&pm.ChatAgentRun{}).Select("agent_session_id, COUNT(*) AS count").Where("agent_session_id IN ?", ids).Group("agent_session_id").Scan(&rows).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "count_chat_agent_session_runs"),
			zap.Int(appLogger.KeyCount, len(ids)))
	}
	for _, r := range rows {
		out[r.AgentSessionID] = r.Count
	}
	return out, nil
}

func (h *CommonPersistenceHandler[U]) LatestDiscardedChildMessages(ctx context.Context, childSessionIDs []uint64) (map[uint64]*pm.ChatAgentMessageReceipt, error) {
	out := map[uint64]*pm.ChatAgentMessageReceipt{}
	childSessionIDs = common_util.DeduplicateNonZero(childSessionIDs)
	if len(childSessionIDs) == 0 {
		return out, nil
	}
	var rows []*pm.ChatAgentMessageReceipt
	if err := h.getDBFromContext(ctx).Model(&pm.ChatAgentMessageReceipt{}).
		Select("DISTINCT ON (child_session_id) *").
		Where("child_session_id IN ? AND kind = ? AND disposition = ?", childSessionIDs, childMessageReceiptKind, pm.ChatAgentMessageDispositionDiscarded).
		Order("child_session_id, id DESC").
		Find(&rows).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "latest_discarded_child_messages"),
			zap.Int(appLogger.KeyCount, len(childSessionIDs)))
	}
	for _, row := range rows {
		out[row.ChildSessionID] = row
	}
	return out, nil
}

type ChatAgentMessagePageInput struct {
	ChannelID, ThreadRootID, BeforeID, AfterID uint64
	Limit                                      int
	IncludeTools                               bool
}
type ChatAgentMessagePage struct {
	Messages            []*pm.ChatMessage
	HasBefore, HasAfter bool
}

// ReadDelegatedAgentMessages pages the model-visible thread newest-first (or oldest-first after
// AfterID). Rows excluded from AI context are omitted from both pages and cursor probes. The
// caller clamps Limit to its configured maximum; only a non-positive limit is rejected here. One
// extra row is fetched in the page direction to learn whether the page continues that way, and a
// single probe answers the other direction.
func (h *CommonPersistenceHandler[U]) ReadDelegatedAgentMessages(ctx context.Context, in ChatAgentMessagePageInput) (*ChatAgentMessagePage, error) {
	if in.Limit < 1 || in.BeforeID != 0 && in.AfterID != 0 || in.ThreadRootID == 0 {
		return nil, ErrChatAgentMessagePageInvalid
	}
	fail := func(err error) error {
		return chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "read_delegated_agent_messages"),
			zap.Uint64(appLogger.KeyChannelID, in.ChannelID),
			zap.Uint64(appLogger.KeyMessageID, in.ThreadRootID))
	}
	kinds := []proto_models.MessageKind{proto_models.MessageKind_MESSAGE_KIND_STANDARD}
	if in.IncludeTools {
		kinds = append(kinds, proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL, proto_models.MessageKind_MESSAGE_KIND_TOOL_RESULT)
	}
	// Matches the closing-row read in settleDelegatedChildRunTx: a row still streaming is not part
	// of the transcript yet.
	streamStates := []proto_models.MessageStreamState{proto_models.MessageStreamState_MESSAGE_STREAM_STATE_UNSPECIFIED, proto_models.MessageStreamState_MESSAGE_STREAM_STATE_COMPLETE}
	base := func() *gorm.DB {
		return applyChatMessageReadOptions(h.getDBFromContext(ctx).Model(&pm.ChatMessage{}), ChatMessageReadOptions{}).
			Where("exclude_from_ai_context IS NOT TRUE").
			Where("channel_id = ? AND parent_message_id = ? AND kind IN ? AND stream_state IN ?", in.ChannelID, in.ThreadRootID, kinds, streamStates)
	}
	query := base().Preload(pm.PreloadSender)
	if in.IncludeTools {
		query = query.Preload(pm.PreloadToolCall)
	}
	order := "id DESC"
	forward := false
	if in.BeforeID != 0 {
		query = query.Where("id < ?", in.BeforeID)
	}
	if in.AfterID != 0 {
		query = query.Where("id > ?", in.AfterID)
		order = "id ASC"
		forward = true
	}
	out := &ChatAgentMessagePage{}
	if err := query.Order(order).Limit(in.Limit + 1).Find(&out.Messages).Error; err != nil {
		return nil, fail(err)
	}
	if len(out.Messages) > in.Limit {
		out.Messages = out.Messages[:in.Limit]
		if forward {
			out.HasAfter = true
		} else {
			out.HasBefore = true
		}
	}
	if len(out.Messages) == 0 {
		return out, nil
	}
	// The first row is the page's edge on the probed side: its lowest id when paging forward, its
	// highest when paging backward.
	edge := out.Messages[0].ID
	probe := base().Select("1").Limit(1)
	if forward {
		probe = probe.Where("id < ?", edge)
	} else {
		probe = probe.Where("id > ?", edge)
	}
	var n int64
	if err := probe.Scan(&n).Error; err != nil {
		return nil, fail(err)
	}
	if forward {
		out.HasBefore = n > 0
	} else {
		out.HasAfter = n > 0
	}
	return out, nil
}
