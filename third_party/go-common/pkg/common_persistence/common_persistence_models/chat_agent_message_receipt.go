package common_persistence_models

import (
	"time"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// ChatAgentMessageMode is how a parent asked its message to reach a delegated child. It is the
// send input; the receipt derives it from the disposition (see ChatAgentMessageReceipt.Mode).
type ChatAgentMessageMode int16

const (
	// ChatAgentMessageModeUnspecified is the zero value and is never a valid send.
	ChatAgentMessageModeUnspecified ChatAgentMessageMode = 0
	// ChatAgentMessageModeContinue asks for a child run: started when the child is idle, coalesced
	// into the child's pending wake when a run is active.
	ChatAgentMessageModeContinue ChatAgentMessageMode = 1
	// ChatAgentMessageModeQueueOnly leaves the message in the thread for the child's next run.
	ChatAgentMessageModeQueueOnly ChatAgentMessageMode = 2
)

// String returns the tool vocabulary name of the mode.
func (m ChatAgentMessageMode) String() string {
	switch m {
	case ChatAgentMessageModeContinue:
		return "continue"
	case ChatAgentMessageModeQueueOnly:
		return "queue_only"
	}
	return "unspecified"
}

// ChatAgentMessageDisposition is the durable outcome of one send. Values are database data; do
// not renumber them.
type ChatAgentMessageDisposition int16

const (
	// ChatAgentMessageDispositionUnspecified is the zero value and is never written.
	ChatAgentMessageDispositionUnspecified ChatAgentMessageDisposition = 0
	// ChatAgentMessageDispositionQueued is a queue_only send: the message waits in the thread.
	ChatAgentMessageDispositionQueued ChatAgentMessageDisposition = 1
	// ChatAgentMessageDispositionStarted means the send minted a child run (CreatedRunID).
	ChatAgentMessageDispositionStarted ChatAgentMessageDisposition = 2
	// ChatAgentMessageDispositionCoalesced means the child had an active run, so the message was
	// recorded as the child's pending wake.
	ChatAgentMessageDispositionCoalesced ChatAgentMessageDisposition = 3
	// ChatAgentMessageDispositionDiscarded means a terminal drain dropped the wake intent before any
	// generation represented the message; DiscardReason says why. Only Started and Coalesced
	// receipts move here.
	ChatAgentMessageDispositionDiscarded ChatAgentMessageDisposition = 4
)

// String returns the tool vocabulary name of the disposition.
func (d ChatAgentMessageDisposition) String() string {
	switch d {
	case ChatAgentMessageDispositionQueued:
		return "queued"
	case ChatAgentMessageDispositionStarted:
		return "started"
	case ChatAgentMessageDispositionCoalesced:
		return "coalesced"
	case ChatAgentMessageDispositionDiscarded:
		return "discarded"
	}
	return "unspecified"
}

// ChatAgentMessageDiscardReason is why a drain discarded a Started or Coalesced message. Values are
// database data; do not renumber them.
type ChatAgentMessageDiscardReason int16

const (
	// ChatAgentMessageDiscardReasonNone is recorded on every receipt that is not Discarded.
	ChatAgentMessageDiscardReasonNone ChatAgentMessageDiscardReason = 0
	// ChatAgentMessageDiscardReasonRunFailed: the child run that owed the answer ended FAILED.
	ChatAgentMessageDiscardReasonRunFailed ChatAgentMessageDiscardReason = 1
	// ChatAgentMessageDiscardReasonRunCancelled: the child run that owed the answer was cancelled.
	ChatAgentMessageDiscardReasonRunCancelled ChatAgentMessageDiscardReason = 2
	// ChatAgentMessageDiscardReasonRunStopped: the child run ended on a step, budget, or approval
	// limit rather than with an answer.
	ChatAgentMessageDiscardReasonRunStopped ChatAgentMessageDiscardReason = 3
	// ChatAgentMessageDiscardReasonChannelUnavailable: the channel is gone, archived, or no longer
	// supports threads.
	ChatAgentMessageDiscardReasonChannelUnavailable ChatAgentMessageDiscardReason = 4
	// ChatAgentMessageDiscardReasonParentUnavailable: the parent is gone, archived, or not the
	// child's main-timeline session.
	ChatAgentMessageDiscardReasonParentUnavailable ChatAgentMessageDiscardReason = 5
	// ChatAgentMessageDiscardReasonChildArchived: the child session was archived.
	ChatAgentMessageDiscardReasonChildArchived ChatAgentMessageDiscardReason = 6
	// ChatAgentMessageDiscardReasonChildNotAutomatic: the child's auto-run policy no longer mints
	// follow-up runs, or a person's Stop suspended the child.
	ChatAgentMessageDiscardReasonChildNotAutomatic ChatAgentMessageDiscardReason = 7
	// ChatAgentMessageDiscardReasonRunLimit: the child reached its per-child run cap.
	ChatAgentMessageDiscardReasonRunLimit ChatAgentMessageDiscardReason = 8
	// ChatAgentMessageDiscardReasonConcurrencyLimit: the parent's active-children cap was full.
	ChatAgentMessageDiscardReasonConcurrencyLimit ChatAgentMessageDiscardReason = 9
)

// String returns the stable identifier-like name used in logs and tool output.
func (r ChatAgentMessageDiscardReason) String() string {
	switch r {
	case ChatAgentMessageDiscardReasonNone:
		return "none"
	case ChatAgentMessageDiscardReasonRunFailed:
		return "run_failed"
	case ChatAgentMessageDiscardReasonRunCancelled:
		return "run_cancelled"
	case ChatAgentMessageDiscardReasonRunStopped:
		return "run_stopped"
	case ChatAgentMessageDiscardReasonChannelUnavailable:
		return "channel_unavailable"
	case ChatAgentMessageDiscardReasonParentUnavailable:
		return "parent_unavailable"
	case ChatAgentMessageDiscardReasonChildArchived:
		return "child_archived"
	case ChatAgentMessageDiscardReasonChildNotAutomatic:
		return "child_not_automatic"
	case ChatAgentMessageDiscardReasonRunLimit:
		return "max_child_runs_reached"
	case ChatAgentMessageDiscardReasonConcurrencyLimit:
		return "max_active_children_reached"
	}
	return "unknown"
}

// ChatAgentMessageReceipt binds a parent tool invocation to its original message and wake outcome,
// so a retried invocation replays the committed result instead of sending twice. Disposition moves
// only from Started or Coalesced to Discarded, written by the child run's terminal drain; the
// receipt carries no delivery or consumption state beyond that. A parent's receipts are deleted
// when the parent session is archived: only the parent's own runs can replay them.
//
// Kind distinguishes a parent's message to a child (CHILD_MESSAGE, the column default) from an
// application's resume of the parent itself (PARENT_RESUME), whose ChildSessionID is the parent and
// whose OriginRunID is zero.
type ChatAgentMessageReceipt struct {
	ID             uint64                                   `gorm:"primaryKey;autoIncrement"`
	Kind           proto_models.ChatAgentMessageReceiptKind `gorm:"type:smallint;not null;default:1"`
	InvocationKey  string                                   `gorm:"size:255;not null;uniqueIndex"`
	Fingerprint    string                                   `gorm:"size:64;not null"`
	OriginRunID    uint64                                   `gorm:"not null"`
	ChildSessionID uint64                                   `gorm:"not null;index:idx_chat_agent_message_receipts_child_message,priority:1"`
	MessageID      uint64                                   `gorm:"not null;uniqueIndex;index:idx_chat_agent_message_receipts_child_message,priority:2"`
	Disposition    ChatAgentMessageDisposition              `gorm:"type:smallint;not null;default:0"`
	DiscardReason  ChatAgentMessageDiscardReason            `gorm:"type:smallint;not null;default:0"`
	CreatedRunID   uint64                                   `gorm:"not null;default:0"`
	CreatedAt      time.Time
}

func (ChatAgentMessageReceipt) TableName() string { return TableNameChatAgentMessageReceipts }

// Mode is the send mode the receipt records, derived from its disposition: Queued is the outcome
// of a queue_only send, and every other written disposition (Started, Coalesced, and the Discarded
// they move to) of a continue send.
func (r ChatAgentMessageReceipt) Mode() ChatAgentMessageMode {
	switch r.Disposition {
	case ChatAgentMessageDispositionQueued:
		return ChatAgentMessageModeQueueOnly
	case ChatAgentMessageDispositionStarted, ChatAgentMessageDispositionCoalesced, ChatAgentMessageDispositionDiscarded:
		return ChatAgentMessageModeContinue
	}
	return ChatAgentMessageModeUnspecified
}
