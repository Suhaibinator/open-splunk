// Package common_persistence_models contains the database models used by the persistence layer.
// This file defines ChatMessageToolCall, the 1:1 side table that holds the agent/tool-only
// columns split off ChatMessage so human-only chat rows do not pay their row-width cost — the
// same precedent as ChatInferenceCall keeping inference accounting off the hot message table.
//
// TOOL_CALL, TOOL_RESULT, COMPACTION_SUMMARY, and PROVIDER_STATE carry structured side
// rows; SYSTEM_EVENT may carry a typed context-event payload. STANDARD and AGENT_NOTE
// never carry side rows. The hot-path discriminators that are filtered on
// every visible read (kind, hidden_from_users, etc.) stay on ChatMessage so reads need no join.
package common_persistence_models

import (
	"time"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// ChatMessageToolCall defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package. One row per agent transcript
// message that carries a structured payload and/or tool-call approval/execution state.
type ChatMessageToolCall struct {
	// ArgumentsRejected is an immutable marker used for durable malformed-generation budgets.
	ArgumentsRejected bool `json:"arguments_rejected" gorm:"column:arguments_rejected;not null;default:false"`
	// MessageID is the primary key and 1:1 FK to chat_messages.id. Deleting the message cascades
	// to this row.
	MessageID uint64 `json:"message_id" gorm:"primaryKey;column:message_id"`

	// Message is the owning message record (when preloaded). Not normally needed since the read
	// path preloads ToolCall onto the message, not the reverse.
	Message *ChatMessage `json:"message,omitempty" gorm:"foreignKey:MessageID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`

	// Denormalized-at-insert keys (set once with the message, immutable thereafter) so every
	// agentic partial index lives entirely on this table without a join back to chat_messages.
	ChannelID  uint64                   `json:"channel_id" gorm:"column:channel_id;not null"`
	AgentRunID *uint64                  `json:"agent_run_id,omitempty" gorm:"column:agent_run_id"`
	Kind       proto_models.MessageKind `json:"kind" gorm:"column:kind;type:smallint;not null;default:0"`

	// ToolCallID is the denormalized pairing key for TOOL_CALL/TOOL_RESULT rows. It must match the
	// payload's tool_call_id. Partial-unique indexes on (agent_run_id, tool_call_id) are added via
	// raw SQL (GORM tags cannot express the WHERE-on-kind clause).
	ToolCallID *string `json:"tool_call_id,omitempty" gorm:"column:tool_call_id;type:varchar(255)"`

	// ModelPayload is the protobuf-serialized MessageModelPayload (tool call args, tool result,
	// compaction metadata), mirroring how MessageEmbed.EmbedData works.
	ModelPayload []byte `json:"model_payload,omitempty" gorm:"column:model_payload;type:bytea"`

	// PayloadType discriminates the oneof inside ModelPayload (tool_call/tool_result/compaction).
	PayloadType proto_models.MessageModelPayloadType `json:"payload_type" gorm:"column:payload_type;type:smallint;not null;default:0"`

	// Approval columns (authoritative, indexed — never inside the payload). These drive the
	// pending-approval sweep, "awaiting me" queries, and the resolution audit trail.
	ApprovalStatus    proto_models.ToolCallApprovalStatus `json:"approval_status" gorm:"column:approval_status;type:smallint;not null;default:0"`
	TriggeringUserID  *uint64                             `json:"triggering_user_id,omitempty" gorm:"column:triggering_user_id;index"`
	ResolvedByUserID  *uint64                             `json:"resolved_by_user_id,omitempty" gorm:"column:resolved_by_user_id"`
	ResolvedAt        *time.Time                          `json:"resolved_at,omitempty" gorm:"column:resolved_at;type:timestamptz"`
	ResolutionReason  *string                             `json:"resolution_reason,omitempty" gorm:"column:resolution_reason;type:varchar(1024)"`
	ApprovalExpiresAt *time.Time                          `json:"approval_expires_at,omitempty" gorm:"column:approval_expires_at;type:timestamptz"`

	// Execution columns (operational state for the durable approved-work queue). Partial indexes
	// on approval_status/execution_state are added via raw SQL.
	ExecutionState          proto_models.ToolCallExecutionState `json:"execution_state" gorm:"column:execution_state;type:smallint;not null;default:0"`
	ExecutionAttemptCount   uint32                              `json:"execution_attempt_count" gorm:"column:execution_attempt_count;not null;default:0"`
	ExecutionLeaseOwner     *string                             `json:"execution_lease_owner,omitempty" gorm:"column:execution_lease_owner;type:varchar(255)"`
	ExecutionLeaseExpiresAt *time.Time                          `json:"execution_lease_expires_at,omitempty" gorm:"column:execution_lease_expires_at;type:timestamptz"`
	// ExecutionNotBefore is the durable retry deadline. A retryable failure returns the row to
	// PENDING_EXECUTION but keeps it out of every replica's claim predicate until this DB-clock
	// timestamp is due, preventing a hot failure from monopolizing the bounded work scan.
	ExecutionNotBefore *time.Time `json:"execution_not_before,omitempty" gorm:"column:execution_not_before;type:timestamptz"`
	ExecutionStartedAt *time.Time `json:"execution_started_at,omitempty" gorm:"column:execution_started_at;type:timestamptz"`
	ExecutedAt         *time.Time `json:"executed_at,omitempty" gorm:"column:executed_at;type:timestamptz"`
	LastExecutionError *string    `json:"last_execution_error,omitempty" gorm:"column:last_execution_error;type:varchar(2048)"`

	// CreatedAt timestamp when the tool-call row was written (alongside its message).
	CreatedAt time.Time `json:"created_at" gorm:"type:timestamptz;autoCreateTime"`

	// UpdatedAt timestamp when the approval/execution state last changed.
	UpdatedAt time.Time `json:"updated_at" gorm:"type:timestamptz;autoUpdateTime"`
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (ChatMessageToolCall) TableName() string {
	return TableNameChatMessageToolCalls
}
