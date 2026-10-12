// Package common_persistence_models contains the database models used by the persistence layer.
// This file defines the ChatAgentRun model: the go-common-owned durable identity for one
// logical agent turn/resume chain. Its ID is stored as ChatMessage.AgentRunID and
// ChatInferenceCall.AgentRunID, grouping every transcript and accounting row a run produces.
package common_persistence_models

import (
	"time"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// ChatAgentRun defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type ChatAgentRun struct {
	RequestedInferenceModelID *uint64                    `json:"requested_inference_model_id,omitempty"`
	RequestedInferenceAPI     *proto_models.InferenceAPI `json:"requested_inference_api,omitempty"`
	ExecutionInferenceModelID *uint64                    `json:"execution_inference_model_id,omitempty"`
	ExecutionInferenceAPI     *proto_models.InferenceAPI `json:"execution_inference_api,omitempty"`
	ExecutionProviderID       *string                    `json:"execution_provider_id,omitempty"`
	ExecutionProviderModelID  *string                    `json:"execution_provider_model_id,omitempty"`

	// ID is the durable go-common run identifier (auto-incrementing primary key).
	// This is the value stored as ChatMessage.AgentRunID and ChatInferenceCall.AgentRunID.
	ID uint64 `json:"id" gorm:"primaryKey;autoIncrement"`

	// OrganizationID is the immutable organization snapshot captured from the owning session when
	// the run is created. Recovery and approval resumes use this value instead of request context.
	// Database ownership constraints require a positive existing organization.
	OrganizationID int64 `json:"organization_id" gorm:"index:idx_chat_agent_runs_organization_started,priority:1"`

	// ChannelID references the channel that owns the run and all transcript rows written by it.
	ChannelID uint64 `json:"channel_id" gorm:"not null"`

	// ParentMessageID is the thread scope for the run; 0 means main timeline (matching the
	// existing chat thread convention where proto uint64 0 == main timeline). The thread-scoped
	// status read uses idx_chat_agent_runs_channel_parent_run (channel_id, parent_message_id,
	// id DESC), which is created via raw SQL because GORM tags cannot express the sort order.
	ParentMessageID uint64 `json:"parent_message_id" gorm:"not null;default:0"`

	// AgentSessionID references ChatAgentSession when the run belongs to a first-class agent
	// chat session. Manual bot runs may omit a session.
	AgentSessionID *uint64 `json:"agent_session_id,omitempty" gorm:"index:idx_chat_agent_runs_session_id;index:idx_chat_agent_runs_session_cfgver_id,priority:1"`

	// AgentSessionConfigVersion is the config snapshot version captured at run start. Required
	// when AgentSessionID is set; used for approval resume and deterministic tool allowlisting.
	AgentSessionConfigVersion *uint64 `json:"agent_session_config_version,omitempty" gorm:"index:idx_chat_agent_runs_session_cfgver_id,priority:2"`

	// BotUserID is the registered bot user whose server-side handler owns the run.
	BotUserID uint64 `json:"bot_user_id" gorm:"not null;index:idx_chat_agent_runs_bot_started,priority:1"`

	// TriggeringUserID is the human actor whose message/event caused the run. Used for
	// approval authority and audit.
	TriggeringUserID *uint64 `json:"triggering_user_id,omitempty" gorm:"index:idx_chat_agent_runs_trigger_user_started,priority:1"`

	// TriggeringMessageID is the user message or system event that triggered the run, when the
	// trigger has a persisted message row.
	TriggeringMessageID *uint64 `json:"triggering_message_id,omitempty" gorm:"index"`

	// Status is the run-level lifecycle state (RUNNING, WAITING_FOR_APPROVAL, RETRYABLE,
	// COMPLETED, FAILED, CANCELLED). Zero (UNSPECIFIED) is never written by go-common.
	Status proto_models.ChatAgentRunStatus `json:"status" gorm:"type:smallint;not null;default:0"`

	// StatusReason qualifies Status with the structured cause go-common could establish. It is
	// written with every status transition: transitions back to RUNNING reset it to UNSPECIFIED,
	// and COMPLETED with UNSPECIFIED is an ordinary completion.
	StatusReason proto_models.ChatAgentRunStatusReason `json:"status_reason" gorm:"type:smallint;not null;default:0"`

	// StatusReasonCode is the optional application-supplied, identifier-like code recorded with a
	// cancellation. Never free text.
	StatusReasonCode *string `json:"status_reason_code,omitempty" gorm:"type:varchar(64)"`

	// ExternalRunID is an optional app-owned run/correlation ID. Not used for FK relationships.
	ExternalRunID *string `json:"external_run_id,omitempty" gorm:"type:varchar(255);index"`

	// Genkit correlation identifiers, optional. These are debugging aids only and are not used
	// for transcript reconstruction.
	GenkitTraceID   *string `json:"genkit_trace_id,omitempty" gorm:"type:varchar(255)"`
	GenkitSessionID *string `json:"genkit_session_id,omitempty" gorm:"type:varchar(255)"`
	GenkitRequestID *string `json:"genkit_request_id,omitempty" gorm:"type:varchar(255)"`

	// MetadataJSON holds small sanitized operational metadata only. Never prompts, full
	// outputs, API keys, or tool bodies.
	MetadataJSON []byte `json:"metadata_json,omitempty" gorm:"type:bytea"`

	// Work-budget ceilings are captured when the run is created so a later deployment increase
	// cannot silently expand an existing run. Zero uses the current service ceilings.
	MaxActiveDurationMs uint64 `json:"max_active_duration_ms" gorm:"not null;default:0"`
	MaxGenerations      uint64 `json:"max_generations" gorm:"not null;default:0"`
	MaxToolCalls        uint64 `json:"max_tool_calls" gorm:"not null;default:0"`

	// Work-budget usage is cumulative across every drive and approval resume.
	ActiveDurationMs uint64 `json:"active_duration_ms" gorm:"not null;default:0"`
	GenerationCount  uint64 `json:"generation_count" gorm:"not null;default:0"`
	ToolCallCount    uint64 `json:"tool_call_count" gorm:"not null;default:0"`

	// StartedAt is when the run was created.
	StartedAt time.Time `json:"started_at" gorm:"type:timestamptz;not null;index:idx_chat_agent_runs_bot_started,priority:2;index:idx_chat_agent_runs_trigger_user_started,priority:2;index:idx_chat_agent_runs_organization_started,priority:2"`

	// CompletedAt is set only when the run reaches COMPLETED/FAILED/CANCELLED.
	CompletedAt *time.Time `json:"completed_at,omitempty" gorm:"type:timestamptz"`

	// DriveLeaseOwner and DriveLeaseExpiresAt fence the one worker allowed to generate and commit
	// the next step for this run. They are deliberately stored on the run row (rather than only in
	// process memory) so separate service replicas cannot both drive the same transcript step.
	DriveLeaseOwner     *string    `json:"drive_lease_owner,omitempty" gorm:"type:varchar(255)"`
	DriveLeaseExpiresAt *time.Time `json:"drive_lease_expires_at,omitempty" gorm:"type:timestamptz"`

	// DriveNotBefore is the durable retry/backoff deadline used by the recovery scanner after an
	// operational drive failure. A nil value is immediately eligible when no live lease exists.
	DriveNotBefore *time.Time `json:"drive_not_before,omitempty" gorm:"type:timestamptz"`

	// DriveRetryReason records why the last drive was released into a backoff. It lives and dies
	// with DriveNotBefore: every release rewrites both, so it never outlives the wait it explains.
	DriveRetryReason proto_models.ChatAgentRunStatusReason `json:"drive_retry_reason" gorm:"type:smallint;not null;default:0"`

	// DriveAbandonCount counts drive attempts abandoned because their call ignored cancellation
	// (shutdown abandonments excluded). Past the configured maximum the run fails.
	DriveAbandonCount uint32 `json:"drive_abandon_count" gorm:"not null;default:0"`

	// LastSealedStepIndex is the newest model step whose complete durable projection was verified
	// under the run-drive lease. Recovery never advances past transcript artifacts that are newer
	// than this marker: they are evidence of a crash in the middle of a model-step commit.
	LastSealedStepIndex *uint32 `json:"last_sealed_step_index,omitempty"`
	// LastSealedStepDisposition classifies the sealed step (continue, tool loop, final, or steered).
	// The persistence package owns the numeric constants so this model stays independent of service
	// code.
	LastSealedStepDisposition int16 `json:"last_sealed_step_disposition" gorm:"type:smallint;not null;default:0"`
	// LastSealedToolCallCount is the exact number of TOOL_CALL rows verified when a tool-loop step
	// was sealed. It is zero for reasoning-only and final steps.
	LastSealedToolCallCount uint32 `json:"last_sealed_tool_call_count" gorm:"not null;default:0"`

	// Stream recovery is bounded per unsealed step and survives worker restarts.
	StreamRecoveryStepIndex uint32 `json:"stream_recovery_step_index" gorm:"not null;default:0"`
	StreamRecoveryCount     uint32 `json:"stream_recovery_count" gorm:"not null;default:0"`

	// ConsumedPendingMessageID is the newest pending wake message represented in a sealed generation.
	ConsumedPendingMessageID uint64 `json:"-" gorm:"not null;default:0"`

	// WokenByAgentRunID is the run whose ending minted this follow-up: the delegated
	// child run whose ending woke the parent, or the session's own previous run whose terminal write
	// drained the session's pending wakes into this follow-up. The raw partial unique index
	// idx_chat_agent_runs_woken_by_session allows at most one wake per destination, whatever path
	// terminalized it. Report wakes leave it nil.
	WokenByAgentRunID *uint64 `json:"woken_by_agent_run_id,omitempty" gorm:"column:woken_by_agent_run_id"`

	// InputDrainedFromRunID is the run whose ending delivered the session's queued human input into
	// this follow-up run. The raw partial unique index idx_chat_agent_runs_input_drained_from_run
	// makes one ending drain at most once. It is separate from WokenByAgentRunID because one ending
	// may be both a child's wake source and, for its own session, a drain source.
	InputDrainedFromRunID *uint64 `json:"input_drained_from_run_id,omitempty" gorm:"column:input_drained_from_run_id"`

	// DelegationNoticeMessageID is the visible broadcast notice a delegated child run posted when
	// it ended without a normal answer. It doubles as the settlement idempotency marker for those
	// endings.
	DelegationNoticeMessageID *uint64 `json:"delegation_notice_message_id,omitempty" gorm:"column:delegation_notice_message_id"`

	// DelegationWakeState records how a delegated child run's ending reached its parent, so a
	// retried settlement neither mints a second run nor re-counts a coalesced wake.
	DelegationWakeState proto_models.DelegationWakeState `json:"delegation_wake_state" gorm:"column:delegation_wake_state;type:smallint;not null;default:0"`

	// CreatedAt timestamp when the run record was created.
	CreatedAt time.Time `json:"created_at" gorm:"type:timestamptz;autoCreateTime"`

	// UpdatedAt timestamp when the run record was last modified.
	UpdatedAt time.Time `json:"updated_at" gorm:"type:timestamptz;autoUpdateTime"`
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (ChatAgentRun) TableName() string {
	return TableNameChatAgentRuns
}
