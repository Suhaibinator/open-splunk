package common_service_models

import (
	json "encoding/json/v2"
	"time"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// Agent-note subtypes identify durable runtime control records independently of display text.
const (
	AgentNoteSubtypeNote           = "note"
	AgentNoteSubtypeReasoningNudge = "reasoning_only_nudge"
	AgentNoteSubtypeEmptyTurnNudge = "empty_turn_nudge"
	// AgentNoteSubtypeAppResume is an application's message to a delegation parent, written by
	// ResumeParent. It renders as a message from the application.
	AgentNoteSubtypeAppResume = "app_resume"
)

// DecodeAgentSessionMetadata decodes persisted launch metadata into a newly owned map.
// Empty objects and JSON null return nil; malformed data is reported to the caller.
func DecodeAgentSessionMetadata(raw []byte) (map[string]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var metadata map[string]string
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return nil, err
	}
	if len(metadata) == 0 {
		return nil, nil
	}
	return metadata, nil
}

// ChatMessageToolCall is the service-layer view of a transcript message's structured tool-call
// payload plus its approval and execution state. It mirrors the persistence ChatMessageToolCall 1:1
// side row (the columns split off ChatMessage). The converter flattens these fields onto the proto
// Message wire shape; the wire contract is unchanged.
type ChatMessageToolCall struct {
	ToolCallID   *string                              `json:"tool_call_id,omitempty"`
	ModelPayload []byte                               `json:"model_payload,omitempty"`
	PayloadType  proto_models.MessageModelPayloadType `json:"payload_type"`

	// Approval columns (authoritative, queryable).
	ApprovalStatus    proto_models.ToolCallApprovalStatus `json:"approval_status"`
	TriggeringUserID  *uint64                             `json:"triggering_user_id,omitempty"`
	ResolvedByUserID  *uint64                             `json:"resolved_by_user_id,omitempty"`
	ResolvedAt        *time.Time                          `json:"resolved_at,omitempty"`
	ResolutionReason  *string                             `json:"resolution_reason,omitempty"`
	ApprovalExpiresAt *time.Time                          `json:"approval_expires_at,omitempty"`

	// Execution columns (operational state).
	ExecutionState          proto_models.ToolCallExecutionState `json:"execution_state"`
	ExecutionAttemptCount   uint32                              `json:"execution_attempt_count"`
	ExecutionLeaseOwner     *string                             `json:"execution_lease_owner,omitempty"`
	ExecutionLeaseExpiresAt *time.Time                          `json:"execution_lease_expires_at,omitempty"`
	ExecutionStartedAt      *time.Time                          `json:"execution_started_at,omitempty"`
	ExecutedAt              *time.Time                          `json:"executed_at,omitempty"`
	LastExecutionError      *string                             `json:"last_execution_error,omitempty"`
}

// ChatAgentSessionStatus mirrors the persistence-local session lifecycle enum at the service
// layer. It is not a proto type; agent-session lifecycle/config is a Go service contract.
// The numbering is the persistence enum's: compare against the
// common_persistence_models.ChatAgentSessionStatus* values, which are the durable ones.
type ChatAgentSessionStatus int16

const (
	// ChatAgentSessionStatusActive is common_persistence_models.ChatAgentSessionStatusActive.
	ChatAgentSessionStatusActive ChatAgentSessionStatus = 1
	// ChatAgentSessionStatusArchived is common_persistence_models.ChatAgentSessionStatusArchived.
	ChatAgentSessionStatusArchived ChatAgentSessionStatus = 2
)

// ChatAgentSession is the service-layer view of a channel-bound agent chat session. It is returned
// by the agent-session service methods; it is not exposed as a public proto/HTTP type in v1.
type ChatAgentSession struct {
	// Execution identity is pinned independently of the session's tool/config version.
	// Optional values remain absent for legacy sessions that have no inference pin.
	InferenceOperation        int32                                 `json:"inference_operation"`
	ExecutionInferenceModelID *uint64                               `json:"execution_inference_model_id,omitempty"`
	ExecutionInferenceAPI     *proto_models.InferenceAPI            `json:"execution_inference_api,omitempty"`
	ExecutionProviderID       *string                               `json:"execution_provider_id,omitempty"`
	ExecutionProviderModelID  *string                               `json:"execution_provider_model_id,omitempty"`
	SelectionSource           proto_models.InferenceSelectionSource `json:"selection_source"`
	SelectingUserID           *uint64                               `json:"selecting_user_id,omitempty"`
	// MemberChoiceSelection retains member-choice attribution across owner retargets.
	MemberChoiceSelection bool `json:"member_choice_selection"`
	// SiteOverrideUserID retains the privileged selector across later retargets.
	SiteOverrideUserID                 *uint64 `json:"site_override_user_id,omitempty"`
	OperationListVersion               uint64  `json:"operation_list_version"`
	OrganizationPolicyVersion          uint64  `json:"organization_policy_version"`
	OrganizationOperationPolicyVersion uint64  `json:"organization_operation_policy_version"`
	InferencePinVersion                uint64  `json:"inference_pin_version"`
	// A dispatch block preserves the session and its pin for explicit retargeting.
	InferenceBlockedReason proto_models.InferenceBlockReason `json:"inference_blocked_reason"`
	InferenceBlockedAt     *time.Time                        `json:"inference_blocked_at,omitempty"`

	ID              uint64             `json:"id"`
	OrganizationID  OrganizationIdType `json:"organization_id"`
	ChannelID       uint64             `json:"channel_id"`
	ParentMessageID uint64             `json:"parent_message_id"` // Immutable thread root; 0 is the main timeline.
	AgentType       string             `json:"agent_type"`
	BotUserID       uint64             `json:"bot_user_id"`
	UserID          uint64             `json:"user_id"`
	CreatedByUserID uint64             `json:"created_by_user_id"`
	// InferenceActorUserID is the workflow session's accountable inference human; nil uses each
	// run's triggering user.
	InferenceActorUserID *uint64                       `json:"inference_actor_user_id,omitempty"`
	ActiveConfigVersion  uint64                        `json:"active_config_version"`
	Status               ChatAgentSessionStatus        `json:"status"`
	AutoRunPolicy        ChatAgentSessionAutoRunPolicy `json:"auto_run_policy"`
	ContextID            *uint64                       `json:"context_id,omitempty"`
	ContextEntityID      *uint64                       `json:"context_entity_id,omitempty"`
	Metadata             map[string]string             `json:"metadata,omitempty"`
	// ParentAgentSessionID links a delegated child session to the main-timeline session that
	// spawned it; nil for every other session.
	ParentAgentSessionID *uint64 `json:"parent_agent_session_id,omitempty"`
	// SpawnedByAgentRunID is the parent run whose spawn tool call created this child.
	SpawnedByAgentRunID *uint64   `json:"spawned_by_agent_run_id,omitempty"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

// ChatAgentRun is the service-layer view of one logical agent turn/resume chain. Its ID is the
// durable run identifier attached to every transcript and inference row the run produces.
type ChatAgentRun struct {
	RequestedInferenceModelID *uint64                    `json:"requested_inference_model_id,omitempty"`
	RequestedInferenceAPI     *proto_models.InferenceAPI `json:"requested_inference_api,omitempty"`
	ExecutionInferenceModelID *uint64                    `json:"execution_inference_model_id,omitempty"`
	ExecutionInferenceAPI     *proto_models.InferenceAPI `json:"execution_inference_api,omitempty"`
	ExecutionProviderID       *string                    `json:"execution_provider_id,omitempty"`
	ExecutionProviderModelID  *string                    `json:"execution_provider_model_id,omitempty"`

	ID                        uint64                          `json:"id"`
	OrganizationID            OrganizationIdType              `json:"organization_id"`
	ChannelID                 uint64                          `json:"channel_id"`
	ParentMessageID           uint64                          `json:"parent_message_id"`
	AgentSessionID            *uint64                         `json:"agent_session_id,omitempty"`
	AgentSessionConfigVersion *uint64                         `json:"agent_session_config_version,omitempty"`
	BotUserID                 uint64                          `json:"bot_user_id"`
	TriggeringUserID          *uint64                         `json:"triggering_user_id,omitempty"`
	TriggeringMessageID       *uint64                         `json:"triggering_message_id,omitempty"`
	Status                    proto_models.ChatAgentRunStatus `json:"status"`
	// StatusReason and StatusReasonCode qualify Status (for example MODEL_STEP_LIMIT, or the
	// delegation_cancelled_by_parent code of a cancelled delegated child).
	StatusReason        proto_models.ChatAgentRunStatusReason `json:"status_reason,omitempty"`
	StatusReasonCode    string                                `json:"status_reason_code,omitempty"`
	ExternalRunID       *string                               `json:"external_run_id,omitempty"`
	MaxActiveDurationMs uint64                                `json:"max_active_duration_ms"`
	MaxGenerations      uint64                                `json:"max_generations"`
	MaxToolCalls        uint64                                `json:"max_tool_calls"`
	ActiveDurationMs    uint64                                `json:"active_duration_ms"`
	GenerationCount     uint64                                `json:"generation_count"`
	ToolCallCount       uint64                                `json:"tool_call_count"`
	StartedAt           time.Time                             `json:"started_at"`
	CompletedAt         *time.Time                            `json:"completed_at,omitempty"`
	CreatedAt           time.Time                             `json:"created_at"`
	UpdatedAt           time.Time                             `json:"updated_at"`
}

// ChatAgentQueuedInput is the service-layer view of a person's input waiting in an agent session's
// queue: it was sent while the session was busy or suspended, and becomes a chat message only when
// the agent receives it. ViewerCanEdit and ViewerCanRemove are decided for the person reading it.
type ChatAgentQueuedInput struct {
	ID               uint64                             `json:"id"`
	AgentSessionID   uint64                             `json:"agent_session_id"`
	ChannelID        uint64                             `json:"channel_id"`
	ParentMessageID  uint64                             `json:"parent_message_id"` // The session's location; 0 is the main timeline.
	SenderID         UserIdType                         `json:"sender_id"`
	Sender           *User                              `json:"sender,omitempty"`
	Content          string                             `json:"content"`
	ContentType      proto_models.MessageContentType    `json:"content_type"`
	ReplyToMessageID *uint64                            `json:"reply_to_message_id,omitempty"`
	AttachmentIDs    []uint64                           `json:"attachment_ids,omitempty"`
	Attachments      []*proto_models.UploadedAttachment `json:"attachments,omitempty"`
	EmbedCount       int                                `json:"embed_count"` // Kept by every edit.
	CreatedAt        time.Time                          `json:"created_at"`
	UpdatedAt        time.Time                          `json:"updated_at"`
	ViewerCanEdit    bool                               `json:"viewer_can_edit"`
	ViewerCanRemove  bool                               `json:"viewer_can_remove"`
}
