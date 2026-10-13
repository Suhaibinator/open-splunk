// Package common_persistence_models contains the database models used by the persistence layer.
// This file defines the agent-session models: ChatAgentSession (one per channel timeline or thread),
// ChatAgentSessionConfigSnapshot (append-only versioned tool/compaction choices), and
// ChatAgentSessionToolConfig (optional normalized per-tool rows for querying/diffing).
//
// These database models are separate from the public protobuf projections. The local status
// enum is mapped explicitly to proto_models.ChatAgentSessionStatus by the converter.
package common_persistence_models

import (
	"time"

	"github.com/lib/pq"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// ChatAgentSessionStatus is the persistence-local lifecycle state of an agent session.
// Public session projections use the separately defined protobuf status enum.
type ChatAgentSessionStatus int16

const (
	// ChatAgentSessionStatusUnspecified is the zero value. It is never stored: creates write ACTIVE
	// in its place, and a CHECK rejects zero in stored rows.
	ChatAgentSessionStatusUnspecified ChatAgentSessionStatus = 0
	// ChatAgentSessionStatusActive marks a session that can accept new runs.
	ChatAgentSessionStatusActive ChatAgentSessionStatus = 1
	// ChatAgentSessionStatusArchived marks a session retired by the app; history is preserved.
	ChatAgentSessionStatusArchived ChatAgentSessionStatus = 2
)

// ChatAgentSession defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package. One row per agent-enabled
// chat location: a main channel timeline or a thread root.
type ChatAgentSession struct {
	// Execution identity is durable across every run, recovery, and approval resume.
	ChatAgentSessionInferencePin `gorm:"embedded"`
	InferenceBlockedReason       proto_models.InferenceBlockReason `json:"inference_blocked_reason" gorm:"type:smallint;not null;default:0"`
	InferenceBlockedAt           *time.Time                        `json:"inference_blocked_at,omitempty" gorm:"type:timestamptz"`

	// EffectiveSystemPrompt is finalized in the creation transaction and never updated afterward.
	// These private fields must not be copied into public service or API models.
	EffectiveSystemPrompt   string `json:"-" gorm:"type:text;not null"`
	PromptCustomizationJSON []byte `json:"-" gorm:"type:bytea"`

	// ID is the unique identifier for the session (auto-incrementing primary key).
	ID uint64 `json:"id" gorm:"primaryKey;autoIncrement"`

	// OrganizationID identifies the organization that owns this session and every run/inference
	// call derived from it. Database ownership constraints require a positive existing organization.
	OrganizationID int64 `json:"organization_id" gorm:"index:idx_chat_agent_sessions_organization"`

	// ChannelID and ParentMessageID identify the immutable session location.
	ChannelID uint64 `json:"channel_id" gorm:"not null;uniqueIndex:idx_chat_agent_sessions_location"`
	// ParentMessageID is zero for the main timeline, otherwise the thread root.
	ParentMessageID uint64 `json:"parent_message_id" gorm:"not null;default:0;uniqueIndex:idx_chat_agent_sessions_location"`

	// AgentType is the stable app-defined agent type string. Resolved against the server-side
	// AgentRegistry at runtime; it is not a database FK.
	AgentType string `json:"agent_type" gorm:"type:varchar(128);not null;index:idx_chat_agent_sessions_agent_type"`

	// BotUserID is the bot user that appears as the chat participant/sender, derived from the
	// registered agent definition.
	BotUserID uint64 `json:"bot_user_id" gorm:"not null"`

	// UserID is the effective chat user the session is for (the CreateAgentChatSessionInput
	// UserID, or ActorUserID when that is nil). Part of the scoped GetOrCreateByContext key.
	// Distinct from CreatedByUserID, which records the actor that performed the operation.
	UserID uint64 `json:"user_id" gorm:"not null;index:idx_chat_agent_sessions_user"`

	// CreatedByUserID references the actor that created the session (for audit).
	CreatedByUserID uint64 `json:"created_by_user_id" gorm:"not null"`

	// InferenceActorUserID is the non-participant human accountable for a workflow-owned session's
	// inference: routing authorizes, revalidates and attributes every run's model calls to this user
	// instead of the run's triggering user, while participation and tools stay with the bot. Fixed
	// at creation; nil uses each run's triggering user.
	InferenceActorUserID *uint64 `json:"inference_actor_user_id,omitempty" gorm:"column:inference_actor_user_id"`

	// CreateIdempotencyKey is an optional create-scoped idempotency key for session creates that
	// include an initial message/embeds. It is unique per organization/creator/effective-user/
	// agent-type when present; raw SQL adds the partial unique index because GORM cannot express
	// the predicate.
	CreateIdempotencyKey *string `json:"create_idempotency_key,omitempty" gorm:"column:create_idempotency_key;type:varchar(255)"`

	// CreateIdempotencyFingerprint is the SHA-256 hex digest of the normalized create inputs that
	// affect persisted state. Replays must match exactly or they are rejected as conflicts.
	CreateIdempotencyFingerprint *string `json:"create_idempotency_fingerprint,omitempty" gorm:"column:create_idempotency_fingerprint;type:char(64)"`

	// InitialMessageID / InitialRunID point at the transactionally-created initial user message and
	// first agent run for idempotent creates. They are nil for sessions created without initial
	// content.
	InitialMessageID *uint64 `json:"initial_message_id,omitempty" gorm:"column:initial_message_id"`
	InitialRunID     *uint64 `json:"initial_run_id,omitempty" gorm:"column:initial_run_id"`

	// ActiveConfigVersion points at the current ChatAgentSessionConfigSnapshot version. New runs
	// capture this value; updates bump it via an atomic compare-and-swap.
	ActiveConfigVersion uint64 `json:"active_config_version" gorm:"not null;default:1"`

	// Status is the session lifecycle state. The column default is ACTIVE, so an insert that leaves
	// it unset creates a live session.
	Status ChatAgentSessionStatus `json:"status" gorm:"type:smallint;not null;default:1"`

	// AutoRunPolicy controls whether eligible human messages automatically mint follow-up runs. The
	// zero/default value preserves the historical automatic behavior for existing rows.
	AutoRunPolicy ChatAgentSessionAutoRunPolicy `json:"auto_run_policy" gorm:"type:smallint;not null;default:0"`

	// IsDefaultScoped marks the one reusable GetOrCreateByContext session for the scoped
	// (organization_id, agent_type, user_id, context_id, context_entity_id) key. Ordinary creates may still carry
	// ContextID / ContextEntityID as navigation metadata, but they do not participate in scoped
	// reuse or uniqueness.
	IsDefaultScoped bool `json:"is_default_scoped" gorm:"not null;default:false"`

	// ContextID / ContextEntityID copy the app navigation context onto the session and form part
	// of the scoped GetOrCreateByContext uniqueness key only when IsDefaultScoped is true. The
	// partial-unique index requires both to be non-NULL and is added via raw SQL.
	ContextID       *uint64 `json:"context_id,omitempty"`
	ContextEntityID *uint64 `json:"context_entity_id,omitempty"`

	// MetadataJSON holds small sanitized app-owned launch metadata. It is for routing/prompt
	// context only; never prompts, raw outputs, API keys, or large payloads.
	MetadataJSON []byte `json:"metadata_json,omitempty" gorm:"type:bytea"`

	// ParentAgentSessionID links a delegated child session (always a thread session) to the
	// main-timeline session that spawned it. It is immutable and nil for every other session. The
	// partial index idx_chat_agent_sessions_parent_id is created in raw SQL because GORM tags cannot
	// express the IS NOT NULL predicate.
	ParentAgentSessionID *uint64 `json:"parent_agent_session_id,omitempty" gorm:"column:parent_agent_session_id"`

	// SpawnedByAgentRunID is the parent run whose spawn tool call created this child. Its
	// TriggeringUserID is the human on whose behalf the delegation happened, which is why the
	// parent wake runs borrow it.
	SpawnedByAgentRunID *uint64 `json:"spawned_by_agent_run_id,omitempty" gorm:"column:spawned_by_agent_run_id"`

	// CreationOrigin records which path created a session: APP or AGENT for a delegated child the
	// application or the parent agent created, FORK for any session a channel fork cloned
	// (top-level or child). UNSPECIFIED on every other session.
	CreationOrigin proto_models.DelegationCreationOrigin `json:"creation_origin" gorm:"column:creation_origin;type:smallint;not null;default:0"`
	// WakePolicy is when this delegated child's ending or report wakes its parent. UNSPECIFIED (agent
	// spawns) means IMMEDIATE.
	WakePolicy proto_models.WakePolicy `json:"wake_policy" gorm:"column:wake_policy;type:smallint;not null;default:0"`
	// WakeGroup names the batch of siblings a WITH_SIBLINGS child waits for. Nil for every other
	// policy. A group is created by one batch and is joined later only by replacements.
	WakeGroup *string `json:"wake_group,omitempty" gorm:"column:wake_group;type:text"`
	// ReplacesChildSessionID is the archived sibling this child replaced in its wake group. Following
	// it backwards from a batch key's child finds the key's current descendant.
	ReplacesChildSessionID *uint64 `json:"replaces_child_session_id,omitempty" gorm:"column:replaces_child_session_id"`
	// DeferredWakeGroups, on a delegation parent, names its WAKE_POLICY_WITH_SIBLINGS groups with a
	// deferred member ending the group has not yet delivered. Written only under this row's lock:
	// a deferral adds the group, and the group's release (its last ending, or the archive of its
	// last working member) removes it.
	DeferredWakeGroups pq.StringArray `json:"deferred_wake_groups,omitempty" gorm:"column:deferred_wake_groups;type:text[]"`

	// ResumeRevision counts the external events (approval resolutions, inference retargets,
	// application resumes) that may make an owner-driven graph rooted here drivable again. Only
	// roots advance it.
	ResumeRevision uint64 `json:"resume_revision" gorm:"column:resume_revision;not null;default:0"`
	// ParkedRevision is the ResumeRevision an application parked this root's graph at, or nil when
	// the graph is not parked. A root with ResumeRevision > ParkedRevision has missed a resume.
	ParkedRevision *uint64 `json:"parked_revision,omitempty" gorm:"column:parked_revision"`

	// PendingWakeMessageID is the newest wake deferred because this session already had an active
	// run. On a main-timeline delegation parent it is a child's closing message, report, or notice;
	// on a delegated child it is the newest orchestrator "continue" message. The terminal write of
	// the active run drains it into one follow-up run (or, for a child, discards it with a receipt
	// reason). PendingWakeCount counts the wakes that single follow-up stands for.
	PendingWakeMessageID *uint64 `json:"pending_wake_message_id,omitempty" gorm:"column:pending_wake_message_id"`
	PendingWakeCount     uint32  `json:"pending_wake_count" gorm:"column:pending_wake_count;not null;default:0"`

	// SuspendedAt, SuspendedByUserID and SuspendedRunID record a person's Stop: when it happened, who
	// stopped the session, and the run they stopped. While set, no run starts automatically and new
	// input is held until Resume clears all three. The raw CHECK
	// chk_chat_agent_sessions_suspension_complete keeps them all set or all nil.
	SuspendedAt       *time.Time `json:"suspended_at,omitempty" gorm:"column:suspended_at;type:timestamptz"`
	SuspendedByUserID *uint64    `json:"suspended_by_user_id,omitempty" gorm:"column:suspended_by_user_id"`
	SuspendedRunID    *uint64    `json:"suspended_run_id,omitempty" gorm:"column:suspended_run_id"`

	// CreatedAt timestamp when the session was created.
	CreatedAt time.Time `json:"created_at" gorm:"type:timestamptz;autoCreateTime"`

	// UpdatedAt timestamp when the session record was last modified.
	UpdatedAt time.Time `json:"updated_at" gorm:"type:timestamptz;autoUpdateTime"`
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (ChatAgentSession) TableName() string {
	return TableNameChatAgentSessions
}

// IsDelegatedChild reports whether a delegation parent spawned this session. It is nil-safe.
func (s *ChatAgentSession) IsDelegatedChild() bool {
	return s != nil && s.ParentAgentSessionID != nil && *s.ParentAgentSessionID != 0
}

// GraphRootID is the root session of the delegation graph the session belongs to: its parent for
// a delegated child (nesting is one level deep), otherwise the session itself.
func (s *ChatAgentSession) GraphRootID() uint64 {
	if s.IsDelegatedChild() {
		return *s.ParentAgentSessionID
	}
	return s.ID
}

// IsMainTimeline reports whether the session is its channel's main-timeline session (no thread
// root, not a delegated child). It is nil-safe.
func (s *ChatAgentSession) IsMainTimeline() bool {
	return s != nil && s.ParentMessageID == 0 && !s.IsDelegatedChild()
}

// IsLive reports whether the session is ACTIVE and inference is not blocked. Every other status, including the zero value and
// values this build does not know, is not live, so unknown state never admits new runs. It is
// nil-safe.
func (s *ChatAgentSession) IsLive() bool {
	return s != nil && s.Status == ChatAgentSessionStatusActive && !s.IsInferenceBlocked()
}

// IsSuspended reports whether a person's Stop left the session suspended. It is nil-safe.
func (s *ChatAgentSession) IsSuspended() bool {
	return s != nil && s.SuspendedAt != nil
}

// CanDelegate reports whether the session's location lets its agent spawn delegated children: a
// live main-timeline session. Whether the agent type delegates is the registry's question.
func (s *ChatAgentSession) CanDelegate() bool {
	return s.IsMainTimeline() && s.IsLive()
}

// ChatAgentSessionConfigSnapshot defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package. Append-only effective
// session-choice snapshots: enabled tools, per-tool approval overrides, and compaction rules.
// Model policy and tool executors/schemas are resolved from the live registry by agent_type /
// tool_name. System instructions are finalized once on ChatAgentSession.EffectiveSystemPrompt;
// they are neither stored in this snapshot nor rebuilt from the live registry for later runs.
type ChatAgentSessionConfigSnapshot struct {
	// ID is the unique identifier for the snapshot (auto-incrementing primary key).
	ID uint64 `json:"id" gorm:"primaryKey;autoIncrement"`

	// AgentSessionID references the owning session. The composite unique index on
	// (agent_session_id, version) enforces the append-only one-row-per-version invariant.
	AgentSessionID uint64 `json:"agent_session_id" gorm:"not null;uniqueIndex:idx_chat_agent_session_cfg_snap_version,priority:1"`

	// Version is the monotonically increasing config version for this session.
	Version uint64 `json:"version" gorm:"not null;uniqueIndex:idx_chat_agent_session_cfg_snap_version,priority:2"`

	// ToolConfigPayload is the serialized effective tool config for this version (enabled tools +
	// per-tool approval overrides + renderer keys).
	ToolConfigPayload []byte `json:"tool_config_payload,omitempty" gorm:"type:bytea"`

	// CompactionPayload is the serialized effective compaction rules for this version.
	CompactionPayload []byte `json:"compaction_payload,omitempty" gorm:"type:bytea"`

	// DelegationOverridePayload is the serialized delegation override that narrows a delegating
	// agent's registered delegation for runs capturing this version; nil when there is none.
	DelegationOverridePayload []byte `json:"delegation_override_payload,omitempty" gorm:"type:bytea"`

	// ChatAgentInteractionPolicy holds the effective input mode and Stop opt-in for this version,
	// resolved when the snapshot is written; readers never fall back to the agent default.
	ChatAgentInteractionPolicy `gorm:"embedded"`

	// CreatedByUserID references the actor that created this config version (for audit).
	CreatedByUserID uint64 `json:"created_by_user_id" gorm:"not null"`

	// CreatedAt timestamp when the snapshot was written.
	CreatedAt time.Time `json:"created_at" gorm:"type:timestamptz;autoCreateTime"`
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (ChatAgentSessionConfigSnapshot) TableName() string {
	return TableNameChatAgentSessionConfigSnapshots
}

// ChatAgentInteractionPolicy is the effective interaction policy a config snapshot captures. Its two
// typed columns let the send path read the policy of one version without the snapshot payloads.
// InputMode is never UNSPECIFIED: the raw CHECK chk_chat_agent_cfg_snapshots_input_mode_specified
// rejects it, and the column default backfilled rows written before the policy existed.
type ChatAgentInteractionPolicy struct {
	InputMode     proto_models.ChatAgentInputMode `json:"input_mode" gorm:"column:input_mode;type:smallint;not null;default:1"`
	AllowUserStop bool                            `json:"allow_user_stop" gorm:"column:allow_user_stop;not null;default:false"`
}

// ChatAgentSessionToolConfig defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package. Optional normalized rows for
// querying/diffing the enabled tools of a config version; the authoritative source is the
// snapshot payload.
type ChatAgentSessionToolConfig struct {
	// ID is the unique identifier for the row (auto-incrementing primary key).
	ID uint64 `json:"id" gorm:"primaryKey;autoIncrement"`

	// AgentSessionID references the owning session.
	AgentSessionID uint64 `json:"agent_session_id" gorm:"not null;index:idx_chat_agent_session_tool_cfg,priority:1"`

	// Version is the config snapshot version these tool rows belong to.
	Version uint64 `json:"version" gorm:"not null;index:idx_chat_agent_session_tool_cfg,priority:2"`

	// ToolName is the stable app-defined tool name.
	ToolName string `json:"tool_name" gorm:"type:varchar(128);not null"`

	// Enabled indicates whether the tool is offered to the model in this version.
	Enabled bool `json:"enabled" gorm:"not null;default:false"`

	// RequiresApproval indicates whether calls to this tool require explicit user approval.
	RequiresApproval bool `json:"requires_approval" gorm:"not null;default:false"`

	// RendererKey is optional frontend metadata for selecting an app-specific renderer.
	RendererKey *string `json:"renderer_key,omitempty" gorm:"type:varchar(128)"`

	// CreatedAt timestamp when the row was written.
	CreatedAt time.Time `json:"created_at" gorm:"type:timestamptz;autoCreateTime"`
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (ChatAgentSessionToolConfig) TableName() string {
	return TableNameChatAgentSessionToolConfigs
}
