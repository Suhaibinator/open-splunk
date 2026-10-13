// Package common_persistence_models contains the database models used by the persistence layer.
// This file defines the AI control-plane attempt ledger: the global source of truth for one
// provider attempt of one logical inference invocation. It never stores prompts, completions,
// reasoning, raw provider response bodies, tool arguments/results, credentials, or authorization
// headers; error strings are length-capped (but not content-sanitized — see ErrorMessage) and
// sanitized context is an allowlisted, size-capped JSON object.
// See docs/backend/external/inference_control_plane.md for ledger and privacy contracts.
//
// The composite "(column, created_at DESC)" listing indexes and the GIN jsonb_path_ops index on
// sanitized context cannot be expressed as GORM struct tags (no descending order, no index method);
// they are created as raw SQL in CommonPersistenceHandler.ensureInferenceControlPlaneIndexes.
package common_persistence_models

import (
	"time"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// InferenceAttempt is one attempt row. All token/character/media counters are int64 so provider
// values never overflow and common persistence/API models share one width.
type InferenceAttempt struct {
	EffectiveSettingsJSON []byte                    `json:"effective_settings_json,omitempty" gorm:"type:jsonb"`
	API                   proto_models.InferenceAPI `json:"api" gorm:"type:smallint;not null;default:0"`
	// Read-only attribution projections. GORM ignores these so ledger writes and schema
	// migration never create associations, columns, or foreign keys.
	Actor        *InferenceAttemptActor        `json:"actor,omitempty" gorm:"-"`
	Organization *InferenceAttemptOrganization `json:"organization,omitempty" gorm:"-"`

	// ID is the ledger row identifier.
	ID uint64 `json:"id" gorm:"primaryKey;autoIncrement"`

	// Operation is the owning application's generated enum value.
	Operation int32 `json:"operation" gorm:"type:integer;not null"`

	// InvocationID is the executor-minted correlation key for one logical Execute/ExecuteStream
	// call; AttemptNumber is one-based. Together they are unique, giving exactly-once attempt rows.
	InvocationID  string `json:"invocation_id" gorm:"type:varchar(64);not null;uniqueIndex:idx_inference_attempts_invocation_attempt,priority:1"`
	AttemptNumber int32  `json:"attempt_number" gorm:"type:integer;not null;uniqueIndex:idx_inference_attempts_invocation_attempt,priority:2"`

	// OrganizationID owns the logical inference invocation. Database ownership
	// constraints require a positive existing organization for every persisted row.
	OrganizationID *int64 `json:"organization_id,omitempty"`

	// RuntimePurpose is the optional common runtime purpose (assistant turn, tool loop, compaction).
	RuntimePurpose proto_models.InferenceCallPurpose `json:"runtime_purpose" gorm:"type:smallint;not null;default:0"`

	// InferenceModelID plus ProviderID/ProviderModelID snapshot the resolved model identity.
	InferenceModelID uint64 `json:"inference_model_id" gorm:"not null"`
	ProviderID       string `json:"provider_id" gorm:"type:varchar(128);not null;default:''"`
	ProviderModelID  string `json:"provider_model_id" gorm:"type:varchar(255);not null;default:''"`

	// Status is the lifecycle state of the attempt.
	Status proto_models.InferenceAttemptStatus `json:"status" gorm:"type:smallint;not null;default:0"`

	// Bounded error classification for a failed attempt. ErrorClass is a stable low-cardinality
	// label. ErrorMessage is the provider error text, length-capped by the recorder but not
	// content-sanitized — it can carry provider response fragments, so treat it as admin-only.
	ErrorClass   string                            `json:"error_class" gorm:"type:varchar(128);not null;default:''"`
	ErrorMessage string                            `json:"error_message" gorm:"type:varchar(1024);not null;default:''"`
	Retryable    bool                              `json:"retryable" gorm:"not null;default:false"`
	RetryReason  proto_models.InferenceRetryReason `json:"retry_reason" gorm:"type:smallint;not null;default:0"`

	// RequestTraceID and ActorUserID are read from the trusted request context by the recorder,
	// never supplied by the caller.
	RequestTraceID string  `json:"request_trace_id" gorm:"type:varchar(128);not null;default:'';index:idx_inference_attempts_trace"`
	ActorUserID    *uint64 `json:"actor_user_id,omitempty"`

	// TokenSource classifies how the token counts were obtained.
	TokenSource proto_models.InferenceTokenSource `json:"token_source" gorm:"type:smallint;not null;default:0"`

	// Nullable-as-zero usage counters mirroring ai.GenerationUsage.
	InputTokens     int64 `json:"input_tokens" gorm:"not null;default:0"`
	OutputTokens    int64 `json:"output_tokens" gorm:"not null;default:0"`
	TotalTokens     int64 `json:"total_tokens" gorm:"not null;default:0"`
	CachedTokens    int64 `json:"cached_tokens" gorm:"not null;default:0"`
	ThoughtTokens   int64 `json:"thought_tokens" gorm:"not null;default:0"`
	InputCharacters int64 `json:"input_characters" gorm:"not null;default:0"`
	InputImages     int64 `json:"input_images" gorm:"not null;default:0"`
	InputVideos     int64 `json:"input_videos" gorm:"not null;default:0"`
	InputAudioFiles int64 `json:"input_audio_files" gorm:"not null;default:0"`

	// CustomUsageJSON is the serialized provider-custom usage map (float values preserved).
	CustomUsageJSON []byte `json:"custom_usage_json,omitempty" gorm:"type:bytea"`

	// SanitizedContextJSON is the allowlisted, size-capped application context stored as jsonb so
	// admin metadata filters can use JSON containment (@>) accelerated by a GIN jsonb_path_ops index.
	SanitizedContextJSON []byte `json:"sanitized_context_json,omitempty" gorm:"type:jsonb"`

	// StartedAt/EndedAt bracket the provider attempt; LatencyMs is the derived duration.
	// DeadlineAt is nullable for historical rows and custom observers without a budget.
	DeadlineAt *time.Time `json:"deadline_at,omitempty" gorm:"type:timestamptz;index:idx_inference_attempts_unfinished_deadline,where:status = 3 AND ended_at IS NULL"`
	StartedAt  *time.Time `json:"started_at,omitempty" gorm:"type:timestamptz"`
	EndedAt    *time.Time `json:"ended_at,omitempty" gorm:"type:timestamptz"`
	// FirstOutputAt is absent for buffered, historical, or uninstrumented attempts.
	FirstOutputAt *time.Time `json:"first_output_at,omitempty" gorm:"type:timestamptz"`
	LatencyMs     int64      `json:"latency_ms" gorm:"not null;default:0"`

	// CreatedAt is the row insertion time and the sort key for every listing index.
	CreatedAt time.Time `json:"created_at" gorm:"type:timestamptz;autoCreateTime"`
}

// TableName returns the attempt-ledger table name.
func (InferenceAttempt) TableName() string {
	return TableNameInferenceAttempts
}

// InferenceAttemptActor contains only the current identity fields needed by the admin ledger.
type InferenceAttemptActor struct {
	ID        uint64
	Username  string
	FirstName string
	LastName  string
	IsBot     bool
}

// InferenceAttemptOrganization is a read-only current organization identity.
type InferenceAttemptOrganization struct {
	ID   int64
	Name string
	Slug string
}
