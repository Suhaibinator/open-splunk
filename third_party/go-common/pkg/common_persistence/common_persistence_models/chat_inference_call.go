// Package common_persistence_models contains the database models used by the persistence layer.
// This file defines the inference accounting models: ChatInferenceCall (append-only source-of-truth
// ledger, one row per model/provider invocation) and ChatInferenceTotals (denormalized per-channel
// aggregate cache). Inference accounting is intentionally kept off the hot ChatMessage/ChatChannel
// tables so human-only chats do not pay row-width cost for agent-only token columns.
package common_persistence_models

import (
	"time"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// ChatInferenceCall defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package. Append-only: a row means
// "one model/provider invocation happened, and this is its usage/finish metadata." Editing or
// deleting a chat message never mutates historical usage.
type ChatInferenceCall struct {
	// ID is the stable row identifier (auto-incrementing primary key).
	ID uint64 `json:"id" gorm:"primaryKey;autoIncrement"`

	// OrganizationID identifies the organization charged for this invocation. The column remains
	// protected by ownership constraints requiring a positive existing organization.
	OrganizationID int64 `json:"organization_id"`

	// ChannelID references the channel that owns the agent run/cost.
	ChannelID uint64 `json:"channel_id" gorm:"not null;index:idx_chat_inference_calls_channel_created,priority:1;index:idx_chat_inference_calls_channel_purpose_created,priority:1"`

	// MessageID is the chat message most directly associated with the call (usually the assistant
	// text message or compaction summary). Nullable for failed/pre-message calls.
	MessageID *uint64 `json:"message_id,omitempty"`

	// AgentRunID references ChatAgentRun, grouping all calls produced by one agent run. Required
	// for bot transcript writers; manual recordings may omit a run.
	AgentRunID *uint64 `json:"agent_run_id,omitempty" gorm:"index:idx_chat_inference_calls_run_callidx,priority:1;index:idx_chat_inference_calls_run_step_callidx,priority:1"`

	// AgentSessionID references the first-class agent session for runtime-produced calls. Nullable
	// for manual or non-agent recordings.
	AgentSessionID *uint64 `json:"agent_session_id,omitempty" gorm:"index:idx_chat_inference_calls_session"`

	// AgentSessionConfigVersion is the captured session config version used by the run.
	AgentSessionConfigVersion *uint64 `json:"agent_session_config_version,omitempty"`

	// AgentType copies the registered app-defined agent type for easy usage attribution.
	AgentType string `json:"agent_type" gorm:"type:varchar(128)"`

	// TriggeringUserID is the durable human actor whose message/event caused the run.
	TriggeringUserID *uint64 `json:"triggering_user_id,omitempty"`

	// ActorUserID is the usage actor for accounting/audit. Agent rows prefer TriggeringUserID and
	// fall back to request context when available.
	ActorUserID *uint64 `json:"actor_user_id,omitempty"`

	// AgentStepIndex is the transcript step this invocation attempted or produced.
	AgentStepIndex *uint32 `json:"agent_step_index,omitempty" gorm:"index:idx_chat_inference_calls_run_step_callidx,priority:2"`

	// CallID is the idempotency key supplied by go-common/app. The insert path is
	// INSERT ... ON CONFLICT(call_id) DO NOTHING; totals are only incremented on a real insert.
	CallID string `json:"call_id" gorm:"type:varchar(255);not null;uniqueIndex:idx_chat_inference_calls_call_id"`

	// CallIndex is the monotonic model/provider invocation order within agent_run_id (for
	// deterministic replay/debugging). This is not the transcript merge key; agent_step_index is.
	// Public run-ledger pagination orders by row ID; CallIndex remains replay/debug metadata.
	CallIndex uint32 `json:"call_index" gorm:"not null;default:0;index:idx_chat_inference_calls_run_callidx,priority:2;index:idx_chat_inference_calls_run_step_callidx,priority:3"`

	// Purpose records why this model call happened.
	Purpose proto_models.InferenceCallPurpose `json:"purpose" gorm:"type:smallint;not null;default:0;index:idx_chat_inference_calls_channel_purpose_created,priority:2"`

	// ProviderID is the provider selected through go-common's model runtime/catalog.
	ProviderID string `json:"provider_id" gorm:"type:varchar(128);not null"`

	// ProviderModelID is the normalized provider model ID, e.g. gpt-4.1, gemini-2.5-flash.
	ProviderModelID string `json:"provider_model_id" gorm:"type:varchar(255);not null"`

	// InferenceModelID links to go-common's configured inference model row when available.
	InferenceModelID *uint64 `json:"inference_model_id,omitempty"`

	// FinishReason is the Genkit FinishReason; empty if the call failed before a model response.
	FinishReason string `json:"finish_reason" gorm:"type:varchar(64)"`

	// FinishMessage is Genkit/provider detail, length-capped to avoid storing huge payloads.
	FinishMessage *string `json:"finish_message,omitempty" gorm:"type:varchar(2048)"`

	// LatencyMs is the Genkit end-to-end call latency in milliseconds.
	LatencyMs int64 `json:"latency_ms" gorm:"not null;default:0"`

	// Usage counters mirror ai.GenerationUsage. All default to 0 so a missing provider value is
	// not double-counted as anything but zero.
	InputTokens         int64 `json:"input_tokens" gorm:"not null;default:0"`
	OutputTokens        int64 `json:"output_tokens" gorm:"not null;default:0"`
	TotalTokens         int64 `json:"total_tokens" gorm:"not null;default:0"`
	ThoughtsTokens      int64 `json:"thoughts_tokens" gorm:"not null;default:0"`
	CachedContentTokens int64 `json:"cached_content_tokens" gorm:"not null;default:0"`
	InputCharacters     int64 `json:"input_characters" gorm:"not null;default:0"`
	OutputCharacters    int64 `json:"output_characters" gorm:"not null;default:0"`
	InputImages         int64 `json:"input_images" gorm:"not null;default:0"`
	OutputImages        int64 `json:"output_images" gorm:"not null;default:0"`
	InputVideos         int64 `json:"input_videos" gorm:"not null;default:0"`
	OutputVideos        int64 `json:"output_videos" gorm:"not null;default:0"`
	InputAudioFiles     int64 `json:"input_audio_files" gorm:"not null;default:0"`
	OutputAudioFiles    int64 `json:"output_audio_files" gorm:"not null;default:0"`

	// CustomUsageJSON is the serialized GenerationUsage.Custom map (preserve float values).
	CustomUsageJSON []byte `json:"custom_usage_json,omitempty" gorm:"type:bytea"`

	// GenkitSpanID is an optional per-model-call Genkit/trace span correlation ID.
	GenkitSpanID *string `json:"genkit_span_id,omitempty" gorm:"type:varchar(255)"`

	// MetadataJSON holds small sanitized operational accounting metadata only — never prompts,
	// full responses, raw provider payloads, API keys, or tool result bodies.
	MetadataJSON []byte `json:"metadata_json,omitempty" gorm:"type:bytea"`

	// CreatedAt timestamp when the call row was recorded. Append-only rows have no UpdatedAt
	// and are not soft-deleted.
	CreatedAt time.Time `json:"created_at" gorm:"type:timestamptz;autoCreateTime;index:idx_chat_inference_calls_channel_created,priority:2;index:idx_chat_inference_calls_channel_purpose_created,priority:3"`
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (ChatInferenceCall) TableName() string {
	return TableNameChatInferenceCalls
}

// ChatInferenceTotals defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package. A denormalized per-channel
// aggregate cache (not the source of truth) so channel lists / dashboards / cost views do not
// aggregate the full ledger every time. It can always be rebuilt from ChatInferenceCall.
type ChatInferenceTotals struct {
	// ChannelID is the primary key — one totals row per channel.
	ChannelID uint64 `json:"channel_id" gorm:"primaryKey"`

	// CallCount is the number of inserted inference-call rows included in these totals.
	CallCount int64 `json:"call_count" gorm:"not null;default:0"`

	// Sums of the corresponding ledger usage columns.
	InputTokensTotal         int64 `json:"input_tokens_total" gorm:"not null;default:0"`
	OutputTokensTotal        int64 `json:"output_tokens_total" gorm:"not null;default:0"`
	TotalTokensTotal         int64 `json:"total_tokens_total" gorm:"not null;default:0"`
	ThoughtsTokensTotal      int64 `json:"thoughts_tokens_total" gorm:"not null;default:0"`
	CachedContentTokensTotal int64 `json:"cached_content_tokens_total" gorm:"not null;default:0"`
	InputCharactersTotal     int64 `json:"input_characters_total" gorm:"not null;default:0"`
	OutputCharactersTotal    int64 `json:"output_characters_total" gorm:"not null;default:0"`
	InputImagesTotal         int64 `json:"input_images_total" gorm:"not null;default:0"`
	OutputImagesTotal        int64 `json:"output_images_total" gorm:"not null;default:0"`
	InputVideosTotal         int64 `json:"input_videos_total" gorm:"not null;default:0"`
	OutputVideosTotal        int64 `json:"output_videos_total" gorm:"not null;default:0"`
	InputAudioFilesTotal     int64 `json:"input_audio_files_total" gorm:"not null;default:0"`
	OutputAudioFilesTotal    int64 `json:"output_audio_files_total" gorm:"not null;default:0"`

	// UpdatedAt timestamp of the last aggregate update.
	UpdatedAt time.Time `json:"updated_at" gorm:"type:timestamptz;autoUpdateTime"`
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (ChatInferenceTotals) TableName() string {
	return TableNameChatInferenceTotals
}
