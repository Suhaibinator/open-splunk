package common_service_models

import "github.com/Suhaibinator/go-common/pkg/proto_models"

const (
	// OpenAIResponsesOutputItemsMetadataKey is the Genkit message metadata key used by the
	// Responses adapter to carry the exact response.output array needed for stateless replay.
	OpenAIResponsesOutputItemsMetadataKey = "openai.responses.output_items_json"
	// OpenAIResponsesResponseIDMetadataKey carries the provider response identifier for tracing.
	OpenAIResponsesResponseIDMetadataKey = "openai.responses.response_id"
	// OpenAIResponsesToolManifestSHA256MetadataKey optionally carries the adapter's manifest guard.
	OpenAIResponsesToolManifestSHA256MetadataKey = "openai.responses.tool_manifest_sha256"
	// OpenAIResponsesAttachmentIDMetadataKey carries an already-externalized internal attachment.
	OpenAIResponsesAttachmentIDMetadataKey = "openai.responses.provider_state_attachment_id"
	// OpenAIResponsesReplayVerifiedMetadataKey is set only after durable replay state passes all
	// attachment, projection, and tool-manifest integrity checks during reconstruction.
	OpenAIResponsesReplayVerifiedMetadataKey = "openai.responses.replay_verified"
	// OpenAIResponsesStateFormat identifies the opaque attachment encoding stored by go-common.
	OpenAIResponsesStateFormat = "openai.responses.output_items"
	// OpenAIResponsesStateVersion is the current provider-state envelope version.
	OpenAIResponsesStateVersion uint32 = 1
)

// openAIResponsesReplayAttestation is an in-process capability proving that opaque Responses
// output items passed the durable transcript's projection, ownership, size, digest, and manifest
// checks. The concrete type and singleton are intentionally private: JSON-decoded model requests
// can reproduce metadata keys and scalar values, but cannot manufacture this identity.
type openAIResponsesReplayAttestation struct{}

var durableOpenAIResponsesReplayAttestation = &openAIResponsesReplayAttestation{}

// NewOpenAIResponsesReplayAttestation returns the process-local capability used by trusted
// transcript reconstruction. It must never be serialized as durable provider state.
func NewOpenAIResponsesReplayAttestation() any {
	return durableOpenAIResponsesReplayAttestation
}

// IsOpenAIResponsesReplayAttested reports whether value is the process-local capability issued
// by NewOpenAIResponsesReplayAttestation. A bool or JSON object with the same metadata key is not
// an attestation.
func IsOpenAIResponsesReplayAttested(value any) bool {
	return value == durableOpenAIResponsesReplayAttestation
}

// ProviderState is the service-layer form of opaque model-provider continuation state. Data is
// present only between generation and the transcript writer; the writer synchronously stores it
// as an internal attachment and persists only the reference/integrity envelope below. Reads load
// Data through the privileged provider-state resolver, never through public attachment hydration.
type ProviderState struct {
	API                     proto_models.InferenceAPI `json:"api"`
	ProviderModelID         string                    `json:"provider_model_id,omitempty"`
	DestinationFingerprint  string                    `json:"destination_fingerprint,omitempty"`
	ProviderID              string                    `json:"provider_id"`
	Format                  string                    `json:"format"`
	Version                 uint32                    `json:"version"`
	ResponseID              string                    `json:"response_id,omitempty"`
	Data                    []byte                    `json:"-"`
	AttachmentID            uint64                    `json:"attachment_id,omitempty"`
	ContentSHA256           string                    `json:"content_sha256,omitempty"`
	SizeBytes               uint64                    `json:"size_bytes,omitempty"`
	VisibleProjectionSHA256 string                    `json:"visible_projection_sha256,omitempty"`
	ToolManifestSHA256      string                    `json:"tool_manifest_sha256,omitempty"`
}

// Clone returns an independent provider-state value, including a copy of the opaque bytes.
func (s *ProviderState) Clone() *ProviderState {
	if s == nil {
		return nil
	}
	out := *s
	out.Data = append([]byte(nil), s.Data...)
	return &out
}
