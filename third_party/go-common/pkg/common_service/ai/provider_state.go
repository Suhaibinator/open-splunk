package ai

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"fmt"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"math"
	"strconv"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/common_json"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/genai/modelconfig"
	"github.com/Suhaibinator/go-common/pkg/genai/partmetadata"
	"github.com/Suhaibinator/go-common/pkg/genai/toolrejection"
	genkitai "github.com/firebase/genkit/go/ai"
)

const openAIResponsesProjectionSHA256MetadataKey = "openai.responses.projection_sha256"

// ErrProviderStateIntegrity means a provider response carried opaque replay state that could not
// be proven to describe the response's current visible content. It unwraps to
// ErrProviderStateCommitFailed so the runtime terminalizes the run instead of retrying a response
// whose provenance was lost at the model/middleware boundary.
var ErrProviderStateIntegrity = fmt.Errorf("%w: provider response integrity check failed", ErrProviderStateCommitFailed)

type assistantProjectionPart struct {
	Kind     string                              `json:"kind"`
	Text     string                              `json:"text,omitempty"`
	Name     string                              `json:"name,omitempty"`
	Ref      string                              `json:"ref,omitempty"`
	Input    jsontext.Value                      `json:"input,omitempty"`
	Rejected *proto_models.RejectedToolArguments `json:"rejected,omitempty"`
}

// CanonicalAssistantProjectionSHA256 fingerprints the cross-run-stable assistant projection in
// the exact shape persisted by go-common: every visible text/data part is concatenated into one
// leading STANDARD-message text value, followed by function requests in their relative order.
// The durable transcript does not retain original text part boundaries or their interleaving with
// tool requests, so hashing those transient distinctions would reject valid replay after the first
// persistence round trip. The provider adapter's separate, exact content hash still protects the
// live response against post-model mutation before this durable projection is derived.
//
// Reasoning is deliberately excluded because AGENT_NOTE reasoning is scoped to the active run and
// disappears from later user turns; including it would make otherwise valid durable replay fail
// its guard. ProviderID replay state is used only when this projection still matches the transcript.
func CanonicalAssistantProjectionSHA256(message *genkitai.Message) (string, error) {
	var visibleText strings.Builder
	toolProjection := make([]assistantProjectionPart, 0)
	if message != nil {
		for _, part := range message.Content {
			if part == nil {
				continue
			}
			switch {
			case toolrejection.FromPart(part) != nil:
				rejected := *toolrejection.FromPart(part)
				if ref, known := partmetadata.FunctionCallID(part.Metadata); known {
					rejected.CallId = ref
				}
				toolProjection = append(toolProjection, assistantProjectionPart{Kind: "rejected_tool_request", Rejected: &rejected})
			case part.IsReasoning():
				continue
			case part.IsText() || part.IsData():
				text, err := assistantVisiblePartText(part)
				if err != nil {
					return "", fmt.Errorf("canonicalize assistant data part: %w", err)
				}
				visibleText.WriteString(text)
			case part.IsToolRequest() && part.ToolRequest != nil:
				input, err := canonicalJSON(part.ToolRequest.Input)
				if err != nil {
					return "", fmt.Errorf("canonicalize tool request %q: %w", part.ToolRequest.Name, err)
				}
				ref := part.ToolRequest.Ref
				if providerRef, known := partmetadata.FunctionCallID(part.Metadata); known {
					ref = providerRef
				}
				toolProjection = append(toolProjection, assistantProjectionPart{
					Kind:  "tool_request",
					Name:  part.ToolRequest.Name,
					Ref:   ref,
					Input: input,
				})
			}
		}
	}
	projection := make([]assistantProjectionPart, 0, len(toolProjection)+1)
	if visibleText.Len() != 0 {
		projection = append(projection, assistantProjectionPart{Kind: "text", Text: visibleText.String()})
	}
	projection = append(projection, toolProjection...)
	return hashCanonicalJSON(projection)
}

type toolManifestEntry struct {
	Binding      string         `json:"binding"`
	Function     string         `json:"function"`
	Description  string         `json:"description,omitempty"`
	InputSchema  jsontext.Value `json:"input_schema,omitempty"`
	OutputSchema jsontext.Value `json:"output_schema,omitempty"`
	Strict       bool           `json:"strict,omitempty"`
}

// CanonicalToolManifestSHA256 fingerprints tool bindings in declaration order. Function names,
// schemas, description, and strictness form the provider-visible specification; executors,
// renderer hints, and approval policy deliberately do not.
func CanonicalToolManifestSHA256(tools []ports.AgentToolDefinition) (string, error) {
	return CanonicalToolManifestWithProviderConfigSHA256(tools, nil)
}

// CanonicalToolManifestWithProviderConfigSHA256 also fingerprints the
// provider-visible tool surface carried outside Genkit definitions: native
// hosted tools and local client-tool bindings/renames. Replaying output items
// across a changed binding could otherwise dispatch a preserved provider name
// to the wrong local tool.
func CanonicalToolManifestWithProviderConfigSHA256(tools []ports.AgentToolDefinition, providerConfig *modelconfig.Config) (string, error) {
	if len(tools) == 0 {
		providerTools, err := providerToolSurface(providerConfig)
		if err != nil || providerTools == nil {
			return "", err
		}
		return hashCanonicalJSON(map[string]any{"provider_tools": providerTools})
	}
	manifest := make([]toolManifestEntry, 0, len(tools))
	for _, tool := range tools {
		entry, err := effectiveToolManifestEntry(tool)
		if err != nil {
			return "", err
		}
		manifest = append(manifest, entry)
	}
	providerTools, err := providerToolSurface(providerConfig)
	if err != nil {
		return "", err
	}
	if providerTools == nil {
		return hashCanonicalJSON(manifest)
	}
	return hashCanonicalJSON(map[string]any{
		"genkit_tools":   manifest,
		"provider_tools": providerTools,
	})
}

// effectiveToolManifestEntry follows genkitToolRefs: native Genkit tools are
// passed through, so their actual definitions are authoritative for the model.
// Execute tools are constructed from the registry's declaration fields.
func effectiveToolManifestEntry(tool ports.AgentToolDefinition) (toolManifestEntry, error) {
	entry := toolManifestEntry{
		Binding: tool.Name, Function: tool.Name, Description: tool.Description, Strict: tool.StrictSchema,
	}
	var err error
	if tool.GenkitTool != nil {
		def := tool.GenkitTool.Definition()
		if def == nil {
			return entry, fmt.Errorf("tool %q has no Genkit definition", tool.Name)
		}
		entry.Function = def.Name
		entry.Description = def.Description
		entry.Strict, _ = def.Metadata["strict"].(bool)
		if def.InputSchema != nil {
			entry.InputSchema, err = canonicalJSON(def.InputSchema)
		}
		if err == nil && def.OutputSchema != nil {
			entry.OutputSchema, err = canonicalJSON(def.OutputSchema)
		}
	} else {
		entry.InputSchema, err = canonicalRawJSON(tool.InputSchemaJSON)
		if err == nil {
			entry.OutputSchema, err = canonicalRawJSON(tool.OutputSchemaJSON)
		}
	}
	if err != nil {
		return entry, fmt.Errorf("canonicalize schemas for tool %q: %w", tool.Name, err)
	}
	return entry, nil
}

func providerToolSurface(config *modelconfig.Config) (map[string]any, error) {
	if config == nil {
		return nil, nil
	}
	// Project only tool fields. Native Google HTTPOptions.Timeout, for example,
	// cannot be encoded by JSON v2 and has no bearing on the tool surface.
	projection := make(map[string]any)
	switch {
	case config.Responses != nil:
		hostedTools, err := modelconfig.EffectiveResponsesHostedTools(config.Responses.Params)
		if err != nil {
			return nil, fmt.Errorf("normalize provider tools: %w", err)
		}
		if len(config.Responses.ClientTools) > 0 {
			projection["client_tools"] = config.Responses.ClientTools
		}
		if len(hostedTools) > 0 {
			projection["hosted_tools"] = hostedTools
		}
	case config.Google != nil:
		if len(config.Google.Tools) > 0 {
			projection["hosted_tools"] = config.Google.Tools
		}
	case config.ChatCompletions != nil:
		if len(config.ChatCompletions.Tools) > 0 {
			projection["hosted_tools"] = config.ChatCompletions.Tools
		}
	case config.Anthropic != nil:
		hostedTools, err := modelconfig.EffectiveAnthropicHostedTools(*config.Anthropic)
		if err != nil {
			return nil, fmt.Errorf("project provider tools: %w", err)
		}
		if len(hostedTools) > 0 {
			projection["hosted_tools"] = hostedTools
		}
	}
	if len(projection) == 0 {
		return nil, nil
	}
	encoded, err := json.Marshal(projection)
	if err != nil {
		return nil, fmt.Errorf("marshal provider tool config: %w", err)
	}
	var surface map[string]any
	if err := common_json.UnmarshalExact(encoded, &surface); err != nil {
		return nil, fmt.Errorf("decode provider tool config: %w", err)
	}
	if len(surface) == 0 {
		return nil, nil
	}
	return surface, nil
}

func hashCanonicalJSON(value any) (string, error) {
	encoded, err := common_json.MarshalCanonical(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func canonicalJSON(value any) (jsontext.Value, error) {
	if value == nil {
		return jsontext.Value("null"), nil
	}
	if raw, ok := value.(jsontext.Value); ok {
		return canonicalRawJSON(raw)
	}
	return common_json.MarshalCanonical(value)
}

func canonicalRawJSON(raw []byte) (jsontext.Value, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	return common_json.Canonicalize(raw)
}

func validateOpenAIProviderStateIntegrity(message *genkitai.Message, requireOutputItems bool) error {
	if message == nil || message.Metadata == nil {
		if requireOutputItems {
			return fmt.Errorf("%w: output-items metadata is missing", ErrProviderStateIntegrity)
		}
		return nil
	}
	outputItems, present := message.Metadata[common_service_models.OpenAIResponsesOutputItemsMetadataKey]
	if !present {
		if requireOutputItems {
			return fmt.Errorf("%w: output-items metadata is missing", ErrProviderStateIntegrity)
		}
		return nil
	}
	if !nonEmptyProviderStateValue(outputItems) {
		return fmt.Errorf("%w: output-items metadata is empty or invalid", ErrProviderStateIntegrity)
	}
	data, ok := providerStateBytes(outputItems)
	var items []jsontext.Value
	if !ok || json.Unmarshal(data, &items) != nil || items == nil {
		return fmt.Errorf("%w: output-items metadata must be a JSON array", ErrProviderStateIntegrity)
	}
	if !common_service_models.IsOpenAIResponsesReplayAttested(
		message.Metadata[common_service_models.OpenAIResponsesReplayVerifiedMetadataKey],
	) {
		return fmt.Errorf("%w: replay attestation is missing or invalid", ErrProviderStateIntegrity)
	}
	adapterProjection, ok := message.Metadata[openAIResponsesProjectionSHA256MetadataKey].(string)
	if !ok || strings.TrimSpace(adapterProjection) == "" {
		return fmt.Errorf("%w: provider-local projection hash is missing or invalid", ErrProviderStateIntegrity)
	}
	currentAdapterProjection, err := hashCanonicalJSON(message.Content)
	if err != nil {
		return fmt.Errorf("%w: fingerprint current response content: %w", ErrProviderStateIntegrity, err)
	}
	if !strings.EqualFold(adapterProjection, currentAdapterProjection) {
		// A post-model middleware may edit visible text or tool requests while
		// retaining the response metadata. Never bind the old opaque output array
		// to that new projection; the adapter's in-memory replay guard would reject
		// the same mismatch.
		return fmt.Errorf("%w: provider-local projection does not match current response content", ErrProviderStateIntegrity)
	}
	return nil
}

func nonEmptyProviderStateValue(value any) bool {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v) != ""
	case []byte:
		return len(bytes.TrimSpace(v)) != 0
	case jsontext.Value:
		return len(bytes.TrimSpace(v)) != 0
	default:
		// Live Responses output and durable reconstruction both use one of the exact byte/string
		// forms above. Reject arbitrary JSON-like metadata instead of silently re-encoding it as a
		// different opaque provider response.
		return false
	}
}

func openAIProviderStateFromMessage(message *genkitai.Message) *common_service_models.ProviderState {
	if err := validateOpenAIProviderStateIntegrity(message, false); err != nil || message == nil || message.Metadata == nil {
		// Production generation validates first and returns the typed integrity error. Keep the
		// extractor defensive for direct callers so invalid opaque state is never materialized.
		return nil
	}
	durableProjection, err := CanonicalAssistantProjectionSHA256(message)
	if err != nil {
		return nil
	}
	value, ok := message.Metadata[common_service_models.OpenAIResponsesOutputItemsMetadataKey]
	if !ok || value == nil {
		return nil
	}
	data, ok := providerStateBytes(value)
	if !ok || len(data) == 0 {
		return nil
	}
	digest := sha256.Sum256(data)
	state := &common_service_models.ProviderState{
		API:           proto_models.InferenceAPI_INFERENCE_API_RESPONSES,
		Format:        common_service_models.OpenAIResponsesStateFormat,
		Version:       common_service_models.OpenAIResponsesStateVersion,
		Data:          data,
		ContentSHA256: hex.EncodeToString(digest[:]),
		SizeBytes:     uint64(len(data)),
	}
	state.ResponseID, _ = message.Metadata[common_service_models.OpenAIResponsesResponseIDMetadataKey].(string)
	state.ToolManifestSHA256, _ = message.Metadata[common_service_models.OpenAIResponsesToolManifestSHA256MetadataKey].(string)
	state.AttachmentID = metadataUint64(message.Metadata[common_service_models.OpenAIResponsesAttachmentIDMetadataKey])
	state.VisibleProjectionSHA256 = durableProjection
	return state
}

func providerStateBytes(value any) ([]byte, bool) {
	switch v := value.(type) {
	case string:
		return []byte(v), true
	case []byte:
		return append([]byte(nil), v...), true
	case jsontext.Value:
		return append([]byte(nil), v...), true
	default:
		encoded, err := json.Marshal(v)
		return encoded, err == nil
	}
}

func metadataUint64(value any) uint64 {
	switch v := value.(type) {
	case uint64:
		return v
	case uint:
		return uint64(v)
	case uint32:
		return uint64(v)
	case int:
		if v >= 0 {
			return uint64(v)
		}
	case int64:
		if v >= 0 {
			return uint64(v)
		}
	case float64:
		if v >= 0 && v <= math.MaxUint64 && math.Trunc(v) == v {
			return uint64(v)
		}
	case jsontext.Value:
		if parsed, err := strconv.ParseUint(string(v), 10, 64); err == nil {
			return parsed
		}
	}
	return 0
}

// EstimateToolManifestTokens includes provider-hosted declarations in the heuristic input budget.
// It is deliberately separate from the hash: a hash's fixed length is not the manifest's cost.
func EstimateToolManifestTokens(tools []ports.AgentToolDefinition, providerConfig *modelconfig.Config) (int, error) {
	total := 0
	for _, tool := range tools {
		entry, err := effectiveToolManifestEntry(tool)
		if err != nil {
			return 0, err
		}
		total += (len(entry.Function)+len(entry.Description)+len(entry.InputSchema)+len(entry.OutputSchema)+3)/4 + 16
	}
	surface, err := providerToolSurface(providerConfig)
	if err != nil {
		return 0, err
	}
	if surface != nil {
		raw, err := json.Marshal(surface)
		if err != nil {
			return 0, err
		}
		total += (len(raw) + 3) / 4
	}
	return total, nil
}
