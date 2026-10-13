package ai

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/common_json"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/genai/partmetadata"
	"github.com/Suhaibinator/go-common/pkg/genai/toolrejection"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	genkitai "github.com/firebase/genkit/go/ai"
	"google.golang.org/protobuf/proto"
)

// MetadataToolResponseOutputSynthesized marks Output that exists only as a compatibility fallback
// for providers that ignore multipart ToolResponse.Content. Multipart-aware adapters must omit
// that synthetic Output so text parts are not sent twice.
const MetadataToolResponseOutputSynthesized = "go-common.tool_response_output_synthesized"

// A deferred attachment is costed conservatively before its private source is resolved. This is
// longer than ordinary presigned URLs, so the post-budget hydration pass normally reduces cost.
const deferredToolMediaPlaceholder = "https://internal.invalid/" +
	"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" +
	"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" +
	"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" +
	"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

// decodeModelPayload extracts the structured MessageModelPayload from a service-model
// ChatMessage's ToolCall side row, but only when the row's discriminator matches expected.
// It returns nil when the row carries no payload, has a different payload type, or cannot be
// unmarshaled, so reconstruction can fall back to the human-readable content.
func decodeModelPayload(msg *common_service_models.ChatMessage, expected proto_models.MessageModelPayloadType) *proto_models.MessageModelPayload {
	if msg == nil || msg.ToolCall == nil || len(msg.ToolCall.ModelPayload) == 0 {
		return nil
	}
	if msg.ToolCall.PayloadType != expected {
		return nil
	}
	var payload proto_models.MessageModelPayload
	if err := proto.Unmarshal(msg.ToolCall.ModelPayload, &payload); err != nil {
		return nil
	}
	return &payload
}

// decodeToolCallPayload extracts the ToolCallPayload from a TOOL_CALL row, or nil.
func decodeToolCallPayload(msg *common_service_models.ChatMessage) *proto_models.ToolCallPayload {
	if p := decodeModelPayload(msg, proto_models.MessageModelPayloadType_MESSAGE_MODEL_PAYLOAD_TYPE_TOOL_CALL); p != nil {
		return p.GetToolCall()
	}
	return nil
}

// decodeProviderStatePayload extracts the reference-only ProviderStatePayload from an internal
// PROVIDER_STATE row, or nil when the discriminator or protobuf oneof does not match.
func decodeProviderStatePayload(msg *common_service_models.ChatMessage) *proto_models.ProviderStatePayload {
	if p := decodeModelPayload(msg, proto_models.MessageModelPayloadType_MESSAGE_MODEL_PAYLOAD_TYPE_PROVIDER_STATE); p != nil {
		return p.GetProviderState()
	}
	return nil
}

// decodeToolResultPayload extracts the ToolResultPayload from a TOOL_RESULT row, or nil.
func decodeToolResultPayload(msg *common_service_models.ChatMessage) *proto_models.ToolResultPayload {
	if p := decodeModelPayload(msg, proto_models.MessageModelPayloadType_MESSAGE_MODEL_PAYLOAD_TYPE_TOOL_RESULT); p != nil {
		return p.GetToolResult()
	}
	return nil
}

// toolCallIDOf returns the pairing key for a tool-call/result row, preferring the
// denormalized ToolCall.ToolCallID column and falling back to the payload's tool_call_id.
func toolCallIDOf(msg *common_service_models.ChatMessage) string {
	if msg == nil || msg.ToolCall == nil {
		return ""
	}
	if msg.ToolCall.ToolCallID != nil && *msg.ToolCall.ToolCallID != "" {
		return *msg.ToolCall.ToolCallID
	}
	if payload := decodeToolCallPayload(msg); payload != nil {
		return payload.GetToolCallId()
	}
	if payload := decodeToolResultPayload(msg); payload != nil {
		return payload.GetToolCallId()
	}
	return ""
}

// toolRequestPartFromMessage builds a genkit ToolRequest part from a TOOL_CALL row. The
// internal call reference (Ref) is the tool_call_id so it pairs with the matching ToolResponse.
// An exact provider call ID, when present, is restored separately in part metadata for the
// Vertex adapter. Returns nil when the row has no decodable tool-call payload.
func toolRequestPartFromMessage(msg *common_service_models.ChatMessage) *genkitai.Part {
	payload := decodeToolCallPayload(msg)
	if payload == nil {
		return nil
	}
	if rejected := payload.GetRejectedArguments(); rejected != nil {
		rejected = proto.Clone(rejected).(*proto_models.RejectedToolArguments)
		if payload.ProviderCallId != nil {
			rejected.CallId = payload.GetProviderCallId()
		}
		part := toolrejection.Part(rejected)
		if payload.ProviderCallId != nil {
			part.Metadata = partmetadata.SetFunctionCallID(part.Metadata, payload.GetProviderCallId())
		}
		return part
	}
	part := genkitai.NewToolRequestPart(&genkitai.ToolRequest{
		Name:  payload.GetToolName(),
		Ref:   payload.GetToolCallId(),
		Input: decodeJSONArguments(payload.GetArgumentsJson()),
	})
	if signature := payload.GetThoughtSignature(); len(signature) > 0 {
		part.Metadata = map[string]any{"signature": append([]byte(nil), signature...)}
	}
	if payload.ProviderCallId != nil {
		if payload.GetProviderCallId() != "" {
			part.ToolRequest.Ref = payload.GetProviderCallId()
		}
		part.Metadata = partmetadata.SetFunctionCallID(part.Metadata, payload.GetProviderCallId())
	}
	return part
}

// toolResponsePartFromMessage builds a genkit ToolResponse part from a TOOL_RESULT row.
// Ref equals the original request's tool_call_id. toolName is the paired TOOL_CALL's tool name:
// the result payload does not carry it, but name-based providers (Vertex/Gemini's
// FunctionResponse) match responses by function name, so it must be populated (Ref alone
// suffices for OpenAI-compatible providers but not for name-based ones). Output carries the JSON
// result; Content carries any multipart parts so media/data survive the round-trip. Returns nil
// when the row has no decodable tool-result payload.
func toolResponsePartFromMessage(ctx context.Context, msg *common_service_models.ChatMessage, toolName string, providerCallID *string, opts AgentContextOptions) (*genkitai.Part, []deferredHydration, error) {
	payload := decodeToolResultPayload(msg)
	if payload == nil {
		return nil, nil, nil
	}
	output := decodeJSONArguments(payload.GetContentJson())
	outputSynthesized := false
	if output == nil {
		output = fallbackOutputFromParts(payload)
		outputSynthesized = true
	}
	content, hydrations, err := genkitPartsFromMessageParts(ctx, msg, payload.GetContentParts(), opts)
	if err != nil {
		return nil, nil, err
	}
	part := genkitai.NewToolResponsePart(&genkitai.ToolResponse{
		Name:    toolName,
		Ref:     payload.GetToolCallId(),
		Output:  output,
		Content: content,
	})
	if outputSynthesized && len(content) > 0 {
		part.Metadata = map[string]any{MetadataToolResponseOutputSynthesized: true}
	}
	if providerCallID != nil {
		if *providerCallID != "" {
			part.ToolResponse.Ref = *providerCallID
		}
		part.Metadata = partmetadata.SetFunctionCallID(part.Metadata, *providerCallID)
	}
	return part, hydrations, nil
}

// fallbackOutputFromParts folds a parts-only result's text into Output. Provider adapters
// (the Chat Completions adapter and the Vertex runtime) serialize only ToolResponse.Output and drop
// Content, so a result whose text lives only in content_parts — notably the synthetic
// denial/expiry result, which leaves content_json empty — would otherwise reach the model
// as a literal null. Error results are wrapped in {"error": ...} so the model can tell a
// failed call from one that returned this text.
func fallbackOutputFromParts(payload *proto_models.ToolResultPayload) any {
	texts := make([]string, 0, len(payload.GetContentParts()))
	for _, part := range payload.GetContentParts() {
		if part == nil || part.GetText() == nil {
			continue
		}
		if text := part.GetText().GetText(); strings.TrimSpace(text) != "" {
			texts = append(texts, text)
		}
	}
	if len(texts) == 0 {
		return nil
	}
	joined := strings.Join(texts, "\n")
	if payload.GetIsError() {
		return map[string]any{"error": joined}
	}
	return joined
}

// decodeJSONArguments decodes a JSON-encoded argument/result string into the any shape
// genkit expects. Empty or invalid JSON yields nil.
func decodeJSONArguments(encoded string) any {
	trimmed := strings.TrimSpace(encoded)
	if trimmed == "" {
		return nil
	}
	var out any
	if err := common_json.UnmarshalExact([]byte(trimmed), &out); err != nil {
		return nil
	}
	return out
}

// genkitPartsFromMessageParts maps proto MessagePart content (text/media/data) back into
// genkit parts for a tool response's multipart Content. Unknown/empty parts are skipped.
func genkitPartsFromMessageParts(ctx context.Context, msg *common_service_models.ChatMessage, parts []*proto_models.MessagePart, opts AgentContextOptions) ([]*genkitai.Part, []deferredHydration, error) {
	if len(parts) == 0 {
		return nil, nil, nil
	}
	out := make([]*genkitai.Part, 0, len(parts))
	hydrations := make([]deferredHydration, 0)
	for partIndex, part := range parts {
		if part == nil {
			continue
		}
		switch {
		case part.GetText() != nil:
			if text := part.GetText().GetText(); strings.TrimSpace(text) != "" {
				out = append(out, genkitai.NewTextPart(text))
			}
		case part.GetMedia() != nil:
			media := part.GetMedia()
			if err := validateProviderToolResultContent(opts.CurrentProviderID, media.GetContentType(), media.GetName(), media.GetDetail(), toolResultContentMedia, opts.CurrentAPI); err != nil {
				return nil, nil, fmt.Errorf("ai context: tool result content part %d: %w", partIndex, err)
			}
			mediaPart, hydration, err := genkitMediaPartFromProto(ctx, msg, media, partIndex, opts)
			if err != nil {
				return nil, nil, err
			}
			if mediaPart != nil {
				out = append(out, mediaPart)
			}
			if hydration != nil {
				hydrations = append(hydrations, hydration)
			}
		case part.GetData() != nil:
			data := part.GetData()
			if err := validateProviderToolResultContent(opts.CurrentProviderID, data.GetContentType(), "", "", toolResultContentData, opts.CurrentAPI); err != nil {
				return nil, nil, fmt.Errorf("ai context: tool result content part %d: %w", partIndex, err)
			}
			// DataPart.data is durable serialized bytes, not a URL. Encode those
			// bytes into Genkit's unambiguous data: transport form so upstream
			// Google adapters and our native provider adapters agree. In
			// particular, payloads beginning with http://, file-, or data: remain
			// literal bytes instead of being reinterpreted as references.
			dataPart := genkitai.NewDataPart(common_util.EncodeDataURI(data.GetContentType(), data.GetData()))
			dataPart.ContentType = data.GetContentType()
			if size := len(data.GetData()); size > 0 {
				// The durable byte count is authoritative. Carrying it as trusted metadata lets
				// context accounting skip a decode and lets the OpenAI adapter cross-check the
				// payload it decodes against the size the transcript recorded.
				dataPart.Metadata = map[string]any{ports.ToolMediaSizeMetadataKey: uint64(size)}
			}
			out = append(out, dataPart)
		}
	}
	if len(out) == 0 {
		return nil, hydrations, nil
	}
	return out, hydrations, nil
}

// genkitMediaPartFromProto maps a proto MediaPart to a genkit media part. A URL source maps
// to a media part with that URL; inline bytes map to a base64 data URI.
func genkitMediaPartFromProto(_ context.Context, msg *common_service_models.ChatMessage, media *proto_models.MediaPart, partIndex int, opts AgentContextOptions) (*genkitai.Part, deferredHydration, error) {
	if media == nil {
		return nil, nil, nil
	}
	contentType := media.GetContentType()
	if url := strings.TrimSpace(media.GetUrl()); url != "" {
		return namedGenkitMediaPart(contentType, url, media.GetName(), media.GetDetail()), nil, nil
	}
	if inline := media.GetInlineData(); len(inline) > 0 {
		dataURI := common_util.EncodeDataURI(contentType, inline)
		return namedGenkitMediaPart(contentType, dataURI, media.GetName(), media.GetDetail()), nil, nil
	}
	if attachmentIDText := strings.TrimSpace(media.GetAttachmentId()); attachmentIDText != "" {
		if media.GetSizeBytes() == 0 {
			return nil, nil, fmt.Errorf("ai context: tool media attachment %q has no persisted size", attachmentIDText)
		}
		attachmentID, err := strconv.ParseUint(attachmentIDText, 10, 64)
		if err != nil || attachmentID == 0 || msg == nil || msg.AgentRunID == nil {
			return nil, nil, fmt.Errorf("ai context: invalid tool media attachment reference %q", attachmentIDText)
		}
		payload := decodeToolResultPayload(msg)
		if payload == nil || strings.TrimSpace(payload.GetToolCallId()) == "" {
			return nil, nil, fmt.Errorf("ai context: tool media attachment %d cannot be resolved", attachmentID)
		}
		resolvedPart := namedGenkitMediaPart(contentType, deferredToolMediaPlaceholder, media.GetName(), media.GetDetail())
		if resolvedPart.Metadata == nil {
			resolvedPart.Metadata = make(map[string]any)
		}
		resolvedPart.Metadata[ports.ToolMediaSizeMetadataKey] = media.GetSizeBytes()
		resolvedPart.Metadata = partmetadata.MarkTrustedHTTPSource(resolvedPart.Metadata)
		hydrate := func(hydrationCtx context.Context, budget *hydrationBudget) error {
			if opts.ToolMediaResolver == nil {
				return fmt.Errorf("ai context: tool media attachment %d cannot be resolved", attachmentID)
			}
			if budget == nil {
				return fmt.Errorf("ai context: tool media hydration budget is required")
			}
			remaining := budget.remaining()
			if remaining == 0 || media.GetSizeBytes() > remaining {
				return fmt.Errorf("ai context: retained internal attachments exceed aggregate byte limit %d", budget.limit)
			}
			// The transcript descriptor is bound to the attachment row during insertion. Use its
			// exact persisted size as the resolver ceiling, rather than the larger aggregate
			// remainder, so an understated or legacy descriptor can never cause an oversized body
			// read before the equality check below fails.
			resolved, err := opts.ToolMediaResolver(hydrationCtx, attachmentID, uint64(msg.SenderID), *msg.AgentRunID, payload.GetToolCallId(), partIndex, contentType, media.GetName(), media.GetSizeBytes())
			if err != nil {
				return fmt.Errorf("ai context: resolve tool media attachment %d: %w", attachmentID, err)
			}
			if resolved == nil || strings.TrimSpace(resolved.Source) == "" {
				return fmt.Errorf("ai context: tool media attachment %d resolved empty", attachmentID)
			}
			if resolved.SizeBytes != media.GetSizeBytes() {
				return fmt.Errorf("ai context: tool media attachment %d size mismatch", attachmentID)
			}
			if err := budget.reserve(resolved.SizeBytes); err != nil {
				return err
			}
			resolvedPart.Text = resolved.Source
			return nil
		}
		return resolvedPart, hydrate, nil
	}
	return nil, nil, nil
}

func namedGenkitMediaPart(contentType, source, filename, detail string) *genkitai.Part {
	part := genkitai.NewMediaPart(contentType, source)
	if filename = strings.TrimSpace(filename); filename != "" {
		part.Metadata = map[string]any{"filename": filename}
	}
	if detail = strings.TrimSpace(detail); detail != "" {
		if part.Metadata == nil {
			part.Metadata = make(map[string]any)
		}
		part.Metadata["detail"] = detail
	}
	return part
}
