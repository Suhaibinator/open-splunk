package openaichat

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"fmt"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/common_json"
	"github.com/Suhaibinator/go-common/pkg/genai/generationfailure"
	"github.com/Suhaibinator/go-common/pkg/genai/retryclassify"
	"github.com/Suhaibinator/go-common/pkg/genai/toolrejection"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/firebase/genkit/go/ai"
	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/packages/respjson"
)

// ResponseMetadata is the completion metadata that is not part of the generated
// message. It is exposed through ai.ModelResponse.Custom.
type ResponseMetadata struct {
	// ID, Model and SystemFingerprint identify the backend configuration that
	// served the request; they are recorded when the provider reports a fingerprint.
	ID                string `json:"id,omitempty"`
	Model             string `json:"model,omitempty"`
	SystemFingerprint string `json:"systemFingerprint,omitempty"`
	// Citations carries the live-search sources a provider (xAI) attaches to the
	// completion in a field the SDK does not model, exactly as returned.
	Citations jsontext.Value `json:"citations,omitempty"`
	// Error is the error object a gateway attaches to a choice when its upstream
	// fails part-way through a generation while the HTTP status is already 200.
	Error jsontext.Value `json:"error,omitempty"`
}

func (m *ResponseMetadata) empty() bool {
	return m.ID == "" && m.Model == "" && m.SystemFingerprint == "" && len(m.Citations) == 0 && len(m.Error) == 0
}

// choiceFailure is the message of the error object a gateway attaches to a
// failing choice; the whole object rides on ResponseMetadata.Error.
type choiceFailure struct {
	Message string         `json:"message"`
	Code    jsontext.Value `json:"code"`
}

// streamExtras carries the fields the accumulator drops while a stream is read.
type streamExtras struct {
	reasoning strings.Builder
	citations jsontext.Value
	failure   jsontext.Value
}

func completionToResponse(completion *openai.ChatCompletion, req *ai.ModelRequest, projection *requestProjection, extras *streamExtras) (*ai.ModelResponse, error) {
	if completion == nil {
		return nil, fmt.Errorf("openaichat: provider returned a nil completion")
	}
	if len(completion.Choices) == 0 {
		return nil, fmt.Errorf("openaichat: no choices in completion")
	}
	choice := completion.Choices[0]
	upstreamFailed := choice.FinishReason == "error" || choice.FinishReason == "network_error" || choice.FinishReason == "insufficient_system_resource"

	message := &ai.Message{Role: ai.RoleModel, Content: make([]*ai.Part, 0, 2+len(choice.Message.ToolCalls))}
	// Reasoning models return their reasoning in a non-standard field:
	// reasoning_content for Kimi and DeepSeek, reasoning for OpenRouter. A stream
	// carries it on deltas, which the accumulator drops, so the collected text
	// stands in there.
	reasoning := firstReasoning(choice.Message.JSON.ExtraFields, metadataReasoningContent, "reasoning")
	if reasoning == "" && extras != nil {
		reasoning = extras.reasoning.String()
	}
	if reasoning != "" {
		message.Content = append(message.Content, ai.NewReasoningPart(reasoning, nil))
	}
	if choice.Message.Content != "" {
		part := ai.NewTextPart(choice.Message.Content)
		if projection != nil && projection.responseIsJSON {
			part = ai.NewJSONPart(choice.Message.Content)
		}
		message.Content = append(message.Content, part)
	}
	rejected := false
	for i, toolCall := range choice.Message.ToolCalls {
		if upstreamFailed {
			break
		}
		part, err := toolRequestPart(toolCall)
		if err != nil {
			return nil, fmt.Errorf("openaichat: tool call %d: %w", i, err)
		}
		if toolrejection.FromPart(part) != nil {
			rejected = true
		}
		message.Content = append(message.Content, part)
	}

	finishReason, finishMessage := finish(choice.FinishReason)
	failure := extraField(choice.JSON.ExtraFields, "error")
	if len(failure) == 0 {
		failure = extraField(completion.JSON.ExtraFields, "error")
	}
	if len(failure) == 0 && extras != nil {
		failure = extras.failure
	}
	// A provider that fails part-way through a generation is answered by a
	// gateway with a 200: the failure rides on the choice as finish_reason "error"
	// and an error object next to whatever text was produced first. A choice that
	// finished cleanly keeps an empty finish message even when an error-shaped
	// extra rides beside it.
	if len(failure) != 0 && finishReason != ai.FinishReasonStop {
		var decoded choiceFailure
		if err := json.Unmarshal(failure, &decoded); err == nil && decoded.Message != "" {
			finishMessage = decoded.Message
		}
	}
	if choice.Message.Refusal != "" {
		finishReason = ai.FinishReasonBlocked
		finishMessage = choice.Message.Refusal
	}

	metadata := ResponseMetadata{Citations: extraField(completion.JSON.ExtraFields, "citations"), Error: failure}
	if completion.SystemFingerprint != "" {
		metadata.SystemFingerprint = completion.SystemFingerprint
		metadata.Model = completion.Model
		metadata.ID = completion.ID
	}
	if len(metadata.Citations) == 0 && extras != nil {
		metadata.Citations = extras.citations
	}

	result := &ai.ModelResponse{
		Request:       req,
		Message:       message,
		FinishReason:  finishReason,
		FinishMessage: finishMessage,
		Usage:         usageFromCompletion(completion.Usage),
	}
	if !metadata.empty() {
		result.Custom = &metadata
	}
	if raw := completion.RawJSON(); raw != "" {
		result.Raw = raw
	}
	// Only recognized failure finishes signal generation failure. Unrelated
	// error-shaped extras beside a clean stop remain diagnostic metadata.
	switch choice.FinishReason {
	case "error", "network_error", "insufficient_system_resource":
		code := 503
		var decoded choiceFailure
		if json.Unmarshal(failure, &decoded) == nil {
			var status int
			if json.Unmarshal(decoded.Code, &status) == nil && status >= 400 && status <= 599 {
				code = status
			}
		}
		cause := &retryclassify.HTTPStatusError{StatusCode: code, Message: finishMessage}
		return nil, &generationfailure.Error{Response: result, Cause: &RequestError{cause: cause, statusCode: code, retryable: retryableStatus(code)}}
	}
	if rejected {
		return nil, &toolrejection.Error{Response: result}
	}
	return result, nil
}

// toolRequestPart projects one tool call. Arguments that are not a JSON value
// become a rejection the runtime can persist and correct instead of an executable
// request; call IDs are passed through because the runtime normalizes absent or
// reused IDs itself.
func toolRequestPart(toolCall openai.ChatCompletionMessageToolCallUnion) (*ai.Part, error) {
	if toolCall.Type != "" && toolCall.Type != "function" {
		return nil, fmt.Errorf("unsupported tool call type %q", toolCall.Type)
	}
	name := toolCall.Function.Name
	arguments := toolCall.Function.Arguments
	var input any // any: Genkit's ai.ToolRequest.Input.
	if err := common_json.UnmarshalExact([]byte(arguments), &input); err != nil {
		diagnostic := err.Error()
		if strings.TrimSpace(arguments) == "" {
			diagnostic = "missing JSON-encoded arguments"
		}
		return toolrejection.Part(&proto_models.RejectedToolArguments{ToolName: name, ProviderToolName: name, CallId: toolCall.ID, RawArguments: arguments, Diagnostic: diagnostic}), nil
	}
	return ai.NewToolRequestPart(&ai.ToolRequest{Name: name, Input: input, Ref: toolCall.ID}), nil
}

// finish maps the provider's finish_reason. end_turn is xAI's name for an answer
// the model chose to end; leaving it out would report an ordinary completion as
// unknown.
func finish(reason string) (ai.FinishReason, string) {
	switch reason {
	case "stop", "tool_calls", "end_turn":
		return ai.FinishReasonStop, ""
	case "length", "model_context_window_exceeded":
		return ai.FinishReasonLength, reason
	case "content_filter", "sensitive":
		return ai.FinishReasonBlocked, reason
	case "error", "function_call", "network_error", "insufficient_system_resource":
		return ai.FinishReasonOther, reason
	default:
		return ai.FinishReasonUnknown, reason
	}
}

func usageFromCompletion(completion openai.CompletionUsage) *ai.GenerationUsage {
	usage := &ai.GenerationUsage{
		InputTokens:    int(completion.PromptTokens),
		OutputTokens:   int(completion.CompletionTokens),
		TotalTokens:    int(completion.TotalTokens),
		ThoughtsTokens: int(completion.CompletionTokensDetails.ReasoningTokens),
	}
	// DeepSeek reports its cache hits as a usage field of its own and returns no
	// prompt_tokens_details at all, so that field stands in when the OpenAI
	// breakdown is absent.
	usage.CachedContentTokens = int(completion.PromptTokensDetails.CachedTokens)
	if usage.CachedContentTokens == 0 {
		usage.CachedContentTokens = extraTokenCount(completion.JSON.ExtraFields, "prompt_cache_hit_tokens")
	}
	// Token counts Genkit has no field of its own for. A zero is dropped: it is
	// read from a field that is absent, and reporting it would not tell that apart
	// from a genuine zero.
	addCustomTokens(usage, "audioTokens", int(completion.CompletionTokensDetails.AudioTokens))
	addCustomTokens(usage, "acceptedPredictionTokens", int(completion.CompletionTokensDetails.AcceptedPredictionTokens))
	addCustomTokens(usage, "rejectedPredictionTokens", int(completion.CompletionTokensDetails.RejectedPredictionTokens))
	addCustomTokens(usage, "imageTokens", int(completion.PromptTokensDetails.ImageTokens))
	// xAI counts the live-search sources it consulted.
	addCustomTokens(usage, "numSourcesUsed", extraTokenCount(completion.JSON.ExtraFields, "num_sources_used"))
	// A gateway prices the request it routed. Unlike the counts above, presence
	// decides rather than the value: a free-tier request is priced at an explicit
	// zero, which is an answer.
	if raw := extraField(completion.JSON.ExtraFields, "cost"); len(raw) != 0 {
		var cost float64
		if err := json.Unmarshal(raw, &cost); err == nil {
			if usage.Custom == nil {
				usage.Custom = make(map[string]float64)
			}
			usage.Custom["cost"] = cost
		}
	}
	return usage
}

func addCustomTokens(usage *ai.GenerationUsage, name string, count int) {
	if count <= 0 {
		return
	}
	if usage.Custom == nil {
		usage.Custom = make(map[string]float64)
	}
	usage.Custom[name] = float64(count)
}

// extraField returns the raw JSON of a response field the SDK does not model,
// or nil when it is absent or null. Unmodeled fields never report Valid, so
// only the raw text is consulted.
func extraField(fields map[string]respjson.Field, key string) jsontext.Value {
	raw := fields[key].Raw()
	if raw == "" || raw == "null" {
		return nil
	}
	return jsontext.Value(raw)
}

func extraTokenCount(fields map[string]respjson.Field, key string) int {
	raw := extraField(fields, key)
	if len(raw) == 0 {
		return 0
	}
	var count int
	if err := json.Unmarshal(raw, &count); err != nil {
		return 0
	}
	return count
}

// firstReasoning returns the first non-empty reasoning string among the fields a
// provider may carry reasoning in. Providers disagree on the name, so the caller
// passes every field it knows in preference order; a provider sending two of
// them is read once rather than appended twice.
func firstReasoning(fields map[string]respjson.Field, keys ...string) string {
	for _, key := range keys {
		raw := extraField(fields, key)
		if len(raw) == 0 {
			continue
		}
		var reasoning string
		if err := json.Unmarshal(raw, &reasoning); err == nil && reasoning != "" {
			return reasoning
		}
	}
	return ""
}
