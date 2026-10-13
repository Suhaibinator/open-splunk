package openairesponses

import (
	"context"
	"errors"
	"fmt"
	"github.com/Suhaibinator/go-common/pkg/genai/toolrejection"
	"net/http"

	"github.com/firebase/genkit/go/ai"
	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/responses"
)

// RequestError preserves an OpenAI request or response failure while exposing provider-neutral
// retryability to the durable agent runtime through the structural `Retryable() bool` contract.
// Deterministic failures must terminalize instead of being redriven forever after every recovery
// scan.
type RequestError struct {
	cause      error
	statusCode int
	retryable  bool
}

func (e *RequestError) Error() string {
	if e == nil || e.cause == nil {
		return "openairesponses: request failed"
	}
	return e.cause.Error()
}

func (e *RequestError) Unwrap() error   { return e.cause }
func (e *RequestError) Retryable() bool { return e != nil && e.retryable }
func (e *RequestError) HTTPStatusCode() int {
	if e == nil {
		return 0
	}
	return e.statusCode
}

func classifyRequestError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, errResponsesResponseBodyTooLarge) {
		return &RequestError{cause: err, statusCode: http.StatusRequestEntityTooLarge, retryable: false}
	}
	var apiErr *openai.Error
	if !errors.As(err, &apiErr) {
		return err
	}
	status := apiErr.StatusCode
	retryable := status < 400 || status >= 500
	switch status {
	case http.StatusRequestTimeout, http.StatusConflict, http.StatusTooManyRequests:
		retryable = true
	default:
		if status >= 400 && status < 500 {
			retryable = false
		}
	}
	return &RequestError{cause: err, statusCode: status, retryable: retryable}
}

func responseFailureError(prefix, code, message string) error {
	if message == "" {
		message = "response failed"
	}
	retryable := false
	switch code {
	case string(responses.ResponseErrorCodeServerError),
		string(responses.ResponseErrorCodeRateLimitExceeded),
		string(responses.ResponseErrorCodeVectorStoreTimeout):
		retryable = true
	}
	return &RequestError{
		cause:     fmt.Errorf("%s (%s): %s", prefix, code, message),
		retryable: retryable,
	}
}

func nonRetryableResponseError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[interface {
		error
		Retryable() bool
	}](err); ok {
		return err
	}
	return &RequestError{cause: err, retryable: false}
}

func (p *Plugin) generate(ctx context.Context, modelID string, req *ai.ModelRequest, cb ai.ModelStreamCallback) (*ai.ModelResponse, error) {
	projection, err := buildRequestWithPolicy(modelID, req, requestPolicy{
		allowHostedImageGeneration: p.allowHostedImageGeneration,
		allowAtLeastOnceHostedMCP:  p.allowAtLeastOnceHostedMCP,
	})
	if err != nil {
		return nil, &RequestError{cause: err, retryable: false}
	}
	p.logRequest(ctx, projection.params, cb != nil)
	if cb == nil {
		resp, err := p.client.Responses.New(ctx, projection.params, projection.requestOptions...)
		if err != nil {
			return nil, fmt.Errorf("openairesponses: create response: %w", classifyRequestError(err))
		}
		if resp != nil {
			p.logNativePayload(ctx, "openai responses terminal", resp)
		}
		response, err := responseToGenkit(resp, req, projection)
		if err != nil {
			return nil, nonRetryableResponseError(err)
		}
		return response, nil
	}
	return p.generateStreaming(ctx, req, projection, cb)
}

func (p *Plugin) generateStreaming(ctx context.Context, req *ai.ModelRequest, projection *requestProjection, cb ai.ModelStreamCallback) (*ai.ModelResponse, error) {
	stream := p.client.Responses.NewStreaming(ctx, projection.params, projection.requestOptions...)
	defer stream.Close()

	var terminal *responses.Response
	var itemValidationErr error
	seenDoneItemIDs := make(map[string]struct{})
	seenDoneCallIDs := make(map[string]string)
	for stream.Next() {
		event := stream.Current()
		switch event.Type {
		case "response.function_call_arguments.delta", "response.function_call_arguments.done", "response.output_item.done", "response.completed", "response.incomplete", "response.failed", "error":
			p.logNativePayload(ctx, "openai responses stream event", event)
		}
		switch event.Type {
		case "response.output_text.delta":
			part := ai.NewTextPart(event.Delta)
			if projection.responseIsJSON {
				part = ai.NewJSONPart(event.Delta)
			}
			part.Metadata = map[string]any{metadataItemID: event.ItemID}
			if err := cb(ctx, &ai.ModelResponseChunk{
				Content: []*ai.Part{part}, Role: ai.RoleModel, Index: int(event.OutputIndex),
			}); err != nil {
				return nil, fmt.Errorf("openairesponses: stream callback: %w", err)
			}

		case "response.reasoning_summary_text.delta", "response.reasoning_text.delta":
			part := newReasoningPart(event.Delta, nil, event.ItemID)
			if err := cb(ctx, &ai.ModelResponseChunk{
				Content: []*ai.Part{part}, Role: ai.RoleModel, Index: int(event.OutputIndex),
			}); err != nil {
				return nil, fmt.Errorf("openairesponses: stream callback: %w", err)
			}

		case "response.refusal.delta":
			part := ai.NewTextPart(event.Delta)
			part.Metadata = map[string]any{metadataItemID: event.ItemID, "refusal": true}
			if err := cb(ctx, &ai.ModelResponseChunk{
				Content: []*ai.Part{part}, Role: ai.RoleModel, Index: int(event.OutputIndex),
			}); err != nil {
				return nil, fmt.Errorf("openairesponses: stream callback: %w", err)
			}

		case "response.output_item.done":
			if event.Item.ID != "" {
				if _, exists := seenDoneItemIDs[event.Item.ID]; exists {
					return nil, nonRetryableResponseError(fmt.Errorf("openairesponses: stream returned duplicate output item id %q", event.Item.ID))
				}
				seenDoneItemIDs[event.Item.ID] = struct{}{}
			}
			if event.Item.Type == "function_call" {
				// A compatible provider may mark the item completed even when its
				// arguments were cut short. Wait for the canonical terminal response
				// so its incomplete reason and usage are not hidden by a JSON error.
				part, err := functionCallPart(event.Item, projection)
				if rejected := toolrejection.FromPart(part); rejected != nil {
					err = fmt.Errorf("decode arguments for function %q: %s", rejected.ProviderToolName, rejected.Diagnostic)
				}
				if err != nil && itemValidationErr == nil {
					itemValidationErr = nonRetryableResponseError(fmt.Errorf("openairesponses: streamed function call %q: %w", event.Item.ID, err))
				}
				if previousItemID, exists := seenDoneCallIDs[event.Item.CallID]; exists {
					return nil, nonRetryableResponseError(fmt.Errorf("openairesponses: stream returned duplicate function call_id %q for output items %q and %q", event.Item.CallID, previousItemID, event.Item.ID))
				}
				seenDoneCallIDs[event.Item.CallID] = event.Item.ID
			}

		case "response.completed", "response.incomplete":
			response := event.Response
			terminal = &response

		case "response.failed":
			response := event.Response
			return nil, responseFailureError(
				"openairesponses: streaming response failed",
				string(response.Error.Code),
				response.Error.Message,
			)

		case "error":
			return nil, responseFailureError(
				fmt.Sprintf("openairesponses: streaming error (param %s)", event.Param),
				event.Code,
				event.Message,
			)
		}
	}
	if err := stream.Err(); err != nil {
		return nil, fmt.Errorf("openairesponses: stream response: %w", classifyRequestError(err))
	}
	if terminal == nil {
		if itemValidationErr != nil {
			return nil, itemValidationErr
		}
		return nil, fmt.Errorf("openairesponses: stream ended without a terminal response event")
	}
	response, err := responseToGenkit(terminal, req, projection)
	if err != nil {
		return nil, nonRetryableResponseError(err)
	}
	// Tool requests are deliberately emitted only after the complete terminal
	// response has passed status, uniqueness, advertised-tool, and argument
	// validation. This prevents a completed output_item from becoming executable
	// if the overall response later ends INCOMPLETE or FAILED.
	for index, item := range terminal.Output {
		if terminal.Status != responses.ResponseStatusCompleted || item.Type != "function_call" {
			continue
		}
		if err := emitFunctionChunk(ctx, cb, item, projection, index); err != nil {
			return nil, err
		}
	}
	return response, nil
}

func emitFunctionChunk(ctx context.Context, cb ai.ModelStreamCallback, item responses.ResponseOutputItemUnion, projection *requestProjection, index int) error {
	part, err := functionCallPart(item, projection)
	if err != nil {
		return nonRetryableResponseError(fmt.Errorf("openairesponses: streamed function call %q: %w", item.ID, err))
	}
	if err := cb(ctx, &ai.ModelResponseChunk{
		Content: []*ai.Part{part}, Role: ai.RoleModel, Index: index,
	}); err != nil {
		return fmt.Errorf("openairesponses: stream callback: %w", err)
	}
	return nil
}
