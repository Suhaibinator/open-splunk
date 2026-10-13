package openaichat

import (
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"net/http"

	"github.com/Suhaibinator/go-common/pkg/genai/bodylimit"
	"github.com/Suhaibinator/go-common/pkg/genai/generationfailure"
	"github.com/Suhaibinator/go-common/pkg/genai/retryclassify"
	"github.com/firebase/genkit/go/ai"
	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/packages/param"
	"github.com/openai/openai-go/v3/packages/ssestream"
)

// RequestError preserves a request or response failure while exposing provider-neutral
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
		return "openaichat: request failed"
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
	if errors.Is(err, bodylimit.ErrResponseBodyTooLarge) {
		return &RequestError{cause: err, statusCode: http.StatusRequestEntityTooLarge, retryable: false}
	}
	var apiErr *openai.Error
	if !errors.As(err, &apiErr) {
		return classifyStreamError(err)
	}
	return &RequestError{cause: err, statusCode: apiErr.StatusCode, retryable: retryableStatus(apiErr.StatusCode)}
}

func retryableStatus(status int) bool {
	return retryclassify.IsRetryableHTTPStatus(status)
}

// streamErrorEnvelope is the error object a gateway reports at the top level of
// a chunk when its upstream fails part-way through a generation. The SDK ends
// the stream when it sees one, keeping the event data on the error; the code is
// an HTTP status for gateways that forward one, and reading it back is the only
// way to recover the status the failure carries.
type streamErrorEnvelope struct {
	Error struct {
		Code    jsontext.Value `json:"code"`
		Message string         `json:"message"`
	} `json:"error"`
}

// classifyStreamError classifies a mid-stream error object by its HTTP status
// code when it carries one. An error this cannot read stays unclassified rather
// than terminal: the runtime's own message classifier still sees it, and a
// failure nobody understands is not a reason to stop trying.
func classifyStreamError(err error) error {
	var streamErr *ssestream.StreamError
	if !errors.As(err, &streamErr) {
		return err
	}
	var envelope streamErrorEnvelope
	if json.Unmarshal(streamErr.Event.Data, &envelope) != nil {
		return err
	}
	var code int
	if json.Unmarshal(envelope.Error.Code, &code) != nil || code < 100 || code > 599 {
		return err
	}
	return &RequestError{cause: err, statusCode: code, retryable: retryclassify.IsRetryableHTTPStatus(code)}
}

func nonRetryableResponseError(err error) error {
	if err == nil {
		return nil
	}
	if _, failed := generationfailure.ResponseFromError(err); failed {
		return err
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
	projection, err := buildRequest(modelID, req)
	if err != nil {
		return nil, &RequestError{cause: err, retryable: false}
	}
	if cb == nil {
		p.logRequest(ctx, projection.params, false)
		completion, err := p.client.Chat.Completions.New(ctx, projection.params)
		if err != nil {
			return nil, fmt.Errorf("openaichat: create completion: %w", classifyRequestError(err))
		}
		if completion != nil {
			p.logNativePayload(ctx, "openai chat completion", completion)
		}
		response, err := completionToResponse(completion, req, projection, nil)
		if err != nil {
			return nil, nonRetryableResponseError(err)
		}
		return response, nil
	}
	return p.generateStreaming(ctx, req, projection, cb)
}

func (p *Plugin) generateStreaming(ctx context.Context, req *ai.ModelRequest, projection *requestProjection, cb ai.ModelStreamCallback) (*ai.ModelResponse, error) {
	// A streamed request carries no token usage unless it asks for it, so a
	// response without usage would otherwise report zero tokens for every stream.
	projection.params.StreamOptions.IncludeUsage = param.NewOpt(true)
	p.logRequest(ctx, projection.params, true)
	stream := p.client.Chat.Completions.NewStreaming(ctx, projection.params)
	defer stream.Close()

	acc := openai.ChatCompletionAccumulator{}
	extras := &streamExtras{}
	// The accumulator adds up the three top-level token counts and drops the
	// reasoning and cache breakdowns, so the usage chunk is kept whole instead.
	var usage openai.CompletionUsage
	usageSeen := false
	for stream.Next() {
		chunk := stream.Current()
		// A chunk the accumulator refuses (an unrelated completion id, an index it
		// cannot place) matches how the SDK's own callers treat the stream; the
		// terminal response is still validated below.
		acc.AddChunk(chunk)
		if len(chunk.Choices) != 0 && (chunk.Choices[0].FinishReason != "" || len(chunk.Choices[0].Delta.ToolCalls) != 0) || chunk.JSON.Usage.Valid() {
			p.logNativePayload(ctx, "openai chat completion chunk", chunk)
		}
		// Usage rides on a final chunk of its own and is null on the rest.
		if chunk.JSON.Usage.Valid() {
			usage = chunk.Usage
			usageSeen = true
		}
		if citations := extraField(chunk.JSON.ExtraFields, "citations"); len(citations) != 0 {
			extras.citations = citations
		}
		if len(chunk.Choices) == 0 {
			continue
		}
		choice := chunk.Choices[0]
		if failure := extraField(choice.JSON.ExtraFields, "error"); len(failure) != 0 {
			extras.failure = failure
		}
		var parts []*ai.Part
		if delta := firstReasoning(choice.Delta.JSON.ExtraFields, metadataReasoningContent, "reasoning"); delta != "" {
			extras.reasoning.WriteString(delta)
			parts = append(parts, ai.NewReasoningPart(delta, nil))
		}
		if choice.Delta.Content != "" {
			part := ai.NewTextPart(choice.Delta.Content)
			if projection.responseIsJSON {
				part = ai.NewJSONPart(choice.Delta.Content)
			}
			parts = append(parts, part)
		}
		// Tool-call deltas are argument fragments; the complete calls are emitted
		// below, only after the terminal completion has passed validation, so a
		// partial call can never be observed as executable.
		if len(parts) == 0 {
			continue
		}
		if err := cb(ctx, &ai.ModelResponseChunk{Content: parts, Role: ai.RoleModel, Index: int(choice.Index)}); err != nil {
			return nil, fmt.Errorf("openaichat: stream callback: %w", err)
		}
	}
	// NewStreaming defers its HTTP error to the stream, so a 4xx on the request
	// that opened it surfaces here, as does a gateway's mid-stream failure.
	// Whatever was generated before the failure has already reached the callback;
	// the aggregate response is dropped so the caller sees a failed generation
	// rather than a short answer that reads as a complete one.
	if err := stream.Err(); err != nil {
		return nil, fmt.Errorf("openaichat: stream completion: %w", classifyRequestError(err))
	}
	if usageSeen {
		acc.Usage = usage
	}
	response, err := completionToResponse(&acc.ChatCompletion, req, projection, extras)
	if err != nil {
		return nil, nonRetryableResponseError(err)
	}
	for _, part := range response.Message.Content {
		if !part.IsToolRequest() {
			continue
		}
		if err := cb(ctx, &ai.ModelResponseChunk{Content: []*ai.Part{part}, Role: ai.RoleModel}); err != nil {
			return nil, fmt.Errorf("openaichat: stream callback: %w", err)
		}
	}
	return response, nil
}
