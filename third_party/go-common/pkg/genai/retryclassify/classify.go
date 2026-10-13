// Package retryclassify supplies provider error classification without runtime dependencies.
package retryclassify

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// RetryReason is the typed classification of why a failed attempt was judged
// retryable or terminal. It aliases the generated protobuf enum (zero is the
// unspecified value) whose integer values the kernel already matched, so the
// classifier, the persisted attempt-ledger retry-reason field, and the wire
// contract share one type.
type RetryReason = proto_models.InferenceRetryReason

const (
	// RetryReasonUnspecified is the zero value: no error, or an unclassifiable empty
	// error message.
	RetryReasonUnspecified = proto_models.InferenceRetryReason_INFERENCE_RETRY_REASON_UNSPECIFIED
	// RetryReasonContextCanceled marks a terminal context cancellation.
	RetryReasonContextCanceled = proto_models.InferenceRetryReason_INFERENCE_RETRY_REASON_CONTEXT_CANCELED
	// RetryReasonContextDeadlineExceeded marks a retryable context deadline.
	RetryReasonContextDeadlineExceeded = proto_models.InferenceRetryReason_INFERENCE_RETRY_REASON_CONTEXT_DEADLINE_EXCEEDED
	// RetryReasonHTTPStatus marks a typed HTTP status error with a retryable status
	// code; the code is carried in RetryDecision.HTTPStatus.
	RetryReasonHTTPStatus = proto_models.InferenceRetryReason_INFERENCE_RETRY_REASON_HTTP_STATUS
	// RetryReasonHTTPStatusTerminal marks a typed HTTP status error with a
	// non-retryable status code.
	RetryReasonHTTPStatusTerminal = proto_models.InferenceRetryReason_INFERENCE_RETRY_REASON_HTTP_STATUS_TERMINAL
	// RetryReasonNetworkTimeout marks a retryable net.Error timeout.
	RetryReasonNetworkTimeout = proto_models.InferenceRetryReason_INFERENCE_RETRY_REASON_NETWORK_TIMEOUT
	// RetryReasonNetworkTemporary marks a retryable net.Error temporary failure.
	RetryReasonNetworkTemporary = proto_models.InferenceRetryReason_INFERENCE_RETRY_REASON_NETWORK_TEMPORARY
	// RetryReasonMessageHTTPStatus marks a retryable HTTP status inferred from the
	// error message text; the code is carried in RetryDecision.HTTPStatus.
	RetryReasonMessageHTTPStatus = proto_models.InferenceRetryReason_INFERENCE_RETRY_REASON_MESSAGE_HTTP_STATUS
	// RetryReasonMessageRateLimit marks a rate-limit or resource-exhaustion phrase in
	// the error message.
	RetryReasonMessageRateLimit = proto_models.InferenceRetryReason_INFERENCE_RETRY_REASON_MESSAGE_RATE_LIMIT
	// RetryReasonMessageUnavailable marks an unavailable/overloaded phrase in the
	// error message.
	RetryReasonMessageUnavailable = proto_models.InferenceRetryReason_INFERENCE_RETRY_REASON_MESSAGE_UNAVAILABLE
	// RetryReasonMessageTimeout marks a timeout/deadline phrase in the error message.
	RetryReasonMessageTimeout = proto_models.InferenceRetryReason_INFERENCE_RETRY_REASON_MESSAGE_TIMEOUT
	// RetryReasonSchemaGeneration marks a retryable structured-output schema failure.
	RetryReasonSchemaGeneration = proto_models.InferenceRetryReason_INFERENCE_RETRY_REASON_SCHEMA_GENERATION
	// RetryReasonNotRetryable marks a classified but terminal failure.
	RetryReasonNotRetryable = proto_models.InferenceRetryReason_INFERENCE_RETRY_REASON_NOT_RETRYABLE
	// RetryReasonStreamCommitted marks a post-commit streaming failure, which is always
	// terminal because a replay could duplicate already-delivered output.
	RetryReasonStreamCommitted = proto_models.InferenceRetryReason_INFERENCE_RETRY_REASON_STREAM_COMMITTED
)

// RetryDecision captures whether a failed attempt should be retried and its typed
// classification. HTTPStatus carries the numeric status for the HTTP-related reasons
// and is zero otherwise.
type RetryDecision struct {
	Retryable  bool
	Reason     RetryReason
	HTTPStatus int
}

// ShouldRetryFunc classifies a failed attempt error into a retry decision.
type ShouldRetryFunc func(err error) RetryDecision

// ProviderCallGateError means the application checkpoint rejected provider entry.
// It is always terminal for inference retries, even if its cause is transient.
// Applications may recover their owning workflow separately.
type ProviderCallGateError struct {
	Err error
}

// Error describes the rejected checkpoint and its cause.
func (e *ProviderCallGateError) Error() string {
	return fmt.Sprintf("provider call gate failed: %v", e.Err)
}

// Unwrap preserves the checkpoint cause for errors.Is and errors.As.
func (e *ProviderCallGateError) Unwrap() error { return e.Err }

// IsProviderCallGateError reports whether err contains a failed provider checkpoint.
func IsProviderCallGateError(err error) bool {
	_, ok := errors.AsType[*ProviderCallGateError](err)
	return ok
}

// HTTPStatusError represents an HTTP response status that can be classified for retry.
type HTTPStatusError struct {
	StatusCode int
	Message    string
}

// Error formats the HTTP status error for retry logs and caller-facing diagnostics.
func (e *HTTPStatusError) Error() string {
	if e == nil {
		return ""
	}
	if strings.TrimSpace(e.Message) == "" {
		return fmt.Sprintf("http status %d", e.StatusCode)
	}
	return fmt.Sprintf("http status %d: %s", e.StatusCode, e.Message)
}

// IsRetryableHTTPStatus reports whether an HTTP status should be retried by inference flows.
func IsRetryableHTTPStatus(status int) bool {
	switch status {
	case http.StatusRequestTimeout, http.StatusTooEarly, http.StatusTooManyRequests,
		http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

// ClassifyRetryableError classifies an error into retryable and non-retryable
// categories for inference calls. It inspects, in order: provider-call gate failures
// (terminal), context cancellation (terminal) and deadline (retryable), typed HTTP
// status errors, net.Error
// timeout/temporary signals, textual HTTP-status and rate-limit mentions, a set of
// provider keyword phrases, and structured-output schema-generation failures.
func ClassifyRetryableError(err error) RetryDecision {
	if err == nil {
		return RetryDecision{Retryable: false, Reason: RetryReasonUnspecified}
	}
	if IsProviderCallGateError(err) {
		return RetryDecision{Reason: RetryReasonNotRetryable}
	}
	if errors.Is(err, context.Canceled) {
		return RetryDecision{Retryable: false, Reason: RetryReasonContextCanceled}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return RetryDecision{Retryable: true, Reason: RetryReasonContextDeadlineExceeded}
	}

	if statusErr, ok := errors.AsType[*HTTPStatusError](err); ok {
		if IsRetryableHTTPStatus(statusErr.StatusCode) {
			return RetryDecision{Retryable: true, Reason: RetryReasonHTTPStatus, HTTPStatus: statusErr.StatusCode}
		}
		return RetryDecision{Retryable: false, Reason: RetryReasonHTTPStatusTerminal, HTTPStatus: statusErr.StatusCode}
	}
	// Adapter errors preserve SDK causes while exposing their authoritative HTTP
	// status structurally. Apply the same policy to HTTP and SSE envelopes.
	if statusErr, ok := errors.AsType[interface {
		error
		HTTPStatusCode() int
	}](err); ok {
		code := statusErr.HTTPStatusCode()
		if code >= 100 && code <= 599 {
			if IsRetryableHTTPStatus(code) {
				return RetryDecision{Retryable: true, Reason: RetryReasonHTTPStatus, HTTPStatus: code}
			}
			return RetryDecision{Reason: RetryReasonHTTPStatusTerminal, HTTPStatus: code}
		}
	}

	if netErr, ok := errors.AsType[net.Error](err); ok {
		if netErr.Timeout() {
			return RetryDecision{Retryable: true, Reason: RetryReasonNetworkTimeout}
		}
		if netErr.Temporary() {
			return RetryDecision{Retryable: true, Reason: RetryReasonNetworkTemporary}
		}
	}

	message := strings.ToLower(strings.TrimSpace(err.Error()))
	if message == "" {
		return RetryDecision{Retryable: false, Reason: RetryReasonUnspecified}
	}
	if statusCode, ok := retryableHTTPStatusMention(message); ok {
		return RetryDecision{Retryable: true, Reason: RetryReasonMessageHTTPStatus, HTTPStatus: statusCode}
	}
	if reason, ok := classifyRetryableKeyword(message); ok {
		return RetryDecision{Retryable: true, Reason: reason}
	}
	if isRetryableSchemaGenerationError(message) {
		return RetryDecision{Retryable: true, Reason: RetryReasonSchemaGeneration}
	}
	return RetryDecision{Retryable: false, Reason: RetryReasonNotRetryable}
}

// retryableKeywordReasons preserves the exact keyword precedence of the moved
// GradeThis classifier while mapping each phrase to a typed message-category reason.
// The first matching phrase wins, so the ordering here is load-bearing.
var retryableKeywordReasons = []struct {
	keyword string
	reason  RetryReason
}{
	{"rate limit", RetryReasonMessageRateLimit},
	{"too many requests", RetryReasonMessageRateLimit},
	{"resource exhausted", RetryReasonMessageRateLimit},
	{"resource has been exhausted", RetryReasonMessageRateLimit},
	{"resource_exhausted", RetryReasonMessageRateLimit},
	{"temporarily unavailable", RetryReasonMessageUnavailable},
	{"unavailable", RetryReasonMessageUnavailable},
	{"timeout", RetryReasonMessageTimeout},
	{"deadline exceeded", RetryReasonMessageTimeout},
	{"deadline expired", RetryReasonMessageTimeout},
	{"deadline_exceeded", RetryReasonMessageTimeout},
	{"server overloaded", RetryReasonMessageUnavailable},
}

func classifyRetryableKeyword(message string) (RetryReason, bool) {
	for _, entry := range retryableKeywordReasons {
		if strings.Contains(message, entry.keyword) {
			return entry.reason, true
		}
	}
	return RetryReasonUnspecified, false
}

// schemaGenerationFailureMarker identifies genkit structured-output failures, where
// the model returned content that did not conform to the requested response schema.
const schemaGenerationFailureMarker = "model failed to generate output matching expected schema"

// isRetryableSchemaGenerationError reports whether an error stems from a model
// producing output that failed structured-output schema validation. These are
// transient model-output glitches (malformed JSON, missing required fields,
// extra/unexpected properties) that frequently succeed on a fresh attempt, so they
// are treated as retryable rather than terminal.
func isRetryableSchemaGenerationError(message string) bool {
	return strings.Contains(message, schemaGenerationFailureMarker)
}

func retryableHTTPStatusMention(message string) (int, bool) {
	retryableStatuses := []int{
		http.StatusRequestTimeout,
		http.StatusTooEarly,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout,
	}
	for _, statusCode := range retryableStatuses {
		if strings.Contains(message, fmt.Sprintf("error %d", statusCode)) ||
			strings.Contains(message, fmt.Sprintf("http status %d", statusCode)) ||
			strings.Contains(message, fmt.Sprintf("status code %d", statusCode)) {
			return statusCode, true
		}
	}
	return 0, false
}
