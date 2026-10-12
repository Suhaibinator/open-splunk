package decisions

import (
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrInvalidConfig classifies client construction or initialization errors.
	ErrInvalidConfig = errors.New("decisions: invalid client configuration")
	// ErrInvalidRequest classifies errors detected before a request is sent.
	ErrInvalidRequest = errors.New("decisions: invalid request")
	// ErrInvalidResponse classifies malformed or semantically invalid successful
	// responses, and response bodies exceeding the client's size limit.
	ErrInvalidResponse = errors.New("decisions: invalid response")
)

// ProviderError is optional structured diagnostic information returned by the
// Decisions API. Code is the provider's raw JSON value and may be absent.
type ProviderError struct {
	Code    jsontext.Value `json:"code,omitempty"`
	Message string         `json:"message,omitempty"`
}

// HTTPError reports a non-2xx response from the Decisions API. Callers can use
// errors.As to inspect StatusCode and decide how to handle the failure.
// ProviderError and Details are optional diagnostics: ProviderError is nil when
// the body has no recognized error envelope, while Details contains a copy of
// the raw response body even when it is not valid JSON. RetryAfter is zero when
// the header is absent, invalid, or names a time in the past.
type HTTPError struct {
	StatusCode    int
	ProviderError *ProviderError
	RetryAfter    time.Duration
	Details       jsontext.Value
}

func (e *HTTPError) Error() string {
	if e == nil {
		return "decisions: HTTP error"
	}
	if e.ProviderError != nil && e.ProviderError.Message != "" {
		return fmt.Sprintf("decisions: HTTP status %d: %s", e.StatusCode, e.ProviderError.Message)
	}
	return fmt.Sprintf("decisions: HTTP status %d", e.StatusCode)
}
