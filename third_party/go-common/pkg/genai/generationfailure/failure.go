// Package generationfailure preserves accounting for explicitly failed provider generations.
package generationfailure

import (
	"errors"

	"github.com/firebase/genkit/go/ai"
)

// Error retains diagnostic response metadata and usage; its content must never
// be accepted as a completed answer or dispatched as executable tools.
type Error struct {
	Response *ai.ModelResponse
	Cause    error
}

func (e *Error) Error() string { return e.Cause.Error() }
func (e *Error) Unwrap() error { return e.Cause }

// ResponseFromError recovers usage while preserving the error's failed disposition.
func ResponseFromError(err error) (*ai.ModelResponse, bool) {
	var failed *Error
	if errors.As(err, &failed) && failed.Response != nil {
		return failed.Response, true
	}
	return nil, false
}
