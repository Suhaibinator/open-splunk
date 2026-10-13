package ai

import (
	"context"
	"errors"
	"fmt"

	"github.com/Suhaibinator/go-common/pkg/genai/retryclassify"
)

// RecoverableGenerationError identifies a transient provider interruption after streaming
// commitment. Only generators that distinguish provider errors from callback/validation errors
// may emit it. Recovery requires durable cleanup by the owning runtime before another generation.
type RecoverableGenerationError struct {
	StepIndex uint32
	Err       error
}

func (e *RecoverableGenerationError) Error() string {
	return fmt.Sprintf("agent generation interrupted at step %d: %v", e.StepIndex, e.Err)
}
func (e *RecoverableGenerationError) Unwrap() error { return e.Err }

// interruptedProviderStream is private so downstream delivery and response validation cannot
// accidentally opt into recovery merely by returning a timeout-shaped error.
type interruptedProviderStream struct{ error }

func (e *interruptedProviderStream) Unwrap() error { return e.error }

func transientStreamProviderError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || retryclassify.IsProviderCallGateError(err) {
		return false
	}
	var classified interface{ Retryable() bool }
	if errors.As(err, &classified) {
		return classified.Retryable()
	}
	return retryclassify.ClassifyRetryableError(err).Retryable
}

func isRecoverableGenerationError(err error) bool {
	var interrupted *RecoverableGenerationError
	return errors.As(err, &interrupted)
}
