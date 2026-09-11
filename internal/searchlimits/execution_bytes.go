package searchlimits

import "context"

type remainingExecutionBytesKey struct{}

// WithRemainingExecutionBytes carries a stage-local allocation ceiling without
// changing the immutable admitted policy. Unlike Policy, the remainder may be
// below the configured minimum, including zero; callers must fail closed.
func WithRemainingExecutionBytes(ctx context.Context, remaining uint64) context.Context {
	return context.WithValue(ctx, remainingExecutionBytesKey{}, remaining)
}

func RemainingExecutionBytes(ctx context.Context) (uint64, bool) {
	if ctx == nil {
		return 0, false
	}
	remaining, ok := ctx.Value(remainingExecutionBytesKey{}).(uint64)
	return remaining, ok
}

type remainingExecutionMemoryKey struct{}

// WithRemainingExecutionMemoryBytes bounds server working memory after the
// executor reserves concurrently live client-side result representations.
func WithRemainingExecutionMemoryBytes(ctx context.Context, remaining uint64) context.Context {
	return context.WithValue(ctx, remainingExecutionMemoryKey{}, remaining)
}

func RemainingExecutionMemoryBytes(ctx context.Context) (uint64, bool) {
	if ctx == nil {
		return 0, false
	}
	remaining, ok := ctx.Value(remainingExecutionMemoryKey{}).(uint64)
	return remaining, ok
}
