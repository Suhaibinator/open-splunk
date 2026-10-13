package ports

import (
	"context"

	"github.com/Suhaibinator/go-common/pkg/common_context"
)

// WithAgentToolInvocation returns a child context carrying the complete persisted
// invocation for a Genkit tool callback.
//
// Genkit's callback signature exposes only a context and decoded tool arguments.
// Side-effecting Genkit tools must retrieve this invocation with
// AgentToolInvocationFromContext and pass IdempotencyKey to downstream systems.
// The key is stable across durable retries and lease reclaims of the same tool call.
//
// The invocation's mutable JSON bytes are copied before they are stored.
func WithAgentToolInvocation(ctx context.Context, invocation AgentToolInvocation) context.Context {
	return common_context.WithAgentToolInvocation(ctx, invocation)
}

// AgentToolInvocationFromContext returns the complete persisted invocation attached
// by go-common before dispatching a Genkit tool.
//
// The boolean is false outside a go-common Genkit tool dispatch. A side-effecting
// Genkit callback must treat a missing invocation or empty IdempotencyKey as unsafe
// and fail without performing the side effect.
//
// The returned invocation owns its ArgumentsJSON bytes and may be modified by the
// caller without changing the value retained in the context.
func AgentToolInvocationFromContext(ctx context.Context) (AgentToolInvocation, bool) {
	return common_context.GetAgentToolInvocation(ctx)
}
