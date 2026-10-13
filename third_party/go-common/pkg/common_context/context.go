// Package common_context provides centralized context management for go-common.
// It implements a single immutable context state, stored under one private key,
// that holds every go-common request-scoped value outside SRouter's own carrier:
// the workflow-owned agent run ID, the run-drive lease, the durable tool
// invocation, delegation origin authority, and the inference attempt actor.
//
// The package mirrors SRouter's scontext conventions: WithX installs a value,
// GetX returns (value, bool), and ClearX removes it. Each value has its own
// presence bit, so an explicitly stored zero value stays distinguishable from an
// absent one.
//
// Unlike scontext, which mutates one shared wrapper in place under a lock, this
// package never mutates published state. Every With* and Clear* call copies the
// state, changes the value and its presence bit, and attaches the copy to a
// child context. Parent and sibling contexts therefore retain their values
// without a copy step, and readers need no lock. This is deliberate: go-common
// values such as the drive lease cross goroutine boundaries (heartbeats,
// transcript writers) and must never change underneath a holder.
//
// This package is a stdlib-only leaf. Persistence, ports, the inference control
// plane, and the service layer all sit above it and expose thin adapters.
package common_context

import (
	"context"
	"encoding/json/jsontext"
	"strings"
)

// commonContextKey is a private type for the context key to avoid collisions.
type commonContextKey struct{}

// presenceBits records which values the state carries.
type presenceBits uint8

const (
	presentWorkflowAgentRun presenceBits = 1 << iota
	presentChatAgentRunDriveLease
	presentAgentToolInvocation
	presentAttemptActorUserID
	presentChatAgentDelegationOrigin
)

// commonContext holds all values that go-common adds to contexts.
//
// A published commonContext is immutable. It is reached only through the
// pointer attached by attach, and no code writes through that pointer
// afterwards; updates copy the struct instead. Larger values are held behind
// pointers to values that are likewise never mutated after being stored, which
// keeps each copy small. All fields are private. The zero value is ready to use
// and carries no values.
type commonContext struct {
	workflowAgentRunID uint64
	attemptActorUserID uint64

	chatAgentRunDriveLease    ChatAgentRunDriveLease
	chatAgentDelegationOrigin ChatAgentRunDriveLease
	agentToolInvocation       *AgentToolInvocation

	// presence distinguishes absent values from explicitly stored zero values.
	presence presenceBits
}

// ChatAgentRunDriveLease is the authority carried by one runtime drive. Persistence writes use it
// as a fencing token; possession of a once-valid token is insufficient after expiry or takeover.
type ChatAgentRunDriveLease struct {
	RunID uint64
	Owner string
}

// WithChatAgentDelegationOrigin preserves a delegation's originating run authority
// independently of the destination writer's drive lease. An empty owner means
// a durable tool executor without a runtime drive lease; run lifecycle still fences it.
func WithChatAgentDelegationOrigin(ctx context.Context, runID uint64, owner string) context.Context {
	if runID == 0 {
		return ctx
	}
	ctx, next := copyCommonContext(ctx)
	next.chatAgentDelegationOrigin = ChatAgentRunDriveLease{RunID: runID, Owner: strings.TrimSpace(owner)}
	next.presence |= presentChatAgentDelegationOrigin
	return attach(ctx, &next)
}

// GetChatAgentDelegationOrigin returns the run authority retained for a delegation.
func GetChatAgentDelegationOrigin(ctx context.Context) (ChatAgentRunDriveLease, bool) {
	cc := getCommonContext(ctx)
	if cc == nil || cc.presence&presentChatAgentDelegationOrigin == 0 {
		return ChatAgentRunDriveLease{}, false
	}
	return cc.chatAgentDelegationOrigin, true
}

// AgentToolInvocation is the input handed to an AgentToolExecutor when a persisted,
// authorized tool call is executed. Genkit callbacks retrieve the same invocation
// from their context with ports.AgentToolInvocationFromContext.
type AgentToolInvocation struct {
	ToolName          string
	ToolCallID        string
	ToolCallMessageID uint64
	AgentRunID        uint64
	AgentSessionID    uint64
	// AgentSessionConfigVersion is the run's captured config snapshot version.
	// App tools can use it with AgentSessionID when they need snapshot-scoped reads.
	AgentSessionConfigVersion uint64
	ParentMessageID           uint64
	ChannelID                 uint64
	// TriggeringUserID has the same type as common_service_models.UserIdType.
	TriggeringUserID uint64
	// BotUserID is the bot that owns the run, so library tools can act as that bot instead of
	// the human who triggered the run.
	BotUserID uint64
	// OrganizationID has the same type as common_service_models.OrganizationIdType.
	OrganizationID int64
	// AgentType is the owning session's agent type; empty for runs with no first-class session.
	AgentType string

	ArgumentsJSON jsontext.Value
	RendererKey   string
	// Scope is the normalized scope the run's session was created with as a delegated child
	// (ports.ChildScopePolicy), so a tool can hold the child to it. Nil when the session has none.
	Scope jsontext.Value

	// IdempotencyKey is the stable key apps must pass to downstream systems when
	// the tool has side effects. go-common may retry after a worker lease expires.
	IdempotencyKey string
}

// getCommonContext retrieves the state from ctx, or nil when ctx carries none.
// It never allocates. A nil ctx carries no state.
func getCommonContext(ctx context.Context) *commonContext {
	if ctx == nil {
		return nil
	}
	cc, _ := ctx.Value(commonContextKey{}).(*commonContext)
	return cc
}

// copyCommonContext returns a private copy of the state carried by ctx (the zero
// value when absent) and a non-nil ctx to derive from.
func copyCommonContext(ctx context.Context) (context.Context, commonContext) {
	if ctx == nil {
		ctx = context.Background()
	}
	if cc := getCommonContext(ctx); cc != nil {
		return ctx, *cc
	}
	return ctx, commonContext{}
}

// attach publishes next as the state of a child of ctx. next must not be written afterwards.
func attach(ctx context.Context, next *commonContext) context.Context {
	return context.WithValue(ctx, commonContextKey{}, next)
}

// WithWorkflowAgentRun adds the workflow-owned agent run ID to the context.
// A zero ID is stored as present and shadows any parent value.
// It returns a child context; ctx and its other children are unchanged.
func WithWorkflowAgentRun(ctx context.Context, runID uint64) context.Context {
	ctx, next := copyCommonContext(ctx)
	next.workflowAgentRunID = runID
	next.presence |= presentWorkflowAgentRun
	return attach(ctx, &next)
}

// GetWorkflowAgentRun retrieves the workflow-owned agent run ID from the context.
// It returns the ID and a boolean indicating whether one was stored, including zero.
func GetWorkflowAgentRun(ctx context.Context) (uint64, bool) {
	cc := getCommonContext(ctx)
	if cc == nil || cc.presence&presentWorkflowAgentRun == 0 {
		return 0, false
	}
	return cc.workflowAgentRunID, true
}

// WithChatAgentRunDriveLease adds the run-drive lease, run ID and owner together, to the context.
// The owner is trimmed. A zero run ID or blank owner is not a lease: it returns ctx unchanged
// without creating a wrapper, so any parent lease stays in effect.
// Otherwise it returns a child context; ctx and its other children are unchanged.
func WithChatAgentRunDriveLease(ctx context.Context, runID uint64, owner string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	owner = strings.TrimSpace(owner)
	if runID == 0 || owner == "" {
		return ctx
	}
	ctx, next := copyCommonContext(ctx)
	next.chatAgentRunDriveLease = ChatAgentRunDriveLease{RunID: runID, Owner: owner}
	next.presence |= presentChatAgentRunDriveLease
	return attach(ctx, &next)
}

// GetChatAgentRunDriveLease retrieves the run-drive lease from the context.
// It returns the lease and a boolean indicating whether one was found.
func GetChatAgentRunDriveLease(ctx context.Context) (ChatAgentRunDriveLease, bool) {
	cc := getCommonContext(ctx)
	if cc == nil || cc.presence&presentChatAgentRunDriveLease == 0 {
		return ChatAgentRunDriveLease{}, false
	}
	return cc.chatAgentRunDriveLease, true
}

// WithAgentToolInvocation adds the durable tool invocation to the context.
// ArgumentsJSON and Scope are cloned, so the caller keeps ownership of their bytes.
// It returns a child context; ctx and its other children are unchanged.
func WithAgentToolInvocation(ctx context.Context, invocation AgentToolInvocation) context.Context {
	ctx, next := copyCommonContext(ctx)
	invocation.ArgumentsJSON = invocation.ArgumentsJSON.Clone()
	invocation.Scope = invocation.Scope.Clone()
	next.agentToolInvocation = &invocation
	next.presence |= presentAgentToolInvocation
	return attach(ctx, &next)
}

// GetAgentToolInvocation retrieves the durable tool invocation from the context.
// It returns the invocation and a boolean indicating whether one was found. The returned
// ArgumentsJSON and Scope are clones the caller may modify without changing the stored value.
func GetAgentToolInvocation(ctx context.Context) (AgentToolInvocation, bool) {
	cc := getCommonContext(ctx)
	if cc == nil || cc.presence&presentAgentToolInvocation == 0 {
		return AgentToolInvocation{}, false
	}
	invocation := *cc.agentToolInvocation
	invocation.ArgumentsJSON = invocation.ArgumentsJSON.Clone()
	invocation.Scope = invocation.Scope.Clone()
	return invocation, true
}

// WithAttemptActorUserID adds trusted durable actor attribution for inference attempts.
// Zero is stored as present and records an unattributed attempt.
// It returns a child context; ctx and its other children are unchanged.
func WithAttemptActorUserID(ctx context.Context, actorUserID uint64) context.Context {
	ctx, next := copyCommonContext(ctx)
	next.attemptActorUserID = actorUserID
	next.presence |= presentAttemptActorUserID
	return attach(ctx, &next)
}

// GetAttemptActorUserID retrieves the attempt actor from the context.
// It returns the user ID and a boolean indicating whether one was stored, including zero.
func GetAttemptActorUserID(ctx context.Context) (uint64, bool) {
	cc := getCommonContext(ctx)
	if cc == nil || cc.presence&presentAttemptActorUserID == 0 {
		return 0, false
	}
	return cc.attemptActorUserID, true
}
