package ports

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// ErrAgentToolPanicked classifies an application or Genkit tool callback panic as a
// terminal operational execution failure. Panics are code defects, not transient
// transport failures, so callers must not automatically retry them.
var ErrAgentToolPanicked = errors.New("agent tool panicked")

// ErrAgentToolActorUnavailable classifies a tool invocation whose persisted
// triggering actor cannot be attached exactly. Running without that actor could
// inherit ambient authority, so this is a terminal operational failure rather
// than a model-visible domain error or a retryable transport failure.
var ErrAgentToolActorUnavailable = errors.New("agent tool actor unavailable")

// AgentToolActorAttacher derives a tool execution context that carries exactly the
// durable triggering human. Implementations must remove or replace any ambient
// actor before installing userID and must fail when that actor cannot be resolved;
// returning the original context can let a Genkit tool inherit worker or request
// authority unrelated to the persisted run.
type AgentToolActorAttacher func(
	ctx context.Context,
	userID common_service_models.UserIdType,
) (context.Context, error)

// AgentToolPanicError is the sanitized operational error produced when tool code
// panics. PanicType and Stack are retained for internal diagnostics; the recovered
// value itself is deliberately not retained or rendered because it may contain
// secrets or implement unsafe formatting methods.
type AgentToolPanicError struct {
	ToolName  string
	PanicType string
	Stack     []byte
}

// NewAgentToolPanicError captures a sanitized description and stack for a recovered
// tool panic.
func NewAgentToolPanicError(toolName string, recovered any) *AgentToolPanicError {
	return &AgentToolPanicError{
		ToolName:  toolName,
		PanicType: fmt.Sprintf("%T", recovered),
		Stack:     debug.Stack(),
	}
}

func (e *AgentToolPanicError) Error() string {
	if e == nil || e.ToolName == "" {
		return ErrAgentToolPanicked.Error()
	}
	return fmt.Sprintf("%s: %q", ErrAgentToolPanicked, e.ToolName)
}

// Unwrap makes errors.Is(err, ErrAgentToolPanicked) work through the normal
// operational-error wrapping layers.
func (e *AgentToolPanicError) Unwrap() error { return ErrAgentToolPanicked }

// ErrToolExecutionAttemptsExceeded means a retryable tool execution exhausted the registered
// tool's MaxExecutionAttempts budget. Durable execution lanes must treat it as terminal.
var ErrToolExecutionAttemptsExceeded = errors.New("tool executor: maximum execution attempts exceeded")

// AgentToolExecutionError classifies an operational failure returned by an
// AgentToolExecutor. Unknown executor errors remain retryable for compatibility;
// apps should wrap deterministic failures (for example invalid durable state or a
// downstream constraint violation) with CanRetry=false so the owning agent run
// fails immediately instead of spending the rest of its retry budget.
//
// A model-visible tool/domain failure is not an AgentToolExecutionError. Return an
// AgentToolResult with IsError=true and a nil error for that case.
type AgentToolExecutionError struct {
	Cause    error
	CanRetry bool
}

func (e *AgentToolExecutionError) Error() string {
	if e == nil || e.Cause == nil {
		return "agent tool execution failed"
	}
	return e.Cause.Error()
}

func (e *AgentToolExecutionError) Unwrap() error   { return e.Cause }
func (e *AgentToolExecutionError) Retryable() bool { return e != nil && e.CanRetry }

// AgentToolExecutionRetryable returns an app executor error's retry classification.
// Unknown errors remain retryable for backwards compatibility. The durable execution-attempt
// ceiling still bounds those retries.
func AgentToolExecutionRetryable(err error) bool {
	if err == nil {
		return false
	}
	if classified, ok := errors.AsType[interface {
		error
		Retryable() bool
	}](err); ok {
		return classified.Retryable()
	}
	return true
}

// ClaimedToolCall is the input to ExecuteClaimedToolCall: a flat, persistence-free
// description of one approved/claimed TOOL_CALL that is ready to execute. It is the
// seam between the durable approved-work queue (which scans, leases, records results,
// and owns all ChatAgentRun status transitions) and the pure tool executor (which only
// runs the tool and returns its result).
//
// The flat shape is deliberate. The executor must not depend on persistence side
// effects, on the service-layer ChatMessage model, or on lazy-loaded associations: a
// caller constructs a ClaimedToolCall from a claimed persistence row plus the run's
// captured config version, and the executor runs from those fields alone. This keeps
// the executor unit-testable from a literal and reusable by both the inline
// continuation loop (non-approval tools) and the approved-work queue (approved tools).
type ClaimedToolCall struct {
	// ChannelID is the channel that owns the run and the tool call.
	ChannelID       uint64
	ParentMessageID uint64
	// AgentRunID is the durable run this tool call belongs to.
	AgentRunID uint64
	// AgentSessionID is the agent session whose captured config snapshot governs
	// this run. Zero for legacy/manual runs with no first-class session.
	AgentSessionID uint64
	// AgentSessionConfigVersion is the config snapshot version captured at run start.
	// The executor verifies the tool was enabled in THIS version, not the session's
	// current mutable config. Zero when there is no session-scoped snapshot.
	AgentSessionConfigVersion uint64

	// BotUserID is the registered bot user that owns the run.
	BotUserID uint64
	// OrganizationID is the run's immutable organization snapshot.
	OrganizationID common_service_models.OrganizationIdType
	// AgentType is the owning session's agent type. Empty for legacy/manual runs with no
	// first-class session.
	AgentType string
	// Scope is the owning session's delegated-child scope (AgentToolInvocation.Scope); nil when it
	// has none.
	Scope jsontext.Value
	// TriggeringUserID is the required, non-zero human actor whose message/event
	// caused the run. Passed through to the tool invocation for audit and app-side
	// authorization.
	TriggeringUserID common_service_models.UserIdType

	// ToolName is the registered tool to execute. It must match a tool enabled in the
	// captured session config snapshot and registered in the AgentToolRegistry.
	ToolName string
	// ToolCallID is the internal call reference (== ToolCallPayload.tool_call_id).
	ToolCallID string
	// ToolCallMessageID is the persisted TOOL_CALL message id. Used as a stable
	// idempotency key for tool side effects when IdempotencyKey is not supplied.
	ToolCallMessageID uint64
	// ExecutionAttemptCount is the one-based durable execution claim count, including
	// the current claim. The zero value is accepted for legacy/direct callers and is
	// treated as the first attempt by the executor.
	ExecutionAttemptCount uint32
	// ApprovalStatus is the claimed row's approval status. The executor refuses to run a
	// tool the captured snapshot marks approval-required unless this is APPROVED, so a row
	// that reached the work queue without a card can never execute unapproved. The zero
	// value (UNSPECIFIED) reads as "no approval on this row", which is what the inline
	// continuation path carries: it only ever executes non-approval tools.
	ApprovalStatus proto_models.ToolCallApprovalStatus

	// ArgumentsJSON is the JSON-encoded tool arguments decoded from ToolCallPayload.
	// The executor hands these to the tool unchanged; the tool validates them.
	ArgumentsJSON []byte

	// IdempotencyKey is the stable key apps must pass to downstream systems for
	// side-effecting tools. Empty means the executor derives one from the globally
	// unique ToolCallMessageID (or a run-scoped call reference for isolated legacy
	// callers that do not provide the durable message id).
	IdempotencyKey string
}

// ToolCallExecutor runs one claimed tool call and returns its result, without any
// persistence, leasing, or run-state writes. The same executor is used by the inline
// continuation loop for non-approval tools and by the approved-work queue for approved
// tools, so there is exactly one execution path (DRY).
//
// The (result, retryable, err) triple separates a tool's domain outcome from an
// operational failure:
//
//   - A tool DOMAIN error the model should see is NOT an execution failure: return
//     (&AgentToolResult{IsError: true, ...}, false, nil). The caller records it as a
//     normal is_error TOOL_RESULT and the run continues.
//   - An OPERATIONAL failure (tool panic, registry miss, tool not enabled in the
//     captured snapshot, transport/provider error) returns a non-nil err. retryable
//     reports whether the failure is transient: true means the work item can be
//     reclaimed and retried; false means it is terminal.
//
// On a non-nil err the *AgentToolResult is nil; callers must not record a TOOL_RESULT
// from a failed execution.
type ToolCallExecutor interface {
	ExecuteClaimedToolCall(ctx context.Context, claimed ClaimedToolCall) (result *AgentToolResult, retryable bool, err error)
}

// ExecuteClaimedToolCallSafely invokes an injected ToolCallExecutor while enforcing
// the port's panic contract. Production and custom executors alike surface a panic as
// a terminal operational error instead of unwinding through a caller goroutine.
func ExecuteClaimedToolCallSafely(
	ctx context.Context,
	executor ToolCallExecutor,
	claimed ClaimedToolCall,
) (result *AgentToolResult, retryable bool, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nil
			retryable = false
			err = NewAgentToolPanicError(claimed.ToolName, recovered)
		}
	}()
	return executor.ExecuteClaimedToolCall(ctx, claimed)
}
