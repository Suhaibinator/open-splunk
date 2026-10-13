package agentkit

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// ScopedReadOptions describes the shared authorization, validation and observation
// hooks for a scope-bounded read. Both result forms use this canonical value.
//
// S is the resolved scope type (e.g. the authorized post/assessment a tool reads
// within); A is the decoded argument type. Both are app-defined.
type ScopedReadOptions[S any, A any] struct {
	// Observer carries the optional metrics + tracing seams. The zero value is fine.
	Observer Observer
	// AgentType, ToolName, RendererKey label the invocation and any error result.
	AgentType   string
	ToolName    string
	RendererKey string
	// Invocation is the raw tool invocation from go-common.
	Invocation ports.AgentToolInvocation
	// Args is the already-decoded argument value. Decode it (via DecodeToolArgs)
	// before constructing the ScopedRead so a decode failure can be reported with
	// GenericErrorCodeInvalidArguments and its own metric before entering the harness.
	Args A

	// Validate optionally checks Args before any I/O. A non-nil error becomes an
	// in-band GenericErrorCodeInvalidArguments result (never an operational error).
	Validate func(A) error
	// AttachActor optionally derives the read context, typically attaching the
	// triggering user so downstream reads run under that user's authority. Nil
	// leaves the context unchanged.
	AttachActor func(context.Context, ports.AgentToolInvocation) (context.Context, error)
	// ResolveScope loads and authorizes the scope the tool reads within. Required.
	ResolveScope func(context.Context, ports.AgentToolInvocation) (S, error)
	// AgentTypeFromScope optionally refines the agent-type label once the scope is
	// known (some scopes carry the concrete registered agent type). A blank return
	// keeps AgentType.
	AgentTypeFromScope func(S) string
	// DomainVisible classifies actor/scope/read errors as model-visible (in-band)
	// rather than operational. ErrAgentToolActorUnavailable is always operational
	// because actor attachment is an authorization prerequisite. A *ToolError is
	// always in-band regardless. Nil treats every non-ToolError as operational.
	DomainVisible func(error) bool
}

// ScopedRead is a scope-bounded read whose payload is marshaled into a success
// result. RunScopedRead validates, attaches the actor, resolves and authorizes
// scope, then performs the read with exactly one observation.
type ScopedRead[S any, A any] struct {
	ScopedReadOptions[S, A]
	// Read performs the tool read against the resolved scope. Required.
	Read func(context.Context, S, A) (any, error)
}

// ScopedResultRead describes a scope-bounded read that returns a complete tool
// result, including ordered multipart content, rather than a JSON payload. Its
// hooks have the same ordering and error contracts as ScopedRead. It does not
// own transactions or workflow receipts.
type ScopedResultRead[S any, A any] struct {
	ScopedReadOptions[S, A]
	// Read must return a non-nil result on success. The result is passed through
	// unchanged, including in-band errors, text, embeds, and multipart content.
	Read func(context.Context, S, A) (*ports.AgentToolResult, error)
}

// RunScopedRead executes a ScopedRead. It always records exactly one metric
// observation and one span for the invocation. A model-visible failure (bad args,
// a *ToolError, or an error the DomainVisible predicate accepts) is returned as an
// in-band is_error result with a nil error; an operational failure is returned as a
// non-nil error with a nil result, matching the go-common executor contract.
func RunScopedRead[S any, A any](ctx context.Context, r ScopedRead[S, A]) (result *ports.AgentToolResult, err error) {
	if r.ResolveScope == nil {
		return nil, fmt.Errorf("agentkit: ScopedRead.ResolveScope is required")
	}
	if r.Read == nil {
		return nil, fmt.Errorf("agentkit: ScopedRead.Read is required")
	}

	return runScopedRead(ctx, r.ScopedReadOptions, r.Read, SuccessResult)
}

// RunScopedResultRead executes the same authorization and observation harness as
// RunScopedRead, without marshaling the returned result. A nil result with a nil
// error is an operational failure, even when DomainVisible accepts every error.
func RunScopedResultRead[S any, A any](ctx context.Context, r ScopedResultRead[S, A]) (*ports.AgentToolResult, error) {
	if r.ResolveScope == nil {
		return nil, fmt.Errorf("agentkit: ScopedResultRead.ResolveScope is required")
	}
	if r.Read == nil {
		return nil, fmt.Errorf("agentkit: ScopedResultRead.Read is required")
	}
	return runScopedRead(ctx, r.ScopedReadOptions, r.Read, func(result *ports.AgentToolResult) (*ports.AgentToolResult, error) {
		if result == nil {
			return nil, fmt.Errorf("agentkit: ScopedResultRead.Read returned a nil result without an error")
		}
		return result, nil
	})
}

// Keep final result conversion outside domain-error mapping: JSON encoding errors
// and invalid nil results are operational, not errors supplied by domain reads.
func runScopedRead[S any, A any, R any](ctx context.Context, r ScopedReadOptions[S, A], read func(context.Context, S, A) (R, error), finish func(R) (*ports.AgentToolResult, error)) (result *ports.AgentToolResult, err error) {
	started := time.Now()
	agentType := r.AgentType
	var span trace.Span
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nil
			err = ports.NewAgentToolPanicError(r.ToolName, recovered)
		}
		r.Observer.RecordMetric(agentType, r.ToolName, started, result, err)
		r.Observer.FinishSpan(span, result, err)
	}()
	ctx, span = r.Observer.StartSpan(ctx, agentType, r.ToolName, r.Invocation)

	if r.Validate != nil {
		if verr := r.Validate(r.Args); verr != nil {
			return ErrorResult(r.ToolName, r.RendererKey, GenericErrorCodeInvalidArguments, verr.Error()), nil
		}
	}

	readCtx := ctx
	if r.AttachActor != nil {
		readCtx, err = r.AttachActor(ctx, r.Invocation)
		if err != nil {
			// Actor attachment is an authorization prerequisite. Never let an app's
			// broad DomainVisible predicate downgrade a missing actor into an in-band
			// result that permits the execution lane to continue normally.
			if errors.Is(err, ports.ErrAgentToolActorUnavailable) {
				return nil, err
			}
			return ResultForError(r.ToolName, r.RendererKey, err, r.DomainVisible)
		}
	}

	var scope S
	scope, err = r.ResolveScope(readCtx, r.Invocation)
	if err != nil {
		return ResultForError(r.ToolName, r.RendererKey, err, r.DomainVisible)
	}
	if r.AgentTypeFromScope != nil {
		if refined := strings.TrimSpace(r.AgentTypeFromScope(scope)); refined != "" {
			agentType = refined
			if span != nil {
				span.SetAttributes(attribute.String("agent.type", agentType))
			}
		}
	}

	payload, err := read(readCtx, scope, r.Args)
	if err != nil {
		return ResultForError(r.ToolName, r.RendererKey, err, r.DomainVisible)
	}
	return finish(payload)
}
