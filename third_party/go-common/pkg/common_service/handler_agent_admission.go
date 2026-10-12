package common_service

import (
	"context"
	"errors"
	"fmt"

	commonpersistence "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	commonai "github.com/Suhaibinator/go-common/pkg/common_service/ai"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/genai/controlplane"
)

// ErrAgentProviderAdmission identifies application admission failures separately
// from provider failures. The owning workflow may recover after renewing its claim.
var ErrAgentProviderAdmission = errors.New("agent provider admission rejected")

// errAgentAttemptIdentityMissing rejects a provider attempt whose GenerationRetrier supplied no
// commonai.GenerationAttempt identity while the agent declares ProviderAttemptAdmission. It is
// reported through ErrAgentProviderAdmission like any other admission failure.
var errAgentAttemptIdentityMissing = errors.New("generation retrier supplied no attempt identity; a GenerationRetrier must pass a GenerationAttempt with InvocationID and a one-based Number to every attempt")

// agentAttemptAdmission decides whether one provider attempt may enter the provider. It receives
// the durable step and the retrier-minted attempt identity as explicit arguments.
type agentAttemptAdmission func(ctx context.Context, step commonai.GenerationStep, attempt commonai.GenerationAttempt) error

type agentAdmissionRetrier struct {
	inner commonai.GenerationRetrier
	admit agentAttemptAdmission
}

type agentAdmissionStreamRetrier struct {
	*agentAdmissionRetrier
	stream commonai.GenerationStreamRetrier
}

func newAgentAdmissionRetrier(inner commonai.GenerationRetrier, admit agentAttemptAdmission) commonai.GenerationRetrier {
	guarded := &agentAdmissionRetrier{inner: inner, admit: admit}
	if stream, ok := inner.(commonai.GenerationStreamRetrier); ok {
		return &agentAdmissionStreamRetrier{agentAdmissionRetrier: guarded, stream: stream}
	}
	return guarded
}

func (r *agentAdmissionRetrier) check(ctx context.Context, step commonai.GenerationStep, attempt commonai.GenerationAttempt) error {
	if err := r.admit(ctx, step, attempt); err != nil {
		// The control plane checks this type before any custom retry classifier.
		// In particular a database timeout here must never be retried as provider work.
		return &controlplane.ProviderCallGateError{Err: fmt.Errorf("%w: %w", ErrAgentProviderAdmission, err)}
	}
	return nil
}

func (r *agentAdmissionRetrier) Execute(ctx context.Context, call commonai.GenerationCallContext, step commonai.GenerationStep, fn commonai.GenerationFn) (*commonai.GenerationOutcome, error) {
	return r.inner.Execute(ctx, call, step, func(ctx context.Context, attempt commonai.GenerationAttempt) (*commonai.GenerationOutcome, error) {
		if err := r.check(ctx, step, attempt); err != nil {
			return nil, err
		}
		return fn(ctx, attempt)
	})
}

func (r *agentAdmissionStreamRetrier) ExecuteStream(ctx context.Context, call commonai.GenerationCallContext, step commonai.GenerationStep, fn commonai.StreamGenerationFn) (*commonai.GenerationOutcome, error) {
	return r.stream.ExecuteStream(ctx, call, step, func(ctx context.Context, attempt commonai.GenerationAttempt, commit func()) (*commonai.GenerationOutcome, error) {
		if err := r.check(ctx, step, attempt); err != nil {
			return nil, err
		}
		return fn(ctx, attempt, commit)
	})
}

// workflowResultReadyCallback adapts a workflow-owned agent's durable result check to the
// continuation runner's RunConfig.ResultReady, revalidating ownership before every call. It
// returns nil for interactive agents and for workflow agents without durable result completion.
func (r *agentRuntime[U]) workflowResultReadyCallback(runID uint64, agent ports.AgentDefinition) func(context.Context) (bool, error) {
	if !agent.IsWorkflowOwned() || agent.Workflow.ResultReady == nil {
		return nil
	}
	resultReady := agent.Workflow.ResultReady
	return func(ctx context.Context) (bool, error) {
		check, err := r.admitWorkflow(ctx, runID)
		if err != nil {
			return false, err
		}
		return resultReady(ctx, check)
	}
}

// runAgentDefinition resolves the registered agent definition behind a run's session so one
// drive can reuse it, with the run's OwnerCheck. ok is false when the run has no session, no
// registry is wired, or the session or agent type no longer resolves; admitWorkflow reports the
// last two as errors, so callers that admit the run need not.
func (r *agentRuntime[U]) runAgentDefinition(ctx context.Context, run *commonpersistence.ChatAgentRun) (ports.AgentDefinition, ports.OwnerCheck, bool, error) {
	if run.AgentSessionID == nil || r.agentRegistry == nil {
		return ports.AgentDefinition{}, ports.OwnerCheck{}, false, nil
	}
	session, err := r.persistence.GetChatAgentSessionByID(ctx, *run.AgentSessionID)
	if err != nil {
		return ports.AgentDefinition{}, ports.OwnerCheck{}, false, err
	}
	if session == nil {
		return ports.AgentDefinition{}, ports.OwnerCheck{}, false, nil
	}
	agent, ok := r.agentRegistry.GetAgentType(session.AgentType)
	return agent, ports.NewOwnerCheck(run.ID, session.ID, session.ParentAgentSessionID), ok, nil
}
