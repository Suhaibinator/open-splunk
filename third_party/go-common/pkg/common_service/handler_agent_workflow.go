package common_service

import (
	"context"
	"errors"
	"fmt"
	"time"

	common_persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	commonai "github.com/Suhaibinator/go-common/pkg/common_service/ai"
	chatpkg "github.com/Suhaibinator/go-common/pkg/common_service/chat"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/genai/retryclassify"
	genkitai "github.com/firebase/genkit/go/ai"
)

var ErrWorkflowAgentRequiresOwner = errors.New("workflow agent requires its owning worker")

// RunWorkflowAgentTurn resumes the existing transcript under caller cancellation.
// The registered validator must independently verify durable domain ownership.
// In a graph with a budget pool, a used-up pool returns a *common_persistence.GraphBudgetExhaustedError
// naming the limit and leaves the run drivable.
func (h *CommonServiceHandler[U]) RunWorkflowAgentTurn(ctx context.Context, runID uint64) error {
	return h.runWorkflowAgentTurn(ctx, runID, nil, 0)
}

// runWorkflowAgentTurn is RunWorkflowAgentTurn for a graph drive: shutdown, when non-nil,
// interrupts the drive's abandonment cleanup like the service root's shutdown does, and graphShare
// caps the drive's active time at its share of the graph budget pool (zero: the whole remaining
// pool).
func (h *CommonServiceHandler[U]) runWorkflowAgentTurn(ctx context.Context, runID uint64, shutdown <-chan struct{}, graphShare time.Duration) error {
	if h == nil || h.agentRuntime == nil {
		return commonai.ErrContinuationNotInitialized
	}
	ctx = ports.WithWorkflowAgentRun(common_persistence.WithoutRequestTransaction[U](ctx), runID)
	var recoverTools workflowToolRecovery
	if recoverer, ok := h.chatHandler.(interface {
		RecoverWorkflowAgentTools(context.Context, uint64) error
	}); ok {
		recoverTools = func(c context.Context) error { return recoverer.RecoverWorkflowAgentTools(c, runID) }
	}
	return h.agentRuntime.driveTurn(ctx, runID, recoverTools, shutdown, graphShare)
}

// admitWorkflow admits work on runID: a no-op for an interactive agent's run, otherwise the run's
// owner drive must be the caller and the application's validator must accept it. It returns the
// run's OwnerCheck (zero when no check applied).
func (r *agentRuntime[U]) admitWorkflow(ctx context.Context, runID uint64) (ports.OwnerCheck, error) {
	if r.agentRegistry == nil {
		return ports.OwnerCheck{}, nil
	}
	run, err := r.persistence.GetChatAgentRunByID(ctx, runID)
	if err != nil {
		return ports.OwnerCheck{}, err
	}
	if run == nil {
		return ports.OwnerCheck{}, fmt.Errorf("agent run %d not found", runID)
	}
	if run.AgentSessionID == nil {
		return ports.OwnerCheck{}, nil
	}
	session, err := r.persistence.GetChatAgentSessionByID(ctx, *run.AgentSessionID)
	if err != nil {
		return ports.OwnerCheck{}, err
	}
	if session == nil {
		return ports.OwnerCheck{}, fmt.Errorf("agent session not found")
	}
	def, ok := r.agentRegistry.GetAgentType(session.AgentType)
	if !ok {
		return ports.OwnerCheck{}, fmt.Errorf("agent type %q unavailable", session.AgentType)
	}
	if !def.IsWorkflowOwned() {
		return ports.OwnerCheck{}, nil
	}
	if ports.WorkflowAgentRunFromContext(ctx) != runID {
		return ports.OwnerCheck{}, ErrWorkflowAgentRequiresOwner
	}
	check := ports.NewOwnerCheck(runID, session.ID, session.ParentAgentSessionID)
	return check, chatpkg.ValidateWorkflowGraphOwner(ctx, r.persistence, r.agentRegistry, def, check)
}

type workflowGenerator[U common_service_models.UserLike] struct {
	runtime *agentRuntime[U]
	runID   uint64
	inner   commonai.Generator
}

func (g *workflowGenerator[U]) Generate(ctx context.Context, step commonai.GenerationStep, h []*genkitai.Message, t []ports.AgentToolDefinition) (*commonai.GenerationOutcome, error) {
	if _, err := g.runtime.admitWorkflow(ctx, g.runID); err != nil {
		return nil, &retryclassify.ProviderCallGateError{Err: fmt.Errorf("%w: %w", ErrAgentProviderAdmission, err)}
	}
	return g.inner.Generate(ctx, step, h, t)
}

type workflowStreamingGenerator[U common_service_models.UserLike] struct {
	*workflowGenerator[U]
	stream commonai.StreamingGenerator
}

func (g *workflowStreamingGenerator[U]) GenerateStream(ctx context.Context, step commonai.GenerationStep, h []*genkitai.Message, t []ports.AgentToolDefinition, delta func(string) error) (*commonai.GenerationOutcome, error) {
	if _, err := g.runtime.admitWorkflow(ctx, g.runID); err != nil {
		return nil, &retryclassify.ProviderCallGateError{Err: fmt.Errorf("%w: %w", ErrAgentProviderAdmission, err)}
	}
	return g.stream.GenerateStream(ctx, step, h, t, delta)
}
func (r *agentRuntime[U]) guardWorkflowGenerator(ctx context.Context, id uint64, g commonai.Generator) commonai.Generator {
	if ports.WorkflowAgentRunFromContext(ctx) != id {
		return g
	}
	guarded := &workflowGenerator[U]{r, id, g}
	if stream, ok := g.(commonai.StreamingGenerator); ok {
		return &workflowStreamingGenerator[U]{guarded, stream}
	}
	return guarded
}

type workflowToolExecutor[U common_service_models.UserLike] struct {
	handler *CommonServiceHandler[U]
	inner   ports.ToolCallExecutor
}

func (e *workflowToolExecutor[U]) ExecuteClaimedToolCall(ctx context.Context, c ports.ClaimedToolCall) (*ports.AgentToolResult, bool, error) {
	if e.handler.agentRuntime != nil {
		if _, err := e.handler.agentRuntime.admitWorkflow(ctx, c.AgentRunID); err != nil {
			return nil, true, err
		}
	}
	return e.inner.ExecuteClaimedToolCall(ctx, c)
}
