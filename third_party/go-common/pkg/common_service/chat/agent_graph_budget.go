package chat

import (
	"context"
	"errors"
	"fmt"
	"time"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
)

// SetGraphBudget creates the budget pool of the delegation graph rooted at rootSessionID, or
// replaces its limits while keeping what the graph used. Raising a used-up pool drains the wakes
// deferred while it was exhausted (children's follow-ups and the root's wake): the minted runs
// start after commit, or, for a workflow-owned root, are left for its owner to drive. It joins the caller's transaction when there
// is one. ErrInvalidRequest for a zero limit or a session that is not a live root of a delegating
// agent type.
func (h *Handler[U]) SetGraphBudget(ctx context.Context, rootSessionID uint64, budget ports.GraphBudget) error {
	if h.chatDisabled() {
		return serviceErrors.ErrChatDisabled
	}
	if rootSessionID == 0 || budget.MaxGenerations == 0 || budget.MaxToolCalls == 0 || budget.MaxActiveDuration < time.Millisecond {
		return fmt.Errorf("%w: every graph budget limit must be positive, and MaxActiveDuration at least one millisecond", serviceErrors.ErrInvalidRequest)
	}
	if h.agentRegistry == nil {
		return serviceErrors.ErrInternalError
	}
	root, err := h.persistenceHandler.GetChatAgentSessionByID(ctx, rootSessionID)
	if err != nil {
		if errors.Is(err, persistence.ErrChatAgentSessionNotFound) {
			return fmt.Errorf("%w: %w", serviceErrors.ErrInvalidRequest, err)
		}
		return serviceErrors.ErrDatabaseError
	}
	def, ok := h.agentRegistry.GetAgentType(root.AgentType)
	if !ok || !def.IsDelegating() {
		return fmt.Errorf("%w: a graph budget needs a root whose agent type delegates", serviceErrors.ErrInvalidRequest)
	}
	drained, err := h.persistenceHandler.SetChatAgentGraphBudget(ctx, rootSessionID, pm.ChatAgentGraphBudget{
		MaxGenerations: budget.MaxGenerations, MaxToolCalls: budget.MaxToolCalls,
		MaxActiveMs: uint64(budget.MaxActiveDuration.Milliseconds()),
	})
	if err != nil {
		if errors.Is(err, persistence.ErrGraphBudgetInvalid) {
			return fmt.Errorf("%w: %w", serviceErrors.ErrInvalidRequest, err)
		}
		return serviceErrors.ErrDatabaseError
	}
	// Delivered input is announced before the runs it started. A workflow root's children have its
	// driver, so its owner drives every drained run.
	for _, input := range drained.Inputs {
		h.PublishDeliveredAgentInputAfterCommit(ctx, input.Session.ID, input.Session.ChannelID, input.Session.ParentMessageID, input.Delivery.MessageIDs())
	}
	for _, run := range drained.Runs {
		h.startAgentRunAfterCommit(ctx, root.AgentType, run.ID, run.ChannelID, run.ParentMessageID)
	}
	return nil
}

// checkGraphBudget refuses new work in the graph rooted at rootSessionID once a limit of its
// budget pool is used up.
func (h *Handler[U]) checkGraphBudget(ctx context.Context, rootSessionID uint64) error {
	if err := h.persistenceHandler.CheckChatAgentGraphBudget(ctx, rootSessionID); err != nil {
		if errors.Is(err, persistence.ErrGraphBudgetExhausted) {
			return fmt.Errorf("%w: %w", serviceErrors.ErrGraphBudgetExhausted, err)
		}
		return serviceErrors.ErrDatabaseError
	}
	return nil
}
