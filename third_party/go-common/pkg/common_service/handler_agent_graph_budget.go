package common_service

import (
	"context"

	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
)

// SetGraphBudget creates the budget pool of the delegation graph rooted at rootSessionID, or
// replaces its limits while keeping what the graph has used (a takeover never resets it). With a
// pool, every run in the graph draws on it, and wake and follow-up runs carry no library work
// ceilings. When a limit is used up, an interactive graph's run fails with a budget notice and its
// spawns and sends fail with ErrGraphBudgetExhausted while its parent wakes stay queued; an
// owner-driven run's drive returns the *common_persistence.GraphBudgetExhaustedError and the run
// stays drivable. Raising the pool drains the queued wakes. It joins the caller's transaction when
// there is one, locking the pool before the graph's channel, so a transaction that composes it
// with other graph mutations (ResumeParent, sends, creation) must call it first; ErrInvalidRequest
// for a zero limit or a session that is not a live graph root.
func (h *CommonServiceHandler[U]) SetGraphBudget(ctx context.Context, rootSessionID uint64, budget ports.GraphBudget) error {
	if h == nil || h.chatHandler == nil {
		return serviceErrors.ErrChatDisabled
	}
	return h.chatHandler.SetGraphBudget(ctx, rootSessionID, budget)
}
