package common_service

import (
	"context"
	"errors"

	common_persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
)

// ParkGraph records, in the application's parking transaction, that the owner-driven delegation
// graph rooted at rootSessionID waits for an external event, at revision, the GraphResult.Revision
// its drive read before the final check. It returns ErrGraphRevisionAdvanced when an event
// committed after that read: the graph may be drivable, and the application keeps driving it.
//
// Every event that may make a parked graph drivable (an approval resolution or expiry, an
// inference-unblock retarget, ResumeParent) advances the root's resume revision in its own
// transaction. After commit it calls the root type's OnGraphEvent when the root is parked; the
// application's recovery calls ListUnparkableGraphs for the events whose call was lost.
func (h *CommonServiceHandler[U]) ParkGraph(ctx context.Context, rootSessionID, revision uint64) error {
	if err := h.requireTransaction(ctx, "park_graph"); err != nil {
		return err
	}
	if rootSessionID == 0 || h.agentRegistry == nil {
		return serviceErrors.ErrInvalidRequest
	}
	root, err := h.commonPersistenceHandler.GetChatAgentSessionByID(ctx, rootSessionID)
	if err != nil {
		return mapGraphParkError(err)
	}
	if root == nil {
		return serviceErrors.ErrChatAgentSessionNotFound
	}
	def, ok := h.agentRegistry.GetAgentType(root.AgentType)
	if !ok || !def.IsWorkflowOwned() || !def.CanBeRoot() || root.IsDelegatedChild() {
		return serviceErrors.ErrInvalidRequest
	}
	err = h.commonPersistenceHandler.ParkChatAgentGraph(ctx, rootSessionID, revision)
	if errors.Is(err, common_persistence.ErrChatAgentGraphRevisionAdvanced) {
		h.commonPersistenceHandler.GetMetricsHandler().RecordChatAgentGraphParkRefused()
	}
	return mapGraphParkError(err)
}

// UnparkGraph clears the root's parked revision. Call it in the transaction that re-queues the
// graph's job, so a crash cannot leave the job idle with its graph no longer listed.
func (h *CommonServiceHandler[U]) UnparkGraph(ctx context.Context, rootSessionID uint64) error {
	if err := h.requireTransaction(ctx, "unpark_graph"); err != nil {
		return err
	}
	if rootSessionID == 0 {
		return serviceErrors.ErrInvalidRequest
	}
	return mapGraphParkError(h.commonPersistenceHandler.UnparkChatAgentGraph(ctx, rootSessionID))
}

// ListUnparkableGraphs pages, by ascending root session id after afterRootID, the unarchived parked
// roots whose resume revision advanced past the parked one: graphs an event resumed whose
// OnGraphEvent call may have been lost. The application calls it from its own job recovery;
// go-common keeps no delivery state.
func (h *CommonServiceHandler[U]) ListUnparkableGraphs(ctx context.Context, afterRootID uint64, limit int) ([]uint64, error) {
	roots, err := h.commonPersistenceHandler.ListUnparkableChatAgentGraphs(ctx, afterRootID, limit)
	return roots, mapGraphParkError(err)
}

// notifyChatAgentGraphEvent is persistence's post-commit fast path for a parked root whose resume
// revision advanced: it calls the root agent type's OnGraphEvent.
func (h *CommonServiceHandler[U]) notifyChatAgentGraphEvent(ctx context.Context, rootAgentType string, rootSessionID uint64) {
	if h.agentRegistry == nil {
		return
	}
	def, ok := h.agentRegistry.GetAgentType(rootAgentType)
	if !ok || !def.IsWorkflowOwned() || def.Workflow.OnGraphEvent == nil {
		return
	}
	def.Workflow.OnGraphEvent(ctx, rootSessionID)
}

func mapGraphParkError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, common_persistence.ErrChatAgentGraphRevisionAdvanced):
		return serviceErrors.ErrGraphRevisionAdvanced
	case errors.Is(err, common_persistence.ErrChatAgentSessionNotFound):
		return serviceErrors.ErrChatAgentSessionNotFound
	case errors.Is(err, common_persistence.ErrChatAgentSessionScopeInvalid):
		return serviceErrors.ErrInvalidRequest
	default:
		return serviceErrors.ErrDatabaseError
	}
}
