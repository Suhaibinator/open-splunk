package common_service

import (
	"context"
	"fmt"

	common_persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// RetireSummary is what RetireGraph leaves for after the caller's commit.
type RetireSummary struct {
	// CascadePending reports retirement work that remains once the caller commits: the root's own
	// active runs, or children not yet retired. A post-commit hook does it; the recovery side pass
	// finishes it if the process stops first.
	CascadePending bool
}

// RetireGraph retires the owner-driven graph rooted at rootSessionID. It requires the caller's
// database-backed, hook-capable transaction (ErrTransactionNotFound otherwise), which should also
// carry the application's accept or cancel: the root's archive, the durable cascade intent, commits
// atomically with it, and clears the root's parked_revision so a retired graph is never resumed.
// Only after the commit does a post-commit hook cancel the root's active runs
// (PARENT_WORKFLOW_TERMINATED) and retire its children through the archive cascade, in pages;
// cancellation expires pending approvals, advancing the root's resume_revision as any expiry does.
// A rollback leaves the root live and runs nothing. Idempotent. ErrInvalidRequest when the session is
// a delegated child or its agent type is not workflow-owned; ErrChatAgentSessionNotFound when absent.
func (h *CommonServiceHandler[U]) RetireGraph(ctx context.Context, rootSessionID uint64) (RetireSummary, error) {
	if h == nil || h.commonPersistenceHandler == nil {
		return RetireSummary{}, serviceErrors.ErrChatAgentSessionNotFound
	}
	if _, ok := common_persistence.DatabasePostCommitHookRegistrarFromContext(ctx); !ok {
		return RetireSummary{}, serviceErrors.ErrTransactionNotFound
	}
	root, err := h.loadAgentSessionForControl(ctx, rootSessionID)
	if err != nil {
		return RetireSummary{}, err
	}
	if root.IsDelegatedChild() {
		return RetireSummary{}, fmt.Errorf("%w: session %d is a delegated child, not a graph root", serviceErrors.ErrInvalidRequest, root.ID)
	}
	if h.agentRegistry == nil {
		return RetireSummary{}, serviceErrors.ErrInternalError
	}
	if def, ok := h.agentRegistry.GetAgentType(root.AgentType); !ok || !def.IsWorkflowOwned() {
		return RetireSummary{}, fmt.Errorf("%w: agent type %q is not workflow-owned", serviceErrors.ErrInvalidRequest, root.AgentType)
	}
	if root.Status != common_persistence_models.ChatAgentSessionStatusArchived {
		posted, err := h.commonPersistenceHandler.ArchiveChatAgentSession(ctx, root.ID)
		if err != nil {
			return RetireSummary{}, mapAgentSessionControlError(err)
		}
		h.publishAgentSessionArchived(ctx, root, posted)
	}
	// Inside the retiring transaction: does the root still have active runs or children to retire?
	active, err := h.commonPersistenceHandler.ListActiveChatAgentRunsBySession(ctx, root.ID)
	var children []*common_persistence_models.ChatAgentSession
	if err == nil && len(active) == 0 {
		children, err = h.commonPersistenceHandler.ListDelegatedChatAgentSessionsNeedingRetirementByParent(ctx, root.ID, 0, 1)
	}
	if err != nil {
		return RetireSummary{}, serviceErrors.ErrDatabaseError
	}
	pending := len(active) > 0 || len(children) > 0
	if pending {
		common_persistence.AddPostCommitHookFromContext(ctx, func(hookCtx context.Context) error {
			// The root's own active runs are cancelled, then its children retire through the archive
			// cascade. Each cancellation is its own transaction.
			h.runOwnedPostCommitHooks(hookCtx, []common_persistence.PostCommitHook{func(c context.Context) error {
				if err := h.cancelGraphRootRuns(c, root.ID); err != nil {
					return err
				}
				return h.retireDelegatedChildren(c, root.ID)
			}}, "retire_graph")
			return nil
		})
	}
	return RetireSummary{CascadePending: pending}, nil
}

// cancelGraphRootRuns cancels a retired owner-driven root's own active runs.
func (h *CommonServiceHandler[U]) cancelGraphRootRuns(ctx context.Context, rootID uint64) error {
	ctx = common_persistence.WithoutRequestTransaction[U](ctx)
	active, err := h.commonPersistenceHandler.ListActiveChatAgentRunsBySession(ctx, rootID)
	if err != nil {
		return serviceErrors.ErrDatabaseError
	}
	_, err = h.cancelDelegatedRuns(ctx, active, ports.AgentRunCancellation{
		Reason: proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_PARENT_WORKFLOW_TERMINATED,
	})
	return err
}
