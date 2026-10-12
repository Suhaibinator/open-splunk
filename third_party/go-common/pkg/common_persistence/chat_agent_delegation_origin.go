package common_persistence

import (
	"context"
	"slices"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_context"
	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
)

// ChatAgentDelegationOriginStore is the shared run fencing seam. All operations
// use the caller's transaction and lock the originating run before channel/session rows.
type ChatAgentDelegationOriginStore interface {
	LockChatAgentRunForUpdate(context.Context, uint64) error
	GetChatAgentRunByID(context.Context, uint64) (*pm.ChatAgentRun, error)
	AssertChatAgentRunDriveLease(context.Context, uint64) error
}

// LockChatAgentDelegationOrigin serializes a new delegated mutation against
// originating-run cancellation, expiry/takeover, and terminalization.
// Lifecycle validation is separate so committed idempotent outcomes can replay first.
func LockChatAgentDelegationOrigin(ctx context.Context, store ChatAgentDelegationOriginStore, runID uint64) (*pm.ChatAgentRun, error) {
	if tx, ok := scontext.GetTransaction(ctx); !ok || tx == nil {
		return nil, ErrChatAgentSessionTransactionRequired
	}
	if err := store.LockChatAgentRunForUpdate(ctx, runID); err != nil {
		return nil, err
	}
	return store.GetChatAgentRunByID(ctx, runID)
}

// ValidateChatAgentDelegationOrigin validates the locked run immediately before
// a fresh write. A preserved source lease never grants destination writer authority.
func ValidateChatAgentDelegationOrigin(ctx context.Context, store ChatAgentDelegationOriginStore, run *pm.ChatAgentRun, sessionID uint64) error {
	if run == nil || run.ID == 0 || sessionID == 0 || common_util.Deref(run.AgentSessionID) != sessionID || !slices.Contains(ActiveChatAgentRunStatuses(), run.Status) {
		return ErrChatAgentRunDriveLeaseLost
	}
	origin, ok := common_context.GetChatAgentDelegationOrigin(ctx)
	if !ok {
		// Direct chat-service callers have not split source and destination authority.
		origin, ok = common_context.GetChatAgentRunDriveLease(ctx)
	}
	if ok {
		if origin.RunID != run.ID {
			return ErrChatAgentRunDriveLeaseLost
		}
		if origin.Owner != "" {
			leaseCtx := common_context.WithChatAgentRunDriveLease(ctx, origin.RunID, origin.Owner)
			return store.AssertChatAgentRunDriveLease(leaseCtx, origin.RunID)
		}
	}
	return nil
}

func (h *CommonPersistenceHandler[U]) fenceContextualDelegationOrigin(ctx context.Context) error {
	origin, ok := common_context.GetChatAgentDelegationOrigin(ctx)
	if !ok {
		return nil
	}
	run, err := LockChatAgentDelegationOrigin(ctx, h, origin.RunID)
	if err != nil {
		return err
	}
	if run == nil {
		return ErrChatAgentRunDriveLeaseLost
	}
	return ValidateChatAgentDelegationOrigin(ctx, h, run, common_util.Deref(run.AgentSessionID))
}
