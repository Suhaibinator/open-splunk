package notification

import (
	"context"
	"errors"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// Chat retirement pacing. Each page is one transaction over
// notification_chat_retirement_page_size members. One maintenance pass drains
// pages back to back for up to the sampled fan-out step budget, then starts the
// next pass immediately while claimable jobs remain; an idle pass waits for the
// next durable deadline, recovery scan, or a post-commit enqueue nudge. One channel's pages are
// serial because its job row is the claim. Measured throughput is in
// docs/backend/external/notifications.md.

// EnqueueChatActivityRetirement records deletion cleanup in the ambient source
// transaction. Enablement and fan-out pause do not gate it.
// After the source commits, a local nudge starts the first page without waiting
// for a recovery scan; a rollback never nudges.
func (h *Handler) EnqueueChatActivityRetirement(ctx context.Context, channelID uint64, scope string, types []uint32) error {
	if h == nil || h.store == nil {
		return nil
	}
	return h.store.WithTransaction(ctx, func(tx context.Context) error {
		if err := h.store.EnqueueChatNotificationRetirement(tx, channelID, scope, types); err != nil {
			return err
		}
		h.nudgeRetirementAfterCommit(tx)
		return nil
	})
}

// nudgeRetirementAfterCommit mirrors nudgeFanoutAfterCommit for the
// maintenance loop. The channel coalesces bursts of deletions into one pass.
func (h *Handler) nudgeRetirementAfterCommit(tx context.Context) {
	run := func(context.Context) error {
		select {
		case h.retirementWake <- struct{}{}:
		default:
		}
		return nil
	}
	// Enqueue requires a transaction, so a hook-less one is suppressed and the
	// slow recovery scan recovers; the job is never visible before commit.
	_ = persistence.RegisterNotificationPostCommitOrSuppress(tx, run)
}

// processChatRetirement drains separate page transactions independently of
// emission configuration until the queue has no claimable job or the pass
// budget is spent. It reports whether claimable work may remain, in which case
// the maintenance loop starts the next pass without waiting for its tick. Row
// claims coordinate all processes. The page size and budgets come from the
// tuning sampled by the maintenance iteration that started this pass.
func (h *Handler) processChatRetirement(ctx context.Context, tuning fanoutTuning) bool {
	more, _ := h.runChatRetirement(ctx, tuning)
	return more
}

func (h *Handler) runChatRetirement(ctx context.Context, tuning fanoutTuning) (bool, error) {
	publish := h.publishAfterCommit
	// The budget is checked between pages rather than imposed as a deadline, so
	// a page in flight is never cancelled and charged as a failure by the pass
	// boundary. Each page keeps its own bounded timeout.
	var failure error
	more := retirementPass(ctx, tuning.stepBudget, func(pageCtx context.Context) bool {
		result, err := h.store.AdvanceChatNotificationRetirement(pageCtx, tuning.retirement, publish)
		if err != nil && !result.Advanced {
			failure = err
		}
		if err != nil && ctx.Err() == nil {
			fields := []zap.Field{zap.String(logkeys.Stage, "chat_retirement"), zap.Uint64(appLogger.KeyChannelID, result.ChannelID), zap.String(appLogger.KeyFailureKind, result.Stage), zap.Uint32(appLogger.KeyAttempt, result.Attempts), zap.Error(err)}
			if result.Exhausted || errors.Is(err, persistence.ErrNotificationChatRetirementExhausted) {
				h.log(ctx).Error("chat notification retirement exhausted", append(fields, zap.String(appLogger.KeyOutcome, "exhausted"))...)
			} else {
				h.log(ctx).Warn("chat notification retirement failed", append(fields, zap.String(appLogger.KeyOutcome, "retry"))...)
			}
		}
		// A recorded failure has backed its job off, so the pass moves on to
		// other due jobs. An unrecorded failure (database unavailable, shutdown)
		// ends the pass with backoff; an empty queue waits for its next deadline.
		return result.Advanced
	})
	return more, failure
}

// processScopeRetirement drains scope-wide retirement pages exactly like
// processChatRetirement, with the same sampled policy and pass budget; a page
// visits at most min(page size, retirement recipient cap) recipient/type pairs.
func (h *Handler) processScopeRetirement(ctx context.Context, tuning fanoutTuning) bool {
	more, _ := h.runScopeRetirement(ctx, tuning)
	return more
}

func (h *Handler) runScopeRetirement(ctx context.Context, tuning fanoutTuning) (bool, error) {
	publish := h.publishAfterCommit
	var failure error
	more := retirementPass(ctx, tuning.stepBudget, func(pageCtx context.Context) bool {
		result, err := h.store.AdvanceNotificationScopeRetirement(pageCtx, tuning.retirement, publish)
		if err != nil && !result.Advanced {
			failure = err
		}
		if err != nil && ctx.Err() == nil {
			fields := []zap.Field{zap.String(logkeys.Stage, "scope_retirement"), zap.String(appLogger.KeyFailureKind, result.Stage), zap.Uint32(appLogger.KeyAttempt, result.Attempts), zap.Error(err)}
			if result.Exhausted || errors.Is(err, persistence.ErrNotificationScopeRetirementExhausted) {
				h.log(ctx).Error("notification scope retirement exhausted", append(fields, zap.String(appLogger.KeyOutcome, "exhausted"))...)
			} else {
				h.log(ctx).Warn("notification scope retirement failed", append(fields, zap.String(appLogger.KeyOutcome, "retry"))...)
			}
		}
		return result.Advanced
	})
	return more, failure
}

// Each running page owns its timeout; the pass budget is observed only between
// pages so crossing it cannot cancel an otherwise successful transaction.
func retirementPass(ctx context.Context, budget time.Duration, advance func(context.Context) bool) bool {
	started := time.Now()
	for ctx.Err() == nil && time.Since(started) < budget {
		pageCtx, cancel := context.WithTimeout(ctx, budget)
		advanced := advance(pageCtx)
		cancel()
		if !advanced {
			return false
		}
	}
	return ctx.Err() == nil
}

func (h *Handler) processSourceResolution(ctx context.Context, tuning fanoutTuning) bool {
	more, _ := h.runSourceResolution(ctx, tuning)
	return more
}

func (h *Handler) runSourceResolution(ctx context.Context, tuning fanoutTuning) (bool, error) {
	var failure error
	more := retirementPass(ctx, tuning.stepBudget, func(pageCtx context.Context) bool {
		result, err := h.store.AdvanceNotificationSourceResolution(pageCtx, tuning.retirement, h.publishAfterCommit)
		if err != nil && !result.Advanced {
			failure = err
		}
		if err != nil && ctx.Err() == nil {
			h.log(ctx).Warn("notification source resolution failed", zap.String(logkeys.Stage, result.Stage), zap.Uint32(appLogger.KeyAttempt, result.Attempts), zap.Error(err))
		}
		return result.Advanced
	})
	return more, failure
}

// publishAfterCommit publishes rows a retirement transaction resolved once that
// transaction commits; it fails the transaction when no hook can be registered.
func (h *Handler) publishAfterCommit(tx context.Context, rows []models.Notification) error {
	if !persistence.AddPostCommitHookFromContext(tx, func(committed context.Context) error {
		for i := range rows {
			h.publish(committed, &rows[i])
		}
		return nil
	}) {
		return ErrInvalid
	}
	return nil
}
