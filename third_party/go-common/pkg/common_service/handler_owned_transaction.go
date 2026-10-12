package common_service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// runOwnedTransaction runs fn inside a transaction this facade owns, then runs the transaction's
// post-commit hooks from hookCtx through runOwnedPostCommitHooks. Any ambient request transaction
// on ctx is stripped first, so the work never joins (or is rolled back with) a caller's
// transaction. operation labels the hooks in logs.
func (h *CommonServiceHandler[U]) runOwnedTransaction(ctx, hookCtx context.Context, operation string, fn func(txCtx context.Context) error) error {
	hooks, err := h.ownedTransaction(ctx, fn)
	if err != nil {
		return err
	}
	h.runOwnedPostCommitHooks(hookCtx, hooks, operation)
	return nil
}

// ownedTransaction begins a transaction on ctx (with any request transaction stripped), installs
// it with post-commit hooks on an isolated clone of ctx, runs fn, and commits. It returns the
// drained post-commit hooks for a caller that must order other work before them; everything else
// uses runOwnedTransaction. A failure from fn or the commit rolls the transaction back.
func (h *CommonServiceHandler[U]) ownedTransaction(ctx context.Context, fn func(txCtx context.Context) error) ([]persistence.PostCommitHook, error) {
	ctx = persistence.WithoutRequestTransaction[U](ctx)
	tx, err := h.commonPersistenceHandler.BeginTransaction(ctx)
	if err != nil {
		return nil, err
	}
	hooks := &persistence.GormTransactionWithHooks{Tx: tx}
	committed := false
	defer func() {
		if !committed {
			_ = hooks.Rollback()
		}
	}()
	txCtx := scontext.CopySRouterContext[common_service_models.UserIdType, U](ctx, ctx)
	txCtx = scontext.WithTransaction[common_service_models.UserIdType, U](txCtx, hooks)
	if err := fn(txCtx); err != nil {
		return nil, err
	}
	if err := hooks.Commit(); err != nil {
		return nil, err
	}
	committed = true
	return hooks.DrainPostCommitHooks(), nil
}

// runOwnedPostCommitHooks runs the post-commit hooks of a transaction this facade owned (a run
// cancellation, a delegation tool's own transaction) once it has committed. Their best-effort work
// follows the owned approval/bot hook lifecycle: detached, deadline-bound, admitted before shutdown
// and joined by Stop. operation labels the hooks in logs.
func (h *CommonServiceHandler[U]) runOwnedPostCommitHooks(ctx context.Context, hooks []persistence.PostCommitHook, operation string) {
	if len(hooks) == 0 {
		return
	}
	h.ownedPostCommitMu.Lock()
	if h.ownedPostCommitStopping {
		h.ownedPostCommitMu.Unlock()
		return
	}
	h.ownedPostCommitWg.Add(1)
	h.ownedPostCommitMu.Unlock()
	postCtx, cancel := context.WithTimeout(persistence.WithoutRequestTransaction[U](context.WithoutCancel(ctx)), 30*time.Second)
	go func() {
		defer h.ownedPostCommitWg.Done()
		defer cancel()
		for index, hook := range hooks {
			if hook != nil {
				h.runOwnedPostCommitHook(postCtx, hook, operation, index, len(hooks))
			}
		}
	}()
}

func (h *CommonServiceHandler[U]) runOwnedPostCommitHook(ctx context.Context, hook persistence.PostCommitHook, operation string, index, count int) {
	base := h.logger
	if base == nil {
		base = zap.NewNop()
	}
	fields := []zap.Field{
		zap.String(logkeys.Operation, operation),
		zap.Int(appLogger.KeyHookIndex, index), zap.Int(appLogger.KeyHookCount, count),
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			appLogger.For(ctx, base, agentRuntimeLoggerName).Error("owned post-commit hook panicked",
				append(fields, zap.String(appLogger.KeyOutcome, "recovered"), zap.String(appLogger.KeyPanicType, fmt.Sprintf("%T", recovered)))...)
		}
	}()
	if err := hook(ctx); err != nil {
		// Application publisher errors can include notification content or credentials.
		appLogger.For(ctx, base, agentRuntimeLoggerName).Warn("owned post-commit hook failed",
			append(fields, zap.String(appLogger.KeyOutcome, "failed"), zap.Error(errors.New("post-commit callback failed")))...)
	}
}

func (h *CommonServiceHandler[U]) stopOwnedPostCommitHooks() {
	h.ownedPostCommitMu.Lock()
	h.ownedPostCommitStopping = true
	h.ownedPostCommitMu.Unlock()
	h.ownedPostCommitWg.Wait()
}
