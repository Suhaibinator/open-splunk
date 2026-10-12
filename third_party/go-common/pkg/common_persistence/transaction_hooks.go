package common_persistence

import (
	"context"
	"sync"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// PostCommitHook runs only after the surrounding DB transaction successfully commits.
//
// Hooks are strictly best-effort and run on a bounded budget: the context they
// receive is detached from the request's cancellation but carries a deadline
// (PostCommitHookTimeout), and the buffering transaction middleware waits for
// them only briefly (PostCommitHookFlushWait) before flushing the response and
// leaving them to finish in the background. A hook must respect its context —
// one that ignores cancellation can outlive its budget and leak a goroutine.
// An error return is logged and does not affect the HTTP response or the other
// hooks. Work that MUST happen whenever the commit happens does not belong
// here; record it in the transaction itself (e.g. an outbox row) and process
// it from a durable queue instead.
type PostCommitHook func(ctx context.Context) error

// PostCommitHookRegistrar is implemented by transactions that support registering
// post-commit hooks.
type PostCommitHookRegistrar interface {
	AddPostCommitHook(hook PostCommitHook)
	DrainPostCommitHooks() []PostCommitHook
}

// GormTransactionWithHooks defines the data structure used in the persistence flow for transactional database operations and repository access.
// It carries state across layer boundaries handled by this package.
type GormTransactionWithHooks struct {
	Tx *gorm.DB

	mu    sync.Mutex
	hooks []PostCommitHook
	// savepoints maps each savepoint to how many hooks were registered when it was taken, so
	// RollbackTo drops the hooks of the work it rolled back.
	savepoints map[string]int
}

// GetDB executes this method as part of the persistence flow for transactional database operations and repository access.
//
// A nil receiver returns nil rather than panicking: a typed-nil
// *GormTransactionWithHooks stored in a context still satisfies the
// transaction interface, and preflights such as
// DatabasePostCommitHookRegistrarFromContext must be able to probe it safely.
func (gt *GormTransactionWithHooks) GetDB() *gorm.DB {
	if gt == nil {
		return nil
	}
	return gt.Tx
}

// Commit executes this method as part of the persistence flow for transactional database operations and repository access.
func (gt *GormTransactionWithHooks) Commit() error {
	tx, err := gt.transactionDB("commit_transaction")
	if err != nil {
		return err
	}
	if err := tx.Commit().Error; err != nil {
		return databaseErrorWithFields(err, zap.String(logkeys.Operation, "commit_transaction"))
	}
	return nil
}

// Rollback executes this method as part of the persistence flow for transactional database operations and repository access.
func (gt *GormTransactionWithHooks) Rollback() error {
	tx, err := gt.transactionDB("rollback_transaction")
	if err != nil {
		return err
	}
	if err := tx.Rollback().Error; err != nil {
		return databaseErrorWithFields(err, zap.String(logkeys.Operation, "rollback_transaction"))
	}
	return nil
}

// SavePoint executes this method as part of the persistence flow for transactional database operations and repository access.
func (gt *GormTransactionWithHooks) SavePoint(name string) error {
	tx, err := gt.transactionDB("save_transaction_point")
	if err != nil {
		return err
	}
	if err := tx.SavePoint(name).Error; err != nil {
		return databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "save_transaction_point"),
			zap.String(appLogger.KeySavepoint, name))
	}
	gt.mu.Lock()
	if gt.savepoints == nil {
		gt.savepoints = map[string]int{}
	}
	gt.savepoints[name] = len(gt.hooks)
	gt.mu.Unlock()
	return nil
}

// RollbackTo rolls the transaction back to savepoint name and drops the post-commit hooks
// registered since it was taken: they belong to work that will never commit.
func (gt *GormTransactionWithHooks) RollbackTo(name string) error {
	tx, err := gt.transactionDB("rollback_to_transaction_point")
	if err != nil {
		return err
	}
	if err := tx.RollbackTo(name).Error; err != nil {
		return databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "rollback_to_transaction_point"),
			zap.String(appLogger.KeySavepoint, name))
	}
	gt.mu.Lock()
	if mark, ok := gt.savepoints[name]; ok && mark < len(gt.hooks) {
		gt.hooks = gt.hooks[:mark]
	}
	gt.mu.Unlock()
	return nil
}

func (gt *GormTransactionWithHooks) transactionDB(operation string) (*gorm.DB, error) {
	if gt == nil || gt.Tx == nil {
		return nil, appLogger.InvariantError("transaction_wrapper_has_database", nil,
			zap.String(logkeys.Operation, operation),
			zap.String(logkeys.Expected, "non-nil transaction database"),
			zap.String(logkeys.Actual, "nil transaction database"))
	}
	return gt.Tx, nil
}

// AddPostCommitHook executes this method as part of the persistence flow for transactional database operations and repository access.
func (gt *GormTransactionWithHooks) AddPostCommitHook(hook PostCommitHook) {
	if gt == nil || hook == nil {
		return
	}
	gt.mu.Lock()
	gt.hooks = append(gt.hooks, hook)
	gt.mu.Unlock()
}

// DrainPostCommitHooks executes this method as part of the persistence flow for transactional database operations and repository access.
func (gt *GormTransactionWithHooks) DrainPostCommitHooks() []PostCommitHook {
	if gt == nil {
		return nil
	}
	gt.mu.Lock()
	defer gt.mu.Unlock()
	out := gt.hooks
	gt.hooks = nil
	return out
}

// RunInTransaction runs fn in one transaction and hands it a context whose DB(ctx) joins that
// transaction. It is the transaction boundary for application work outside an HTTP request, such
// as background workers and agentkit.WorkflowAgentStart.Transaction; request handlers already get
// one from the transaction middleware.
//
// Without an ambient transaction on ctx, fn runs in a new transaction. With one, it runs in a
// savepoint of it, so a failure rolls back only fn's work. Either way fn receives a copy of ctx:
// the caller's context never observes the transaction, before or after it finishes. A panic in fn
// rolls the transaction (or savepoint) back and is re-raised.
//
// fn's context supports post-commit hooks (AddPostCommitHookFromContext). A failure in fn, or in
// the commit, discards the hooks fn registered. On success they move to ctx's ambient transaction
// when it supports hooks, so they wait for its commit. Without an ambient transaction they run
// before RunInTransaction returns: each one is detached from ctx's cancellation and runs on the
// post_commit_hook_timeout budget, and failures and panics are logged, never returned. An ambient
// transaction without hook support drops them, because it gives no point after its commit to run them.
func (h *CommonPersistenceHandler[U]) RunInTransaction(ctx context.Context, fn func(context.Context) error) error {
	if fn == nil {
		return appLogger.InvariantError("run_in_transaction_requires_callback", ErrDatabaseError,
			zap.String(logkeys.Operation, "run_in_transaction"),
			zap.String(logkeys.Expected, "non-nil callback"),
			zap.String(logkeys.Actual, "nil callback"))
	}
	db := h.DB(ctx)
	if db == nil {
		return appLogger.InvariantError("run_in_transaction_requires_database", ErrDatabaseError,
			zap.String(logkeys.Operation, "run_in_transaction"),
			zap.String(logkeys.Expected, "open database"),
			zap.String(logkeys.Actual, "nil database"))
	}
	return transactionWithHooks[U](ctx, db, h.postCommitHookTimeout, h.logger, func(txCtx context.Context, _ *gorm.DB) error {
		return fn(txCtx)
	})
}

// transactionWithHooks runs fn in one transaction of db, a savepoint under ctx's ambient
// transaction, and hands it txCtx: a copy of ctx that carries this transaction with post-commit
// hook support, for callees that join the transaction through their context. The hooks registered
// on txCtx join the ambient transaction's, to run when it commits; without one they run once this
// transaction commits, each on the hookTimeout budget (sampled once per drain; non-positive uses
// the key's default), with failures logged through fallback. A failed fn runs none, and an
// ambient transaction without hook support suppresses them.
func transactionWithHooks[U common_service_models.UserLike](
	ctx context.Context,
	db *gorm.DB,
	hookTimeout func() time.Duration,
	fallback *zap.Logger,
	fn func(txCtx context.Context, tx *gorm.DB) error,
) error {
	var hooks *GormTransactionWithHooks
	err := db.Transaction(func(tx *gorm.DB) error {
		hooks = &GormTransactionWithHooks{Tx: tx}
		txCtx := scontext.CopySRouterContext[uint64, U](ctx, ctx)
		return fn(scontext.WithTransaction[uint64, U](txCtx, hooks), tx)
	})
	if err != nil {
		return err
	}
	var timeout time.Duration
	for _, hook := range hooks.DrainPostCommitHooks() {
		if AddPostCommitHookFromContext(ctx, hook) {
			continue
		}
		if ambient, ok := scontext.GetTransaction(ctx); ok && ambient != nil {
			continue
		}
		if timeout == 0 {
			if timeout = hookTimeout(); timeout <= 0 { // sampled once per drain
				timeout = defaultPostCommitHookTimeout
			}
		}
		runStandalonePostCommitHook(ctx, fallback, hook, timeout)
	}
	return nil
}

func runStandalonePostCommitHook(ctx context.Context, fallback *zap.Logger, hook PostCommitHook, timeout time.Duration) {
	if hook == nil {
		return
	}
	hookCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	logger := appLogger.For(ctx, fallback, loggerName)
	defer func() {
		if recovered := recover(); recovered != nil && logger != nil {
			logger.Error("post-commit hook panicked",
				zap.String(logkeys.Stage, "run_transaction_post_commit_hooks"),
				zap.Any(logkeys.Panic, recovered))
		}
	}()
	if err := hook(hookCtx); err != nil && logger != nil {
		logger.Warn("post-commit hook failed",
			zap.String(logkeys.Stage, "run_transaction_post_commit_hooks"), zap.Error(err))
	}
}
