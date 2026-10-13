// Package middleware provides HTTP middleware functionality for the application
package middleware

import (
	"context"
	"net/http"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/internal/httpstatus"
	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// PostCommitCallback is invoked asynchronously after a successful transaction commit.
// The callback must be best-effort; failures are logged but do not affect the HTTP response.
type PostCommitCallback func(ctx context.Context, tx common_persistence.PostCommitHookRegistrar) error

// Defaults applied by PostCommitHookOptions.withDefaults when a field is left
// zero, so directly constructed middleware is bounded without configuration.
const (
	DefaultPostCommitHookTimeout   = 30 * time.Second
	DefaultPostCommitHookFlushWait = 5 * time.Second
	// DefaultMaxBufferedResponseBytes is the maximum response body retained in
	// memory by TransactionMiddleware when the caller leaves the option unset.
	// Four MiB accommodates normal unary API responses while failing accidental
	// exports before memory use can grow without bound.
	DefaultMaxBufferedResponseBytes int64 = 4 * 1024 * 1024
)

// PostCommitHookOptions bounds post-commit hook execution and the in-memory
// response retained by TransactionMiddleware. Hooks are best-effort: they are
// detached from the request's cancellation (the client disconnecting must not
// abort work about an already-committed transaction), so without these bounds a
// hook that blocks forever would run — and, in the buffering middleware, hold
// the response — forever.
type PostCommitHookOptions struct {
	// HookTimeout is the deadline on the context passed to the hooks. It is
	// cooperative: a hook that ignores its context can outlive it.
	HookTimeout time.Duration
	// FlushWait is how long the buffering TransactionMiddleware holds the
	// committed response waiting for hooks to finish before flushing anyway
	// and leaving them to run in the background. It guarantees the response
	// is released even against a hook that ignores its context. Unused by
	// StreamingTransactionMiddleware, which never waits on hooks.
	FlushWait time.Duration
	// MaxBufferedResponseBytes is the largest response body
	// TransactionMiddleware will retain. A response that attempts to exceed the
	// limit is discarded, its transaction is rolled back, and the client receives
	// a generic 500. Non-positive values use DefaultMaxBufferedResponseBytes; the
	// limit cannot be disabled. This option is ignored by
	// StreamingTransactionMiddleware, which does not buffer responses.
	//
	// Routes that intentionally return bodies larger than this limit, such as
	// exports, must use StreamingTransactionMiddleware and accept its documented
	// commit-after-write tradeoff, or be redesigned so response generation happens
	// outside the transaction.
	MaxBufferedResponseBytes int64
}

// withDefaults replaces non-positive fields with the package defaults so a
// zero PostCommitHookOptions is safe rather than unbounded.
func (o PostCommitHookOptions) withDefaults() PostCommitHookOptions {
	if o.HookTimeout <= 0 {
		o.HookTimeout = DefaultPostCommitHookTimeout
	}
	if o.FlushWait <= 0 {
		o.FlushWait = DefaultPostCommitHookFlushWait
	}
	if o.MaxBufferedResponseBytes <= 0 {
		o.MaxBufferedResponseBytes = DefaultMaxBufferedResponseBytes
	}
	return o
}

// postCommitContext derives the context passed to post-commit hooks: detached
// from the request's cancellation (the commit already happened, so a client
// disconnect must not abort the hooks), stripped of the completed transaction,
// but bounded by hookTimeout so a context-respecting hook cannot run forever.
// The returned cancel must be called once the hooks finish.
func postCommitContext[U common_service_models.UserLike](baseCtx context.Context, hookTimeout time.Duration) (context.Context, context.CancelFunc) {
	hookCtx := context.WithoutCancel(baseCtx)
	// Clone SRouter context to avoid sharing the request's transaction in post-commit hooks.
	hookCtx = scontext.CopySRouterContext[common_service_models.UserIdType, U](hookCtx, hookCtx)
	scontext.ClearTransaction[common_service_models.UserIdType, U](hookCtx)

	return context.WithTimeout(hookCtx, hookTimeout)
}

// RunRegisteredPostCommitHooks drains hooks in registration order. Errors and
// panics are logged per hook so one failed callback cannot skip later cleanup.
func RunRegisteredPostCommitHooks(logger *zap.Logger) PostCommitCallback {
	return func(ctx context.Context, tx common_persistence.PostCommitHookRegistrar) error {
		if tx == nil {
			return nil
		}
		runPostCommitHooks(ctx, appLogger.For(ctx, logger, commonAPILoggerName), tx.DrainPostCommitHooks())
		return nil
	}
}

// runPostCommitHooks is shared by buffered and streaming transaction middleware.
// Context cancellation is left to each hook; local cache invalidation can still
// finish when an earlier hook has consumed the context's time budget.
func runPostCommitHooks(ctx context.Context, logger *zap.Logger, hooks []common_persistence.PostCommitHook) {
	for i, hook := range hooks {
		if hook == nil {
			continue
		}
		func() {
			defer func() {
				if panicValue := recover(); panicValue != nil {
					logger.Error("Post-commit hook panicked",
						zap.String(logkeys.Operation, "run_registered_post_commit_hooks"),
						zap.Int(appLogger.KeyHookIndex, i),
						zap.Int(appLogger.KeyHookCount, len(hooks)),
						zap.Any(logkeys.Panic, panicValue))
				}
			}()
			if err := hook(ctx); err != nil {
				appLogger.LogOperationWarn(logger, "Post-commit hook failed", "run_registered_post_commit_hooks", err,
					zap.Int(appLogger.KeyHookIndex, i),
					zap.Int(appLogger.KeyHookCount, len(hooks)))
			}
		}()
	}
}

// StreamingTransactionMiddleware is a non-buffering transaction middleware with
// post-commit hooks, intended ONLY for streaming, Server-Sent Events, WebSocket,
// or intentionally large/export routes where response buffering is impossible or
// inappropriate. It begins a hook-capable
// transaction, exposes it on the request context, and lets the handler write
// directly to the client. On success it commits and then runs registered
// post-commit hooks asynchronously (best-effort); on handler error or panic it
// rolls back.
//
// CAVEAT: because the handler streams its response as it runs, the success status
// and body reach the client BEFORE the commit is attempted. If that commit then
// fails, the client has already been told the operation succeeded — this middleware
// can leak a success status describing a write that was actually rolled back. It
// trades that safety for the ability to stream. For every bounded unary
// request/response route (the common case), use TransactionMiddleware instead,
// which commits before flushing and therefore cannot leak an uncommitted success.
// When practical, large exports should finish their transactional work first and
// generate or serve the response outside the transaction rather than accepting
// this tradeoff.
//
// Parameters:
// - persistenceHandler (common_persistence.CommonPersistenceInterface): provides input required by StreamingTransactionMiddleware.
// - logger (*zap.Logger): provides input required by StreamingTransactionMiddleware.
// - hookOpts (PostCommitHookOptions): bounds post-commit hook execution; only HookTimeout applies here. FlushWait and MaxBufferedResponseBytes are ignored (zero fields use package defaults).
func StreamingTransactionMiddleware[U common_service_models.UserLike](
	persistenceHandler common_persistence.CommonPersistenceInterface,
	logger *zap.Logger,
	hookOpts PostCommitHookOptions,
) common.Middleware {
	return streamingTransactionMiddleware[U](func(ctx context.Context) (commitBeforeFlushTransaction, error) {
		tx, err := persistenceHandler.BeginTransaction(ctx)
		if err != nil {
			return nil, err
		}
		return &common_persistence.GormTransactionWithHooks{Tx: tx}, nil
	}, logger, hookOpts)
}

// streamingTransactionMiddleware is the injectable core of
// StreamingTransactionMiddleware. Keeping the transaction behind the same seam as
// the buffering middleware lets tests substitute a fake transaction with
// controllable commit/rollback outcomes and post-commit hooks.
func streamingTransactionMiddleware[U common_service_models.UserLike](
	begin beginCommitBeforeFlushTransaction,
	logger *zap.Logger,
	hookOpts PostCommitHookOptions,
) common.Middleware {
	if logger == nil {
		logger = zap.NewNop()
	}
	hookOpts = hookOpts.withDefaults()
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			baseCtx := r.Context()

			// Begin transaction
			tx, err := begin(baseCtx)
			if err != nil || tx == nil {
				fields := transactionRequestLogFields[U](r, nil, false)
				fields = append(fields, zap.String(logkeys.Operation, "begin_streaming_transaction"))
				if err != nil {
					fields = append(fields, zap.Error(err))
					appLogger.For(baseCtx, logger, commonAPILoggerName).Error("Failed to begin transaction in middleware", fields...)
				} else {
					appLogger.LogInvariantViolation(appLogger.For(baseCtx, logger, commonAPILoggerName), "transaction_begin_returns_transaction", nil,
						append(fields,
							zap.String(logkeys.Stage, "begin_transaction"),
							zap.String(logkeys.Expected, "non-nil transaction"),
							zap.String(logkeys.Actual, "nil transaction"),
							zap.String(logkeys.Fallback, "return_internal_server_error"))...)
				}
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}

			// Expose the hook-capable transaction on the SRouter context.
			ctxWithTx := scontext.WithTransaction[common_service_models.UserIdType, U](baseCtx, tx)
			r = r.WithContext(ctxWithTx)

			// Track if transaction was handled
			var committed bool

			// Defer rollback as safety net
			defer func() {
				if panicValue := recover(); panicValue != nil {
					if !committed {
						if rollbackErr := tx.Rollback(); rollbackErr != nil {
							fields := transactionRequestLogFields[U](r, nil, false)
							fields = append(fields,
								zap.Error(rollbackErr),
								zap.Any(logkeys.Panic, panicValue))
							appLogger.For(baseCtx, logger, commonAPILoggerName).Error("Failed to rollback transaction after panic",
								fields...)
						} else if appLogger.DebugEnabled(logger) {
							fields := transactionRequestLogFields[U](r, nil, false)
							fields = append(fields, zap.Any(logkeys.Panic, panicValue))
							appLogger.For(baseCtx, logger, commonAPILoggerName).Debug("Transaction rolled back successfully after panic", fields...)
						}
					}
					// Re-panic to propagate the original error
					panic(panicValue)
				}
			}()

			// Track the streamed status without buffering the body. A 4xx/5xx response
			// without the corresponding SRouter handler error violates the transaction
			// contract and must not be committed.
			recorder := &httpstatus.Recorder{ResponseWriter: w}
			next.ServeHTTP(recorder, r)

			// Get the handler error from context (set by SRouter)
			handlerErr, handlerErrorSet := scontext.GetHandlerError(r.Context())
			statusCode := recorder.Status
			if statusCode == 0 {
				statusCode = http.StatusOK
			}
			if statusCode >= http.StatusBadRequest && handlerErr == nil {
				fields := transactionRequestLogFields[U](r, handlerErr, handlerErrorSet)
				fields = append(fields,
					zap.Int(logkeys.StatusCode, statusCode),
					zap.String(logkeys.Expected, "handler error for unsuccessful response"),
					zap.String(logkeys.Actual, "nil handler error"),
					zap.String(logkeys.Fallback, "rollback transaction"))
				appLogger.LogInvariantViolation(appLogger.For(baseCtx, logger, commonAPILoggerName), "streaming_error_response_has_handler_error", nil, fields...)
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					appLogger.For(baseCtx, logger, commonAPILoggerName).Error("Failed to rollback transaction after handler-error invariant violation",
						append(fields, zap.Error(rollbackErr))...)
				}
				return
			}

			// Commit or rollback based on handler error
			// A nil handler error is a successful response after the status check above.
			if !handlerErrorSet || handlerErr == nil {
				// Success: commit transaction
				if commitErr := tx.Commit(); commitErr != nil {
					fields := transactionRequestLogFields[U](r, handlerErr, handlerErrorSet)
					fields = append(fields, zap.Error(commitErr))
					appLogger.For(baseCtx, logger, commonAPILoggerName).Error("Failed to commit transaction", fields...)
					// Note: We don't write another error response here since the handler
					// already wrote its response
					return
				}
				committed = true

				hooks := tx.DrainPostCommitHooks()
				if len(hooks) == 0 {
					return
				}

				// Run hooks asynchronously (best-effort).
				hookCtx, cancelHooks := postCommitContext[U](baseCtx, hookOpts.HookTimeout)
				requestFields := transactionRequestLogFields[U](r, nil, false)
				go func() {
					defer cancelHooks()
					runPostCommitHooks(hookCtx, appLogger.For(hookCtx, logger, commonAPILoggerName).With(requestFields...), hooks)
				}()
			} else {
				// Error: rollback transaction
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					fields := transactionRequestLogFields[U](r, handlerErr, handlerErrorSet)
					fields = append(fields, zap.Error(rollbackErr))
					appLogger.For(baseCtx, logger, commonAPILoggerName).Error("Failed to rollback transaction", fields...)
				}
				// Log the handler error for debugging
				if appLogger.DebugEnabled(logger) {
					appLogger.For(baseCtx, logger, commonAPILoggerName).Debug("Transaction rolled back due to handler error",
						transactionRequestLogFields[U](r, handlerErr, handlerErrorSet)...)
				}
			}
		})
	}
}
