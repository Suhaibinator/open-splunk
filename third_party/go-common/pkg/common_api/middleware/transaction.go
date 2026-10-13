// Package middleware provides HTTP middleware functionality for the application
package middleware

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// commitBeforeFlushTransaction is both the transaction stored in SRouter's
// request context and the collector for post-commit work. Keeping this an
// injectable interface makes commit-failure behavior testable without relying
// on a database driver to fail at exactly the right instant.
type commitBeforeFlushTransaction interface {
	scontext.DatabaseTransaction
	common_persistence.PostCommitHookRegistrar
}

// beginCommitBeforeFlushTransaction begins a hook-capable transaction. It is a
// seam that lets tests substitute a fake transaction with controllable commit
// and rollback outcomes.
type beginCommitBeforeFlushTransaction func(context.Context) (commitBeforeFlushTransaction, error)

// TransactionMiddleware is THE default transaction middleware for unary
// request/response routes. It wraps a request in a database transaction whose HTTP
// response is buffered in memory and flushed to the client only AFTER the
// transaction has durably committed.
//
// It begins a hook-capable transaction, exposes it on the request context, commits
// on success, rolls back on handler error, rolls back and re-panics on panic, and
// runs registered post-commit hooks after commit — with one crucial safety
// property: the handler never writes directly to the client. Every status code,
// header, and body byte is captured in a buffer while the handler runs, and the
// buffer is replayed to the real ResponseWriter only once the commit succeeds.
// Consequently a deferred commit failure (e.g. a deferred foreign-key constraint)
// can never leak a 2xx status, success headers, or a success body describing a
// write that was actually rolled back — on commit failure the buffered success is
// discarded and a generic 500 is sent instead.
//
// Post-commit hooks run after the durable commit, off the request goroutine, and
// the response flush waits for them only up to hookOpts.FlushWait: when hooks
// finish within that budget the response is flushed after them (preserving
// event-before-response ordering), otherwise the buffered response is flushed
// anyway and hookOpts.HookTimeout expires the background hooks' context. A hook
// that ignores cancellation can keep running beyond that deadline. Hooks are best-effort — a slow, hung, or failing hook
// can therefore never withhold a response describing a commit that already
// happened. Work that MUST happen whenever the commit happens does not belong in
// a hook; record it transactionally (e.g. an outbox row) instead.
//
// Response buffering is bounded by hookOpts.MaxBufferedResponseBytes (4 MiB by
// default). If the handler attempts to exceed the limit, the partial response is
// discarded, the transaction is rolled back, post-commit hooks are skipped, and
// the client receives a generic 500. The middleware owns this check because HTTP
// handlers commonly ignore errors returned by ResponseWriter.Write.
//
// WARNING: This middleware MUST NOT be attached to streaming, Server-Sent Events,
// WebSocket, or intentionally large/export routes. Buffering defeats incremental
// delivery, and the buffered writer's http.Flusher implementation is an
// intentional no-op — Flush() does not push bytes to the client. Connection
// hijacking (http.Hijacker) is likewise not supported. Apply this to bounded unary
// request/response routes (the typical protobuf create/update/delete endpoints).
// Use StreamingTransactionMiddleware for incremental or large responses, after
// accepting its documented risk that a commit failure can occur after success
// bytes have reached the client; when possible, generate large exports outside a
// database transaction instead.
//
// The type parameter U identifies the application user type carried on the
// SRouter context, matching StreamingTransactionMiddleware.
func TransactionMiddleware[U common_service_models.UserLike](
	persistenceHandler common_persistence.CommonPersistenceInterface,
	logger *zap.Logger,
	hookOpts PostCommitHookOptions,
) common.Middleware {
	return commitBeforeFlushTransactionMiddleware[U](func(ctx context.Context) (commitBeforeFlushTransaction, error) {
		tx, err := persistenceHandler.BeginTransaction(ctx)
		if err != nil {
			return nil, err
		}
		return &common_persistence.GormTransactionWithHooks{Tx: tx}, nil
	}, logger, hookOpts)
}

// commitBeforeFlushTransactionMiddleware is the injectable core of
// TransactionMiddleware. It holds all response bytes until the database outcome is
// known so a deferred commit failure cannot leak a success status, headers, or body
// to the caller.
func commitBeforeFlushTransactionMiddleware[U common_service_models.UserLike](
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

			tx, err := begin(baseCtx)
			if err != nil || tx == nil {
				fields := transactionRequestLogFields[U](r, nil, false)
				fields = append(fields, zap.String(logkeys.Operation, "begin_commit_before_flush_transaction"))
				if err != nil {
					fields = append(fields, zap.Error(err))
					appLogger.For(baseCtx, logger, commonAPILoggerName).Error("Failed to begin commit-before-flush transaction", fields...)
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

			buffered := newBufferedResponseWriter(hookOpts.MaxBufferedResponseBytes)
			ctxWithTx := scontext.WithTransaction[common_service_models.UserIdType, U](baseCtx, tx)
			r = r.WithContext(ctxWithTx)

			committed := false
			defer func() {
				if panicValue := recover(); panicValue != nil {
					// Roll back the still-open transaction, then re-panic so the
					// outer recovery/logging middleware handles it as usual. The
					// buffered (never-flushed) response is discarded with the panic.
					if !committed {
						if rollbackErr := tx.Rollback(); rollbackErr != nil {
							fields := transactionRequestLogFields[U](r, nil, false)
							fields = append(fields, zap.Error(rollbackErr), zap.Any(logkeys.Panic, panicValue))
							appLogger.For(baseCtx, logger, commonAPILoggerName).Error("Failed to rollback commit-before-flush transaction after panic", fields...)
						}
					}
					panic(panicValue)
				}
			}()

			next.ServeHTTP(buffered, r)

			handlerErr, handlerErrorSet := scontext.GetHandlerError(r.Context())
			// SRouter's inner timeout uses a child context and may return before
			// the application handler exits. Its buffered 408 is an owned failure,
			// even when the still-running handler has not recorded an error.
			if buffered.statusCode == http.StatusRequestTimeout || baseCtx.Err() != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					appLogger.For(baseCtx, logger, commonAPILoggerName).Error("Failed to rollback timed-out transaction", zap.Error(rollbackErr))
				}
				if buffered.statusCode == http.StatusRequestTimeout {
					buffered.flushTo(w, r)
				} else {
					http.Error(w, "Request Timeout", http.StatusRequestTimeout)
				}
				return
			}
			if buffered.overflowed {
				// Write errors are frequently ignored by handlers, so overflow is a
				// middleware-owned failure. Never commit work whose response cannot be
				// delivered, and never replay the partial buffered status, headers, or
				// body to the client.
				fields := transactionRequestLogFields[U](r, handlerErr, handlerErrorSet)
				fields = append(fields,
					zap.Int64(appLogger.KeyResponseBufferLimitBytes, buffered.maxBytes),
					zap.Int64(appLogger.KeyResponseBytesAttempted, buffered.attemptedBytes),
				)
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					fields = append(fields, zap.NamedError(appLogger.KeyRollbackError, rollbackErr))
				}
				appLogger.For(baseCtx, logger, commonAPILoggerName).Error("Transaction response exceeded buffer limit; transaction rolled back", fields...)
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}
			if handlerErrorSet && handlerErr != nil {
				// The handler already produced its (buffered) error response; roll
				// back and replay it verbatim.
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					fields := transactionRequestLogFields[U](r, handlerErr, handlerErrorSet)
					fields = append(fields, zap.Error(rollbackErr))
					appLogger.For(baseCtx, logger, commonAPILoggerName).Error("Failed to rollback commit-before-flush transaction", fields...)
				} else if appLogger.DebugEnabled(logger) {
					appLogger.For(baseCtx, logger, commonAPILoggerName).Debug("Transaction rolled back due to handler error",
						transactionRequestLogFields[U](r, handlerErr, handlerErrorSet)...)
				}
				buffered.flushTo(w, r)
				return
			}

			if commitErr := tx.Commit(); commitErr != nil {
				fields := transactionRequestLogFields[U](r, handlerErr, handlerErrorSet)
				fields = append(fields, zap.Error(commitErr))
				appLogger.For(baseCtx, logger, commonAPILoggerName).Error("Failed to commit commit-before-flush transaction", fields...)
				// Rollback is best effort after a failed commit. Some drivers have
				// already closed the transaction, but others still need cleanup.
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					appLogger.For(baseCtx, logger, commonAPILoggerName).Error("Rollback after commit failure did not complete",
						zap.String(logkeys.Operation, "rollback_after_commit_failure"),
						zap.Error(rollbackErr))
				}
				// Discard the buffered success entirely: the caller must never see a
				// 2xx describing a write that did not durably land.
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}
			committed = true

			// Hooks run after the durable commit, off the request goroutine. The
			// flush waits for them only up to hookOpts.FlushWait, so a slow or
			// hung hook can never withhold the already-committed response.
			runCommitBeforeFlushPostCommitHooks[U](r.Context(), tx, logger, hookOpts)
			buffered.flushTo(w, r)
		})
	}
}

// runCommitBeforeFlushPostCommitHooks drains and runs the registered post-commit
// hooks on a dedicated goroutine, reusing the shared post-commit plumbing
// (postCommitContext + RunRegisteredPostCommitHooks), and returns once the hooks
// finish or opts.FlushWait elapses — whichever comes first — so the caller can
// flush the committed response no matter how the hooks behave. Hooks that
// overrun the wait keep running in the background until opts.HookTimeout expires
// their context. A hook panic is contained on the hook goroutine so it can never
// prevent the already-committed response from flushing. The shared hook runner
// contains individual hook panics and continues with subsequent hooks.
func runCommitBeforeFlushPostCommitHooks[U common_service_models.UserLike](
	ctx context.Context,
	tx common_persistence.PostCommitHookRegistrar,
	logger *zap.Logger,
	opts PostCommitHookOptions,
) {
	if tx == nil {
		return
	}
	hooks := tx.DrainPostCommitHooks()
	if len(hooks) == 0 {
		return
	}
	hookCtx, cancelHooks := postCommitContext[U](ctx, opts.HookTimeout)
	done := make(chan struct{})
	go func() {
		defer cancelHooks()
		defer close(done)
		defer func() {
			if panicValue := recover(); panicValue != nil {
				appLogger.For(hookCtx, logger, commonAPILoggerName).Error("Commit-before-flush post-commit hook panicked",
					zap.String(logkeys.Operation, "post_commit_hook"),
					zap.Any(logkeys.Panic, panicValue))
			}
		}()
		runPostCommitHooks(hookCtx, appLogger.For(hookCtx, logger, commonAPILoggerName), hooks)
	}()

	flushWait := time.NewTimer(opts.FlushWait)
	defer flushWait.Stop()
	select {
	case <-done:
	case <-flushWait.C:
		appLogger.For(ctx, logger, commonAPILoggerName).Warn(
			"Post-commit hooks exceeded the flush wait; releasing the response while they finish in the background",
			appLogger.DurationMS(appLogger.KeyFlushWaitMS, opts.FlushWait),
			appLogger.DurationSeconds(appLogger.KeyHookTimeoutSeconds, opts.HookTimeout))
	}
}

// bufferedResponseWriter captures the handler's status code, headers, and a
// bounded body in memory instead of forwarding them to the client. A write that
// would cross maxBytes is rejected atomically: none of that write is retained,
// and overflowed remains set so the middleware rolls back even when the handler
// ignores the error. flushTo replays a complete captured response onto the real
// http.ResponseWriter once the transaction outcome is known.
type bufferedResponseWriter struct {
	header         http.Header
	body           bytes.Buffer
	statusCode     int
	wroteHeader    bool
	maxBytes       int64
	overflowed     bool
	attemptedBytes int64
}

var errBufferedResponseLimitExceeded = errors.New("transaction response exceeds configured buffer limit")

func newBufferedResponseWriter(maxBytes int64) *bufferedResponseWriter {
	return &bufferedResponseWriter{
		header:   make(http.Header),
		maxBytes: maxBytes,
	}
}

func (w *bufferedResponseWriter) Header() http.Header { return w.header }

func (w *bufferedResponseWriter) WriteHeader(statusCode int) {
	if w.wroteHeader {
		return
	}
	w.statusCode = statusCode
	w.wroteHeader = true
}

func (w *bufferedResponseWriter) Write(body []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if len(body) == 0 {
		return 0, nil
	}
	if w.overflowed {
		return 0, errBufferedResponseLimitExceeded
	}
	bufferedBytes := int64(w.body.Len())
	writeBytes := int64(len(body))
	if w.maxBytes <= 0 || writeBytes > w.maxBytes-bufferedBytes {
		w.overflowed = true
		w.attemptedBytes = bufferedBytes + writeBytes
		return 0, errBufferedResponseLimitExceeded
	}
	return w.body.Write(body)
}

// Flush is an intentional no-op. Implementing http.Flusher keeps handlers that
// probe for it from panicking, but nothing is ever pushed to the client early:
// this middleware does not support streaming responses. See the constructor's
// WARNING.
func (w *bufferedResponseWriter) Flush() {}

// flushTo replays the buffered response onto destination: headers first, then
// the status code, then the body.
//
// Because the entire body is already buffered, flushTo sets an explicit
// Content-Length so responses larger than net/http's ~2KB sniff buffer are sent
// with a fixed length instead of falling back to chunked transfer encoding. It
// only does so when the handler did not set Content-Length itself (a handler
// value is never overwritten), the status code permits a body, and the request
// method is not HEAD — for HEAD the buffered body is discarded by net/http and
// its length would not describe the response the client receives.
func (w *bufferedResponseWriter) flushTo(destination http.ResponseWriter, r *http.Request) {
	header := destination.Header()
	for key, values := range w.header {
		header[key] = append([]string(nil), values...)
	}
	statusCode := w.statusCode
	if !w.wroteHeader {
		statusCode = http.StatusOK
	}
	if header.Get("Content-Length") == "" && bodyAllowedForStatus(statusCode) &&
		(r == nil || r.Method != http.MethodHead) {
		header.Set("Content-Length", strconv.Itoa(w.body.Len()))
	}
	destination.WriteHeader(statusCode)
	_, _ = destination.Write(w.body.Bytes())
}

// bodyAllowedForStatus mirrors net/http's unexported helper of the same name:
// 1xx informational, 204 No Content, and 304 Not Modified responses must not
// carry a body, so no Content-Length should be emitted for them.
func bodyAllowedForStatus(status int) bool {
	switch {
	case status >= 100 && status <= 199:
		return false
	case status == http.StatusNoContent:
		return false
	case status == http.StatusNotModified:
		return false
	}
	return true
}

var _ http.ResponseWriter = (*bufferedResponseWriter)(nil)
var _ http.Flusher = (*bufferedResponseWriter)(nil)
var _ commitBeforeFlushTransaction = (*common_persistence.GormTransactionWithHooks)(nil)
