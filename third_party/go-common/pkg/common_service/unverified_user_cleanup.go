package common_service

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// startUnverifiedUserCleanup executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) startUnverifiedUserCleanup() {
	interval := h.cleanupInterval()
	if interval <= 0 && h.runtimeConfig == nil {
		return
	}
	if h.unverifiedUserCleanupCtx != nil {
		return
	}

	h.unverifiedUserCleanupCtx, h.unverifiedUserCleanupCancel = context.WithCancel(context.Background())
	h.unverifiedUserCleanupWg.Add(1)

	logger := appLogger.For(context.Background(), h.logger, unverifiedUserCleanupLoggerName)
	go func() {
		defer h.unverifiedUserCleanupWg.Done()

		ticker := time.NewTicker(cleanupPollInterval(interval))
		lastRun := time.Now()
		defer ticker.Stop()

		logger.Info("Unverified user cleanup routine started",
			appLogger.DurationSeconds(appLogger.KeyIntervalSeconds, interval))

		// The first sweep runs at the first tick rather than immediately, so
		// starting the worker (e.g. at handler construction) never issues DB
		// deletes synchronously with startup.
		for {
			select {
			case <-h.unverifiedUserCleanupCtx.Done():
				logger.Info("Unverified user cleanup routine stopping")
				return
			case <-ticker.C:
				interval = h.cleanupInterval()
				ticker.Reset(cleanupPollInterval(interval))
				if interval <= 0 || time.Since(lastRun) < interval {
					continue
				}
				lastRun = time.Now()
				h.runUnverifiedUserCleanup(h.unverifiedUserCleanupCtx, h.logger)
			}
		}
	}()
}

// runUnverifiedUserCleanup executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) runUnverifiedUserCleanup(ctx context.Context, logger *zap.Logger) {
	cleanupConfig := h.runtimeView()
	if cleanupConfig == nil {
		appLogger.LogInvariantViolation(appLogger.For(ctx, logger, unverifiedUserCleanupLoggerName), "unverified_user_cleanup_config_view_non_nil",
			nil,
			zap.String(logkeys.Stage, "unverified_user_cleanup"),
			zap.String(logkeys.Expected, "non-nil cleanup configuration view"),
			zap.String(logkeys.Actual, "nil"),
			zap.String(logkeys.Fallback, "skip cleanup sweep"))
		return
	}
	validity := cleanupConfig.VerificationTokenValidity()
	if validity <= 0 {
		appLogger.LogInvariantViolation(appLogger.For(ctx, logger, unverifiedUserCleanupLoggerName), "verification_token_validity_positive",
			nil,
			zap.String(logkeys.Stage, "unverified_user_cleanup"),
			zap.String(logkeys.Expected, "positive verification token validity"),
			appLogger.DurationSeconds(appLogger.KeyActualSeconds, validity),
			zap.String(logkeys.Fallback, "skip cleanup sweep"))
		return
	}
	// Every log line this sweep writes is correlatable with the configuration
	// generation it was sampled from.
	logger = appLogger.WithConfigGeneration(
		appLogger.For(ctx, logger, unverifiedUserCleanupLoggerName),
		cleanupConfig,
	)

	cutoff := time.Now().UTC().Add(-validity)
	cleanupTimeout := cleanupConfig.UnverifiedUserCleanupTimeout()
	cleanupCtx, cancel := context.WithTimeout(ctx, cleanupTimeout)
	defer cancel()

	userIDs, err := h.commonPersistenceHandler.ListUnverifiedUserIDs(cleanupCtx, cutoff, cleanupConfig.UnverifiedUserCleanupBatchSize())
	if err != nil {
		logger.Warn("Failed to list unverified users",
			zap.Error(err),
			zap.String(logkeys.Stage, "list_unverified_users"),
			zap.String(logkeys.Fallback, "retry next cleanup sweep"),
			zap.Time(appLogger.KeyCutoffTime, cutoff))
		return
	}

	if len(userIDs) == 0 {
		return
	}

	deletedCount := 0
	for _, userID := range userIDs {
		if cleanupCtx.Err() != nil {
			break
		}
		shouldDelete, checkErr := h.shouldDeleteUnverifiedUser(cleanupCtx, userID)
		if checkErr != nil {
			logger.Warn("Failed to check unverified user status",
				zap.Error(checkErr),
				zap.Uint64(appLogger.KeyUserID, userID))
			continue
		}
		if !shouldDelete {
			continue
		}

		deleted, err := h.deleteUnverifiedUser(cleanupCtx, userID)
		if err != nil {
			logger.Warn("Failed to delete unverified user",
				zap.Error(err),
				zap.Uint64(appLogger.KeyUserID, userID))
			continue
		}
		if !deleted {
			continue
		}
		deletedCount++
	}

	if deletedCount > 0 {
		logger.Info("Unverified user cleanup processed",
			zap.Int(appLogger.KeyCandidateCount, len(userIDs)),
			zap.Int(appLogger.KeyDeletedCount, deletedCount),
			zap.Time(appLogger.KeyCutoffTime, cutoff))
	}
}

// shouldDeleteUnverifiedUser executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) shouldDeleteUnverifiedUser(ctx context.Context, userID uint64) (bool, error) {
	ev, err := h.commonPersistenceHandler.GetEmailVerificationByUserID(ctx, userID)
	if err != nil {
		if errors.Is(err, persistence.ErrEmailVerificationNotFound) {
			return false, nil
		}
		return false, err
	}

	return ev.VerifiedAt == nil, nil
}

// deleteUnverifiedUser executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
//
// The application's OnUserDeleted callback and the core deletion run in a
// single transaction, and the core deletion re-checks the verification status
// atomically, so a user who verifies mid-cleanup keeps both their core and
// application records.
//
// Returns:
// - `result1`: reports whether the user was deleted.
// - `err`: reports any failure encountered while running this operation.
func (h *CommonServiceHandler[U]) deleteUnverifiedUser(ctx context.Context, userID uint64) (bool, error) {
	tx, err := h.commonPersistenceHandler.BeginTransaction(ctx)
	if err != nil {
		h.logUnverifiedUserCleanupFailure(ctx, "begin_transaction", userID, err)
		return false, err
	}
	gormTx := &persistence.GormTransactionWithHooks{Tx: tx}
	transactionClosed := false
	defer func() {
		if transactionClosed {
			return
		}
		if rollbackErr := gormTx.Rollback(); rollbackErr != nil {
			h.logUnverifiedUserCleanupFailure(ctx, "rollback_on_exit", userID, rollbackErr)
		}
	}()
	// Clone the carrier first: WithTransaction mutates it in place, and the sweep's context must
	// not see this transaction after it finishes.
	txCtx := scontext.CopySRouterContext[common_service_models.UserIdType, U](ctx, ctx)
	txCtx = scontext.WithTransaction[common_service_models.UserIdType, U](txCtx, gormTx)

	// Establish user-before-children locking before application callbacks too.
	// A candidate that disappeared while the sweep waited needs no cleanup.
	if _, err := h.commonPersistenceHandler.GetUserForUpdate(txCtx, userID); err != nil {
		if errors.Is(err, persistence.ErrUserNotFound) {
			return false, nil
		}
		h.logUnverifiedUserCleanupFailure(ctx, "lock_user", userID, err)
		return false, err
	}

	if h.userCallbacks != nil && h.userCallbacks.SyncCallbacks != nil && h.userCallbacks.SyncCallbacks.OnUserDeleted != nil {
		if err := h.userCallbacks.SyncCallbacks.OnUserDeleted(txCtx, common_service_models.UserIdType(userID)); err != nil {
			h.logUnverifiedUserCleanupFailure(ctx, "on_user_deleted_callback", userID, err)
			return false, err
		}
	}

	deleted, err := h.commonPersistenceHandler.DeleteUserIfUnverified(txCtx, userID, true, "unverified account cleanup")
	if err != nil {
		h.logUnverifiedUserCleanupFailure(ctx, "delete_user_if_unverified", userID, err)
		return false, err
	}
	if !deleted {
		// The user verified after being selected as a candidate; roll back so
		// the callback's changes are undone as well.
		return false, nil
	}

	if err := gormTx.Commit(); err != nil {
		h.logUnverifiedUserCleanupFailure(ctx, "commit_transaction", userID, err)
		return false, err
	}
	transactionClosed = true

	// This maintenance path owns its transaction rather than relying on HTTP middleware, so it
	// drains every post-commit hook (including attachment-object cleanup) after durability.
	h.runCleanupPostCommitHooks(ctx, gormTx.DrainPostCommitHooks())
	return true, nil
}

// runCleanupPostCommitHooks runs hooks drained from a cleanup-owned transaction
// after its commit succeeds, on a detached bounded context. A hook failure or
// panic is logged and never affects the cleanup result or the hooks queued
// after it.
func (h *CommonServiceHandler[U]) runCleanupPostCommitHooks(ctx context.Context, hooks []persistence.PostCommitHook) {
	if len(hooks) == 0 {
		return
	}
	hookCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	for index, hook := range hooks {
		if hook == nil {
			continue
		}
		h.runCleanupPostCommitHook(hookCtx, hook, index, len(hooks))
	}
}

// runCleanupPostCommitHook runs a single drained post-commit hook, containing a
// panic so one misbehaving hook cannot terminate the process or skip the hooks
// queued after it.
func (h *CommonServiceHandler[U]) runCleanupPostCommitHook(ctx context.Context, hook persistence.PostCommitHook, index, count int) {
	defer func() {
		if recovered := recover(); recovered != nil {
			appLogger.For(ctx, h.logger, unverifiedUserCleanupLoggerName).Error("cleanup post-commit hook panicked; recovered",
				zap.String(logkeys.Operation, "unverified_user_cleanup_post_commit_hook"),
				zap.String(appLogger.KeyPanicType, fmt.Sprintf("%T", recovered)),
				zap.ByteString(appLogger.KeyStacktrace, debug.Stack()),
				zap.Int(appLogger.KeyHookIndex, index),
				zap.Int(appLogger.KeyHookCount, count))
		}
	}()
	if err := hook(ctx); err != nil {
		appLogger.LogOperationWarn(appLogger.For(ctx, h.logger, unverifiedUserCleanupLoggerName), "cleanup post-commit hook failed", "unverified_user_cleanup", err,
			zap.Int(appLogger.KeyHookIndex, index),
			zap.Int(appLogger.KeyHookCount, count))
	}
}

func (h *CommonServiceHandler[U]) logUnverifiedUserCleanupFailure(ctx context.Context, stage string, userID uint64, err error) {
	appLogger.LogOperationWarn(appLogger.For(ctx, h.logger, unverifiedUserCleanupLoggerName), "Unverified user cleanup stage failed", "unverified_user_cleanup", err,
		zap.String(appLogger.KeyCleanupStage, stage),
		zap.Uint64(appLogger.KeyUserID, userID),
		zap.Bool(appLogger.KeyDeleteAttachments, true))
}

func (h *CommonServiceHandler[U]) cleanupInterval() time.Duration {
	if view := h.runtimeView(); view != nil {
		return view.UnverifiedUserCleanupInterval()
	}
	return 0
}
func cleanupPollInterval(interval time.Duration) time.Duration {
	if interval <= 0 || interval > time.Second {
		return time.Second
	}
	return interval
}
