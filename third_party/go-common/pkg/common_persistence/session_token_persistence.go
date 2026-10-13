package common_persistence

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ============================================================================
// SESSION TOKEN CRUD METHODS
// ============================================================================

// CreateSessionToken executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) CreateSessionToken(ctx context.Context, token *common_persistence_models.SessionToken) error {
	db := h.getDBFromContext(ctx)
	create := func(tx *gorm.DB) error {
		result := tx.Create(token)
		if result.Error != nil {
			return databaseErrorWithFields(result.Error,
				zap.String(logkeys.Operation, "create_session_token"),
				zap.String(logkeys.Stage, "insert_token"),
				zap.Uint64(appLogger.KeyUserID, token.UserID),
				zap.Stringer(appLogger.KeyTokenType, token.TokenType))
		}

		if !token.RecordLogin || token.TokenType != proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION {
			return nil
		}

		loginAt := token.IssuedAt.UTC()
		result = tx.Model(&common_persistence_models.User{}).
			Where("id = ?", token.UserID).
			UpdateColumn("last_login_at", gorm.Expr("GREATEST(COALESCE(last_login_at, ?), ?)", loginAt, loginAt))
		if result.Error != nil {
			return databaseErrorWithFields(result.Error,
				zap.String(logkeys.Operation, "create_session_token"),
				zap.String(logkeys.Stage, "record_last_login"),
				zap.Uint64(appLogger.KeyUserID, token.UserID))
		}
		if result.RowsAffected != 1 {
			return appLogger.InvariantError("fresh_session_user_exists", nil,
				zap.String(logkeys.Operation, "create_session_token"),
				zap.String(logkeys.Stage, "record_last_login"),
				zap.Uint64(appLogger.KeyUserID, token.UserID),
				zap.Int64(appLogger.KeyRows, result.RowsAffected))
		}
		return nil
	}

	if !token.RecordLogin || token.TokenType != proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION {
		return create(db)
	}
	return db.Transaction(create)
}

// sessionTokenWithOwner is a query projection, not a persisted model.
type sessionTokenWithOwner struct {
	common_persistence_models.SessionToken
	OwnerEmail *string `gorm:"column:token_owner_email"`
}

// GetValidSessionToken executes this method as part of the persistence flow for transactional database operations and repository access.
//
// A revocation takes effect at its revoked_at instant (like expires_at): a token
// with a future revoked_at is pending revocation and still validates until then.
// Sliding-session refresh writes future-dated revocations so in-flight requests
// carrying a just-replaced token survive the cookie switchover; explicit logout
// revokes at now and takes effect immediately.
func (h *CommonPersistenceHandler[U]) GetValidSessionToken(ctx context.Context, token string) (*common_persistence_models.SessionToken, error) {
	const tokensTable = common_persistence_models.TableNameSessionTokens
	const usersTable = common_persistence_models.TableNameUsers

	var sessionToken sessionTokenWithOwner
	now := time.Now().UTC()
	db := h.getDBFromContext(ctx).
		Model(&common_persistence_models.SessionToken{}).
		Select(tokensTable + ".*, token_owner.email AS token_owner_email").
		Joins("LEFT JOIN " + usersTable + " AS token_owner ON token_owner.id = " + tokensTable + ".user_id AND token_owner.deleted_at IS NULL").
		Session(&gorm.Session{})

	result := db.Where(
		tokensTable+".token = ? AND expires_at > ? AND (revoked_at IS NULL OR revoked_at > ?)",
		token, now, now,
	).First(&sessionToken)

	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			// Check if token exists but is invalid
			var expiredToken sessionTokenWithOwner
			expiredCheck := db.Where(tokensTable+".token = ?", token).First(&expiredToken)
			if expiredCheck.Error == nil {
				// A row excluded by the primary filter is necessarily expired or
				// carries an effective (not future-dated) revocation. A future
				// revoked_at is pending revocation and still passes the primary
				// filter, so it never reaches here; used_at is not part of that
				// filter either, so a used-but-live token matches above and is
				// classified by the IsValid check below.
				if expiredToken.RevokedAt != nil && !expiredToken.RevokedAt.After(now) {
					return nil, h.rejectedSessionTokenError(ctx, &expiredToken, "revoked", ErrTokenRevoked)
				}
				if expiredToken.ExpiresAt.Before(now) {
					return nil, h.rejectedSessionTokenError(ctx, &expiredToken, "expired", ErrTokenExpired)
				}
				// The primary and classification queries should agree on why the row
				// was excluded. Propagate the invariant so the owning boundary records
				// the broken database-state assumption exactly once at Error.
				return nil, appLogger.InvariantError("rejected_session_token_has_exclusion_reason", nil,
					zap.String(logkeys.Stage, "classify_rejected_session_token"),
					zap.Uint64(appLogger.KeyTokenID, expiredToken.ID),
					zap.Uint64(appLogger.KeyUserID, expiredToken.UserID),
					zap.String(appLogger.KeyToken, expiredToken.Token),
					zap.Stringer(appLogger.KeyTokenType, expiredToken.TokenType),
					zap.Time(appLogger.KeyExpiresAt, expiredToken.ExpiresAt),
					zap.String(logkeys.Expected, "expired or effectively revoked token"),
					zap.String(logkeys.Actual, "live unrevoked token excluded by validation query"))
			}
			if !errors.Is(expiredCheck.Error, gorm.ErrRecordNotFound) {
				return nil, databaseErrorWithFields(expiredCheck.Error,
					zap.String(logkeys.Operation, "classify_rejected_session_token"),
					zap.String(appLogger.KeyToken, token))
			}
			// No row for this token at all — indistinguishable from a genuinely
			// bogus token at this layer (account deletion revokes rather than
			// removes rows, so a not-found token was never issued or is malformed).
			h.logSessionTokenRejected(ctx, reasonNotFound, zap.String(appLogger.KeyToken, token))
			return nil, ErrSessionTokenNotFound
		}
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_valid_session_token"),
			zap.String(appLogger.KeyToken, token))
	}

	// The row passed the not-expired / not-revoked filter above, so a single-use
	// token that is no longer valid was already consumed (e.g. the verification
	// link was clicked twice). Report "already used" rather than the generic
	// "invalid token" so the user gets an accurate message.
	if !sessionToken.IsValid() {
		if sessionToken.UsedAt != nil {
			return nil, h.rejectedSessionTokenError(ctx, &sessionToken, "already_used", ErrTokenAlreadyUsed)
		}
		return nil, h.rejectedSessionTokenError(ctx, &sessionToken, "failed_is_valid", ErrInvalidToken)
	}

	return &sessionToken.SessionToken, nil
}

// rejectedSessionTokenError emits a structured record for a security/session
// token that exists in the database but failed validation. It logs the
// token's identifying fields (id, owning user, type) and full lifecycle
// timestamps alongside the classified reason, so operators can definitively
// tell an expired-but-otherwise-valid token apart from one that was revoked,
// already used, or malformed. token_ip / token_user_agent are the client that
// minted the token (from the row); the current requester's IP is added by
// logSessionTokenRejected. The same diagnostics accompany the returned error.
func (h *CommonPersistenceHandler[U]) rejectedSessionTokenError(ctx context.Context, t *sessionTokenWithOwner, reason string, cause error) error {
	fields := []zap.Field{
		zap.Uint64(appLogger.KeyTokenID, t.ID),
		zap.Uint64(appLogger.KeyUserID, t.UserID),
		zap.String(appLogger.KeyToken, t.Token),
		zap.Stringer(appLogger.KeyTokenType, t.TokenType),
		zap.Time(appLogger.KeyIssuedAt, t.IssuedAt),
		zap.Time(appLogger.KeyExpiresAt, t.ExpiresAt),
	}
	if t.IPAddress != "" {
		fields = append(fields, zap.String(appLogger.KeyTokenIP, t.IPAddress))
	}
	if t.UserAgent != "" {
		fields = append(fields, zap.String(appLogger.KeyTokenUserAgent, t.UserAgent))
	}
	if t.UsedAt != nil {
		fields = append(fields, zap.Time(appLogger.KeyUsedAt, *t.UsedAt))
	}
	if t.RevokedAt != nil {
		fields = append(fields, zap.Time(appLogger.KeyRevokedAt, *t.RevokedAt))
	}

	// The left join preserves the token when its owner is missing or deleted.
	if t.OwnerEmail == nil {
		fields = append(fields, zap.String("token_owner_lookup_status", "not_found"))
	} else {
		fields = append(fields, zap.String(appLogger.KeyEmail, *t.OwnerEmail))
	}
	h.logSessionTokenRejected(ctx, reason, fields...)
	return appLogger.WithErrorFields(cause, append(fields, zap.String(logkeys.Reason, reason))...)
}

// reasonNotFound classifies a rejection where no token row exists — it cannot be
// tied to an id/user and is trivially produced by clients probing the public
// verification endpoint, so it is logged at Debug rather than Info.
const reasonNotFound = "not_found"

// logSessionTokenRejected writes the shared "token rejected during validation"
// record, always annotating it with the classified reason and — comparing
// against any token_ip in fields to spot a token replayed elsewhere — the
// current request's client IP. UserIdType is an alias for uint64, so this reads
// the same request context the middleware populated. Unknown tokens remain
// Debug noise; ordinary expiry is Info. Revoked, already-used, and malformed
// rows are Warn because replay or atypical-token spikes are security signals.
func (h *CommonPersistenceHandler[U]) logSessionTokenRejected(ctx context.Context, reason string, fields ...zap.Field) {
	record := make([]zap.Field, 0, len(fields)+2)
	record = append(record,
		zap.String(logkeys.Operation, "validate_session_token"),
		zap.String(logkeys.Reason, reason))
	record = append(record, fields...)
	if ip, ok := scontext.GetClientIP(ctx); ok && ip != "" {
		record = append(record, zap.String(logkeys.ClientIP, ip))
	}
	logger := appLogger.For(ctx, h.logger, loggerName)
	if logger == nil {
		return
	}
	const msg = "Session token rejected during validation"
	switch reason {
	case reasonNotFound:
		if ce := logger.Check(zap.DebugLevel, msg); ce != nil {
			ce.Write(record...)
		}
	case "expired":
		logger.Info(msg, record...)
	case "revoked", "already_used", "invalid", "failed_is_valid":
		logger.Warn(msg, record...)
	default:
		logger.Warn(msg, record...)
	}
}

// ============================================================================
// TOKEN MANAGEMENT METHODS
// ============================================================================

// ConsumeSecurityToken validates and consumes a single-use security token of tokenType under
// the account lock order: the owning users row is locked FOR UPDATE before the token row. Every
// flow that consumes a token and then writes the account (email verification, password reset,
// invitation acceptance) must use it, because transactions that lock the user first and then
// update the user's tokens (signup-credential discard, deletion) would otherwise deadlock with
// one that locked the token first and then touched the user row.
//
// The owner is found with an unlocked read, the user is locked, and the token is then re-read FOR
// UPDATE and re-validated: another transaction may have consumed or revoked it while this one
// waited. The caller must supply an ambient transaction; the user lock is held until it ends.
//
// Both reads filter on tokenType, so a token of another type is ErrSessionTokenNotFound before
// any lock is taken: it neither locks an unrelated account nor reveals that token's state.
//
// Returns the consumed row, or ErrSessionTokenNotFound, ErrTokenRevoked, ErrTokenExpired, or
// ErrTokenAlreadyUsed.
func (h *CommonPersistenceHandler[U]) ConsumeSecurityToken(ctx context.Context, token string, tokenType proto_models.TokenType) (*common_persistence_models.SessionToken, error) {
	if tx, ok := scontext.GetTransaction(ctx); !ok || tx == nil || tx.GetDB() == nil {
		return nil, ErrSecurityTokenTransactionRequired
	}
	const tokensTable = common_persistence_models.TableNameSessionTokens
	const usersTable = common_persistence_models.TableNameUsers
	db := h.getDBFromContext(ctx)

	var owner common_persistence_models.SessionToken
	if err := db.Select("user_id").Where("token = ? AND token_type = ?", token, tokenType).Take(&owner).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			h.logSessionTokenRejected(ctx, reasonNotFound, zap.String(appLogger.KeyToken, token))
			return nil, ErrSessionTokenNotFound
		}
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "consume_security_token"),
			zap.String(logkeys.Stage, "read_token_owner"),
			zap.String(appLogger.KeyToken, token))
	}
	// A deleted owner has no row left to lock; deletion revoked its tokens, which the
	// classification below reports.
	if _, err := h.GetUserForUpdate(ctx, owner.UserID); err != nil && !errors.Is(err, ErrUserNotFound) {
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "consume_security_token"),
			zap.String(logkeys.Stage, "lock_token_owner"),
			zap.Uint64(appLogger.KeyUserID, owner.UserID))
	}

	var locked sessionTokenWithOwner
	err := db.Model(&common_persistence_models.SessionToken{}).
		Select(tokensTable+".*, token_owner.email AS token_owner_email").
		Joins("LEFT JOIN "+usersTable+" AS token_owner ON token_owner.id = "+tokensTable+".user_id AND token_owner.deleted_at IS NULL").
		Clauses(clause.Locking{Strength: "UPDATE", Table: clause.Table{Name: tokensTable}}).
		Where(tokensTable+".token = ? AND "+tokensTable+".token_type = ?", token, tokenType).
		Take(&locked).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			h.logSessionTokenRejected(ctx, reasonNotFound, zap.String(appLogger.KeyToken, token))
			return nil, ErrSessionTokenNotFound
		}
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "consume_security_token"),
			zap.String(logkeys.Stage, "lock_token"),
			zap.String(appLogger.KeyToken, token))
	}
	now := time.Now().UTC()
	switch {
	case locked.RevokedAt != nil && !locked.RevokedAt.After(now):
		return nil, h.rejectedSessionTokenError(ctx, &locked, "revoked", ErrTokenRevoked)
	case !now.Before(locked.ExpiresAt):
		return nil, h.rejectedSessionTokenError(ctx, &locked, "expired", ErrTokenExpired)
	case locked.UsedAt != nil:
		return nil, h.rejectedSessionTokenError(ctx, &locked, "already_used", ErrTokenAlreadyUsed)
	}
	// The account lock protects the address comparison through the caller's
	// verification write. Unbound tokens from older deployments must be reissued.
	if tokenType == proto_models.TokenType_TOKEN_TYPE_EMAIL_VERIFICATION &&
		(locked.BoundEmail == "" || locked.OwnerEmail == nil ||
			common_persistence_models.NormalizeUserEmail(locked.BoundEmail) != common_persistence_models.NormalizeUserEmail(*locked.OwnerEmail)) {
		return nil, ErrInvalidToken
	}

	// Guard the write as well as the locked read: only one consumer may win.
	result := db.Model(&common_persistence_models.SessionToken{}).Where("id = ? AND used_at IS NULL", locked.ID).Update("used_at", now)
	if err := result.Error; err != nil {
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "consume_security_token"),
			zap.String(logkeys.Stage, "mark_token_used"),
			zap.Uint64(appLogger.KeyTokenID, locked.ID))
	}
	if result.RowsAffected != 1 {
		return nil, ErrTokenAlreadyUsed
	}
	locked.UsedAt = &now
	return &locked.SessionToken, nil
}

// RevokeAllUserTokens executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) RevokeAllUserTokens(ctx context.Context, userID uint64, tokenType proto_models.TokenType) error {
	db := h.getDBFromContext(ctx)
	now := time.Now().UTC()

	// Force semantics: pending (future-dated) refresh revocations are pulled
	// forward too, so password changes and admin actions take effect immediately.
	result := db.Model(&common_persistence_models.SessionToken{}).
		Where("user_id = ? AND token_type = ? AND (revoked_at IS NULL OR revoked_at > ?)", userID, tokenType, now).
		Update("revoked_at", now)

	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "revoke_user_tokens"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Stringer(appLogger.KeyTokenType, tokenType))
	}

	logger := appLogger.For(ctx, h.logger, loggerName)
	fields := []zap.Field{
		zap.String(logkeys.Operation, "revoke_user_tokens"),
		zap.Uint64(appLogger.KeyUserID, userID),
		zap.Stringer(appLogger.KeyTokenType, tokenType),
		zap.Int64(appLogger.KeyCount, result.RowsAffected),
	}
	if result.RowsAffected == 0 {
		if ce := logger.Check(zap.DebugLevel, "No active user tokens matched revocation"); ce != nil {
			ce.Write(fields...)
		}
	} else {
		logger.Info("Revoked user tokens", fields...)
	}

	return nil
}

// RevokeSessionToken executes this method as part of the persistence flow for transactional database operations and repository access.
//
// It force-revokes effective immediately: a pending (future-dated) refresh
// revocation is pulled forward to now, so logout always wins over a scheduled
// refresh revocation. Only tokens whose revocation has already elapsed count
// as "already revoked".
func (h *CommonPersistenceHandler[U]) RevokeSessionToken(ctx context.Context, token string) error {
	db := h.getDBFromContext(ctx)
	now := time.Now().UTC()

	result := db.Model(&common_persistence_models.SessionToken{}).
		Where("token = ? AND (revoked_at IS NULL OR revoked_at > ?)", token, now).
		Update("revoked_at", now)

	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "revoke_session_token"),
			zap.String(appLogger.KeyToken, token))
	}

	if result.RowsAffected == 0 {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("Token not found or already revoked",
				zap.String(logkeys.Operation, "revoke_session_token"),
				zap.String(appLogger.KeyToken, token))
		}
		return ErrSessionTokenNotFound
	}

	appLogger.For(ctx, h.logger, loggerName).Info("Revoked session token",
		zap.String(logkeys.Operation, "revoke_session_token"),
		zap.String(appLogger.KeyToken, token))

	return nil
}

// RevokeSessionTokenAt schedules a session token's revocation at the given
// instant. This is the sliding-session refresh primitive: a future revokeAt is a
// pending revocation, so in-flight requests carrying the replaced token survive
// the cookie switchover. It never overrides an existing revocation — pending or
// elapsed — so it cannot postpone a revocation another flow already wrote; use
// RevokeSessionToken to force immediate revocation.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - token: Input consumed by this operation.
//   - revokeAt: Instant at which the token stops being accepted.
func (h *CommonPersistenceHandler[U]) RevokeSessionTokenAt(ctx context.Context, token string, revokeAt time.Time) error {
	db := h.getDBFromContext(ctx)

	result := db.Model(&common_persistence_models.SessionToken{}).
		Where("token = ? AND revoked_at IS NULL", token).
		Update("revoked_at", revokeAt.UTC())

	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "schedule_session_token_revocation"),
			zap.String(appLogger.KeyToken, token),
			zap.Time(appLogger.KeyRevokedAt, revokeAt.UTC()))
	}

	if result.RowsAffected == 0 {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("Token not found or already revoked",
				zap.String(logkeys.Operation, "schedule_session_token_revocation"),
				zap.String(appLogger.KeyToken, token))
		}
		return ErrSessionTokenNotFound
	}

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("Scheduled session token revocation",
			zap.String(logkeys.Operation, "schedule_session_token_revocation"),
			zap.String(appLogger.KeyToken, token),
			zap.Time(appLogger.KeyRevokedAt, revokeAt.UTC()))
	}

	return nil
}

// RevokeAllSessionTokensForUser executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) RevokeAllSessionTokensForUser(ctx context.Context, userID uint64) error {
	_, err := h.RevokeAllSessionTokensForUserAndReturnTokens(ctx, userID)
	return err
}

// RevokeAllSessionTokensForUserAndReturnTokens revokes all active authentication tokens for a user.
// A single UPDATE ... RETURNING revokes the tokens and yields their raw values,
// halving the round trips of the previous SELECT-then-UPDATE and removing the
// race between the two statements.
func (h *CommonPersistenceHandler[U]) RevokeAllSessionTokensForUserAndReturnTokens(ctx context.Context, userID uint64) ([]string, error) {
	db := h.getDBFromContext(ctx)
	now := time.Now().UTC()

	// Only revoke AUTHENTICATION tokens, not verification or password reset tokens.
	// Force semantics: pending (future-dated) refresh revocations are pulled
	// forward too, so "logout everywhere" and admin bans take effect immediately.
	rawTokens := []string{}
	if err := db.Raw(
		fmt.Sprintf("UPDATE %s SET revoked_at = ? WHERE user_id = ? AND token_type = ? AND expires_at > ? AND (revoked_at IS NULL OR revoked_at > ?) RETURNING token",
			common_persistence_models.TableNameSessionTokens),
		now, userID, proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION, now, now,
	).Scan(&rawTokens).Error; err != nil {
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "revoke_all_session_tokens_for_user"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Stringer(appLogger.KeyTokenType, proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION))
	}

	if len(rawTokens) == 0 {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("No active session tokens to revoke",
				zap.String(logkeys.Operation, "revoke_all_session_tokens_for_user"),
				zap.Uint64(appLogger.KeyUserID, userID))
		}
		return []string{}, nil
	}

	appLogger.For(ctx, h.logger, loggerName).Info("Revoked all session tokens for user",
		zap.String(logkeys.Operation, "revoke_all_session_tokens_for_user"),
		zap.Uint64(appLogger.KeyUserID, userID),
		zap.Int(appLogger.KeyCount, len(rawTokens)))

	return rawTokens, nil
}

// ============================================================================
// TOKEN CLEANUP METHODS
// ============================================================================

// DeleteExpiredSessionTokens executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) DeleteExpiredSessionTokens(ctx context.Context) error {
	startTime := time.Now().UTC()

	queryTimeout := h.runtime().DBQueryTimeout()
	queryCtx := ctx
	cancel := func() {}
	bulkTimeout := time.Duration(0)
	if queryTimeout > 0 {
		bulkTimeout = queryTimeout * 2
		queryCtx, cancel = context.WithTimeout(ctx, bulkTimeout)
	}
	defer cancel()

	db := h.getDBFromContext(queryCtx)

	// Keep the unused, unrevoked invitation so an expired link can be resent.
	// Resending revokes its predecessor; used/revoked tokens follow normal cleanup.
	result := db.WithContext(queryCtx).
		Where("expires_at < ?", time.Now().UTC()).
		Where("token_type != ? OR used_at IS NOT NULL OR revoked_at IS NOT NULL", proto_models.TokenType_TOKEN_TYPE_ACCOUNT_INVITATION).
		Delete(&common_persistence_models.SessionToken{})

	// Record metrics
	queryDuration := time.Since(startTime)
	if h.metricsHandler != nil && appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("Bulk delete operation metrics",
			zap.String(logkeys.Operation, "delete_expired_session_tokens"),
			appLogger.DurationMS(logkeys.DurationMS, queryDuration),
			appLogger.DurationSeconds(appLogger.KeyTimeoutSeconds, bulkTimeout),
			zap.Int64(appLogger.KeyRows, result.RowsAffected),
			zap.Bool(appLogger.KeySuccess, result.Error == nil))
	}

	if bulkTimeout > 0 && errors.Is(queryCtx.Err(), context.DeadlineExceeded) {
		return appLogger.WithErrorLevel(
			errors.Join(ErrDatabaseError, queryCtx.Err()),
			zapcore.WarnLevel,
			zap.String(logkeys.Operation, "delete_expired_session_tokens"),
			appLogger.DurationSeconds(appLogger.KeyTimeoutSeconds, bulkTimeout))
	}

	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "delete_expired_session_tokens"),
			appLogger.DurationMS(logkeys.DurationMS, queryDuration))
	}

	logger := appLogger.For(ctx, h.logger, loggerName)
	fields := []zap.Field{
		zap.String(logkeys.Operation, "delete_expired_session_tokens"),
		zap.Int64(appLogger.KeyCount, result.RowsAffected),
		appLogger.DurationMS(logkeys.DurationMS, queryDuration),
	}
	if result.RowsAffected == 0 {
		if ce := logger.Check(zap.DebugLevel, "Expired token cleanup completed"); ce != nil {
			ce.Write(fields...)
		}
	} else {
		logger.Info("Expired token cleanup completed", fields...)
	}
	return nil
}

// GetActiveTokensByUserID executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) GetActiveTokensByUserID(ctx context.Context, userID uint64) ([]*common_persistence_models.SessionToken, error) {
	db := h.getDBFromContext(ctx)
	var tokens []*common_persistence_models.SessionToken
	result := db.Where("user_id = ? AND expires_at > ? AND revoked_at IS NULL", userID, time.Now().UTC()).
		Order("issued_at DESC").
		Find(&tokens)
	if result.Error != nil {
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_active_tokens_by_user_id"),
			zap.Uint64(appLogger.KeyUserID, userID))
	}
	return tokens, nil
}

// GetActiveSessionTokensByUserID retrieves active authentication session tokens for a user.
func (h *CommonPersistenceHandler[U]) GetActiveSessionTokensByUserID(ctx context.Context, userID uint64) ([]*common_persistence_models.SessionToken, error) {
	db := h.getDBFromContext(ctx)
	var tokens []*common_persistence_models.SessionToken
	result := db.Where("user_id = ? AND token_type = ? AND expires_at > ? AND revoked_at IS NULL",
		userID, proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION, time.Now().UTC()).
		Order("issued_at DESC").
		Find(&tokens)
	if result.Error != nil {
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_active_session_tokens_by_user_id"),
			zap.Uint64(appLogger.KeyUserID, userID))
	}
	return tokens, nil
}

func buildActiveSessionTokensByUserIDBaseQuery(db *gorm.DB, userID uint64, now time.Time) *gorm.DB {
	return db.Model(&common_persistence_models.SessionToken{}).
		Where("user_id = ? AND token_type = ? AND expires_at > ? AND revoked_at IS NULL",
			userID, proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION, now)
}

func buildActiveSessionTokensByUserIDQuery(db *gorm.DB, userID uint64, now time.Time) *gorm.DB {
	return buildActiveSessionTokensByUserIDBaseQuery(db, userID, now).Order("issued_at DESC")
}

// ListActiveSessionTokensByUserID retrieves active authentication session tokens for a user with pagination.
func (h *CommonPersistenceHandler[U]) ListActiveSessionTokensByUserID(ctx context.Context, userID uint64, pagination *proto_models.PaginationRequest) ([]*common_persistence_models.SessionToken, *proto_models.PaginationResponse, error) {
	if err := requirePageLimit("list_active_session_tokens_by_user_id", pagination); err != nil {
		return nil, nil, err
	}
	db := h.getDBFromContext(ctx)

	now := time.Now().UTC()
	baseQuery := buildActiveSessionTokensByUserIDBaseQuery(db, userID, now)

	includeTotalCount := common_util.ShouldIncludeTotalCount(pagination)
	var totalCount int64
	if includeTotalCount {
		if err := baseQuery.Count(&totalCount).Error; err != nil {
			return nil, nil, databaseErrorWithFields(err,
				zap.String(logkeys.Operation, "count_active_session_tokens_by_user_id"),
				zap.Uint64(appLogger.KeyUserID, userID))
		}
	}

	offset := common_util.PaginationOffset(pagination.Page, pagination.Limit)
	var tokens []*common_persistence_models.SessionToken
	result := baseQuery.
		Order("issued_at DESC").
		Offset(offset).
		Limit(common_util.PaginationQueryLimit(pagination.Limit, includeTotalCount)).
		Find(&tokens)
	if result.Error != nil {
		return nil, nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "list_active_session_tokens_by_user_id"),
			zap.Uint64(appLogger.KeyUserID, userID))
	}

	tokens, hasNextPage := common_util.TrimPaginationProbe(tokens, pagination.Limit, includeTotalCount)
	var paginationResponse *proto_models.PaginationResponse
	if includeTotalCount {
		paginationResponse = common_util.CreatePaginationResponse(totalCount, pagination.Limit, pagination.Page)
	} else {
		paginationResponse = common_util.CreatePaginationResponseWithoutTotalCount(pagination.Limit, pagination.Page, hasNextPage)
	}

	return tokens, paginationResponse, nil
}

// RevokeSessionTokenByIDForUser revokes one active authentication session owned by a user.
func (h *CommonPersistenceHandler[U]) RevokeSessionTokenByIDForUser(ctx context.Context, userID uint64, sessionID uint64) (string, error) {
	db := h.getDBFromContext(ctx)
	now := time.Now().UTC()

	// Force semantics: a session pending a refresh revocation can still be
	// force-revoked immediately.
	var token common_persistence_models.SessionToken
	err := db.Where("id = ? AND user_id = ? AND token_type = ? AND expires_at > ? AND (revoked_at IS NULL OR revoked_at > ?)",
		sessionID, userID, proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION, now, now).
		First(&token).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", ErrSessionTokenNotFound
		}
		return "", databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "find_session_token_for_revocation"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Uint64(appLogger.KeyTokenID, sessionID))
	}

	result := db.Model(&common_persistence_models.SessionToken{}).
		Where("id = ? AND user_id = ? AND token_type = ? AND expires_at > ? AND (revoked_at IS NULL OR revoked_at > ?)",
			sessionID, userID, proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION, now, now).
		Update("revoked_at", now)
	if result.Error != nil {
		return "", databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "revoke_session_token_by_id"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Uint64(appLogger.KeyTokenID, sessionID))
	}
	if result.RowsAffected == 0 {
		return "", ErrSessionTokenNotFound
	}

	return token.Token, nil
}

// RevokeOtherSessionTokensForUser revokes active authentication sessions except the current token.
func (h *CommonPersistenceHandler[U]) RevokeOtherSessionTokensForUser(ctx context.Context, userID uint64, currentToken string) ([]string, error) {
	db := h.getDBFromContext(ctx)
	now := time.Now().UTC()

	// A single UPDATE ... RETURNING revokes the other sessions and yields their
	// raw tokens, halving the round trips of the previous SELECT-then-UPDATE
	// and removing the race between the two statements. Force semantics: pending
	// (future-dated) refresh revocations are pulled forward too.
	rawTokens := []string{}
	if err := db.Raw(
		fmt.Sprintf("UPDATE %s SET revoked_at = ? WHERE user_id = ? AND token_type = ? AND token <> ? AND expires_at > ? AND (revoked_at IS NULL OR revoked_at > ?) RETURNING token",
			common_persistence_models.TableNameSessionTokens),
		now, userID, proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION, currentToken, now, now,
	).Scan(&rawTokens).Error; err != nil {
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "revoke_other_session_tokens"),
			zap.Uint64(appLogger.KeyUserID, userID))
	}

	return rawTokens, nil
}
