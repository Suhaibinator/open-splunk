package common_service

import (
	"context"
	stdErrors "errors"
	"math"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// Session refresh metric outcomes (bounded label values for session_refreshes_total).
const (
	sessionRefreshStatusRefreshed      = "refreshed"
	sessionRefreshStatusSkippedRevoked = "skipped_revoked"
	sessionRefreshStatusError          = "error"
)

// sessionRefreshProbability returns the probability [0,1] of refreshing a session
// token given the elapsed fraction of its lifetime. Below the soft threshold the
// probability is 0; at or above the hard threshold it is 1; in between it ramps as
// x^exponent where x is the normalized position in the window (1 = linear, higher
// exponents back-load refreshes toward the hard threshold).
func sessionRefreshProbability(elapsedFraction, soft, hard, exponent float64) float64 {
	if hard <= soft {
		// Degenerate configuration collapses to a hard threshold at `soft`.
		if elapsedFraction >= soft {
			return 1
		}
		return 0
	}
	if elapsedFraction <= soft {
		return 0
	}
	if elapsedFraction >= hard {
		return 1
	}
	if exponent <= 0 {
		exponent = 1
	}
	return math.Pow((elapsedFraction-soft)/(hard-soft), exponent)
}

// MaybeRefreshSession statelessly inspects a session token and, with a probability
// that ramps up as the token approaches expiry, replaces it (mint new + revoke old).
//
// It fails open by design: a request must never be broken by refresh trouble, so
// every skip condition — feature disabled, invalid/revoked/expired token, dice
// miss, or an internal refresh error — returns ("", false, nil).
//
// Parameters:
//   - ctx: context.Context input consumed by MaybeRefreshSession.
//   - authToken: the session token presented by the current request.
//
// Returns:
//   - result1: the replacement token when a refresh happened, otherwise "".
//   - result2: whether a refresh happened.
//   - err: always nil today; reserved for future non-fail-open conditions.
func (h *CommonServiceHandler[U]) MaybeRefreshSession(ctx context.Context, authToken string) (string, bool, error) {
	refreshConfig := h.runtimeView()
	if refreshConfig == nil || !refreshConfig.EnableSessionRefresh() || h.authHandler == nil {
		return "", false, nil
	}

	// Stateless inspection only — no DB access on the per-request hot path.
	claims, err := h.authHandler.ValidateSessionToken(authToken)
	if err != nil || claims == nil {
		return "", false, nil
	}

	ttl := claims.ExpiresAt.Sub(claims.IssuedAt)
	if ttl <= 0 {
		return "", false, nil
	}
	elapsedFraction := float64(time.Since(claims.IssuedAt)) / float64(ttl)

	p := sessionRefreshProbability(
		elapsedFraction,
		refreshConfig.SessionRefreshSoftThreshold(),
		refreshConfig.SessionRefreshHardThreshold(),
		refreshConfig.SessionRefreshRampExponent(),
	)
	if p <= 0 || (p < 1 && h.sessionRefreshRand() >= p) {
		return "", false, nil
	}

	newToken, err := h.authHandler.RefreshSessionToken(ctx, authToken)
	if err != nil {
		if stdErrors.Is(err, errors.ErrTokenRevoked) ||
			stdErrors.Is(err, errors.ErrInvalidToken) ||
			stdErrors.Is(err, errors.ErrTokenExpired) {
			h.recordSessionRefresh(sessionRefreshStatusSkippedRevoked)
			return "", false, nil
		}
		h.recordSessionRefresh(sessionRefreshStatusError)
		appLogger.For(ctx, h.logger, loggerName).Warn("Session refresh failed",
			zap.Error(err),
			zap.Uint64(appLogger.KeyUserID, claims.UserID),
			zap.String(logkeys.Stage, "refresh_session_token"),
			zap.Stringer(appLogger.KeyTokenType, proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION),
			zap.Float64(appLogger.KeyElapsedFraction, elapsedFraction),
			zap.String(logkeys.Fallback, "keep existing session until its current expiry"))
		return "", false, nil
	}

	h.recordSessionRefresh(sessionRefreshStatusRefreshed)
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("Session refreshed",
			zap.Uint64(appLogger.KeyUserID, claims.UserID),
			zap.Float64(appLogger.KeyElapsedFraction, elapsedFraction))
	}
	return newToken, true, nil
}

func (h *CommonServiceHandler[U]) recordSessionRefresh(status string) {
	if h.metricsHandler != nil {
		h.metricsHandler.RecordSessionRefresh(status)
	}
}
