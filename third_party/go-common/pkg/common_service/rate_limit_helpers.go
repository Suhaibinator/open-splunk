package common_service

import (
	"context"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	apperrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/rate_limiter"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// checkRateLimit executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) checkRateLimit(ctx context.Context, action rate_limiter.RateLimitAction, identifier string) error {
	if h.rateLimiter == nil {
		return nil
	}

	allowed, retryAfter, err := h.rateLimiter.CheckRateLimit(ctx, action, identifier)
	if err != nil {
		appLogger.For(ctx, h.logger, loggerName).Warn("rate limiter failed; allowing request without enforcement",
			zap.String(logkeys.Stage, "rate_limit_check"),
			zap.Stringer(appLogger.KeyAction, action),
			zap.String(appLogger.KeyIdentifier, identifier),
			zap.String(logkeys.Fallback, "allow_request_without_rate_limit"),
			zap.Error(err))
		return nil
	}
	if allowed {
		return nil
	}

	return appLogger.WithErrorLevel(apperrors.ErrRateLimitExceeded, zapcore.WarnLevel,
		zap.Stringer(appLogger.KeyAction, action),
		zap.String(appLogger.KeyIdentifier, identifier),
		appLogger.DurationSeconds(appLogger.KeyRetryAfterSeconds, retryAfter))
}

// checkIPRateLimit executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) checkIPRateLimit(ctx context.Context, action rate_limiter.RateLimitAction) error {
	return h.checkRateLimit(ctx, action, h.getIPFromContext(ctx))
}

// CheckOrganizationSlugAvailability executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) CheckOrganizationSlugAvailability(ctx context.Context, slug string) (bool, string, error) {
	if err := h.checkIPRateLimit(ctx, rate_limiter.ActionOrganizationSlugAvailability); err != nil {
		return false, "", err
	}

	return h.organizationHandler.CheckSlugAvailability(ctx, slug)
}
