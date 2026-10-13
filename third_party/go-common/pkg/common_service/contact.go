package common_service

import (
	"context"
	"time"
	"uuid"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/rate_limiter"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// SubmitContactMessage executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) SubmitContactMessage(
	ctx context.Context,
	name, email, subject, message string,
) (string, error) {
	// Start timing for metrics
	startTime := time.Now()

	// Rate limiting check
	if h.rateLimiter != nil {
		// Get IP address from context for rate limiting
		ipAddress := h.getIPFromContext(ctx)
		allowed, retryAfter, err := h.rateLimiter.CheckRateLimit(ctx, rate_limiter.ActionContact, ipAddress)
		if err != nil {
			appLogger.For(ctx, h.logger, loggerName).Warn("Contact rate limiter failed; allowing submission without enforcement",
				zap.Error(err),
				zap.String(logkeys.Stage, "contact_rate_limit_check"),
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionContact),
				zap.String(appLogger.KeyIdentifier, ipAddress),
				zap.String(logkeys.Fallback, "allow_contact_submission_without_rate_limit"))
			// Continue with submission even if rate limiter fails
		} else if !allowed {
			rateLimitErr := appLogger.WithErrorLevel(errors.ErrRateLimitExceeded, zapcore.WarnLevel,
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionContact),
				zap.String(appLogger.KeyIdentifier, ipAddress),
				appLogger.DurationSeconds(appLogger.KeyRetryAfterSeconds, retryAfter))
			// Record metrics for rate limited request
			if h.metricsHandler != nil {
				duration := time.Since(startTime).Seconds()
				h.metricsHandler.RecordContactSubmission("rate_limited", duration)
			}
			return "", rateLimitErr
		}
	}

	// Get IP and user agent from context
	ipAddress := h.getIPFromContext(ctx)
	userAgent := h.getUserAgentFromContext(ctx)

	referenceID := uuid.NewV7().String()

	// Get user ID if authenticated
	var userID *uint64
	// Note: In service layer, we would need the user ID to be passed from API layer
	// or extracted from context if it was set there

	// Basic length validation (sanitizer does the more comprehensive check)
	if err := h.validateContactMessage(name, email, subject, message); err != nil {
		// Record as invalid input rather than a server-side failure
		if h.metricsHandler != nil {
			duration := time.Since(startTime).Seconds()
			h.metricsHandler.RecordContactSubmission("invalid", duration)
		}
		return "", err
	}

	// Create contact message
	contactMsg := &common_persistence_models.ContactMessage{
		UserID:      userID,
		Name:        name,
		Email:       email,
		Subject:     subject,
		Message:     message,
		IPAddress:   ipAddress,
		UserAgent:   userAgent,
		Status:      common_persistence_models.ContactStatusPending,
		ReferenceID: referenceID,
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}

	// Save to database
	err := h.commonPersistenceHandler.CreateContactMessage(ctx, contactMsg)
	if err != nil {
		// Record failure metric
		if h.metricsHandler != nil {
			duration := time.Since(startTime).Seconds()
			h.metricsHandler.RecordContactSubmission("failure", duration)
		}
		return "", serviceContextualError(errors.ErrInternalError, err,
			zap.String(logkeys.Stage, "persist_contact_message"),
			zap.String(appLogger.KeyReferenceID, referenceID),
			zap.String(appLogger.KeyEmail, email),
			zap.String(appLogger.KeyIdentifier, ipAddress))
	}

	// Send notification email to admin (async)
	go h.notifyAdminOfContactMessage(ctx, contactMsg)

	// Send confirmation email to user (async)
	go h.sendContactConfirmationEmail(ctx, email, name, referenceID)

	// Record success if rate limiter is configured
	if h.rateLimiter != nil {
		ipAddress := h.getIPFromContext(ctx)
		if recordErr := h.rateLimiter.RecordSuccess(ctx, rate_limiter.ActionContact, ipAddress); recordErr != nil {
			appLogger.For(ctx, h.logger, loggerName).Warn("Failed to record contact rate-limit success",
				zap.Error(recordErr),
				zap.String(logkeys.Stage, "contact_rate_limit_record_success"),
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionContact),
				zap.String(appLogger.KeyIdentifier, ipAddress),
				zap.String(logkeys.Fallback, "complete successful submission without counter reset"))
		}
	}

	appLogger.For(ctx, h.logger, loggerName).Info("Contact message submitted",
		zap.Uint(appLogger.KeyContactID, contactMsg.ID),
		zap.String(appLogger.KeyEmail, email),
		zap.String(appLogger.KeyIdentifier, ipAddress),
		zap.String(appLogger.KeyReferenceID, referenceID))

	// Record success metric
	if h.metricsHandler != nil {
		duration := time.Since(startTime).Seconds()
		h.metricsHandler.RecordContactSubmission("success", duration)
	}

	return referenceID, nil
}

// validateContactMessage executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) validateContactMessage(name, email, subject, message string) error {
	// Basic length checks (detailed validation is done in sanitizer)
	if len(name) < 2 || len(email) < 5 || len(subject) < 5 || len(message) < 10 {
		return errors.ErrInvalidRequest
	}
	return nil
}

// getIPFromContext executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) getIPFromContext(ctx context.Context) string {
	// This would be set by the API layer middleware
	ip, found := scontext.GetClientIP(ctx)
	if found {
		return ip
	}
	return "unknown-ip"
}

// getUserAgentFromContext executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) getUserAgentFromContext(ctx context.Context) string {
	// This would be set by the API layer middleware
	if ua, ok := scontext.GetUserAgent(ctx); ok {
		return ua
	}
	return ""
}

// notifyAdminOfContactMessage executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) notifyAdminOfContactMessage(ctx context.Context, msg *common_persistence_models.ContactMessage) {
	// TODO: Implement admin notification
	// This would use the email client to send a notification to configured admin emails
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("Admin notification would be sent",
			zap.Uint(appLogger.KeyMessageID, msg.ID))
	}
}

// sendContactConfirmationEmail executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) sendContactConfirmationEmail(ctx context.Context, email, _ string, referenceID string) {
	// TODO: Implement user confirmation email
	// This would use the email client to send a confirmation with the reference ID
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("User confirmation would be sent",
			zap.String(appLogger.KeyEmail, email),
			zap.String(appLogger.KeyReferenceID, referenceID))
	}
}
