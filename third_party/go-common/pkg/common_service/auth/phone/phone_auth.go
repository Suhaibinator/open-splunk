package phone

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	apperrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_service/sms_client"
	"github.com/Suhaibinator/go-common/pkg/common_service/token"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const phoneAuthLoggerName = "common_service.auth.phone_auth"

// PhoneAuthHandlerInterface defines the interface for phone authentication operations.
// It provides methods for sending OTP codes and verifying phone numbers using SMS.
// The generic type U represents the user model that implements the UserLike interface.
type PhoneAuthHandlerInterface interface {
	SendPhoneVerificationOTP(ctx context.Context, userID uint64, phoneNumber string) error
	VerifyPhoneOTP(ctx context.Context, userID uint64, otpCode string) error
	ResendPhoneVerificationOTP(ctx context.Context, userID uint64) error
	Stop()
}

// PhoneAuthHandler carries state used by the phone backend flow.
type PhoneAuthHandler[U common_service_models.UserLike] struct {
	logger             *zap.Logger
	smsClient          sms_client.SMSService
	persistenceHandler persistence.CommonPersistenceInterface
	userCallbacks      *ports.UserCallbackConfig[U]
	tokenService       token.TokenService
	runtimeConfig      RuntimeConfigProvider
}

// RuntimeConfigView is the hot phone-verification policy captured for one OTP
// send operation. Values returned by one view must belong to one immutable
// configuration generation.
type RuntimeConfigView interface {
	PhoneVerificationOTPValidity() time.Duration
	PhoneVerificationCooldown() time.Duration
}

// RuntimeConfigProvider captures one immutable SMS configuration generation.
type RuntimeConfigProvider func() RuntimeConfigView

// NewPhoneAuthHandler builds the phone auth handler. provider is sampled once
// per OTP operation, so OTP validity and resend cooldown are hot.
func NewPhoneAuthHandler[U common_service_models.UserLike](
	logger *zap.Logger,
	smsClient sms_client.SMSService,
	persistenceHandler persistence.CommonPersistenceInterface,
	userCallbacks *ports.UserCallbackConfig[U],
	tokenService token.TokenService,
	provider RuntimeConfigProvider,
) (*PhoneAuthHandler[U], error) {
	if provider == nil {
		return nil, fmt.Errorf("phone.NewPhoneAuthHandler: config provider cannot be nil")
	}
	return &PhoneAuthHandler[U]{
		logger:             logger,
		smsClient:          smsClient,
		persistenceHandler: persistenceHandler,
		userCallbacks:      userCallbacks,
		tokenService:       tokenService,
		runtimeConfig:      provider,
	}, nil
}

func userFacingMinutes(duration time.Duration) int {
	minutes := int(math.Ceil(duration.Minutes()))
	if minutes < 1 {
		return 1
	}
	return minutes
}

// SendPhoneVerificationOTP executes this step in the phone backend flow.
func (h *PhoneAuthHandler[U]) SendPhoneVerificationOTP(ctx context.Context, userID uint64, phoneNumber string) error {
	view := h.runtimeConfig()
	if phoneNumber == "" {
		return sms_client.ErrNoRecipient
	}

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, phoneAuthLoggerName).Debug("Sending phone verification OTP",
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyPhoneNumber, phoneNumber))
	}

	// Check if there's an existing unverified phone verification for cooldown
	existingVerification, err := h.persistenceHandler.GetPhoneVerificationByUserID(ctx, userID)
	if err != nil && err != persistence.ErrPhoneVerificationNotFound {
		return appLogger.WithErrorFields(fmt.Errorf("failed to check phone verification: %w", err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyPhoneNumber, phoneNumber),
			zap.String(logkeys.Stage, "load_existing_phone_verification"))
	}

	// Check cooldown if there's an existing verification
	if existingVerification != nil {
		if !existingVerification.CanSendVerificationAfter(view.PhoneVerificationCooldown()) {
			// Calculate remaining time more accurately
			timeSinceLastSent := time.Since(existingVerification.LastVerificationSent)
			remainingTime := view.PhoneVerificationCooldown() - timeSinceLastSent
			remainingMinutes := userFacingMinutes(remainingTime)

			return appLogger.WithErrorLevel(fmt.Errorf("please wait %d minutes before requesting another code", remainingMinutes), zapcore.WarnLevel,
				zap.Uint64(appLogger.KeyUserID, userID),
				zap.String(appLogger.KeyPhoneNumber, phoneNumber),
				zap.String(logkeys.Stage, "enforce_phone_verification_cooldown"),
				zap.Time(appLogger.KeyLastSent, existingVerification.LastVerificationSent),
				zap.Int(appLogger.KeyRemainingMinutes, remainingMinutes))
		}
	}

	// Generate new OTP code using token service
	otpCode, err := h.tokenService.GenerateOTPCode(ctx)
	if err != nil {
		return appLogger.WithErrorFields(fmt.Errorf("failed to generate OTP: %w", err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyPhoneNumber, phoneNumber),
			zap.String(logkeys.Stage, "generate_phone_otp"))
	}

	// Create or update phone verification record
	if existingVerification != nil {
		// Update existing verification with new OTP
		existingVerification.SetOTPCodeWithValidity(otpCode, view.PhoneVerificationOTPValidity())
		existingVerification.IncrementVerificationCount()
		existingVerification.PhoneNumber = phoneNumber // Update phone number if changed

		if err := h.persistenceHandler.UpdatePhoneVerification(ctx, existingVerification); err != nil {
			return appLogger.WithErrorFields(fmt.Errorf("failed to update phone verification: %w", err),
				zap.Uint64(appLogger.KeyUserID, userID),
				zap.String(appLogger.KeyPhoneNumber, phoneNumber),
				zap.String(logkeys.Stage, "update_phone_verification"))
		}
	} else {
		// Create new phone verification record
		phoneVerification := &common_persistence_models.PhoneVerification{
			UserID:            userID,
			PhoneNumber:       phoneNumber,
			IsVerified:        false,
			VerificationCount: 1,
		}
		phoneVerification.SetOTPCodeWithValidity(otpCode, view.PhoneVerificationOTPValidity())

		if err := h.persistenceHandler.CreatePhoneVerification(ctx, phoneVerification); err != nil {
			return appLogger.WithErrorFields(fmt.Errorf("failed to create phone verification: %w", err),
				zap.Uint64(appLogger.KeyUserID, userID),
				zap.String(appLogger.KeyPhoneNumber, phoneNumber),
				zap.String(logkeys.Stage, "create_phone_verification"))
		}
	}

	// Send SMS with OTP code
	message := fmt.Sprintf("Your verification code is: %s. This code expires in %d minutes.",
		otpCode, userFacingMinutes(view.PhoneVerificationOTPValidity()))

	if err := h.smsClient.SendSMS(ctx, proto_models.MessageType_MESSAGE_TYPE_WHATSAPP, phoneNumber, message); err != nil {
		return appLogger.WithErrorFields(fmt.Errorf("failed to send WhatsApp message: %w", err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyPhoneNumber, phoneNumber),
			zap.String(logkeys.Stage, "send_phone_verification_message"))
	}

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, phoneAuthLoggerName).Debug("Phone verification OTP sent successfully",
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyPhoneNumber, phoneNumber))
	}

	return nil
}

// VerifyPhoneOTP executes this step in the phone backend flow.
func (h *PhoneAuthHandler[U]) VerifyPhoneOTP(ctx context.Context, userID uint64, otpCode string) error {
	if otpCode == "" {
		return sms_client.ErrNoContent
	}

	if !IsValidOTPFormat(otpCode) {
		return apperrors.ErrInvalidToken
	}

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, phoneAuthLoggerName).Debug("Verifying phone OTP",
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyOTPCode, otpCode))
	}

	// Get the phone verification record
	phoneVerification, err := h.persistenceHandler.GetPhoneVerificationByUserID(ctx, userID)
	if err != nil {
		if err == persistence.ErrPhoneVerificationNotFound {
			return appLogger.WithErrorFields(apperrors.ErrTokenNotFound,
				zap.Uint64(appLogger.KeyUserID, userID),
				zap.Bool(appLogger.KeyOTPProvided, true))
		}
		return appLogger.WithErrorFields(fmt.Errorf("failed to get phone verification: %w", err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Stage, "load_phone_verification"))
	}

	// Check if OTP has expired
	if phoneVerification.IsOTPExpired() {
		return appLogger.WithErrorFields(apperrors.ErrTokenExpired,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Time(appLogger.KeyOTPExpiresAt, phoneVerification.OTPExpiresAt))
	}

	// Validate OTP code using token service
	if !h.tokenService.ValidateOTPCode(otpCode, phoneVerification.OTPCode) {
		return appLogger.WithErrorFields(apperrors.ErrInvalidToken,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Bool(appLogger.KeyOTPProvided, true))
	}

	// Mark phone verification as completed
	phoneVerification.MarkVerified()
	if err := h.persistenceHandler.UpdatePhoneVerification(ctx, phoneVerification); err != nil {
		return appLogger.WithErrorFields(fmt.Errorf("failed to update phone verification: %w", err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyPhoneNumber, phoneVerification.PhoneNumber),
			zap.String(logkeys.Stage, "mark_phone_verified"))
	}

	// Now update the user's phone number with the verified number
	persistenceUser, err := h.persistenceHandler.GetUserByID(ctx, userID)
	if err != nil {
		return appLogger.WithErrorFields(fmt.Errorf("failed to get user: %w", err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyPhoneNumber, phoneVerification.PhoneNumber),
			zap.String(logkeys.Stage, "load_user"))
	}

	// Update user's phone number with the clean, verified number
	verifiedPhoneNumber := phoneVerification.PhoneNumber
	persistenceUser.PhoneNumber = &verifiedPhoneNumber

	if err := h.persistenceHandler.UpdateUser(ctx, persistenceUser); err != nil {
		return appLogger.WithErrorFields(fmt.Errorf("failed to update user phone: %w", err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyPhoneNumber, verifiedPhoneNumber),
			zap.String(logkeys.Stage, "update_user_phone"))
	}

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, phoneAuthLoggerName).Debug("Phone verification completed successfully",
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyPhoneNumber, verifiedPhoneNumber))
	}

	return nil
}

// ResendPhoneVerificationOTP executes this step in the phone backend flow.
func (h *PhoneAuthHandler[U]) ResendPhoneVerificationOTP(ctx context.Context, userID uint64) error {
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, phoneAuthLoggerName).Debug("Resending phone verification OTP",
			zap.Uint64(appLogger.KeyUserID, userID))
	}

	// Get the existing phone verification record
	phoneVerification, err := h.persistenceHandler.GetPhoneVerificationByUserID(ctx, userID)
	if err != nil {
		if err == persistence.ErrPhoneVerificationNotFound {
			return appLogger.WithErrorFields(apperrors.ErrTokenNotFound,
				zap.Uint64(appLogger.KeyUserID, userID))
		}
		return appLogger.WithErrorFields(fmt.Errorf("failed to get phone verification: %w", err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Stage, "load_phone_verification_for_resend"))
	}

	// Check if phone verification is already completed
	if phoneVerification.IsVerified {
		return appLogger.WithErrorFields(apperrors.ErrTokenNotFound,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyPhoneNumber, phoneVerification.PhoneNumber),
			zap.String(logkeys.Reason, "phone already verified"))
	}

	// Use the same phone number from the verification record for resend
	return h.SendPhoneVerificationOTP(ctx, userID, phoneVerification.PhoneNumber)
}

// Stop executes this step in the phone backend flow.
func (h *PhoneAuthHandler[U]) Stop() {
	if h.smsClient != nil {
		if err := h.smsClient.Stop(); err != nil {
			h.logger.Named(phoneAuthLoggerName).Warn("Failed to stop SMS client",
				zap.Error(err),
				zap.String(logkeys.Stage, "stop_sms_client"),
				zap.String(logkeys.Fallback, "continue shutdown with SMS client stop failure"))
		}
	}
}
