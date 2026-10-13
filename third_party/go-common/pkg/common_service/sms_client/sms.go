// Package sms_client provides SMS sending functionality with support for multiple providers.
// It uses an adapter pattern to support different SMS services (Twilio, etc.)
// while providing a consistent interface for sending various types of SMS messages.
package sms_client

import (
	"context"
	"errors"

	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// SMS client error definitions.
var (
	// ErrSMSDisabled indicates SMS functionality is disabled in configuration.
	ErrSMSDisabled = errors.New("SMS sending is disabled")

	// ErrInvalidConfiguration indicates the SMS provider configuration is invalid.
	ErrInvalidConfiguration = errors.New("invalid SMS configuration")

	// ErrSendFailed indicates the SMS send operation failed.
	ErrSendFailed = errors.New("failed to send SMS")

	// ErrNoRecipient indicates no recipient phone number was provided.
	ErrNoRecipient = errors.New("no recipient phone number defined")

	// ErrNoContent indicates the SMS has no message content.
	ErrNoContent = errors.New("no message content defined")
)

// SMSService defines the interface for sending SMS messages.
// It provides methods for common SMS operations including plain text messages,
// verification codes, and other transactional messages. Implementations may use different
// SMS providers while maintaining this consistent interface.
type SMSService interface {
	// SendSMS sends a message to the specified phone number using the specified message type.
	// Parameters:
	//   - messageType: Type of message (SMS, WhatsApp, etc.)
	//   - to: Recipient phone number in E.164 format (e.g., +1234567890)
	//   - message: Message content
	SendSMS(ctx context.Context, messageType proto_models.MessageType, to, message string) error

	// SendVerificationCode sends a verification code using the specified message type.
	// This is commonly used for two-factor authentication or phone verification.
	// Parameters:
	//   - messageType: Type of message (SMS, WhatsApp, etc.)
	//   - to: Recipient phone number in E.164 format
	//   - verificationCode: The verification code to send
	SendVerificationCode(ctx context.Context, messageType proto_models.MessageType, to, verificationCode string) error

	// SendPasswordResetCode sends a password reset code using the specified message type.
	// Parameters:
	//   - messageType: Type of message (SMS, WhatsApp, etc.)
	//   - to: Recipient phone number in E.164 format
	//   - resetCode: The password reset code to send
	SendPasswordResetCode(ctx context.Context, messageType proto_models.MessageType, to, resetCode string) error

	// Stop gracefully shuts down the SMS service and releases resources.
	// This should be called when the application is shutting down.
	Stop() error
}

func NewSMSServiceFromView(cfg config.SmsView, logger *zap.Logger) (SMSService, error) {
	return CreateSMSClientFromView(cfg, logger)
}
