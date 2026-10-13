package sms_client

import (
	"context"

	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// MockSMSAdapter implements the SMSAdapter interface for testing purposes.
// It logs SMS operations instead of actually sending messages, making it
// useful for development, testing, and CI environments where actual SMS
// sending is not desired.
type MockSMSAdapter struct {
	logger *zap.Logger
}

// NewMockSMSAdapterWithLogger creates a mock SMS adapter without accepting
// configuration it does not consume.
func NewMockSMSAdapterWithLogger(logger *zap.Logger) (*MockSMSAdapter, error) {
	return &MockSMSAdapter{
		logger: logger,
	}, nil
}

// SendSMS logs an SMS message instead of sending it in the SMS delivery flow.
//
// Parameters:
//   - ctx: Request context propagated through the SMS client call path.
//   - messageType: Outbound message channel recorded in the mock log entry.
//   - to: Destination phone number that would receive the message.
//   - message: SMS body recorded in truncated form for debugging.
//
// Returns:
//   - err: Error describing why SendSMS could not complete.
func (a *MockSMSAdapter) SendSMS(ctx context.Context, messageType proto_models.MessageType, to, message string) error {
	appLogger.For(ctx, a.logger, mockSMSLoggerName).Info("Mock SMS send",
		zap.Stringer(appLogger.KeyMessageType, messageType),
		zap.String(appLogger.KeyTo, to),
		zap.String(appLogger.KeyMessagePreview, truncateString(message, 100)),
		zap.Int(appLogger.KeyMessageLength, len(message)))
	return nil
}

// Close completes mock adapter shutdown in the SMS client flow.
//
// Returns:
//   - err: Error describing why Close could not complete.
func (a *MockSMSAdapter) Close() error {
	a.logger.Named(mockSMSLoggerName).Debug("Mock SMS adapter closed")
	return nil
}

// truncateString truncates a string for concise mock logging in the SMS client flow.
//
// Parameters:
//   - s: Source string that may need truncation before logging.
//   - maxLen: Maximum number of characters to preserve before appending an ellipsis.
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
