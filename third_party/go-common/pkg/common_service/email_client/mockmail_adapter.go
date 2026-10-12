package email_client

import (
	"context"

	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// MockmailAdapter implements the EmailAdapter interface for testing purposes.
// It logs email operations instead of actually sending emails, making it
// useful for development, testing, and CI environments where actual email
// sending is not desired.
type MockmailAdapter struct {
	logger *zap.Logger
}

// NewMockmailAdapterWithLogger creates a mockmail adapter without accepting
// configuration it does not consume.
func NewMockmailAdapterWithLogger(logger *zap.Logger) (*MockmailAdapter, error) {
	return &MockmailAdapter{
		logger: logger,
	}, nil
}

// SendEmail logs a plain text email instead of sending it in the email delivery flow.
//
// Parameters:
//   - ctx: Request context propagated through the email client call path.
//   - to: Destination email address that would receive the message.
//   - subject: Email subject recorded in the mock log entry.
//   - body: Plain text body recorded in truncated form for debugging.
//
// Returns:
//   - err: Error describing why SendEmail could not complete.
func (a *MockmailAdapter) SendEmail(ctx context.Context, to, subject, body string) error {
	appLogger.For(ctx, a.logger, mockmailLoggerName).Info("Mock email send (plain text)",
		zap.String(appLogger.KeyTo, to),
		zap.String(appLogger.KeySubject, subject),
		zap.String(appLogger.KeyBodyPreview, truncateString(body, 100)),
		zap.Int(appLogger.KeyBodyLength, len(body)))
	return nil
}

// SendHTMLEmail logs an HTML email instead of sending it in the email delivery flow.
//
// Parameters:
//   - ctx: Request context propagated through the email client call path.
//   - to: Destination email address that would receive the message.
//   - subject: Email subject recorded in the mock log entry.
//   - htmlBody: HTML body recorded in truncated form for debugging.
//
// Returns:
//   - err: Error describing why SendHTMLEmail could not complete.
func (a *MockmailAdapter) SendHTMLEmail(ctx context.Context, to, subject, htmlBody string) error {
	appLogger.For(ctx, a.logger, mockmailLoggerName).Info("Mock email send (HTML)",
		zap.String(appLogger.KeyTo, to),
		zap.String(appLogger.KeySubject, subject),
		zap.String(appLogger.KeyHTMLBodyPreview, truncateString(htmlBody, 100)),
		zap.Int(appLogger.KeyHTMLBodyLength, len(htmlBody)))
	return nil
}

// Close completes mock adapter shutdown in the email client flow.
//
// Returns:
//   - err: Error describing why Close could not complete.
func (a *MockmailAdapter) Close() error {
	a.logger.Named(mockmailLoggerName).Debug("Mock email adapter closed")
	return nil
}

// truncateString truncates a string for concise mock logging in the email client flow.
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
