package email_client

import (
	"context"
)

// EmailAdapter defines the interface that all email provider adapters must implement.
// This allows the email client to work with different email services (Gmail, Postal, Resend)
// through a common interface. Each adapter handles provider-specific authentication
// and API calls while exposing standard email operations.
type EmailAdapter interface {
	// SendEmail sends a plain text email through the provider.
	// The adapter handles provider-specific formatting and delivery.
	SendEmail(ctx context.Context, to, subject, body string) error

	// SendHTMLEmail sends an HTML email through the provider.
	// The adapter should handle any necessary HTML sanitization or formatting
	// required by the specific email provider.
	SendHTMLEmail(ctx context.Context, to, subject, htmlBody string) error

	// Close closes the adapter and releases any resources.
	// This may include closing HTTP clients, connection pools, or other resources
	// specific to the email provider implementation.
	Close() error
}
