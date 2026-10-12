// Package email_client provides email sending functionality with support for multiple providers.
// It uses an adapter pattern to support different email services (Gmail, Postal, Resend)
// while providing a consistent interface for sending various types of emails.
package email_client

import (
	"errors"
)

// Email client error definitions.
var (
	// ErrEmailDisabled indicates email functionality is disabled in configuration.
	ErrEmailDisabled = errors.New("email sending is disabled")

	// ErrInvalidConfiguration indicates the email provider configuration is invalid.
	ErrInvalidConfiguration = errors.New("invalid email configuration")

	// ErrSendFailed indicates the email send operation failed.
	ErrSendFailed = errors.New("failed to send email")

	// ErrNoRecipients indicates no recipient email addresses were provided.
	ErrNoRecipients = errors.New("no recipients defined")

	// ErrNoContent indicates the email has no subject or body content.
	ErrNoContent = errors.New("no content defined")
)
