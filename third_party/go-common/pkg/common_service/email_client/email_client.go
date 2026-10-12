package email_client

import (
	"context"
	"fmt"

	"github.com/Suhaibinator/go-common/pkg/config"
	"go.uber.org/zap"
)

const (
	emailLoggerName    = "common_service.email"
	gmailLoggerName    = "common_service.email.gmail"
	mockmailLoggerName = "common_service.email.mockmail"
	postalLoggerName   = "common_service.email.postal"
	resendLoggerName   = "common_service.email.resend"
)

// EmailClient carries state used by the email_client backend flow.
type EmailClient struct {
	adapter EmailAdapter
	logger  *zap.Logger
}

// newEmailClient executes this step in the email_client backend flow.
func newEmailClient(adapter EmailAdapter, logger *zap.Logger) *EmailClient {
	return &EmailClient{
		adapter: adapter,
		logger:  logger,
	}
}

// SetAdapter executes this step in the email_client backend flow.
func (c *EmailClient) SetAdapter(adapter EmailAdapter) {
	if c.adapter != nil {
		// Close the current adapter
		if err := c.adapter.Close(); err != nil {
			c.logger.Named(emailLoggerName).Warn("Failed to close previous adapter", zap.Error(err))
		}
	}
	c.adapter = adapter
}

// SendEmail executes this step in the email_client backend flow.
func (c *EmailClient) SendEmail(ctx context.Context, to, subject, body string) error {
	if c.adapter == nil {
		return fmt.Errorf("no email adapter configured")
	}
	return c.adapter.SendEmail(ctx, to, subject, body)
}

// SendHTMLEmail executes this step in the email_client backend flow.
func (c *EmailClient) SendHTMLEmail(ctx context.Context, to, subject, htmlBody string) error {
	if c.adapter == nil {
		return fmt.Errorf("no email adapter configured")
	}
	return c.adapter.SendHTMLEmail(ctx, to, subject, htmlBody)
}

// Stop executes this step in the email_client backend flow.
func (c *EmailClient) Stop() error {
	if c.adapter == nil {
		return nil
	}
	return c.adapter.Close()
}

// CreateEmailClientFromView creates an email client from the email consumer view.
func CreateEmailClientFromView(cfg config.EmailView, logger *zap.Logger) (*EmailClient, error) {
	if !cfg.EmailEnabled() {
		return nil, ErrEmailDisabled
	}

	emailClient := newEmailClient(nil, logger)

	var adapter EmailAdapter
	var err error

	switch cfg.EmailProvider() {
	case config.EmailProviderPostal:
		adapter, err = NewPostalEmailServiceFromView(cfg, emailClient.logger)
	case config.EmailProviderGmail:
		adapter, err = NewGmailEmailServiceFromView(cfg, emailClient.logger)
	case config.EmailProviderResend:
		adapter, err = NewResendEmailServiceFromView(cfg, emailClient.logger)
	case config.EmailProviderMockmail:
		adapter, err = NewMockmailAdapterWithLogger(emailClient.logger)
	default:
		return nil, fmt.Errorf("unsupported email provider: %s", cfg.EmailProvider().String())
	}

	if err != nil {
		return nil, err
	}
	emailClient.SetAdapter(adapter)
	return emailClient, nil
}
