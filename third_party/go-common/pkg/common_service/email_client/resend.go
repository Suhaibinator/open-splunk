package email_client

import (
	"context"
	"fmt"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/resend/resend-go/v2"
	"go.uber.org/zap"
)

// ResendEmailService carries state used by the email_client backend flow.
type ResendEmailService struct {
	client           *resend.Client
	logger           *zap.Logger
	emailFromAddress string
	emailFromName    string
}

// NewResendEmailServiceFromView creates a Resend service from the email consumer view.
func NewResendEmailServiceFromView(cfg config.EmailView, logger *zap.Logger) (*ResendEmailService, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	if !cfg.EmailEnabled() {
		return nil, ErrEmailDisabled
	}

	// Check if the API key value is empty after initialization
	apiKey := cfg.ResendAPIKey()
	if apiKey.IsZero() {
		return nil, emailProviderError(ErrInvalidConfiguration, fmt.Errorf("Resend API key is missing"),
			zap.String(appLogger.KeyProvider, "resend"),
			zap.String(logkeys.Stage, "load_credentials"))
	}

	client := resend.NewClient(apiKey.StringValue())

	return &ResendEmailService{
		client:           client,
		logger:           logger,
		emailFromAddress: cfg.EmailFromAddress(),
		emailFromName:    cfg.EmailFromName(),
	}, nil
}

// SendEmail executes this step in the email_client backend flow.
func (s *ResendEmailService) SendEmail(ctx context.Context, to, subject, body string) error {
	if to == "" {
		return ErrNoRecipients
	}
	if body == "" {
		return ErrNoContent
	}

	from := fmt.Sprintf("%s <%s>", s.emailFromName, s.emailFromAddress)

	params := &resend.SendEmailRequest{
		From:    from,
		To:      []string{to},
		Subject: subject,
		Text:    body,
	}

	sent, err := s.client.Emails.SendWithContext(ctx, params)
	if err != nil {
		return emailProviderError(ErrSendFailed, err,
			zap.String(appLogger.KeyProvider, "resend"),
			zap.String(logkeys.Operation, "send_email"),
			zap.String(logkeys.Stage, "provider_request"),
			zap.String(appLogger.KeyTo, to),
			zap.String(appLogger.KeySubject, subject))
	}

	if sent != nil {
		appLogger.For(ctx, s.logger, resendLoggerName).Info("Email sent successfully via Resend",
			zap.String(appLogger.KeyTo, to),
			zap.String(appLogger.KeySubject, subject),
			zap.String(appLogger.KeyResendID, sent.Id),
		)
	} else {
		appLogger.For(ctx, s.logger, resendLoggerName).Info("Email sent successfully via Resend",
			zap.String(appLogger.KeyTo, to),
			zap.String(appLogger.KeySubject, subject),
		)
	}

	return nil
}

// Close executes this step in the email_client backend flow.
func (s *ResendEmailService) Close() error {
	// The Resend client doesn't require explicit closing
	return nil
}

// SendHTMLEmail executes this step in the email_client backend flow.
func (s *ResendEmailService) SendHTMLEmail(ctx context.Context, to, subject, htmlBody string) error {
	if to == "" {
		return ErrNoRecipients
	}
	if htmlBody == "" {
		return ErrNoContent
	}

	from := fmt.Sprintf("%s <%s>", s.emailFromName, s.emailFromAddress)

	params := &resend.SendEmailRequest{
		From:    from,
		To:      []string{to},
		Subject: subject,
		Html:    htmlBody,
	}

	sent, err := s.client.Emails.SendWithContext(ctx, params)
	if err != nil {
		return emailProviderError(ErrSendFailed, err,
			zap.String(appLogger.KeyProvider, "resend"),
			zap.String(logkeys.Operation, "send_html_email"),
			zap.String(logkeys.Stage, "provider_request"),
			zap.String(appLogger.KeyTo, to),
			zap.String(appLogger.KeySubject, subject))
	}

	appLogger.For(ctx, s.logger, resendLoggerName).Info("HTML email sent successfully via Resend",
		zap.String(appLogger.KeyTo, to),
		zap.String(appLogger.KeySubject, subject),
		zap.String(appLogger.KeyResendID, sent.Id),
	)

	return nil
}
