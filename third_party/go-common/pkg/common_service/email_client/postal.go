package email_client

import (
	"context"
	"fmt"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/postalclient-go"
	"github.com/Suhaibinator/postalclient-go/models"
	"go.uber.org/zap"
)

// PostalEmailService carries state used by the email_client backend flow.
type PostalEmailService struct {
	client           *postalclient.Client
	logger           *zap.Logger
	emailFromName    string
	emailFromAddress string
}

// NewPostalEmailServiceFromView creates a Postal service from the email consumer view.
func NewPostalEmailServiceFromView(cfg config.EmailView, logger *zap.Logger) (*PostalEmailService, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	if !cfg.EmailEnabled() {
		return nil, ErrEmailDisabled
	}

	endpoint := cfg.PostalAPIEndpoint()
	apiKey := cfg.PostalAPIKey()
	if endpoint == "" || apiKey.IsZero() {
		return nil, ErrInvalidConfiguration
	}

	// Create a new Postal client with custom options
	client := postalclient.NewClientWithOptions(
		apiKey.StringValue(),
		endpoint,
		10*time.Second,
	)

	return &PostalEmailService{
		client:           client,
		logger:           logger,
		emailFromName:    cfg.EmailFromName(),
		emailFromAddress: cfg.EmailFromAddress(),
	}, nil
}

// SendEmail executes this step in the email_client backend flow.
func (s *PostalEmailService) SendEmail(ctx context.Context, to, subject, body string) error {
	if to == "" {
		return ErrNoRecipients
	}

	if body == "" {
		return ErrNoContent
	}

	// Create a message request
	req := &models.SendMessageRequest{
		To:        []string{to},
		From:      fmt.Sprintf("%s <%s>", s.emailFromName, s.emailFromAddress),
		Subject:   subject,
		PlainBody: body,
	}

	// Send the message
	resp, err := s.client.SendMessage(req)
	if err != nil {
		return emailProviderError(ErrSendFailed, err,
			zap.String(appLogger.KeyProvider, "postal"),
			zap.String(logkeys.Operation, "send_email"),
			zap.String(logkeys.Stage, "provider_request"),
			zap.String(appLogger.KeyTo, to),
			zap.String(appLogger.KeySubject, subject))
	}

	if resp != nil {
		appLogger.For(ctx, s.logger, postalLoggerName).Info("Email sent successfully",
			zap.String(appLogger.KeyTo, to),
			zap.String(appLogger.KeySubject, subject),
			zap.Int(appLogger.KeyMessageID, resp.MessageID),
			zap.String(appLogger.KeyToken, resp.Token),
		)
	} else {
		appLogger.For(ctx, s.logger, postalLoggerName).Info("Email sent successfully",
			zap.String(appLogger.KeyTo, to),
			zap.String(appLogger.KeySubject, subject),
		)
	}

	return nil
}

// SendHTMLEmail executes this step in the email_client backend flow.
func (s *PostalEmailService) SendHTMLEmail(ctx context.Context, to, subject, htmlBody string) error {
	if to == "" {
		return ErrNoRecipients
	}

	if htmlBody == "" {
		return ErrNoContent
	}

	// Create a message request
	req := &models.SendMessageRequest{
		To:       []string{to},
		From:     fmt.Sprintf("%s <%s>", s.emailFromName, s.emailFromAddress),
		Subject:  subject,
		HTMLBody: htmlBody,
	}

	// Send the message
	resp, err := s.client.SendMessage(req)
	if err != nil {
		return emailProviderError(ErrSendFailed, err,
			zap.String(appLogger.KeyProvider, "postal"),
			zap.String(logkeys.Operation, "send_html_email"),
			zap.String(logkeys.Stage, "provider_request"),
			zap.String(appLogger.KeyTo, to),
			zap.String(appLogger.KeySubject, subject))
	}

	if resp != nil {
		appLogger.For(ctx, s.logger, postalLoggerName).Info("HTML email sent successfully",
			zap.String(appLogger.KeyTo, to),
			zap.String(appLogger.KeySubject, subject),
			zap.Int(appLogger.KeyMessageID, resp.MessageID),
			zap.String(appLogger.KeyToken, resp.Token),
		)
	} else {
		appLogger.For(ctx, s.logger, postalLoggerName).Info("HTML email sent successfully",
			zap.String(appLogger.KeyTo, to),
			zap.String(appLogger.KeySubject, subject),
		)
	}

	return nil
}

// Close executes this step in the email_client backend flow.
func (s *PostalEmailService) Close() error {
	// Nothing to close for the client
	return nil
}
