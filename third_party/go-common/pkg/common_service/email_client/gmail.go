package email_client

import (
	"context"
	"encoding/base64"
	json "encoding/json/v2"
	"fmt"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/option"
)

// GmailEmailService carries state used by the email_client backend flow.
type GmailEmailService struct {
	service          *gmail.Service
	logger           *zap.Logger
	emailFromAddress string
	emailFromName    string
	userID           string
}

// NewGmailEmailServiceFromView creates a Gmail service from the email consumer view.
func NewGmailEmailServiceFromView(cfg config.EmailView, logger *zap.Logger) (*GmailEmailService, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	if !cfg.EmailEnabled() {
		return nil, ErrEmailDisabled
	}

	gmailCredentials := cfg.GmailCredentials()
	gmailToken := cfg.GmailToken()
	if gmailCredentials.IsZero() || gmailToken.IsZero() {
		return nil, ErrInvalidConfiguration
	}

	// Extract credentials only at the provider boundary.
	credentials := gmailCredentials.StringValue()
	if credentials == "" {
		return nil, emailProviderError(ErrInvalidConfiguration, fmt.Errorf("empty Gmail credentials"),
			zap.String(appLogger.KeyProvider, "gmail"),
			zap.String(logkeys.Stage, "load_credentials"))
	}
	b := []byte(credentials)

	// Configure the OAuth2 config
	config, err := google.ConfigFromJSON(b, gmail.GmailSendScope)
	if err != nil {
		return nil, emailProviderError(ErrInvalidConfiguration, err,
			zap.String(appLogger.KeyProvider, "gmail"),
			zap.String(logkeys.Stage, "parse_credentials"))
	}

	// Get the token
	tokenString := gmailToken.StringValue()
	if tokenString == "" {
		return nil, emailProviderError(ErrInvalidConfiguration, fmt.Errorf("empty Gmail token"),
			zap.String(appLogger.KeyProvider, "gmail"),
			zap.String(logkeys.Stage, "load_token"))
	}

	// Parse the token
	token := &oauth2.Token{}
	err = json.Unmarshal([]byte(tokenString), token)
	if err != nil {
		return nil, emailProviderError(ErrInvalidConfiguration, err,
			zap.String(appLogger.KeyProvider, "gmail"),
			zap.String(logkeys.Stage, "parse_token"))
	}

	// Create the Gmail service
	ctx := context.Background()
	client := config.Client(ctx, token)
	service, err := gmail.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return nil, emailProviderError(ErrSendFailed, err,
			zap.String(appLogger.KeyProvider, "gmail"),
			zap.String(logkeys.Stage, "create_client"))
	}

	return &GmailEmailService{
		service:          service,
		logger:           logger,
		emailFromAddress: cfg.EmailFromAddress(),
		emailFromName:    cfg.EmailFromName(),
		userID:           cfg.GmailUserID(),
	}, nil
}

// SendEmail executes this step in the email_client backend flow.
func (s *GmailEmailService) SendEmail(ctx context.Context, to, subject, body string) error {
	if to == "" {
		return ErrNoRecipients
	}

	if body == "" {
		return ErrNoContent
	}

	// Create the message
	message := createMessage(s.emailFromName, s.emailFromAddress, to, subject, body, "")

	// Send the message
	_, err := s.service.Users.Messages.Send(s.userID, message).Context(ctx).Do()
	if err != nil {
		return emailProviderError(ErrSendFailed, err,
			zap.String(appLogger.KeyProvider, "gmail"),
			zap.String(logkeys.Operation, "send_email"),
			zap.String(logkeys.Stage, "provider_request"),
			zap.String(appLogger.KeyTo, to),
			zap.String(appLogger.KeySubject, subject))
	}

	appLogger.For(ctx, s.logger, gmailLoggerName).Info("Email sent successfully",
		zap.String(appLogger.KeyTo, to),
		zap.String(appLogger.KeySubject, subject),
	)

	return nil
}

// SendHTMLEmail executes this step in the email_client backend flow.
func (s *GmailEmailService) SendHTMLEmail(ctx context.Context, to, subject, htmlBody string) error {
	if to == "" {
		return ErrNoRecipients
	}

	if htmlBody == "" {
		return ErrNoContent
	}

	// Create the message
	message := createMessage(s.emailFromName, s.emailFromAddress, to, subject, "", htmlBody)

	// Send the message
	_, err := s.service.Users.Messages.Send(s.userID, message).Context(ctx).Do()
	if err != nil {
		return emailProviderError(ErrSendFailed, err,
			zap.String(appLogger.KeyProvider, "gmail"),
			zap.String(logkeys.Operation, "send_html_email"),
			zap.String(logkeys.Stage, "provider_request"),
			zap.String(appLogger.KeyTo, to),
			zap.String(appLogger.KeySubject, subject))
	}

	appLogger.For(ctx, s.logger, gmailLoggerName).Info("HTML email sent successfully",
		zap.String(appLogger.KeyTo, to),
		zap.String(appLogger.KeySubject, subject),
	)

	return nil
}

// createMessage executes this step in the email_client backend flow.
func createMessage(fromName, fromEmail, to, subject, plainBody, htmlBody string) *gmail.Message {
	// Create the message headers
	var headers []string
	headers = append(headers, fmt.Sprintf("From: %s <%s>", fromName, fromEmail))
	headers = append(headers, fmt.Sprintf("To: %s", to))
	headers = append(headers, fmt.Sprintf("Subject: %s", subject))
	headers = append(headers, "MIME-Version: 1.0")

	// Create the message body
	var messageBody string
	if plainBody != "" && htmlBody != "" {
		// Multipart message with both plain text and HTML
		boundary := "boundary_" + fmt.Sprintf("%d", time.Now().UTC().UnixNano())
		headers = append(headers, fmt.Sprintf("Content-Type: multipart/alternative; boundary=%s", boundary))

		messageBody = fmt.Sprintf("\r\n--%s\r\n", boundary)
		messageBody += "Content-Type: text/plain; charset=UTF-8\r\n\r\n"
		messageBody += plainBody

		messageBody += fmt.Sprintf("\r\n--%s\r\n", boundary)
		messageBody += "Content-Type: text/html; charset=UTF-8\r\n\r\n"
		messageBody += htmlBody

		messageBody += fmt.Sprintf("\r\n--%s--", boundary)
	} else if htmlBody != "" {
		// HTML only
		headers = append(headers, "Content-Type: text/html; charset=UTF-8")
		messageBody = htmlBody
	} else {
		// Plain text only
		headers = append(headers, "Content-Type: text/plain; charset=UTF-8")
		messageBody = plainBody
	}

	// Combine headers and body
	rawMessage := strings.Join(headers, "\r\n") + "\r\n\r\n" + messageBody

	// Encode the message
	return &gmail.Message{
		Raw: base64.URLEncoding.EncodeToString([]byte(rawMessage)),
	}
}

// Close executes this step in the email_client backend flow.
func (s *GmailEmailService) Close() error {
	// Nothing to close for the Gmail service
	return nil
}
