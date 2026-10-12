package sms_client

import (
	"bytes"
	"context"
	"crypto/rand"
	json "encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/golang-jwt/jwt/v5"
	"go.uber.org/zap"
)

// VonageSMSOptions carries state used by the sms_client backend flow.
type VonageSMSOptions struct {
	EncodingType string `json:"encoding_type,omitempty"` // text, unicode, auto
	ContentID    string `json:"content_id,omitempty"`    // for regulatory compliance
	EntityID     string `json:"entity_id,omitempty"`     // for regulatory compliance
}

// VonageMessageRequest carries state used by the sms_client backend flow.
type VonageMessageRequest struct {
	MessageType    string            `json:"message_type"`              // Required: "text"
	Text           string            `json:"text"`                      // Required: message content
	To             string            `json:"to"`                        // Required: recipient phone number
	From           string            `json:"from"`                      // Required: sender phone number
	Channel        string            `json:"channel"`                   // Required: "sms"
	TTL            *int              `json:"ttl,omitempty"`             // Optional: time-to-live in seconds
	SMS            *VonageSMSOptions `json:"sms,omitempty"`             // Optional: SMS-specific settings
	WebhookVersion string            `json:"webhook_version,omitempty"` // Optional: v0.1 or v1
	ClientRef      string            `json:"client_ref,omitempty"`      // Optional: up to 100 characters
	WebhookURL     string            `json:"webhook_url,omitempty"`     // Optional: status webhook URL
}

// VonageMessageResponse carries state used by the sms_client backend flow.
type VonageMessageResponse struct {
	MessageUUID string `json:"message_uuid"`          // UUID of the sent message
	WorkflowID  string `json:"workflow_id,omitempty"` // ID of failover workflow (if used)
}

// VonageErrorResponse carries state used by the sms_client backend flow.
type VonageErrorResponse struct {
	Type     string `json:"type"`     // Error type URL
	Title    string `json:"title"`    // Error code
	Detail   string `json:"detail"`   // Error description
	Instance string `json:"instance"` // Error instance ID
}

// VonageJWTClaims carries state used by the sms_client backend flow.
type VonageJWTClaims struct {
	Iss string `json:"iss"` // Application ID (API Key)
	Iat int64  `json:"iat"` // Issued at time
	Exp int64  `json:"exp"` // Expiration time
	Jti string `json:"jti"` // Unique identifier
	jwt.RegisteredClaims
}

// VonageSMSService carries state used by the sms_client backend flow.
type VonageSMSService struct {
	httpClient    *http.Client
	apiKey        string
	apiSecret     string
	logger        *zap.Logger
	smsFromNumber string
}

func NewVonageSMSServiceFromView(cfg config.SmsView, logger *zap.Logger) (*VonageSMSService, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	if !cfg.SMSEnabled() {
		return nil, ErrSMSDisabled
	}

	apiKey := cfg.VonageAPIKey()
	apiSecret := cfg.VonageAPISecret()
	fromNumber := cfg.SMSFromNumber()

	// Check if the credentials are configured after initialization
	if apiKey.IsZero() {
		return nil, smsProviderError(ErrInvalidConfiguration, fmt.Errorf("Vonage API Key is missing"),
			zap.String(appLogger.KeyProvider, "vonage"), zap.String(logkeys.Stage, "load_api_key"))
	}

	if apiSecret.IsZero() {
		return nil, smsProviderError(ErrInvalidConfiguration, fmt.Errorf("Vonage API Secret is missing"),
			zap.String(appLogger.KeyProvider, "vonage"), zap.String(logkeys.Stage, "load_api_secret"))
	}

	if fromNumber == "" {
		return nil, smsProviderError(ErrInvalidConfiguration, fmt.Errorf("SMS from number is missing"),
			zap.String(appLogger.KeyProvider, "vonage"), zap.String(logkeys.Stage, "load_sender"))
	}

	// Create HTTP client with timeout
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	return &VonageSMSService{
		httpClient:    httpClient,
		apiKey:        apiKey.StringValue(),
		apiSecret:     apiSecret.StringValue(),
		logger:        logger,
		smsFromNumber: fromNumber,
	}, nil
}

// SendSMS executes this step in the sms_client backend flow.
func (s *VonageSMSService) SendSMS(ctx context.Context, to, message string) error {
	if to == "" {
		return ErrNoRecipient
	}
	if message == "" {
		return ErrNoContent
	}

	// Create request payload for Vonage Messages API
	// Set reasonable defaults for SMS delivery
	ttl := 72 * 3600 // 72 hours (Vonage default)
	request := VonageMessageRequest{
		MessageType: "text",
		Text:        message,
		To:          to,
		From:        s.smsFromNumber,
		Channel:     "sms",
		TTL:         &ttl,
		SMS: &VonageSMSOptions{
			EncodingType: "auto", // Let Vonage auto-detect text vs unicode
		},
		WebhookVersion: "v1", // Use latest webhook format
	}

	// Marshal request to JSON
	jsonData, err := json.Marshal(request)
	if err != nil {
		return smsProviderError(ErrSendFailed, err,
			zap.String(appLogger.KeyProvider, "vonage"), zap.String(logkeys.Operation, "send_sms"),
			zap.String(logkeys.Stage, "marshal_request"), zap.String(appLogger.KeyTo, to))
	}

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, "POST", "https://api.nexmo.com/v1/messages", bytes.NewBuffer(jsonData))
	if err != nil {
		return smsProviderError(ErrSendFailed, err,
			zap.String(appLogger.KeyProvider, "vonage"), zap.String(logkeys.Operation, "send_sms"),
			zap.String(logkeys.Stage, "create_request"), zap.String(appLogger.KeyTo, to))
	}

	// Generate JWT token for authentication
	jwtToken, err := s.generateJWT()
	if err != nil {
		return smsProviderError(ErrSendFailed, err,
			zap.String(appLogger.KeyProvider, "vonage"), zap.String(logkeys.Operation, "send_sms"),
			zap.String(logkeys.Stage, "generate_auth_token"), zap.String(appLogger.KeyTo, to))
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+jwtToken)

	// Send request
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return smsProviderError(ErrSendFailed, err,
			zap.String(appLogger.KeyProvider, "vonage"), zap.String(logkeys.Operation, "send_sms"),
			zap.String(logkeys.Stage, "provider_request"), zap.String(appLogger.KeyTo, to),
			zap.String(appLogger.KeyFrom, s.smsFromNumber))
	}
	defer resp.Body.Close()

	// Read response body
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return smsProviderError(ErrSendFailed, err,
			zap.String(appLogger.KeyProvider, "vonage"), zap.String(logkeys.Operation, "send_sms"),
			zap.String(logkeys.Stage, "read_response"), zap.String(appLogger.KeyTo, to),
			zap.Int(logkeys.StatusCode, resp.StatusCode))
	}

	// Check HTTP status
	if resp.StatusCode != http.StatusAccepted {
		// Try to parse error response for more detailed error info
		var errorResp VonageErrorResponse
		if err := json.Unmarshal(respBody, &errorResp); err == nil && errorResp.Detail != "" {
			cause := fmt.Errorf("Vonage error %s: %s", errorResp.Title, errorResp.Detail)
			return smsProviderError(ErrSendFailed, cause,
				zap.String(appLogger.KeyProvider, "vonage"), zap.String(logkeys.Operation, "send_sms"),
				zap.String(logkeys.Stage, "provider_response"),
				zap.String(appLogger.KeyTo, to),
				zap.Int(logkeys.StatusCode, resp.StatusCode),
				zap.String(appLogger.KeyErrorType, errorResp.Type),
				zap.String(appLogger.KeyErrorTitle, errorResp.Title),
				zap.String(appLogger.KeyErrorDetail, errorResp.Detail),
				zap.String(appLogger.KeyErrorInstance, errorResp.Instance))
		} else {
			// Fallback to raw response if error parsing fails
			cause := fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
			return smsProviderError(ErrSendFailed, cause,
				zap.String(appLogger.KeyProvider, "vonage"), zap.String(logkeys.Operation, "send_sms"),
				zap.String(logkeys.Stage, "provider_response"),
				zap.String(appLogger.KeyTo, to),
				zap.Int(logkeys.StatusCode, resp.StatusCode),
				zap.String(appLogger.KeyResponse, string(respBody)))
		}
	}

	// Parse successful response
	var response VonageMessageResponse
	if err := json.Unmarshal(respBody, &response); err != nil {
		appLogger.For(ctx, s.logger, vonageLoggerName).Warn("Failed to parse Vonage response, but SMS likely sent",
			zap.String(appLogger.KeyProvider, "vonage"),
			zap.String(logkeys.Operation, "send_sms"),
			zap.String(logkeys.Stage, "parse_success_response"),
			zap.String(appLogger.KeyTo, to),
			zap.Int(logkeys.StatusCode, resp.StatusCode),
			zap.String(logkeys.Fallback, "treat_accepted_response_as_sent_without_provider_id"),
			zap.Error(err))
		// Don't return error since SMS was likely sent (status 202)
	}

	// Log successful send
	logFields := []zap.Field{
		zap.String(appLogger.KeyTo, to),
		zap.String(appLogger.KeyFrom, s.smsFromNumber),
		zap.String(appLogger.KeyVonageMessageUUID, response.MessageUUID),
		zap.String(appLogger.KeyMessagePreview, fmt.Sprintf("%.50s...", message)),
	}

	// Add workflow ID if present (indicates failover was used)
	if response.WorkflowID != "" {
		logFields = append(logFields, zap.String(appLogger.KeyVonageWorkflowID, response.WorkflowID))
	}

	appLogger.For(ctx, s.logger, vonageLoggerName).Info("SMS sent successfully via Vonage", logFields...)

	return nil
}

// generateJWT executes this step in the sms_client backend flow.
func (s *VonageSMSService) generateJWT() (string, error) {
	now := time.Now().Unix()
	exp := now + 300 // 5 minutes expiry

	// Generate a random JTI (JWT ID)
	jtiBytes := make([]byte, 16)
	if _, err := rand.Read(jtiBytes); err != nil {
		return "", fmt.Errorf("failed to generate JWT ID: %w", err)
	}
	jti := fmt.Sprintf("%x", jtiBytes)

	// Create claims
	claims := VonageJWTClaims{
		Iss: s.apiKey,
		Iat: now,
		Exp: exp,
		Jti: jti,
	}

	// Create token
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	// Sign with secret
	tokenString, err := token.SignedString([]byte(s.apiSecret))
	if err != nil {
		return "", fmt.Errorf("failed to sign JWT: %w", err)
	}

	return tokenString, nil
}

// Close executes this step in the sms_client backend flow.
func (s *VonageSMSService) Close() error {
	// Close HTTP client if needed (in this case it's fine to leave as-is)
	return nil
}
