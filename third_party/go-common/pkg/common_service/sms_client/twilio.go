package sms_client

import (
	"context"
	"fmt"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/twilio/twilio-go"
	twilioApi "github.com/twilio/twilio-go/rest/api/v2010"
	"go.uber.org/zap"
)

// TwilioSMSService carries state used by the sms_client backend flow.
type TwilioSMSService struct {
	client        *twilio.RestClient
	logger        *zap.Logger
	smsFromNumber string
}

func NewTwilioSMSServiceFromView(cfg config.SmsView, logger *zap.Logger) (*TwilioSMSService, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	if !cfg.SMSEnabled() {
		return nil, ErrSMSDisabled
	}

	accountSID := cfg.TwilioAccountSID()
	authToken := cfg.TwilioAuthToken()
	fromNumber := cfg.SMSFromNumber()

	// Check if the credentials are configured after initialization
	if accountSID.IsZero() {
		return nil, smsProviderError(ErrInvalidConfiguration, fmt.Errorf("Twilio Account SID is missing"),
			zap.String(appLogger.KeyProvider, "twilio"), zap.String(logkeys.Stage, "load_account_sid"))
	}

	if authToken.IsZero() {
		return nil, smsProviderError(ErrInvalidConfiguration, fmt.Errorf("Twilio Auth Token is missing"),
			zap.String(appLogger.KeyProvider, "twilio"), zap.String(logkeys.Stage, "load_auth_token"))
	}

	if fromNumber == "" {
		return nil, smsProviderError(ErrInvalidConfiguration, fmt.Errorf("SMS from number is missing"),
			zap.String(appLogger.KeyProvider, "twilio"), zap.String(logkeys.Stage, "load_sender"))
	}

	client := twilio.NewRestClientWithParams(twilio.ClientParams{
		Username: accountSID.StringValue(),
		Password: authToken.StringValue(),
	})

	return &TwilioSMSService{
		client:        client,
		logger:        logger,
		smsFromNumber: fromNumber,
	}, nil
}

// SendSMS executes this step in the sms_client backend flow.
func (s *TwilioSMSService) SendSMS(ctx context.Context, messageType proto_models.MessageType, to, message string) error {
	if to == "" {
		return ErrNoRecipient
	}
	if message == "" {
		return ErrNoContent
	}

	params := &twilioApi.CreateMessageParams{}

	// Format the "to" and "from" numbers based on message type
	switch messageType {
	case proto_models.MessageType_MESSAGE_TYPE_WHATSAPP:
		// Format for WhatsApp: whatsapp:+1234567890
		formattedTo := fmt.Sprintf("whatsapp:%s", to)
		formattedFrom := fmt.Sprintf("whatsapp:%s", s.smsFromNumber)
		params.SetTo(formattedTo)
		params.SetFrom(formattedFrom)
	case proto_models.MessageType_MESSAGE_TYPE_SMS:
		// Standard SMS format
		params.SetTo(to)
		params.SetFrom(s.smsFromNumber)
	default:
		// Default to SMS for unspecified or unknown types
		params.SetTo(to)
		params.SetFrom(s.smsFromNumber)
	}

	params.SetBody(message)

	resp, err := s.client.Api.CreateMessage(params)
	if err != nil {
		return smsProviderError(ErrSendFailed, err,
			zap.String(appLogger.KeyProvider, "twilio"),
			zap.String(logkeys.Operation, "send_sms"),
			zap.String(logkeys.Stage, "provider_request"),
			zap.Stringer(appLogger.KeyMessageType, messageType),
			zap.String(appLogger.KeyTo, to),
			zap.String(appLogger.KeyFrom, s.smsFromNumber))
	}

	if resp != nil && resp.Sid != nil {
		appLogger.For(ctx, s.logger, twilioLoggerName).Info("Message sent successfully via Twilio",
			zap.Stringer(appLogger.KeyMessageType, messageType),
			zap.String(appLogger.KeyTo, to),
			zap.String(appLogger.KeyFrom, s.smsFromNumber),
			zap.String(appLogger.KeyTwilioSID, *resp.Sid),
			zap.String(appLogger.KeyMessagePreview, fmt.Sprintf("%.50s...", message)),
		)
	} else {
		appLogger.For(ctx, s.logger, twilioLoggerName).Info("Message sent successfully via Twilio",
			zap.Stringer(appLogger.KeyMessageType, messageType),
			zap.String(appLogger.KeyTo, to),
			zap.String(appLogger.KeyFrom, s.smsFromNumber),
			zap.String(appLogger.KeyMessagePreview, fmt.Sprintf("%.50s...", message)),
		)
	}

	return nil
}

// Close executes this step in the sms_client backend flow.
func (s *TwilioSMSService) Close() error {
	// The Twilio client doesn't require explicit closing
	return nil
}
