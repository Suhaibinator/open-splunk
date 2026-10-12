package sms_client

import (
	"context"
	"fmt"

	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

const (
	smsLoggerName     = "common_service.sms"
	mockSMSLoggerName = "common_service.sms.mocksms"
	twilioLoggerName  = "common_service.sms.twilio"
	vonageLoggerName  = "common_service.sms.vonage"
)

// SMSClient carries state used by the sms_client backend flow.
type SMSClient struct {
	adapter SMSAdapter
	logger  *zap.Logger
}

// NewSMSClient executes this step in the sms_client backend flow.
func NewSMSClient(adapter SMSAdapter, logger *zap.Logger) *SMSClient {
	return &SMSClient{
		adapter: adapter,
		logger:  logger,
	}
}

// SetAdapter executes this step in the sms_client backend flow.
func (c *SMSClient) SetAdapter(adapter SMSAdapter) {
	if c.adapter != nil {
		// Close the current adapter
		if err := c.adapter.Close(); err != nil {
			c.logger.Named(smsLoggerName).Warn("Failed to close previous adapter", zap.Error(err))
		}
	}
	c.adapter = adapter
}

// SendSMS executes this step in the sms_client backend flow.
func (c *SMSClient) SendSMS(ctx context.Context, messageType proto_models.MessageType, to, message string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if c.adapter == nil {
		return fmt.Errorf("no SMS adapter configured")
	}

	if to == "" {
		return ErrNoRecipient
	}

	if message == "" {
		return ErrNoContent
	}

	if appLogger.DebugEnabled(c.logger) {
		appLogger.For(ctx, c.logger, smsLoggerName).Debug("Sending message",
			zap.Stringer(appLogger.KeyMessageType, messageType),
			zap.String(appLogger.KeyTo, to),
			zap.String(appLogger.KeyMessagePreview, fmt.Sprintf("%.50s...", message)))
	}

	return c.adapter.SendSMS(ctx, messageType, to, message)
}

// SendVerificationCode executes this step in the sms_client backend flow.
func (c *SMSClient) SendVerificationCode(ctx context.Context, messageType proto_models.MessageType, to, verificationCode string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if c.adapter == nil {
		return fmt.Errorf("no SMS adapter configured")
	}

	if to == "" {
		return ErrNoRecipient
	}

	if verificationCode == "" {
		return ErrNoContent
	}

	// Create the verification message
	message := fmt.Sprintf("Your verification code is: %s. If you did not request this code, please ignore this message.", verificationCode)

	if appLogger.DebugEnabled(c.logger) {
		appLogger.For(ctx, c.logger, smsLoggerName).Debug("Sending verification code",
			zap.Stringer(appLogger.KeyMessageType, messageType),
			zap.String(appLogger.KeyTo, to),
			zap.String(appLogger.KeyCode, verificationCode))
	}

	return c.adapter.SendSMS(ctx, messageType, to, message)
}

// SendPasswordResetCode executes this step in the sms_client backend flow.
func (c *SMSClient) SendPasswordResetCode(ctx context.Context, messageType proto_models.MessageType, to, resetCode string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if c.adapter == nil {
		return fmt.Errorf("no SMS adapter configured")
	}

	if to == "" {
		return ErrNoRecipient
	}

	if resetCode == "" {
		return ErrNoContent
	}

	// Create the password reset message
	message := fmt.Sprintf("Your password reset code is: %s. If you did not request this reset, please ignore this message.", resetCode)

	if appLogger.DebugEnabled(c.logger) {
		appLogger.For(ctx, c.logger, smsLoggerName).Debug("Sending password reset code",
			zap.Stringer(appLogger.KeyMessageType, messageType),
			zap.String(appLogger.KeyTo, to),
			zap.String(appLogger.KeyCode, resetCode))
	}

	return c.adapter.SendSMS(ctx, messageType, to, message)
}

// Stop executes this step in the sms_client backend flow.
func (c *SMSClient) Stop() error {
	if c.adapter == nil {
		return nil
	}
	return c.adapter.Close()
}

func CreateSMSClientFromView(cfg config.SmsView, logger *zap.Logger) (*SMSClient, error) {
	if !cfg.SMSEnabled() {
		return nil, ErrSMSDisabled
	}

	smsClient := NewSMSClient(nil, logger)

	var adapter SMSAdapter
	var err error

	switch cfg.SMSProvider() {
	case config.SMSProviderTwilio:
		adapter, err = NewTwilioSMSServiceFromView(cfg, smsClient.logger)
	case config.SMSProviderVonage:
		adapter, err = NewVonageAdapterFromView(cfg, smsClient.logger)
	case config.SMSProviderMocksms:
		adapter, err = NewMockSMSAdapterWithLogger(smsClient.logger)
	default:
		return nil, fmt.Errorf("unsupported SMS provider: %s", cfg.SMSProvider().String())
	}

	if err != nil {
		return nil, err
	}
	smsClient.SetAdapter(adapter)
	return smsClient, nil
}
