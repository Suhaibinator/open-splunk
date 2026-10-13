package sms_client

import (
	"context"
	"errors"

	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// VonageAdapter carries state used by the sms_client backend flow.
type VonageAdapter struct {
	vonageService *VonageSMSService
	logger        *zap.Logger
}

func NewVonageAdapterFromView(cfg config.SmsView, logger *zap.Logger) (*VonageAdapter, error) {
	// Create a new Vonage service
	vonageService, err := NewVonageSMSServiceFromView(cfg, logger)
	if err != nil {
		return nil, err
	}

	return &VonageAdapter{
		vonageService: vonageService,
		logger:        logger,
	}, nil
}

// SendSMS executes this step in the sms_client backend flow.
func (a *VonageAdapter) SendSMS(ctx context.Context, messageType proto_models.MessageType, to, message string) error {
	// Check if WhatsApp is requested - not currently supported by this adapter
	if messageType == proto_models.MessageType_MESSAGE_TYPE_WHATSAPP {
		return errors.New("WhatsApp messaging is not currently supported by the Vonage adapter")
	}

	// For SMS and other types, delegate to the underlying service
	return a.vonageService.SendSMS(ctx, to, message)
}

// Close executes this step in the sms_client backend flow.
func (a *VonageAdapter) Close() error {
	if a.vonageService != nil {
		return a.vonageService.Close()
	}
	return nil
}
