package sms_client

import (
	"context"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// SMSAdapter defines the interface that all SMS provider adapters must implement.
// This allows the SMS client to work with different SMS services (Twilio, etc.)
// through a common interface. Each adapter handles provider-specific authentication
// and API calls while exposing standard SMS operations.
type SMSAdapter interface {
	// SendSMS sends a message through the provider using the specified message type.
	// The adapter handles provider-specific formatting and delivery.
	// Parameters:
	//   - messageType: Type of message (SMS, WhatsApp, etc.)
	//   - to: Recipient phone number in E.164 format
	//   - message: Message content
	SendSMS(ctx context.Context, messageType proto_models.MessageType, to, message string) error

	// Close closes the adapter and releases any resources.
	// This may include closing HTTP clients, connection pools, or other resources
	// specific to the SMS provider implementation.
	Close() error
}
