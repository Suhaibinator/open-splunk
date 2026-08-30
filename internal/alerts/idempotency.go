package alerts

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

const (
	minimumClientRequestIDBytes = 16
	maximumClientRequestIDBytes = 128
)

// validateClientRequestID keeps request identities safe for durable binary
// comparison and consistent with the control-plane mutation convention.
func validateClientRequestID(value string) error {
	if len(value) < minimumClientRequestIDBytes || len(value) > maximumClientRequestIDBytes {
		return fmt.Errorf("%w: client request ID must contain between %d and %d bytes", ErrInvalidArgument, minimumClientRequestIDBytes, maximumClientRequestIDBytes)
	}
	for index := 0; index < len(value); index++ {
		if value[index] < 0x21 || value[index] > 0x7e {
			return fmt.Errorf("%w: client request ID must contain printable ASCII without spaces", ErrInvalidArgument)
		}
	}
	return nil
}

// createRequestFingerprint binds an idempotency identity to all persisted
// create intent without retaining a plaintext webhook endpoint in a receipt.
func createRequestFingerprint(definition Definition, webhookURL string) ([sha256.Size]byte, error) {
	payload, err := json.Marshal(struct {
		ContractVersion uint8      `json:"contract_version"`
		Definition      Definition `json:"definition"`
		WebhookURL      string     `json:"webhook_url"`
	}{
		ContractVersion: 1,
		Definition:      cloneDefinition(definition),
		WebhookURL:      webhookURL,
	})
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("fingerprint alert creation: %w", err)
	}
	return sha256.Sum256(payload), nil
}
