package config

import "github.com/Suhaibinator/kms/sdk/go/kmsclient"

// Secret is the KMS SDK's redacting secret value.
type Secret = kmsclient.Secret

// NewSecret wraps plaintext as a redacting Secret. A KMS-managed configuration
// receives its secrets from exact release pins; an application that fills
// Config without KMS, and tests, set Secret fields with NewSecret.
func NewSecret(value string) Secret {
	return kmsclient.NewSecret([]byte(value))
}
