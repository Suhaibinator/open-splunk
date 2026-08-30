package alerts

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
)

const SecretBytes = 32

type SecretPurpose string

const (
	PurposeEndpoint   SecretPurpose = "endpoint"
	PurposeHMACSecret SecretPurpose = "hmac-secret"
)

type SecretContext struct {
	AlertID    string
	Purpose    SecretPurpose
	Generation uint64
}

type SensitiveCipher interface {
	Encrypt(SecretContext, []byte) (EncryptedValue, error)
	Decrypt(SecretContext, EncryptedValue) ([]byte, error)
}

type AESGCMCipher struct {
	aead   cipher.AEAD
	random io.Reader
}

func NewAESGCMCipher(key []byte, random io.Reader) (*AESGCMCipher, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("%w: AES-256-GCM key must be 32 bytes", ErrInvalidArgument)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.New("alerts: initialize endpoint encryption")
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errors.New("alerts: initialize endpoint authentication")
	}
	if random == nil {
		random = rand.Reader
	}
	return &AESGCMCipher{aead: aead, random: random}, nil
}

func (cipher *AESGCMCipher) Encrypt(secretContext SecretContext, plaintext []byte) (EncryptedValue, error) {
	if err := validateSecretContext(secretContext); err != nil {
		return EncryptedValue{}, err
	}
	nonce := make([]byte, cipher.aead.NonceSize())
	if _, err := io.ReadFull(cipher.random, nonce); err != nil {
		return EncryptedValue{}, errors.New("alerts: secure randomness unavailable")
	}
	sealed := cipher.aead.Seal(nil, nonce, plaintext, additionalData(secretContext))
	return EncryptedValue{Nonce: nonce, Ciphertext: sealed}, nil
}

func (cipher *AESGCMCipher) Decrypt(secretContext SecretContext, encrypted EncryptedValue) ([]byte, error) {
	if err := validateSecretContext(secretContext); err != nil {
		return nil, err
	}
	if len(encrypted.Nonce) != cipher.aead.NonceSize() || len(encrypted.Ciphertext) < cipher.aead.Overhead() {
		return nil, errors.New("alerts: encrypted value is malformed")
	}
	plaintext, err := cipher.aead.Open(nil, encrypted.Nonce, encrypted.Ciphertext, additionalData(secretContext))
	if err != nil {
		return nil, errors.New("alerts: encrypted value authentication failed")
	}
	return plaintext, nil
}

type SecretGenerator interface {
	Generate() ([]byte, error)
}

type RandomSecretGenerator struct {
	Random io.Reader
}

func (generator RandomSecretGenerator) Generate() ([]byte, error) {
	random := generator.Random
	if random == nil {
		random = rand.Reader
	}
	secret := make([]byte, SecretBytes)
	if _, err := io.ReadFull(random, secret); err != nil {
		return nil, errors.New("alerts: secure randomness unavailable")
	}
	return secret, nil
}

func EncodeIssuedSecret(secret []byte) (string, error) {
	if len(secret) != SecretBytes {
		return "", fmt.Errorf("%w: HMAC secret must be 32 bytes", ErrInvalidArgument)
	}
	return base64.RawURLEncoding.EncodeToString(secret), nil
}

func validateSecretContext(secretContext SecretContext) error {
	if secretContext.AlertID == "" {
		return fmt.Errorf("%w: alert ID is required for encryption", ErrInvalidArgument)
	}
	if secretContext.Generation == 0 {
		return fmt.Errorf("%w: secret generation must be positive", ErrInvalidArgument)
	}
	if secretContext.Purpose != PurposeEndpoint && secretContext.Purpose != PurposeHMACSecret {
		return fmt.Errorf("%w: encryption purpose is invalid", ErrInvalidArgument)
	}
	return nil
}

func additionalData(secretContext SecretContext) []byte {
	return []byte(fmt.Sprintf("open-splunk/alerts/v1/%s/%s/%d", secretContext.AlertID, secretContext.Purpose, secretContext.Generation))
}
