// Package cursorcodec implements the common authenticated envelope used by
// opaque Open Splunk pagination cursors. Payload meaning and replay checks stay
// with each owning package.
package cursorcodec

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// ErrInvalidToken deliberately combines malformed encodings and failed
// authentication so callers cannot use cursor errors as an oracle.
var ErrInvalidToken = errors.New("invalid signed cursor")

// Encode serializes value as JSON and returns a canonical base64url payload
// followed by its domain-separated HMAC-SHA256 signature.
func Encode(key []byte, domain string, version, maximumBytes int, value any) (string, error) {
	if err := validateConfiguration(key, domain, version, maximumBytes); err != nil {
		return "", err
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode signed cursor payload: %w", err)
	}
	signature := sign(key, domain, version, payload)
	token := base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(signature)
	if len(token) > maximumBytes {
		return "", errors.New("encode signed cursor: token exceeds its size limit")
	}
	return token, nil
}

// Decode authenticates token before strictly decoding its JSON payload into
// target. Unknown fields, trailing JSON, and non-canonical base64url fail.
func Decode(key []byte, domain string, version, maximumBytes int, token string, target any) error {
	if err := validateConfiguration(key, domain, version, maximumBytes); err != nil {
		return err
	}
	if token == "" || len(token) > maximumBytes || strings.Count(token, ".") != 1 {
		return ErrInvalidToken
	}
	parts := strings.SplitN(token, ".", 2)
	payload, err := decodeCanonicalBase64(parts[0])
	if err != nil {
		return ErrInvalidToken
	}
	signature, err := decodeCanonicalBase64(parts[1])
	if err != nil || len(signature) != sha256.Size || !hmac.Equal(signature, sign(key, domain, version, payload)) {
		return ErrInvalidToken
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return ErrInvalidToken
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ErrInvalidToken
	}
	return nil
}

func validateConfiguration(key []byte, domain string, version, maximumBytes int) error {
	if len(key) == 0 {
		return errors.New("signed cursor key is empty")
	}
	if domain == "" {
		return errors.New("signed cursor domain is empty")
	}
	if version <= 0 {
		return errors.New("signed cursor version is invalid")
	}
	if maximumBytes <= 0 {
		return errors.New("signed cursor size limit is invalid")
	}
	return nil
}

func sign(key []byte, domain string, version int, payload []byte) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte("open-splunk/" + domain + "/v" + strconv.Itoa(version) + "\x00"))
	_, _ = mac.Write(payload)
	return mac.Sum(nil)
}

func decodeCanonicalBase64(value string) ([]byte, error) {
	if value == "" {
		return nil, ErrInvalidToken
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || base64.RawURLEncoding.EncodeToString(decoded) != value {
		return nil, ErrInvalidToken
	}
	return decoded, nil
}
