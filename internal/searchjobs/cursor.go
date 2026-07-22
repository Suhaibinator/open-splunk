package searchjobs

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"strconv"
	"strings"
)

const (
	cursorVersion   = 1
	maxCursorLength = 2048
)

type cursorPayload struct {
	Version    int    `json:"v"`
	Scope      string `json:"s"`
	JobID      string `json:"j"`
	Generation uint64 `json:"g"`
	Offset     uint64 `json:"o"`
}

func encodeCursor(key []byte, scope, jobID string, generation, offset uint64) (string, error) {
	payload, err := json.Marshal(cursorPayload{
		Version:    cursorVersion,
		Scope:      scope,
		JobID:      jobID,
		Generation: generation,
		Offset:     offset,
	})
	if err != nil {
		return "", err
	}
	signature := signCursor(key, payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func decodeCursor(key []byte, token string) (cursorPayload, error) {
	if token == "" || len(token) > maxCursorLength || strings.Count(token, ".") != 1 {
		return cursorPayload{}, ErrInvalidCursor
	}
	parts := strings.SplitN(token, ".", 2)
	payload, err := decodeCanonicalBase64(parts[0])
	if err != nil {
		return cursorPayload{}, ErrInvalidCursor
	}
	signature, err := decodeCanonicalBase64(parts[1])
	if err != nil || len(signature) != sha256.Size || !hmac.Equal(signature, signCursor(key, payload)) {
		return cursorPayload{}, ErrInvalidCursor
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var decoded cursorPayload
	if err := decoder.Decode(&decoded); err != nil {
		return cursorPayload{}, ErrInvalidCursor
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return cursorPayload{}, ErrInvalidCursor
	}
	if decoded.Version != cursorVersion || decoded.Scope == "" || decoded.JobID == "" || decoded.Generation == 0 || decoded.Offset == 0 {
		return cursorPayload{}, ErrInvalidCursor
	}
	return decoded, nil
}

func decodeCanonicalBase64(encoded string) ([]byte, error) {
	if encoded == "" {
		return nil, ErrInvalidCursor
	}
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || base64.RawURLEncoding.EncodeToString(decoded) != encoded {
		return nil, ErrInvalidCursor
	}
	return decoded, nil
}

func signCursor(key, payload []byte) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte("open-splunk/search-result-cursor/v" + strconv.Itoa(cursorVersion) + "\x00"))
	_, _ = mac.Write(payload)
	return mac.Sum(nil)
}
