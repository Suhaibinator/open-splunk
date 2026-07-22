package savedobjects

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

	"github.com/Suhaibinator/open-splunk/internal/control"
)

const (
	savedSearchCursorVersion = 1
	maximumCursorBytes       = 4096
)

type listCursor struct {
	Version     int    `json:"v"`
	FilterHash  string `json:"f"`
	StringKey   string `json:"s,omitempty"`
	IntegerKey  *int64 `json:"n,omitempty"`
	SavedSearch string `json:"i"`
}

func encodeListCursor(key []byte, cursor listCursor) (string, error) {
	cursor.Version = savedSearchCursorVersion
	payload, err := json.Marshal(cursor)
	if err != nil {
		return "", fmt.Errorf("encode saved-search cursor: %w", err)
	}
	signature := signListCursor(key, payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func decodeListCursor(key []byte, token, filterHash string, stringSort bool) (listCursor, error) {
	invalid := func() (listCursor, error) {
		return listCursor{}, fmt.Errorf("%w: page token is invalid or does not match the request", control.ErrInvalidArgument)
	}
	if token == "" || len(token) > maximumCursorBytes || strings.Count(token, ".") != 1 {
		return invalid()
	}
	parts := strings.SplitN(token, ".", 2)
	payload, err := decodeCanonicalBase64(parts[0])
	if err != nil {
		return invalid()
	}
	signature, err := decodeCanonicalBase64(parts[1])
	if err != nil || len(signature) != sha256.Size || !hmac.Equal(signature, signListCursor(key, payload)) {
		return invalid()
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var cursor listCursor
	if err := decoder.Decode(&cursor); err != nil {
		return invalid()
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return invalid()
	}
	if cursor.Version != savedSearchCursorVersion || cursor.FilterHash != filterHash || cursor.SavedSearch == "" {
		return invalid()
	}
	if stringSort {
		if cursor.IntegerKey != nil || cursor.StringKey == "" {
			return invalid()
		}
	} else if cursor.IntegerKey == nil || cursor.StringKey != "" {
		return invalid()
	}
	return cursor, nil
}

func decodeCanonicalBase64(value string) ([]byte, error) {
	if value == "" {
		return nil, errors.New("empty base64 value")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || base64.RawURLEncoding.EncodeToString(decoded) != value {
		return nil, errors.New("non-canonical base64 value")
	}
	return decoded, nil
}

func signListCursor(key, payload []byte) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte("open-splunk/saved-search-list-cursor/v" + strconv.Itoa(savedSearchCursorVersion) + "\x00"))
	_, _ = mac.Write(payload)
	return mac.Sum(nil)
}
