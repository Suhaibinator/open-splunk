package searchhistory

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

const historyCursorVersion = 1

type listCursor struct {
	Version    int    `json:"v"`
	FilterHash string `json:"f"`
	SortKey    *int64 `json:"k"`
	JobID      string `json:"i"`
}

func encodeCursor(key []byte, cursor listCursor) (string, error) {
	cursor.Version = historyCursorVersion
	payload, err := json.Marshal(cursor)
	if err != nil {
		return "", fmt.Errorf("encode search-history cursor: %w", err)
	}
	signature := signCursor(key, payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func decodeCursor(key []byte, token, filterHash string) (listCursor, error) {
	invalidToken := func() (listCursor, error) {
		return listCursor{}, fmt.Errorf("%w: page token is invalid or does not match the request", control.ErrInvalidArgument)
	}
	if token == "" || len(token) > maximumCursorBytes || strings.Count(token, ".") != 1 {
		return invalidToken()
	}
	parts := strings.SplitN(token, ".", 2)
	payload, err := decodeCanonicalBase64(parts[0])
	if err != nil {
		return invalidToken()
	}
	signature, err := decodeCanonicalBase64(parts[1])
	if err != nil || len(signature) != sha256.Size || !hmac.Equal(signature, signCursor(key, payload)) {
		return invalidToken()
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var cursor listCursor
	if err := decoder.Decode(&cursor); err != nil {
		return invalidToken()
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return invalidToken()
	}
	if cursor.Version != historyCursorVersion || cursor.FilterHash != filterHash || cursor.SortKey == nil || cursor.JobID == "" {
		return invalidToken()
	}
	if err := validateText("cursor search job ID", cursor.JobID, maximumSearchJobIDBytes, false); err != nil {
		return invalidToken()
	}
	return cursor, nil
}

func signCursor(key, payload []byte) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte("open-splunk/search-history-list-cursor/v" + strconv.Itoa(historyCursorVersion) + "\x00"))
	_, _ = mac.Write(payload)
	return mac.Sum(nil)
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
