package common_util

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// errInvalidDataURI is returned when a value cannot be parsed as a data: URI.
var errInvalidDataURI = errors.New("invalid data uri")

// EncodeDataURI renders raw bytes as a base64 data: URI for the given content
// type, the inverse of DecodeDataURI.
func EncodeDataURI(contentType string, data []byte) string {
	return "data:" + contentType + ";base64," + base64.StdEncoding.EncodeToString(data)
}

// HasDataURIScheme reports whether value starts with a case-insensitive "data:"
// scheme. Callers trim surrounding whitespace first when their source allows it.
func HasDataURIScheme(value string) bool {
	return len(value) >= len("data:") && strings.EqualFold(value[:len("data:")], "data:")
}

// DecodeDataURI parses a data: URI and returns its decoded payload and content
// type. The format is data:[mediatype][;param=value]*[;base64],payload — a
// base64 flag in any semicolon-delimited segment selects base64 decoding, and
// everything else is treated as percent-encoded text.
func DecodeDataURI(value string) ([]byte, string, error) {
	header, payload, ok := strings.Cut(value, ",")
	if !ok {
		return nil, "", errInvalidDataURI
	}
	if !HasDataURIScheme(header) {
		return nil, "", errInvalidDataURI
	}

	metadata := header[len("data:"):]
	contentType := metadata
	isBase64 := false
	if before, after, ok := strings.Cut(metadata, ";"); ok {
		contentType = before
		for segment := range strings.SplitSeq(after, ";") {
			if strings.EqualFold(segment, "base64") {
				isBase64 = true
				break
			}
		}
	} else if strings.EqualFold(metadata, "base64") {
		contentType = ""
		isBase64 = true
	}

	if isBase64 {
		data, err := base64.StdEncoding.DecodeString(payload)
		if err != nil {
			return nil, "", fmt.Errorf("failed decoding data uri payload: %w", err)
		}
		return data, contentType, nil
	}

	data, err := url.PathUnescape(payload)
	if err != nil {
		return nil, "", fmt.Errorf("failed decoding data uri payload: %w", err)
	}
	return []byte(data), contentType, nil
}
