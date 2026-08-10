package hec

import (
	"errors"
	"mime"
	"strings"
)

// ParseContentEncoding validates the complete Content-Encoding header source.
// The returned value is either empty (identity) or the canonical string gzip.
func ParseContentEncoding(values []string) (string, error) {
	if len(values) == 0 {
		return "", nil
	}
	if len(values) != 1 || !strings.EqualFold(values[0], "gzip") {
		return "", NewProtocolError(ErrorUnsupportedContentEncoding, nil)
	}
	return "gzip", nil
}

// ParseContentType validates one endpoint's closed media-type allowlist. An
// absent header is accepted. Repeated values, extra parameters, and non-UTF-8
// charsets fail with the HTTP 415/code 6 category.
func ParseContentType(endpoint Endpoint, values []string) error {
	if len(values) == 0 {
		return nil
	}
	if len(values) != 1 || values[0] == "" {
		return NewProtocolError(ErrorUnsupportedMediaType, nil)
	}
	mediaType, parameters, err := mime.ParseMediaType(values[0])
	if err != nil {
		return NewProtocolError(ErrorUnsupportedMediaType, err)
	}
	mediaType = strings.ToLower(mediaType)
	charsetOnly := func() bool {
		if len(parameters) == 0 {
			return true
		}
		charset, ok := parameters["charset"]
		return ok && len(parameters) == 1 && strings.EqualFold(charset, "utf-8")
	}
	switch endpoint {
	case EndpointEvent:
		if mediaType == "application/json" && charsetOnly() ||
			mediaType == "application/x-www-form-urlencoded" && len(parameters) == 0 {
			return nil
		}
	case EndpointAcknowledgment:
		if mediaType == "application/json" && charsetOnly() {
			return nil
		}
	case EndpointRaw:
		if mediaType == "text/plain" && charsetOnly() ||
			mediaType == "application/octet-stream" && len(parameters) == 0 {
			return nil
		}
	default:
		return NewProtocolError(ErrorInternal, errors.New("HEC endpoint has no request media contract"))
	}
	return NewProtocolError(ErrorUnsupportedMediaType, nil)
}

// ValidateConsumedHeaderBytes applies the aggregate HEC-consumed header-value
// bound before any value is copied into reusable request state.
func ValidateConsumedHeaderBytes(maximum int, valueGroups ...[]string) error {
	if maximum <= 0 || maximum > HardMaximumHeaderBytes {
		return NewProtocolError(ErrorInternal, errors.New("HEC header limit is invalid"))
	}
	total := 0
	for _, values := range valueGroups {
		for _, value := range values {
			if len(value) > maximum-total {
				return NewProtocolError(ErrorHeaderTooLarge, nil)
			}
			total += len(value)
		}
	}
	return nil
}
