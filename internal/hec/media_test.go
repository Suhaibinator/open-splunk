package hec

import "testing"

func TestParseContentEncodingUsesOneCaseInsensitiveGzipToken(t *testing.T) {
	t.Parallel()
	for _, values := range [][]string{nil, {}} {
		got, err := ParseContentEncoding(values)
		if err != nil || got != "" {
			t.Fatalf("ParseContentEncoding(%q) = %q, %v", values, got, err)
		}
	}
	for _, value := range []string{"gzip", "GZIP", "GZip"} {
		got, err := ParseContentEncoding([]string{value})
		if err != nil || got != "gzip" {
			t.Errorf("ParseContentEncoding(%q) = %q, %v", value, got, err)
		}
	}
	for _, values := range [][]string{{"identity"}, {" gzip"}, {"gzip "}, {"gzip,gzip"}, {"gzip", "gzip"}} {
		got, err := ParseContentEncoding(values)
		if got != "" || !IsErrorKind(err, ErrorUnsupportedContentEncoding) {
			t.Errorf("ParseContentEncoding(%q) = %q, %v", values, got, err)
		}
	}
}

func TestParseContentTypeAppliesEndpointAllowlist(t *testing.T) {
	t.Parallel()
	tests := []struct {
		endpoint Endpoint
		value    string
		valid    bool
	}{
		{EndpointEvent, "", true},
		{EndpointEvent, "application/json", true},
		{EndpointEvent, "Application/JSON; Charset=UTF-8", true},
		{EndpointEvent, "application/x-www-form-urlencoded", true},
		{EndpointEvent, "application/x-www-form-urlencoded; charset=utf-8", false},
		{EndpointAcknowledgment, "application/json; charset=utf-8", true},
		{EndpointAcknowledgment, "application/x-www-form-urlencoded", false},
		{EndpointRaw, "text/plain", true},
		{EndpointRaw, "TEXT/PLAIN; CHARSET=utf-8", true},
		{EndpointRaw, "application/octet-stream", true},
		{EndpointRaw, "application/octet-stream; charset=utf-8", false},
		{EndpointRaw, "text/plain; charset=ascii", false},
		{EndpointRaw, "text/plain; charset=utf-8; format=flowed", false},
		{EndpointRaw, "application/json", false},
	}
	for _, test := range tests {
		var values []string
		if test.value != "" {
			values = []string{test.value}
		}
		err := ParseContentType(test.endpoint, values)
		if test.valid && err != nil {
			t.Errorf("ParseContentType(%v, %q) = %v", test.endpoint, test.value, err)
		} else if !test.valid && !IsErrorKind(err, ErrorUnsupportedMediaType) {
			t.Errorf("ParseContentType(%v, %q) = %v", test.endpoint, test.value, err)
		}
	}
	if err := ParseContentType(EndpointEvent, []string{"application/json", "application/json"}); !IsErrorKind(err, ErrorUnsupportedMediaType) {
		t.Fatalf("repeated content type error = %v", err)
	}
	if err := ParseContentType(EndpointHealth, []string{"application/json"}); !IsErrorKind(err, ErrorInternal) {
		t.Fatalf("health content type error = %v", err)
	}
}

func TestValidateConsumedHeaderBytesIsAggregateAndOverflowSafe(t *testing.T) {
	t.Parallel()
	if err := ValidateConsumedHeaderBytes(5, []string{"ab"}, []string{"cde"}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateConsumedHeaderBytes(5, []string{"ab"}, []string{"cdef"}); !IsErrorKind(err, ErrorHeaderTooLarge) {
		t.Fatalf("oversized headers error = %v", err)
	}
	if err := ValidateConsumedHeaderBytes(0, nil); !IsErrorKind(err, ErrorInternal) {
		t.Fatalf("invalid limit error = %v", err)
	}
}
