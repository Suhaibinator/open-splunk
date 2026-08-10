package hec

import (
	"net/http"
	"strings"
	"testing"
)

func TestParseAuthorizationUsesExactClosedGrammar(t *testing.T) {
	t.Parallel()
	validToken := "os_hec_Abc-123._~"
	tests := []struct {
		name     string
		values   []string
		limit    int
		want     string
		wantKind ErrorKind
	}{
		{name: "exact", values: []string{"Splunk " + validToken}, limit: HardMaximumHeaderBytes, want: validToken},
		{name: "missing", limit: HardMaximumHeaderBytes, wantKind: ErrorTokenRequired},
		{name: "repeated", values: []string{"Splunk a", "Splunk b"}, limit: HardMaximumHeaderBytes, wantKind: ErrorInvalidAuthorization},
		{name: "wrong scheme case", values: []string{"splunk token"}, limit: HardMaximumHeaderBytes, wantKind: ErrorInvalidAuthorization},
		{name: "wrong scheme", values: []string{"Bearer token"}, limit: HardMaximumHeaderBytes, wantKind: ErrorInvalidAuthorization},
		{name: "no separator", values: []string{"Splunktoken"}, limit: HardMaximumHeaderBytes, wantKind: ErrorInvalidAuthorization},
		{name: "two separators", values: []string{"Splunk  token"}, limit: HardMaximumHeaderBytes, wantKind: ErrorInvalidAuthorization},
		{name: "empty credential", values: []string{"Splunk "}, limit: HardMaximumHeaderBytes, wantKind: ErrorInvalidAuthorization},
		{name: "tab", values: []string{"Splunk token\ttail"}, limit: HardMaximumHeaderBytes, wantKind: ErrorInvalidAuthorization},
		{name: "unicode whitespace", values: []string{"Splunk token\u00a0tail"}, limit: HardMaximumHeaderBytes, wantKind: ErrorInvalidAuthorization},
		{name: "control", values: []string{"Splunk token\x00tail"}, limit: HardMaximumHeaderBytes, wantKind: ErrorInvalidAuthorization},
		{name: "comma joined", values: []string{"Splunk first,Splunk second"}, limit: HardMaximumHeaderBytes, wantKind: ErrorInvalidAuthorization},
		{name: "invalid UTF-8", values: []string{"Splunk \xff"}, limit: HardMaximumHeaderBytes, wantKind: ErrorInvalidAuthorization},
		{name: "over limit", values: []string{"Splunk token"}, limit: 5, wantKind: ErrorInvalidAuthorization},
		{name: "invalid configured limit", values: []string{"Splunk token"}, limit: 0, wantKind: ErrorInvalidAuthorization},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseAuthorization(test.values, test.limit)
			if test.wantKind == 0 {
				if err != nil || got != test.want {
					t.Fatalf("ParseAuthorization() = %q, %v, want %q, nil", got, err, test.want)
				}
				return
			}
			if got != "" || !IsErrorKind(err, test.wantKind) {
				t.Fatalf("ParseAuthorization() = %q, %v, want kind %v", got, err, test.wantKind)
			}
		})
	}
}

func TestClassifyRouteCoversExactSurfaceAndNamespace(t *testing.T) {
	t.Parallel()
	tests := []struct {
		method      string
		path        string
		endpoint    Endpoint
		namespace   bool
		known       bool
		allowed     bool
		failureKind ErrorKind
	}{
		{http.MethodPost, "/services/collector", EndpointEvent, true, true, true, 0},
		{http.MethodPost, "/services/collector/event", EndpointEvent, true, true, true, 0},
		{http.MethodPost, "/services/collector/event/1.0", EndpointEvent, true, true, true, 0},
		{http.MethodPost, "/services/collector/raw", EndpointRaw, true, true, true, 0},
		{http.MethodPost, "/services/collector/raw/1.0", EndpointRaw, true, true, true, 0},
		{http.MethodPost, "/services/collector/ack", EndpointAcknowledgment, true, true, true, 0},
		{http.MethodGet, "/services/collector/health", EndpointHealth, true, true, true, 0},
		{http.MethodGet, "/services/collector/health/1.0", EndpointHealth, true, true, true, 0},
		{http.MethodGet, "/services/collector/event", EndpointEvent, true, true, false, ErrorMethodNotAllowed},
		{http.MethodPost, "/services/collector/health", EndpointHealth, true, true, false, ErrorMethodNotAllowed},
		{"post", "/services/collector", EndpointEvent, true, true, false, ErrorMethodNotAllowed},
		{http.MethodPost, "/services/collector/", EndpointUnknown, true, false, false, ErrorUnknownPath},
		{http.MethodPost, "/services/collector/event/", EndpointUnknown, true, false, false, ErrorUnknownPath},
		{http.MethodPost, "/services/collector/nope", EndpointUnknown, true, false, false, ErrorUnknownPath},
		{http.MethodPost, "/services/collectorx", EndpointOutside, false, false, false, 0},
		{http.MethodPost, "/", EndpointOutside, false, false, false, 0},
	}
	for _, test := range tests {
		t.Run(test.method+" "+test.path, func(t *testing.T) {
			t.Parallel()
			got := ClassifyRoute(test.method, test.path)
			wantAllow := ""
			if test.known {
				wantAllow = http.MethodPost
				if test.endpoint == EndpointHealth {
					wantAllow = http.MethodGet
				}
			}
			if got.Endpoint != test.endpoint || got.HECNamespace != test.namespace ||
				got.KnownPath != test.known || got.MethodAllowed != test.allowed || got.Method != test.method || got.Allow != wantAllow {
				t.Fatalf("ClassifyRoute() = %#v", got)
			}
			err := got.ProtocolError()
			if test.failureKind == 0 {
				if err != nil {
					t.Fatalf("ProtocolError() = %v, want nil", err)
				}
			} else if !IsErrorKind(err, test.failureKind) {
				t.Fatalf("ProtocolError() = %v, want kind %v", err, test.failureKind)
			}
		})
	}
}

func TestParseChannelPreservesCanonicalGUIDIdentity(t *testing.T) {
	t.Parallel()
	lower := "123e4567-e89b-12d3-a456-426614174000"
	upper := strings.ToUpper(lower)
	for _, value := range []string{lower, upper, "00000000-0000-0000-0000-000000000000"} {
		got, err := ParseChannel(value, HardMaximumChannelBytes)
		if err != nil || string(got) != value {
			t.Fatalf("ParseChannel(%q) = %q, %v", value, got, err)
		}
	}
	invalid := []string{
		"", " 123e4567-e89b-12d3-a456-426614174000", "123e4567-e89b-12d3-a456-426614174000 ",
		"123e4567e89b12d3a456426614174000", "123e4567-e89b-12d3-a456-42661417400",
		"123e4567-e89b-12d3-a456-4266141740000", "123e4567_e89b-12d3-a456-426614174000",
		"123e4567-e89b-12d3-a456-42661417400g", "123e4567-é89b-12d3-a456-426614174000",
	}
	for _, value := range invalid {
		if got, err := ParseChannel(value, HardMaximumChannelBytes); got != "" || !IsErrorKind(err, ErrorChannelInvalid) {
			t.Errorf("ParseChannel(%q) = %q, %v, want invalid channel", value, got, err)
		}
	}
	if _, err := ParseChannel(lower, 35); !IsErrorKind(err, ErrorChannelInvalid) {
		t.Fatalf("ParseChannel() below GUID length error = %v", err)
	}
}

func TestParseRequestChannelClosesRepetitionConflictAndMissingCases(t *testing.T) {
	t.Parallel()
	channel := "123e4567-e89b-12d3-a456-426614174000"
	tests := []struct {
		name     string
		header   []string
		query    []string
		required bool
		want     Channel
		present  bool
		wantKind ErrorKind
	}{
		{name: "optional absent", want: "", present: false},
		{name: "required absent", required: true, wantKind: ErrorChannelMissing},
		{name: "header", header: []string{channel}, want: Channel(channel), present: true},
		{name: "query", query: []string{channel}, want: Channel(channel), present: true},
		{name: "matching sources still conflict", header: []string{channel}, query: []string{channel}, wantKind: ErrorChannelInvalid},
		{name: "conflicting sources", header: []string{channel}, query: []string{strings.ToUpper(channel)}, wantKind: ErrorChannelInvalid},
		{name: "repeated header", header: []string{channel, channel}, wantKind: ErrorChannelInvalid},
		{name: "repeated query", query: []string{channel, channel}, wantKind: ErrorChannelInvalid},
		{name: "invalid present", header: []string{"not-a-guid"}, wantKind: ErrorChannelInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, present, err := ParseRequestChannel(test.header, test.query, test.required, HardMaximumChannelBytes)
			if test.wantKind != 0 {
				if got != "" || present || !IsErrorKind(err, test.wantKind) {
					t.Fatalf("ParseRequestChannel() = %q, %t, %v, want kind %v", got, present, err, test.wantKind)
				}
				return
			}
			if err != nil || got != test.want || present != test.present {
				t.Fatalf("ParseRequestChannel() = %q, %t, %v", got, present, err)
			}
		})
	}
}

func FuzzParseAuthorization(f *testing.F) {
	f.Add("Splunk token")
	f.Add("splunk token")
	f.Add("Splunk token token")
	f.Add("\xff")
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > HardMaximumHeaderBytes+1 {
			return
		}
		token, err := ParseAuthorization([]string{value}, HardMaximumHeaderBytes)
		if err == nil {
			if token == "" || value != authorizationPrefix+token {
				t.Fatalf("accepted noncanonical authorization %q as %q", value, token)
			}
			return
		}
		if token != "" || !IsErrorKind(err, ErrorInvalidAuthorization) {
			t.Fatalf("rejected authorization returned token/kind: %q, %v", token, err)
		}
	})
}

func FuzzParseChannel(f *testing.F) {
	f.Add("123e4567-e89b-12d3-a456-426614174000")
	f.Add("not-a-guid")
	f.Add("")
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > 256 {
			return
		}
		channel, err := ParseChannel(value, HardMaximumChannelBytes)
		if err == nil {
			if string(channel) != value || len(value) != 36 {
				t.Fatalf("accepted channel changed identity: %q -> %q", value, channel)
			}
			return
		}
		if channel != "" || !IsErrorKind(err, ErrorChannelInvalid) {
			t.Fatalf("invalid channel result = %q, %v", channel, err)
		}
	})
}
