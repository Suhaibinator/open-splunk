package collector

import (
	"bytes"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/protobuf/proto"
)

func TestAccessTimestampCanonicalSpellingRemainsStrict(t *testing.T) {
	decoder := newTestDecoder(t, DecodeConfig{Format: "apache-common"})
	for _, tc := range []struct {
		stamp string
		valid bool
	}{
		{"29/Feb/2024:12:34:56 +0000", true},
		{"29/Feb/2000:12:34:56 -0730", true},
		{"29/feb/2024:12:34:56 +0000", false},
		{"29/FEB/2024:12:34:56 +0000", false},
		{"28/Feb/+024:12:34:56 +0000", false},
		{"28/Feb/-024:12:34:56 +0000", false},
		{"29/Feb/2023:12:34:56 +0000", false},
	} {
		raw := []byte(`host - - [` + tc.stamp + `] "GET / HTTP/1.1" 200 1`)
		_, err := decoder.Decode(raw, SourcePosition{}, time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC))
		if (err == nil) != tc.valid {
			t.Errorf("timestamp %q: error=%v, want valid=%v", tc.stamp, err, tc.valid)
		}
	}
}

func TestAccessRequestProjectionOwnsOneStableResult(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		format  string
		request string
		want    string
		parts   map[string]string
	}{
		{name: "nginx escaped", format: "nginx-combined", request: `GET\x20/escaped\x20HTTP/1.1`, want: "GET /escaped HTTP/1.1", parts: map[string]string{"method": "GET", "request_target": "/escaped", "protocol": "HTTP/1.1"}},
		{name: "apache escaped", format: "apache-combined", request: `GET /escaped\"quote HTTP/1.1`, want: `GET /escaped"quote HTTP/1.1`, parts: map[string]string{"method": "GET", "request_target": `/escaped"quote`, "protocol": "HTTP/1.1"}},
		{name: "whitespace remains malformed", format: "apache-common", request: "GET  /two-spaces HTTP/1.1", want: "GET  /two-spaces HTTP/1.1"},
		{name: "malformed remains an event", format: "nginx-combined", request: "GET /missing-version", want: "GET /missing-version"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			decoder := newTestDecoder(t, DecodeConfig{Format: InputFormat(tc.format)})
			record := `host - - [28/Feb/2026:20:30:12 +0000] "` + tc.request + `" 200 12`
			if tc.format != "apache-common" {
				record += ` "-" "agent"`
			}
			raw := []byte(record)
			event := nativeFixedDecode(t, decoder, raw)
			snapshot := proto.Clone(event)
			for i := range raw {
				raw[i] ^= 0xff
			}
			if !proto.Equal(event, snapshot) {
				t.Fatal("request projection aliases the caller's record")
			}
			if event.GetMessage() != tc.want {
				t.Fatalf("message = %q, want %q", event.GetMessage(), tc.want)
			}
			fields := objectFields(event.GetFields())
			assertStringValue(t, fields["request"], tc.want)
			for _, name := range []string{"method", "request_target", "protocol"} {
				if want, ok := tc.parts[name]; ok {
					assertStringValue(t, fields[name], want)
				} else if fields[name] != nil {
					t.Fatalf("malformed request produced %s", name)
				}
			}
		})
	}
}

func TestAccessBinaryRequestProjectionRemainsBytes(t *testing.T) {
	t.Parallel()
	decoder := newTestDecoder(t, DecodeConfig{Format: InputFormat("nginx-combined")})
	raw := []byte(`host - - [28/Feb/2026:20:30:12 +0000] "GET /bin\xFF HTTP/1.1" 200 12 "-" "agent"`)
	event := nativeFixedDecode(t, decoder, raw)
	fields := objectFields(event.GetFields())
	wantRequest := []byte("GET /bin\xff HTTP/1.1")
	if event.Message != nil {
		t.Fatalf("binary request produced message %q", event.GetMessage())
	}
	if got := fields["request"].GetBytesValue(); !bytes.Equal(got, wantRequest) {
		t.Fatalf("request bytes = %x, want %x", got, wantRequest)
	}
	assertStringValue(t, fields["method"], "GET")
	if got := fields["request_target"].GetBytesValue(); !bytes.Equal(got, []byte("/bin\xff")) {
		t.Fatalf("request target bytes = %x", got)
	}
	assertStringValue(t, fields["protocol"], "HTTP/1.1")
	snapshot := proto.Clone(event)
	for i := range raw {
		raw[i] = 0
	}
	if !proto.Equal(event, snapshot) {
		t.Fatal("binary request projection aliases the caller's record")
	}
	if _, ok := fields["request"].GetKind().(*opensplunk.TypedValue_BytesValue); !ok {
		t.Fatalf("binary request kind = %T", fields["request"].GetKind())
	}
}
