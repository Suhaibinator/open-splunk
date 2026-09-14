package collector

import (
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/collector/parserconfig"
	"google.golang.org/protobuf/proto"
)

func TestPerfReviewAstraNativeOwnershipAndEmptyShape(t *testing.T) {
	for _, tc := range []struct {
		format string
		raw    string
		parser *parserconfig.Options
	}{
		{"logfmt", `message="hello"`, nil},
		{"logback-pattern", "hello", &parserconfig.Options{Pattern: "%{message}"}},
		{"docker-json-file", `{"log":"hello","time":"2026-09-08T12:00:00Z","stream":"stdout"}`, nil},
		{"nginx-combined", `host - - [28/Feb/2026:20:30:12 +0000] "GET /hello HTTP/1.1" 200 12 "-" "agent"`, nil},
	} {
		t.Run(tc.format, func(t *testing.T) {
			d, err := NewDecoder(nativeTextConfig(tc.format, tc.parser))
			if err != nil {
				t.Fatal(err)
			}
			raw := []byte(tc.raw)
			decode := func() *opensplunk.LogEvent {
				t.Helper()
				e, err := d.Decode(raw, SourcePosition{}, nativeTextNow)
				if err != nil {
					t.Fatal(err)
				}
				return e
			}
			a, b := decode(), decode()
			want := proto.Clone(b)
			if a.Fields == nil || a.Fields.Fields == nil {
				t.Fatal("native fields lost nonnil shape")
			}
			*a.Message = "changed canonical"
			if tc.format == "nginx-combined" {
				fields := objectFields(a.Fields)
				assertStringValue(t, fields["request"], "GET /hello HTTP/1.1")
				fields["request"].Kind.(*opensplunk.TypedValue_StringValue).StringValue = "changed request"
				assertStringValue(t, fields["request_target"], "/hello")
				assertStringValue(t, fields["method"], "GET")
			}
			a.Fields.Fields = append(a.Fields.Fields, &opensplunk.TypedObjectField{Name: "extra", Value: stringValue("x")})
			a.EventTime.Seconds++
			a.CollectedAt.Seconds++
			a.Raw[0] = '!'
			if !proto.Equal(b, want) || !proto.Equal(decode(), want) {
				t.Fatal("native events share mutable ownership")
			}
			raw[0] = '?'
			if !proto.Equal(b, want) {
				t.Fatal("native event aliases raw")
			}
		})
	}
}

func TestPerfReviewAstraFallbackNullAndProvenance(t *testing.T) {
	d := newTestDecoder(t, DecodeConfig{Format: InputFormatNDJSON})
	now := time.Date(2026, 9, 8, 12, 34, 56, 123456789, time.FixedZone("offset", 20700))
	for _, raw := range []string{`{}`, `{"timestamp":null}`, `{"ts":null}`, `{"time":null}`, `{"@timestamp":null}`} {
		e, err := d.Decode([]byte(raw), SourcePosition{}, now)
		if err != nil {
			t.Fatal(err)
		}
		if !e.EventTime.AsTime().Equal(now) || e.EventTimeSource != opensplunk.EventTimeSource_EVENT_TIME_SOURCE_COLLECTED_AT_FALLBACK {
			t.Fatalf("fallback changed for %s", raw)
		}
		e.EventTime.Nanos++
		if !e.CollectedAt.AsTime().Equal(now) {
			t.Fatal("fallback aliases collected timestamp")
		}
	}
}
