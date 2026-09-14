package collector

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/protobuf/proto"
)

// Fixtures follow the producer contracts, not a parser-generated oracle:
// https://docs.docker.com/engine/logging/drivers/json-file/
// https://nginx.org/en/docs/http/ngx_http_log_module.html#log_format
// https://httpd.apache.org/docs/2.4/mod/mod_log_config.html#formats
const nativeFixedTime = "2026-02-28T20:30:12.123456789Z"

func TestNativeFixedDockerCanonicalAndExtras(t *testing.T) {
	t.Parallel()
	d := newTestDecoder(t, DecodeConfig{Format: InputFormat("docker-json-file"), ConstantFields: &opensplunk.TypedObject{Fields: []*opensplunk.TypedObjectField{{Name: "environment", Value: stringValue("prod")}}}})
	raw := []byte(`{"log":"first\nsecond\n","stream":"stderr","time":"2026-02-28T20:30:12.123456789Z","attempt":3,"tax":0.1,"attrs":{"tag":"日本語"},"tags":[true,null],"environment":"forged","host":"forged","index_name":"forged","source":"forged","sourcetype":"forged","message":"forged","level":"ERROR"}`)
	event := nativeFixedDecode(t, d, raw)
	if event.GetMessage() != "first\nsecond\n" || !bytes.Equal(event.GetRaw(), raw) {
		t.Fatalf("message/raw lost Docker framing semantics: %q / %q", event.GetMessage(), event.GetRaw())
	}
	want, err := time.Parse(time.RFC3339Nano, nativeFixedTime)
	if err != nil {
		t.Fatal(err)
	}
	if !event.GetEventTime().AsTime().Equal(want) || event.GetEventTimeSource() != opensplunk.EventTimeSource_EVENT_TIME_SOURCE_PARSED {
		t.Fatalf("timestamp = %v", event.GetEventTime())
	}
	if event.GetHost() != "fixture-host" || event.GetIndexName() != "gradethis" || event.GetSource() != "app.log" || event.GetSourcetype() != "go:zap:json" {
		t.Fatal("Docker payload overrode trusted metadata")
	}
	fields := objectFields(event.GetFields())
	assertStringValue(t, fields["docker_stream"], "stderr")
	assertStringValue(t, fields["environment"], "prod")
	nativeFixedSigned(t, fields["attempt"], 3)
	assertDecimalValue(t, fields["tax"], "0.1")
	assertStringValue(t, objectFields(fields["attrs"].GetObjectValue())["tag"], "日本語")
	if len(fields["tags"].GetListValue().GetValues()) != 2 {
		t.Fatal("Docker nested list lost")
	}
	for _, name := range []string{"log", "time", "stream", "message", "host", "index_name", "source", "sourcetype"} {
		if fields[name] != nil {
			t.Errorf("consumed/reserved field %q leaked", name)
		}
	}
	// Decode results must not borrow either input bytes or mutable parser scratch.
	snapshot := proto.Clone(event)
	for i := range raw {
		raw[i] = 'x'
	}
	_ = nativeFixedDecode(t, d, []byte(`{"log":"","stream":"stdout","time":"2026-02-28T20:30:12Z"}`))
	if !proto.Equal(event, snapshot) {
		t.Fatal("later decode or caller mutation changed an earlier event")
	}
}

func TestNativeFixedDockerRejectsInvalidEnvelope(t *testing.T) {
	t.Parallel()
	d := newTestDecoder(t, DecodeConfig{Format: InputFormat("docker-json-file")})
	valid := `{"log":"ok","stream":"stdout","time":"2026-02-28T20:30:12Z"}`
	cases := []string{
		"", " ", "[]", "null", valid + " trailing",
		`{"stream":"stdout","time":"2026-02-28T20:30:12Z"}`,
		`{"log":"ok","time":"2026-02-28T20:30:12Z"}`,
		`{"log":"ok","stream":"stdout"}`,
		strings.Replace(valid, `"ok"`, `null`, 1), strings.Replace(valid, `"ok"`, `12`, 1),
		strings.Replace(valid, `"stdout"`, `"STDOUT"`, 1), strings.Replace(valid, `"stdout"`, `"other"`, 1),
		strings.Replace(valid, `"stdout"`, `true`, 1), strings.Replace(valid, `"2026-02-28T20:30:12Z"`, `1`, 1),
		strings.Replace(valid, `2026-02-28`, `2026-02-29`, 1), strings.Replace(valid, `20:30:12Z`, `20:30:60Z`, 1),
		strings.Replace(valid, `20:30:12Z`, `20:30:12`, 1),
		strings.Replace(valid, `"log":"ok"`, `"log":"ok","log":"again"`, 1),
		strings.Replace(valid, `"log":"ok"`, `"log":"ok","attrs":{"x":1,"x":2}`, 1),
		strings.Replace(valid, `"log":"ok"`, `"log":"ok","`+strings.Repeat("k", 257)+`":1`, 1),
	}
	for i, raw := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) { nativeFixedReject(t, d, raw) })
	}
}

func TestNativeFixedAccessPresets(t *testing.T) {
	t.Parallel()
	for _, format := range []string{"nginx-combined", "apache-common", "apache-combined"} {
		t.Run(format, func(t *testing.T) {
			d := newTestDecoder(t, DecodeConfig{Format: InputFormat(format)})
			for _, address := range []string{"192.0.2.9", "2001:db8::17", "client.example"} {
				raw := address + ` ident alice [29/Feb/2024:23:59:58 +0530] "GET /a?x=1=2 HTTP/1.1" 200 123`
				if format != "apache-common" {
					raw += ` "https://example.test/" "agent"`
				}
				event := nativeFixedDecode(t, d, []byte(raw))
				fields := objectFields(event.GetFields())
				for key, want := range map[string]string{"client_address": address, "ident": "ident", "user": "alice", "request": "GET /a?x=1=2 HTTP/1.1", "method": "GET", "request_target": "/a?x=1=2", "protocol": "HTTP/1.1"} {
					assertStringValue(t, fields[key], want)
				}
				nativeFixedSigned(t, fields["status"], 200)
				nativeFixedSigned(t, fields["response_bytes"], 123)
				if format != "apache-common" {
					assertStringValue(t, fields["referrer"], "https://example.test/")
					assertStringValue(t, fields["user_agent"], "agent")
				}
				if event.GetMessage() != "GET /a?x=1=2 HTTP/1.1" || !bytes.Equal(event.GetRaw(), []byte(raw)) {
					t.Fatal("access canonical message/raw mismatch")
				}
				want := time.Date(2024, 2, 29, 18, 29, 58, 0, time.UTC)
				if !event.GetEventTime().AsTime().Equal(want) {
					t.Fatalf("event time = %v, want %v", event.GetEventTime(), want)
				}
				if event.Level != nil {
					t.Fatalf("access log invented a level: %q", event.GetLevel())
				}
			}
		})
	}
}

func TestNativeFixedAccessEscapesAndSentinels(t *testing.T) {
	t.Parallel()
	for _, format := range []string{"nginx-combined", "apache-combined"} {
		t.Run(format, func(t *testing.T) {
			d := newTestDecoder(t, DecodeConfig{Format: InputFormat(format)})
			ua, want := `a\x22b\x5Cc\x09d\x0Ae`, "a\"b\\c\td\ne"
			if format == "apache-combined" {
				ua = `a\"b\\c\td\ne`
			}
			raw := `2001:db8::1 - - [28/Feb/2026:20:30:12 +0000] "GET / HTTP/1.1" 503 0 "" "` + ua + `"`
			event := nativeFixedDecode(t, d, []byte(raw))
			assertStringValue(t, fieldValue(event, "user_agent"), want)
			assertStringValue(t, fieldValue(event, "referrer"), "")
			for _, key := range []string{"ident", "user"} {
				if fieldValue(event, key) != nil {
					t.Errorf("dash sentinel emitted as %s", key)
				}
			}
			if event.Level != nil {
				t.Fatal("HTTP 503 inferred severity")
			}
			binary := nativeFixedDecode(t, d, []byte(strings.Replace(raw, ua, `bad\xFF\x00`, 1)))
			if value := fieldValue(binary, "user_agent"); !bytes.Equal(value.GetBytesValue(), []byte{'b', 'a', 'd', 0xff, 0}) {
				t.Fatalf("binary header = %v", value)
			}
			if _, err := proto.Marshal(binary); err != nil {
				t.Fatalf("binary access value cannot cross WAL: %v", err)
			}
		})
	}
	d := newTestDecoder(t, DecodeConfig{Format: InputFormat("apache-common")})
	event := nativeFixedDecode(t, d, []byte(`host - - [28/Feb/2026:20:30:12 +0000] "-" 400 -`))
	nativeFixedSigned(t, fieldValue(event, "response_bytes"), 0)
}

func TestNativeFixedAccessMalformedRequestIsAnEvent(t *testing.T) {
	t.Parallel()
	for _, format := range []string{"nginx-combined", "apache-common", "apache-combined"} {
		t.Run(format, func(t *testing.T) {
			d := newTestDecoder(t, DecodeConfig{Format: InputFormat(format)})
			for _, request := range []string{"", "-", "GET", "GET /missing-version", "GET /has space HTTP/1.1", "not an http request"} {
				raw := `host - - [28/Feb/2026:20:30:12 +0000] "` + request + `" 400 0`
				if format != "apache-common" {
					raw += ` "-" "-"`
				}
				event := nativeFixedDecode(t, d, []byte(raw))
				if event.GetMessage() != request {
					t.Fatalf("malformed request %q lost: %q", request, event.GetMessage())
				}
				for _, key := range []string{"method", "request_target", "protocol"} {
					if fieldValue(event, key) != nil {
						t.Errorf("invented component %q for malformed request %q", key, request)
					}
				}
			}
		})
	}
}

func TestNativeFixedAccessRejectsMalformedEnvelope(t *testing.T) {
	t.Parallel()
	for _, format := range []string{"nginx-combined", "apache-common", "apache-combined"} {
		t.Run(format, func(t *testing.T) {
			d := newTestDecoder(t, DecodeConfig{Format: InputFormat(format)})
			valid := `host - - [28/Feb/2026:20:30:12 +0000] "GET / HTTP/1.1" 200 12`
			if format != "apache-common" {
				valid += ` "-" "agent"`
			}
			cases := []string{"", " ", valid + ` trailing`, strings.Replace(valid, "28/Feb", "30/Feb", 1), strings.Replace(valid, "+0000", "+2560", 1), strings.Replace(valid, "[28/Feb", "28/Feb", 1), strings.Replace(valid, `] "GET`, ` "GET`, 1), strings.Replace(valid, `HTTP/1.1"`, `HTTP/1.1`, 1), strings.Replace(valid, " 200 ", " -1 ", 1), strings.Replace(valid, " 200 ", " 200.5 ", 1), strings.Replace(valid, " 200 ", " 18446744073709551616 ", 1), strings.Replace(valid, " 12", " 18446744073709551616", 1), strings.Replace(valid, " 12", " -12", 1), strings.Replace(valid, "GET /", `GET /\xQ1`, 1)}
			for i, raw := range cases {
				t.Run(fmt.Sprint(i), func(t *testing.T) { nativeFixedReject(t, d, raw) })
			}
			if format == "nginx-combined" {
				nativeFixedReject(t, d, strings.Replace(valid, "agent", `bad\n`, 1))
			}
		})
	}
}

func TestNativeFixedDockerBounds(t *testing.T) {
	t.Parallel()
	d := newTestDecoder(t, DecodeConfig{Format: InputFormat("docker-json-file")})
	prefix, suffix := `{"log":"`, `","stream":"stdout","time":"2026-02-28T20:30:12Z"}`
	atLimit := prefix + strings.Repeat("x", defaultMaxLineBytes-len(prefix)-len(suffix)) + suffix
	_ = nativeFixedDecode(t, d, []byte(atLimit))
	nativeFixedReject(t, d, atLimit+" ")
	var wide strings.Builder
	wide.WriteString(`{"log":"ok","stream":"stdout","time":"2026-02-28T20:30:12Z"`)
	for i := range defaultMaxJSONFields {
		fmt.Fprintf(&wide, `,"extra%d":0`, i)
	}
	wide.WriteByte('}')
	nativeFixedReject(t, d, wide.String())
	deep := `{"log":"ok","stream":"stdout","time":"2026-02-28T20:30:12Z","extra":` + strings.Repeat(`{"x":`, defaultMaxJSONDepth) + `0` + strings.Repeat("}", defaultMaxJSONDepth) + `}`
	nativeFixedReject(t, d, deep)
}

func nativeFixedDecode(t *testing.T, d *Decoder, raw []byte) *opensplunk.LogEvent {
	t.Helper()
	event, err := d.Decode(raw, SourcePosition{FileIdentity: "native-fixed", StartOffset: 10, EndOffset: 10 + uint64(len(raw)), LineNumber: 2, NextLineNumber: 3}, time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	return event
}

func nativeFixedReject(t *testing.T, d *Decoder, raw string) {
	t.Helper()
	if event, err := d.Decode([]byte(raw), SourcePosition{}, time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)); err == nil {
		t.Fatalf("accepted malformed record (%d bytes): %v", len(raw), event)
	}
}

func FuzzNativeFixedDecoders(f *testing.F) {
	formats := []string{"docker-json-file", "nginx-combined", "apache-common", "apache-combined"}
	decoders := make([]*Decoder, len(formats))
	for i, format := range formats {
		d, err := NewDecoder(DecodeConfig{Format: InputFormat(format), InputID: "fuzz", IndexName: "test", Source: "fuzz", Sourcetype: format, Host: "test", MaxLineBytes: 4096})
		if err != nil {
			f.Fatal(err)
		}
		decoders[i] = d
	}
	f.Add(uint8(0), []byte(`{"log":"hello\n","stream":"stdout","time":"2026-02-28T20:30:12Z"}`))
	f.Add(uint8(1), []byte(`host - - [28/Feb/2026:20:30:12 +0000] "GET / HTTP/1.1" 200 0 "-" "\xFF"`))
	f.Add(uint8(2), []byte(`host - - [28/Feb/2026:20:30:12 +0000] "broken" 400 -`))
	f.Add(uint8(3), []byte{0xff, '"', '\\', 0})
	f.Fuzz(func(t *testing.T, which uint8, raw []byte) {
		event, err := decoders[int(which)%len(decoders)].Decode(raw, SourcePosition{}, time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC))
		if err != nil {
			return
		}
		if !bytes.Equal(event.GetRaw(), raw) {
			t.Fatal("accepted event altered raw")
		}
		if _, err := proto.Marshal(event); err != nil {
			t.Fatalf("accepted event cannot serialize: %v", err)
		}
		if event.GetEventTime().CheckValid() != nil {
			t.Fatal("accepted event has invalid timestamp")
		}
		if _, err := json.Marshal(event); err != nil {
			t.Fatalf("accepted event cannot be inspected: %v", err)
		}
	})
}

func nativeFixedSigned(t *testing.T, value *opensplunk.TypedValue, want int64) {
	t.Helper()
	integer, ok := value.GetKind().(*opensplunk.TypedValue_Sint64Value)
	if !ok || integer.Sint64Value != want {
		t.Fatalf("signed integer value = %#v, want %d", value, want)
	}
}
