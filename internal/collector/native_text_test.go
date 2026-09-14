package collector

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/collector/parserconfig"
	"google.golang.org/protobuf/proto"
)

var nativeTextNow = time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)

func nativeTextConfig(format string, options *parserconfig.Options) DecodeConfig {
	return DecodeConfig{Format: InputFormat(format), InputID: "text", IndexName: "main", Source: "trusted-source", Sourcetype: format, Host: "trusted-host", Service: "trusted-service", Parser: options}
}

func nativeTextDecoder(t testing.TB, format string, options *parserconfig.Options) *Decoder {
	t.Helper()
	decoder, err := NewDecoder(nativeTextConfig(format, options))
	if err != nil {
		t.Fatalf("NewDecoder(%s): %v", format, err)
	}
	return decoder
}

func nativeTextDecode(t testing.TB, decoder *Decoder, raw string) *opensplunk.LogEvent {
	t.Helper()
	event, err := decoder.Decode([]byte(raw), SourcePosition{FileIdentity: "text-fixture", StartOffset: 17, EndOffset: 17 + uint64(len(raw)) + 1, LineNumber: 2, NextLineNumber: 3}, nativeTextNow)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(event.GetRaw(), []byte(raw)) {
		t.Fatalf("raw changed: %q", event.GetRaw())
	}
	return event
}

func TestNativeTextLogfmtTypedGrammar(t *testing.T) {
	decoder := nativeTextDecoder(t, "logfmt", nil)
	raw := `timestamp=2024-02-29T23:59:59.123456789+05:45 level=warn message="hello\nworld\t\"quoted\" \\ ☕" empty= embedded=a=b=c quoted="12" yes=true no=false integer=-9223372036854775808 unsigned=18446744073709551615 huge=18446744073709551616 decimal=0.12345678901234567890123456789 float=1.5 nil=null leading=01 trace_id=trace span_id=span`
	event := nativeTextDecode(t, decoder, raw)
	if event.GetMessage() != "hello\nworld\t\"quoted\" \\ ☕" || event.GetSeverity() != opensplunk.LogSeverity_LOG_SEVERITY_WARN || event.GetTraceId() != "trace" || event.GetSpanId() != "span" {
		t.Fatalf("canonical projection: %v", event)
	}
	wantTime := time.Date(2024, 2, 29, 18, 14, 59, 123456789, time.UTC)
	if !event.GetEventTime().AsTime().Equal(wantTime) {
		t.Fatalf("event time = %s, want %s", event.GetEventTime().AsTime(), wantTime)
	}
	fields := objectFields(event.GetFields())
	for name, want := range map[string]string{"empty": "", "embedded": "a=b=c", "quoted": "12", "nil": "null", "leading": "01"} {
		assertStringValue(t, fields[name], want)
		if _, ok := fields[name].GetKind().(*opensplunk.TypedValue_StringValue); !ok {
			t.Fatalf("%s lost string type", name)
		}
	}
	if _, ok := fields["yes"].GetKind().(*opensplunk.TypedValue_BoolValue); !ok || !fields["yes"].GetBoolValue() {
		t.Fatal("true lost boolean type")
	}
	if _, ok := fields["no"].GetKind().(*opensplunk.TypedValue_BoolValue); !ok || fields["no"].GetBoolValue() {
		t.Fatal("false lost boolean type")
	}
	assertSignedValue(t, fields["integer"], -9223372036854775808)
	if fields["unsigned"].GetUint64Value() != 18446744073709551615 {
		t.Fatal("uint64 precision lost")
	}
	if fields["huge"].GetDecimalValue().GetValue() != "18446744073709551616" || fields["decimal"].GetDecimalValue().GetValue() != "0.12345678901234567890123456789" {
		t.Fatal("decimal precision lost")
	}
	if fields["float"].GetDoubleValue() != 1.5 {
		t.Fatal("float conversion changed")
	}
}

func TestNativeTextLogfmtMappingsAndOwnership(t *testing.T) {
	options := &parserconfig.Options{Fields: map[string]string{"timestamp": "when", "message": "text", "level": "priority", "trace_id": "trace", "span_id": "span"}}
	cfg := nativeTextConfig("logfmt", options)
	cfg.ConstantFields = &opensplunk.TypedObject{Fields: []*opensplunk.TypedObjectField{{Name: "environment", Value: stringValue("production")}}}
	decoder, err := NewDecoder(cfg)
	if err != nil {
		t.Fatal(err)
	}
	options.Fields["message"] = "changed"
	cfg.ConstantFields.Fields[0].Value = stringValue("changed")
	raw := "when=1700000000.000000001 text=hello priority=ERROR trace=a span=b host=attacker source=attacker service=attacker index=attacker environment=attacker"
	first := nativeTextDecode(t, decoder, raw)
	if first.GetMessage() != "hello" || first.GetHost() != "trusted-host" || first.GetSource() != "trusted-source" || first.GetService() != "trusted-service" || first.GetIndexName() != "main" {
		t.Fatalf("configuration or trusted metadata corrupted: %v", first)
	}
	if first.GetEventTime().GetSeconds() != 1700000000 || first.GetEventTime().GetNanos() != 1 {
		t.Fatal("numeric timestamp lost nanosecond precision")
	}
	assertStringValue(t, fieldValue(first, "environment"), "production")
	second := nativeTextDecode(t, decoder, raw)
	first.Raw[0] = '!'
	first.Fields.Fields[0].Name = "mutated"
	third := nativeTextDecode(t, decoder, raw)
	if !proto.Equal(second, third) {
		t.Fatal("a returned event mutated the decoder or later result")
	}
	if first.GetEventId() != second.GetEventId() {
		t.Fatal("stable ID changed between identical decodes")
	}
}

func TestNativeTextLogfmtMalformed(t *testing.T) {
	decoder := nativeTextDecoder(t, "logfmt", nil)
	cases := []string{"", " \t", "bare", "=empty-key", "key=one key=two", `key="unterminated`, `key="bad\q"`, `key="value"suffix=1`, "key=first\nnext=second", "key=\x00", "bad\x01key=value", string([]byte{'k', '=', '\xff'}), `timestamp=0.0000000001`, `timestamp=253402300800`, `timestamp=-62135596801`, `timestamp=2023-02-29T12:00:00Z`, `timestamp=2024-01-01T00:00:00+25:00`, `message=123`, `level=true`, strings.Repeat("k", 257) + "=value"}
	for i, raw := range cases {
		t.Run(fmt.Sprintf("case_%02d", i), func(t *testing.T) {
			if _, err := decoder.Decode([]byte(raw), SourcePosition{}, nativeTextNow); err == nil {
				t.Fatalf("accepted malformed record %q", raw)
			}
		})
	}
}

func TestNativeTextLimitsCountReservedAndConstants(t *testing.T) {
	for _, raw := range []string{"host=a source=b index=c", "a=1 b=2 c=3"} {
		cfg := nativeTextConfig("logfmt", nil)
		cfg.MaxJSONFields = 2
		decoder, err := NewDecoder(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = decoder.Decode([]byte(raw), SourcePosition{}, nativeTextNow); err == nil {
			t.Fatalf("field limit bypassed: %q", raw)
		}
	}
	cfg := nativeTextConfig("logfmt", nil)
	cfg.MaxJSONFields = 2
	cfg.ConstantFields = &opensplunk.TypedObject{Fields: []*opensplunk.TypedObjectField{{Name: "static", Value: stringValue("yes")}}}
	decoder, err := NewDecoder(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = decoder.Decode([]byte("a=1 b=2"), SourcePosition{}, nativeTextNow); err == nil {
		t.Fatal("merged output bypasses field bound")
	}
	nativeTextDecode(t, decoder, "a=1 static=override")
	cfg = nativeTextConfig("logfmt", nil)
	cfg.MaxLineBytes = 12
	decoder, err = NewDecoder(cfg)
	if err != nil {
		t.Fatal(err)
	}
	nativeTextDecode(t, decoder, "a=1234567890")
	if _, err = decoder.Decode([]byte("a=12345678901"), SourcePosition{}, nativeTextNow); err == nil {
		t.Fatal("event byte limit bypassed")
	}
}

// These are emitted-text patterns, not Java conversion programs. Padding and
// exception placement follow https://logging.apache.org/log4j/2.x/manual/pattern-layout.html
// and https://logback.qos.ch/manual/layouts.html .
func TestNativeTextJavaPatternProjection(t *testing.T) {
	for _, format := range []string{"log4j2-pattern", "logback-pattern"} {
		t.Run(format, func(t *testing.T) {
			options := &parserconfig.Options{Pattern: "%{timestamp} [%{thread}] %{level} %{logger} - %% %{message}", TimestampLayout: "2006-01-02 15:04:05,000", Timezone: "+05:45"}
			decoder := nativeTextDecoder(t, format, options)
			raw := "2024-02-29 23:59:59,123 [worker-1] WARN  checkout.Service - % failed - detail\njava.lang.IllegalStateException: boom\n\tat checkout.Service.run(Service.java:42)\n\nCaused by: java.io.IOException: disk"
			event := nativeTextDecode(t, decoder, raw)
			want := "failed - detail\njava.lang.IllegalStateException: boom\n\tat checkout.Service.run(Service.java:42)\n\nCaused by: java.io.IOException: disk"
			if event.GetMessage() != want || event.GetSeverity() != opensplunk.LogSeverity_LOG_SEVERITY_WARN {
				t.Fatalf("Java message or padded level incorrect: %v", event)
			}
			assertStringValue(t, fieldValue(event, "thread"), "worker-1")
			assertStringValue(t, fieldValue(event, "logger"), "checkout.Service")
			if !event.GetEventTime().AsTime().Equal(time.Date(2024, 2, 29, 18, 14, 59, 123000000, time.UTC)) {
				t.Fatalf("timestamp = %s", event.GetEventTime().AsTime())
			}
			tabbed := strings.Replace(raw, "] WARN  checkout", "]\tWARN\tcheckout", 1)
			if nativeTextDecode(t, decoder, tabbed).GetMessage() != want {
				t.Fatal("horizontal whitespace matching changed message")
			}
			for _, malformed := range []string{"", strings.Replace(raw, " - % ", " -- % ", 1), strings.Replace(raw, "2024-02-29", "2023-02-29", 1), "2024-02-29 23:59:59,123 [worker-1] WARN checkout.Service -", "2024-02-29 23:59:59,123 [worker-1\n] WARN checkout.Service - % message"} {
				if _, err := decoder.Decode([]byte(malformed), SourcePosition{}, nativeTextNow); err == nil {
					t.Fatalf("accepted malformed Java record %q", malformed)
				}
			}
		})
	}
}

func TestNativeTextJavaTimezoneGapFoldAndBounds(t *testing.T) {
	for _, format := range []string{"log4j2-pattern", "logback-pattern"} {
		t.Run(format, func(t *testing.T) {
			decoder := nativeTextDecoder(t, format, &parserconfig.Options{Pattern: "%{timestamp}|%{message}", TimestampLayout: "2006-01-02 15:04:05", Timezone: "America/New_York"})
			for _, raw := range []string{"2024-03-10 02:30:00|gap", "2024-11-03 01:30:00|fold", "2024-02-30 12:00:00|invalid", "0000-01-01 00:00:00|bounds"} {
				if _, err := decoder.Decode([]byte(raw), SourcePosition{}, nativeTextNow); err == nil {
					t.Fatalf("accepted invalid or ambiguous wall time %q", raw)
				}
			}
			for raw, want := range map[string]time.Time{"2024-03-10 03:30:00|after": time.Date(2024, 3, 10, 7, 30, 0, 0, time.UTC), "2024-11-03 02:30:00|after": time.Date(2024, 11, 3, 7, 30, 0, 0, time.UTC)} {
				if got := nativeTextDecode(t, decoder, raw).GetEventTime().AsTime(); !got.Equal(want) {
					t.Fatalf("time=%s want %s", got, want)
				}
			}
			offset := nativeTextDecoder(t, format, &parserconfig.Options{Pattern: "%{timestamp}|%{message}", TimestampLayout: time.RFC3339Nano})
			if got := nativeTextDecode(t, offset, "2024-11-03T01:30:00.000000007-04:00|explicit").GetEventTime().AsTime(); !got.Equal(time.Date(2024, 11, 3, 5, 30, 0, 7, time.UTC)) {
				t.Fatalf("explicit offset lost: %s", got)
			}
		})
	}
}

func TestNativeTextConfigurationRejectsAmbiguity(t *testing.T) {
	cases := []struct {
		format  string
		options *parserconfig.Options
	}{
		{"log4j2-pattern", nil}, {"logback-pattern", &parserconfig.Options{Pattern: "%{message}suffix"}},
		{"log4j2-pattern", &parserconfig.Options{Pattern: "%{level}%{message}"}},
		{"log4j2-pattern", &parserconfig.Options{Pattern: "%{level} %{level} %{message}"}},
		{"log4j2-pattern", &parserconfig.Options{Pattern: "%{timestamp} %{message}"}},
		{"log4j2-pattern", &parserconfig.Options{Pattern: "%{timestamp} %{message}", TimestampLayout: "15:04:05", Timezone: "UTC"}},
		{"log4j2-pattern", &parserconfig.Options{Pattern: "%{timestamp}|%{message}", TimestampLayout: "2006-01-02 15:04:05"}},
		{"log4j2-pattern", &parserconfig.Options{Pattern: "%{timestamp}|%{message}", TimestampLayout: "2006-01-02 15:04:05 MST", Timezone: "UTC"}},
		{"log4j2-pattern", &parserconfig.Options{Pattern: "%{timestamp}|%{message}", TimestampLayout: "2006-01-02 15:04:05", Timezone: "Mars/Olympus"}},
		{"log4j2-pattern", &parserconfig.Options{Pattern: "%{message}", Fields: map[string]string{"message": "msg"}}},
		{"logfmt", &parserconfig.Options{Pattern: "%{message}"}},
		{"logfmt", &parserconfig.Options{Fields: map[string]string{"not_a_role": "msg"}}},
		{"logfmt", &parserconfig.Options{Fields: map[string]string{"message": ""}}},
		{"logfmt", &parserconfig.Options{Fields: map[string]string{"message": "same", "level": "same"}}},
		{"logfmt", &parserconfig.Options{Fields: map[string]string{"message": "bad name"}}},
	}
	for i, tc := range cases {
		t.Run(fmt.Sprintf("case_%02d", i), func(t *testing.T) {
			if _, err := NewDecoder(nativeTextConfig(tc.format, tc.options)); err == nil {
				t.Fatal("accepted invalid parser configuration")
			}
		})
	}
}

func TestNativeTextErrorsDoNotLeakValues(t *testing.T) {
	const secret = "SECRET-DO-NOT-LOG-7aa82"
	for _, tc := range []struct {
		format, raw string
		options     *parserconfig.Options
	}{
		{"logfmt", "timestamp=" + secret, nil},
		{"logfmt", `field="` + secret + `\q"`, nil},
		{"log4j2-pattern", secret + "|message", &parserconfig.Options{Pattern: "%{timestamp}|%{message}", TimestampLayout: "2006-01-02", Timezone: "UTC"}},
	} {
		t.Run(tc.format+tc.raw[:1], func(t *testing.T) {
			decoder := nativeTextDecoder(t, tc.format, tc.options)
			_, err := decoder.Decode([]byte(tc.raw), SourcePosition{}, nativeTextNow)
			if err == nil {
				t.Fatal("expected decode error")
			}
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("error leaks payload: %v", err)
			}
		})
	}
}

func FuzzNativeTextLogfmt(f *testing.F) {
	for _, seed := range []string{`message="hello\nworld" n=18446744073709551616`, `a=1 a=2`, `message=`, `timestamp=0.000000001`, `a="\ud800"`, strings.Repeat("a", 257) + "=1"} {
		f.Add([]byte(seed))
	}
	decoder := nativeTextDecoder(f, "logfmt", nil)
	f.Fuzz(func(t *testing.T, raw []byte) {
		event, err := decoder.Decode(raw, SourcePosition{}, nativeTextNow)
		if err != nil {
			return
		}
		if !bytes.Equal(event.GetRaw(), raw) {
			t.Fatal("raw changed")
		}
		second, err := decoder.Decode(raw, SourcePosition{}, nativeTextNow)
		if err != nil || !proto.Equal(event, second) {
			t.Fatal("nondeterministic decoding")
		}
		if _, err := proto.Marshal(event); err != nil {
			t.Fatalf("accepted unserializable event: %v", err)
		}
	})
}

func FuzzNativeTextJava(f *testing.F) {
	for _, seed := range []string{"INFO [main] - hello", "WARN  [main] - failure\n\tat a.run(A.java:1)", "INFO [misleading] bracket] - hello", "INFO [missing"} {
		f.Add([]byte(seed))
	}
	decoder := nativeTextDecoder(f, "log4j2-pattern", &parserconfig.Options{Pattern: "%{level} [%{thread}] - %{message}"})
	f.Fuzz(func(t *testing.T, raw []byte) {
		event, err := decoder.Decode(raw, SourcePosition{}, nativeTextNow)
		if err != nil {
			return
		}
		if !bytes.Equal(event.GetRaw(), raw) {
			t.Fatal("raw changed")
		}
		if _, err := proto.Marshal(event); err != nil {
			t.Fatalf("accepted unserializable event: %v", err)
		}
	})
}
