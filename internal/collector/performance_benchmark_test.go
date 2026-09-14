package collector

import (
	"fmt"
	"strings"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/collector/parserconfig"
)

// Keep this harness identical in both production snapshots. These cases cover
// allocation tradeoffs that small canonical fixtures do not expose.
func BenchmarkCollectorAllocationCases(b *testing.B) {
	for _, fixture := range []struct {
		name, format, raw string
		constants         bool
	}{
		{"logfmt_plain_large", "logfmt", `message="` + strings.Repeat("a", 64<<10) + `" tag=plain`, false},
		{"logfmt_escaped_large", "logfmt", `message="` + strings.Repeat(`a\n`, 16<<10) + `" tag=escaped`, false},
		{"logfmt_numeric_lookalikes", "logfmt", `message=done date=2026-09-08 version=1.2.3 leading=01 exponent=1e- path=123/abc`, false},
		{"ndjson_constants", "ndjson", `{"message":"done","region":"untrusted","status":200}`, true},
		{"logfmt_constants", "logfmt", `message=done region=untrusted status=200`, true},
		{"raw_large", "raw", strings.Repeat("original bytes ", 4096), false},
		{"access_plain", "nginx-combined", `127.0.0.1 - alice [07/Sep/2026:12:34:56 +0000] "GET /api/events HTTP/1.1" 200 1234 "-" "agent"`, false},
		{"access_binary", "nginx-combined", `127.0.0.1 - alice [07/Sep/2026:12:34:56 +0000] "GET /api/\xff HTTP/1.1" 200 1234 "-" "agent\xff"`, false},
	} {
		b.Run(fixture.name, func(b *testing.B) {
			cfg := DecodeConfig{Format: InputFormat(fixture.format), InputID: "allocation-bench", IndexName: "main", Source: "bench.log", Sourcetype: fixture.format, Host: "bench"}
			if fixture.constants {
				cfg.ConstantFields = &opensplunk.TypedObject{Fields: []*opensplunk.TypedObjectField{
					{Name: "region", Value: &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_StringValue{StringValue: "trusted"}}},
					{Name: "environment", Value: &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_StringValue{StringValue: "production"}}},
				}}
			}
			decoder, err := NewDecoder(cfg)
			if err != nil {
				b.Fatal(err)
			}
			raw := []byte(fixture.raw)
			position := SourcePosition{FileIdentity: "benchmark", EndOffset: uint64(len(raw))}
			now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			b.ResetTimer()
			for b.Loop() {
				event, err := decoder.Decode(raw, position, now)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkDecodedEventID = event.GetEventId()
			}
		})
	}
}

func BenchmarkCollectorPatternAllocations(b *testing.B) {
	for _, fixture := range []struct{ fields, bytes int }{{1, 32}, {5, 32}, {32, 32}, {1024, 32}, {5, 1 << 20}} {
		b.Run(fmt.Sprintf("fields_%d/bytes_%d", fixture.fields, fixture.bytes), func(b *testing.B) {
			var pattern, record strings.Builder
			for i := 0; i < fixture.fields-1; i++ {
				fmt.Fprintf(&pattern, "%%{field%d}|", i)
				record.WriteString("value|")
			}
			pattern.WriteString("%{message}")
			message := strings.Repeat("m", max(0, fixture.bytes-record.Len()))
			record.WriteString(message)
			decoder, err := NewDecoder(DecodeConfig{Format: "logback-pattern", InputID: "pattern-bench", IndexName: "main", Source: "bench.log", Sourcetype: "logback-pattern", Host: "bench", Parser: &parserconfig.Options{Pattern: pattern.String()}})
			if err != nil {
				b.Fatal(err)
			}
			raw := []byte(record.String())
			position := SourcePosition{FileIdentity: "benchmark", EndOffset: uint64(len(raw))}
			now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			b.ResetTimer()
			for b.Loop() {
				event, err := decoder.Decode(raw, position, now)
				if err != nil || event.GetMessage() != message {
					b.Fatalf("unexpected pattern result: %v", err)
				}
				benchmarkDecodedEventID = event.GetEventId()
			}
		})
	}
}
