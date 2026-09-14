package collector

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/collector/parserconfig"
)

type nativeBenchmarkFixture struct {
	format  string
	options *parserconfig.Options
	raw     string
	message string
}

func nativeBenchmarkFixtures() []nativeBenchmarkFixture {
	java := &parserconfig.Options{Pattern: "%{timestamp} [%{thread}] %{level} %{logger} - %{message}", TimestampLayout: "2006-01-02 15:04:05.000", Timezone: "UTC"}
	return []nativeBenchmarkFixture{
		{"docker-json-file", nil, `{"log":"request complete\n","stream":"stdout","time":"2026-09-07T12:34:56.123456789Z","attrs":{"service":"web"}}`, "request complete\n"},
		{"nginx-combined", nil, `2001:db8::1 - alice [07/Sep/2026:12:34:56 +0000] "GET /api/events HTTP/1.1" 200 1234 "-" "agent\x22v1"`, "GET /api/events HTTP/1.1"},
		{"apache-common", nil, `example.test - alice [07/Sep/2026:12:34:56 +0000] "GET /api/events HTTP/1.1" 200 1234`, "GET /api/events HTTP/1.1"},
		{"apache-combined", nil, `127.0.0.1 - alice [07/Sep/2026:12:34:56 +0000] "GET /api/events HTTP/1.1" 200 1234 "-" "agent\"v1"`, "GET /api/events HTTP/1.1"},
		{"logfmt", nil, `timestamp=2026-09-07T12:34:56.123456789Z message="request complete" level=info status=200 duration=0.125 ok=true`, "request complete"},
		{"log4j2-pattern", java, "2026-09-07 12:34:56.123 [main] INFO web.Handler - request complete\n\tat web.Handler.run(Handler.java:42)", "request complete\n\tat web.Handler.run(Handler.java:42)"},
		{"logback-pattern", java, "2026-09-07 12:34:56.123 [main] WARN web.Handler - request complete", "request complete"},
	}
}

func nativeBenchmarkConfig(f nativeBenchmarkFixture) DecodeConfig {
	return DecodeConfig{Format: InputFormat(f.format), Parser: f.options, InputID: "native-bench", IndexName: "main", Source: "bench.log", Sourcetype: f.format, Host: "bench"}
}

func BenchmarkNativeDecoder(b *testing.B) {
	for _, fixture := range nativeBenchmarkFixtures() {
		b.Run(fixture.format, func(b *testing.B) {
			decoder, err := NewDecoder(nativeBenchmarkConfig(fixture))
			if err != nil {
				b.Fatal(err)
			}
			raw := []byte(fixture.raw)
			position := SourcePosition{FileIdentity: "benchmark", EndOffset: uint64(len(raw))}
			now := time.Date(2026, 9, 7, 12, 35, 0, 0, time.UTC)
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			b.ResetTimer()
			for b.Loop() {
				event, err := decoder.Decode(raw, position, now)
				if err != nil {
					b.Fatal(err)
				}
				if event.GetMessage() != fixture.message {
					b.Fatalf("message = %q, want %q", event.GetMessage(), fixture.message)
				}
				benchmarkDecodedEventID = event.GetEventId()
			}
		})
	}
}

func BenchmarkNativeJavaCompile(b *testing.B) {
	for _, fixture := range nativeBenchmarkFixtures()[5:] {
		b.Run(fixture.format, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				compiled, err := parserconfig.Compile(fixture.format, fixture.options)
				if err != nil {
					b.Fatal(err)
				}
				if compiled == nil {
					b.Fatal("nil compiled parser")
				}
			}
		})
	}
}

// Invalid inputs consume progressively longer prefixes before failing. Every
// fixture stays inside the framing ceiling, so these measure parser work rather
// than the constant-time event-size guard.
func BenchmarkNativeMalformed(b *testing.B) {
	for _, size := range []int{1 << 10, 16 << 10, 256 << 10} {
		padding := strings.Repeat("a", size)
		fixtures := []nativeBenchmarkFixture{
			{format: "docker-json-file", raw: `{"time":"2026-09-07T12:34:56Z","stream":"stdout","log":"` + padding},
			{format: "nginx-combined", raw: `127.0.0.1 - - [07/Sep/2026:12:34:56 +0000] "GET / HTTP/1.1" 200 1 "-" "` + padding},
			{format: "apache-common", raw: `127.0.0.1 - - [07/Sep/2026:12:34:56 +0000] "` + padding},
			{format: "apache-combined", raw: `127.0.0.1 - - [07/Sep/2026:12:34:56 +0000] "GET / HTTP/1.1" 200 1 "-" "` + padding},
			{format: "logfmt", raw: `message="` + padding},
			{format: "log4j2-pattern", options: &parserconfig.Options{Pattern: "%{logger}aaaaaaaaab%{message}"}, raw: padding},
			{format: "logback-pattern", options: &parserconfig.Options{Pattern: "%{logger}aaaaaaaaab%{message}"}, raw: padding},
		}
		for _, fixture := range fixtures {
			b.Run(fmt.Sprintf("%s/%d", fixture.format, size), func(b *testing.B) {
				decoder, err := NewDecoder(nativeBenchmarkConfig(fixture))
				if err != nil {
					b.Fatal(err)
				}
				raw := []byte(fixture.raw)
				position := SourcePosition{FileIdentity: "benchmark", EndOffset: uint64(len(raw))}
				now := time.Date(2026, 9, 7, 12, 35, 0, 0, time.UTC)
				b.ReportAllocs()
				b.SetBytes(int64(len(raw)))
				b.ResetTimer()
				for b.Loop() {
					if _, err := decoder.Decode(raw, position, now); err == nil {
						b.Fatal("malformed fixture accepted")
					}
				}
			})
		}
	}
}

// Both cohorts process two events per operation. NDJSON's direct decoding path
// is exercised in exactly the same first slot; the second input changes format.
// Use profiles alongside this comparison: overall mixed throughput is not a
// measurement of NDJSON-only latency under external CPU contention.
func BenchmarkNativeMixedInputs(b *testing.B) {
	ndjson := nativeBenchmarkFixture{format: "ndjson", raw: `{"message":"request complete","level":"info","status":200}`}
	for _, mixed := range []bool{false, true} {
		name := "ndjson-only"
		second := ndjson
		if mixed {
			name = "ndjson-and-java"
			second = nativeBenchmarkFixtures()[5]
		}
		b.Run(name, func(b *testing.B) {
			inputs := []nativeBenchmarkFixture{ndjson, second}
			decoders := make([]*Decoder, len(inputs))
			payloads := make([][]byte, len(inputs))
			var bytesPerOperation int64
			for i, input := range inputs {
				var err error
				decoders[i], err = NewDecoder(nativeBenchmarkConfig(input))
				if err != nil {
					b.Fatal(err)
				}
				payloads[i] = []byte(input.raw)
				bytesPerOperation += int64(len(payloads[i]))
			}
			now := time.Date(2026, 9, 7, 12, 35, 0, 0, time.UTC)
			b.ReportAllocs()
			b.SetBytes(bytesPerOperation)
			b.ResetTimer()
			for b.Loop() {
				for i, decoder := range decoders {
					event, err := decoder.Decode(payloads[i], SourcePosition{FileIdentity: "benchmark", EndOffset: uint64(len(payloads[i]))}, now)
					if err != nil {
						b.Fatal(err)
					}
					benchmarkDecodedEventID = event.GetEventId()
				}
			}
		})
	}
}
