package collector

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

var benchmarkDecodedEventID string

// Keep this corpus and harness identical when measuring the baseline and PR.
func BenchmarkNDJSONDecoder(b *testing.B) {
	var wide strings.Builder
	wide.WriteString(`{"message":"wide"`)
	for i := range 900 {
		fmt.Fprintf(&wide, `,"field_%d":%d`, i, i)
	}
	wide.WriteByte('}')
	for _, fixture := range []struct{ name, raw string }{
		{"canonical", `{"timestamp":"2026-09-07T12:34:56.123456789Z","message":"request complete","level":"info","status":200}`},
		{"nested", `{"message":"nested","context":{"request":{"path":"/api/events","attempt":2},"tags":["one","two"],"ok":true},"duration":0.125}`},
		{"high_fields", wide.String()},
	} {
		b.Run(fixture.name, func(b *testing.B) {
			decoder, err := NewDecoder(DecodeConfig{Format: InputFormatNDJSON, InputID: "bench", IndexName: "main", Source: "bench.log", Sourcetype: "ndjson", Host: "bench"})
			if err != nil {
				b.Fatal(err)
			}
			raw := []byte(fixture.raw)
			now := time.Date(2026, 9, 7, 12, 35, 0, 0, time.UTC)
			position := SourcePosition{FileIdentity: "benchmark", EndOffset: uint64(len(raw))}
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
