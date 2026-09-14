package collector

import (
	"bytes"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/collector/parserconfig"
)

func TestNativeTimestampLiteralPrecision(t *testing.T) {
	for _, format := range []InputFormat{"logfmt", "log4j2-pattern", "logback-pattern"} {
		t.Run(string(format), func(t *testing.T) {
			options := &parserconfig.Options{TimestampLayout: "2006-01-02T15:04:05Z07:00 .8888888888"}
			if format != "logfmt" {
				options.Pattern = "%{timestamp}|%{level}|%{message}"
			}
			decoder, err := NewDecoder(DecodeConfig{Format: format, Parser: options, InputID: "precision", IndexName: "main", Source: "precision.log", Sourcetype: string(format), Host: "trusted"})
			if err != nil {
				t.Fatal(err)
			}
			for _, fraction := range []string{"", ".123456789", ".1234567890"} {
				stamp := "2026-09-08T12:34:56" + fraction + "Z .8888888888"
				raw := []byte(stamp + "|INFO|hello")
				if format == "logfmt" {
					raw = []byte("timestamp=\"" + stamp + "\" level=INFO message=hello")
				}
				collected := time.Date(2026, 9, 8, 13, 0, 0, 0, time.UTC)
				event, decodeErr := decoder.Decode(raw, SourcePosition{FileIdentity: "precision", EndOffset: uint64(len(raw))}, collected)
				if len(fraction) > 10 {
					if decodeErr == nil {
						t.Fatal("accepted ten fractional digits")
					}
					continue
				}
				if decodeErr != nil {
					t.Fatalf("rejected literal digits: %v", decodeErr)
				}
				nanos := 0
				if fraction != "" {
					nanos = 123456789
				}
				want := time.Date(2026, 9, 8, 12, 34, 56, nanos, time.UTC)
				if !event.GetEventTime().AsTime().Equal(want) || event.GetMessage() != "hello" || !bytes.Equal(event.GetRaw(), raw) {
					t.Fatal("timestamp projection changed event time, message, or raw")
				}
			}
		})
	}
}
