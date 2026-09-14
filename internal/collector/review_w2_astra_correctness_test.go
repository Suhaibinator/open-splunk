package collector

import (
	"fmt"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/collector/parserconfig"
)

func TestReviewW2AstraCorrectnessOffsetBeforeVariableTimestampSuffix(t *testing.T) {
	const layout = "2006-01-02 -07:00 15:04:05.999999999"
	const stamp = "2026-09-08 +00:00 12:34:56.100"
	want := time.Date(2026, 9, 8, 12, 34, 56, 100000000, time.UTC)
	parsed, err := time.Parse(layout, stamp)
	if err != nil || !parsed.Equal(want) {
		t.Fatalf("invalid regression fixture: parsed=%v err=%v", parsed, err)
	}
	for _, format := range []string{"logfmt", "log4j2-pattern", "logback-pattern"} {
		t.Run(format, func(t *testing.T) {
			options := &parserconfig.Options{TimestampLayout: layout}
			raw := fmt.Sprintf("timestamp=%q message=hello", stamp)
			if format != "logfmt" {
				options.Pattern = "%{timestamp}|%{message}"
				raw = stamp + "|hello"
			}
			decoder := nativeTextDecoder(t, format, options)
			event, err := decoder.Decode([]byte(raw), SourcePosition{FileIdentity: "review-w2", EndOffset: uint64(len(raw))}, nativeTextNow)
			if err != nil {
				t.Fatalf("valid configured Go timestamp rejected: %v", err)
			}
			if !event.GetEventTime().AsTime().Equal(want) {
				t.Fatalf("event timestamp=%v want=%v", event.GetEventTime().AsTime(), want)
			}
		})
	}
}
