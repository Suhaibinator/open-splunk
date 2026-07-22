package loggen

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestGeneratorIsDeterministicAndProducesExactLineCount(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Format:      FormatMixed,
		Seed:        42,
		Start:       time.Date(2026, time.June, 29, 19, 9, 12, 0, time.UTC),
		Interval:    250 * time.Millisecond,
		Service:     "gradethis",
		Environment: "test",
		Host:        "fixture-host",
		Cardinality: 7,
	}

	var first bytes.Buffer
	if err := Generate(context.Background(), &first, cfg, 12); err != nil {
		t.Fatalf("Generate(first): %v", err)
	}
	var second bytes.Buffer
	if err := Generate(context.Background(), &second, cfg, 12); err != nil {
		t.Fatalf("Generate(second): %v", err)
	}

	if got, want := first.String(), second.String(); got != want {
		t.Fatal("same configuration did not produce byte-identical output")
	}
	if got := strings.Count(first.String(), "\n"); got != 12 {
		t.Fatalf("newline count = %d, want 12", got)
	}
}

func TestGeneratorFormatsExerciseFixtureShapes(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, time.January, 2, 3, 4, 5, 6, time.UTC)
	tests := []struct {
		name   string
		format Format
		check  func(*testing.T, []byte)
	}{
		{
			name:   "zap JSON",
			format: FormatZapJSON,
			check: func(t *testing.T, line []byte) {
				t.Helper()
				var event map[string]any
				if err := json.Unmarshal(line, &event); err != nil {
					t.Fatalf("decode: %v", err)
				}
				if event["timestamp"] != "2026-01-02T03:04:05.000000006Z" {
					t.Fatalf("timestamp = %#v", event["timestamp"])
				}
				if _, ok := event["trace_id"].(string); !ok {
					t.Fatalf("trace_id = %#v", event["trace_id"])
				}
				if _, ok := event["duration_ms"].(float64); !ok {
					t.Fatalf("duration_ms = %#v", event["duration_ms"])
				}
			},
		},
		{
			name:   "nested JSON",
			format: FormatNestedJSON,
			check: func(t *testing.T, line []byte) {
				t.Helper()
				var event map[string]any
				if err := json.Unmarshal(line, &event); err != nil {
					t.Fatalf("decode: %v", err)
				}
				httpValue, ok := event["http"].(map[string]any)
				if !ok || httpValue["request"] == nil || httpValue["response"] == nil {
					t.Fatalf("nested http object = %#v", event["http"])
				}
				if _, ok := event["tags"].([]any); !ok {
					t.Fatalf("tags = %#v", event["tags"])
				}
			},
		},
		{
			name:   "raw",
			format: FormatRaw,
			check: func(t *testing.T, line []byte) {
				t.Helper()
				if !bytes.Contains(line, []byte("service=gradethis")) || !bytes.Contains(line, []byte("request_id=")) {
					t.Fatalf("raw line = %q", line)
				}
			},
		},
		{
			name:   "high cardinality JSON",
			format: FormatCardinalityJSON,
			check: func(t *testing.T, line []byte) {
				t.Helper()
				var event map[string]any
				if err := json.Unmarshal(line, &event); err != nil {
					t.Fatalf("decode: %v", err)
				}
				if event["request_id"] == nil || event["session_id"] == nil || event["user_id"] == nil {
					t.Fatalf("cardinality fields missing: %#v", event)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			generator, err := New(Config{
				Format:      tt.format,
				Seed:        9,
				Start:       start,
				Interval:    time.Second,
				Service:     "gradethis",
				Environment: "test",
				Host:        "fixture-host",
				Cardinality: 100,
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			line, err := generator.Next()
			if err != nil {
				t.Fatalf("Next: %v", err)
			}
			tt.check(t, line)
		})
	}
}

func TestMixedFormatCyclesAllFixtureShapes(t *testing.T) {
	t.Parallel()

	generator, err := New(DefaultConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	for i, want := range []Format{FormatZapJSON, FormatNestedJSON, FormatRaw, FormatCardinalityJSON} {
		line, err := generator.Next()
		if err != nil {
			t.Fatalf("Next(%d): %v", i, err)
		}
		if got := DetectFormat(line); got != want {
			t.Fatalf("line %d format = %q, want %q; line=%q", i, got, want, line)
		}
	}
}

func TestNewRejectsUnsafeOrInvalidConfiguration(t *testing.T) {
	t.Parallel()

	tests := []Config{
		{Format: "unknown", Start: time.Now(), Interval: time.Second, Service: "svc", Environment: "test", Host: "host", Cardinality: 1},
		{Format: FormatRaw, Start: time.Now(), Interval: -time.Second, Service: "svc", Environment: "test", Host: "host", Cardinality: 1},
		{Format: FormatRaw, Start: time.Now(), Interval: time.Second, Service: "bad\nline", Environment: "test", Host: "host", Cardinality: 1},
		{Format: FormatRaw, Start: time.Now(), Interval: time.Second, Service: "svc", Environment: "test", Host: "host", Cardinality: 0},
	}

	for i, cfg := range tests {
		if _, err := New(cfg); err == nil {
			t.Fatalf("New(invalid config %d) unexpectedly succeeded", i)
		}
	}
}

func TestGenerateHonorsCancellationAndWriterErrors(t *testing.T) {
	t.Parallel()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Generate(canceled, &bytes.Buffer{}, DefaultConfig(), 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Generate error = %v, want context.Canceled", err)
	}

	wantErr := errors.New("disk full")
	if err := Generate(context.Background(), errorWriter{err: wantErr}, DefaultConfig(), 1); !errors.Is(err, wantErr) {
		t.Fatalf("writer error = %v, want %v", err, wantErr)
	}
}

type errorWriter struct{ err error }

func (w errorWriter) Write([]byte) (int, error) { return 0, w.err }
