package parserconfig

import (
	"strings"
	"testing"
	"time"
)

func TestCompileRejectsInvalidOptions(t *testing.T) {
	tests := []struct {
		format  string
		options *Options
	}{
		{"ndjson", &Options{}}, {"raw", &Options{}}, {"docker-json-file", &Options{}},
		{"nginx-combined", &Options{Pattern: "x"}}, {"unknown", nil},
		{"logfmt", &Options{Pattern: "%{message}"}},
		{"logfmt", &Options{Fields: map[string]string{"bogus": "x"}}},
		{"logfmt", &Options{Fields: map[string]string{"level": "message"}}},
		{"logfmt", &Options{Fields: map[string]string{"message": "tenant_id"}}},
		{"logfmt", &Options{Timezone: "UTC"}},
		{"log4j2-pattern", nil},
		{"log4j2-pattern", &Options{Pattern: "%{level}%{message}"}},
		{"log4j2-pattern", &Options{Pattern: "%{level} %{level} %{message}"}},
		{"log4j2-pattern", &Options{Pattern: "%{tenant_id} %{message}"}},
		{"log4j2-pattern", &Options{Pattern: "%{message} trailing"}},
		{"log4j2-pattern", &Options{Pattern: "%{timestamp} %{message}"}},
		{"logback-pattern", &Options{Pattern: "%{message}", TimestampLayout: time.RFC3339}},
		{"logfmt", &Options{TimestampLayout: "15:04:05", Timezone: "UTC"}},
		{"logfmt", &Options{TimestampLayout: "2006-01-02 15:04:05 MST"}},
		{"logfmt", &Options{TimestampLayout: "2006-01-02 15:04:05"}},
		{"logfmt", &Options{TimestampLayout: time.RFC3339, Timezone: "UTC"}},
		{"log4j2-pattern", &Options{Pattern: strings.Repeat("x", 16385) + "%{message}"}},
	}
	for _, tt := range tests {
		if _, err := Compile(tt.format, tt.options); err == nil {
			t.Errorf("accepted %s %+v", tt.format, tt.options)
		}
	}
}

func TestCaptureLiteralWhitespaceAndMultiline(t *testing.T) {
	c, err := Compile("log4j2-pattern", &Options{Pattern: "[%{timestamp}] %{level} [%{thread}] %% %{message}", TimestampLayout: time.RFC3339})
	if err != nil {
		t.Fatal(err)
	}
	raw := "[2024-02-29T23:59:58Z]\t INFO   [thread-1] % boom\n\n\tat app.run(App.java:5)"
	fields, err := c.Capture([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	want := []Capture{{"timestamp", "2024-02-29T23:59:58Z"}, {"level", "INFO"}, {"thread", "thread-1"}, {"message", "boom\n\n\tat app.run(App.java:5)"}}
	if len(fields) != len(want) {
		t.Fatalf("captures=%+v", fields)
	}
	for i := range want {
		if fields[i] != want[i] {
			t.Errorf("capture %d=%+v want %+v", i, fields[i], want[i])
		}
	}
	if _, err := c.Capture([]byte("[2024-02-29T23:59:58Z] INFO [thread-1] wrong")); err == nil {
		t.Fatal("accepted broken literal")
	}
}

func TestTimeZoneTransitionsAndBounds(t *testing.T) {
	c, err := Compile("logfmt", &Options{TimestampLayout: "2006-01-02 15:04:05", Timezone: "America/New_York"})
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"2024-03-10 02:30:00", "2024-11-03 01:30:00", "2023-02-29 12:00:00", "0000-01-01 00:00:00"} {
		if _, err := c.ParseTime(raw); err == nil {
			t.Errorf("accepted %q", raw)
		}
	}
	actual, err := c.ParseTime("2024-03-10 03:30:00")
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2024, 3, 10, 7, 30, 0, 0, time.UTC); !actual.Equal(want) {
		t.Fatalf("actual=%v want=%v", actual, want)
	}
	utc, err := Compile("logfmt", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"0000-01-01T00:00:00Z", "0001-01-01T00:00:00+01:00", "9999-12-31T23:59:59-01:00"} {
		if _, err := utc.ParseTime(raw); err == nil {
			t.Errorf("accepted out-of-range %q", raw)
		}
	}
	if _, err := utc.ParseTime("2024-02-29T23:59:59.123456789+05:45"); err != nil {
		t.Fatal(err)
	}
}

func TestFieldMapIsCopiedAndDefaultsAreComplete(t *testing.T) {
	options := &Options{Fields: map[string]string{"message": "msg"}}
	c, err := Compile("logfmt", options)
	if err != nil {
		t.Fatal(err)
	}
	options.Fields["message"] = "changed"
	if c.SourceField("message") != "msg" || c.SourceField("timestamp") != "timestamp" || c.CanonicalField("msg") != "message" || c.CanonicalField("changed") != "" {
		t.Fatal("mappings not immutable/defaulted")
	}
}

func TestCaptureRejectsHeaderNewlinesAndInvalidUTF8(t *testing.T) {
	c, err := Compile("logback-pattern", &Options{Pattern: "%{level} [%{thread}] %{message}"})
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"INFO [worker\n1] message", "IN\rFO [worker] message", "INFO [\xff] message"} {
		if _, err := c.Capture([]byte(raw)); err == nil {
			t.Errorf("accepted invalid header %q", raw)
		}
	}
}

func TestTimeMoreOffsetsAndHistoricalTransitions(t *testing.T) {
	cases := []struct {
		zone, input, want string
		bad               bool
	}{
		{"Australia/Lord_Howe", "2024-04-07 01:45:00", "", true},
		{"Australia/Lord_Howe", "2024-10-06 02:15:00", "", true},
		{"Pacific/Apia", "2011-12-30 12:00:00", "", true},
		{"America/New_York", "9999-12-31 12:00:00", "9999-12-31T17:00:00Z", false},
		{"America/New_York", "0001-01-02 12:00:00", "0001-01-02T16:56:02Z", false},
		{"+05:45", "2024-02-29 12:00:00", "2024-02-29T06:15:00Z", false},
	}
	for _, tt := range cases {
		t.Run(tt.zone+tt.input, func(t *testing.T) {
			c, err := Compile("logfmt", &Options{TimestampLayout: "2006-01-02 15:04:05", Timezone: tt.zone})
			if err != nil {
				t.Fatal(err)
			}
			got, err := c.ParseTime(tt.input)
			if tt.bad {
				if err == nil {
					t.Fatal("accepted invalid local timestamp")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.Format(time.RFC3339) != tt.want {
				t.Fatalf("got %s want %s", got.Format(time.RFC3339), tt.want)
			}
		})
	}
	c, err := Compile("logfmt", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"2024-02-29T12:00:00+24:00", "2024-02-29T12:00:00+01:60", "2024-02-29T12:00:00.1234567891Z"} {
		if _, err := c.ParseTime(raw); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	for _, zone := range []string{"++1:00", "+01:+1", "+01:-1", "+24:00", "+01:60", "Local", "PST"} {
		if _, err := Compile("logfmt", &Options{TimestampLayout: "2006-01-02", Timezone: zone}); err == nil {
			t.Errorf("accepted zone %s", zone)
		}
	}
}

func TestMixedLiteralOverlapsAndWhitespacePositions(t *testing.T) {
	for _, tt := range []struct {
		literal, input string
		start, end     int
	}{
		{" a a b ", "x a a a a\t b   tail", 5, 15},
		{"a b a b c", "a b a b a b a b c", 8, 17},
		{" x ", "prefix\t  x\t\tvalue", 6, 12},
		{" \uFFFD ", "x \xff y", -1, -1},
	} {
		actualStart, actualEnd := compileLiteral(tt.literal).find(tt.input)
		if actualStart != tt.start || actualEnd != tt.end {
			t.Errorf("%q in %q: %d,%d want %d,%d", tt.literal, tt.input, actualStart, actualEnd, tt.start, tt.end)
		}
	}
}

func TestCompileRejectsMultilineHeaders(t *testing.T) {
	for _, pattern := range []string{"prefix\n%{message}", "%{level}\r%{message}", "%{level}\x00%{message}"} {
		if _, err := Compile("log4j2-pattern", &Options{Pattern: pattern}); err == nil {
			t.Errorf("accepted non-line pattern %q", pattern)
		}
	}
}
