package parserconfig

import (
	"fmt"
	"strings"
	"testing"
)

func TestPerfReviewAstraCaptureIntoErrorsAndLargeFallback(t *testing.T) {
	c, err := Compile("logback-pattern", &Options{Pattern: "PREFIX %{one}|%{two}|%{message}"})
	if err != nil {
		t.Fatal(err)
	}
	var local [8]Capture
	for _, tc := range []struct{ raw, want string }{
		{strings.Repeat("x", maxEventBytes) + "\xff", "record is not valid UTF-8"},
		{strings.Repeat("x", maxEventBytes+1), "record exceeds parser byte limit"},
		{"wrong", "record does not match pattern prefix"},
		{"PREFIX one|two", "record is missing pattern delimiter"},
		{"PREFIX one\n|two|message", "header capture contains a line break"},
	} {
		got, err := c.CaptureInto([]byte(tc.raw), local[:0])
		if got != nil || err == nil || err.Error() != tc.want {
			t.Fatalf("got %v / %v; want %s", got, err, tc.want)
		}
	}
	var pattern, raw strings.Builder
	for i := 0; i < 1023; i++ {
		fmt.Fprintf(&pattern, "%%{f%d}|", i)
		raw.WriteString("value|")
	}
	pattern.WriteString("%{message}")
	raw.WriteString("message")
	c, err = Compile("logback-pattern", &Options{Pattern: pattern.String()})
	if err != nil {
		t.Fatal(err)
	}
	local[0] = Capture{Name: "sentinel", Value: "untouched"}
	input := []byte(raw.String())
	got, err := c.CaptureInto(input, local[:1])
	if err != nil || len(got) != 1024 {
		t.Fatalf("large fallback len %d: %v", len(got), err)
	}
	if local[0].Name != "sentinel" {
		t.Fatal("insufficient destination capacity was partially overwritten")
	}
	for i := range input {
		input[i] = 'x'
	}
	for i, capture := range got {
		want := "value"
		if i == len(got)-1 {
			want = "message"
		}
		if capture.Value != want {
			t.Fatalf("capture %d aliases input", i)
		}
	}
}
