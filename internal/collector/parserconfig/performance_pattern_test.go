package parserconfig

import (
	"fmt"
	"strings"
	"testing"
)

func TestPerformancePatternCaptureOwnershipAndCounts(t *testing.T) {
	for _, count := range []int{1, 5, 9, 1024} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			var pattern, record strings.Builder
			for i := 0; i < count-1; i++ {
				fmt.Fprintf(&pattern, "%%{field%d}|", i)
				record.WriteString("value|")
			}
			pattern.WriteString("%{message}")
			record.WriteString("message\n\tcontinuation")
			compiled, err := Compile("logback-pattern", &Options{Pattern: pattern.String()})
			if err != nil {
				t.Fatal(err)
			}
			raw := []byte(record.String())
			captures, err := compiled.Capture(raw)
			if err != nil || len(captures) != count {
				t.Fatalf("captures=%d error=%v", len(captures), err)
			}
			for i := range raw {
				raw[i] = 'x'
			}
			for i, capture := range captures {
				name, value := fmt.Sprintf("field%d", i), "value"
				if i == count-1 {
					name, value = "message", "message\n\tcontinuation"
				}
				if capture.Name != name || capture.Value != value {
					t.Fatalf("capture %d=%#v, want %q=%q", i, capture, name, value)
				}
			}
		})
	}
}

func TestPerformancePatternCaptureIntoStorage(t *testing.T) {
	compiled, err := Compile("log4j2-pattern", &Options{Pattern: "%{thread}|%{message}"})
	if err != nil {
		t.Fatal(err)
	}
	var local [8]Capture
	local[0] = Capture{Name: "old", Value: "old"}
	raw := []byte("main|first\nline")
	captures, err := compiled.CaptureInto(raw, local[:1])
	if err != nil || len(captures) != 2 || &captures[0] != &local[0] {
		t.Fatalf("destination was not reused: len=%d err=%v", len(captures), err)
	}
	message := captures[1].Value
	for i := range raw {
		raw[i] = 'x'
	}
	if message != "first\nline" || captures[0] != (Capture{Name: "thread", Value: "main"}) {
		t.Fatal("captured strings alias input")
	}
	captures, err = compiled.CaptureInto([]byte("worker|second"), captures)
	if err != nil || len(captures) != 2 || captures[1] != (Capture{Name: "message", Value: "second"}) || message != "first\nline" {
		t.Fatalf("destination reuse changed retained strings: captures=%v err=%v", captures, err)
	}
	if result, err := compiled.CaptureInto([]byte("missing delimiter"), local[:0]); err == nil || result != nil {
		t.Fatalf("malformed result=%v err=%v", result, err)
	}
	result, err := compiled.CaptureInto([]byte("tiny|allocated"), make([]Capture, 0, 1))
	if err != nil || len(result) != 2 || result[1].Value != "allocated" {
		t.Fatalf("small destination result=%v err=%v", result, err)
	}
}
