package parserconfig

import (
	"strconv"
	"strings"
	"testing"
)

func BenchmarkPatternCompile(b *testing.B) {
	options := &Options{Pattern: "[%{timestamp}] %{level} [%{thread}] %{logger} - %{message}", TimestampLayout: "2006-01-02 15:04:05.000", Timezone: "UTC"}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Compile("log4j2-pattern", options); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPatternCapture(b *testing.B) {
	for _, test := range []struct{ name, pattern, input string }{
		{"exact", "[%{level}][%{thread}]%{message}", "[INFO][worker]ready"},
		{"whitespace", "%{level} %{thread} %{message}", "INFO worker ready"},
		{"mixed", "%{level} [%{thread}] - %{message}", "INFO [worker] - ready"},
	} {
		b.Run(test.name, func(b *testing.B) {
			c, err := Compile("logback-pattern", &Options{Pattern: test.pattern})
			if err != nil {
				b.Fatal(err)
			}
			raw := []byte(test.input)
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			b.ResetTimer()
			for b.Loop() {
				if _, err := c.Capture(raw); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkPatternAlmostMatching(b *testing.B) {
	c, err := Compile("logback-pattern", &Options{Pattern: "%{thread} a a a a a b %{message}"})
	if err != nil {
		b.Fatal(err)
	}
	for _, size := range []int{1024, 16384, 1 << 20} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			raw := []byte(strings.Repeat(" a", size/2))
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			b.ResetTimer()
			for b.Loop() {
				if _, err := c.Capture(raw); err == nil {
					b.Fatal("accepted missing delimiter")
				}
			}
		})
	}
}
