package parserconfig

import "testing"

func FuzzCompileAndCapture(f *testing.F) {
	f.Add("[%{timestamp}] %{level} [%{thread}] %{message}", "[2024-02-29T12:00:00Z] INFO [worker] message")
	f.Add("%{level} %{message}", "INFO message\n\tat app.Main")
	f.Add("prefix %% %{message}", "prefix % ")
	f.Add("%{bad}%{message}", "")
	f.Fuzz(func(t *testing.T, pattern, raw string) {
		c, err := Compile("log4j2-pattern", &Options{Pattern: pattern, TimestampLayout: "2006-01-02T15:04:05Z07:00"})
		if err != nil {
			c, err = Compile("logback-pattern", &Options{Pattern: pattern})
			if err != nil {
				return
			}
		}
		captures, err := c.Capture([]byte(raw))
		if err != nil {
			return
		}
		if len(captures) == 0 || captures[len(captures)-1].Name != "message" {
			t.Fatal("accepted pattern without terminal message")
		}
		for _, capture := range captures {
			if len(capture.Value) > len(raw) {
				t.Fatal("capture exceeds input")
			}
		}
	})
}

func FuzzTimestampParsing(f *testing.F) {
	f.Add("2024-02-29T12:00:00.123456789Z")
	f.Add("2024-11-03 01:30:00")
	f.Add("9999-12-31 23:59:59")
	f.Add("2024-03-10 02:30:00")
	offset, err := Compile("logfmt", nil)
	if err != nil {
		f.Fatal(err)
	}
	local, err := Compile("logfmt", &Options{TimestampLayout: "2006-01-02 15:04:05", Timezone: "America/New_York"})
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		for _, c := range []*Compiled{offset, local} {
			got, err := c.ParseTime(raw)
			if err != nil {
				continue
			}
			if got.Year() < 1 || got.Year() > 9999 {
				t.Fatal("timestamp outside protobuf bounds")
			}
		}
	})
}

func FuzzLiteralMatcher(f *testing.F) {
	f.Add(" a a b ", "x a a a a\t b   tail")
	f.Add("a b a b c", "a b a b a b a b c")
	f.Add(" \uFFFD ", "x \xff y")
	f.Fuzz(func(t *testing.T, pattern, raw string) {
		if len(pattern) > 256 || len(raw) > 4096 {
			return
		}
		start, end := compileLiteral(pattern).find(raw)
		wantStart, wantEnd := literalOracle(pattern, raw)
		if start != wantStart || end != wantEnd {
			t.Fatalf("literal=%q raw=%q got=%d,%d want=%d,%d", pattern, raw, start, end, wantStart, wantEnd)
		}
	})
}

// literalOracle is intentionally simple and independent of normalization/KMP.
func literalOracle(pattern, raw string) (int, int) {
	for start := 0; start <= len(raw); start++ {
		position := start
		matched := true
		for i := 0; i < len(pattern); i++ {
			if pattern[i] == ' ' || pattern[i] == '\t' {
				if position == len(raw) || (raw[position] != ' ' && raw[position] != '\t') {
					matched = false
					break
				}
				for position < len(raw) && (raw[position] == ' ' || raw[position] == '\t') {
					position++
				}
				for i+1 < len(pattern) && (pattern[i+1] == ' ' || pattern[i+1] == '\t') {
					i++
				}
			} else {
				if position == len(raw) || raw[position] != pattern[i] {
					matched = false
					break
				}
				position++
			}
		}
		if matched {
			return start, position
		}
	}
	return -1, -1
}
