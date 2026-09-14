package parserconfig

import (
	"fmt"
	"testing"
	"time"
)

func TestOffsetBeforeVariableTimestampSuffix(t *testing.T) {
	tests := []struct {
		layout, value string
		want          time.Time
	}{
		{"2006-01-02 -07:00 15:04:05.999999999", "2026-09-08 +00:00 12:34:56.100", time.Date(2026, 9, 8, 12, 34, 56, 100000000, time.UTC)},
		{"2006-01-02 -07:00 15:04:05.999999999", "2026-09-08 +00:00 2:34:56.1000", time.Date(2026, 9, 8, 2, 34, 56, 100000000, time.UTC)},
		{"2006-01-02 -07:00 15:04:05", "2026-09-08 +00:00 2:34:56,100", time.Date(2026, 9, 8, 2, 34, 56, 100000000, time.UTC)},
		{"2006-1-2 3:4:5.999999999PM Z0700 suffix", "2026-9-8 1:2:3.100PM +0530 suffix", time.Date(2026, 9, 8, 7, 32, 3, 100000000, time.UTC)},
		{"Mon Jan _2 -07:00 15:4:5,999999999 2006", "Tue Sep  8 +00:00 2:3:4.100 2026", time.Date(2026, 9, 8, 2, 3, 4, 100000000, time.UTC)},
		{"2006-01-02 15:04:05.999999999 -07:00 4:5", "2026-09-08 2:03:04.100 +00:00 03:04", time.Date(2026, 9, 8, 2, 3, 4, 100000000, time.UTC)},
		{"2006-01-02 15:04:05 -07:00 |end", "2026-09-08 2:03:04.100 +00:00 |end", time.Date(2026, 9, 8, 2, 3, 4, 100000000, time.UTC)},
		{"2006-01-02 -07:00 15:04:05.999999999", "2026-09-08    +00:00   12:34:56", time.Date(2026, 9, 8, 12, 34, 56, 0, time.UTC)},
		{"Z07:00 2006-01-02 15:04:05.999999999", "Z 2026-09-08 12:34:56.100", time.Date(2026, 9, 8, 12, 34, 56, 100000000, time.UTC)},
		{"2006-01-02 15:04:05\x00-07:00 4", "2026-09-08 12:34:56\x00+00:00 34", time.Date(2026, 9, 8, 12, 34, 56, 0, time.UTC)},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			reference, err := time.Parse(tt.layout, tt.value)
			if err != nil || !reference.Equal(tt.want) {
				t.Fatalf("invalid independently specified fixture: reference=%v err=%v want=%v", reference, err, tt.want)
			}
			c, err := Compile("logfmt", &Options{TimestampLayout: tt.layout})
			if err != nil {
				t.Fatal(err)
			}
			actual, err := c.ParseTime(tt.value)
			if err != nil {
				t.Fatal(err)
			}
			if !actual.Equal(tt.want) {
				t.Fatalf("actual=%v want=%v", actual, tt.want)
			}
		})
	}
}

func TestAllNumericOffsetFormsKeepOriginalSpelling(t *testing.T) {
	for _, tt := range []struct {
		token, zone string
		seconds     int
	}{
		{"-07", "+05", 18000}, {"Z07", "-05", -18000},
		{"-0700", "+0530", 19800}, {"Z0700", "-0530", -19800},
		{"-07:00", "+05:30", 19800}, {"Z07:00", "-05:30", -19800},
		{"-070000", "+053045", 19845}, {"Z070000", "-053045", -19845},
		{"-07:00:00", "+05:30:45", 19845}, {"Z07:00:00", "-05:30:45", -19845},
		{"-07:00:00", "-00:00:01", -1}, {"-070000", "-000001", -1},
		{"Z07", "Z", 0}, {"Z0700", "Z", 0}, {"Z07:00", "Z", 0}, {"Z070000", "Z", 0}, {"Z07:00:00", "Z", 0},
	} {
		for _, terminal := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%s/terminal=%t", tt.token, tt.zone, terminal), func(t *testing.T) {
				layout := "2006-01-02 " + tt.token + " 15:04:05.999999999"
				value := "2026-09-08 " + tt.zone + " 2:34:56.100"
				if terminal {
					layout = "2006-01-02 15:04:05.999999999 " + tt.token
					value = "2026-09-08 2:34:56.100 " + tt.zone
				}
				c, err := Compile("logfmt", &Options{TimestampLayout: layout})
				if err != nil {
					t.Fatal(err)
				}
				actual, err := c.ParseTime(value)
				if err != nil {
					t.Fatal(err)
				}
				want := time.Date(2026, 9, 8, 2, 34, 56, 100000000, time.UTC).Add(-time.Duration(tt.seconds) * time.Second)
				if !actual.Equal(want) {
					t.Fatalf("actual=%v want=%v", actual, want)
				}
			})
		}
	}
}

func TestOffsetBeforeSuffixStillRejectsInvalidComponents(t *testing.T) {
	for _, tt := range []struct{ token, zone string }{
		{"-07", "+24"}, {"Z07", "-24"},
		{"-0700", "+0160"}, {"Z0700", "-2460"},
		{"-07:00", "+01:60"}, {"Z07:00", "-24:00"},
		{"-070000", "+013060"}, {"Z070000", "-016030"},
		{"-07:00:00", "+01:30:60"}, {"Z07:00:00", "-01:60:30"},
	} {
		t.Run(tt.token, func(t *testing.T) {
			c, err := Compile("logfmt", &Options{TimestampLayout: "2006-01-02 " + tt.token + " 15:04:05.999999999"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.ParseTime("2026-09-08 " + tt.zone + " 12:34:56.100"); err == nil {
				t.Fatal("accepted invalid numeric offset component")
			}
		})
	}
}

// FuzzOriginalOffsetSpelling constructs independently specified valid wall
// times and offsets, then varies their legal Go-layout spellings. The expected
// instant comes directly from numeric components, not the production parser.
func FuzzOriginalOffsetSpelling(f *testing.F) {
	f.Add([]byte{0, 0, 0, 8, 12, 34, 56, 100, 0, 0, 0, 0})
	f.Add([]byte{7, 49, 11, 27, 23, 59, 59, 255, 23, 59, 59, 255})
	f.Add([]byte{2, 24, 1, 27, 0, 0, 0, 1, 5, 30, 45, 32})
	tokens := []string{"-07", "Z07", "-0700", "Z0700", "-07:00", "Z07:00", "-070000", "Z070000", "-07:00:00", "Z07:00:00"}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 64 {
			return
		}
		var data [12]byte
		copy(data[:], raw)
		year, month, day := 2000+int(data[1])%50, time.Month(1+data[2]%12), 1+int(data[3])%28
		hour, minute, second := int(data[4]%24), int(data[5]%60), int(data[6]%60)
		nanos := int(data[7]) * 1000000
		fraction := fmt.Sprintf(".%09d", nanos)
		clockLayout := "15:4:5.999999999"
		if data[11]&1 != 0 {
			clockLayout = "15:4:5"
		}
		if data[11]&2 != 0 {
			fraction = "," + fraction[1:]
		}
		if data[11]&4 != 0 {
			fraction = ""
			nanos = 0
		}
		clock := fmt.Sprintf("%d:%d:%d%s", hour, minute, second, fraction)
		if data[11]&8 != 0 {
			clock = fmt.Sprintf("%02d:%02d:%02d%s", hour, minute, second, fraction)
		}
		tokenIndex := int(data[0]) % len(tokens)
		token := tokens[tokenIndex]
		zoneHours, zoneMinutes, zoneSeconds := int(data[8]%24), int(data[9]%60), int(data[10]%60)
		if tokenIndex < 6 {
			zoneSeconds = 0
		}
		if tokenIndex < 2 {
			zoneMinutes = 0
		}
		offset := zoneHours*3600 + zoneMinutes*60 + zoneSeconds
		sign := "+"
		if data[11]&16 != 0 {
			sign = "-"
			offset = -offset
		}
		zone := fmt.Sprintf("%s%02d", sign, zoneHours)
		if tokenIndex >= 2 {
			separator := ""
			if tokenIndex == 4 || tokenIndex == 5 || tokenIndex >= 8 {
				separator = ":"
			}
			zone += fmt.Sprintf("%s%02d", separator, zoneMinutes)
			if tokenIndex >= 6 {
				zone += fmt.Sprintf("%s%02d", separator, zoneSeconds)
			}
		}
		if tokenIndex%2 == 1 && data[11]&32 != 0 {
			zone = "Z"
			offset = 0
		}
		date := fmt.Sprintf("%04d-%02d-%02d", year, month, day)
		var layout, value string
		switch (data[11] >> 6) % 4 {
		case 0:
			layout = "2006-01-02 " + token + " " + clockLayout
			value = date + " " + zone + " " + clock
		case 1:
			layout = "2006-01-02 " + clockLayout + " " + token
			value = date + " " + clock + " " + zone
		case 2:
			layout = token + " " + clockLayout + " 2006-01-02"
			value = zone + " " + clock + " " + date
		case 3:
			layout = "2006-01-02 " + clockLayout + " [" + token + "] suffix"
			value = date + " " + clock + " [" + zone + "] suffix"
		}
		c, err := Compile("logfmt", &Options{TimestampLayout: layout})
		if err != nil {
			t.Fatal(err)
		}
		got, err := c.ParseTime(value)
		if err != nil {
			t.Fatalf("layout=%q value=%q: %v", layout, value, err)
		}
		want := time.Date(year, month, day, hour, minute, second, nanos, time.UTC).Add(-time.Duration(offset) * time.Second)
		if !got.Equal(want) {
			t.Fatalf("layout=%q value=%q: got=%v want=%v", layout, value, got, want)
		}
	})
}

func BenchmarkTimestampOffsetPosition(b *testing.B) {
	for _, test := range []struct{ name, layout, value string }{
		{"terminal_z", time.RFC3339Nano, "2026-09-08T12:34:56.100Z"},
		{"terminal_numeric", time.RFC3339Nano, "2026-09-08T12:34:56.100+05:30"},
		{"nonterminal_z", "2006-01-02 Z07:00 15:04:05.999999999", "2026-09-08 Z 12:34:56.100"},
		{"nonterminal_numeric", "2006-01-02 -07:00 15:04:05.999999999", "2026-09-08 +05:30 12:34:56.100"},
	} {
		b.Run(test.name, func(b *testing.B) {
			c, err := Compile("logfmt", &Options{TimestampLayout: test.layout})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(test.value)))
			b.ResetTimer()
			for b.Loop() {
				if _, err := c.ParseTime(test.value); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
