package parserconfig

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestPrecisionWalkPreservesGoTokenSpelling(t *testing.T) {
	tests := []struct {
		layout, value string
		want          time.Time
	}{
		{".88820060102 15:04:05Z07:00", ".88820260908 12:34:56.123000000Z", time.Date(2026, 9, 8, 12, 34, 56, 123000000, time.UTC)},
		{",88820060102 15:4:5Z07:00", ",88820260908 2:3:4,100Z", time.Date(2026, 9, 8, 2, 3, 4, 100000000, time.UTC)},
		{".8888888888 Mon January _2 _2006 3:4:5.999 PM Z07:00", ".8888888888 tUE sEPTEMBER  8 _2026 1:2:3.100 PM Z", time.Date(2026, 9, 8, 13, 2, 3, 100000000, time.UTC)},
		{"2006 002 __2 .8888888888 15:04:05-070000", "2026 251 251 .8888888888 12:34:56-000001", time.Date(2026, 9, 8, 12, 34, 57, 0, time.UTC)},
		{"Jan 2 06 .8888888888 03:04:05pm Z07:00", "Sep 8 26 .8888888888 01:02:03pm Z", time.Date(2026, 9, 8, 13, 2, 3, 0, time.UTC)},
		{".8888888888 2006-01-02 15:04:05 | .999999999 Z07:00", ".8888888888 2026-09-08 12:34:56 | .100000000 Z", time.Date(2026, 9, 8, 12, 34, 56, 100000000, time.UTC)},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			c, err := Compile("logfmt", &Options{TimestampLayout: tt.layout})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := time.Parse(tt.layout, tt.value); err != nil {
				t.Fatalf("fixture is not a valid Go timestamp: %v", err)
			}
			// Go's -1-second offset sentinel defect is covered separately; the explicit
			// expected UTC instant remains the oracle here as well.
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

func TestPrecisionWalkRejectsAllActualOverprecision(t *testing.T) {
	for _, layout := range []string{
		".8888888888 2006-01-02T15:04:05Z07:00",
		".8888888888 2006-01-02T15:04:05.999999999Z07:00",
	} {
		c, err := Compile("logfmt", &Options{TimestampLayout: layout})
		if err != nil {
			t.Fatal(err)
		}
		for _, fraction := range []string{".1234567890", ",0000000000"} {
			if _, err := c.ParseTime(".8888888888 2026-09-08T12:34:56" + fraction + "Z"); err == nil {
				t.Fatalf("accepted real excessive precision: %s", fraction)
			}
		}
	}
	c, err := Compile("logfmt", &Options{TimestampLayout: ".8888888888 2006-01-02T15:04:05|05Z07:00"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ParseTime(".8888888888 2026-09-08T12:34:56.1234567890|56Z"); err == nil {
		t.Fatal("accepted overwritten earlier excessive fraction")
	}
}

func TestPrecisionLayoutRejectsUnsupportedFractionWidths(t *testing.T) {
	for _, digit := range []string{"0", "9"} {
		if _, err := Compile("logfmt", &Options{TimestampLayout: "2006-01-02T15:04:05." + strings.Repeat(digit, 10) + "Z07:00"}); err == nil {
			t.Fatalf("accepted unsupported ten-%s fractional layout", digit)
		}
	}
}

// FuzzPrecisionTokenBoundaries constructs the UTC instant independently, then
// varies token boundaries and decimal spelling. It exercises both acceptance
// and rejection with literal decimals forcing the spelling walk.
func FuzzPrecisionTokenBoundaries(f *testing.F) {
	f.Add([]byte{0, 26, 8, 7, 12, 34, 56, 123, 0})
	f.Add([]byte{5, 26, 8, 7, 12, 34, 56, 0, 16})
	f.Add([]byte{7, 24, 1, 27, 23, 59, 59, 255, 3})
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 64 {
			return
		}
		var data [9]byte
		copy(data[:], raw)
		year, month, day := 2000+int(data[1]%50), time.Month(1+data[2]%12), 1+int(data[3]%28)
		hour, minute, second := int(data[4]%24), int(data[5]%60), int(data[6]%60)
		nanos := int(data[7]) * 1000000
		fraction := fmt.Sprintf(".%09d", nanos)
		if data[8]&1 != 0 {
			fraction = "," + fraction[1:]
		}
		invalid := data[8]&16 != 0
		if invalid {
			fraction += "0"
		}
		want := time.Date(year, month, day, hour, minute, second, nanos, time.UTC)
		var layout, value string
		switch data[0] % 8 {
		case 0:
			layout = "20060102 150405Z07:00"
			value = fmt.Sprintf("%04d%02d%02d %02d%02d%02d%sZ", year, month, day, hour, minute, second, fraction)
		case 1:
			h12 := hour % 12
			if h12 == 0 {
				h12 = 12
			}
			marker := "am"
			if hour >= 12 {
				marker = "pm"
			}
			layout = "06/1/2 3:4:5pm Z0700"
			value = fmt.Sprintf("%02d/%d/%d %d:%d:%d%s%s Z", year%100, month, day, h12, minute, second, fraction, marker)
		case 2:
			layout = "Mon Jan _2 15:04:05.999999999 2006 -07:00"
			value = fmt.Sprintf("%s %s %2d %d:%02d:%02d%s %04d +00:00", want.Weekday().String()[:3], month.String()[:3], day, hour, minute, second, fraction, year)
		case 3:
			layout = "2006 __2 15:04:05Z07:00"
			value = fmt.Sprintf("%04d %3d %d:%02d:%02d%sZ", year, want.YearDay(), hour, minute, second, fraction)
		case 4:
			layout = "Monday January 2 _2006 15:4:5Z07:00"
			value = fmt.Sprintf("%s %s %d _%04d %d:%d:%d%sZ", strings.ToLower(want.Weekday().String()), strings.ToUpper(month.String()), day, year, hour, minute, second, fraction)
		case 5:
			layout = "2006-01-02T15:04:05|05Z07:00"
			value = fmt.Sprintf("%04d-%02d-%02dT%d:%02d:%02d%s|%02d%sZ", year, month, day, hour, minute, second, fraction, second, fraction)
		case 6:
			layout = "2006-01-02T15:04:05 | .999999999 Z07:00"
			value = fmt.Sprintf("%04d-%02d-%02dT%d:%02d:%02d | %s Z", year, month, day, hour, minute, second, fraction)
		case 7:
			layout = ".88820060102 15:04:05Z07:00"
			value = fmt.Sprintf(".888%04d%02d%02d %d:%02d:%02d%sZ", year, month, day, hour, minute, second, fraction)
		}
		switch (data[8] >> 1) % 3 {
		case 0:
			layout = ".8888888888 " + layout
			value = ".8888888888 " + value
		case 1:
			layout += " ,8888888888"
			value += " ,8888888888"
		case 2:
			layout = ".8888888888 " + layout + " ,8888888888"
			value = ".8888888888 " + value + " ,8888888888"
		}
		c, err := Compile("logfmt", &Options{TimestampLayout: layout})
		if err != nil {
			t.Fatalf("layout %q: %v", layout, err)
		}
		actual, err := c.ParseTime(value)
		if invalid {
			if err == nil {
				t.Fatalf("accepted excessive precision: layout=%q value=%q", layout, value)
			}
			return
		}
		if err != nil {
			t.Fatalf("rejected valid precision: layout=%q value=%q: %v", layout, value, err)
		}
		if !actual.Equal(want) {
			t.Fatalf("layout=%q value=%q actual=%v want=%v", layout, value, actual, want)
		}
	})
}

func BenchmarkTimestampPrecisionSpelling(b *testing.B) {
	for _, count := range []int{0, 1, 64, 1024} {
		b.Run(fmt.Sprintf("literal_runs_%d", count), func(b *testing.B) {
			literals := strings.Repeat(".8888888888 ", count)
			c, err := Compile("logfmt", &Options{TimestampLayout: literals + time.RFC3339Nano})
			if err != nil {
				b.Fatal(err)
			}
			value := literals + "2026-09-08T12:34:56.123000000Z"
			b.ReportAllocs()
			b.SetBytes(int64(len(value)))
			b.ResetTimer()
			for b.Loop() {
				if _, err := c.ParseTime(value); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
