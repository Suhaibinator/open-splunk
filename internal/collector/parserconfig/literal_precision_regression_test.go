package parserconfig

import (
	"strings"
	"testing"
	"time"
)

func TestTimestampLiteralDecimalPrecision(t *testing.T) {
	for _, literal := range []string{".8888888888", ",8888888888"} {
		t.Run(literal, func(t *testing.T) {
			compiled, err := Compile("logfmt", &Options{TimestampLayout: "2006-01-02T15:04:05Z07:00 " + literal})
			if err != nil {
				t.Fatal(err)
			}
			got, err := compiled.ParseTime("2026-09-08T12:34:56Z " + literal)
			if err != nil {
				t.Fatalf("literal digits are not fractional seconds: %v", err)
			}
			want := time.Date(2026, 9, 8, 12, 34, 56, 0, time.UTC)
			if !got.Equal(want) {
				t.Fatalf("timestamp = %v, want %v", got, want)
			}
		})
	}
}

func TestTimestampLiteralAndRealFractionPrecision(t *testing.T) {
	for _, prefix := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			for _, separator := range []string{".", ","} {
				layout := "2006-01-02T15:04:05"
				if explicit {
					layout += separator + "999999999"
				}
				layout += "Z07:00"
				literal := separator + strings.Repeat("8", 64)
				if prefix {
					layout = literal + " " + layout
				} else {
					layout += " " + literal
				}
				compiled, err := Compile("logfmt", &Options{TimestampLayout: layout})
				if err != nil {
					t.Fatal(err)
				}
				for _, digits := range []string{"123456789", "1234567890", "0000000000"} {
					raw := "2026-09-08T12:34:56" + separator + digits + "Z"
					if prefix {
						raw = literal + " " + raw
					} else {
						raw += " " + literal
					}
					got, parseErr := compiled.ParseTime(raw)
					if len(digits) > 9 {
						if parseErr == nil {
							t.Errorf("accepted lossy fraction %q with literal (prefix=%v explicit=%v)", digits, prefix, explicit)
						}
						continue
					}
					want := time.Date(2026, 9, 8, 12, 34, 56, 123456789, time.UTC)
					if parseErr != nil || !got.Equal(want) {
						t.Errorf("literal changed valid precision: got %v, %v; want %v (prefix=%v explicit=%v separator=%q)", got, parseErr, want, prefix, explicit, separator)
					}
				}
			}
		}
	}
}

func TestTimestampRejectsUnusableFixedPrecisionLayout(t *testing.T) {
	for _, separator := range []string{".", ","} {
		_, err := Compile("logfmt", &Options{TimestampLayout: "2006-01-02T15:04:05" + separator + "0000000000Z07:00"})
		if err == nil {
			t.Errorf("accepted fixed ten-digit layout whose required precision exceeds timestamp capacity")
		}
	}
}
