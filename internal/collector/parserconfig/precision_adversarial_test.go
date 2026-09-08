package parserconfig

import (
	"testing"
	"time"
)

func TestTimestampPrecisionAdversarialLiteralBoundaries(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, layout, value string
		want                time.Time
	}{
		{
			name:   "compact-adjacent-tokens",
			layout: "20060102T150405Z0700|.8888888888",
			value:  "20260908T123456.123456789+0130|.8888888888",
			want:   time.Date(2026, 9, 8, 11, 4, 56, 123456789, time.UTC),
		},
		{
			name:   "unpadded-clock-with-adjacent-meridiem",
			layout: "2006-1-2 3:4:5PM -0700 ,8888888888",
			value:  "2026-9-8 1:2:3,123456789PM -0030 ,8888888888",
			want:   time.Date(2026, 9, 8, 13, 32, 3, 123456789, time.UTC),
		},
		{
			name:   "unpadded-tokens-padded-value",
			layout: "2006-1-2 3:4:5PM -0700 ,8888888888",
			value:  "2026-09-08 01:02:03.000000009PM +0000 ,8888888888",
			want:   time.Date(2026, 9, 8, 13, 2, 3, 9, time.UTC),
		},
		{
			name:   "space-padded-day-offset-before-clock",
			layout: "Jan _2 2006 -07:00 15:04:05|.8888888888",
			value:  "Sep  8 2026 +01:30 12:34:56.123456789|.8888888888",
			want:   time.Date(2026, 9, 8, 11, 4, 56, 123456789, time.UTC),
		},
		{
			name:   "unpadded-day-variable-whitespace",
			layout: "Jan _2 2006 -07:00 15:04:05|,8888888888",
			value:  "Sep 8   2026   +01:30  12:34:56,123456789|,8888888888",
			want:   time.Date(2026, 9, 8, 11, 4, 56, 123456789, time.UTC),
		},
		{
			name:   "space-padded-year-day",
			layout: "2006-__2 15:04:05Z07:00 .8888888888",
			value:  "2026-  8 12:34:56.123456789Z .8888888888",
			want:   time.Date(2026, 1, 8, 12, 34, 56, 123456789, time.UTC),
		},
		{
			name:   "literal-between-clock-and-offset",
			layout: "2006-01-02T15:04:05|.8888888888|Z07:00",
			value:  "2026-09-08T12:34:56.123456789|.8888888888|+01:30",
			want:   time.Date(2026, 9, 8, 11, 4, 56, 123456789, time.UTC),
		},
		{
			name:   "seconds-offset-at-start",
			layout: "-07:00:00|2006-01-02T15:04:05|,8888888888",
			value:  "-00:00:30|2026-09-08T12:34:56,123456789|,8888888888",
			want:   time.Date(2026, 9, 8, 12, 35, 26, 123456789, time.UTC),
		},
		{
			name:   "zero-padded-explicit-fraction-with-other-separator-literal",
			layout: "2006-01-02T15:04:05.000000000Z07:00|,8888888888",
			value:  "2026-09-08T12:34:56,000000009Z|,8888888888",
			want:   time.Date(2026, 9, 8, 12, 34, 56, 9, time.UTC),
		},
		{
			name:   "explicit-optional-fraction-absent-with-literal",
			layout: "2006-01-02T15:04:05.999999999Z07:00|.8888888888",
			value:  "2026-09-08T12:34:56Z|.8888888888",
			want:   time.Date(2026, 9, 8, 12, 34, 56, 0, time.UTC),
		},
		{
			name:   "repeated-seconds-later-fraction-wins",
			layout: "2006-01-02 15:04:05 / 5Z07:00 .8888888888",
			value:  "2026-09-08 12:34:03.123456789 / 56,000000009Z .8888888888",
			want:   time.Date(2026, 9, 8, 12, 34, 56, 9, time.UTC),
		},
		{
			name:   "repeated-seconds-earlier-fraction-persists",
			layout: "2006-01-02 15:04:05 / 5Z07:00 ,8888888888",
			value:  "2026-09-08 12:34:03,123456789 / 56Z ,8888888888",
			want:   time.Date(2026, 9, 8, 12, 34, 56, 123456789, time.UTC),
		},
		{
			name:   "fraction-token-away-from-seconds",
			layout: "2006-01-02 .999999999|15:04:05Z07:00 .8888888888",
			value:  "2026-09-08 .000000009|12:34:56Z .8888888888",
			want:   time.Date(2026, 9, 8, 12, 34, 56, 9, time.UTC),
		},
		{
			name:   "long-names-case-and-original-padding",
			layout: "Monday, January _2, 2006 15:04:05 Z0700|.8888888888",
			value:  "tuesday, september  8, 2026 12:34:56.123456789 +0000|.8888888888",
			want:   time.Date(2026, 9, 8, 12, 34, 56, 123456789, time.UTC),
		},
		{
			name:   "underscore-year-and-literal-tab",
			layout: "_2006-01-02\t15:04:05Z07:00|,8888888888",
			value:  "_2026-09-08\t12:34:56.123456789Z|,8888888888",
			want:   time.Date(2026, 9, 8, 12, 34, 56, 123456789, time.UTC),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Independently written UTC expectations also establish that these are
			// actual Go layouts, without treating production ParseTime as an oracle.
			standard, err := time.Parse(tc.layout, tc.value)
			if err != nil || !standard.Equal(tc.want) {
				t.Fatalf("invalid test fixture: standard Go result=%v err=%v want=%v", standard, err, tc.want)
			}
			compiled, err := Compile("logfmt", &Options{TimestampLayout: tc.layout})
			if err != nil {
				t.Fatalf("Compile valid layout: %v", err)
			}
			got, err := compiled.ParseTime(tc.value)
			if err != nil || !got.Equal(tc.want) {
				t.Fatalf("literal/token boundary changed timestamp: got=%v err=%v want=%v", got, err, tc.want)
			}
		})
	}
}

func TestTimestampPrecisionAdversarialRejectsConsumedExcess(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, layout, value string }{
		{"compact-implicit", "20060102T150405Z0700", "20260908T123456.1234567890+0130"},
		{"unpadded-comma-before-meridiem", "2006-1-2 3:4:5PM -0700", "2026-9-8 1:2:3,0000000000PM -0030"},
		{"padded-day-offset-before-fraction", "Jan _2 2006 -07:00 15:04:05", "Sep 8   2026  +01:30  12:34:56.1234567890"},
		{"first-of-two-seconds", "2006-01-02 15:04:05 / 5Z07:00", "2026-09-08 12:34:03.1234567890 / 56.000000009Z"},
		{"second-of-two-seconds", "2006-01-02 15:04:05 / 5Z07:00", "2026-09-08 12:34:03.123456789 / 56.0000000000Z"},
		{"fraction-before-seconds", "2006-01-02 .999999999|15:04:05Z07:00", "2026-09-08 .1234567890|12:34:56Z"},
		{"fraction-before-seconds-overwritten", "2006-01-02 .999999999|15:04:05Z07:00", "2026-09-08 .1234567890|12:34:56.000000009Z"},
		{"middle-offset-optional-fraction-after", "2006-01-02 Z07:00 15:04:05.999999999", "2026-09-08 +01:30 12:34:56.1234567890"},
	} {
		for _, literal := range []string{"", " |.8888888888", " |,8888888888"} {
			t.Run(tc.name+literal, func(t *testing.T) {
				layout, value := tc.layout+literal, tc.value+literal
				if _, err := time.Parse(layout, value); err != nil {
					t.Fatalf("fixture does not exercise Go's lossy accepted precision: %v", err)
				}
				compiled, err := Compile("logfmt", &Options{TimestampLayout: layout})
				if err != nil {
					t.Fatalf("Compile valid layout: %v", err)
				}
				if got, err := compiled.ParseTime(value); err == nil {
					t.Fatalf("accepted consumed fractional digits beyond nanosecond precision: %v", got)
				}
			})
		}
	}
}
