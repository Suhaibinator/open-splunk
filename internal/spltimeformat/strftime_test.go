package spltimeformat

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestCompileStrftimeFormatAcceptsBoundedLocaleStableSubset(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name        string
		format      string
		outputBytes uint64
		directives  []Directive
	}{
		{name: "empty", format: "", outputBytes: 0},
		{
			name:        "Splunk ISO example",
			format:      "%Y-%m-%dT%H:%M:%S.%Q",
			outputBytes: 23,
			directives: []Directive{
				DirectiveYear, DirectiveMonthNumber, DirectiveDay,
				DirectiveHour24, DirectiveMinute, DirectiveSecond,
				DirectiveSubseconds,
			},
		},
		{
			name:        "precision and timezone",
			format:      "%3Q/%6Q/%9Q %3N/%6N/%9N %f %z %:z",
			outputBytes: 3 + 1 + 6 + 1 + 9 + 1 + 3 + 1 + 6 + 1 + 9 + 1 + 6 + 1 + 5 + 1 + 6,
			directives: []Directive{
				DirectiveSubseconds, DirectiveSubseconds, DirectiveSubseconds,
				DirectiveSubseconds, DirectiveSubseconds, DirectiveSubseconds,
				DirectiveMicroseconds, DirectiveTimezoneOffset,
				DirectiveTimezoneOffsetColon,
			},
		},
		{
			name:        "calendar and names",
			format:      "%F %T %a %A %b %B %j %V %G %g %w %y %e %I %p %s %%",
			outputBytes: 99,
			directives: []Directive{
				DirectiveISODate, DirectiveTime24, DirectiveWeekdayShort,
				DirectiveWeekdayLong, DirectiveMonthShort, DirectiveMonthLong,
				DirectiveDayOfYear, DirectiveISOWeek, DirectiveISOWeekYear,
				DirectiveISOWeekYearShort, DirectiveWeekdayNumber,
				DirectiveYearShort, DirectiveDaySpace, DirectiveHour12,
				DirectiveAMPM, DirectiveEpochSeconds, DirectivePercent,
			},
		},
		{
			name:        "Unicode literal",
			format:      "東京 %H時%M分",
			outputBytes: uint64(len("東京 ") + 2 + len("時") + 2 + len("分")),
			directives:  []Directive{DirectiveHour24, DirectiveMinute},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			compiled, err := CompileStrftimeFormat(test.format)
			if err != nil {
				t.Fatalf("CompileStrftimeFormat(%q): %v", test.format, err)
			}
			if compiled.MaximumOutputBytes != test.outputBytes {
				t.Fatalf(
					"CompileStrftimeFormat(%q) = %#v, want output bound %d",
					test.format,
					compiled,
					test.outputBytes,
				)
			}
			gotDirectives := make([]Directive, 0, len(compiled.Parts))
			for _, part := range compiled.Parts {
				if part.Directive != DirectiveLiteral {
					gotDirectives = append(gotDirectives, part.Directive)
				}
			}
			if !slices.Equal(gotDirectives, test.directives) {
				t.Fatalf("directives = %v, want %v", gotDirectives, test.directives)
			}
		})
	}
}

func TestCompileStrftimeFormatPinsSubsecondDefaultsAndParts(t *testing.T) {
	t.Parallel()

	compiled, err := CompileStrftimeFormat("pre%Q:%N:post")
	if err != nil {
		t.Fatal(err)
	}
	if len(compiled.Parts) != 5 ||
		compiled.Parts[0] != (Part{Directive: DirectiveLiteral, Literal: "pre"}) ||
		compiled.Parts[1] != (Part{Directive: DirectiveSubseconds, Width: 3}) ||
		compiled.Parts[2] != (Part{Directive: DirectiveLiteral, Literal: ":"}) ||
		compiled.Parts[3] != (Part{Directive: DirectiveSubseconds, Width: 9}) ||
		compiled.Parts[4] != (Part{Directive: DirectiveLiteral, Literal: ":post"}) {
		t.Fatalf("parts = %#v", compiled.Parts)
	}
}

func TestCompileStrftimeFormatRejectsInvalidUnsupportedAndOversizedFormats(t *testing.T) {
	t.Parallel()

	for _, format := range []string{
		"bad\x00format",
		"bad\xffformat",
		"%",
		"%2Q",
		"%4N",
		"%0Q",
		"%12Q",
		"%q",
		"%c",
		"%+",
		"%x",
		"%X",
		"%U",
		"%k",
		"%C",
		"%Z",
		"%Ez",
		"%::z",
		"%:::z",
	} {
		if _, err := CompileStrftimeFormat(format); !errors.Is(err, ErrInvalidStrftimeFormat) {
			t.Errorf(
				"CompileStrftimeFormat(%q) error = %v, want ErrInvalidStrftimeFormat",
				format,
				err,
			)
		}
	}
	if _, err := CompileStrftimeFormat(
		strings.Repeat("x", MaximumStrftimeFormatBytes+1),
	); !errors.Is(err, ErrStrftimeFormatTooLarge) {
		t.Fatalf("oversized format error = %v, want ErrStrftimeFormatTooLarge", err)
	}
	if _, err := CompileStrftimeFormat(
		strings.Repeat("%s", MaximumStrftimeOutputBytes/20+1),
	); !errors.Is(err, ErrStrftimeFormatTooLarge) {
		t.Fatalf("output-amplifying format error = %v, want ErrStrftimeFormatTooLarge", err)
	}
}
