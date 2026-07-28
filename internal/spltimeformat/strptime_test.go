package spltimeformat

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestCompileStrptimeFormatAcceptsDeterministicFullDateSubset(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		format     string
		directives []Directive
	}{
		{
			name:       "date only",
			format:     "%F",
			directives: []Directive{DirectiveISODate},
		},
		{
			name:   "numeric date time",
			format: "%Y-%m-%dT%T",
			directives: []Directive{
				DirectiveYear,
				DirectiveMonthNumber,
				DirectiveDay,
				DirectiveTime24,
			},
		},
		{
			name:   "12 hour and offset",
			format: "%m/%d/%Y %I:%M:%S %p %z",
			directives: []Directive{
				DirectiveMonthNumber,
				DirectiveDay,
				DirectiveYear,
				DirectiveHour12,
				DirectiveMinute,
				DirectiveSecond,
				DirectiveAMPM,
				DirectiveTimezoneOffset,
			},
		},
		{
			name:   "default milliseconds",
			format: "%Y-%m-%d %H:%M:%S.%Q",
		},
		{
			name:   "explicit milliseconds",
			format: "%Y-%m-%d %H:%M:%S.%3N",
		},
		{
			name:   "microseconds",
			format: "%Y-%m-%d %H:%M:%S.%6Q",
		},
		{
			name:   "microsecond alias",
			format: "%Y-%m-%d %H:%M:%S.%f",
		},
		{
			name:   "literal percent and letters",
			format: "on %Y%%%m%%%d",
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			compiled, err := CompileStrptimeFormat(test.format)
			if err != nil {
				t.Fatalf("CompileStrptimeFormat(%q): %v", test.format, err)
			}
			if compiled.WorkUnits == 0 {
				t.Fatalf("CompileStrptimeFormat(%q) = %#v", test.format, compiled)
			}
			if test.directives == nil {
				return
			}
			got := make([]Directive, 0, len(compiled.Parts))
			for _, part := range compiled.Parts {
				if part.Directive != DirectiveLiteral {
					got = append(got, part.Directive)
				}
			}
			if !slices.Equal(got, test.directives) {
				t.Fatalf("directives = %v, want %v", got, test.directives)
			}
		})
	}
}

func TestCompileStrptimeFormatRejectsAmbiguousIncompleteOrUnsupportedFormats(t *testing.T) {
	t.Parallel()

	for _, format := range []string{
		"",
		"%Y-%m",
		"%m-%d",
		"%Y-%d",
		"%H:%M",
		"%Y-%m-%d %M",
		"%Y-%m-%d %H:%S",
		"%Y-%m-%d %H:%M.%3N",
		"%Y-%m-%d %H:%M:%S %p",
		"%Y-%m-%d %I:%M:%S",
		"%Y-%m-%d %H:%M:%S %p",
		"%F %Y",
		"%F %m",
		"%F %d",
		"%Y-%m-%d %T %H",
		"%Y-%m-%d %T %M",
		"%Y-%m-%d %T %S",
		"%Y-%m-%d %H:%M:%S %z %z",
		"%Y-%m-%d %H:%M:%S %Q %f",
		"%Y-%m-%d %H:%M:%S.%N",
		"%Y-%m-%d %H:%M:%S.%9N",
		"%Y-%m-%d %H:%M:%S.%9Q",
		"%Y-%m-%d %H:%M:%S %:z",
		"%y-%m-%d",
		"%Y-%b-%d",
		"%Y-%B-%d",
		"%Y-%m-%e",
		"%Y-%j",
		"%G-%V-%w",
		"%Y-%m-%d %a",
		"%Y-%m-%d %A",
		"%s",
		"%Y-%m-%d %Z",
		"%Y-%m-%d %",
		"%Y-%m-%d %q",
	} {
		if _, err := CompileStrptimeFormat(format); !errors.Is(
			err,
			ErrInvalidStrptimeFormat,
		) {
			t.Errorf(
				"CompileStrptimeFormat(%q) error = %v, want ErrInvalidStrptimeFormat",
				format,
				err,
			)
		}
	}
}

func TestCompileStrptimeFormatPinsIndependentResourceLimit(t *testing.T) {
	t.Parallel()

	if MaximumStrptimeFormatBytes != 4<<10 ||
		MaximumStrptimeWorkUnits != 4<<10 {
		t.Fatalf(
			"strptime limits = %d bytes/%d work units, want 4096/4096",
			MaximumStrptimeFormatBytes,
			MaximumStrptimeWorkUnits,
		)
	}
	if _, err := CompileStrptimeFormat(
		strings.Repeat("x", MaximumStrptimeFormatBytes+1),
	); !errors.Is(err, ErrStrptimeFormatTooLarge) {
		t.Fatalf("oversized format error = %v, want ErrStrptimeFormatTooLarge", err)
	}
}

func TestCompileStrptimeFormatDoesNotInheritFormatterOutputLimit(t *testing.T) {
	t.Parallel()

	format := strings.Repeat("%F", 1700)
	if _, err := CompileStrptimeFormat(format); !errors.Is(
		err,
		ErrInvalidStrptimeFormat,
	) {
		t.Fatalf(
			"formatter-amplifying parse format error = %v, want ErrInvalidStrptimeFormat",
			err,
		)
	}
}
