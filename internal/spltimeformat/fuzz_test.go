package spltimeformat

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"
)

func FuzzCompileStrftimeFormat(f *testing.F) {
	for _, seed := range []string{
		"",
		"%Y-%m-%d %H:%M:%S",
		"%Y-%m-%dT%H:%M:%S.%3N%z",
		"%F %T %:z",
		"%s",
		"%a %b %e %I:%M %p",
		"%G-W%V-%w %j",
		"%Q %N %f %3Q %6Q %9Q %3N %6N %9N",
		"100%% done",
		"%",
		"%%%",
		"%1N",
		"%:",
		"%:Y",
		"%c",
		"%Ey",
		"名前 %Y 値",
		"\x00",
		"\xff%Y",
		strings.Repeat("%s", 900),
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, format string) {
		first, firstErr := CompileStrftimeFormat(format)
		second, secondErr := CompileStrftimeFormat(format)
		if !errors.Is(firstErr, secondErr) || !errors.Is(secondErr, firstErr) || !reflect.DeepEqual(first, second) {
			t.Fatalf("CompileStrftimeFormat changed across identical input: first=%#v/%v second=%#v/%v",
				first, firstErr, second, secondErr)
		}
		if firstErr != nil {
			if !reflect.DeepEqual(first, StrftimeFormat{}) {
				t.Fatalf("error published a partial format: %#v", first)
			}
			timeFormatFuzzCheckSentinel(t, firstErr, ErrInvalidStrftimeFormat, ErrStrftimeFormatTooLarge)
			return
		}
		if first.MaximumOutputBytes > MaximumStrftimeOutputBytes {
			t.Fatalf("accepted format expands to %d bytes, more than %d", first.MaximumOutputBytes, MaximumStrftimeOutputBytes)
		}
		timeFormatFuzzCheckParts(t, format, first.Parts, first.WorkUnits, MaximumStrftimeWorkUnits)
		if got := timeFormatFuzzOutputBytes(first.Parts); got != first.MaximumOutputBytes {
			t.Fatalf("parts expand to %d bytes, metadata says %d: %#v", got, first.MaximumOutputBytes, first.Parts)
		}

		// The canonical spelling of the parts is itself a valid format that
		// compiles to the same program with the same resource charge.
		canonical := timeFormatFuzzRender(first.Parts)
		again, err := CompileStrftimeFormat(canonical)
		if err != nil {
			t.Fatalf("canonical format %q of %q does not compile: %v", canonical, format, err)
		}
		if !reflect.DeepEqual(again, first) {
			t.Fatalf("canonical format %q compiles differently from %q:\nfirst:  %#v\nsecond: %#v",
				canonical, format, first, again)
		}
	})
}

func FuzzCompileStrptimeFormat(f *testing.F) {
	for _, seed := range []string{
		"%Y-%m-%d",
		"%Y-%m-%d %H:%M:%S",
		"%Y-%m-%dT%H:%M:%S.%3N%z",
		"%Y-%m-%d %H:%M:%S.%6N",
		"%Y-%m-%d %H:%M:%S.%f",
		"%F %T",
		"%F %I:%M %p",
		"%d/%m/%Y",
		"%Y-%m-%d %I:%M",
		"%Y-%m-%d %H:%M %p",
		"%Y-%m-%d %M:%S",
		"%Y-%m-%d %H:%S",
		"%Y-%m-%d %H:%M:%S.%9N",
		"%Y-%m-%d %H:%M.%3N",
		"%Y-%m",
		"%Y-%m-%d-%d",
		"%F-%Y",
		"%Y-%m-%d %s",
		"%Y-%m-%d %a",
		"%Y-%m-%d %:z",
		"%Y-%m-%d %z %z",
		"",
		"%",
		"\x00",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, format string) {
		first, firstErr := CompileStrptimeFormat(format)
		second, secondErr := CompileStrptimeFormat(format)
		if !errors.Is(firstErr, secondErr) || !errors.Is(secondErr, firstErr) || !reflect.DeepEqual(first, second) {
			t.Fatalf("CompileStrptimeFormat changed across identical input: first=%#v/%v second=%#v/%v",
				first, firstErr, second, secondErr)
		}
		lexed, lexErr := compileBoundedTimeFormat(format, timeFormatLimits{
			maximumFormatBytes: MaximumStrptimeFormatBytes,
			maximumWorkUnits:   MaximumStrptimeWorkUnits,
		})
		if firstErr != nil {
			if !reflect.DeepEqual(first, StrptimeFormat{}) {
				t.Fatalf("error published a partial format: %#v", first)
			}
			timeFormatFuzzCheckSentinel(t, firstErr, ErrInvalidStrptimeFormat, ErrStrptimeFormatTooLarge)
			if errors.Is(firstErr, ErrStrptimeFormatTooLarge) != errors.Is(lexErr, errTimeFormatTooLarge) {
				t.Fatalf("strptime error %v disagrees with the shared lexer error %v", firstErr, lexErr)
			}
			return
		}
		if lexErr != nil || !reflect.DeepEqual(lexed.Parts, first.Parts) || lexed.WorkUnits != first.WorkUnits {
			t.Fatalf("accepted strptime format disagrees with the shared lexer: %#v vs %#v (%v)", first, lexed, lexErr)
		}
		timeFormatFuzzCheckParts(t, format, first.Parts, first.WorkUnits, MaximumStrptimeWorkUnits)
		timeFormatFuzzCheckStrptimeComponents(t, format, first.Parts)

		canonical := timeFormatFuzzRender(first.Parts)
		again, err := CompileStrptimeFormat(canonical)
		if err != nil {
			t.Fatalf("canonical format %q of %q does not compile: %v", canonical, format, err)
		}
		if !reflect.DeepEqual(again, first) {
			t.Fatalf("canonical format %q compiles differently from %q:\nfirst:  %#v\nsecond: %#v",
				canonical, format, first, again)
		}
	})
}

// timeFormatFuzzCheckSentinel requires the exact published sentinel, since
// the compilers deliberately return it unwrapped so callers can switch on it.
func timeFormatFuzzCheckSentinel(t *testing.T, err error, sentinels ...error) {
	t.Helper()
	matched := 0
	for _, sentinel := range sentinels {
		if errors.Is(err, sentinel) {
			matched++
		}
	}
	if matched != 1 {
		t.Fatalf("error %v matches %d published sentinels, want exactly one", err, matched)
	}
}

// timeFormatFuzzCheckParts holds the lexed parts to their structural
// contract: maximal non-empty literals without a bare %, directives with a
// width only where the directive has one, and a work charge that counts
// literal runes and directives exactly.
func timeFormatFuzzCheckParts(t *testing.T, format string, parts []Part, workUnits, maximumWorkUnits int) {
	t.Helper()
	if workUnits < 1 || workUnits > maximumWorkUnits {
		t.Fatalf("work units %d outside [1, %d] for %q", workUnits, maximumWorkUnits, format)
	}
	wantWork := 0
	for index, part := range parts {
		switch part.Directive {
		case DirectiveLiteral:
			if part.Literal == "" {
				t.Fatalf("part %d of %q is an empty literal", index, format)
			}
			if strings.ContainsRune(part.Literal, '%') {
				t.Fatalf("part %d of %q carries an unlexed %% in literal %q", index, format, part.Literal)
			}
			if index > 0 && parts[index-1].Directive == DirectiveLiteral {
				t.Fatalf("parts %d and %d of %q are adjacent literals", index-1, index, format)
			}
			if part.Width != 0 {
				t.Fatalf("literal part %d of %q has width %d", index, format, part.Width)
			}
			wantWork += utf8.RuneCountInString(part.Literal)
		case DirectiveSubseconds:
			if part.Width != 3 && part.Width != 6 && part.Width != 9 {
				t.Fatalf("subsecond part %d of %q has width %d", index, format, part.Width)
			}
			if part.Literal != "" {
				t.Fatalf("directive part %d of %q carries literal %q", index, format, part.Literal)
			}
			wantWork++
		default:
			if _, known := timeFormatFuzzSpellings[part.Directive]; !known {
				t.Fatalf("part %d of %q has unknown directive %d", index, format, part.Directive)
			}
			if part.Width != 0 || part.Literal != "" {
				t.Fatalf("directive part %d of %q carries width %d or literal %q", index, format, part.Width, part.Literal)
			}
			wantWork++
		}
	}
	if wantWork == 0 {
		wantWork = 1
	}
	if wantWork != workUnits {
		t.Fatalf("parts of %q charge %d work units, metadata says %d", format, wantWork, workUnits)
	}
}

func timeFormatFuzzOutputBytes(parts []Part) uint64 {
	total := uint64(0)
	for _, part := range parts {
		if part.Directive == DirectiveLiteral {
			total += uint64(len(part.Literal))
			continue
		}
		total += directiveMaximumBytes(part.Directive, part.Width)
	}
	return total
}

// timeFormatFuzzCheckStrptimeComponents re-derives the calendar-completeness
// rule from the parts: exactly one year, month, and day, at most one of each
// clock component, and no component without the coarser one it refines.
func timeFormatFuzzCheckStrptimeComponents(t *testing.T, format string, parts []Part) {
	t.Helper()
	counts := map[string]int{}
	for _, part := range parts {
		switch part.Directive {
		case DirectiveLiteral, DirectivePercent:
		case DirectiveYear:
			counts["year"]++
		case DirectiveMonthNumber:
			counts["month"]++
		case DirectiveDay:
			counts["day"]++
		case DirectiveISODate:
			counts["year"]++
			counts["month"]++
			counts["day"]++
		case DirectiveHour24:
			counts["hour"]++
		case DirectiveHour12:
			counts["hour"]++
			counts["hour12"]++
		case DirectiveTime24:
			counts["hour"]++
			counts["minute"]++
			counts["second"]++
		case DirectiveMinute:
			counts["minute"]++
		case DirectiveSecond:
			counts["second"]++
		case DirectiveAMPM:
			counts["ampm"]++
		case DirectiveSubseconds, DirectiveMicroseconds:
			if part.Directive == DirectiveSubseconds && part.Width == 9 {
				t.Fatalf("strptime %q accepted a nanosecond fraction", format)
			}
			counts["fraction"]++
		case DirectiveTimezoneOffset:
			counts["offset"]++
		default:
			t.Fatalf("strptime %q accepted formatting-only directive %d", format, part.Directive)
		}
	}
	if counts["year"] != 1 || counts["month"] != 1 || counts["day"] != 1 {
		t.Fatalf("strptime %q accepted an incomplete or repeated calendar: %v", format, counts)
	}
	for _, component := range []string{"hour", "minute", "second", "ampm", "fraction", "offset"} {
		if counts[component] > 1 {
			t.Fatalf("strptime %q accepted a repeated %s: %v", format, component, counts)
		}
	}
	if (counts["hour12"] == 1) != (counts["ampm"] == 1) {
		t.Fatalf("strptime %q accepted a 12-hour clock without its meridiem or vice versa: %v", format, counts)
	}
	if counts["minute"] > counts["hour"] || counts["second"] > counts["minute"] || counts["fraction"] > counts["second"] {
		t.Fatalf("strptime %q accepted a clock component without its coarser component: %v", format, counts)
	}
}

// timeFormatFuzzSpellings is the canonical percent spelling of every
// width-less directive; subseconds are rendered as %<width>N.
var timeFormatFuzzSpellings = map[Directive]string{
	DirectivePercent:             "%%",
	DirectiveYear:                "%Y",
	DirectiveYearShort:           "%y",
	DirectiveISOWeekYear:         "%G",
	DirectiveISOWeekYearShort:    "%g",
	DirectiveMonthNumber:         "%m",
	DirectiveMonthShort:          "%b",
	DirectiveMonthLong:           "%B",
	DirectiveDay:                 "%d",
	DirectiveDaySpace:            "%e",
	DirectiveDayOfYear:           "%j",
	DirectiveISOWeek:             "%V",
	DirectiveWeekdayNumber:       "%w",
	DirectiveWeekdayShort:        "%a",
	DirectiveWeekdayLong:         "%A",
	DirectiveHour24:              "%H",
	DirectiveHour12:              "%I",
	DirectiveMinute:              "%M",
	DirectiveSecond:              "%S",
	DirectiveAMPM:                "%p",
	DirectiveTime24:              "%T",
	DirectiveISODate:             "%F",
	DirectiveEpochSeconds:        "%s",
	DirectiveTimezoneOffset:      "%z",
	DirectiveTimezoneOffsetColon: "%:z",
	DirectiveMicroseconds:        "%f",
}

func timeFormatFuzzRender(parts []Part) string {
	var rendered strings.Builder
	for _, part := range parts {
		switch part.Directive {
		case DirectiveLiteral:
			rendered.WriteString(part.Literal)
		case DirectiveSubseconds:
			rendered.WriteByte('%')
			rendered.WriteByte('0' + part.Width)
			rendered.WriteByte('N')
		default:
			rendered.WriteString(timeFormatFuzzSpellings[part.Directive])
		}
	}
	return rendered.String()
}

func TestTimeFormatFuzzSpellingsCoverEveryDirective(t *testing.T) {
	t.Parallel()

	for spelling, directive := range simpleDirectives {
		if directive == DirectiveSubseconds {
			continue
		}
		if got := timeFormatFuzzSpellings[directive]; got != "%"+string(spelling) {
			t.Errorf("directive %d spelled %q, lexer accepts %%%c", directive, got, spelling)
		}
	}
	if len(timeFormatFuzzSpellings) != len(simpleDirectives)-2+1 {
		t.Fatalf("spelling table has %d entries for %d simple directives (%%Q and %%N share one, %%:z is extra)",
			len(timeFormatFuzzSpellings), len(simpleDirectives))
	}
}
