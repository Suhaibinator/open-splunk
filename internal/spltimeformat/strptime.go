package spltimeformat

import "errors"

const (
	MaximumStrptimeFormatBytes = MaximumStrftimeFormatBytes
	MaximumStrptimeWorkUnits   = MaximumStrftimeWorkUnits
)

var (
	ErrInvalidStrptimeFormat  = errors.New("invalid strptime format")
	ErrStrptimeFormatTooLarge = errors.New("strptime format exceeds its resource limit")
)

// StrptimeFormat is one validated, deterministic full-date parser format.
// Parsing deliberately has a narrower directive set than formatting.
type StrptimeFormat struct {
	Parts             []Part
	WorkUnits         int
	FractionalDigits  uint8
	HasTimezoneOffset bool
}

// CompileStrptimeFormat validates a bounded parsing subset with an explicit
// year, month, and day. Requiring complete calendar input avoids inheriting
// process time, search start, or backend-specific defaults for omitted fields.
func CompileStrptimeFormat(format string) (StrptimeFormat, error) {
	lexed, err := CompileStrftimeFormat(format)
	if err != nil {
		if errors.Is(err, ErrStrftimeFormatTooLarge) {
			return StrptimeFormat{}, ErrStrptimeFormatTooLarge
		}
		return StrptimeFormat{}, ErrInvalidStrptimeFormat
	}

	type componentCounts struct {
		year     int
		month    int
		day      int
		hour24   int
		hour12   int
		minute   int
		second   int
		ampm     int
		fraction int
		offset   int
	}
	var counts componentCounts
	compiled := StrptimeFormat{
		Parts:     lexed.Parts,
		WorkUnits: lexed.WorkUnits,
	}
	for _, part := range lexed.Parts {
		switch part.Directive {
		case DirectiveLiteral, DirectivePercent:
		case DirectiveYear:
			counts.year++
		case DirectiveMonthNumber:
			counts.month++
		case DirectiveDay:
			counts.day++
		case DirectiveHour24:
			counts.hour24++
		case DirectiveHour12:
			counts.hour12++
		case DirectiveMinute:
			counts.minute++
		case DirectiveSecond:
			counts.second++
		case DirectiveAMPM:
			counts.ampm++
		case DirectiveISODate:
			counts.year++
			counts.month++
			counts.day++
		case DirectiveTime24:
			counts.hour24++
			counts.minute++
			counts.second++
		case DirectiveTimezoneOffset:
			counts.offset++
			compiled.HasTimezoneOffset = true
		case DirectiveSubseconds:
			if part.Width != 3 && part.Width != 6 {
				return StrptimeFormat{}, ErrInvalidStrptimeFormat
			}
			counts.fraction++
			compiled.FractionalDigits = part.Width
		case DirectiveMicroseconds:
			counts.fraction++
			compiled.FractionalDigits = 6
		default:
			return StrptimeFormat{}, ErrInvalidStrptimeFormat
		}
	}

	hours := counts.hour24 + counts.hour12
	if counts.year != 1 || counts.month != 1 || counts.day != 1 ||
		hours > 1 || counts.minute > 1 || counts.second > 1 ||
		counts.ampm > 1 || counts.fraction > 1 || counts.offset > 1 ||
		(counts.hour12 == 1) != (counts.ampm == 1) ||
		(counts.hour24 == 1 && counts.ampm != 0) ||
		(counts.minute != 0 && hours == 0) ||
		(counts.second != 0 && counts.minute == 0) ||
		(counts.fraction != 0 && counts.second == 0) {
		return StrptimeFormat{}, ErrInvalidStrptimeFormat
	}
	return compiled, nil
}
