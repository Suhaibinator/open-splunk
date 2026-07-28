// Package spltimeformat validates bounded, locale-stable SPL date/time
// formats before they reach a backend compiler.
package spltimeformat

import (
	"errors"
	"strings"
	"unicode/utf8"
)

const (
	// MaximumStrftimeFormatBytes bounds the authored literal independently of
	// the surrounding SPL source ceiling.
	MaximumStrftimeFormatBytes = 4 << 10
	// MaximumStrftimeWorkUnits bounds literal runes plus directives so future
	// normalization cannot introduce unbounded compiler work.
	MaximumStrftimeWorkUnits = 4 << 10
	// MaximumStrftimeOutputBytes bounds the conservative result of expanding
	// every directive in one call.
	MaximumStrftimeOutputBytes = 16 << 10
)

var (
	ErrInvalidStrftimeFormat  = errors.New("invalid strftime format")
	ErrStrftimeFormatTooLarge = errors.New("strftime format exceeds its resource limit")
	errInvalidTimeFormat      = errors.New("invalid time format")
	errTimeFormatTooLarge     = errors.New("time format exceeds its resource limit")
)

type timeFormatLimits struct {
	maximumFormatBytes int
	maximumWorkUnits   int
}

// Directive identifies one supported, locale-stable SPL strftime variable.
type Directive uint8

const (
	DirectiveLiteral Directive = iota
	DirectivePercent
	DirectiveYear
	DirectiveYearShort
	DirectiveISOWeekYear
	DirectiveISOWeekYearShort
	DirectiveMonthNumber
	DirectiveMonthShort
	DirectiveMonthLong
	DirectiveDay
	DirectiveDaySpace
	DirectiveDayOfYear
	DirectiveISOWeek
	DirectiveWeekdayNumber
	DirectiveWeekdayShort
	DirectiveWeekdayLong
	DirectiveHour24
	DirectiveHour12
	DirectiveMinute
	DirectiveSecond
	DirectiveAMPM
	DirectiveTime24
	DirectiveISODate
	DirectiveEpochSeconds
	DirectiveTimezoneOffset
	DirectiveTimezoneOffsetColon
	DirectiveSubseconds
	DirectiveMicroseconds
)

// Part is either one exact literal or one supported directive. Width is used
// only by DirectiveSubseconds.
type Part struct {
	Directive Directive
	Literal   string
	Width     uint8
}

// StrftimeFormat is one validated format and its conservative resource
// metadata.
type StrftimeFormat struct {
	Parts              []Part
	WorkUnits          int
	MaximumOutputBytes uint64
}

// CompileStrftimeFormat validates a locale-stable, bounded subset of Splunk's
// date/time variables. Locale-dependent variables and unimplemented offset
// variants are rejected rather than delegated to server configuration.
func CompileStrftimeFormat(format string) (StrftimeFormat, error) {
	compiled, err := compileBoundedTimeFormat(format, timeFormatLimits{
		maximumFormatBytes: MaximumStrftimeFormatBytes,
		maximumWorkUnits:   MaximumStrftimeWorkUnits,
	})
	if err != nil {
		if errors.Is(err, errTimeFormatTooLarge) {
			return StrftimeFormat{}, ErrStrftimeFormatTooLarge
		}
		return StrftimeFormat{}, ErrInvalidStrftimeFormat
	}
	if compiled.MaximumOutputBytes > MaximumStrftimeOutputBytes {
		return StrftimeFormat{}, ErrStrftimeFormatTooLarge
	}
	return compiled, nil
}

// compileBoundedTimeFormat lexes the common directive superset under
// caller-owned source and work limits. Output expansion is formatter policy
// and is deliberately enforced only by CompileStrftimeFormat.
func compileBoundedTimeFormat(
	format string,
	limits timeFormatLimits,
) (StrftimeFormat, error) {
	if !utf8.ValidString(format) || strings.IndexByte(format, 0) >= 0 {
		return StrftimeFormat{}, errInvalidTimeFormat
	}
	if len(format) > limits.maximumFormatBytes {
		return StrftimeFormat{}, errTimeFormatTooLarge
	}

	compiled := StrftimeFormat{}
	literalStart := 0
	for offset := 0; offset < len(format); {
		if format[offset] != '%' {
			_, width := utf8.DecodeRuneInString(format[offset:])
			offset += width
			continue
		}
		if literalStart < offset {
			compiled.appendLiteral(format[literalStart:offset])
			if compiled.WorkUnits > limits.maximumWorkUnits {
				return StrftimeFormat{}, errTimeFormatTooLarge
			}
		}
		directive, width, consumed, err := parseDirective(format[offset:])
		if err != nil {
			return StrftimeFormat{}, err
		}
		compiled.appendDirective(directive, width)
		if compiled.WorkUnits > limits.maximumWorkUnits {
			return StrftimeFormat{}, errTimeFormatTooLarge
		}
		offset += consumed
		literalStart = offset
	}
	if literalStart < len(format) {
		compiled.appendLiteral(format[literalStart:])
		if compiled.WorkUnits > limits.maximumWorkUnits {
			return StrftimeFormat{}, errTimeFormatTooLarge
		}
	}
	if compiled.WorkUnits == 0 {
		compiled.WorkUnits = 1
	}
	return compiled, nil
}

func (compiled *StrftimeFormat) appendLiteral(literal string) {
	compiled.Parts = append(compiled.Parts, Part{
		Directive: DirectiveLiteral,
		Literal:   strings.Clone(literal),
	})
	compiled.WorkUnits += utf8.RuneCountInString(literal)
	compiled.MaximumOutputBytes += uint64(len(literal))
}

func (compiled *StrftimeFormat) appendDirective(directive Directive, width uint8) {
	compiled.Parts = append(compiled.Parts, Part{
		Directive: directive,
		Width:     width,
	})
	compiled.WorkUnits++
	compiled.MaximumOutputBytes += directiveMaximumBytes(directive, width)
}

func parseDirective(source string) (Directive, uint8, int, error) {
	if len(source) < 2 || source[0] != '%' {
		return DirectiveLiteral, 0, 0, errInvalidTimeFormat
	}
	if source[1] == ':' {
		if len(source) >= 3 && source[2] == 'z' {
			return DirectiveTimezoneOffsetColon, 0, 3, nil
		}
		return DirectiveLiteral, 0, 0, errInvalidTimeFormat
	}
	if source[1] >= '0' && source[1] <= '9' {
		if len(source) < 3 ||
			(source[1] != '3' && source[1] != '6' && source[1] != '9') ||
			(source[2] != 'Q' && source[2] != 'N') {
			return DirectiveLiteral, 0, 0, errInvalidTimeFormat
		}
		return DirectiveSubseconds, source[1] - '0', 3, nil
	}
	directive, ok := simpleDirectives[source[1]]
	if !ok {
		return DirectiveLiteral, 0, 0, errInvalidTimeFormat
	}
	width := uint8(0)
	switch source[1] {
	case 'Q':
		width = 3
	case 'N':
		width = 9
	}
	return directive, width, 2, nil
}

var simpleDirectives = map[byte]Directive{
	'%': DirectivePercent,
	'Y': DirectiveYear,
	'y': DirectiveYearShort,
	'G': DirectiveISOWeekYear,
	'g': DirectiveISOWeekYearShort,
	'm': DirectiveMonthNumber,
	'b': DirectiveMonthShort,
	'B': DirectiveMonthLong,
	'd': DirectiveDay,
	'e': DirectiveDaySpace,
	'j': DirectiveDayOfYear,
	'V': DirectiveISOWeek,
	'w': DirectiveWeekdayNumber,
	'a': DirectiveWeekdayShort,
	'A': DirectiveWeekdayLong,
	'H': DirectiveHour24,
	'I': DirectiveHour12,
	'M': DirectiveMinute,
	'S': DirectiveSecond,
	'p': DirectiveAMPM,
	'T': DirectiveTime24,
	'F': DirectiveISODate,
	's': DirectiveEpochSeconds,
	'z': DirectiveTimezoneOffset,
	'Q': DirectiveSubseconds,
	'N': DirectiveSubseconds,
	'f': DirectiveMicroseconds,
}

func directiveMaximumBytes(directive Directive, width uint8) uint64 {
	switch directive {
	case DirectivePercent, DirectiveWeekdayNumber:
		return 1
	case DirectiveYearShort, DirectiveISOWeekYearShort, DirectiveMonthNumber,
		DirectiveDay, DirectiveDaySpace, DirectiveISOWeek, DirectiveHour24,
		DirectiveHour12, DirectiveMinute, DirectiveSecond, DirectiveAMPM:
		return 2
	case DirectiveMonthShort, DirectiveWeekdayShort, DirectiveDayOfYear:
		return 3
	case DirectiveYear, DirectiveISOWeekYear:
		return 4
	case DirectiveTimezoneOffset:
		return 5
	case DirectiveTimezoneOffsetColon, DirectiveMicroseconds:
		return 6
	case DirectiveTime24:
		return 8
	case DirectiveMonthLong, DirectiveWeekdayLong:
		return 9
	case DirectiveISODate:
		return 10
	case DirectiveEpochSeconds:
		return 20
	case DirectiveSubseconds:
		return uint64(width)
	default:
		return 0
	}
}
