// Package splrelativetime validates the bounded relative-time specifier
// language shared by SPL parsing, logical planning, and backend lowering.
package splrelativetime

import (
	"errors"
	"strconv"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/searchtimebounds"
)

const (
	MaximumSpecifierBytes     = 1 << 10
	MaximumSpecifierWorkUnits = 1 << 10
	MaximumOperations         = 3
)

var (
	ErrInvalidSpecifier    = errors.New("invalid relative-time specifier")
	ErrMagnitudeOutOfRange = errors.New(
		"relative-time magnitude exceeds the representable timestamp domain",
	)
	ErrSpecifierTooLarge = errors.New(
		"relative-time specifier exceeds its resource limit",
	)
)

// Unit is one normalized relative-time offset or snap unit.
type Unit uint8

const (
	UnitSecond Unit = iota + 1
	UnitMinute
	UnitHour
	UnitDay
	UnitWeek
	UnitMonth
	UnitQuarter
	UnitYear
)

// OperationKind distinguishes an offset from a snap-to boundary.
type OperationKind uint8

const (
	OperationOffset OperationKind = iota + 1
	OperationSnap
)

// Operation is one normalized operation in source evaluation order.
// Weekday is meaningful only for a week snap and uses Sunday=0 through
// Saturday=6. The documented w7 spelling is normalized to Sunday.
type Operation struct {
	Kind      OperationKind
	Unit      Unit
	Magnitude uint64
	Negative  bool
	Weekday   uint8
}

// Specifier is one validated, normalized relative-time program.
type Specifier struct {
	Operations     [MaximumOperations]Operation
	OperationCount uint8
	WorkUnits      int
}

// CompileSpecifier accepts one optional signed offset, one optional snap, and
// one optional signed post-snap offset. When the explicit snap is absent,
// Splunk's documented second-boundary snap is appended to the typed program.
func CompileSpecifier(source string) (Specifier, error) {
	if len(source) > MaximumSpecifierBytes {
		return Specifier{}, ErrSpecifierTooLarge
	}
	if source == "" || !utf8.ValidString(source) {
		return Specifier{}, ErrInvalidSpecifier
	}
	for index := range len(source) {
		if source[index] > 0x7f || source[index] == 0 {
			return Specifier{}, ErrInvalidSpecifier
		}
	}
	workUnits := len(source)
	if workUnits > MaximumSpecifierWorkUnits {
		return Specifier{}, ErrSpecifierTooLarge
	}

	var operations [MaximumOperations]Operation
	operationCount := 0
	offset := 0
	if source[offset] == '+' || source[offset] == '-' {
		operation, next, err := parseOffset(source, offset)
		if err != nil {
			return Specifier{}, err
		}
		operations[operationCount] = operation
		operationCount++
		offset = next
	}

	if offset == len(source) {
		if operationCount == 0 {
			return Specifier{}, ErrInvalidSpecifier
		}
		operations[operationCount] = Operation{
			Kind: OperationSnap,
			Unit: UnitSecond,
		}
		operationCount++
		return Specifier{
			Operations:     operations,
			OperationCount: uint8(operationCount),
			WorkUnits:      workUnits,
		}, nil
	}
	if source[offset] != '@' {
		return Specifier{}, ErrInvalidSpecifier
	}
	snap, next, err := parseSnap(source, offset)
	if err != nil {
		return Specifier{}, err
	}
	operations[operationCount] = snap
	operationCount++
	offset = next

	if offset < len(source) {
		if source[offset] != '+' && source[offset] != '-' {
			return Specifier{}, ErrInvalidSpecifier
		}
		post, postEnd, err := parseOffset(source, offset)
		if err != nil {
			return Specifier{}, err
		}
		operations[operationCount] = post
		operationCount++
		offset = postEnd
	}
	if offset != len(source) || operationCount > MaximumOperations {
		return Specifier{}, ErrInvalidSpecifier
	}
	return Specifier{
		Operations:     operations,
		OperationCount: uint8(operationCount),
		WorkUnits:      workUnits,
	}, nil
}

func parseOffset(source string, start int) (Operation, int, error) {
	operation := Operation{
		Kind:     OperationOffset,
		Negative: source[start] == '-',
	}
	offset := start + 1
	digitStart := offset
	for offset < len(source) && asciiDigit(source[offset]) {
		offset++
	}
	unitStart := offset
	for offset < len(source) && asciiLetter(source[offset]) {
		offset++
	}
	unit, ok := normalizeUnit(source[unitStart:offset])
	if !ok {
		return Operation{}, 0, ErrInvalidSpecifier
	}

	if digitStart == unitStart {
		operation.Magnitude = 1
	} else {
		magnitude, err := strconv.ParseUint(source[digitStart:unitStart], 10, 64)
		if err != nil {
			return Operation{}, 0, ErrMagnitudeOutOfRange
		}
		operation.Magnitude = magnitude
	}
	if operation.Magnitude > maximumMagnitude(unit) {
		return Operation{}, 0, ErrMagnitudeOutOfRange
	}
	if operation.Magnitude == 0 {
		operation.Negative = false
	}
	operation.Unit = unit
	return operation, offset, nil
}

func parseSnap(source string, start int) (Operation, int, error) {
	offset := start + 1
	tokenStart := offset
	for offset < len(source) &&
		source[offset] != '+' &&
		source[offset] != '-' &&
		source[offset] != '@' {
		offset++
	}
	token := source[tokenStart:offset]
	if len(token) == 2 && token[0] == 'w' &&
		token[1] >= '0' && token[1] <= '7' {
		weekday := token[1] - '0'
		if weekday == 7 {
			weekday = 0
		}
		return Operation{
			Kind:    OperationSnap,
			Unit:    UnitWeek,
			Weekday: weekday,
		}, offset, nil
	}
	unit, ok := normalizeUnit(token)
	if !ok {
		return Operation{}, 0, ErrInvalidSpecifier
	}
	return Operation{Kind: OperationSnap, Unit: unit}, offset, nil
}

func normalizeUnit(source string) (Unit, bool) {
	switch source {
	case "s", "sec", "secs", "second", "seconds":
		return UnitSecond, true
	case "m", "min", "mins", "minute", "minutes":
		return UnitMinute, true
	case "h", "hr", "hrs", "hour", "hours":
		return UnitHour, true
	case "d", "day", "days":
		return UnitDay, true
	case "w", "week", "weeks":
		return UnitWeek, true
	case "mon", "month", "months":
		return UnitMonth, true
	case "q", "qtr", "qtrs", "quarter", "quarters":
		return UnitQuarter, true
	case "y", "yr", "yrs", "year", "years":
		return UnitYear, true
	default:
		return 0, false
	}
}

func maximumMagnitude(unit Unit) uint64 {
	switch unit {
	case UnitSecond:
		return searchtimebounds.MaximumSpanSeconds
	case UnitMinute:
		return searchtimebounds.MaximumSpanMinutes
	case UnitHour:
		return searchtimebounds.MaximumSpanHours
	case UnitDay:
		return searchtimebounds.MaximumSpanDays
	case UnitWeek:
		return searchtimebounds.MaximumSpanWeeks
	case UnitMonth:
		return searchtimebounds.MaximumSpanMonths
	case UnitQuarter:
		return searchtimebounds.MaximumSpanQuarters
	case UnitYear:
		return searchtimebounds.MaximumSpanYears
	default:
		return 0
	}
}

func asciiDigit(value byte) bool {
	return value >= '0' && value <= '9'
}

func asciiLetter(value byte) bool {
	return value >= 'a' && value <= 'z'
}
