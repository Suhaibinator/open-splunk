// Package searchtime validates and resolves the bounded time-expression subset
// accepted when an executable search job is created. Saved objects may retain
// broader future syntax, but execution never guesses at unsupported semantics.
package searchtime

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
	_ "time/tzdata"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

const (
	MaximumExpressionBytes = 1_024
	MaximumTimezoneBytes   = 255
)

var (
	locationCache    sync.Map
	errLocalTimezone = errors.New("timezone must not depend on the server's local configuration")
)

// Intent is the normalized reusable time-range input retained separately from
// the resolved execution interval. Timezone is always effective; the presence
// bit lets API adapters preserve whether the user supplied it.
type Intent struct {
	Earliest          string
	Latest            string
	Timezone          string
	TimezoneSpecified bool
}

// Range is one immutable half-open execution interval and its reusable intent.
// Its fields are deliberately private so intent and resolved bounds cannot be
// separated or changed after successful resolution.
type Range struct {
	intent   Intent
	earliest time.Time
	latest   time.Time
	valid    bool
}

// Intent returns the normalized reusable input that produced the range.
func (resolved Range) Intent() Intent { return resolved.intent }

// Earliest returns the inclusive UTC boundary.
func (resolved Range) Earliest() time.Time { return resolved.earliest }

// Latest returns the exclusive UTC boundary.
func (resolved Range) Latest() time.Time { return resolved.latest }

// Valid reports whether the range was produced by this package's checked
// constructors. The zero value is invalid.
func (resolved Range) Valid() bool { return resolved.valid }

// Resolve accepts strict RFC3339/RFC3339Nano timestamps, now, and offsets of
// the form -N[s|m|h|d]. Both expressions share one anchor. Seconds, minutes,
// and hours are elapsed durations; days are calendar days in the effective
// timezone, matching Splunk's distinction between -1d and -24h across DST.
func Resolve(earliest, latest string, timezone *string, now time.Time) (Range, error) {
	intent, err := normalizeIntent(earliest, latest, timezone)
	if err != nil {
		return Range{}, err
	}
	parsed, err := parseIntent(intent)
	if err != nil {
		return Range{}, err
	}
	anchor := now.Round(0).UTC()
	resolvedEarliest, err := resolveExpression(parsed.earliest, anchor, parsed.location)
	if err != nil {
		return Range{}, err
	}
	resolvedLatest, err := resolveExpression(parsed.latest, anchor, parsed.location)
	if err != nil {
		return Range{}, err
	}
	return newRange(intent, resolvedEarliest, resolvedLatest)
}

// NewAbsoluteRange creates a checked UTC range for trusted callers that
// already hold absolute boundaries. The retained intent uses canonical RFC
// 3339 nanosecond spellings and an implicit UTC timezone.
func NewAbsoluteRange(earliest, latest time.Time) (Range, error) {
	resolvedEarliest := earliest.Round(0).UTC()
	resolvedLatest := latest.Round(0).UTC()
	intent := Intent{
		Earliest: resolvedEarliest.Format(time.RFC3339Nano),
		Latest:   resolvedLatest.Format(time.RFC3339Nano),
		Timezone: "UTC",
	}
	return newRange(intent, resolvedEarliest, resolvedLatest)
}

func newRange(intent Intent, earliest, latest time.Time) (Range, error) {
	if !clickhouse.SupportsSearchTimeRange(earliest, latest) {
		return Range{}, errors.New("time range is outside the supported ClickHouse DateTime64 range")
	}
	if !earliest.Before(latest) {
		return Range{}, errors.New("earliest must be before latest")
	}
	return Range{intent: intent, earliest: earliest, latest: latest, valid: true}, nil
}

// ValidateIntent verifies that intent is already canonical and uses only the
// executable time-expression subset. It deliberately does not resolve or
// compare the expressions because relative values require the caller's single
// captured anchor; Resolve performs those checks.
func ValidateIntent(intent Intent) error {
	_, err := parseIntent(intent)
	return err
}

type parsedIntent struct {
	earliest parsedExpression
	latest   parsedExpression
	location *time.Location
}

func parseIntent(intent Intent) (parsedIntent, error) {
	earliest, err := parseCanonicalExpression("earliest", intent.Earliest)
	if err != nil {
		return parsedIntent{}, err
	}
	latest, err := parseCanonicalExpression("latest", intent.Latest)
	if err != nil {
		return parsedIntent{}, err
	}
	if intent.Timezone == "" || len(intent.Timezone) > MaximumTimezoneBytes ||
		!utf8.ValidString(intent.Timezone) || strings.TrimSpace(intent.Timezone) != intent.Timezone {
		return parsedIntent{}, errors.New("timezone is invalid")
	}
	if !intent.TimezoneSpecified && intent.Timezone != "UTC" {
		return parsedIntent{}, errors.New("unspecified timezone must use UTC")
	}
	location, err := loadLocation(intent.Timezone)
	if err != nil {
		if errors.Is(err, errLocalTimezone) {
			return parsedIntent{}, err
		}
		return parsedIntent{}, errors.New("timezone is invalid")
	}
	return parsedIntent{earliest: earliest, latest: latest, location: location}, nil
}

func normalizeIntent(earliest, latest string, timezone *string) (Intent, error) {
	if len(earliest) > MaximumExpressionBytes {
		return Intent{}, errors.New("earliest time expression is invalid")
	}
	if len(latest) > MaximumExpressionBytes {
		return Intent{}, errors.New("latest time expression is invalid")
	}
	if timezone != nil && len(*timezone) > MaximumTimezoneBytes {
		return Intent{}, errors.New("timezone is invalid")
	}
	intent := Intent{
		Earliest: strings.Clone(strings.TrimSpace(earliest)),
		Latest:   strings.Clone(strings.TrimSpace(latest)),
		Timezone: "UTC",
	}
	if timezone != nil {
		intent.TimezoneSpecified = true
		intent.Timezone = strings.Clone(strings.TrimSpace(*timezone))
	}
	return intent, nil
}

func loadLocation(name string) (*time.Location, error) {
	if name == "Local" {
		return nil, errLocalTimezone
	}
	if name == "UTC" {
		return time.UTC, nil
	}
	if cached, ok := locationCache.Load(name); ok {
		return cached.(*time.Location), nil
	}
	location, err := time.LoadLocation(name)
	if err != nil {
		return nil, err
	}
	// A short timezone may be a substring of a much larger request buffer.
	// Clone the retained key so the process-wide cache never pins that buffer.
	cached, _ := locationCache.LoadOrStore(strings.Clone(name), location)
	return cached.(*time.Location), nil
}

type expressionKind uint8

const (
	expressionNow expressionKind = iota
	expressionRelative
	expressionAbsolute
)

type parsedExpression struct {
	kind     expressionKind
	relative relativeOffset
	absolute time.Time
	name     string
}

func resolveExpression(expression parsedExpression, now time.Time, location *time.Location) (time.Time, error) {
	switch expression.kind {
	case expressionNow:
		return now, nil
	case expressionRelative:
		shifted, err := resolveRelative(expression.relative, now, location)
		if err != nil {
			return time.Time{}, fmt.Errorf("%s relative time expression is invalid", expression.name)
		}
		return shifted, nil
	case expressionAbsolute:
		return expression.absolute, nil
	default:
		return time.Time{}, fmt.Errorf("%s time expression is invalid", expression.name)
	}
}

func parseCanonicalExpression(name, expression string) (parsedExpression, error) {
	if expression == "" {
		return parsedExpression{}, fmt.Errorf("%s time expression is required", name)
	}
	if len(expression) > MaximumExpressionBytes || !utf8.ValidString(expression) ||
		strings.TrimSpace(expression) != expression {
		return parsedExpression{}, fmt.Errorf("%s time expression is invalid", name)
	}
	if expression == "now" {
		return parsedExpression{kind: expressionNow, name: name}, nil
	}
	if offset, relative, err := parseRelativeOffset(expression); relative {
		if err != nil {
			return parsedExpression{}, fmt.Errorf("%s relative time expression is invalid", name)
		}
		return parsedExpression{kind: expressionRelative, relative: offset, name: name}, nil
	}
	if !validRFC3339Nano(expression) {
		return parsedExpression{}, fmt.Errorf("%s must be RFC 3339, now, or -N[s|m|h|d]", name)
	}
	parsed, err := time.Parse(time.RFC3339Nano, expression)
	if err != nil {
		return parsedExpression{}, fmt.Errorf("%s RFC 3339 timestamp is invalid", name)
	}
	return parsedExpression{kind: expressionAbsolute, absolute: parsed.Round(0).UTC(), name: name}, nil
}

type relativeOffset struct {
	calendarDays   int
	elapsedSeconds int64
}

func resolveRelative(offset relativeOffset, now time.Time, location *time.Location) (time.Time, error) {
	if offset.calendarDays != 0 {
		return now.In(location).AddDate(0, 0, -offset.calendarDays).UTC(), nil
	}
	anchorSeconds := now.Unix()
	if anchorSeconds < math.MinInt64+offset.elapsedSeconds {
		return time.Time{}, errors.New("relative offset underflows time")
	}
	return time.Unix(anchorSeconds-offset.elapsedSeconds, int64(now.Nanosecond())).UTC(), nil
}

func parseRelativeOffset(expression string) (relativeOffset, bool, error) {
	if len(expression) < 3 || expression[0] != '-' {
		return relativeOffset{}, false, nil
	}
	unit := expression[len(expression)-1]
	switch unit {
	case 's', 'm', 'h', 'd':
	default:
		return relativeOffset{}, false, nil
	}
	digits := expression[1 : len(expression)-1]
	for index := range len(digits) {
		if digits[index] < '0' || digits[index] > '9' {
			return relativeOffset{}, true, errors.New("relative magnitude is not decimal")
		}
	}
	amount, err := strconv.ParseUint(digits, 10, 64)
	if err != nil || amount == 0 {
		return relativeOffset{}, true, errors.New("relative magnitude is outside the supported range")
	}
	if unit == 'd' {
		if amount > math.MaxInt32 {
			return relativeOffset{}, true, errors.New("relative calendar offset is outside the supported range")
		}
		return relativeOffset{calendarDays: int(amount)}, true, nil
	}
	unitSeconds := uint64(1)
	if unit == 'm' {
		unitSeconds = 60
	} else if unit == 'h' {
		unitSeconds = 60 * 60
	}
	if amount > uint64(math.MaxInt64)/unitSeconds {
		return relativeOffset{}, true, errors.New("relative offset is outside the supported range")
	}
	return relativeOffset{elapsedSeconds: int64(amount * unitSeconds)}, true, nil
}

func validRFC3339Nano(value string) bool {
	if len(value) < len("0001-01-01T00:00:00Z") ||
		value[4] != '-' || value[7] != '-' || value[10] != 'T' ||
		value[13] != ':' || value[16] != ':' {
		return false
	}
	for _, index := range [...]int{0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18} {
		if !asciiDigit(value[index]) {
			return false
		}
	}

	zoneStart := 19
	if value[zoneStart] == '.' {
		fractionStart := zoneStart + 1
		zoneStart = fractionStart
		for zoneStart < len(value) && asciiDigit(value[zoneStart]) {
			zoneStart++
		}
		fractionDigits := zoneStart - fractionStart
		if fractionDigits < 1 || fractionDigits > 9 {
			return false
		}
	}
	if zoneStart == len(value)-1 && value[zoneStart] == 'Z' {
		return true
	}
	if len(value)-zoneStart != len("+00:00") ||
		(value[zoneStart] != '+' && value[zoneStart] != '-') ||
		value[zoneStart+3] != ':' ||
		!asciiDigit(value[zoneStart+1]) ||
		!asciiDigit(value[zoneStart+2]) ||
		!asciiDigit(value[zoneStart+4]) ||
		!asciiDigit(value[zoneStart+5]) {
		return false
	}
	hour := int(value[zoneStart+1]-'0')*10 + int(value[zoneStart+2]-'0')
	minute := int(value[zoneStart+4]-'0')*10 + int(value[zoneStart+5]-'0')
	return hour <= 23 && minute <= 59
}

func asciiDigit(value byte) bool { return value >= '0' && value <= '9' }
