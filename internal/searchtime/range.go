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
	"time"
	"unicode/utf8"

	"fortio.org/safecast"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/ianatimezone"
	"github.com/Suhaibinator/open-splunk/internal/searchtimebounds"
)

const (
	MaximumExpressionBytes = 1_024
	MaximumTimezoneBytes   = ianatimezone.MaximumNameBytes
)

var errLocalTimezone = ianatimezone.ErrLocal

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

// Resolve accepts strict RFC3339/RFC3339Nano timestamps, now, offsets of the
// form -N[s|m|h|d], and the bounded day snaps @d and -Nd@d. The exact earliest
// expression 0 means the earliest backend-representable event time and is
// rejected as latest. Both expressions share one anchor. Seconds, minutes,
// and hours are elapsed durations; days and day snaps use the effective IANA
// timezone, matching Splunk's calendar behavior across DST.
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
	earliest, err := parseCanonicalExpression(
		expressionEndpointEarliest,
		"earliest",
		intent.Earliest,
	)
	if err != nil {
		return parsedIntent{}, err
	}
	latest, err := parseCanonicalExpression(
		expressionEndpointLatest,
		"latest",
		intent.Latest,
	)
	if err != nil {
		return parsedIntent{}, err
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
	return ianatimezone.Load(name)
}

type expressionKind uint8

const (
	expressionNow expressionKind = iota
	expressionRelative
	expressionAbsolute
	expressionEarliestData
)

type expressionEndpoint uint8

const (
	expressionEndpointEarliest expressionEndpoint = iota + 1
	expressionEndpointLatest
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
	case expressionEarliestData:
		return clickhouse.MinimumSearchTime(), nil
	default:
		return time.Time{}, fmt.Errorf("%s time expression is invalid", expression.name)
	}
}

func parseCanonicalExpression(
	endpoint expressionEndpoint,
	name string,
	expression string,
) (parsedExpression, error) {
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
	if expression == "0" {
		if endpoint != expressionEndpointEarliest {
			return parsedExpression{}, errors.New(
				"latest cannot use 0 because earliest-data is only an earliest boundary",
			)
		}
		return parsedExpression{kind: expressionEarliestData, name: name}, nil
	}
	if offset, relative, err := parseRelativeOffset(expression); relative {
		if err != nil {
			return parsedExpression{}, fmt.Errorf("%s relative time expression is invalid", name)
		}
		return parsedExpression{kind: expressionRelative, relative: offset, name: name}, nil
	}
	if !searchtimebounds.ValidRFC3339Nano(expression) {
		return parsedExpression{}, fmt.Errorf(
			"%s must be RFC 3339, now, -N[s|m|h|d], @d, or -Nd@d",
			name,
		)
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
	snapToDay      bool
}

func resolveRelative(offset relativeOffset, now time.Time, location *time.Location) (time.Time, error) {
	if offset.snapToDay {
		return resolveSnappedDay(offset.calendarDays, now, location)
	}
	if offset.calendarDays != 0 {
		return now.In(location).AddDate(0, 0, -offset.calendarDays).UTC(), nil
	}
	anchorSeconds := now.Unix()
	if anchorSeconds < math.MinInt64+offset.elapsedSeconds {
		return time.Time{}, errors.New("relative offset underflows time")
	}
	return time.Unix(anchorSeconds-offset.elapsedSeconds, int64(now.Nanosecond())).UTC(), nil
}

func resolveSnappedDay(
	calendarDays int,
	now time.Time,
	location *time.Location,
) (time.Time, error) {
	localAnchor := now.In(location)
	// Compute the authored civil target independently of timezone transitions.
	// Calling AddDate directly in a zone can silently normalize a skipped civil
	// date such as Pacific/Apia 2011-12-30 onto a different day.
	target := time.Date(
		localAnchor.Year(),
		localAnchor.Month(),
		localAnchor.Day()-calendarDays,
		0,
		0,
		0,
		0,
		time.UTC,
	)
	snapped := time.Date(
		target.Year(),
		target.Month(),
		target.Day(),
		0,
		0,
		0,
		0,
		location,
	)
	if !sameLocalMidnight(snapped, target, location) {
		return time.Time{}, errors.New("snapped local day does not have a valid midnight")
	}
	// time.Date deliberately makes no promise about which side of an
	// ambiguous local time it returns. A few IANA zones moved clocks backward
	// at 00:00, producing two midnights. Inspect the adjacent constant-offset
	// intervals and select the first matching instant so @d always means the
	// start of the civil day rather than silently dropping the folded hour.
	start, end := snapped.ZoneBounds()
	for _, adjacent := range [...]time.Time{start, end} {
		if adjacent.IsZero() {
			continue
		}
		probe := adjacent
		if adjacent.Equal(start) {
			probe = adjacent.Add(-time.Nanosecond)
		}
		_, offset := probe.In(location).Zone()
		candidate := target.Add(-time.Duration(offset) * time.Second)
		if sameLocalMidnight(candidate, target, location) && candidate.Before(snapped) {
			snapped = candidate
		}
	}
	if snapped.After(now) {
		return time.Time{}, errors.New("snapped local day does not have a valid midnight")
	}
	return snapped.UTC(), nil
}

func sameLocalMidnight(candidate, target time.Time, location *time.Location) bool {
	roundTrip := candidate.In(location)
	return roundTrip.Year() == target.Year() &&
		roundTrip.Month() == target.Month() &&
		roundTrip.Day() == target.Day() &&
		roundTrip.Hour() == 0 &&
		roundTrip.Minute() == 0 &&
		roundTrip.Second() == 0 &&
		roundTrip.Nanosecond() == 0
}

func parseRelativeOffset(expression string) (relativeOffset, bool, error) {
	if expression == "@d" {
		return relativeOffset{snapToDay: true}, true, nil
	}
	const daySnapSuffix = "d@d"
	if strings.HasPrefix(expression, "-") &&
		strings.HasSuffix(expression, daySnapSuffix) {
		digits := expression[1 : len(expression)-len(daySnapSuffix)]
		if digits == "" {
			return relativeOffset{}, true, errors.New(
				"snapped-day magnitude is required",
			)
		}
		for index := range len(digits) {
			if digits[index] < '0' || digits[index] > '9' {
				return relativeOffset{}, true, errors.New(
					"snapped-day magnitude is not decimal",
				)
			}
		}
		amount, err := strconv.ParseUint(digits, 10, 64)
		if err != nil || amount == 0 ||
			amount > searchtimebounds.MaximumSpanDays {
			return relativeOffset{}, true, errors.New(
				"snapped-day magnitude is outside the supported range",
			)
		}
		return relativeOffset{

			calendarDays: safecast.MustConv[int](amount),
			snapToDay:    true,
		}, true, nil
	}
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
	var unitSeconds uint64
	switch unit {
	case 'm':
		unitSeconds = 60
	case 'h':
		unitSeconds = 60 * 60
	default:
		unitSeconds = 1
	}
	if amount > uint64(math.MaxInt64)/unitSeconds {
		return relativeOffset{}, true, errors.New("relative offset is outside the supported range")
	}

	return relativeOffset{elapsedSeconds: safecast.MustConv[int64](amount * unitSeconds)}, true, nil
}
