package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math/bits"
	"regexp"
	"strings"
	"time"

	"fortio.org/safecast"
	"github.com/robfig/cron/v3"
)

const (
	maximumCronExpressionBytes = 255
	// Catch-up walks fixed-size absolute-time windows and counts matching wall
	// minutes from the parsed cron bitsets. The bound covers more than a century
	// while preventing corrupt persisted timestamps from causing unbounded work.
	maximumCatchUpHourWindows = 1 << 20
	maximumTimezoneBytes      = 255
	cronStarBit               = uint64(1) << 63
)

var numericCronField = regexp.MustCompile(`^[0-9*/?,\-]+$`)

// CronSchedule is one validated numeric five-field cron expression evaluated
// in an IANA timezone. Next and Period return UTC instants so callers do not
// accidentally persist ambiguous local wall times across DST transitions.
type CronSchedule struct {
	expression string
	location   *time.Location
	schedule   cron.Schedule
	spec       *cron.SpecSchedule
	timezone   string
}

// ParseCron accepts exactly minute, hour, day-of-month, month, and day-of-week
// fields. Named months/weekdays, seconds, descriptors, and CRON_TZ prefixes are
// deliberately rejected to keep persisted syntax Splunk-compatible.
func ParseCron(expression, timezone string) (CronSchedule, error) {
	expression = strings.TrimSpace(expression)
	timezone = strings.TrimSpace(timezone)
	if expression == "" || len(expression) > maximumCronExpressionBytes {
		return CronSchedule{}, errors.New("cron expression must contain 1 to 255 bytes")
	}
	fields := strings.Fields(expression)
	if len(fields) != 5 {
		return CronSchedule{}, errors.New("cron expression must contain exactly five fields")
	}
	for _, field := range fields {
		if !numericCronField.MatchString(field) {
			return CronSchedule{}, errors.New("cron fields must use numeric five-field syntax")
		}
	}
	if timezone == "" || len(timezone) > maximumTimezoneBytes {
		return CronSchedule{}, errors.New("timezone must contain 1 to 255 bytes")
	}
	location, err := time.LoadLocation(timezone)
	if err != nil {
		return CronSchedule{}, fmt.Errorf("load IANA timezone: %w", err)
	}
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	parsed, err := parser.Parse(strings.Join(fields, " "))
	if err != nil {
		return CronSchedule{}, fmt.Errorf("parse cron expression: %w", err)
	}
	spec, ok := parsed.(*cron.SpecSchedule)
	if !ok {
		return CronSchedule{}, errors.New("cron parser returned an unsupported schedule")
	}
	return CronSchedule{
		expression: strings.Join(fields, " "),
		location:   location,
		schedule:   parsed,
		spec:       spec,
		timezone:   timezone,
	}, nil
}

// Expression returns the canonical expression.
func (schedule CronSchedule) Expression() string { return schedule.expression }

// Timezone returns the configured IANA timezone name.
func (schedule CronSchedule) Timezone() string { return schedule.timezone }

// Next returns the first occurrence strictly after the supplied instant.
func (schedule CronSchedule) Next(after time.Time) time.Time {
	if schedule.schedule == nil || schedule.location == nil {
		return time.Time{}
	}
	return schedule.schedule.Next(after.In(schedule.location)).UTC()
}

// Period resolves Splunk's p unit for claimed by measuring the real elapsed
// interval to the next cron occurrence. It therefore naturally reflects DST.
func (schedule CronSchedule) Period(claimed time.Time) (time.Duration, error) {
	next := schedule.Next(claimed)
	if next.IsZero() || !next.After(claimed) {
		return 0, errors.New("cron schedule has no next occurrence")
	}
	return next.Sub(claimed), nil
}

// AdvancePast identifies the latest due occurrence and the first future
// occurrence. skipped is the number of earlier due occurrences discarded by
// the report missed-period policy.
func (schedule CronSchedule) AdvancePast(firstDue, now time.Time) (latestDue, next time.Time, skipped uint64, err error) {
	return schedule.AdvancePastContext(context.Background(), firstDue, now)
}

// AdvancePastContext is the cancellation-aware catch-up calculation. It
// counts cron matches in hour-sized windows rather than calling Next once per
// missed occurrence, so an every-minute schedule delayed for years remains
// bounded by elapsed hours rather than elapsed minutes.
func (schedule CronSchedule) AdvancePastContext(ctx context.Context, firstDue, now time.Time) (latestDue, next time.Time, skipped uint64, err error) {
	if ctx == nil {
		return time.Time{}, time.Time{}, 0, errors.New("cron catch-up context is required")
	}
	if err := ctx.Err(); err != nil {
		return time.Time{}, time.Time{}, 0, err
	}
	if firstDue.IsZero() || now.IsZero() {
		return time.Time{}, time.Time{}, 0, errors.New("due and current times are required")
	}
	if schedule.spec == nil || schedule.location == nil {
		return time.Time{}, time.Time{}, 0, errors.New("cron schedule is unavailable")
	}
	firstDue = firstDue.UTC()
	now = now.UTC()
	if firstDue.After(now) {
		return time.Time{}, firstDue, 0, nil
	}
	localFirstDue := firstDue.In(schedule.location)
	if localFirstDue.Second() != 0 || localFirstDue.Nanosecond() != 0 {
		return time.Time{}, time.Time{}, 0, errors.New("five-field cron occurrence is not minute aligned")
	}
	lastGrid := firstDue.Add(now.Sub(firstDue).Truncate(time.Minute))
	hourWindows := lastGrid.Sub(firstDue)/time.Hour + 1
	if hourWindows > maximumCatchUpHourWindows {
		return time.Time{}, time.Time{}, 0, errors.New("cron catch-up range exceeds the supported bound")
	}

	var occurrences uint64
	for windowStart := firstDue; !windowStart.After(lastGrid); windowStart = windowStart.Add(time.Hour) {
		if err := ctx.Err(); err != nil {
			return time.Time{}, time.Time{}, 0, err
		}
		windowEnd := windowStart.Add(59 * time.Minute)
		if lastGrid.Before(windowEnd) {
			windowEnd = lastGrid
		}
		count, last, countErr := schedule.countWindow(windowStart, windowEnd)
		if countErr != nil {
			return time.Time{}, time.Time{}, 0, countErr
		}
		occurrences += count
		if !last.IsZero() {
			latestDue = last
		}
	}
	if occurrences == 0 || latestDue.IsZero() {
		return time.Time{}, time.Time{}, 0, errors.New("due time does not match cron schedule")
	}
	next = schedule.Next(latestDue)
	if next.IsZero() || !next.After(latestDue) {
		return time.Time{}, time.Time{}, 0, errors.New("cron schedule did not advance")
	}
	return latestDue, next, occurrences - 1, nil
}

func (schedule CronSchedule) countWindow(start, end time.Time) (uint64, time.Time, error) {
	localStart := start.In(schedule.location)
	localEnd := end.In(schedule.location)
	if wallMinute(localEnd)-wallMinute(localStart) != int64(end.Sub(start)/time.Minute) {
		return schedule.countTransitionWindow(start, end)
	}

	var count uint64
	var last time.Time
	remaining := int(end.Sub(start)/time.Minute) + 1
	cursor := start
	for remaining > 0 {
		local := cursor.In(schedule.location)
		segmentLength := min(remaining, 60-local.Minute())
		if schedule.matchesDateHour(local) {
			mask := minuteRangeMask(local.Minute(), local.Minute()+segmentLength-1) & schedule.spec.Minute
			count += safecast.MustConv[uint64](bits.OnesCount64(mask))
			if mask != 0 {
				lastMinute := bits.Len64(mask) - 1
				last = cursor.Add(time.Duration(lastMinute-local.Minute()) * time.Minute)
			}
		}
		cursor = cursor.Add(time.Duration(segmentLength) * time.Minute)
		remaining -= segmentLength
	}
	return count, last, nil
}

func (schedule CronSchedule) countTransitionWindow(start, end time.Time) (uint64, time.Time, error) {
	var count uint64
	var last time.Time
	for candidate := start; !candidate.After(end); candidate = candidate.Add(time.Minute) {
		if schedule.matches(candidate.In(schedule.location)) {
			count++
			last = candidate
		}
	}
	return count, last, nil
}

func (schedule CronSchedule) matches(value time.Time) bool {
	return value.Second() == 0 && schedule.matchesDateHour(value) && schedule.spec.Minute&(uint64(1)<<uint(value.Minute())) != 0
}

func (schedule CronSchedule) matchesDateHour(value time.Time) bool {
	if schedule.spec.Month&(uint64(1)<<uint(value.Month())) == 0 || schedule.spec.Hour&(uint64(1)<<uint(value.Hour())) == 0 {
		return false
	}
	domMatch := schedule.spec.Dom&(uint64(1)<<uint(value.Day())) != 0
	dowMatch := schedule.spec.Dow&(uint64(1)<<uint(value.Weekday())) != 0
	if schedule.spec.Dom&cronStarBit != 0 || schedule.spec.Dow&cronStarBit != 0 {
		return domMatch && dowMatch
	}
	return domMatch || dowMatch
}

func minuteRangeMask(first, last int) uint64 {
	upper := (uint64(1) << uint(last+1)) - 1
	lower := (uint64(1) << uint(first)) - 1
	return upper &^ lower
}

func wallMinute(value time.Time) int64 {
	return time.Date(value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), 0, 0, time.UTC).Unix() / 60
}
