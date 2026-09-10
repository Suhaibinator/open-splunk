package plan

import (
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"fortio.org/safecast"

	"github.com/Suhaibinator/open-splunk/internal/ianatimezone"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/splrelativetime"
)

// ResolveTimechartGrid resolves a complete dense grid before result validation.
// Runtime input extents use a latest-exclusive endpoint one nanosecond beyond
// the last observed event. SearchEarliest/SearchLatest retain the original bounds.
func ResolveTimechartGrid(op *Timechart, earliest, latest time.Time, timezone string) error {
	if op == nil || !earliest.Before(latest) {
		return fmt.Errorf("timechart requires a nonempty input extent")
	}
	location, err := ianatimezone.Load(timezone)
	if err != nil {
		return err
	}
	candidates := []spl.TimeSpan{op.AuthoredSpan}
	bins, err := validateTimechartAxisOptions(op.Axis, op.Range)
	if err != nil {
		return err
	}
	if op.AuthoredSpan == (spl.TimeSpan{}) {
		candidates = nil
		for _, step := range AutomaticTimeSpanSteps() {
			candidates = append(candidates, automaticTimeSpanAsSPL(step, op.Range))
		}
		for _, magnitude := range []uint64{2, 3, 6, 12, 24, 60, 120, 240, 600, 1200, 2400, 6000} {
			candidates = append(candidates, spl.TimeSpan{Magnitude: magnitude, Unit: spl.TimeSpanUnitMonth, Range: op.Range})
		}
	}
	for _, candidate := range candidates {
		if op.AuthoredSpan == (spl.TimeSpan{}) && op.Axis.MinSpanSpecified && !automaticTimeSpanAtLeast(candidate, op.Axis.MinSpan) {
			continue
		}
		span, calendar, magnitude, spanErr := enhancedTimechartSpan(candidate)
		if spanErr != nil {
			return spanErr
		}
		boundaries, gridErr := timechartBoundarySequence(earliest, latest, span, calendar, magnitude, op.Alignment, location)
		if gridErr != nil {
			if op.AuthoredSpan == (spl.TimeSpan{}) {
				continue
			}
			return &Diagnostic{Code: "SPL_QUERY_TOO_COMPLEX", Message: gridErr.Error(), Range: op.Range}
		}
		if op.AuthoredSpan == (spl.TimeSpan{}) && safecast.MustConv[uint64](len(boundaries)-1) > bins {
			continue
		}
		op.Span, op.Calendar, op.CalendarMagnitude = span, calendar, magnitude
		op.GridBoundaries, op.FirstBucket, op.BucketCount = boundaries, boundaries[0], safecast.MustConv[uint64](len(boundaries)-1)
		return nil
	}
	return &Diagnostic{Code: "SPL_UNSUPPORTED_TIMECHART_SPAN", Message: "timechart cannot select an automatic span for the input extent", Range: op.Range}
}

func enhancedTimechartSpan(authored spl.TimeSpan) (time.Duration, CalendarUnit, uint64, error) {
	magnitude := authored.Magnitude
	if magnitude == 0 {
		return 0, CalendarNone, 0, fmt.Errorf("timechart span magnitude must be positive")
	}
	switch authored.Unit {
	case spl.TimeSpanUnitQuarter, spl.TimeSpanUnitYear:
		multiplier := uint64(3)
		if authored.Unit == spl.TimeSpanUnitYear {
			multiplier = 12
		}
		if magnitude > math.MaxInt32/multiplier {
			return 0, CalendarNone, 0, fmt.Errorf("timechart calendar magnitude overflows")
		}
		return 0, CalendarMonth, magnitude * multiplier, nil
	case spl.TimeSpanUnitDay, spl.TimeSpanUnitWeek, spl.TimeSpanUnitMonth:
		if magnitude > math.MaxInt32/7 {
			return 0, CalendarNone, 0, fmt.Errorf("timechart calendar magnitude overflows")
		}
		calendar, _ := calendarUnit(authored.Unit)
		return 0, calendar, magnitude, nil
	}
	var unit time.Duration
	switch authored.Unit {
	case spl.TimeSpanUnitMicrosecond:
		unit = time.Microsecond
	case spl.TimeSpanUnitMillisecond:
		unit = time.Millisecond
	case spl.TimeSpanUnitCentisecond:
		unit = 10 * time.Millisecond
	case spl.TimeSpanUnitDecisecond:
		unit = 100 * time.Millisecond
	case spl.TimeSpanUnitSecond:
		unit = time.Second
	case spl.TimeSpanUnitMinute:
		unit = time.Minute
	case spl.TimeSpanUnitHour:
		unit = time.Hour
	default:
		return 0, CalendarNone, 0, fmt.Errorf("unsupported timechart span unit")
	}
	if magnitude > uint64(math.MaxInt64)/uint64(unit) {
		return 0, CalendarNone, 0, fmt.Errorf("timechart span overflows")
	}
	span := time.Duration(safecast.MustConv[int64](magnitude)) * unit
	if unit < time.Second && (span >= time.Second || time.Second%span != 0) {
		return 0, CalendarNone, 0, fmt.Errorf("timechart subsecond span must be below and divide one second")
	}
	return span, CalendarNone, 0, nil
}

func timechartBoundarySequence(earliest, latest time.Time, span time.Duration, calendar CalendarUnit, magnitude uint64, alignment time.Time, location *time.Location) ([]time.Time, error) {
	if !time.Unix(0, earliest.UnixNano()).Equal(earliest) || !time.Unix(0, latest.UnixNano()).Equal(latest) || (!alignment.IsZero() && !time.Unix(0, alignment.UnixNano()).Equal(alignment)) {
		return nil, fmt.Errorf("timechart range exceeds exact timestamp domain")
	}
	var first time.Time
	var advance func(time.Time) time.Time
	if calendar == CalendarNone {
		anchor := int64(0)
		if !alignment.IsZero() {
			anchor = alignment.UnixNano()
		}
		delta := new(big.Int).Sub(big.NewInt(earliest.UnixNano()), big.NewInt(anchor))
		quotient := new(big.Int).Div(delta, big.NewInt(int64(span)))
		ticks := quotient.Mul(quotient, big.NewInt(int64(span))).Add(quotient, big.NewInt(anchor))
		if !ticks.IsInt64() {
			return nil, fmt.Errorf("timechart boundary exceeds timestamp range")
		}
		first = time.Unix(0, ticks.Int64()).UTC()
		advance = func(value time.Time) time.Time { return value.Add(span) }
	} else {
		local := earliest.In(location)
		origin := time.Date(1970, 1, 1, 0, 0, 0, 0, location)
		step := safecast.MustConv[int64](magnitude)
		if calendar == CalendarWeek {
			origin = time.Date(1969, 12, 28, 0, 0, 0, 0, location)
			step *= 7
			if !alignment.IsZero() {
				origin = alignment.In(location)
			}
		}
		if calendar == CalendarMonth {
			months := int64(local.Year()-1970)*12 + int64(local.Month()-1)
			endLocal := latest.In(location)
			extentMonths := int64(endLocal.Year()-local.Year())*12 + int64(endLocal.Month()) - int64(local.Month())
			if extentMonths/step > maxTimechartBuckets {
				return nil, fmt.Errorf("timechart produces more than %d buckets", maxTimechartBuckets)
			}
			first = origin.AddDate(0, int(floorInt64(months, step)*step), 0)
			advance = func(value time.Time) time.Time { return value.AddDate(0, safecast.MustConv[int](magnitude), 0) }
		} else {
			civil := func(value time.Time) int64 {
				return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC).Unix() / 86400
			}
			days := civil(local) - civil(origin)
			if (civil(latest.In(location))-civil(local))/step > maxTimechartBuckets {
				return nil, fmt.Errorf("timechart produces more than %d buckets", maxTimechartBuckets)
			}
			first = origin.AddDate(0, 0, int(floorInt64(days, step)*step))
			if first.After(local) {
				first = first.AddDate(0, 0, -int(step))
			}
			advance = func(value time.Time) time.Time { return value.AddDate(0, 0, int(step)) }
		}
		first = first.UTC()
	}
	if !time.Unix(0, first.UnixNano()).Equal(first) {
		return nil, fmt.Errorf("timechart boundary exceeds timestamp range")
	}
	capacity := 1
	if calendar == CalendarNone {
		distance := new(big.Int).Sub(big.NewInt(latest.UnixNano()), big.NewInt(first.UnixNano()))
		count := distance.Add(distance, big.NewInt(int64(span)-1)).Div(distance, big.NewInt(int64(span)))
		if !count.IsInt64() || count.Int64() > maxTimechartBuckets {
			return nil, fmt.Errorf("timechart produces more than %d buckets", maxTimechartBuckets)
		}
		capacity = int(count.Int64()) + 1
	}
	boundaries := make([]time.Time, 1, capacity)
	boundaries[0] = first
	for boundaries[len(boundaries)-1].Before(latest) {
		if len(boundaries) > maxTimechartBuckets {
			return nil, fmt.Errorf("timechart produces more than %d buckets", maxTimechartBuckets)
		}
		current := boundaries[len(boundaries)-1]
		next := advance(current.In(location)).UTC()
		if !next.After(current) || !time.Unix(0, next.UnixNano()).Equal(next) {
			return nil, fmt.Errorf("timechart boundary exceeds timestamp range")
		}
		boundaries = append(boundaries, next)
	}
	return boundaries, nil
}

func resolveTimechartAlignment(axis spl.TimechartAxisOptions, earliest, latest, searchStart time.Time, location *time.Location) (time.Time, error) {
	if !axis.AlignTimeSpecified {
		if axis.AlignTime != "" || axis.AlignTimeRange != (spl.Range{}) {
			return time.Time{}, fmt.Errorf("unspecified timechart alignment contains metadata")
		}
		return time.Time{}, nil
	}
	if axis.AlignTimeRange == (spl.Range{}) {
		return time.Time{}, fmt.Errorf("timechart alignment source range is missing")
	}
	source := axis.AlignTime
	switch strings.ToLower(source) {
	case "earliest":
		return earliest, nil
	case "latest":
		return latest, nil
	}
	// Decimal epoch timestamps retain exact fractional precision without Float64.
	if !strings.ContainsAny(source, "@abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ") {
		negative := strings.HasPrefix(source, "-")
		digits := strings.TrimPrefix(strings.TrimPrefix(source, "-"), "+")
		parts := strings.Split(digits, ".")
		if len(parts) <= 2 {
			seconds, err := strconv.ParseInt(parts[0], 10, 64)
			if err == nil {
				nanos := int64(0)
				if len(parts) == 2 {
					if len(parts[1]) > 9 || parts[1] == "" {
						return time.Time{}, fmt.Errorf("invalid alignment timestamp precision")
					}
					nanos, err = strconv.ParseInt(parts[1]+strings.Repeat("0", 9-len(parts[1])), 10, 64)
				}
				if err == nil && seconds <= math.MaxInt64/int64(time.Second) {
					if negative {
						seconds = -seconds
						nanos = -nanos
					}
					value := time.Unix(seconds, nanos).UTC()
					if time.Unix(0, value.UnixNano()).Equal(value) {
						return value, nil
					}
				}
			}
		}
	}
	spec, err := splrelativetime.CompileSpecifier(source)
	if err != nil {
		return time.Time{}, err
	}
	value := searchStart.In(location)
	for index := 0; index < spec.OperationCount(); index++ {
		operation, _ := spec.Operation(index)
		if operation.Kind == splrelativetime.OperationOffset {
			if operation.Magnitude > math.MaxInt32 {
				return time.Time{}, fmt.Errorf("alignment offset overflows")
			}
			magnitude := int(operation.Magnitude)
			if operation.Negative {
				magnitude = -magnitude
			}
			switch operation.Unit {
			case splrelativetime.UnitSecond, splrelativetime.UnitMinute, splrelativetime.UnitHour:
				unit := time.Second
				if operation.Unit == splrelativetime.UnitMinute {
					unit = time.Minute
				}
				if operation.Unit == splrelativetime.UnitHour {
					unit = time.Hour
				}
				if operation.Magnitude > uint64(math.MaxInt64)/uint64(unit) {
					return time.Time{}, fmt.Errorf("alignment offset exceeds duration range")
				}
				value = value.Add(time.Duration(safecast.MustConv[int64](magnitude)) * unit)
			case splrelativetime.UnitDay:
				value = value.AddDate(0, 0, magnitude)
			case splrelativetime.UnitWeek:
				value = value.AddDate(0, 0, 7*magnitude)
			case splrelativetime.UnitMonth:
				value = value.AddDate(0, magnitude, 0)
			case splrelativetime.UnitQuarter:
				value = value.AddDate(0, 3*magnitude, 0)
			case splrelativetime.UnitYear:
				value = value.AddDate(magnitude, 0, 0)
			}
		} else {
			year, month, day := value.Date()
			hour, minute, second := value.Clock()
			switch operation.Unit {
			case splrelativetime.UnitYear:
				month = 1
				fallthrough
			case splrelativetime.UnitQuarter:
				if operation.Unit == splrelativetime.UnitQuarter {
					month = time.Month((int(month)-1)/3*3 + 1)
				}
				fallthrough
			case splrelativetime.UnitMonth:
				day = 1
				fallthrough
			case splrelativetime.UnitWeek, splrelativetime.UnitDay:
				hour = 0
				fallthrough
			case splrelativetime.UnitHour:
				minute = 0
				fallthrough
			case splrelativetime.UnitMinute:
				second = 0
			}
			value = time.Date(year, month, day, hour, minute, second, 0, location)
			if operation.Unit == splrelativetime.UnitWeek {
				value = value.AddDate(0, 0, -(int(value.Weekday())-int(operation.Weekday)+7)%7)
			}
		}
		if !time.Unix(0, value.UnixNano()).Equal(value) {
			return time.Time{}, fmt.Errorf("alignment exceeds timestamp range")
		}
	}
	if !time.Unix(0, value.UnixNano()).Equal(value) {
		return time.Time{}, fmt.Errorf("alignment exceeds timestamp range")
	}
	return value.UTC(), nil
}
