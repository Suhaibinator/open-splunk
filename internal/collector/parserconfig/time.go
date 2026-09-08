package parserconfig

import (
	"errors"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata" // Collector binaries must resolve IANA zones in minimal containers.
)

type timeParser struct {
	layout          string
	location        *time.Location
	zoneToken       string
	zoneProbeLayout string
	fixed           bool
	fixedOffset     int
	precisionSteps  []timeSpellingStep
}

func compileTime(layout, zone string) (timeParser, error) {
	p := timeParser{layout: layout}
	if layout == "" {
		if zone != "" {
			return p, errors.New("timezone requires timestamp_layout")
		}
		p.layout = time.RFC3339Nano
	}
	precisionSteps, err := compilePrecisionLayout(p.layout)
	if err != nil {
		return p, err
	}
	p.precisionSteps = precisionSteps
	if strings.Contains(p.layout, "MST") {
		return p, errors.New("timezone abbreviations are unsupported; use a numeric offset")
	}
	// Two independently chosen dates detect missing date components, including
	// layouts that use day-of-year or a two-digit year, using Go's own tokenizer.
	for _, sample := range []time.Time{time.Date(2024, 2, 29, 13, 47, 58, 123456789, time.UTC), time.Date(2013, 11, 7, 2, 16, 39, 987654321, time.UTC)} {
		parsed, err := time.Parse(p.layout, sample.Format(p.layout))
		if err != nil || parsed.Year() != sample.Year() || parsed.Month() != sample.Month() || parsed.Day() != sample.Day() {
			return p, errors.New("timestamp_layout must contain a complete valid Go date layout")
		}
	}
	// Tokens are ordered longest-first, matching Go's numeric zone forms.
	tokens := []string{"Z07:00:00", "-07:00:00", "Z070000", "-070000", "Z07:00", "-07:00", "Z0700", "-0700", "Z07", "-07"}
	zoneStart := -1
	for i := 0; i < len(p.layout); i++ {
		for _, token := range tokens {
			if strings.HasPrefix(p.layout[i:], token) {
				if zoneStart >= 0 {
					return p, errors.New("timestamp_layout has multiple timezone offsets")
				}
				zoneStart = i
				p.zoneToken = token
				if i+len(token) < len(p.layout) {
					p.zoneProbeLayout = p.layout[:i] + "\x00"
				}
				i += len(token) - 1
				break
			}
		}
	}
	if zoneStart >= 0 {
		if zone != "" {
			return p, errors.New("timezone is irrelevant to an offset-bearing timestamp_layout")
		}
		return p, nil
	}
	if zone == "" {
		return p, errors.New("offset-free timestamp_layout requires timezone")
	}
	if zone == "UTC" {
		p.location = time.UTC
		p.fixed = true
		return p, nil
	}
	if zone[0] == '+' || zone[0] == '-' {
		if len(zone) != 6 || zone[3] != ':' {
			return p, errors.New("numeric timezone must use +HH:MM or -HH:MM")
		}
		hours, errH := strconv.Atoi(zone[1:3])
		minutes, errM := strconv.Atoi(zone[4:6])
		if errH != nil || errM != nil || hours < 0 || hours > 23 || minutes < 0 || minutes > 59 || !asciiDigits(zone[1:3]) || !asciiDigits(zone[4:6]) {
			return p, errors.New("invalid numeric timezone")
		}
		offset := hours*3600 + minutes*60
		if zone[0] == '-' {
			offset = -offset
		}
		p.location = time.FixedZone(zone, offset)
		p.fixed = true
		p.fixedOffset = offset
		return p, nil
	}
	if !strings.Contains(zone, "/") {
		return p, errors.New("timezone must be UTC, numeric offset, or IANA name")
	}
	location, err := time.LoadLocation(zone)
	if err != nil {
		return p, errors.New("unknown IANA timezone")
	}
	p.location = location
	return p, nil
}

// ParseTime parses timestamps without depending on the machine's local zone.
// Local wall times must identify exactly one instant, rejecting both DST gaps
// and repeated local times. Errors intentionally omit source payloads.
func (c *Compiled) ParseTime(value string) (time.Time, error) {
	p := c.clock
	if len(value) > maxEventBytes || p.layout == "" {
		return time.Time{}, errors.New("invalid timestamp")
	}
	excessive := excessiveFraction(value)
	if excessive && p.precisionSteps == nil {
		return time.Time{}, errors.New("timestamp exceeds nanosecond precision")
	}
	parsed, err := time.Parse(p.layout, value)
	if err != nil {
		return time.Time{}, errors.New("invalid timestamp")
	}
	if excessive && !validFractionSpelling(p.precisionSteps, value) {
		return time.Time{}, errors.New("timestamp exceeds nanosecond precision")
	}
	if p.location == nil {
		offset, valid := p.parseOffset(value)
		if !valid {
			return time.Time{}, errors.New("invalid timestamp offset")
		}
		// Derive the instant from the original numeric offset. In particular,
		// Go currently treats -00:00:01 as its internal unspecified-offset
		// sentinel; trusting the returned zone would silently lose one second.
		_, parsedOffset := parsed.Zone()
		return boundedTime(parsed.Add(time.Duration(parsedOffset-offset) * time.Second))
	}
	if p.fixed {
		return boundedTime(parsed.Add(-time.Duration(p.fixedOffset) * time.Second))
	}
	// Parse as UTC first to retain exact wall components. Every IANA UTC offset
	// lies within 24 hours, including historical local mean time. Walk constant
	// offset intervals via ZoneBounds rather than sampling or guessing DST size.
	lower := parsed.Add(-24 * time.Hour)
	upper := parsed.Add(24 * time.Hour)
	var result time.Time
	matches := 0
	for probe := lower; !probe.After(upper); {
		localized := probe.In(p.location)
		_, offset := localized.Zone()
		start, end := localized.ZoneBounds()
		candidate := parsed.Add(-time.Duration(offset) * time.Second)
		if (start.IsZero() || !candidate.Before(start)) && (end.IsZero() || candidate.Before(end)) {
			result = candidate
			matches++
		}
		if end.IsZero() || end.After(upper) {
			break
		}
		if !end.After(probe) {
			return time.Time{}, errors.New("invalid timezone transition")
		}
		probe = end
	}
	if matches != 1 {
		return time.Time{}, errors.New("timestamp is nonexistent or ambiguous in configured timezone")
	}
	return boundedTime(result)
}

func boundedTime(value time.Time) (time.Time, error) {
	utc := value.UTC()
	if utc.Year() < 1 || utc.Year() > 9999 {
		return time.Time{}, errors.New("timestamp is outside supported year range")
	}
	return utc, nil
}

func (p timeParser) parseOffset(value string) (int, bool) {
	if p.zoneToken == "" {
		return 0, true
	}
	zone := value
	if p.zoneProbeLayout == "" {
		// Terminal zone tokens have fixed width (or a single Z). This includes
		// RFC3339 and avoids a second parse or allocation for the common path.
		if len(value) > 0 && value[len(value)-1] == 'Z' && p.zoneToken[0] == 'Z' {
			return 0, true
		}
		if len(value) < len(p.zoneToken) {
			return 0, false
		}
		zone = value[len(value)-len(p.zoneToken):]
	} else {
		// Parse the original prefix once, terminating at an impossible zone byte.
		// ValueElem then starts at the original zone spelling, even when preceding
		// layout tokens permit optional fractions, padding, or variable-width
		// numbers. Reformatting a suffix loses those spellings. The full timestamp
		// has already parsed successfully, so the sentinel must be the first error.
		_, err := time.Parse(p.zoneProbeLayout, value)
		parseErr, ok := errors.AsType[*time.ParseError](err)
		if !ok || !strings.HasSuffix(parseErr.LayoutElem, "\x00") {
			return 0, false
		}
		zone = parseErr.ValueElem
		if len(zone) > 0 && zone[0] == 'Z' && p.zoneToken[0] == 'Z' {
			return 0, true
		}
		if len(zone) < len(p.zoneToken) {
			return 0, false
		}
		zone = zone[:len(p.zoneToken)]
	}
	if zone[0] != '+' && zone[0] != '-' {
		return 0, false
	}
	offset, multiplier := 0, 3600
	for i := 1; i < len(zone); {
		if zone[i] == ':' {
			i++
			continue
		}
		if i+1 >= len(zone) || !asciiDigits(zone[i:i+2]) {
			return 0, false
		}
		part := int(zone[i]-'0')*10 + int(zone[i+1]-'0')
		limit := 59
		if i == 1 {
			limit = 23
		}
		if part > limit {
			return 0, false
		}
		offset += part * multiplier
		multiplier /= 60
		i += 2
	}
	if zone[0] == '-' {
		offset = -offset
	}
	return offset, true
}

func asciiDigits(value string) bool {
	for i := range len(value) {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}
