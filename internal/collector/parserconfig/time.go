package parserconfig

import (
	"errors"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata" // Collector binaries must resolve IANA zones in minimal containers.
)

type timeParser struct {
	layout      string
	location    *time.Location
	zoneToken   string
	zoneSuffix  string
	fixed       bool
	fixedOffset int
}

func compileTime(layout, zone string) (timeParser, error) {
	p := timeParser{layout: layout}
	if layout == "" {
		if zone != "" {
			return p, errors.New("timezone requires timestamp_layout")
		}
		p.layout = time.RFC3339Nano
	}
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
				p.zoneSuffix = p.layout[i+len(token):]
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
	if len(value) > maxEventBytes || p.layout == "" || excessiveFraction(value) {
		return time.Time{}, errors.New("invalid timestamp")
	}
	parsed, err := time.Parse(p.layout, value)
	if err != nil {
		return time.Time{}, errors.New("invalid timestamp")
	}
	if p.location == nil {
		if !p.validOffset(value, parsed) {
			return time.Time{}, errors.New("invalid timestamp offset")
		}
		return boundedTime(parsed)
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

func (p timeParser) validOffset(value string, parsed time.Time) bool {
	if p.zoneToken == "" {
		return true
	}
	suffix := parsed.Format(p.zoneSuffix)
	if !strings.HasSuffix(value, suffix) {
		return false
	}
	end := len(value) - len(suffix)
	if end > 0 && value[end-1] == 'Z' && p.zoneToken[0] == 'Z' {
		return true
	}
	size := len(p.zoneToken)
	if end < size {
		return false
	}
	zone := value[end-size : end]
	if zone[0] != '+' && zone[0] != '-' {
		return false
	}
	digits := strings.ReplaceAll(zone[1:], ":", "")
	for i := 0; i < len(digits); i += 2 {
		part, err := strconv.Atoi(digits[i : i+2])
		if err != nil {
			return false
		}
		limit := 59
		if i == 0 {
			limit = 23
		}
		if part > limit {
			return false
		}
	}
	return true
}

func asciiDigits(value string) bool {
	for i := range len(value) {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

// Go's parser silently truncates fractional seconds after nine digits. Reject
// that lossy conversion even when all discarded digits happen to be zero.
func excessiveFraction(value string) bool {
	for i := 0; i < len(value); i++ {
		if value[i] != '.' && value[i] != ',' {
			continue
		}
		digits := 0
		for i+1 < len(value) && value[i+1] >= '0' && value[i+1] <= '9' {
			i++
			digits++
			if digits > 9 {
				return true
			}
		}
	}
	return false
}
