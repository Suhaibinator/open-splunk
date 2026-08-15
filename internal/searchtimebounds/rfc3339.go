package searchtimebounds

// ValidRFC3339Nano reports whether value matches the RFC 3339 wire grammar.
// RFC 3339 permits a period-separated fractional second of up to nine digits.
// time.Parse is intentionally more permissive (it accepts commas, excess
// fractional digits, and invalid zone bounds), so validate the wire grammar
// before parsing and canonicalizing it.
func ValidRFC3339Nano(value string) bool {
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
