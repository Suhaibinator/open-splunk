package parserconfig

import (
	"errors"
	"strings"
)

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

// The spelling program only locates fractional digits after time.Parse has
// validated the complete value. It is needed when literal decimal separators
// can resemble fractions; ordinary layouts retain the digit-run fast path.
type timeSpellingKind uint8

const (
	spellingLiteral timeSpellingKind = iota
	spellingNumber
	spellingFixed
	spellingSeconds
	spellingFractionFixed
	spellingFractionOptional
	spellingZone
	spellingLongMonth
	spellingLongWeekday
)

type timeSpellingStep struct {
	text             string
	kind             timeSpellingKind
	width            int
	padding          int
	implicitFraction bool
}

// Longest overlapping spellings come first. This table describes Go layout
// tokens, not timestamp semantics; numeric ranges and dates remain owned by
// time.Parse. In particular, 15 is an unpadded hour while 05 is seconds.
var timeSpellingTokens = []timeSpellingStep{
	{text: "January", kind: spellingLongMonth}, {text: "Monday", kind: spellingLongWeekday},
	{text: "Jan", kind: spellingFixed, width: 3}, {text: "Mon", kind: spellingFixed, width: 3},
	{text: "Z07:00:00", kind: spellingZone, width: 9}, {text: "-07:00:00", kind: spellingZone, width: 9},
	{text: "Z070000", kind: spellingZone, width: 7}, {text: "-070000", kind: spellingZone, width: 7},
	{text: "Z07:00", kind: spellingZone, width: 6}, {text: "-07:00", kind: spellingZone, width: 6},
	{text: "Z0700", kind: spellingZone, width: 5}, {text: "-0700", kind: spellingZone, width: 5},
	{text: "Z07", kind: spellingZone, width: 3}, {text: "-07", kind: spellingZone, width: 3},
	{text: "_2006", kind: spellingFixed, width: 5}, {text: "2006", kind: spellingNumber, width: 4},
	{text: "__2", kind: spellingNumber, width: 3, padding: 2}, {text: "_2", kind: spellingNumber, width: 2, padding: 1},
	{text: "002", kind: spellingNumber, width: 3},
	{text: "01", kind: spellingNumber, width: 2}, {text: "02", kind: spellingNumber, width: 2},
	{text: "03", kind: spellingNumber, width: 2}, {text: "04", kind: spellingNumber, width: 2},
	{text: "05", kind: spellingSeconds, width: 2}, {text: "06", kind: spellingNumber, width: 2},
	{text: "15", kind: spellingNumber, width: 2},
	{text: "1", kind: spellingNumber, width: 2}, {text: "2", kind: spellingNumber, width: 2},
	{text: "3", kind: spellingNumber, width: 2}, {text: "4", kind: spellingNumber, width: 2}, {text: "5", kind: spellingSeconds, width: 2},
	{text: "PM", kind: spellingFixed, width: 2}, {text: "pm", kind: spellingFixed, width: 2},
}

func compilePrecisionLayout(layout string) ([]timeSpellingStep, error) {
	// Ordinary layouts need validation only, with no temporary program allocation.
	needed := false
	for at := 0; at < len(layout); {
		_, size, err := precisionToken(layout[at:])
		if err != nil {
			return nil, err
		}
		if size == 0 {
			needed = needed || layout[at] == '.' || layout[at] == ','
			at++
		} else {
			at += size
		}
	}
	if !needed {
		return nil, nil
	}
	var steps []timeSpellingStep
	literalStart := 0
	for at := 0; at < len(layout); {
		token, size, err := precisionToken(layout[at:])
		if err != nil {
			return nil, err
		}
		if size == 0 {
			at++
			continue
		}
		if literalStart < at {
			text := layout[literalStart:at]
			steps = append(steps, timeSpellingStep{kind: spellingLiteral, text: text})
		}
		steps = append(steps, token)
		at += size
		literalStart = at
	}
	if literalStart < len(layout) {
		text := layout[literalStart:]
		steps = append(steps, timeSpellingStep{kind: spellingLiteral, text: text})
	}
	for i := range steps {
		if steps[i].kind != spellingSeconds {
			continue
		}
		next := i + 1
		if next < len(steps) && steps[next].kind == spellingLiteral {
			next++
		}
		steps[i].implicitFraction = next == len(steps) || (steps[next].kind != spellingFractionFixed && steps[next].kind != spellingFractionOptional)
	}
	return steps, nil
}

func precisionToken(layout string) (timeSpellingStep, int, error) {
	if (layout[0] == '.' || layout[0] == ',') && len(layout) > 1 && (layout[1] == '0' || layout[1] == '9') {
		end := 2
		for end < len(layout) && layout[end] == layout[1] {
			end++
		}
		if end == len(layout) || layout[end] < '0' || layout[end] > '9' {
			if end-1 > 9 {
				return timeSpellingStep{}, 0, errors.New("timestamp_layout exceeds nanosecond precision")
			}
			kind := spellingFractionFixed
			if layout[1] == '9' {
				kind = spellingFractionOptional
			}
			return timeSpellingStep{kind: kind, width: end - 1}, end, nil
		}
	}
	for _, token := range timeSpellingTokens {
		if !strings.HasPrefix(layout, token.text) {
			continue
		}
		if (token.text == "Jan" || token.text == "Mon") && len(layout) > 3 && layout[3] >= 'a' && layout[3] <= 'z' {
			continue
		}
		return token, len(token.text), nil
	}
	return timeSpellingStep{}, 0, nil
}

func validFractionSpelling(steps []timeSpellingStep, value string) bool {
	at := 0
	for _, step := range steps {
		switch step.kind {
		case spellingLiteral:
			for i := 0; i < len(step.text); i++ {
				if step.text[i] == ' ' {
					if at < len(value) && value[at] != ' ' {
						return false
					}
					for i+1 < len(step.text) && step.text[i+1] == ' ' {
						i++
					}
					for at < len(value) && value[at] == ' ' {
						at++
					}
				} else {
					if at == len(value) || value[at] != step.text[i] {
						return false
					}
					at++
				}
			}
		case spellingNumber, spellingSeconds:
			for count := 0; count < step.padding && at < len(value) && value[at] == ' '; count++ {
				at++
			}
			for count := 0; count < step.width && at < len(value) && value[at] >= '0' && value[at] <= '9'; count++ {
				at++
			}
			if step.implicitFraction {
				var valid bool
				at, valid = consumeFraction(value, at)
				if !valid {
					return false
				}
			}
		case spellingFixed:
			at += step.width
		case spellingFractionFixed:
			at += 1 + step.width
		case spellingFractionOptional:
			var valid bool
			at, valid = consumeFraction(value, at)
			if !valid {
				return false
			}
		case spellingZone:
			if at < len(value) && value[at] == 'Z' && step.text[0] == 'Z' {
				at++
			} else {
				at += step.width
			}
		case spellingLongMonth:
			at += spellingNameLength(value[at:], []string{"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"})
		case spellingLongWeekday:
			at += spellingNameLength(value[at:], []string{"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"})
		}
		if at > len(value) {
			return false
		}
	}
	return at == len(value)
}

func consumeFraction(value string, at int) (int, bool) {
	if at+1 >= len(value) || (value[at] != '.' && value[at] != ',') || value[at+1] < '0' || value[at+1] > '9' {
		return at, true
	}
	start := at
	at++
	for at < len(value) && value[at] >= '0' && value[at] <= '9' {
		at++
		if at-start > 10 {
			return at, false
		}
	}
	return at, true
}

func spellingNameLength(value string, names []string) int {
	for _, name := range names {
		if len(value) >= len(name) && strings.EqualFold(value[:len(name)], name) {
			return len(name)
		}
	}
	return 0
}
