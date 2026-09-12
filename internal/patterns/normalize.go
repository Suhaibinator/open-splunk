package patterns

import (
	"context"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

type segmentKind uint8

const (
	literalSegment segmentKind = iota
	hexSegment
	integerSegment
	decimalSegment
)

type patternSegment struct {
	kind segmentKind
	text string
}

type normalizedPattern struct {
	display   string
	canonical string
}

func normalizePatternContext(ctx context.Context, raw string, sensitivity Sensitivity, maximumBytes int) (normalizedPattern, error) {
	return normalizePatternContextWithWorking(ctx, raw, sensitivity, maximumBytes, int(DefaultMaximumWorkingBytes))
}

func normalizePatternContextWithWorking(
	ctx context.Context,
	raw string,
	sensitivity Sensitivity,
	maximumBytes int,
	maximumWorkingBytes int,
) (normalizedPattern, error) {
	tokenLimit := 0
	if sensitivity == Broad {
		tokenLimit = 2
	}
	whitespace, err := collapseWhitespace(ctx, raw, tokenLimit, maximumWorkingBytes)
	if err != nil {
		return normalizedPattern{}, err
	}
	segments, err := classifySegments(ctx, whitespace, sensitivity)
	if err != nil {
		return normalizedPattern{}, err
	}
	var display strings.Builder
	var canonical strings.Builder
	for _, segment := range segments {
		switch segment.kind {
		case literalSegment:
			appendEscapedLiteral(&display, segment.text)
			canonical.WriteByte('l')
			canonical.WriteString(strconv.Itoa(len(segment.text)))
			canonical.WriteByte(':')
			canonical.WriteString(segment.text)
			canonical.WriteByte(';')
		case hexSegment:
			display.WriteString("<hex>")
			canonical.WriteString("h;")
		case integerSegment:
			display.WriteString("<int>")
			canonical.WriteString("i;")
		case decimalSegment:
			display.WriteString("<decimal>")
			canonical.WriteString("d;")
		}
		if display.Len() > maximumBytes || canonical.Len() > maximumBytes*2 {
			return normalizedPattern{}, ErrLimit
		}
	}
	return normalizedPattern{display: display.String(), canonical: canonical.String()}, nil
}

func collapseWhitespace(ctx context.Context, value string, tokenLimit, maximumBytes int) (string, error) {
	var result strings.Builder
	result.Grow(min(len(value), maximumBytes))
	pendingSpace := false
	wrote := false
	tokens := 0
	inToken := false
	cut := false
	nextCheck := 0
	for index := 0; index < len(value); {
		if index >= nextCheck {
			if err := ctx.Err(); err != nil {
				return "", err
			}
			nextCheck = index + 4096
		}
		character, size := utf8.DecodeRuneInString(value[index:])
		if character == utf8.RuneError && size == 1 {
			return "", ErrUnsupported
		}
		index += size
		if unicode.IsSpace(character) {
			pendingSpace = wrote && !cut
			inToken = false
			continue
		}
		if cut {
			continue
		}
		if !inToken {
			tokens++
			inToken = true
			if tokenLimit > 0 && tokens > tokenLimit {
				cut = true
				pendingSpace = false
				continue
			}
		}
		if pendingSpace {
			if result.Len() == maximumBytes {
				return "", ErrLimit
			}
			result.WriteByte(' ')
			pendingSpace = false
		}
		if size > maximumBytes-result.Len() {
			return "", ErrLimit
		}
		result.WriteRune(character)
		wrote = true
	}
	return result.String(), nil
}

func classifySegments(ctx context.Context, value string, sensitivity Sensitivity) ([]patternSegment, error) {
	segments := make([]patternSegment, 0, 8)
	literalStart := 0
	nextCheck := 0
	for index := 0; index < len(value); {
		if index >= nextCheck {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			nextCheck = index + 4096
		}
		kind, end, matched := numericSegmentAt(value, index, sensitivity)
		if !matched {
			_, size := utf8.DecodeRuneInString(value[index:])
			index += size
			continue
		}
		if kind == literalSegment {
			index = end
			continue
		}
		if literalStart < index {
			segments = append(segments, patternSegment{kind: literalSegment, text: value[literalStart:index]})
		}
		segments = append(segments, patternSegment{kind: kind})
		index = end
		literalStart = end
	}
	if literalStart < len(value) || len(segments) == 0 {
		segments = append(segments, patternSegment{kind: literalSegment, text: value[literalStart:]})
	}
	return segments, nil
}

func numericSegmentAt(value string, start int, sensitivity Sensitivity) (segmentKind, int, bool) {
	if start > 0 && (!asciiBoundary(value[start-1]) || value[start-1] == '.') {
		return 0, 0, false
	}
	if end, ok := hexadecimalEnd(value, start); ok {
		return hexSegment, end, true
	}
	if sensitivity == Precise {
		return 0, 0, false
	}
	if end, ok := decimalEnd(value, start); ok {
		if sensitivity == Broad {
			return decimalSegment, end, true
		}
		// Balanced preserves the complete decimal token. Returning one literal
		// segment advances over it atomically, so integer matching cannot rewrite
		// a decimal prefix or exponent.
		return literalSegment, end, true
	}
	if end, ok := integerEnd(value, start); ok {
		return integerSegment, end, true
	}
	return 0, 0, false
}

func hexadecimalEnd(value string, start int) (int, bool) {
	if start >= len(value) || !asciiHex(value[start]) {
		return 0, false
	}
	end := start
	for end < len(value) && asciiHex(value[end]) {
		end++
	}
	if end-start < 16 || !rightNumericBoundary(value, end) {
		return 0, false
	}
	return end, true
}

func decimalEnd(value string, start int) (int, bool) {
	index := start
	if index < len(value) && (value[index] == '+' || value[index] == '-') {
		index++
	}
	digitsBefore := scanDigits(value, index)
	index += digitsBefore
	hasPoint := index < len(value) && value[index] == '.'
	digitsAfter := 0
	if hasPoint {
		index++
		digitsAfter = scanDigits(value, index)
		index += digitsAfter
	}
	if digitsBefore == 0 && digitsAfter == 0 {
		return 0, false
	}
	hasExponent := false
	if index < len(value) && (value[index] == 'e' || value[index] == 'E') {
		exponent := index + 1
		if exponent < len(value) && (value[exponent] == '+' || value[exponent] == '-') {
			exponent++
		}
		exponentDigits := scanDigits(value, exponent)
		if exponentDigits == 0 {
			return 0, false
		}
		index = exponent + exponentDigits
		hasExponent = true
	}
	if !hasPoint && !hasExponent || !rightNumericBoundary(value, index) {
		return 0, false
	}
	return index, true
}

func integerEnd(value string, start int) (int, bool) {
	index := start
	if index < len(value) && (value[index] == '+' || value[index] == '-') {
		index++
	}
	digits := scanDigits(value, index)
	if digits == 0 {
		return 0, false
	}
	end := index + digits
	if !rightNumericBoundary(value, end) {
		return 0, false
	}
	return end, true
}

func scanDigits(value string, start int) int {
	end := start
	for end < len(value) && value[end] >= '0' && value[end] <= '9' {
		end++
	}
	return end - start
}

func rightNumericBoundary(value string, end int) bool {
	if end == len(value) {
		return true
	}
	if !asciiBoundary(value[end]) {
		return false
	}
	// A period directly after a digit continues a decimal token. Refusing the
	// partial match keeps a long decimal prefix from becoming a hex pattern and
	// keeps Balanced integers literal inside decimals.
	return value[end] != '.'
}

func asciiBoundary(character byte) bool {
	return (character < 'a' || character > 'z') &&
		(character < 'A' || character > 'Z') &&
		(character < '0' || character > '9') && character != '_'
}

func asciiHex(character byte) bool {
	return character >= '0' && character <= '9' ||
		character >= 'a' && character <= 'f' ||
		character >= 'A' && character <= 'F'
}

func appendEscapedLiteral(destination *strings.Builder, value string) {
	for _, character := range value {
		if character == '\\' || character == '<' {
			destination.WriteByte('\\')
		}
		destination.WriteRune(character)
	}
}
