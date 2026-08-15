package splcompataudit

import (
	"strings"
	"testing"
)

// legacyParseNumber reproduces the pre-refactor numericSegmentParser.parseNumber
// byte scanner verbatim. It is the oracle the scanUnsignedNumericLiteral
// delegation must still match on offset and acceptance.
func legacyParseNumber(source string, offset int) (int, bool) {
	start := offset
	digits := 0
	for offset < len(source) && source[offset] >= '0' && source[offset] <= '9' {
		offset++
		digits++
	}
	if offset < len(source) && source[offset] == '.' {
		offset++
		for offset < len(source) && source[offset] >= '0' && source[offset] <= '9' {
			offset++
			digits++
		}
	}
	if digits == 0 {
		return start, false
	}
	if offset < len(source) && (source[offset] == 'e' || source[offset] == 'E') {
		exponentStart := offset
		offset++
		if offset < len(source) && (source[offset] == '+' || source[offset] == '-') {
			offset++
		}
		exponentDigits := offset
		for offset < len(source) && source[offset] >= '0' && source[offset] <= '9' {
			offset++
		}
		if offset == exponentDigits {
			offset = exponentStart
		}
	}
	return offset, true
}

// TestParseNumberDelegationMatchesLegacyScannerExhaustively enumerates every
// string up to length four over the bytes that steer the numeric scanner and
// starts the scan at every offset inside it, asserting the delegated
// implementation reports the identical cursor and acceptance.
func TestParseNumberDelegationMatchesLegacyScannerExhaustively(t *testing.T) {
	t.Parallel()

	alphabet := []byte{'0', '9', '.', 'e', 'E', '+', '-', 'x'}
	var sources []string
	var generate func(prefix []byte, depth int)
	generate = func(prefix []byte, depth int) {
		sources = append(sources, string(prefix))
		if depth == 0 {
			return
		}
		for _, character := range alphabet {
			generate(append(prefix, character), depth-1)
		}
	}
	generate(nil, 4)

	for _, source := range sources {
		for offset := 0; offset <= len(source); offset++ {
			parser := numericSegmentParser{source: source, offset: offset}
			got := parser.parseNumber()
			wantOffset, wantOK := legacyParseNumber(source, offset)
			if got != wantOK || parser.offset != wantOffset {
				t.Fatalf("parseNumber(%q, %d) = (%d, %t), want (%d, %t)",
					source, offset, parser.offset, got, wantOffset, wantOK)
			}
		}
	}
	if len(sources) < 4000 {
		t.Fatalf("generated %d sources, want the full enumeration", len(sources))
	}
}

// TestNumericArithmeticSegmentBoundaryLiterals covers the leading-zero, lone
// dot and exponent boundary shapes the delegated scanner has to classify.
func TestNumericArithmeticSegmentBoundaryLiterals(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		segment string
		numeric bool
	}{
		{"", false},
		{".", false},
		{"..", false},
		{"0", true},
		{"007", true},
		{"0007.0000", true},
		{".5", true},
		{"5.", true},
		{"5..", false},
		{"0.0.0", false},
		{"1e", false},
		{"1E", false},
		{"1e+", false},
		{"1e-", false},
		{"1e0", true},
		{"1E-0", true},
		{"007e+007", true},
		{".5e-5", true},
		{"5.e2", true},
		{"1e+2e+3", false},
		{"1e2.5", false},
		{"-.5", true},
		{"+-+-1", true},
		{"--1e-1", true},
		{"1+2*3/4%5", true},
		{"1e-3+2", true},
		{"1e-3+", false},
		{"1..2", false},
		{"1 + 2", false},
		{"0x10", false},
		{"1e999999", true},
	} {
		if got := numericArithmeticSegment(test.segment); got != test.numeric {
			t.Fatalf("numericArithmeticSegment(%q) = %t, want %t", test.segment, got, test.numeric)
		}
	}
}

// TestNumericLiteralOperatorOffsetsTerminatesOnHostileSegments guards the
// shared scanner's forward-progress contract: every accepted literal must
// advance the cursor, so no pathological segment can spin the marking loop.
func TestNumericLiteralOperatorOffsetsTerminatesOnHostileSegments(t *testing.T) {
	t.Parallel()

	for _, segment := range []string{
		strings.Repeat(".", 64),
		strings.Repeat("-", 64),
		strings.Repeat("+-", 32),
		strings.Repeat(".5e-", 16),
		strings.Repeat("1e", 32),
		strings.Repeat("0", 64) + "e",
		"." + strings.Repeat("0.", 32),
		strings.Repeat("+.5", 21),
		strings.Repeat("e", 64),
	} {
		protected := numericLiteralOperatorOffsets(segment)
		if len(protected) != len(segment) {
			t.Fatalf("numericLiteralOperatorOffsets(%q) length = %d, want %d",
				segment, len(protected), len(segment))
		}
		for index, marked := range protected {
			if marked && !arithmeticOperatorByte(segment[index]) {
				t.Fatalf("numericLiteralOperatorOffsets(%q) marked non-operator byte %q at %d",
					segment, segment[index], index)
			}
		}
	}
}
