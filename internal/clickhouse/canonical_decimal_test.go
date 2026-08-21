package clickhouse

import (
	"strconv"
	"strings"
	"testing"
)

func TestCanonicalDecimalPayloadTextSQLIsBoundedAndArgumentSafe(t *testing.T) {
	t.Parallel()

	compiled := canonicalDecimalPayloadTextSQL("CAST(? AS String)")
	if placeholders := strings.Count(compiled, "?"); placeholders != 1 {
		t.Fatalf("canonical decimal SQL placeholders = %d, want 1", placeholders)
	}
	// Keep this guard well above the current expression while preventing a
	// later implementation from expanding one expression per input byte.
	if len(compiled) > 50_000 {
		t.Fatalf("canonical decimal SQL length = %d, want at most 50000", len(compiled))
	}
	for _, required := range []string{
		canonicalDecimalPayloadPattern,
		strconv.Itoa(MaximumExactNumericBinTextBytes),
		canonicalDecimalMagnitudeChunkBase,
		"__os_canonical_decimal_adjusted_exponent",
		"replaceRegexpOne",
	} {
		if !strings.Contains(compiled, required) {
			t.Errorf("canonical decimal SQL does not contain %q", required)
		}
	}
}

func TestCanonicalDecimalMagnitudeArithmeticUsesBoundedNumericChunks(t *testing.T) {
	t.Parallel()

	for name, compiled := range map[string]string{
		"add":      canonicalDecimalMagnitudeAddSmallSQL("huge_magnitude", "small_adjustment"),
		"subtract": canonicalDecimalMagnitudeSubtractSmallSQL("huge_magnitude", "small_adjustment"),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if strings.Count(compiled, "huge_magnitude") != 1 {
				t.Fatalf("%s SQL evaluates attacker-controlled magnitude more than once", name)
			}
			if !strings.Contains(compiled, canonicalDecimalMagnitudeChunkBase) ||
				!strings.Contains(compiled, "<= "+strconv.Itoa(canonicalDecimalMagnitudeChunkDigits)) {
				t.Fatalf("%s SQL does not preserve the fixed-width chunk bound", name)
			}
			if len(compiled) > 12_000 {
				t.Fatalf("%s SQL length = %d, want at most 12000", name, len(compiled))
			}
		})
	}
}
