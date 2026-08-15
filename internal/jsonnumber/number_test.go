package jsonnumber

import (
	"math"
	"strconv"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/testsupport/jsonnumbercorpus"
)

func TestClassifyPreservesBoundedJSONNumberSemantics(t *testing.T) {
	t.Parallel()

	for _, test := range jsonnumbercorpus.Cases() {
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()
			got := Classify(test.Lexeme)
			gotType, gotValue := classificationRepresentation(got)
			if gotType != test.DynamicType || gotValue != test.Value {
				t.Fatalf("Classify(%q) = %#v (%s/%s), want %s/%s",
					test.Lexeme, got, gotType, gotValue, test.DynamicType, test.Value)
			}
		})
	}
}

func classificationRepresentation(classification Classification) (string, string) {
	switch classification.Kind {
	case KindSint64:
		return "Int64", strconv.FormatInt(classification.Sint64, 10)
	case KindUint64:
		return "UInt64", strconv.FormatUint(classification.Uint64, 10)
	case KindFloat64:
		return "Float64", strconv.FormatUint(math.Float64bits(classification.Float64), 10)
	case KindDecimal:
		return "Map(String, String)", classification.Decimal
	default:
		return "invalid", ""
	}
}

func TestParseDecimalRatRejectsInvalidJSONNumberLexemes(t *testing.T) {
	t.Parallel()

	for _, text := range []string{
		"",
		"+1",
		"-",
		".1",
		"1.",
		"01",
		"-01",
		"1e",
		"1e+",
		"1e-",
		"1e1e1",
		"1..0",
		" 1",
		"1 ",
		"NaN",
		"Inf",
	} {
		t.Run(text, func(t *testing.T) {
			t.Parallel()
			if value, err := ParseDecimalRat(text); err == nil {
				t.Fatalf("ParseDecimalRat(%q) = %s, want error", text, value.RatString())
			}
		})
	}
}

func TestParseDecimalRatReturnsZeroAtExponentBounds(t *testing.T) {
	t.Parallel()

	for _, text := range []string{
		"0e10000",
		"-0.0e-10000",
	} {
		t.Run(text, func(t *testing.T) {
			t.Parallel()
			value, err := ParseDecimalRat(text)
			if err != nil {
				t.Fatalf("ParseDecimalRat(%q): %v", text, err)
			}
			if value.Sign() != 0 {
				t.Fatalf("ParseDecimalRat(%q) = %s, want 0", text, value.RatString())
			}
		})
	}
}

func TestNormalizeDecimalLexemeOnlyCanonicalizesExponentSpelling(t *testing.T) {
	t.Parallel()

	for input, want := range map[string]string{
		"1E+0400":   "1e400",
		"1E-0400":   "1e-400",
		"1.2300e00": "1.2300e0",
		"-0.0":      "-0.0",
		"42":        "42",
	} {
		if got := NormalizeDecimalLexeme(input); got != want {
			t.Fatalf("NormalizeDecimalLexeme(%q) = %q, want %q", input, got, want)
		}
	}
}
