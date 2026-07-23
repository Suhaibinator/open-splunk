package searchjobs

import (
	"math/big"
	"strconv"
	"strings"
	"testing"
)

func TestCanonicalDecimalCollapsesEquivalentSpellings(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"0":                      "0",
		"-0.000e99":              "0",
		"001.2300":               "1.23",
		"123e-2":                 "1.23",
		"0.000001":               "0.000001",
		"0.0000001":              "1e-7",
		"100000000000000000000":  "100000000000000000000",
		"1000000000000000000000": "1e21",
		"-12.3400e+7":            "-123400000",
		"10e20":                  "1e21",
		"10e-1":                  "1",
		"100e-2":                 "1",
		"0.00100e2":              "0.1",
		"123e999":                "1.23e1001",
		"123E-999":               "1.23e-997",
	}
	for source, want := range tests {
		source, want := source, want
		t.Run(source, func(t *testing.T) {
			t.Parallel()
			got, err := CanonicalDecimal(source)
			if err != nil || got != want {
				t.Fatalf("CanonicalDecimal(%q) = (%q, %v), want %q", source, got, err, want)
			}
		})
	}
}

func TestCanonicalDecimalHandlesAttackerSizedExponentsWithLinearArithmetic(t *testing.T) {
	t.Parallel()

	huge := strings.Repeat("9", 32<<10)
	if got, err := CanonicalDecimal("1e" + huge); err != nil || got != "1e"+huge {
		t.Fatalf("positive huge exponent length = %d, error = %v", len(got), err)
	}
	if got, err := CanonicalDecimal("1e-" + huge); err != nil || got != "1e-"+huge {
		t.Fatalf("negative huge exponent length = %d, error = %v", len(got), err)
	}
	if got, err := CanonicalDecimal("10e" + huge); err != nil || got != "1e1"+strings.Repeat("0", len(huge)) {
		t.Fatalf("huge exponent carry length = %d, error = %v", len(got), err)
	}
	if got, err := CanonicalDecimal("10e-" + huge); err != nil || got != "1e-"+huge[:len(huge)-1]+"8" {
		t.Fatalf("huge exponent borrow length = %d, error = %v", len(got), err)
	}
	leadingZeros := strings.Repeat("0", 32<<10)
	if got, err := CanonicalDecimal("1e" + leadingZeros + "7"); err != nil || got != "10000000" {
		t.Fatalf("zero-padded exponent = (%q, %v), want 10000000", got, err)
	}
}

func TestAddDecimalExponentHandlesSignsCarriesAndCancellation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sign       int
		magnitude  string
		adjustment int
		wantSign   int
		want       string
	}{
		{sign: 0, magnitude: "0", adjustment: 0, wantSign: 0, want: "0"},
		{sign: 0, magnitude: "0", adjustment: -12, wantSign: -1, want: "12"},
		{sign: 1, magnitude: "999", adjustment: 1, wantSign: 1, want: "1000"},
		{sign: -1, magnitude: "999", adjustment: -1, wantSign: -1, want: "1000"},
		{sign: 1, magnitude: "999", adjustment: -1, wantSign: 1, want: "998"},
		{sign: -1, magnitude: "999", adjustment: 1, wantSign: -1, want: "998"},
		{sign: 1, magnitude: "1", adjustment: -1, wantSign: 0, want: "0"},
		{sign: -1, magnitude: "1", adjustment: 1, wantSign: 0, want: "0"},
		{sign: 1, magnitude: "1", adjustment: -999, wantSign: -1, want: "998"},
		{sign: -1, magnitude: "1", adjustment: 999, wantSign: 1, want: "998"},
	}
	for _, test := range tests {
		gotSign, got := addDecimalExponent(test.sign, test.magnitude, test.adjustment)
		if gotSign != test.wantSign || got != test.want {
			t.Errorf("addDecimalExponent(%d, %q, %d) = (%d, %q), want (%d, %q)",
				test.sign, test.magnitude, test.adjustment, gotSign, got, test.wantSign, test.want)
		}
	}
}

func TestCanonicalDecimalMatchesBigIntReference(t *testing.T) {
	t.Parallel()

	mantissas := []string{
		"0", "1", "-1", "+1", "10", "-10", "001.2300", "0.00000100",
		"0.00000010", "99999999999999999999.999900", "-0000010000.00001000",
	}
	for _, mantissa := range mantissas {
		for exponent := -1_024; exponent <= 1_024; exponent += 17 {
			source := mantissa + "e" + strconv.Itoa(exponent)
			got, err := CanonicalDecimal(source)
			if err != nil {
				t.Fatalf("CanonicalDecimal(%q) error = %v", source, err)
			}
			if want := canonicalDecimalBigIntReference(source); got != want {
				t.Fatalf("CanonicalDecimal(%q) = %q, want %q", source, got, want)
			}
		}
	}
}

func canonicalDecimalBigIntReference(source string) string {
	lower := strings.ToLower(source)
	mantissa, exponent, hasExponent := strings.Cut(lower, "e")
	negative := false
	if mantissa[0] == '-' || mantissa[0] == '+' {
		negative = mantissa[0] == '-'
		mantissa = mantissa[1:]
	}
	integer, fraction, _ := strings.Cut(mantissa, ".")
	explicitExponent := new(big.Int)
	if hasExponent {
		exponentNegative := exponent[0] == '-'
		if exponent[0] == '-' || exponent[0] == '+' {
			exponent = exponent[1:]
		}
		explicitExponent.SetString(exponent, 10)
		if exponentNegative {
			explicitExponent.Neg(explicitExponent)
		}
	}
	digits := integer + fraction
	firstNonzero := strings.IndexFunc(digits, func(character rune) bool { return character != '0' })
	if firstNonzero < 0 {
		return "0"
	}
	lastNonzero := len(digits) - 1
	for digits[lastNonzero] == '0' {
		lastNonzero--
	}
	coefficient := digits[firstNonzero : lastNonzero+1]
	scientificExponent := new(big.Int).Set(explicitExponent)
	scientificExponent.Add(scientificExponent, big.NewInt(int64(len(integer)-firstNonzero-1)))
	sign := ""
	if negative {
		sign = "-"
	}
	if scientificExponent.IsInt64() {
		exponentValue := scientificExponent.Int64()
		if exponentValue >= -6 && exponentValue < 21 {
			decimalPoint := int(exponentValue) + 1
			switch {
			case decimalPoint <= 0:
				return sign + "0." + strings.Repeat("0", -decimalPoint) + coefficient
			case decimalPoint >= len(coefficient):
				return sign + coefficient + strings.Repeat("0", decimalPoint-len(coefficient))
			default:
				return sign + coefficient[:decimalPoint] + "." + coefficient[decimalPoint:]
			}
		}
	}
	if len(coefficient) == 1 {
		return sign + coefficient + "e" + scientificExponent.String()
	}
	return sign + coefficient[:1] + "." + coefficient[1:] + "e" + scientificExponent.String()
}

func TestCanonicalDecimalRejectsMalformedValuesWithoutExpandingHugeExponents(t *testing.T) {
	t.Parallel()

	for _, source := range []string{"", ".1", "1.", "1e", "1e+", "1_0", "nan", "--1"} {
		if got, err := CanonicalDecimal(source); err == nil || got != "" {
			t.Errorf("CanonicalDecimal(%q) = (%q, %v), want error", source, got, err)
		}
	}
	if got, err := CanonicalDecimal("1e999999999999999999999999"); err != nil || got != "1e999999999999999999999999" {
		t.Fatalf("CanonicalDecimal(huge exponent) = (%q, %v)", got, err)
	}
}
