package searchjobs

import "testing"

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
