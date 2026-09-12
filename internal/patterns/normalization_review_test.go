package patterns

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestNormalizationReviewFrozenVersionOneBoundaries(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		raw  string
		mode Sensitivity
		want string
	}{
		{"hex sixteen case insensitive", "id=0123456789ABCDEF", Precise, "id=<hex>"},
		{"hex fifteen stays literal", "id=0123456789abcde", Precise, "id=0123456789abcde"},
		{"hex ASCII word prefix", "x0123456789abcdef", Precise, "x0123456789abcdef"},
		{"hex ASCII word suffix", "0123456789abcdefx", Precise, "0123456789abcdefx"},
		{"hex underscore boundary", "_0123456789abcdef_", Precise, "_0123456789abcdef_"},
		{"hex Unicode boundaries", "é0123456789abcdef界", Precise, "é<hex>界"},
		{"hex prefixed stays literal", "0x0123456789abcdef", Precise, "0x0123456789abcdef"},
		{"signed integers", "-12 +34", Balanced, "<int> <int>"},
		{"decimal fractions stay atomic", "1.25 .5 1.", Balanced, "1.25 .5 1."},
		{"decimal exponents stay atomic", "1e3 -1.5E-3 +.5e+2", Balanced, "1e3 -1.5E-3 +.5e+2"},
		{"precise decimal hex suffix", "1.2345678901234567", Precise, "1.2345678901234567"},
		{"broad first two tokens", "cost=1.25 retries=12 ignored=999", Broad, "cost=<decimal> retries=<int>"},
		{"broad leading point and exponent", ".5 -1e3 extra", Broad, "<decimal> <decimal>"},
		{"malformed decimal whole token", "1.2.3 4.5.6", Broad, "1.2.3 4.5.6"},
		{"malformed exponent whole token", "1e+2x 1e-3x", Broad, "1e+2x 1e-3x"},
		{"ASCII integer word boundaries", "a12 12z", Balanced, "a12 12z"},
		{"non ASCII digits stay literal", "٢ ٣", Broad, "٢ ٣"},
		{"Unicode whitespace", "\u2003request\t\n123\u00a0", Balanced, "request <int>"},
		{"zero width space stays literal", "a\u200bb", Precise, "a\u200bb"},
		{"empty eligible signature", "", Broad, ""},
		{"whitespace eligible signature", "\t\n\u2003", Broad, ""},
		{"short broad text", "hello", Broad, "hello"},
		{"literal placeholder escapes", "<int> <hex> *", Balanced, `\<int> \<hex> *`},
		{"literal backslash escapes", `\<int>`, Precise, `\\\<int>`},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			t.Parallel()
			got, err := normalizePatternContext(context.Background(), item.raw, item.mode, DefaultMaximumSignatureBytes)
			if err != nil {
				t.Fatalf("normalize: %v", err)
			}
			if got.display != item.want {
				t.Fatalf("display = %q, want %q", got.display, item.want)
			}
		})
	}
}

func TestNormalizationReviewTypedPlaceholdersCannotImpersonateLiterals(t *testing.T) {
	t.Parallel()
	inputs := []string{"42", "<int>", `\<int>`, "*", "0123456789abcdef", "<hex>", "1.5", "<decimal>"}
	keys := make(map[string]string)
	displays := make(map[string]string)
	for _, raw := range inputs {
		got, err := normalizePatternContext(context.Background(), raw, Broad, DefaultMaximumSignatureBytes)
		if err != nil {
			t.Fatal(err)
		}
		if previous, exists := keys[got.canonical]; exists {
			t.Fatalf("canonical collision for %q and %q", previous, raw)
		}
		if previous, exists := displays[got.display]; exists {
			t.Fatalf("display collision for %q and %q", previous, raw)
		}
		keys[got.canonical], displays[got.display] = raw, raw
	}
}

func TestNormalizationReviewCancellationAndOutputBound(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := normalizePatternContext(ctx, strings.Repeat("x ", 10_000), Balanced, DefaultMaximumSignatureBytes); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled normalization error = %v", err)
	}
	if _, err := normalizePatternContext(context.Background(), strings.Repeat("<", 33), Precise, 64); !errors.Is(err, ErrLimit) {
		t.Fatalf("escaped signature bound error = %v", err)
	}
}

func TestNormalizationReviewSignatureLimitAppliesAfterNormalization(t *testing.T) {
	t.Parallel()
	got, err := normalizePatternContext(context.Background(), strings.Repeat("a", DefaultMaximumSignatureBytes+1), Precise, DefaultMaximumSignatureBytes)
	if err != nil || got.display != "<hex>" {
		t.Fatalf("long raw token with short normalized signature: display=%q error=%v", got.display, err)
	}
	if _, err := normalizePatternContext(context.Background(), strings.Repeat("z", DefaultMaximumSignatureBytes+1), Precise, DefaultMaximumSignatureBytes); !errors.Is(err, ErrLimit) {
		t.Fatalf("long literal signature error = %v, want ErrLimit", err)
	}
}

func TestNormalizationReviewOversizedWorkingLimitReturnsAtomicLimitError(t *testing.T) {
	// Even an invalid limit supplied directly to this boundary must not panic or
	// wrap to a negative native int before the normalizer checks its budget.
	result, release, err := normalizeWithBudget(t.Context(), nil, "event 42", Balanced, DefaultMaximumSignatureBytes, ^uint64(0))
	if !errors.Is(err, ErrLimit) || release != nil || result != (normalizedPattern{}) {
		t.Fatalf("oversized working limit = %+v, release=%t, error=%v", result, release != nil, err)
	}
}
