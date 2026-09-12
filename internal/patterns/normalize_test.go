package patterns

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestNormalizationAppliesSignatureLimitAfterReplacement(t *testing.T) {
	t.Parallel()

	longHex := strings.Repeat("a", DefaultMaximumSignatureBytes+1)
	result, err := normalizePatternContext(context.Background(), longHex, Precise, DefaultMaximumSignatureBytes)
	if err != nil {
		t.Fatalf("normalize long hex: %v", err)
	}
	if result.display != "<hex>" {
		t.Fatalf("long hex display = %q", result.display)
	}
	if _, err := normalizePatternContext(
		context.Background(), strings.Repeat("g", DefaultMaximumSignatureBytes+1), Precise, DefaultMaximumSignatureBytes,
	); !errors.Is(err, ErrLimit) {
		t.Fatalf("long literal error = %v, want ErrLimit", err)
	}
}
