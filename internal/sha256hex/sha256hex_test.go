package sha256hex

import (
	"strings"
	"testing"
)

func TestValid(t *testing.T) {
	t.Parallel()
	valid := strings.Repeat("0123456789abcdef", 4)
	if !Valid(valid) {
		t.Fatalf("Valid(%q) = false", valid)
	}
	for _, value := range []string{
		"",
		valid[:len(valid)-1],
		valid + "0",
		strings.Repeat("A", EncodedSize),
		strings.Repeat("g", EncodedSize),
		strings.Repeat("/", EncodedSize),
	} {
		if Valid(value) {
			t.Fatalf("Valid(%q) = true", value)
		}
	}
}

func TestValidDoesNotAllocate(t *testing.T) {
	valid := strings.Repeat("ab", EncodedSize/2)
	if got := testing.AllocsPerRun(1_000, func() { _ = Valid(valid) }); got != 0 {
		t.Fatalf("Valid allocations = %v, want 0", got)
	}
}
