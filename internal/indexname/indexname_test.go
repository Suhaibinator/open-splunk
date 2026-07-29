package indexname

import (
	"strings"
	"testing"
)

func TestValidCanonical(t *testing.T) {
	t.Parallel()

	for _, value := range []string{
		"a",
		"main",
		"index_2-prod",
		strings.Repeat("a", MaximumBytes),
	} {
		if !ValidCanonical(value) {
			t.Errorf("ValidCanonical(%q) = false, want true", value)
		}
	}
	for _, value := range []string{
		"",
		"_leading",
		"-leading",
		"Uppercase",
		"contains space",
		"contains.dot",
		"mykvstoreindex",
		strings.Repeat("a", MaximumBytes+1),
	} {
		if ValidCanonical(value) {
			t.Errorf("ValidCanonical(%q) = true, want false", value)
		}
	}
}
