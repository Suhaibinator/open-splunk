package knowledge

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
)

const compatibilityFixtureSHA256 = "958eb18284f45a895951e5a1539537dda19f78c0ad380acb8847312b6ebe7fd4"

type compatibilityFixture struct {
	CompatibilityVersion string                     `json:"compatibility_version"`
	Cases                []compatibilityFixtureCase `json:"cases"`
}

type compatibilityFixtureCase struct {
	ID     string `json:"id"`
	Stage  string `json:"stage"`
	Rule   string `json:"rule"`
	Expect string `json:"expect"`
}

func TestCompatibilityV0_1FixtureContract(t *testing.T) {
	t.Parallel()

	encoded, err := os.ReadFile("testdata/compatibility-v0.1.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	if digest := fmt.Sprintf("%x", sha256.Sum256(encoded)); digest != compatibilityFixtureSHA256 {
		t.Fatalf("fixture SHA-256 = %s, want %s; intentional corpus changes must update the reviewed digest", digest, compatibilityFixtureSHA256)
	}
	var fixture compatibilityFixture
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&fixture); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		t.Fatalf("decode trailing fixture data: %v", err)
	}
	if fixture.CompatibilityVersion != CompatibilityVersion {
		t.Fatalf("compatibility version = %q, want %q", fixture.CompatibilityVersion, CompatibilityVersion)
	}
	if len(fixture.Cases) != 55 {
		t.Fatalf("fixture cases = %d, want 55", len(fixture.Cases))
	}

	requiredPrefixes := []string{
		"identity.",
		"precedence.",
		"visibility.",
		"selector.",
		"extraction.",
		"alias.",
		"calculated.",
		"dependency.",
		"collision.",
		"snapshot.",
		"lifecycle.",
		"forward-compat.",
		"authorization.",
		"resource.",
		"audit.",
		"idempotency.",
		"recovery.",
	}
	seenIDs := make(map[string]struct{}, len(fixture.Cases))
	seenPrefixes := make(map[string]bool, len(requiredPrefixes))
	for index, testCase := range fixture.Cases {
		if strings.TrimSpace(testCase.ID) != testCase.ID || testCase.ID == "" {
			t.Errorf("case %d has invalid id %q", index, testCase.ID)
		}
		if _, exists := seenIDs[testCase.ID]; exists {
			t.Errorf("case %d duplicates id %q", index, testCase.ID)
		}
		seenIDs[testCase.ID] = struct{}{}
		if strings.TrimSpace(testCase.Stage) == "" {
			t.Errorf("case %q has empty stage", testCase.ID)
		}
		if strings.TrimSpace(testCase.Rule) == "" {
			t.Errorf("case %q has empty rule", testCase.ID)
		}
		if strings.TrimSpace(testCase.Expect) == "" {
			t.Errorf("case %q has empty expectation", testCase.ID)
		}
		for _, prefix := range requiredPrefixes {
			if strings.HasPrefix(testCase.ID, prefix) {
				seenPrefixes[prefix] = true
			}
		}
	}
	for _, prefix := range requiredPrefixes {
		if !seenPrefixes[prefix] {
			t.Errorf("fixture has no %q case", prefix)
		}
	}
}
