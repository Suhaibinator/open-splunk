package knowledge

import (
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/testsupport/knowledgecompat"
)

const compatibilityFixtureSHA256 = "35cae144db3ae70ca26ac82fb5fee4e106983967b7425e636cc1509651057071"

func TestCompatibilityV0_1FixtureContract(t *testing.T) {
	t.Parallel()

	encoded, err := os.ReadFile("testdata/compatibility-v0.1.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	if digest := fmt.Sprintf("%x", sha256.Sum256(encoded)); digest != compatibilityFixtureSHA256 {
		t.Fatalf("fixture SHA-256 = %s, want %s; intentional corpus changes must update the reviewed digest", digest, compatibilityFixtureSHA256)
	}
	fixture := knowledgecompat.Load(t)
	if fixture.CompatibilityVersion != knowledgecompat.CompatibilityVersion {
		t.Fatalf(
			"historical compatibility version = %q, want %q",
			fixture.CompatibilityVersion,
			knowledgecompat.CompatibilityVersion,
		)
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
	ownerCounts := make(map[knowledgecompat.Owner]int)
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
		ownerCounts[testCase.Authority.Owner]++
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
	expectedOwnerCounts := map[knowledgecompat.Owner]int{
		knowledgecompat.OwnerKnowledge:                7,
		knowledgecompat.OwnerKnowledgeDefinition:      1,
		knowledgecompat.OwnerKnowledgeProgram:         6,
		knowledgecompat.OwnerKnowledgeSnapshot:        3,
		knowledgecompat.OwnerKnowledgeCatalog:         10,
		knowledgecompat.OwnerKnowledgeCatalogBlackbox: 2,
		knowledgecompat.OwnerKnowledgeAttemptAudit:    1,
		knowledgecompat.OwnerKnowledgePreview:         1,
		knowledgecompat.OwnerServer:                   4,
		knowledgecompat.OwnerControl:                  3,
		knowledgecompat.OwnerQueryExec:                17,
	}
	if len(expectedOwnerCounts) != len(knowledgecompat.Owners()) {
		t.Fatalf(
			"reviewed compatibility owner count = %d, closed taxonomy = %d",
			len(expectedOwnerCounts),
			len(knowledgecompat.Owners()),
		)
	}
	for _, owner := range knowledgecompat.Owners() {
		if ownerCounts[owner] != expectedOwnerCounts[owner] {
			t.Errorf(
				"fixture owner %q cases = %d, want %d",
				owner,
				ownerCounts[owner],
				expectedOwnerCounts[owner],
			)
		}
	}
}
