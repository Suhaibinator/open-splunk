package spl_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

type compatibilityV02StatsByEvidenceTarget struct {
	Path     string
	Identity string
}

// Each stats-BY corpus claim is bound to a concrete test identity. Separate
// tokens intentionally may point at the same matrix test: the corpus cases
// remain independently reviewable, while renaming or deleting their shared
// executable evidence fails this gate.
var compatibilityV02StatsByEvidenceRegistry = map[string][]compatibilityV02StatsByEvidenceTarget{
	"stats-by-live-raw-multivalue": {
		{Path: "internal/clickhouse/stats_multivalue_group_runtime_integration_test.go", Identity: "TestStatsMultivalueByAgainstClickHouse"},
	},
	"stats-by-live-split-deduplication": {
		{Path: "internal/clickhouse/stats_multivalue_group_runtime_integration_test.go", Identity: "TestStatsMultivalueByAgainstClickHouse"},
	},
	"stats-by-live-empty-domain": {
		{Path: "internal/clickhouse/stats_by_deferred_validation_adversarial_integration_test.go", Identity: "TestStatsByDeferredValidationAdversarialAgainstClickHouse"},
	},
	"stats-by-live-cartesian-product": {
		{Path: "internal/clickhouse/stats_multivalue_group_runtime_integration_test.go", Identity: "TestStatsMultivalueByAgainstClickHouse"},
	},
	"stats-by-live-string-or-bytes-publication": {
		{Path: "internal/queryexec/stats_by_string_or_bytes_integration_test.go", Identity: "TestStatsByFixedMultivalueStringOrBytesAgainstClickHouse"},
	},
	"stats-by-live-exact-10000-boundary": {
		{Path: "internal/clickhouse/stats_multivalue_expansion_limit_runtime_integration_test.go", Identity: "TestStatsMultivalueByExpansionLimitAgainstClickHouse"},
	},
	"stats-by-live-10001-rejection": {
		{Path: "internal/clickhouse/stats_multivalue_expansion_limit_runtime_integration_test.go", Identity: "TestStatsMultivalueByExpansionLimitAgainstClickHouse"},
		{Path: "internal/queryexec/stats_by_atomic_adversarial_test.go", Identity: "TestStatsByLateExpansionLimitIsAtomicAndRedacted"},
		{Path: "internal/searchjobs/stats_by_atomic_manager_adversarial_test.go", Identity: "TestStatsByExpansionLimitManagerHidesStagedPrefixAndClearsItOnFailure"},
	},
	"stats-by-live-container-rejection": {
		{Path: "internal/clickhouse/stats_by_deferred_validation_adversarial_integration_test.go", Identity: "TestStatsByDeferredValidationAdversarialAgainstClickHouse"},
		{Path: "internal/queryexec/stats_by_atomic_adversarial_test.go", Identity: "TestStatsByLateUnsupportedValueIsAtomicAndRedacted"},
		{Path: "internal/searchjobs/stats_by_atomic_manager_adversarial_test.go", Identity: "TestStatsByAtomicManagerHidesStagedPrefixAndClearsItOnFailure"},
	},
	"stats-by-executor-late-marker-atomic": {
		{Path: "internal/queryexec/stats_by_atomic_adversarial_test.go", Identity: "TestStatsByLateUnsupportedValueIsAtomicAndRedacted"},
	},
	"stats-by-manager-no-public-prefix": {
		{Path: "internal/searchjobs/stats_by_atomic_manager_adversarial_test.go", Identity: "TestStatsByAtomicManagerHidesStagedPrefixAndClearsItOnFailure"},
	},
}

func TestCompatibilityV02StatsByCorpusHasExactExecutableEvidence(t *testing.T) {
	t.Parallel()

	corpusPath, _ := compatibilityV02Paths(t)
	encoded, err := os.ReadFile(corpusPath)
	if err != nil {
		t.Fatalf("read v0.2 compatibility corpus: %v", err)
	}
	corpus, err := loadCompatibilityV02Corpus(encoded)
	if err != nil {
		t.Fatalf("load v0.2 compatibility corpus: %v", err)
	}

	const ruleID = "SPL-V02-STATS-BY-MULTIVALUE-001"
	wantEvidence := map[string]string{
		"raw multivalue grouping":                             "stats-by-live-raw-multivalue",
		"split-value deduplication":                           "stats-by-live-split-deduplication",
		"missing null and empty inputs":                       "stats-by-live-empty-domain",
		"authored Cartesian product":                          "stats-by-live-cartesian-product",
		"fixed multivalue preserves String and Bytes members": "stats-by-live-string-or-bytes-publication",
		"exact 10000 combination boundary":                    "stats-by-live-exact-10000-boundary",
		"10001 combinations fail atomically":                  "stats-by-live-10001-rejection",
		"nested container rejection":                          "stats-by-live-container-rejection",
		"late marker publication is atomic":                   "stats-by-executor-late-marker-atomic",
		"public preview remains empty":                        "stats-by-manager-no-public-prefix",
	}

	var rule *compatibilityV02Rule
	for index := range corpus.Rules {
		if corpus.Rules[index].ID == ruleID {
			rule = &corpus.Rules[index]
			break
		}
	}
	if rule == nil {
		t.Fatalf("v0.2 corpus is missing rule %q", ruleID)
	}
	if len(rule.Cases) != len(wantEvidence) {
		t.Fatalf("%s has %d cases, want exact evidence inventory of %d", ruleID, len(rule.Cases), len(wantEvidence))
	}

	repositoryRoot := filepath.Clean(filepath.Join(filepath.Dir(corpusPath), "..", "..", ".."))
	seenCases := make(map[string]struct{}, len(rule.Cases))
	seenEvidence := make(map[string]struct{}, len(rule.Cases))
	for _, testCase := range rule.Cases {
		want, required := wantEvidence[testCase.Name]
		if !required {
			t.Fatalf("%s has an unreviewed case %q", ruleID, testCase.Name)
		}
		seenCases[testCase.Name] = struct{}{}
		if testCase.RowFixture == "" {
			t.Fatalf("%s case %q has no explicit runtime row fixture", ruleID, testCase.Name)
		}
		if testCase.Evidence != want {
			t.Fatalf("%s case %q evidence = %q, want %q", ruleID, testCase.Name, testCase.Evidence, want)
		}
		if _, duplicate := seenEvidence[testCase.Evidence]; duplicate {
			t.Fatalf("%s reuses evidence token %q", ruleID, testCase.Evidence)
		}
		seenEvidence[testCase.Evidence] = struct{}{}
		targets, registered := compatibilityV02StatsByEvidenceRegistry[testCase.Evidence]
		if !registered || len(targets) == 0 {
			t.Fatalf("evidence token %q has no concrete test target", testCase.Evidence)
		}
		for _, target := range targets {
			requireCompatibilityV02StatsByEvidenceTarget(
				t,
				repositoryRoot,
				testCase.Evidence,
				target,
			)
		}
	}
	for caseName := range wantEvidence {
		if _, found := seenCases[caseName]; !found {
			t.Errorf("%s is missing required case %q", ruleID, caseName)
		}
	}
	for evidence := range compatibilityV02StatsByEvidenceRegistry {
		if _, used := seenEvidence[evidence]; !used {
			t.Errorf("stats BY evidence registry contains stale token %q", evidence)
		}
	}
}

func requireCompatibilityV02StatsByEvidenceTarget(
	t *testing.T,
	repositoryRoot, evidence string,
	target compatibilityV02StatsByEvidenceTarget,
) {
	t.Helper()
	if target.Path == "" || target.Identity == "" ||
		!strings.HasSuffix(target.Path, "_test.go") {
		t.Fatalf("evidence %q contains an invalid target: %+v", evidence, target)
	}
	path := filepath.Clean(filepath.Join(repositoryRoot, target.Path))
	relative, err := filepath.Rel(repositoryRoot, path)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		t.Fatalf("evidence %q target escapes repository: %+v", evidence, target)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read evidence %q target %s: %v", evidence, target.Path, err)
	}
	declaration := regexp.MustCompile(
		`(?m)^func\s+` + regexp.QuoteMeta(target.Identity) + `\s*\(`,
	)
	if !declaration.Match(contents) {
		t.Fatalf("evidence %q target %s does not declare %q", evidence, target.Path, target.Identity)
	}
}
