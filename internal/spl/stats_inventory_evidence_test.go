package spl_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// statsInventoryEvidenceTarget binds one stats-inventory ledger layer to the
// concrete test that earned its `tested` value. The ledger's evidence values
// are otherwise free-form strings, so this gate is what stops a `tested`
// claim from outliving the executable evidence behind it.
type statsInventoryEvidenceTarget struct {
	Layer    string
	Path     string
	Identity string
	// Subtest, when set, is the exact t.Run literal the target file must
	// carry for this ledger item.
	Subtest string
}

const (
	statsInventoryPinnedRuntimeLayer      = "pinned_runtime"
	statsInventoryClickHouseLoweringLayer = "clickhouse_lowering"
	statsInventoryRuntimeTestPath         = "internal/clickhouse/stats_inventory_runtime_integration_test.go"
	statsInventoryRuntimeTestIdentity     = "TestStatsInventoryPinnedRuntimeAgainstClickHouse"
)

func statsInventoryPinnedRuntimeTarget(id string) statsInventoryEvidenceTarget {
	return statsInventoryEvidenceTarget{
		Layer:    statsInventoryPinnedRuntimeLayer,
		Path:     statsInventoryRuntimeTestPath,
		Identity: statsInventoryRuntimeTestIdentity,
		Subtest:  id,
	}
}

// Each registered ledger item was flipped from not_tested by the named test.
// The registry is intentionally scoped to those flips: older `tested` layers
// are pinned by the focused suites listed under implementation_evidence.
var statsInventoryEvidenceRegistry = map[string][]statsInventoryEvidenceTarget{
	"partitions":                 {statsInventoryPinnedRuntimeTarget("partitions")},
	"delim":                      {statsInventoryPinnedRuntimeTarget("delim")},
	"eval-count-default-alias":   {statsInventoryPinnedRuntimeTarget("eval-count-default-alias")},
	"eval-numeric-default-alias": {statsInventoryPinnedRuntimeTarget("eval-numeric-default-alias")},
	"wildcard-implicit":          {statsInventoryPinnedRuntimeTarget("wildcard-implicit")},
	"input-quoted-exact":         {statsInventoryPinnedRuntimeTarget("input-quoted-exact")},
	"alias-quoted":               {statsInventoryPinnedRuntimeTarget("alias-quoted")},
	"by-quoted-field":            {statsInventoryPinnedRuntimeTarget("by-quoted-field")},
	"sparkline-wildcard-alias":   {statsInventoryPinnedRuntimeTarget("sparkline-wildcard-alias")},
	"alias-same-source-twice-rejection": {
		{
			Layer:    statsInventoryClickHouseLoweringLayer,
			Path:     "internal/clickhouse/stats_related_command_trust_test.go",
			Identity: "TestCompileStatsRejectsForgedDuplicateSourceRenames",
		},
		statsInventoryPinnedRuntimeTarget("alias-same-source-twice-rejection"),
	},
}

type statsInventoryLedgerItem struct {
	ID       string            `json:"id"`
	Evidence map[string]string `json:"evidence"`
}

type statsInventoryLedger struct {
	EvidenceValues         []string                   `json:"evidence_values"`
	EvidenceLayers         map[string]string          `json:"evidence_layers"`
	ImplementationEvidence []string                   `json:"implementation_evidence"`
	CommandOptions         []statsInventoryLedgerItem `json:"command_options"`
	Aggregates             []statsInventoryLedgerItem `json:"aggregates"`
	CrossCuttingSurface    []statsInventoryLedgerItem `json:"cross_cutting_surface"`
}

func TestStatsInventoryTestedLayersHaveExactExecutableEvidence(t *testing.T) {
	t.Parallel()

	corpusPath, _ := compatibilityCorpusPaths(t)
	encoded, err := os.ReadFile(corpusPath)
	if err != nil {
		t.Fatalf("read compatibility corpus: %v", err)
	}
	corpus, err := loadCompatibilityCorpus(encoded)
	if err != nil {
		t.Fatalf("load compatibility corpus: %v", err)
	}
	var ledger statsInventoryLedger
	if err := json.Unmarshal(corpus.StatsInventory, &ledger); err != nil {
		t.Fatalf("decode stats inventory: %v", err)
	}
	if !slices.Contains(ledger.EvidenceValues, "tested") {
		t.Fatalf("evidence_values = %q, want to include %q", ledger.EvidenceValues, "tested")
	}

	items := make(map[string]statsInventoryLedgerItem)
	for _, section := range [][]statsInventoryLedgerItem{
		ledger.CommandOptions,
		ledger.Aggregates,
		ledger.CrossCuttingSurface,
	} {
		for _, item := range section {
			if item.ID == "" {
				t.Fatal("stats inventory item has an empty id")
			}
			if _, duplicate := items[item.ID]; duplicate {
				t.Fatalf("stats inventory item %q is declared twice", item.ID)
			}
			items[item.ID] = item
		}
	}

	repositoryRoot := filepath.Clean(filepath.Join(filepath.Dir(corpusPath), "..", "..", ".."))
	for id, targets := range statsInventoryEvidenceRegistry {
		item, present := items[id]
		if !present {
			t.Errorf("stats inventory evidence registry contains stale item %q", id)
			continue
		}
		if len(targets) == 0 {
			t.Fatalf("stats inventory item %q has an empty evidence target list", id)
		}
		seenLayers := make(map[string]struct{}, len(targets))
		for _, target := range targets {
			if _, known := ledger.EvidenceLayers[target.Layer]; !known {
				t.Fatalf("stats inventory item %q binds unknown evidence layer %q", id, target.Layer)
			}
			if _, duplicate := seenLayers[target.Layer]; duplicate {
				t.Fatalf("stats inventory item %q binds layer %q twice", id, target.Layer)
			}
			seenLayers[target.Layer] = struct{}{}
			if value := item.Evidence[target.Layer]; value != "tested" {
				t.Fatalf(
					"stats inventory item %q layer %q = %q, want %q for registered evidence %s %s",
					id, target.Layer, value, "tested", target.Path, target.Identity,
				)
			}
			if !slices.Contains(ledger.ImplementationEvidence, target.Path) {
				t.Errorf("stats inventory implementation_evidence does not list %s", target.Path)
			}
			requireStatsInventoryEvidenceTarget(t, repositoryRoot, id, target)
		}
	}
}

func requireStatsInventoryEvidenceTarget(
	t *testing.T,
	repositoryRoot, id string,
	target statsInventoryEvidenceTarget,
) {
	t.Helper()
	if target.Path == "" || target.Identity == "" ||
		!strings.HasSuffix(target.Path, "_test.go") {
		t.Fatalf("stats inventory item %q contains an invalid target: %+v", id, target)
	}
	path := filepath.Clean(filepath.Join(repositoryRoot, target.Path))
	relative, err := filepath.Rel(repositoryRoot, path)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		t.Fatalf("stats inventory item %q target escapes repository: %+v", id, target)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read stats inventory item %q target %s: %v", id, target.Path, err)
	}
	declaration := regexp.MustCompile(
		`(?m)^func\s+` + regexp.QuoteMeta(target.Identity) + `\s*\(`,
	)
	if !declaration.Match(contents) {
		t.Fatalf("stats inventory item %q target %s does not declare %q", id, target.Path, target.Identity)
	}
	if target.Subtest != "" {
		subtest := regexp.MustCompile(`t\.Run\(` + regexp.QuoteMeta(strconv.Quote(target.Subtest)) + `,`)
		if !subtest.Match(contents) {
			t.Fatalf("stats inventory item %q target %s does not run subtest %q", id, target.Path, target.Subtest)
		}
	}
}
