package clickhouse

import (
	"crypto/sha256"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestAutomaticLookupGroupFreezesParallelMatchBeforeAuthoredFilter(t *testing.T) {
	t.Parallel()

	authored := buildPlan(t, `index=gradethis status=200`)
	empty, err := knowledgeprogram.Prepare(knowledgeprogram.Input{})
	if err != nil {
		t.Fatalf("Prepare(empty knowledge): %v", err)
	}
	admitted, err := plan.InjectKnowledgePrelude(authored, empty)
	if err != nil {
		t.Fatalf("InjectKnowledgePrelude(): %v", err)
	}
	unrestricted, err := knowledge.CompileSelector(knowledge.SelectorSpec{})
	if err != nil {
		t.Fatalf("CompileSelector(unrestricted): %v", err)
	}
	hostOnly, err := knowledge.CompileSelector(knowledge.SelectorSpec{
		Dimensions: []knowledge.DimensionSpec{{
			Dimension: knowledge.DimensionHost,
			Patterns:  []string{"never-matches.example"},
		}},
	})
	if err != nil {
		t.Fatalf("CompileSelector(host): %v", err)
	}

	first := automaticLookupTestBinding(
		t,
		"auto-a",
		"auto_a",
		"service",
		"next_key",
		unrestricted,
		[]string{"service_id", "next_key"},
		[][]string{{"api", "from-first"}},
	)
	second := automaticLookupTestBinding(
		t,
		"auto-b",
		"auto_b",
		"next_key",
		"owner",
		hostOnly,
		[]string{"service_id", "owner"},
		[][]string{{"from-first", "must-not-chain"}},
	)
	injected, compiler, err := (Compiler{}).WithAutomaticLookupBindings(
		admitted,
		[]AutomaticLookupBinding{first, second},
		nil,
	)
	if err != nil {
		t.Fatalf("WithAutomaticLookupBindings(): %v", err)
	}
	if got := injected.Operators[1].LogicalName(); got != "AutomaticLookupGroup" {
		t.Fatalf("operator 1 = %q, want generated automatic group", got)
	}
	if _, ok := injected.Operators[2].(*plan.Filter); !ok {
		t.Fatalf("operator 2 = %T, want authored base Filter", injected.Operators[2])
	}

	compiled, err := compiler.Compile(injected)
	if err != nil {
		t.Fatalf("Compile(automatic group): %v", err)
	}
	for _, required := range []string{
		`ARRAY JOIN [tuple(`,
		`"__os_auto_lookup_selector_0"`,
		`"__os_auto_lookup_selector_1"`,
		`tupleElement("__os_auto_lookup_key_0_0", 1)`,
		`tupleElement("__os_auto_lookup_key_1_0", 1)`,
		`tupleElement("__os_auto_lookup_selector_1", 1) != toUInt8(0)`,
		`"__os_ko_selector_input_bytes_0"`,
		KnowledgeSelectorEventLimitMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("automatic SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(
		compiled.SQL,
		`"next_key" = "__os_lookup_table_1000001"`,
	) {
		t.Fatalf("second automatic lookup chained through first output:\n%s", compiled.SQL)
	}
	freeze := strings.Index(compiled.SQL, `ARRAY JOIN [tuple(`)
	firstJoin := strings.Index(compiled.SQL, `LEFT ANY JOIN "__os_lookup_table_1000000"`)
	secondJoin := strings.Index(compiled.SQL, `LEFT ANY JOIN "__os_lookup_table_1000001"`)
	if freeze < 0 || firstJoin <= freeze || secondJoin <= firstJoin {
		t.Fatalf(
			"automatic physical placement = freeze %d, first %d, second %d",
			freeze,
			firstJoin,
			secondJoin,
		)
	}
	if !compiled.HasValidExecutionSeal() || len(compiled.lookupTables) != 2 {
		t.Fatalf("automatic executable authority = seal %v, tables %#v", compiled.HasValidExecutionSeal(), compiled.lookupTables)
	}
	versions, ok := compiled.LookupAssetVersions()
	if !ok || len(versions) != 2 ||
		!slices.Equal(
			[]string{versions[0].ObjectID(), versions[1].ObjectID()},
			[]string{"asset-auto-a", "asset-auto-b"},
		) || !slices.Equal(
		[]string{versions[0].LookupID(), versions[1].LookupID()},
		[]string{"auto-a", "auto-b"},
	) || versions[0].LookupVersion() != 1 || versions[0].AssetVersion() != 1 ||
		versions[0].AssetID() != "asset-auto-a" || versions[0].SizeBytes() == 0 {
		t.Fatalf("automatic lookup provenance = %#v, %v", versions, ok)
	}
}

func automaticLookupTestBinding(
	t *testing.T,
	stableID string,
	name string,
	keyEventField string,
	outputEventField string,
	selector *knowledge.Selector,
	headers []string,
	rows [][]string,
) AutomaticLookupBinding {
	t.Helper()
	key, err := plan.ResolveField(keyEventField, spl.Range{})
	if err != nil {
		t.Fatalf("ResolveField(%q): %v", keyEventField, err)
	}
	output, err := plan.ResolveField(outputEventField, spl.Range{})
	if err != nil {
		t.Fatalf("ResolveField(%q): %v", outputEventField, err)
	}
	contract := plan.Lookup{
		DefinitionName: name,
		Keys: []plan.LookupKey{{
			LookupField: "service_id",
			EventField:  key,
		}},
		Outputs: []plan.LookupOutput{{
			LookupField: headers[1],
			EventField:  output,
		}},
		WriteMode: plan.LookupWriteModeOverwrite,
	}
	resolution, err := NewLookupResolutionWithContract(
		contract,
		stableID,
		1,
		"tenant-1",
		"asset-"+stableID,
		1,
		uint64(len(strings.Join(headers, ","))+1),
		sha256.Sum256([]byte("asset-"+stableID)),
		headers,
		rows,
	)
	if err != nil {
		t.Fatalf("NewLookupResolutionWithContract(%q): %v", name, err)
	}
	return AutomaticLookupBinding{
		StableID:   stableID,
		Lookup:     contract,
		Selector:   selector,
		Resolution: resolution,
	}
}
