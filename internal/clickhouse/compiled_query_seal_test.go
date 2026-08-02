package clickhouse

import (
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestSealCompiledQueryReadScopeRejectsInvalidCompilerMarkers(t *testing.T) {
	t.Parallel()

	marker := func(ordinal int, value string) compiledReadScopeArgument {
		return compiledReadScopeArgument{ordinal: ordinal, value: value}
	}
	tests := []struct {
		name       string
		tenantID   string
		indexNames []string
		arguments  []any
		wantError  string
	}{
		{
			name:       "missing",
			tenantID:   "tenant-1",
			indexNames: []string{"gradethis"},
			arguments:  []any{marker(0, "tenant-1")},
			wantError:  "marker ordinal 1 is missing",
		},
		{
			name:       "duplicate",
			tenantID:   "tenant-1",
			indexNames: []string{"gradethis"},
			arguments: []any{
				marker(0, "tenant-1"),
				marker(0, "tenant-1"),
				marker(1, "gradethis"),
			},
			wantError: "marker ordinal 0 is duplicated",
		},
		{
			name:       "out of range",
			tenantID:   "tenant-1",
			indexNames: []string{"gradethis"},
			arguments: []any{
				marker(0, "tenant-1"),
				marker(1, "gradethis"),
				marker(2, "internal"),
			},
			wantError: "marker ordinal 2 is out of range",
		},
		{
			name:       "tenant value mismatch",
			tenantID:   "tenant-1",
			indexNames: []string{"gradethis"},
			arguments: []any{
				marker(0, "other-tenant"),
				marker(1, "gradethis"),
			},
			wantError: "marker ordinal 0 has an unexpected value",
		},
		{
			name:       "index value mismatch",
			tenantID:   "tenant-1",
			indexNames: []string{"gradethis"},
			arguments: []any{
				marker(0, "tenant-1"),
				marker(1, "internal"),
			},
			wantError: "marker ordinal 1 has an unexpected value",
		},
		{
			name:       "out of order",
			tenantID:   "tenant-1",
			indexNames: []string{"gradethis"},
			arguments: []any{
				marker(1, "gradethis"),
				marker(0, "tenant-1"),
			},
			wantError: "markers are out of order",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			compiled, err := sealCompiledQueryReadScope(
				CompiledQuery{SQL: "SELECT ?", Args: test.arguments},
				test.tenantID,
				test.indexNames,
			)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("seal error = %v, want containing %q", err, test.wantError)
			}
			if compiled.HasValidSQLSeal() {
				t.Fatal("invalid compiler markers produced a trusted query")
			}
		})
	}
}

func TestSealCompiledQueryReadScopeProducesOrderedDetachedEvidence(t *testing.T) {
	t.Parallel()

	indexNames := []string{"gradethis", "internal"}
	compiled, err := sealCompiledQueryReadScope(
		CompiledQuery{
			SQL: "SELECT ?, ?, ?",
			Args: []any{
				compiledReadScopeArgument{ordinal: 0, value: "tenant-1"},
				compiledReadScopeArgument{ordinal: 1, value: "gradethis"},
				compiledReadScopeArgument{ordinal: 2, value: "internal"},
			},
		},
		"tenant-1",
		indexNames,
	)
	if err != nil {
		t.Fatalf("seal read scope: %v", err)
	}
	indexNames[0] = "mutated-input"
	tenantID, sealedIndexes, ok := compiled.ReadScope()
	if !ok || tenantID != "tenant-1" || !slices.Equal(sealedIndexes, []string{"gradethis", "internal"}) {
		t.Fatalf("sealed read scope = %q, %v, %v", tenantID, sealedIndexes, ok)
	}

	mutated := compiled
	mutated.readScope.argumentPositions = slices.Clone(compiled.readScope.argumentPositions)
	mutated.readScope.argumentPositions[0], mutated.readScope.argumentPositions[1] =
		mutated.readScope.argumentPositions[1], mutated.readScope.argumentPositions[0]
	if mutated.HasValidSQLSeal() {
		t.Fatal("non-increasing argument positions retained a trusted seal")
	}
}

func TestCompilerSealsEveryMainQueryShapeAgainstSQLMutation(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | head 1`,
		`index=gradethis | timechart span=5m count BY level`,
		`index=gradethis | chart count OVER level BY message`,
	} {
		compiled := compileSPL(t, source)
		if !compiled.HasValidSQLSeal() {
			t.Fatalf("%q main compiler result is not sealed", source)
		}

		copied := compiled
		if !copied.HasValidSQLSeal() {
			t.Fatalf("%q copied compiler result lost its seal", source)
		}
		copied.Args = append(copied.Args, "mutable-driver-argument")
		if !copied.HasValidSQLSeal() {
			t.Fatalf("%q argument mutation invalidated the structural seal", source)
		}

		for _, suffix := range []string{
			" SETTINGS max_execution_time = 0",
			"; SELECT currentUser()",
			" /* post-compile mutation */",
		} {
			mutated := compiled
			mutated.SQL += suffix
			if mutated.HasValidSQLSeal() {
				t.Fatalf("%q accepted SQL suffix %q", source, suffix)
			}
		}
	}
}

func TestCompiledQuerySQLSealCannotBeForgedThroughPublicFields(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | head 1`)
	forged := CompiledQuery{
		SQL:          compiled.SQL,
		Args:         compiled.Args,
		OutputFields: compiled.OutputFields,
		Timechart:    compiled.Timechart,
		Chart:        compiled.Chart,
		SparseFields: compiled.SparseFields,
	}
	if forged.HasValidSQLSeal() {
		t.Fatal("public CompiledQuery fields forged a compiler SQL seal")
	}

	mutated := compiled
	mutated.SQL = strings.Clone(compiled.SQL)
	if !mutated.HasValidSQLSeal() {
		t.Fatal("byte-identical SQL copy unexpectedly invalidated the seal")
	}
}

func TestCompiledQuerySealBindsImmutableReadScope(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | head 1`)
	tenantID, indexNames, ok := compiled.ReadScope()
	if !ok || tenantID != "tenant-1" || !slices.Equal(indexNames, []string{"gradethis"}) {
		t.Fatalf("read scope = %q, %v, %v", tenantID, indexNames, ok)
	}

	indexNames[0] = "mutated-return-value"
	_, freshIndexes, freshOK := compiled.ReadScope()
	if !freshOK || !slices.Equal(freshIndexes, []string{"gradethis"}) {
		t.Fatalf("returned read scope aliases compiler state: %v, %v", freshIndexes, freshOK)
	}

	mutatedTenant := compiled
	mutatedTenant.readScope.tenantID = "other-tenant"
	if _, _, ok := mutatedTenant.ReadScope(); ok || mutatedTenant.HasValidSQLSeal() {
		t.Fatal("compiled query accepted a post-compilation tenant mutation")
	}

	mutatedIndexes := compiled
	mutatedIndexes.readScope.indexNames = slices.Clone(mutatedIndexes.readScope.indexNames)
	mutatedIndexes.readScope.indexNames[0] = "other-index"
	if _, _, ok := mutatedIndexes.ReadScope(); ok || mutatedIndexes.HasValidSQLSeal() {
		t.Fatal("compiled query accepted a post-compilation index mutation")
	}

	mutations := []struct {
		name   string
		mutate func(*CompiledQuery)
	}{
		{name: "tenant value", mutate: func(query *CompiledQuery) {
			query.Args[query.readScope.argumentPositions[0]] = "other-tenant"
		}},
		{name: "tenant type", mutate: func(query *CompiledQuery) {
			query.Args[query.readScope.argumentPositions[0]] = []byte("tenant-1")
		}},
		{name: "index value", mutate: func(query *CompiledQuery) {
			query.Args[query.readScope.argumentPositions[1]] = "other-index"
		}},
		{name: "truncated arguments", mutate: func(query *CompiledQuery) {
			query.Args = query.Args[:query.readScope.argumentPositions[1]]
		}},
	}
	for _, test := range mutations {
		mutated := compiled
		mutated.Args = slices.Clone(compiled.Args)
		test.mutate(&mutated)
		if _, _, ok := mutated.ReadScope(); ok {
			t.Errorf("compiled query accepted mutated %s", test.name)
		}
		if !mutated.HasValidSQLSeal() {
			t.Errorf("mutated %s unexpectedly changed the structural SQL seal", test.name)
		}
	}
}

func TestCompiledQuerySealRejectsReorderedMultiIndexBindings(t *testing.T) {
	t.Parallel()

	scope := testChartScope()
	scope.AuthorizedIndexes = []string{"gradethis", "internal"}
	compiled := compileSPLWithScope(
		t,
		`index=gradethis OR index=internal | head 1`,
		scope,
	)
	_, indexes, ok := compiled.ReadScope()
	if !ok || len(indexes) != 2 {
		t.Fatalf("compiled multi-index scope = %v, %v", indexes, ok)
	}
	mutated := compiled
	mutated.Args = slices.Clone(compiled.Args)
	first := compiled.readScope.argumentPositions[1]
	second := compiled.readScope.argumentPositions[2]
	mutated.Args[first], mutated.Args[second] = mutated.Args[second], mutated.Args[first]
	if _, _, ok := mutated.ReadScope(); ok {
		t.Fatal("compiled query accepted reordered index bind values")
	}
	if !mutated.HasValidSQLSeal() {
		t.Fatal("reordered bind values unexpectedly changed the structural SQL seal")
	}
}

func TestEverySearchDerivedCompilerQueryCarriesSealedReadScope(t *testing.T) {
	t.Parallel()

	logical := func() *plan.Query { return buildPlan(t, `index=gradethis level=error`) }
	ordinary, err := (Compiler{}).Compile(logical())
	if err != nil {
		t.Fatalf("Compile ordinary: %v", err)
	}
	timeline, err := (Compiler{}).CompileTimeline(logical(), validTimelineSpec())
	if err != nil {
		t.Fatalf("CompileTimeline: %v", err)
	}
	catalog, err := (Compiler{}).CompileFieldCatalog(logical(), FieldCatalogSpec{MaximumFields: 10})
	if err != nil {
		t.Fatalf("CompileFieldCatalog: %v", err)
	}
	summary, err := (Compiler{}).CompileFieldSummary(logical(), fieldSummaryTestSpec("level"))
	if err != nil {
		t.Fatalf("CompileFieldSummary: %v", err)
	}
	suggestions, err := (Compiler{}).CompileFieldSuggestions(logical(), FieldSuggestionSpec{MaximumFields: 10})
	if err != nil {
		t.Fatalf("CompileFieldSuggestions: %v", err)
	}

	queries := []struct {
		name  string
		query interface {
			ReadScope() (string, []string, bool)
		}
	}{
		{name: "ordinary", query: ordinary},
		{name: "timeline", query: timeline},
		{name: "field catalog", query: catalog},
		{name: "field summary", query: summary},
		{name: "field suggestions", query: suggestions},
	}
	for _, test := range queries {
		tenantID, indexNames, ok := test.query.ReadScope()
		if !ok || tenantID != "tenant-1" || !slices.Equal(indexNames, []string{"gradethis"}) {
			t.Errorf("%s read scope = %q, %v, %v", test.name, tenantID, indexNames, ok)
		}
	}

	mutatedTimeline := timeline
	mutatedTimeline.Args = slices.Clone(timeline.Args)
	mutatedTimeline.Args[timeline.readScope.argumentPositions[0]] = "other-tenant"
	if _, _, ok := mutatedTimeline.ReadScope(); ok {
		t.Fatal("timeline read scope accepted a tenant bind mutation")
	}
	mutatedTimeline = timeline
	mutatedTimeline.SQL += " /* mutation */"
	if _, _, ok := mutatedTimeline.ReadScope(); ok {
		t.Fatal("timeline read scope remained trusted after SQL mutation")
	}
	mutatedCatalog := catalog
	mutatedCatalog.Args = slices.Clone(catalog.Args)
	mutatedCatalog.Args[catalog.readScope.argumentPositions[1]] = "other-index"
	if _, _, ok := mutatedCatalog.ReadScope(); ok {
		t.Fatal("field-catalog read scope accepted an index bind mutation")
	}
	mutatedCatalog = catalog
	mutatedCatalog.SQL += " /* mutation */"
	if _, _, ok := mutatedCatalog.ReadScope(); ok {
		t.Fatal("field-catalog read scope remained trusted after SQL mutation")
	}
	mutatedSummary := summary
	mutatedSummary.Args = slices.Clone(summary.Args)
	mutatedSummary.Args[summary.readScope.argumentPositions[0]] = []byte("tenant-1")
	if _, _, ok := mutatedSummary.ReadScope(); ok {
		t.Fatal("field-summary read scope accepted a tenant bind type mutation")
	}
	mutatedSummary = summary
	mutatedSummary.SQL += " /* mutation */"
	if _, _, ok := mutatedSummary.ReadScope(); ok {
		t.Fatal("field-summary read scope remained trusted after SQL mutation")
	}
	mutatedSuggestions := suggestions
	mutatedSuggestions.Args = slices.Clone(suggestions.Args)
	mutatedSuggestions.Args = mutatedSuggestions.Args[:suggestions.readScope.argumentPositions[1]]
	if _, _, ok := mutatedSuggestions.ReadScope(); ok {
		t.Fatal("field-suggestions read scope accepted truncated bind arguments")
	}
	mutatedSuggestions = suggestions
	mutatedSuggestions.SQL += " /* mutation */"
	if _, _, ok := mutatedSuggestions.ReadScope(); ok {
		t.Fatal("field-suggestions read scope remained trusted after SQL mutation")
	}
}
