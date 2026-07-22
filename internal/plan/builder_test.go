package plan

import (
	"slices"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildIntersectsTrustedIndexScope(t *testing.T) {
	t.Parallel()

	query := mustParse(t, `index=gradethis trace_id="abc" | sort -_time | table _time level message | head 20`)
	logical, err := Build(query, testScope([]string{"internal", "gradethis"}, []string{"gradethis"}))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.EffectiveIndexes, []string{"gradethis"}) {
		t.Fatalf("effective indexes = %v", logical.EffectiveIndexes)
	}
	if len(logical.Operators) != 5 {
		t.Fatalf("operator count = %d, want 5", len(logical.Operators))
	}
	scan := logical.Operators[0].(*Scan)
	if scan.TenantID != "tenant-1" || !slices.Equal(scan.Indexes, []string{"gradethis"}) {
		t.Fatalf("scan = %#v", scan)
	}
	if scan.VisibilityCutoff != 41 {
		t.Fatalf("visibility cutoff = %d, want 41", scan.VisibilityCutoff)
	}
	if !scan.Earliest.Equal(time.Date(2026, 7, 21, 8, 0, 0, 0, time.UTC)) || !scan.Latest.Equal(time.Date(2026, 7, 21, 9, 0, 0, 0, time.UTC)) {
		t.Fatalf("scan range = [%v, %v)", scan.Earliest, scan.Latest)
	}
	sortOp := logical.Operators[2].(*Sort)
	if len(sortOp.Keys) != 1 || sortOp.Keys[0].Field.Name != "_time" || !sortOp.Keys[0].Descending {
		t.Fatalf("sort = %#v", sortOp)
	}
	project := logical.Operators[3].(*Project)
	if project.Mode != ProjectModeTable || !slices.Equal(logical.OutputFields, []string{"_time", "level", "message"}) {
		t.Fatalf("project/output = %#v / %v", project, logical.OutputFields)
	}
}

func TestBuildRejectsExplicitForbiddenIndex(t *testing.T) {
	t.Parallel()

	tests := []string{
		`index=secret`,
		`index=gradethis OR index=secret`,
		`index=gradethis | search index=secret`,
	}
	for _, source := range tests {
		source := source
		t.Run(source, func(t *testing.T) {
			t.Parallel()
			_, err := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, "SPL_INDEX_FORBIDDEN")
		})
	}
}

func TestBuildDoesNotTreatNegatedIndexAsRequestedScope(t *testing.T) {
	t.Parallel()

	queries := []string{`NOT index=secret`, `index!=secret`}
	for _, source := range queries {
		logical, err := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil))
		if err != nil {
			t.Fatalf("Build(%q): %v", source, err)
		}
		if !slices.Equal(logical.EffectiveIndexes, []string{"gradethis"}) {
			t.Fatalf("effective indexes = %v", logical.EffectiveIndexes)
		}
	}
}

func TestBuildRejectsWildcardIndexSelection(t *testing.T) {
	t.Parallel()

	_, err := Build(mustParse(t, `index=grade*`), testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_INDEX_SELECTOR")
}

func TestBuildPreservesLiteralTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source string
		kind   ValueKind
	}{
		{`status=500`, ValueKindInt64},
		{`counter=18446744073709551615`, ValueKindUint64},
		{`ratio=0.5`, ValueKindFloat64},
		{`ok=true`, ValueKindBool},
		{`value=null`, ValueKindNull},
		{`status="500"`, ValueKindString},
	}
	for _, test := range tests {
		test := test
		t.Run(test.source, func(t *testing.T) {
			t.Parallel()
			logical, err := Build(mustParse(t, test.source), testScope([]string{"gradethis"}, nil))
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			filter := logical.Operators[1].(*Filter)
			comparison := filter.Expression.(*ComparisonExpression)
			if comparison.Value.Kind != test.kind {
				t.Fatalf("kind = %v, want %v", comparison.Value.Kind, test.kind)
			}
			if comparison.Value.SourceText == "" {
				t.Fatalf("source text was not retained: %#v", comparison.Value)
			}
		})
	}
}

func TestBuildAppliesSplunkDefaultSortLimitButHonorsSortZero(t *testing.T) {
	t.Parallel()

	defaulted, err := Build(mustParse(t, `* | sort -_time`), testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatalf("Build(default sort): %v", err)
	}
	if got := defaulted.Operators[2].(*Sort).Limit; got != 10_000 {
		t.Fatalf("default sort limit = %d, want 10000", got)
	}

	unlimited, err := Build(mustParse(t, `* | sort 0 -_time`), testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatalf("Build(sort 0): %v", err)
	}
	if got := unlimited.Operators[2].(*Sort).Limit; got != 0 {
		t.Fatalf("sort 0 limit = %d, want unlimited", got)
	}
}

func TestBuildStatsCountReplacesEventSchema(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | stats count AS events by host, status`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"host", "status", "events"}) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}
	aggregate, ok := logical.Operators[len(logical.Operators)-1].(*Aggregate)
	if !ok {
		t.Fatalf("last operator = %T, want *Aggregate", logical.Operators[len(logical.Operators)-1])
	}
	if len(aggregate.GroupBy) != 2 || aggregate.GroupBy[0].Name != "host" ||
		aggregate.GroupBy[1].Name != "status" || len(aggregate.Measures) != 1 ||
		aggregate.Measures[0].Function != AggregateFunctionCountRows || aggregate.Measures[0].Output != "events" {
		t.Fatalf("aggregate = %#v", aggregate)
	}
}

func TestBuildStatsRejectsAmbiguousOutputAndUnsupportedFollowingStage(t *testing.T) {
	t.Parallel()

	_, err := Build(
		mustParse(t, `index=gradethis | stats count by count`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_DUPLICATE_FIELD")

	_, err = Build(
		mustParse(t, `index=gradethis | stats count by host | sort -count`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_AFTER_STATS")
}

func TestResolveFieldRejectsCompilerPrivateNamespace(t *testing.T) {
	t.Parallel()

	_, err := ResolveField(`__os_sort_time`, spl.Range{})
	assertDiagnosticCode(t, err, "SPL_RESERVED_FIELD")
}

func TestResolveFieldSupportsNestedAndEscapedDot(t *testing.T) {
	t.Parallel()

	nested, err := ResolveField(`http.response.status`, spl.Range{})
	if err != nil {
		t.Fatalf("ResolveField nested: %v", err)
	}
	if !slices.Equal(nested.Path, []string{"http", "response", "status"}) {
		t.Fatalf("nested path = %v", nested.Path)
	}
	literalDot, err := ResolveField(`labels.kubernetes\.io/app`, spl.Range{})
	if err != nil {
		t.Fatalf("ResolveField escaped: %v", err)
	}
	if !slices.Equal(literalDot.Path, []string{"labels", "kubernetes.io/app"}) {
		t.Fatalf("escaped path = %v", literalDot.Path)
	}
}

func TestResolveFieldRejectsAmbiguousPaths(t *testing.T) {
	t.Parallel()

	for _, field := range []string{"", ".foo", "foo.", "foo..bar", `foo\q`, "foo\x00bar"} {
		_, err := ResolveField(field, spl.Range{})
		assertDiagnosticCode(t, err, "SPL_INVALID_FIELD")
	}
}

func TestBuildRejectsInvalidSnapshot(t *testing.T) {
	t.Parallel()

	scope := testScope([]string{"gradethis"}, nil)
	scope.Latest = scope.Earliest
	_, err := Build(mustParse(t, `index=gradethis`), scope)
	assertDiagnosticCode(t, err, "SPL_INVALID_TIME_RANGE")
}

func TestBuildRequiresResolvedVisibilitySnapshotButAllowsEmptyTable(t *testing.T) {
	t.Parallel()

	scope := testScope([]string{"gradethis"}, nil)
	scope.VisibilityCutoff = nil
	_, err := Build(mustParse(t, `index=gradethis`), scope)
	assertDiagnosticCode(t, err, "SPL_INVALID_SNAPSHOT")

	emptyCutoff := uint64(0)
	scope.VisibilityCutoff = &emptyCutoff
	logical, err := Build(mustParse(t, `index=gradethis`), scope)
	if err != nil {
		t.Fatalf("Build empty-table snapshot: %v", err)
	}
	if got := logical.Operators[0].(*Scan).VisibilityCutoff; got != 0 {
		t.Fatalf("empty-table cutoff = %d, want 0", got)
	}
}

func testScope(authorized, requested []string) Scope {
	visibilityCutoff := uint64(41)
	return Scope{
		TenantID:          "tenant-1",
		AuthorizedIndexes: authorized,
		RequestedIndexes:  requested,
		Earliest:          time.Date(2026, 7, 21, 1, 0, 0, 0, time.FixedZone("test", -7*60*60)),
		Latest:            time.Date(2026, 7, 21, 2, 0, 0, 0, time.FixedZone("test", -7*60*60)),
		IndexTimeCutoff:   time.Date(2026, 7, 21, 2, 0, 1, 0, time.UTC),
		VisibilityCutoff:  &visibilityCutoff,
	}
}

func mustParse(t *testing.T, source string) *spl.Query {
	t.Helper()
	query, err := spl.Parse(source)
	if err != nil {
		t.Fatalf("Parse(%q): %v", source, err)
	}
	return query
}

func assertDiagnosticCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("error is nil, want %s", code)
	}
	diagnostic, ok := err.(*Diagnostic)
	if !ok {
		t.Fatalf("error = %T, want *Diagnostic", err)
	}
	if diagnostic.Code != code {
		t.Fatalf("code = %q, want %q", diagnostic.Code, code)
	}
}
