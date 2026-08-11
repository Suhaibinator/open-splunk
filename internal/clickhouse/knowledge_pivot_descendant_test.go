package clickhouse

import (
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestPivotDescendantSourceColumnsRetainsReferencedKnowledgeSidecars(t *testing.T) {
	t.Parallel()

	fieldSidecar := quoteIdentifier("__os_ko_field_descendant_3_0")
	extractionSidecar := quoteIdentifier("__os_ko_extract_descendant_0_0")
	state := compileState{privateColumns: []string{
		quoteIdentifier("__os_ko_field_exists_3_0"),
		fieldSidecar,
		extractionSidecar,
		quoteIdentifier("__os_ko_extract_type_0_0"),
	}}
	got := pivotDescendantSourceColumns(
		state,
		fieldState{kind: fieldKindDynamic, descendantSQL: fieldSidecar},
		fieldState{kind: fieldKindDynamic, descendantSQL: extractionSidecar},
		fieldState{kind: fieldKindDynamic, descendantSQL: fieldSidecar},
	)
	want := []string{
		quoteIdentifier(internalFieldNamesColumn),
		fieldSidecar,
		extractionSidecar,
	}
	if !slices.Equal(got, want) {
		t.Fatalf("pivot descendant source columns = %v, want %v", got, want)
	}
}

func TestKnowledgeDescendantSidecarsCrossRuntimeWidePivotSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		source        string
		sourceCTE     string
		sidecarFields []string
	}{
		{
			name:          "count timechart extraction split",
			source:        `index=gradethis | timechart span=5m count BY regex_value`,
			sourceCTE:     "__os_timechart_source",
			sidecarFields: []string{"regex_value"},
		},
		{
			name:          "numeric timechart extraction split",
			source:        `index=gradethis | timechart span=5m sum(duration) BY regex_value`,
			sourceCTE:     "__os_timechart_source",
			sidecarFields: []string{"regex_value"},
		},
		{
			name:          "count chart generated row and extraction split",
			source:        `index=gradethis | chart count OVER calculated_value BY regex_value`,
			sourceCTE:     "__os_chart_source",
			sidecarFields: []string{"calculated_value", "regex_value"},
		},
		{
			name:          "numeric chart generated row and extraction split",
			source:        `index=gradethis | chart sum(duration) OVER calculated_value BY regex_value`,
			sourceCTE:     "__os_chart_source",
			sidecarFields: []string{"calculated_value", "regex_value"},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			compiled, state := compileKnowledgePivotForTest(t, test.source)

			projection := pivotSourceProjectionForTest(t, compiled.SQL, test.sourceCTE)
			for _, name := range test.sidecarFields {
				field, ok := state.visible[name]
				if !ok || field.descendantSQL == "" {
					t.Fatalf("knowledge field %q has no descendant sidecar", name)
				}
				if !strings.Contains(projection, field.descendantSQL) {
					t.Fatalf(
						"%s projection dropped %q descendant sidecar %s:\n%s",
						test.sourceCTE,
						name,
						field.descendantSQL,
						projection,
					)
				}
			}
			if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
				t.Fatalf("placeholder count = %d, args = %d", got, want)
			}
		})
	}
}

func compileKnowledgePivotForTest(t *testing.T, source string) (CompiledQuery, compileState) {
	t.Helper()
	base, err := plan.InjectKnowledgePrelude(
		buildPlan(t, `index=gradethis`),
		deferredMixedKnowledgeProgramForTest(t),
	)
	if err != nil {
		t.Fatalf("InjectKnowledgePrelude: %v", err)
	}
	capture, sealed, compileErr := compileCentralKnowledgeCapture(base)
	requireCentralKnowledgeCompilerBoundary(t, sealed.HasValidExecutionSeal(), compileErr)
	if !capture.called {
		t.Fatal("knowledge lowering did not reach the compiler finalizer")
	}

	logical := buildPlan(t, source)
	operator := logical.Operators[len(logical.Operators)-1]
	alias := quoteIdentifier("_stage_1")
	var compiled CompiledQuery
	switch operator := operator.(type) {
	case *plan.Timechart:
		scan, ok := logical.Operators[0].(*plan.Scan)
		if !ok {
			t.Fatal("timechart fixture has no scan")
		}
		compiled, err = compileTimechart(
			capture.relation,
			capture.state,
			slices.Clone(capture.args),
			operator,
			logical.OutputFields,
			logical.DynamicOutput,
			scan,
			alias,
		)
	case *plan.Chart:
		compiled, err = compileChart(
			capture.relation,
			capture.state,
			slices.Clone(capture.args),
			operator,
			logical.DynamicOutput,
			alias,
		)
	default:
		t.Fatalf("terminal fixture operator = %T, want chart or timechart", operator)
	}
	if err != nil {
		t.Fatalf("compile knowledge pivot: %v", err)
	}
	return compiled, capture.state
}

func pivotSourceProjectionForTest(t *testing.T, sql, cte string) string {
	t.Helper()
	marker := quoteIdentifier(cte) + " AS (SELECT "
	start := strings.Index(sql, marker)
	if start < 0 {
		t.Fatalf("compiled SQL contains no %s source CTE:\n%s", cte, sql)
	}
	remainder := sql[start+len(marker):]
	end := strings.Index(remainder, " FROM (")
	if end < 0 {
		t.Fatalf("%s source CTE contains no scoped relation:\n%s", cte, remainder)
	}
	return remainder[:end]
}
