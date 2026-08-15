package clickhouse

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

// chartAxisBreakCase is one pair of axes charted twice: once through the count
// compiler and once through the numeric compiler. Both spellings route their
// axis validation through the single shared resolver, so the two must agree
// bit-for-bit on acceptance, on the diagnostic they raise, and on the row
// contract they publish.
type chartAxisBreakCase struct {
	name     string
	upstream string
	row      string
	column   string
	// code is the diagnostic both compilers must raise, or "" when both must
	// accept the pivot.
	code string
	// rowKind and rowType pin the transport contract the shared resolver must
	// derive; they apply only when code is empty.
	rowKind ChartRowKind
	rowType string
}

func chartAxisBreakCases() []chartAxisBreakCase {
	return []chartAxisBreakCase{
		{
			name: "plain string axes", upstream: `index=gradethis`, row: "path", column: "level",
			rowKind: ChartRowKindString, rowType: "String",
		},
		{
			// _raw is the only Mixed row: it needs semantic Bytes provenance,
			// and the resolver fails closed when that provenance is missing.
			name: "canonical raw row is Mixed", upstream: `index=gradethis`, row: "_raw", column: "level",
			rowKind: ChartRowKindMixed, rowType: "String",
		},
		{
			name: "canonical time row", upstream: `index=gradethis`, row: "_time", column: "level",
			rowKind: ChartRowKindTime, rowType: "DateTime64(9, 'UTC')",
		},
		{
			name: "runtime typed row", upstream: `index=gradethis`, row: "metric", column: "level",
			rowKind: ChartRowKindString, rowType: "String",
		},
		{
			name:     "aggregate group row",
			upstream: `index=gradethis | stats count BY path, level`,
			row:      "path", column: "level",
			rowKind: ChartRowKindString, rowType: "String",
		},
		{
			name:     "projected-away row becomes the typed null column",
			upstream: `index=gradethis | table host level | fields - host`,
			row:      "host", column: "level",
			rowKind: ChartRowKindString, rowType: "String",
		},
		{
			name:     "projected-away column becomes the NULL series",
			upstream: `index=gradethis | table path level | fields - level`,
			row:      "path", column: "level",
			rowKind: ChartRowKindString, rowType: "String",
		},
		{
			name:     "static null column literal",
			upstream: `index=gradethis | eval n=null`,
			row:      "path", column: "n",
			rowKind: ChartRowKindString, rowType: "String",
		},
		{
			name:     "negative numeric literal row",
			upstream: `index=gradethis | eval offset=-3`,
			row:      "offset", column: "level",
			rowKind: ChartRowKindDouble, rowType: "Float64",
		},
		{
			name:     "unsigned ceiling literal row",
			upstream: `index=gradethis | eval big=18446744073709551615`,
			row:      "big", column: "level",
			rowKind: ChartRowKindUnsigned, rowType: "UInt64",
		},
		{
			name:     "bool row",
			upstream: `index=gradethis | eval flag=true`,
			row:      "flag", column: "level",
			rowKind: ChartRowKindBool, rowType: "Bool",
		},

		// Reserved series labels are rejected on either axis by the shared
		// resolver's label-collision guard, before any field is resolved.
		{name: "NULL row", upstream: `index=gradethis`, row: "NULL", column: "level", code: "SPL_UNSUPPORTED_CHART_FIELD_TYPE"},
		{name: "NULL column", upstream: `index=gradethis`, row: "path", column: "NULL", code: "SPL_UNSUPPORTED_CHART_FIELD_TYPE"},
		{name: "OTHER row", upstream: `index=gradethis`, row: "OTHER", column: "level", code: "SPL_UNSUPPORTED_CHART_FIELD_TYPE"},
		{name: "OTHER column", upstream: `index=gradethis`, row: "path", column: "OTHER", code: "SPL_UNSUPPORTED_CHART_FIELD_TYPE"},

		// The reserved Dynamic payload container is unusable as an axis while
		// the upstream schema is still open.
		{name: "fields row", upstream: `index=gradethis`, row: "fields", column: "level", code: "SPL_UNSUPPORTED_CHART_FIELD_TYPE"},
		{name: "fields column", upstream: `index=gradethis`, row: "path", column: "fields", code: "SPL_UNSUPPORTED_CHART_FIELD_TYPE"},

		// Non-string column domains stay fatal, and stay fatal identically for
		// both measure families.
		{name: "numeric column", upstream: `index=gradethis`, row: "path", column: "severity", code: "SPL_UNSUPPORTED_CHART_FIELD_TYPE"},
		{name: "timestamp column", upstream: `index=gradethis`, row: "path", column: "_time", code: "SPL_UNSUPPORTED_CHART_FIELD_TYPE"},
		{name: "bool column", upstream: `index=gradethis | eval flag=true`, row: "path", column: "flag", code: "SPL_UNSUPPORTED_CHART_FIELD_TYPE"},
		{name: "aggregate column", upstream: `index=gradethis | stats count BY path, level`, row: "path", column: "count", code: "SPL_UNSUPPORTED_CHART_FIELD_TYPE"},

		// Multivalue axes are the other shared rejection, on either side.
		{
			name:     "multivalue row",
			upstream: `index=gradethis | stats values(user) AS users BY path, level`,
			row:      "users", column: "level", code: "SPL_UNSUPPORTED_MULTIVALUE_USAGE",
		},
		{
			name:     "multivalue column",
			upstream: `index=gradethis | stats values(user) AS users BY path, level`,
			row:      "path", column: "users", code: "SPL_UNSUPPORTED_MULTIVALUE_USAGE",
		},
	}
}

// TestChartAxisResolutionAgreesBetweenCountAndNumericMeasures pins that the two
// chart compilers share one axis contract. For every hostile axis pair the
// count pivot and the numeric pivot must both compile or both fail, with the
// same diagnostic code and the same source range, and — when they compile —
// publish the same row kind and physical row type. A divergence would make the
// accepted axis surface depend on which aggregate the user asked for.
func TestChartAxisResolutionAgreesBetweenCountAndNumericMeasures(t *testing.T) {
	t.Parallel()

	for _, test := range chartAxisBreakCases() {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			pivot := " OVER " + test.row + " BY " + test.column
			countSource := test.upstream + " | chart count" + pivot
			numericSource := test.upstream + " | chart avg(measurement)" + pivot

			countQuery, countErr := chartAxisBreakCompile(t, countSource)
			numericQuery, numericErr := chartAxisBreakCompile(t, numericSource)

			if (countErr == nil) != (numericErr == nil) {
				t.Fatalf("count error = %v, numeric error = %v: the shared axis resolver disagreed", countErr, numericErr)
			}
			if countErr != nil {
				countDiagnostic := chartAxisBreakDiagnostic(t, countSource, countErr, test.code)
				numericDiagnostic := chartAxisBreakDiagnostic(t, numericSource, numericErr, test.code)
				if countDiagnostic.Code != numericDiagnostic.Code ||
					countDiagnostic.Message != numericDiagnostic.Message ||
					!slices.Equal(countDiagnostic.Suggestions, numericDiagnostic.Suggestions) {
					t.Fatalf("count diagnostic = %#v, numeric diagnostic = %#v", countDiagnostic, numericDiagnostic)
				}
				// The two sources differ only in the measure text, so the
				// ranges must cover the same token, not the same offsets.
				countText := chartAxisBreakRangeText(t, countSource, countDiagnostic)
				numericText := chartAxisBreakRangeText(t, numericSource, numericDiagnostic)
				if countText != numericText {
					t.Fatalf("count diagnostic covered %q, numeric covered %q", countText, numericText)
				}
				return
			}
			if test.code != "" {
				t.Fatalf("%q compiled but %s was expected", countSource, test.code)
			}
			if countQuery.Chart == nil || numericQuery.Chart == nil {
				t.Fatalf("chart contracts = %#v / %#v", countQuery.Chart, numericQuery.Chart)
			}
			if countQuery.Chart.RowField != numericQuery.Chart.RowField ||
				countQuery.Chart.RowKind != numericQuery.Chart.RowKind ||
				countQuery.Chart.RowDatabaseType != numericQuery.Chart.RowDatabaseType ||
				countQuery.Chart.RowSemanticBytes != numericQuery.Chart.RowSemanticBytes ||
				countQuery.Chart.RowLimit != numericQuery.Chart.RowLimit ||
				countQuery.Chart.MaxSeries != numericQuery.Chart.MaxSeries ||
				countQuery.Chart.MaxLabelBytes != numericQuery.Chart.MaxLabelBytes {
				t.Fatalf("row contracts diverged: count %#v, numeric %#v", countQuery.Chart, numericQuery.Chart)
			}
			if countQuery.Chart.RowKind != test.rowKind || countQuery.Chart.RowDatabaseType != test.rowType {
				t.Fatalf("row contract = (%v, %q), want (%v, %q)",
					countQuery.Chart.RowKind, countQuery.Chart.RowDatabaseType, test.rowKind, test.rowType)
			}
			if (countQuery.Chart.RowKind == ChartRowKindMixed) != countQuery.Chart.RowSemanticBytes {
				t.Fatalf("Mixed row and semantic-Bytes provenance disagree: %#v", countQuery.Chart)
			}
			for _, compiled := range []CompiledQuery{countQuery, numericQuery} {
				if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
					t.Fatalf("placeholders = %d, args = %d:\n%s", got, want, compiled.SQL)
				}
			}
		})
	}
}

// TestChartAxisResolutionRejectsBeforeResolvingFields pins the order the shared
// resolver checks in: the bounding-contract and reserved-label guards run over
// the *declared* axis names, so a reserved label is rejected even when the same
// name is also an unresolvable or multivalue field. Reordering the guards would
// leak a different diagnostic for the same query.
func TestChartAxisResolutionRejectsBeforeResolvingFields(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		code   string
	}{
		// A multivalue field renamed onto a reserved label must still fail the
		// label guard, not the multivalue guard.
		{`index=gradethis | stats values(user) AS users BY path | rename users AS NULL | chart count OVER path BY NULL`, "SPL_UNSUPPORTED_CHART_FIELD_TYPE"},
		{`index=gradethis | stats values(user) AS users BY path, level | rename users AS OTHER | chart avg(measurement) OVER OTHER BY level`, "SPL_UNSUPPORTED_CHART_FIELD_TYPE"},
		// A numeric column named NULL is a label collision first.
		{`index=gradethis | rename severity AS NULL | chart count OVER path BY NULL`, "SPL_UNSUPPORTED_CHART_FIELD_TYPE"},
	} {
		_, err := chartAxisBreakCompile(t, test.source)
		diagnostic := chartAxisBreakDiagnostic(t, test.source, err, test.code)
		if !strings.Contains(diagnostic.Message, "reserved chart series names") {
			t.Fatalf("Compile(%q) message = %q, want the reserved-series-name guard", test.source, diagnostic.Message)
		}
	}
}

func chartAxisBreakRangeText(t *testing.T, source string, diagnostic *plan.Diagnostic) string {
	t.Helper()
	start := diagnostic.Range.Start.Offset
	end := diagnostic.Range.End.Offset
	if start < 0 || end > len(source) || start > end {
		t.Fatalf("diagnostic range %#v is outside %q", diagnostic.Range, source)
	}
	return source[start:end]
}

func chartAxisBreakCompile(t *testing.T, source string) (CompiledQuery, error) {
	t.Helper()
	logical, err := plan.Build(mustParseChartBreakPipeline(t, source), testChartScope())
	if err != nil {
		return CompiledQuery{}, err
	}
	return (Compiler{}).Compile(logical)
}

func chartAxisBreakDiagnostic(t *testing.T, source string, err error, code string) *plan.Diagnostic {
	t.Helper()
	if err == nil {
		t.Fatalf("Compile(%q) succeeded, want %s", source, code)
	}
	diagnostic := &plan.Diagnostic{}
	if !errors.As(err, &diagnostic) {
		t.Fatalf("Compile(%q) error = %#v, want a plan diagnostic (%s)", source, err, code)
	}
	if code != "" && diagnostic.Code != code {
		t.Fatalf("Compile(%q) diagnostic code = %q, want %q", source, diagnostic.Code, code)
	}
	return diagnostic
}
