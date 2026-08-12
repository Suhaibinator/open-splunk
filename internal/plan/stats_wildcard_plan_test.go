package plan

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

func TestBuildStatsExpandsWildcardAgainstClosedSchemaInFieldOrder(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | table display,delay,latency | stats count avg(*lay)`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	aggregate := logical.Operators[len(logical.Operators)-1].(*Aggregate)
	if len(aggregate.Measures) != 3 {
		t.Fatalf("measures = %#v", aggregate.Measures)
	}
	if aggregate.Measures[0].Function != AggregateFunctionCountRows ||
		aggregate.Measures[1].Function != AggregateFunctionAverage ||
		aggregate.Measures[1].Input.Name != "display" ||
		aggregate.Measures[2].Function != AggregateFunctionAverage ||
		aggregate.Measures[2].Input.Name != "delay" {
		t.Fatalf("measures = %#v", aggregate.Measures)
	}
	if !slices.Equal(logical.OutputFields, []string{"count", "avg(display)", "avg(delay)"}) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}
}

func TestBuildStatsExpandsImplicitWildcard(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | table bytes,latency | stats sum`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	aggregate := logical.Operators[len(logical.Operators)-1].(*Aggregate)
	if len(aggregate.Measures) != 2 ||
		aggregate.Measures[0].Input.Name != "bytes" ||
		aggregate.Measures[1].Input.Name != "latency" ||
		!slices.Equal(logical.OutputFields, []string{"sum(bytes)", "sum(latency)"}) {
		t.Fatalf("aggregate/output = %#v / %v", aggregate, logical.OutputFields)
	}
}

func TestBuildStatsWildcardAliasSubstitutesCapturesInSchemaOrder(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | table http_request_bytes,http_response_bytes,latency | stats avg(http_*_*) AS mean_*_* sum AS total_*`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{
		"mean_request_bytes",
		"mean_response_bytes",
		"total_http_request_bytes",
		"total_http_response_bytes",
		"total_latency",
	}) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}
	aggregate := logical.Operators[len(logical.Operators)-1].(*Aggregate)
	for index, measure := range aggregate.Measures {
		if measure.Output != logical.OutputFields[index] {
			t.Fatalf("measure[%d] = %#v", index, measure)
		}
	}
}

func TestBuildStatsWildcardPreservesClosedSchemaLiteralMatches(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		want   []string
	}{
		{
			source: `index=gradethis | table '.com','Product Name' | stats avg(*)`,
			want:   []string{"avg(.com)", "avg(Product Name)"},
		},
		{
			source: `index=gradethis | table '.com','Product Name' | stats avg(*) AS out_*`,
			want:   []string{"out_.com", "out_Product Name"},
		},
	} {
		logical, err := Build(
			mustParse(t, test.source),
			testScope([]string{"gradethis"}, nil),
		)
		if err != nil {
			t.Fatalf("Build(%q): %v", test.source, err)
		}
		if !slices.Equal(logical.OutputFields, test.want) {
			t.Fatalf("Build(%q) fields = %v, want %v", test.source, logical.OutputFields, test.want)
		}
		aggregate := logical.Operators[len(logical.Operators)-1].(*Aggregate)
		for _, measure := range aggregate.Measures {
			if !measure.OutputLiteral {
				t.Fatalf("Build(%q) measure = %#v, want literal output", test.source, measure)
			}
		}
	}
}

func TestBuildStatsExpandsSparklineWildcardAgainstClosedSchema(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | table _time,delay,xdelay,latency | stats sparkline(avg(*lay),5m) AS trend_*`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"trend_de", "trend_xde"}) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}
	aggregate := logical.Operators[len(logical.Operators)-1].(*Aggregate)
	if len(aggregate.Measures) != 2 {
		t.Fatalf("measures = %#v", aggregate.Measures)
	}
	for index, input := range []string{"delay", "xdelay"} {
		measure := aggregate.Measures[index]
		if measure.Sparkline == nil ||
			measure.Sparkline.Function != AggregateFunctionAverage ||
			measure.Sparkline.Input.Name != input ||
			measure.Output != logical.OutputFields[index] {
			t.Fatalf("measure[%d] = %#v", index, measure)
		}
	}
}

func TestBuildStatsWildcardRequiresClosedMatchingSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source string
		code   string
	}{
		{`index=gradethis | stats avg(*)`, "SPL_UNSUPPORTED_STATS_WILDCARD"},
		{`index=gradethis | stats avg`, "SPL_UNSUPPORTED_STATS_WILDCARD"},
		{`index=gradethis | stats sparkline(avg(*))`, "SPL_UNSUPPORTED_STATS_WILDCARD"},
		{`index=gradethis | table latency | stats avg(bytes*)`, "SPL_NO_MATCHING_STATS_FIELDS"},
		{`index=gradethis | table _time,latency | stats sparkline(avg(bytes*))`, "SPL_NO_MATCHING_STATS_FIELDS"},
	}
	for _, test := range tests {
		_, err := Build(
			mustParse(t, test.source),
			testScope([]string{"gradethis"}, nil),
		)
		assertDiagnosticCode(t, err, test.code)
	}
}

func TestBuildStatsWildcardAppliesExpandedResourceAndCollisionChecks(t *testing.T) {
	t.Parallel()

	fields := make([]string, 17)
	for index := range fields {
		fields[index] = fmt.Sprintf("field_%02d", index)
	}
	tooWide := `index=gradethis | table ` + strings.Join(fields, ",") + ` | stats avg(*)`
	_, err := Build(mustParse(t, tooWide), testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_QUERY_TOO_COMPLEX")

	_, err = Build(
		mustParse(t, `index=gradethis | table latency | stats avg(*) mean(*)`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_DUPLICATE_STATS_AGGREGATE")

	_, err = Build(
		mustParse(t, `index=gradethis | table x | stats avg(*) BY 'avg(x)'`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_DUPLICATE_FIELD")

	_, err = Build(
		mustParse(t, `index=gradethis | table left_bytes,right_bytes | stats avg(*_bytes) AS value_* sum(*_bytes) AS value_*`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_DUPLICATE_FIELD")
}
