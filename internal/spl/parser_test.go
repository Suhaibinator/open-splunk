package spl

import (
	"strings"
	"testing"
)

func TestParseGradeThisEventSearch(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=gradethis trace_id="abc-123" | sort _time | table _time level layer logger message`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	base, ok := query.Search.(*BinaryExpr)
	if !ok || base.Op != BoolOpAnd {
		t.Fatalf("base expression = %#v, want implicit AND", query.Search)
	}
	assertComparison(t, base.Left, "index", CompareOpEqual, "gradethis", false)
	assertComparison(t, base.Right, "trace_id", CompareOpEqual, "abc-123", true)

	if len(query.Commands) != 2 {
		t.Fatalf("command count = %d, want 2", len(query.Commands))
	}
	sortCommand, ok := query.Commands[0].(*SortCommand)
	if !ok || len(sortCommand.Fields) != 1 || sortCommand.Fields[0].Field != "_time" || sortCommand.Fields[0].Descending {
		t.Fatalf("sort command = %#v", query.Commands[0])
	}
	tableCommand, ok := query.Commands[1].(*TableCommand)
	if !ok {
		t.Fatalf("table command = %T", query.Commands[1])
	}
	wantFields := []string{"_time", "level", "layer", "logger", "message"}
	if strings.Join(tableCommand.Fields, ",") != strings.Join(wantFields, ",") {
		t.Fatalf("table fields = %v, want %v", tableCommand.Fields, wantFields)
	}
}

func TestBaseSearchORPrecedesAND(t *testing.T) {
	t.Parallel()

	query, err := Parse(`level=ERROR OR level=WARN index=gradethis`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	and, ok := query.Search.(*BinaryExpr)
	if !ok || and.Op != BoolOpAnd {
		t.Fatalf("root = %#v, want AND", query.Search)
	}
	or, ok := and.Left.(*BinaryExpr)
	if !ok || or.Op != BoolOpOr {
		t.Fatalf("left = %#v, want OR", and.Left)
	}
	assertComparison(t, or.Left, "level", CompareOpEqual, "ERROR", false)
	assertComparison(t, or.Right, "level", CompareOpEqual, "WARN", false)
	assertComparison(t, and.Right, "index", CompareOpEqual, "gradethis", false)
}

func TestParenthesesAndNOTOverridePrecedence(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=gradethis (level=ERROR OR NOT level=WARN)`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	and := query.Search.(*BinaryExpr)
	or := and.Right.(*BinaryExpr)
	if _, ok := or.Right.(*NotExpr); !ok {
		t.Fatalf("right = %T, want *NotExpr", or.Right)
	}
}

func TestParseProjectionSortAndLimits(t *testing.T) {
	t.Parallel()

	query, err := Parse(`"connection refused" | fields - token,password | sort 20 -_time,+host | head 10 | tail 3`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	term, ok := query.Search.(*TermExpr)
	if !ok || term.Value != "connection refused" || !term.Quoted {
		t.Fatalf("term = %#v", query.Search)
	}
	fields := query.Commands[0].(*FieldsCommand)
	if !fields.Exclude || strings.Join(fields.Fields, ",") != "token,password" {
		t.Fatalf("fields = %#v", fields)
	}
	sortCommand := query.Commands[1].(*SortCommand)
	if sortCommand.Limit != 20 || len(sortCommand.Fields) != 2 || !sortCommand.Fields[0].Descending || sortCommand.Fields[1].Descending {
		t.Fatalf("sort = %#v", sortCommand)
	}
	if got := query.Commands[2].(*LimitCommand); got.Name() != "head" || got.Count != 10 {
		t.Fatalf("head = %#v", got)
	}
	if got := query.Commands[3].(*LimitCommand); got.Name() != "tail" || got.Count != 3 {
		t.Fatalf("tail = %#v", got)
	}
}

func TestParseSortDistinguishesDefaultBoundFromExplicitUnlimited(t *testing.T) {
	t.Parallel()

	defaulted, err := Parse(`* | sort -_time`)
	if err != nil {
		t.Fatalf("Parse(default): %v", err)
	}
	defaultSort := defaulted.Commands[0].(*SortCommand)
	if defaultSort.LimitSpecified {
		t.Fatalf("omitted sort count marked specified: %#v", defaultSort)
	}

	unlimited, err := Parse(`* | sort 0 -_time`)
	if err != nil {
		t.Fatalf("Parse(unlimited): %v", err)
	}
	unlimitedSort := unlimited.Commands[0].(*SortCommand)
	if !unlimitedSort.LimitSpecified || unlimitedSort.Limit != 0 {
		t.Fatalf("explicit unlimited sort = %#v", unlimitedSort)
	}
}

func TestParseSortRejectsAmbiguousOrMalformedArguments(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`* | sort desc status`,
		`* | sort , status`,
		`* | sort status,,host`,
		`* | sort status,`,
		`* | sort 18446744073709551616 status`,
	} {
		if _, err := Parse(source); err == nil {
			t.Fatalf("Parse(%q) unexpectedly succeeded", source)
		}
	}
}

func TestPipelineSearchUsesSearchPrecedence(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=gradethis | search level=ERROR OR level=WARN host=api-1`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*SearchCommand)
	root := command.Expression.(*BinaryExpr)
	if root.Op != BoolOpAnd || root.Left.(*BinaryExpr).Op != BoolOpOr {
		t.Fatalf("search expression = %#v", command.Expression)
	}
}

func TestLiteralsRetainTypeIntent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		query string
		kind  LiteralKind
		text  string
	}{
		{`status>=500`, LiteralKindInteger, "500"},
		{`ratio<0.75`, LiteralKindFloat, "0.75"},
		{`success=true`, LiteralKindBool, "true"},
		{`deleted=null`, LiteralKindNull, "null"},
		{`duration>=-1.5`, LiteralKindFloat, "-1.5"},
		{`code="500"`, LiteralKindString, "500"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.query, func(t *testing.T) {
			t.Parallel()
			query, err := Parse(test.query)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			comparison := query.Search.(*ComparisonExpr)
			if comparison.Value.Kind != test.kind || comparison.Value.Text != test.text {
				t.Fatalf("literal = %#v, want kind %v text %q", comparison.Value, test.kind, test.text)
			}
		})
	}
}

func TestOutOfRangeFloatRemainsNumericIntent(t *testing.T) {
	t.Parallel()

	query, err := Parse(`ratio=1e309`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	comparison := query.Search.(*ComparisonExpr)
	if comparison.Value.Kind != LiteralKindFloat {
		t.Fatalf("literal kind = %v, want float intent", comparison.Value.Kind)
	}
}

func TestUnsupportedCommandHasStageAndLocation(t *testing.T) {
	t.Parallel()

	_, err := Parse("index=gradethis\n| sort _time\n| transaction trace_id")
	if err == nil {
		t.Fatal("Parse succeeded, want error")
	}
	diagnostic, ok := err.(*Diagnostic)
	if !ok {
		t.Fatalf("error = %T, want *Diagnostic", err)
	}
	if diagnostic.Code != "SPL_UNSUPPORTED_COMMAND" {
		t.Fatalf("code = %q", diagnostic.Code)
	}
	if diagnostic.Range.Start.Line != 3 || diagnostic.Range.Start.Column != 3 {
		t.Fatalf("position = %#v, want line 3 column 3", diagnostic.Range.Start)
	}
	if !strings.Contains(diagnostic.Message, `unsupported command "transaction" at pipeline stage 2`) {
		t.Fatalf("message = %q", diagnostic.Message)
	}
}

func TestParseStatsCountAndGroupedAlias(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		source     string
		alias      string
		groupNames []string
	}{
		{name: "global count", source: `index=main | stats count`, alias: "count"},
		{
			name:       "aliased grouped count",
			source:     "index=main\n| stats count AS events BY host, source",
			alias:      "events",
			groupNames: []string{"host", "source"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			query, err := Parse(test.source)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if len(query.Commands) != 1 {
				t.Fatalf("command count = %d, want 1", len(query.Commands))
			}
			command, ok := query.Commands[0].(*StatsCommand)
			if !ok {
				t.Fatalf("command = %T, want *StatsCommand", query.Commands[0])
			}
			if command.Aggregate.Function != AggregateFunctionCount || command.Aggregate.Alias != test.alias {
				t.Fatalf("aggregate = %#v, want count AS %q", command.Aggregate, test.alias)
			}
			if len(command.GroupBy) != len(test.groupNames) {
				t.Fatalf("group fields = %#v, want %v", command.GroupBy, test.groupNames)
			}
			for index, want := range test.groupNames {
				if command.GroupBy[index].Name != want {
					t.Fatalf("group field %d = %q, want %q", index, command.GroupBy[index].Name, want)
				}
			}
		})
	}
}

func TestUnsupportedStatsAggregatesAreSourceLocated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		code   string
		line   int
		column int
	}{
		{"other function", "index=main\n| stats sum(bytes)", "SPL_UNSUPPORTED_STATS_AGGREGATE", 2, 9},
		{"count argument", `* | stats count(host)`, "SPL_UNSUPPORTED_STATS_AGGREGATE", 1, 16},
		{"second aggregate", `* | stats count, dc(host)`, "SPL_UNSUPPORTED_STATS_AGGREGATE", 1, 16},
		{"missing AS", `* | stats count total`, "SPL_UNSUPPORTED_STATS_SYNTAX", 1, 17},
		{"missing group field", `* | stats count by`, "SPL_EXPECTED_FIELD", 1, 19},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Parse(test.source)
			if err == nil {
				t.Fatal("Parse succeeded, want error")
			}
			diagnostic, ok := err.(*Diagnostic)
			if !ok {
				t.Fatalf("error = %T, want *Diagnostic", err)
			}
			if diagnostic.Code != test.code || diagnostic.Range.Start.Line != test.line || diagnostic.Range.Start.Column != test.column {
				t.Fatalf("diagnostic = %#v, want %s at %d:%d", diagnostic, test.code, test.line, test.column)
			}
		})
	}
}

func TestParseTopSingleFieldAndLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		field  string
		limit  uint64
	}{
		{name: "default", source: `index=main | top message`, field: "message", limit: 10},
		{name: "limit option", source: `index=main | top limit=20 message`, field: "message", limit: 20},
		{name: "positional limit", source: `index=main | top 5 status`, field: "status", limit: 5},
		{name: "unlimited", source: `index=main | top limit=0 host`, field: "host", limit: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			query, err := Parse(test.source)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			command, ok := query.Commands[0].(*TopCommand)
			if !ok || command.Field != test.field || command.Limit != test.limit {
				t.Fatalf("top command = %#v, want field %q limit %d", query.Commands[0], test.field, test.limit)
			}
			if command.FieldRange.Start.Column <= command.Range.Start.Column {
				t.Fatalf("field range = %#v, command range = %#v", command.FieldRange, command.Range)
			}
		})
	}
}

func TestParseTopRejectsUnsupportedOrMalformedSyntax(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		code   string
	}{
		{name: "missing field", source: `index=main | top`, code: "SPL_EXPECTED_FIELD"},
		{name: "missing limit", source: `index=main | top limit= message`, code: "SPL_INVALID_ARGUMENT"},
		{name: "negative limit", source: `index=main | top limit=-1 message`, code: "SPL_INVALID_ARGUMENT"},
		{name: "negative positional limit", source: `index=main | top -1 message`, code: "SPL_INVALID_ARGUMENT"},
		{name: "limit overflow", source: `index=main | top limit=18446744073709551616 message`, code: "SPL_NUMBER_OUT_OF_RANGE"},
		{name: "multiple fields", source: `index=main | top message, host`, code: "SPL_UNSUPPORTED_TOP_SYNTAX"},
		{name: "by clause", source: `index=main | top message BY host`, code: "SPL_UNSUPPORTED_TOP_SYNTAX"},
		{name: "unsupported option", source: `index=main | top showperc=false message`, code: "SPL_UNSUPPORTED_TOP_SYNTAX"},
		{name: "wildcard field", source: `index=main | top mes*`, code: "SPL_UNSUPPORTED_TOP_SYNTAX"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Parse(test.source)
			if err == nil {
				t.Fatal("Parse succeeded, want error")
			}
			diagnostic, ok := err.(*Diagnostic)
			if !ok || diagnostic.Code != test.code {
				t.Fatalf("diagnostic = %#v, want %s", err, test.code)
			}
		})
	}
}

func TestParseTopLocatesUnsupportedOptionAfterLimit(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | top limit=20 showperc=false message`,
		`index=main | top 20 showperc=false message`,
	} {
		_, err := Parse(source)
		if err == nil {
			t.Fatalf("Parse(%q) succeeded, want error", source)
		}
		diagnostic, ok := err.(*Diagnostic)
		if !ok || diagnostic.Code != "SPL_UNSUPPORTED_TOP_SYNTAX" ||
			!strings.Contains(diagnostic.Message, `option "showperc"`) {
			t.Fatalf("diagnostic = %#v", err)
		}
		if got := source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset]; got != "showperc" {
			t.Fatalf("diagnostic source = %q, want showperc", got)
		}
	}
}

func TestParseErrorsAreSourceLocated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		query  string
		code   string
		line   int
		column int
	}{
		{"unterminated quote", `index="gradethis`, "SPL_UNTERMINATED_STRING", 1, 7},
		{"missing value", `index= | head`, "SPL_EXPECTED_LITERAL", 1, 8},
		{"bad head", `* | head zero`, "SPL_INVALID_ARGUMENT", 1, 10},
		{"dangling pipe", `index=gradethis |`, "SPL_EXPECTED_COMMAND", 1, 18},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Parse(test.query)
			if err == nil {
				t.Fatal("Parse succeeded, want error")
			}
			diagnostic := err.(*Diagnostic)
			if diagnostic.Code != test.code || diagnostic.Range.Start.Line != test.line || diagnostic.Range.Start.Column != test.column {
				t.Fatalf("diagnostic = %#v", diagnostic)
			}
		})
	}
}

func FuzzParseDoesNotPanic(f *testing.F) {
	for _, seed := range []string{
		`index=gradethis`,
		`index=gradethis (level=ERROR OR level=WARN) | sort -_time | head 20`,
		`"connection refused" | table _time message`,
		`index=main | stats count AS events by host, service`,
		`index=main | stats sum(bytes) by host`,
		`index=main | top limit=20 message`,
		"index=x\n| transaction trace_id",
		"\x00\xff",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, source string) {
		_, _ = Parse(source)
	})
}

func assertComparison(t *testing.T, expression Expr, field string, op CompareOp, value string, quoted bool) {
	t.Helper()
	comparison, ok := expression.(*ComparisonExpr)
	if !ok {
		t.Fatalf("expression = %T, want *ComparisonExpr", expression)
	}
	if comparison.Field != field || comparison.Op != op || comparison.Value.Text != value || comparison.Value.Quoted != quoted {
		t.Fatalf("comparison = %#v, want %s%s%q (quoted=%t)", comparison, field, op, value, quoted)
	}
}
