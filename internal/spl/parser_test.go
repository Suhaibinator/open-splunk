package spl

import (
	"strconv"
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

func TestParseRenameExactPairs(t *testing.T) {
	t.Parallel()

	source := `index=gradethis | rename logger AS component, request.path AS route`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command, ok := query.Commands[0].(*RenameCommand)
	if !ok || len(command.Assignments) != 2 {
		t.Fatalf("command = %#v, want two rename assignments", query.Commands[0])
	}
	want := [][2]string{{"logger", "component"}, {"request.path", "route"}}
	for index, assignment := range command.Assignments {
		if assignment.Source != want[index][0] || assignment.Destination != want[index][1] {
			t.Fatalf("assignment %d = %#v, want %q AS %q", index, assignment, want[index][0], want[index][1])
		}
		if got := source[assignment.SourceRange.Start.Offset:assignment.SourceRange.End.Offset]; got != want[index][0] {
			t.Fatalf("assignment %d source range = %q", index, got)
		}
		if got := source[assignment.DestinationRange.Start.Offset:assignment.DestinationRange.End.Offset]; got != want[index][1] {
			t.Fatalf("assignment %d destination range = %q", index, got)
		}
	}
}

func TestParseRenameRejectsPatternsDuplicatesAndAmbiguousSyntax(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source string
		code   string
	}{
		{`* | rename`, "SPL_EXPECTED_FIELD"},
		{`* | rename old new`, "SPL_EXPECTED_AS"},
		{`* | rename old AS`, "SPL_EXPECTED_FIELD"},
		{`* | rename old AS new,`, "SPL_EXPECTED_FIELD"},
		{`* | rename old AS new next AS final`, "SPL_EXPECTED_COMMA"},
		{`* | rename old* AS new`, "SPL_UNSUPPORTED_RENAME_PATTERN"},
		{`* | rename old AS new*`, "SPL_UNSUPPORTED_RENAME_PATTERN"},
		{`* | rename old AS old`, "SPL_INVALID_RENAME"},
		{`* | rename old AS first, old AS second`, "SPL_DUPLICATE_RENAME_SOURCE"},
		{`* | rename first AS target, second AS target`, "SPL_DUPLICATE_RENAME_TARGET"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.source, func(t *testing.T) {
			t.Parallel()
			assertParseDiagnosticCode(t, test.source, test.code)
		})
	}
}

func TestParseRenameBoundsAssignmentCount(t *testing.T) {
	t.Parallel()

	var source strings.Builder
	source.WriteString(`* | rename `)
	for index := 0; index <= maxRenameAssignments; index++ {
		if index > 0 {
			source.WriteString(", ")
		}
		source.WriteString("source")
		source.WriteString(strconv.Itoa(index))
		source.WriteString(" AS target")
		source.WriteString(strconv.Itoa(index))
	}
	assertParseDiagnosticCode(t, source.String(), "SPL_QUERY_TOO_COMPLEX")
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

func TestParseDedupCountAndExactFieldList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		count  uint64
		fields []string
	}{
		{name: "default count", source: `index=main | dedup session_id`, count: 1, fields: []string{"session_id"}},
		{name: "positional count and commas", source: `index=main | dedup 2 host, source`, count: 2, fields: []string{"host", "source"}},
		{name: "whitespace list", source: `index=main | dedup service severity level`, count: 1, fields: []string{"service", "severity", "level"}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			query, err := Parse(test.source)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			command, ok := query.Commands[0].(*DedupCommand)
			if !ok || command.Count != test.count || len(command.Fields) != len(test.fields) {
				t.Fatalf("dedup command = %#v, want count %d fields %v", query.Commands[0], test.count, test.fields)
			}
			for index, want := range test.fields {
				if command.Fields[index].Name != want {
					t.Fatalf("field %d = %q, want %q", index, command.Fields[index].Name, want)
				}
				gotRange := command.Fields[index].Range
				if got := test.source[gotRange.Start.Offset:gotRange.End.Offset]; got != want {
					t.Fatalf("field %d range = %q, want %q", index, got, want)
				}
			}
		})
	}
}

func TestParseDedupRejectsUnsupportedOrAmbiguousSyntax(t *testing.T) {
	t.Parallel()

	tests := []string{
		`index=main | dedup`,
		`index=main | dedup 0 host`,
		`index=main | dedup -1 host`,
		`index=main | dedup 18446744073709551616 host`,
		`index=main | dedup host,`,
		`index=main | dedup host,,source`,
		`index=main | dedup host host`,
		`index=main | dedup ho*`,
		`index=main | dedup "host"`,
		`index=main | dedup host keepempty=true`,
		`index=main | dedup consecutive=true host`,
		`index=main | dedup keepevents=true host`,
		`index=main | dedup host sortby -_time`,
	}
	for _, source := range tests {
		source := source
		t.Run(source, func(t *testing.T) {
			t.Parallel()
			_, err := Parse(source)
			if err == nil {
				t.Fatal("Parse unexpectedly succeeded")
			}
			diagnostic, ok := err.(*Diagnostic)
			if !ok || diagnostic.Code != "SPL_UNSUPPORTED_DEDUP_SYNTAX" {
				t.Fatalf("diagnostic = %#v, want SPL_UNSUPPORTED_DEDUP_SYNTAX", err)
			}
			if diagnostic.Range.Start.Offset < 0 || diagnostic.Range.Start.Offset > len(source) || diagnostic.Range.End.Offset > len(source) {
				t.Fatalf("diagnostic range = %#v outside source length %d", diagnostic.Range, len(source))
			}
		})
	}
}

func TestParseDedupBoundsFieldCount(t *testing.T) {
	t.Parallel()

	var source strings.Builder
	source.WriteString(`index=main | dedup `)
	for index := 0; index <= maxDedupFields; index++ {
		if index > 0 {
			source.WriteByte(' ')
		}
		source.WriteString("field")
		source.WriteString(strconv.Itoa(index))
	}
	assertParseDiagnosticCode(t, source.String(), "SPL_UNSUPPORTED_DEDUP_SYNTAX")
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

func TestParseWhereUsesExpressionPrecedence(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=gradethis | where status=500 OR duration_ms>500 AND level="ERROR"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command, ok := query.Commands[0].(*WhereCommand)
	if !ok {
		t.Fatalf("command = %T, want *WhereCommand", query.Commands[0])
	}
	root, ok := command.Expression.(*WhereBoolExpr)
	if !ok || root.Op != BoolOpOr {
		t.Fatalf("where root = %#v, want OR", command.Expression)
	}
	and, ok := root.Right.(*WhereBoolExpr)
	if !ok || and.Op != BoolOpAnd {
		t.Fatalf("where right = %#v, want AND", root.Right)
	}
	assertWhereLiteralComparison(t, root.Left, "status", CompareOpEqual, "500", false)
	assertWhereLiteralComparison(t, and.Left, "duration_ms", CompareOpGreater, "500", false)
	assertWhereLiteralComparison(t, and.Right, "level", CompareOpEqual, "ERROR", true)
}

func TestParseWhereTreatsBareRightHandNameAsField(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | where source_ip=client_ip`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	comparison := query.Commands[0].(*WhereCommand).Expression.(*WhereComparisonExpr)
	left, leftOK := comparison.Left.(*ScalarFieldExpr)
	right, rightOK := comparison.Right.(*ScalarFieldExpr)
	if !leftOK || !rightOK || left.Field != "source_ip" || right.Field != "client_ip" {
		t.Fatalf("where comparison = %#v", comparison)
	}
}

func TestParseWhereAllowsLiteralLeftOperandAfterBooleanOperators(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | where status=500 OR "api"=host`,
		`index=main | where status=500 AND "api"=host`,
		`index=main | where NOT "api"=host`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
	}
}

func TestParseWhereRejectsSearchTermsAndImplicitAND(t *testing.T) {
	t.Parallel()

	tests := []string{
		`index=main | where "connection refused"`,
		`index=main | where status=500 level=ERROR`,
		`index=main | where status`,
	}
	for _, source := range tests {
		if _, err := Parse(source); err == nil {
			t.Fatalf("Parse(%q) unexpectedly succeeded", source)
		}
	}
}

func TestParseEvalNestedReplaceAndToNumber(t *testing.T) {
	t.Parallel()

	source := `index=gradethis | eval duration_ms=tonumber(replace(duration, "ms$", ""))`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command, ok := query.Commands[0].(*EvalCommand)
	if !ok || len(command.Assignments) != 1 {
		t.Fatalf("command = %#v, want one eval assignment", query.Commands[0])
	}
	assignment := command.Assignments[0]
	if assignment.Field != "duration_ms" || source[assignment.FieldRange.Start.Offset:assignment.FieldRange.End.Offset] != "duration_ms" {
		t.Fatalf("assignment = %#v", assignment)
	}
	toNumber, ok := assignment.Expression.(*ScalarCallExpr)
	if !ok || toNumber.Function != ScalarFunctionToNumber || len(toNumber.Arguments) != 1 {
		t.Fatalf("outer expression = %#v", assignment.Expression)
	}
	replace, ok := toNumber.Arguments[0].(*ScalarCallExpr)
	if !ok || replace.Function != ScalarFunctionReplace || len(replace.Arguments) != 3 {
		t.Fatalf("inner expression = %#v", toNumber.Arguments[0])
	}
	field, ok := replace.Arguments[0].(*ScalarFieldExpr)
	pattern, patternOK := replace.Arguments[1].(*ScalarLiteralExpr)
	replacement, replacementOK := replace.Arguments[2].(*ScalarLiteralExpr)
	if !ok || field.Field != "duration" || !patternOK || pattern.Value.Text != "ms$" ||
		!replacementOK || replacement.Value.Text != "" {
		t.Fatalf("replace arguments = %#v", replace.Arguments)
	}
}

func TestParseEvalAssignmentsRemainLeftToRight(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | eval first=tonumber(raw), second=replace(first, "x", "y")`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*EvalCommand)
	if len(command.Assignments) != 2 || command.Assignments[0].Field != "first" || command.Assignments[1].Field != "second" {
		t.Fatalf("assignments = %#v", command.Assignments)
	}
	secondInput := command.Assignments[1].Expression.(*ScalarCallExpr).Arguments[0].(*ScalarFieldExpr)
	if secondInput.Field != "first" {
		t.Fatalf("second input = %#v", secondInput)
	}
}

func TestParseEvalReplacePreservesRegexAndBackreferenceEscapes(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | eval formatted=replace(date, "^(\d{1,2})/(\d{1,2})/", "\2/\1/")`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	call := query.Commands[0].(*EvalCommand).Assignments[0].Expression.(*ScalarCallExpr)
	pattern := call.Arguments[1].(*ScalarLiteralExpr).Value.Text
	replacement := call.Arguments[2].(*ScalarLiteralExpr).Value.Text
	if pattern != `^(\d{1,2})/(\d{1,2})/` || replacement != `\2/\1/` {
		t.Fatalf("replace escapes = %q/%q", pattern, replacement)
	}
}

func TestParseRexExtractionOptionsAndNamedCaptures(t *testing.T) {
	t.Parallel()

	source := `index=gradethis | rex field=duration "^(?:elapsed=)?(?<duration_value>\d+(?:\.\d+)?)(?P<duration_unit>µs|ms)$" max_match=1`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command, ok := query.Commands[0].(*RexCommand)
	if !ok {
		t.Fatalf("command = %T, want *RexCommand", query.Commands[0])
	}
	if command.Field != "duration" || command.Pattern != `^(?:elapsed=)?(?<duration_value>\d+(?:\.\d+)?)(?P<duration_unit>µs|ms)$` ||
		command.MaxMatch != 1 {
		t.Fatalf("rex command = %#v", command)
	}
	if got := source[command.FieldRange.Start.Offset:command.FieldRange.End.Offset]; got != "duration" {
		t.Fatalf("field source range = %q", got)
	}
	if got := source[command.PatternRange.Start.Offset:command.PatternRange.End.Offset]; got !=
		`"^(?:elapsed=)?(?<duration_value>\d+(?:\.\d+)?)(?P<duration_unit>µs|ms)$"` {
		t.Fatalf("pattern source range = %q", got)
	}
}

func TestParseRexDefaultsToRawAndPreservesRegexEscapes(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=gradethis | rex "method=(?<method>[A-Z]+)\s+path=(?<path>\S+)"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*RexCommand)
	if command.Field != "_raw" || command.MaxMatch != 1 {
		t.Fatalf("rex defaults = %#v", command)
	}
	if command.Pattern != `method=(?<method>[A-Z]+)\s+path=(?<path>\S+)` {
		t.Fatalf("pattern = %q", command.Pattern)
	}
}

func TestParseRexAcceptsSupportedOptionsBeforePatternInEitherOrder(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | rex field=duration max_match=1 "(?<value>\d+)"`,
		`index=gradethis | rex max_match=1 field=duration "(?<value>\d+)"`,
	} {
		query, err := Parse(source)
		if err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
		command := query.Commands[0].(*RexCommand)
		if command.Field != "duration" || command.MaxMatch != 1 {
			t.Fatalf("Parse(%q) rex = %#v", source, command)
		}
	}
}

func TestParseRexRejectsUnsupportedOrMalformedForms(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source string
		code   string
	}{
		{`index=main | rex`, "SPL_EXPECTED_REX_PATTERN"},
		{`index=main | rex status=(?<code>\d+)`, "SPL_EXPECTED_REX_PATTERN"},
		{`index=main | rex field= "(?<code>\d+)"`, "SPL_EXPECTED_FIELD"},
		{`index=main | rex "status=(\d+)"`, "SPL_UNSUPPORTED_REGEX"},
		{`index=main | rex "(?<value>x)|(?P<value>y)"`, "SPL_UNSUPPORTED_REGEX"},
		{`index=main | rex "(?<value>x)(?=y)"`, "SPL_UNSUPPORTED_REGEX"},
		{`index=main | rex "(?<value>x)\1"`, "SPL_UNSUPPORTED_REGEX"},
		{`index=main | rex "(?<value>x)" max_match=2`, "SPL_UNSUPPORTED_REX_SYNTAX"},
		{`index=main | rex "(?<value>x)" max_match=0`, "SPL_UNSUPPORTED_REX_SYNTAX"},
		{`index=main | rex "(?<value>x)" offset_field=offsets`, "SPL_UNSUPPORTED_REX_SYNTAX"},
		{`index=main | rex mode=sed "s/x/y/g"`, "SPL_UNSUPPORTED_REX_SYNTAX"},
		{`index=main | rex field=message mode=sed "s/x/y/g"`, "SPL_UNSUPPORTED_REX_SYNTAX"},
		{`index=main | rex field=message offset_field=offsets "(?<value>x)"`, "SPL_UNSUPPORTED_REX_SYNTAX"},
		{`index=main | rex "(?<value>x)" field=message`, "SPL_UNSUPPORTED_REX_SYNTAX"},
		{`index=main | rex field=message "(?<value>x)" unknown=1`, "SPL_UNSUPPORTED_REX_SYNTAX"},
	}
	for _, test := range tests {
		_, err := Parse(test.source)
		if err == nil {
			t.Fatalf("Parse(%q) unexpectedly succeeded", test.source)
		}
		diagnostic, ok := err.(*Diagnostic)
		if !ok || diagnostic.Code != test.code {
			t.Fatalf("Parse(%q) diagnostic = %#v, want %s", test.source, err, test.code)
		}
	}
}

func TestParseRexBoundsRegexWork(t *testing.T) {
	t.Parallel()

	tooLong := `index=main | rex "(?<value>` + strings.Repeat("x", 4096) + `)"`
	assertParseDiagnosticCode(t, tooLong, "SPL_QUERY_TOO_COMPLEX")

	var captures strings.Builder
	captures.WriteString(`index=main | rex "`)
	for index := 0; index <= 16; index++ {
		captures.WriteString("(?<f")
		captures.WriteString(strconv.Itoa(index))
		captures.WriteString(">x)")
	}
	captures.WriteByte('"')
	assertParseDiagnosticCode(t, captures.String(), "SPL_QUERY_TOO_COMPLEX")
}

func TestParseEvalRejectsMalformedOrUnsupportedExpressions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source string
		code   string
	}{
		{`index=main | eval duration_ms`, "SPL_EXPECTED_EQUAL"},
		{`index=main | eval duration_ms=`, "SPL_EXPECTED_SCALAR_EXPRESSION"},
		{`index=main | eval duration_ms=tonumber()`, "SPL_INVALID_EVAL_ARITY"},
		{`index=main | eval duration_ms=tonumber(duration, 10)`, "SPL_INVALID_EVAL_ARITY"},
		{`index=main | eval value=replace(duration, pattern, "")`, "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{`index=main | eval value=replace(message, "(?=secret)", "")`, "SPL_UNSUPPORTED_REGEX"},
		{`index=main | eval value=replace(message, "", "x")`, "SPL_UNSUPPORTED_REGEX"},
		{`index=main | eval value=replace(message, "a*", "x")`, "SPL_UNSUPPORTED_REGEX"},
		{`index=main | eval value=trim(duration)`, "SPL_UNSUPPORTED_EVAL_FUNCTION"},
		{`index=main | eval value=duration_ms+1`, "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{`index=main | eval value='duration_ms'`, "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{`index=main | eval x+1=duration_ms`, "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{`index=main | eval 'x'=duration_ms`, "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{`index=main | where duration_ms+1>500`, "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{`index=main | where 'duration_ms'>500`, "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{`index=main | eval value=tonumber(duration),`, "SPL_EXPECTED_FIELD"},
	}
	for _, test := range tests {
		_, err := Parse(test.source)
		if err == nil {
			t.Fatalf("Parse(%q) unexpectedly succeeded", test.source)
		}
		diagnostic, ok := err.(*Diagnostic)
		if !ok || diagnostic.Code != test.code {
			t.Fatalf("Parse(%q) diagnostic = %#v, want %s", test.source, err, test.code)
		}
	}
}

func TestParseBoundsQueryComplexity(t *testing.T) {
	t.Parallel()

	var assignments strings.Builder
	assignments.WriteString(`index=main | eval `)
	for index := 0; index <= maxEvalAssignments; index++ {
		if index > 0 {
			assignments.WriteByte(',')
		}
		assignments.WriteString("f")
		assignments.WriteString(strconv.Itoa(index))
		assignments.WriteString("=1")
	}
	assertParseDiagnosticCode(t, assignments.String(), "SPL_QUERY_TOO_COMPLEX")
	exactAssignments := strings.TrimSuffix(assignments.String(), ",f"+strconv.Itoa(maxEvalAssignments)+"=1")
	if _, err := Parse(exactAssignments); err != nil {
		t.Fatalf("Parse(exact eval assignment limit): %v", err)
	}

	var commands strings.Builder
	commands.WriteString("index=main")
	for index := 0; index <= maxPipelineCommands; index++ {
		commands.WriteString(" | head 1")
	}
	assertParseDiagnosticCode(t, commands.String(), "SPL_QUERY_TOO_COMPLEX")
	exactCommands := strings.TrimSuffix(commands.String(), " | head 1")
	if _, err := Parse(exactCommands); err != nil {
		t.Fatalf("Parse(exact pipeline command limit): %v", err)
	}

	nested := strings.Repeat("tonumber(", maxScalarNestingDepth) + "duration" + strings.Repeat(")", maxScalarNestingDepth)
	assertParseDiagnosticCode(t, "index=main | eval value="+nested, "SPL_QUERY_TOO_COMPLEX")
	exactNested := strings.Repeat("tonumber(", maxScalarNestingDepth-1) + "duration" + strings.Repeat(")", maxScalarNestingDepth-1)
	if _, err := Parse("index=main | eval value=" + exactNested); err != nil {
		t.Fatalf("Parse(exact scalar nesting limit): %v", err)
	}

	var tokens strings.Builder
	for index := 0; index < maxSPLTokens+1; index++ {
		if index > 0 {
			tokens.WriteByte(' ')
		}
		tokens.WriteByte('x')
	}
	assertParseDiagnosticCode(t, tokens.String(), "SPL_QUERY_TOO_COMPLEX")
	exactTokens := strings.TrimSuffix(tokens.String(), " x")
	if _, err := Parse(exactTokens); err != nil {
		t.Fatalf("Parse(exact token limit): %v", err)
	}

	var measures strings.Builder
	measures.WriteString("index=main | stats ")
	for index := 0; index <= maxStatsAggregates; index++ {
		if index > 0 {
			measures.WriteByte(' ')
		}
		measures.WriteString("p95(f")
		measures.WriteString(strconv.Itoa(index))
		measures.WriteString(") AS p")
		measures.WriteString(strconv.Itoa(index))
	}
	assertParseDiagnosticCode(t, measures.String(), "SPL_QUERY_TOO_COMPLEX")
	lastMeasure := " p95(f" + strconv.Itoa(maxStatsAggregates) + ") AS p" + strconv.Itoa(maxStatsAggregates)
	if _, err := Parse(strings.TrimSuffix(measures.String(), lastMeasure)); err != nil {
		t.Fatalf("Parse(exact stats measure limit): %v", err)
	}

	var groups strings.Builder
	groups.WriteString("index=main | stats count BY ")
	for index := 0; index <= maxStatsGroupFields; index++ {
		if index > 0 {
			groups.WriteByte(' ')
		}
		groups.WriteString("f")
		groups.WriteString(strconv.Itoa(index))
	}
	assertParseDiagnosticCode(t, groups.String(), "SPL_QUERY_TOO_COMPLEX")
	lastGroup := " f" + strconv.Itoa(maxStatsGroupFields)
	if _, err := Parse(strings.TrimSuffix(groups.String(), lastGroup)); err != nil {
		t.Fatalf("Parse(exact stats BY field limit): %v", err)
	}

	exactSource := `"` + strings.Repeat("x", maxSPLSourceBytes-2) + `"`
	if _, err := Parse(exactSource); err != nil {
		t.Fatalf("Parse(exact source-byte limit): %v", err)
	}
	assertParseDiagnosticCode(t, exactSource+"x", "SPL_QUERY_TOO_COMPLEX")

	var where strings.Builder
	where.WriteString("index=main | where ")
	for index := 0; index <= maxWhereComparisons; index++ {
		if index > 0 {
			where.WriteString(" AND ")
		}
		where.WriteString("f")
		where.WriteString(strconv.Itoa(index))
		where.WriteString("=1")
	}
	assertParseDiagnosticCode(t, where.String(), "SPL_QUERY_TOO_COMPLEX")
	lastComparison := " AND f" + strconv.Itoa(maxWhereComparisons) + "=1"
	if _, err := Parse(strings.TrimSuffix(where.String(), lastComparison)); err != nil {
		t.Fatalf("Parse(exact where comparison limit): %v", err)
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
			if len(command.Aggregates) != 1 || command.Aggregates[0].Function != AggregateFunctionCount || command.Aggregates[0].Alias != test.alias {
				t.Fatalf("aggregates = %#v, want count AS %q", command.Aggregates, test.alias)
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

func TestParseStatsMultipleMeasuresWithP95(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | stats count p95(duration_ms) AS p95_ms BY path`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*StatsCommand)
	if len(command.Aggregates) != 2 {
		t.Fatalf("aggregates = %#v", command.Aggregates)
	}
	count := command.Aggregates[0]
	percentile := command.Aggregates[1]
	if count.Function != AggregateFunctionCount || count.Alias != "count" || count.Input != "" {
		t.Fatalf("count aggregate = %#v", count)
	}
	if percentile.Function != AggregateFunctionP95 || percentile.Input != "duration_ms" || percentile.Alias != "p95_ms" {
		t.Fatalf("percentile aggregate = %#v", percentile)
	}
	if len(command.GroupBy) != 1 || command.GroupBy[0].Name != "path" {
		t.Fatalf("group fields = %#v", command.GroupBy)
	}
}

func TestParseStatsP95DefaultOutputName(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | stats p95(duration_ms)`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	aggregate := query.Commands[0].(*StatsCommand).Aggregates[0]
	if aggregate.Alias != "p95(duration_ms)" {
		t.Fatalf("default alias = %q", aggregate.Alias)
	}
}

func TestParseStatsSumAndAvgFieldsAliasesAndFunctionCase(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | stats SUM(amount) AvG(latency) AS mean BY service`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*StatsCommand)
	if len(command.Aggregates) != 2 {
		t.Fatalf("aggregates = %#v", command.Aggregates)
	}
	sum := command.Aggregates[0]
	if sum.Function != AggregateFunctionSum || sum.Input != "amount" || sum.Alias != "sum(amount)" {
		t.Fatalf("sum aggregate = %#v", sum)
	}
	average := command.Aggregates[1]
	if average.Function != AggregateFunctionAverage || average.Input != "latency" || average.Alias != "mean" {
		t.Fatalf("avg aggregate = %#v", average)
	}
	if len(command.GroupBy) != 1 || command.GroupBy[0].Name != "service" {
		t.Fatalf("group fields = %#v", command.GroupBy)
	}
}

func TestParseStatsSumAndAvgRequireExactlyOneField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		code   string
	}{
		{name: "sum missing parentheses", source: `index=main | stats sum`, code: "SPL_UNSUPPORTED_STATS_SYNTAX"},
		{name: "avg missing parentheses", source: `index=main | stats avg`, code: "SPL_UNSUPPORTED_STATS_SYNTAX"},
		{name: "sum missing field", source: `index=main | stats sum()`, code: "SPL_EXPECTED_FIELD"},
		{name: "avg multiple fields", source: `index=main | stats avg(left,right)`, code: "SPL_EXPECTED_RIGHT_PAREN"},
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

func TestParseTimechartFixedSpanCountByField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		source    string
		magnitude uint64
		unit      TimeSpanUnit
		field     string
	}{
		{
			name:      "minutes corpus query",
			source:    `index=gradethis | timechart span=5m count by level`,
			magnitude: 5,
			unit:      TimeSpanUnitMinute,
			field:     "level",
		},
		{
			name:      "seconds with whitespace and case",
			source:    `index=gradethis | TIMECHART SPAN = 30S COUNT BY service`,
			magnitude: 30,
			unit:      TimeSpanUnitSecond,
			field:     "service",
		},
		{
			name:      "hours",
			source:    `index=gradethis | timechart span=2h count BY http.route`,
			magnitude: 2,
			unit:      TimeSpanUnitHour,
			field:     "http.route",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			query, err := Parse(test.source)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if len(query.Commands) != 1 {
				t.Fatalf("command count = %d, want 1", len(query.Commands))
			}
			command, ok := query.Commands[0].(*TimechartCommand)
			if !ok {
				t.Fatalf("command = %T, want *TimechartCommand", query.Commands[0])
			}
			if command.Span.Magnitude != test.magnitude || command.Span.Unit != test.unit ||
				command.Function != AggregateFunctionCount || command.SplitBy.Name != test.field {
				t.Fatalf("timechart = %#v", command)
			}
			spanText := test.source[command.Span.Range.Start.Offset:command.Span.Range.End.Offset]
			if !strings.EqualFold(spanText, strconv.FormatUint(test.magnitude, 10)+command.Span.Unit.String()) {
				t.Fatalf("span source = %q", spanText)
			}
			if aggregateText := test.source[command.AggregateRange.Start.Offset:command.AggregateRange.End.Offset]; !strings.EqualFold(aggregateText, "count") {
				t.Fatalf("aggregate source = %q", aggregateText)
			}
			if fieldText := test.source[command.SplitBy.Range.Start.Offset:command.SplitBy.Range.End.Offset]; fieldText != test.field {
				t.Fatalf("split field source = %q", fieldText)
			}
		})
	}
}

func TestParseTimechartRejectsUnsupportedOrMalformedSyntax(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		source    string
		code      string
		locatedAt string
	}{
		{"missing arguments", `index=main | timechart`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", ""},
		{"missing equal", `index=main | timechart span 5m count by level`, "SPL_EXPECTED_EQUAL", "span"},
		{"missing span", `index=main | timechart span=`, "SPL_INVALID_ARGUMENT", ""},
		{"zero span", `index=main | timechart span=0m count by level`, "SPL_INVALID_ARGUMENT", "0m"},
		{"negative span", `index=main | timechart span=-5m count by level`, "SPL_INVALID_ARGUMENT", "-5m"},
		{"duration overflow", `index=main | timechart span=2562048h count by level`, "SPL_NUMBER_OUT_OF_RANGE", "2562048h"},
		{"integer overflow", `index=main | timechart span=18446744073709551616s count by level`, "SPL_NUMBER_OUT_OF_RANGE", "18446744073709551616s"},
		{"calendar day", `index=main | timechart span=1d count by level`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", "1d"},
		{"subsecond", `index=main | timechart span=5ms count by level`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", "5ms"},
		{"compound span", `index=main | timechart span=1h30m count by level`, "SPL_INVALID_ARGUMENT", "1h30m"},
		{"missing aggregate", `index=main | timechart span=5m`, "SPL_UNSUPPORTED_TIMECHART_AGGREGATE", ""},
		{"unsupported aggregate", `index=main | timechart span=5m p95(duration) by level`, "SPL_UNSUPPORTED_TIMECHART_AGGREGATE", "p95"},
		{"count arguments", `index=main | timechart span=5m count() by level`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", "("},
		{"missing by", `index=main | timechart span=5m count level`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", "level"},
		{"missing split field", `index=main | timechart span=5m count by`, "SPL_EXPECTED_FIELD", ""},
		{"quoted split field", `index=main | timechart span=5m count by "level"`, "SPL_EXPECTED_FIELD", `"level"`},
		{"wildcard split field", `index=main | timechart span=5m count by level*`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", "level*"},
		{"multiple split fields", `index=main | timechart span=5m count by level host`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", "host"},
		{"unsupported option", `index=main | timechart span=5m count by level useother=false`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", "useother"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Parse(test.source)
			if err == nil {
				t.Fatal("Parse succeeded")
			}
			diagnostic, ok := err.(*Diagnostic)
			if !ok || diagnostic.Code != test.code {
				t.Fatalf("diagnostic = %#v, want %s", err, test.code)
			}
			if test.locatedAt != "" {
				got := test.source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset]
				if got != test.locatedAt {
					t.Fatalf("diagnostic source = %q, want %q", got, test.locatedAt)
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
		{"other function", "index=main\n| stats min(bytes)", "SPL_UNSUPPORTED_STATS_AGGREGATE", 2, 9},
		{"count argument", `* | stats count(host)`, "SPL_UNSUPPORTED_STATS_AGGREGATE", 1, 16},
		{"second aggregate", `* | stats count, dc(host)`, "SPL_UNSUPPORTED_STATS_AGGREGATE", 1, 18},
		{"space-separated aggregate", `* | stats count dc(host)`, "SPL_UNSUPPORTED_STATS_AGGREGATE", 1, 17},
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

func TestParseRareSingleFieldAndLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		field  string
		limit  uint64
	}{
		{name: "default", source: `index=main | rare message`, field: "message", limit: 10},
		{name: "limit option", source: `index=main | rare limit=20 message`, field: "message", limit: 20},
		{name: "positional limit", source: `index=main | rare 5 status`, field: "status", limit: 5},
		{name: "unlimited", source: `index=main | rare limit=0 host`, field: "host", limit: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			query, err := Parse(test.source)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			command, ok := query.Commands[0].(*RareCommand)
			if !ok || command.Field != test.field || command.Limit != test.limit || command.Name() != "rare" {
				t.Fatalf("rare command = %#v, want field %q limit %d", query.Commands[0], test.field, test.limit)
			}
			if command.FieldRange.Start.Column <= command.Range.Start.Column {
				t.Fatalf("field range = %#v, command range = %#v", command.FieldRange, command.Range)
			}
		})
	}
}

func TestParseRareRejectsUnsupportedOrMalformedSyntax(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		code   string
	}{
		{name: "missing field", source: `index=main | rare`, code: "SPL_EXPECTED_FIELD"},
		{name: "missing limit", source: `index=main | rare limit= message`, code: "SPL_INVALID_ARGUMENT"},
		{name: "negative limit", source: `index=main | rare limit=-1 message`, code: "SPL_INVALID_ARGUMENT"},
		{name: "negative positional limit", source: `index=main | rare -1 message`, code: "SPL_INVALID_ARGUMENT"},
		{name: "limit overflow", source: `index=main | rare limit=18446744073709551616 message`, code: "SPL_NUMBER_OUT_OF_RANGE"},
		{name: "multiple fields", source: `index=main | rare message, host`, code: "SPL_UNSUPPORTED_RARE_SYNTAX"},
		{name: "by clause", source: `index=main | rare message BY host`, code: "SPL_UNSUPPORTED_RARE_SYNTAX"},
		{name: "unsupported option", source: `index=main | rare showperc=false message`, code: "SPL_UNSUPPORTED_RARE_SYNTAX"},
		{name: "wildcard field", source: `index=main | rare mes*`, code: "SPL_UNSUPPORTED_RARE_SYNTAX"},
		{name: "trailing option", source: `index=main | rare message limit=5`, code: "SPL_UNSUPPORTED_RARE_SYNTAX"},
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
		`index=main | rare limit=20 message`,
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

func assertWhereLiteralComparison(t *testing.T, expression WhereExpr, field string, op CompareOp, value string, quoted bool) {
	t.Helper()
	comparison, ok := expression.(*WhereComparisonExpr)
	if !ok {
		t.Fatalf("expression = %T, want *WhereComparisonExpr", expression)
	}
	left, leftOK := comparison.Left.(*ScalarFieldExpr)
	right, rightOK := comparison.Right.(*ScalarLiteralExpr)
	if !leftOK || !rightOK || left.Field != field || comparison.Op != op ||
		right.Value.Text != value || right.Value.Quoted != quoted {
		t.Fatalf("comparison = %#v, want %s%s%q (quoted=%t)", comparison, field, op, value, quoted)
	}
}

func assertParseDiagnosticCode(t *testing.T, source, code string) {
	t.Helper()
	_, err := Parse(source)
	if err == nil {
		t.Fatalf("Parse(%q) unexpectedly succeeded", source)
	}
	diagnostic, ok := err.(*Diagnostic)
	if !ok || diagnostic.Code != code {
		t.Fatalf("Parse(%q) error = %#v, want %s", source, err, code)
	}
}
