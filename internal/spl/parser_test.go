package spl

import (
	"errors"
	"fmt"
	"slices"
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

func TestParseWhereNullPredicatesUseBooleanPrecedence(t *testing.T) {
	t.Parallel()

	source := `index=main | where isnull(optional) OR NOT ISNOTNULL(required) AND isnull(replace(message, "x", ""))`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*WhereCommand)
	root, ok := command.Expression.(*WhereBoolExpr)
	if !ok || root.Op != BoolOpOr {
		t.Fatalf("where root = %#v, want OR", command.Expression)
	}
	left, ok := root.Left.(*WhereScalarPredicateExpr)
	if !ok {
		t.Fatalf("where left = %T, want *WhereScalarPredicateExpr", root.Left)
	}
	leftCall, ok := left.Value.(*ScalarCallExpr)
	if !ok || leftCall.Function != ScalarFunctionIsNull ||
		leftCall.Arguments[0].(*ScalarFieldExpr).Field != "optional" {
		t.Fatalf("where left predicate = %#v", left)
	}
	and, ok := root.Right.(*WhereBoolExpr)
	if !ok || and.Op != BoolOpAnd {
		t.Fatalf("where right = %#v, want AND", root.Right)
	}
	not, ok := and.Left.(*WhereNotExpr)
	if !ok {
		t.Fatalf("where AND left = %T, want *WhereNotExpr", and.Left)
	}
	notPredicate, ok := not.Operand.(*WhereScalarPredicateExpr)
	if !ok {
		t.Fatalf("NOT operand = %T, want *WhereScalarPredicateExpr", not.Operand)
	}
	notCall := notPredicate.Value.(*ScalarCallExpr)
	if notCall.Function != ScalarFunctionIsNotNull ||
		notCall.Arguments[0].(*ScalarFieldExpr).Field != "required" {
		t.Fatalf("NOT predicate = %#v", notPredicate)
	}
	nested := and.Right.(*WhereScalarPredicateExpr).Value.(*ScalarCallExpr)
	if nested.Function != ScalarFunctionIsNull ||
		nested.Arguments[0].(*ScalarCallExpr).Function != ScalarFunctionReplace {
		t.Fatalf("nested null predicate = %#v", nested)
	}
	if source[left.Range.Start.Offset:left.Range.End.Offset] != "isnull(optional)" {
		t.Fatalf("left predicate range = %#v", left.Range)
	}
}

func TestParseWhereNullPredicatesAllowExplicitBooleanComparison(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | where isnull(status)=true`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	comparison := query.Commands[0].(*WhereCommand).Expression.(*WhereComparisonExpr)
	call, ok := comparison.Left.(*ScalarCallExpr)
	if !ok || call.Function != ScalarFunctionIsNull {
		t.Fatalf("comparison left = %#v, want isnull call", comparison.Left)
	}
	literal, ok := comparison.Right.(*ScalarLiteralExpr)
	if !ok || literal.Value.Kind != LiteralKindBool || !strings.EqualFold(literal.Value.Text, "true") {
		t.Fatalf("comparison right = %#v, want true", comparison.Right)
	}
}

func TestParseWhereNullPredicatesEnforceArityEvalBoundaryAndPredicateLimit(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		code   string
	}{
		{source: `index=main | where isnull()`, code: "SPL_INVALID_EVAL_ARITY"},
		{source: `index=main | where isnotnull(left, right)`, code: "SPL_INVALID_EVAL_ARITY"},
		{source: `index=main | eval flag=isnull(optional)`, code: "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{source: `index=main | eval flag=tonumber(isnotnull(optional))`, code: "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{source: `index=main | where tonumber(isnull(optional))=0`, code: "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{source: `index=main | where replace(isnotnull(optional), "true", "yes")="yes"`, code: "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{source: `index=main | where tonumber(optional)`, code: "SPL_EXPECTED_COMPARISON"},
	} {
		assertParseDiagnosticCode(t, test.source, test.code)
	}

	var predicates strings.Builder
	predicates.WriteString("index=main | where ")
	for index := 0; index <= maxEvalPredicates; index++ {
		if index > 0 {
			predicates.WriteString(" AND ")
		}
		predicates.WriteString("isnull(f")
		predicates.WriteString(strconv.Itoa(index))
		predicates.WriteByte(')')
	}
	assertParseDiagnosticCode(t, predicates.String(), "SPL_QUERY_TOO_COMPLEX")
	lastPredicate := " AND isnull(f" + strconv.Itoa(maxEvalPredicates) + ")"
	if _, err := Parse(strings.TrimSuffix(predicates.String(), lastPredicate)); err != nil {
		t.Fatalf("Parse(exact where null-predicate limit): %v", err)
	}
}

func TestParseEvalIfConsumesBooleanPredicateAndPreservesPrecedence(t *testing.T) {
	t.Parallel()

	source := `index=main | eval label=IF(isnull(status) OR NOT status=200 AND source="api", "bad", if(isnotnull(host), host, "missing"))`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*EvalCommand)
	conditional, ok := command.Assignments[0].Expression.(*ScalarIfExpr)
	if !ok {
		t.Fatalf("eval expression = %T, want *ScalarIfExpr", command.Assignments[0].Expression)
	}
	root, ok := conditional.Condition.(*WhereBoolExpr)
	if !ok || root.Op != BoolOpOr {
		t.Fatalf("if condition = %#v, want OR", conditional.Condition)
	}
	if _, leftOK := root.Left.(*WhereScalarPredicateExpr); !leftOK {
		t.Fatalf("if condition left = %T, want null predicate", root.Left)
	}
	and, ok := root.Right.(*WhereBoolExpr)
	if !ok || and.Op != BoolOpAnd {
		t.Fatalf("if condition right = %#v, want AND", root.Right)
	}
	not, ok := and.Left.(*WhereNotExpr)
	if !ok {
		t.Fatalf("if condition AND left = %T, want NOT", and.Left)
	}
	assertWhereLiteralComparison(t, not.Operand, "status", CompareOpEqual, "200", false)
	assertWhereLiteralComparison(t, and.Right, "source", CompareOpEqual, "api", true)

	trueValue, ok := conditional.True.(*ScalarLiteralExpr)
	if !ok || trueValue.Value.Text != "bad" {
		t.Fatalf("if true branch = %#v, want string literal", conditional.True)
	}
	nested, ok := conditional.False.(*ScalarIfExpr)
	if !ok {
		t.Fatalf("if false branch = %T, want nested *ScalarIfExpr", conditional.False)
	}
	if nested.True.(*ScalarFieldExpr).Field != "host" ||
		nested.False.(*ScalarLiteralExpr).Value.Text != "missing" {
		t.Fatalf("nested if = %#v", nested)
	}
	if got := source[conditional.Range.Start.Offset:conditional.Range.End.Offset]; got !=
		`IF(isnull(status) OR NOT status=200 AND source="api", "bad", if(isnotnull(host), host, "missing"))` {
		t.Fatalf("if range = %q", got)
	}
}

func TestParseEvalIfUsesConsumerAwareBooleanTyping(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval label=if(isnull(optional), "missing", "present")`,
		`index=main | eval number=tonumber(if(isnull(optional), "0", "1"))`,
		`index=main | eval flag=if(isnull(optional), true, false)`,
		`index=main | where if(isnull(optional), isnull(left), isnotnull(right))`,
		`index=main | where if(isnull(optional), true, false)`,
		`index=main | where isnull(if(isnull(optional), left, right))`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
	}

	for _, test := range []struct {
		source string
		code   string
	}{
		{source: `index=main | eval flag=isnull(optional)`, code: "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{source: `index=main | eval flag=if(isnull(optional), isnull(left), isnotnull(right))`, code: "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{source: `index=main | eval flag=if(isnull(optional), isnull(left), "no")`, code: "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{source: `index=main | eval number=tonumber(if(isnull(optional), isnull(left), "0"))`, code: "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{source: `index=main | eval label=if(optional, "yes", "no")`, code: "SPL_EXPECTED_COMPARISON"},
		{source: `index=main | where true`, code: "SPL_EXPECTED_COMPARISON"},
		{source: `index=main | eval label=if(status=200 XOR host="api", "yes", "no")`, code: "SPL_UNSUPPORTED_WHERE_EXPRESSION"},
		{source: `index=main | eval label=if(status=200 host="api", "yes", "no")`, code: "SPL_UNSUPPORTED_WHERE_EXPRESSION"},
	} {
		assertParseDiagnosticCode(t, test.source, test.code)
	}
}

func TestParseEvalIfEnforcesArityAndSharedPredicateLimit(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval label=if()`,
		`index=main | eval label=if(isnull(optional))`,
		`index=main | eval label=if(isnull(optional), "yes")`,
		`index=main | eval label=if(isnull(optional), "yes", "no", "extra")`,
		`index=main | eval label=if(isnull(optional),, "no")`,
		`index=main | eval label=if(isnull(optional), "yes",)`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_INVALID_EVAL_ARITY")
	}

	var assignments strings.Builder
	assignments.WriteString("index=main | eval ")
	for index := 0; index <= maxEvalPredicates; index++ {
		if index > 0 {
			assignments.WriteString(", ")
		}
		assignments.WriteString("f")
		assignments.WriteString(strconv.Itoa(index))
		assignments.WriteString("=if(p")
		assignments.WriteString(strconv.Itoa(index))
		assignments.WriteString("=1, 1, 0)")
	}
	assertParseDiagnosticCode(t, assignments.String(), "SPL_QUERY_TOO_COMPLEX")

	lastAssignment := ", f" + strconv.Itoa(maxEvalPredicates) +
		"=if(p" + strconv.Itoa(maxEvalPredicates) + "=1, 1, 0)"
	if _, err := Parse(strings.TrimSuffix(assignments.String(), lastAssignment)); err != nil {
		t.Fatalf("Parse(exact eval predicate limit): %v", err)
	}

	var mixedBudget strings.Builder
	mixedBudget.WriteString("index=main | eval ")
	for index := 0; index < maxEvalPredicates/2; index++ {
		if index > 0 {
			mixedBudget.WriteString(", ")
		}
		fmt.Fprintf(&mixedBudget, "f%d=if(p%d=1, 1, 0)", index, index)
	}
	mixedBudget.WriteString(" | where ")
	for index := 0; index <= maxEvalPredicates/2; index++ {
		if index > 0 {
			mixedBudget.WriteString(" AND ")
		}
		fmt.Fprintf(&mixedBudget, "w%d=1", index)
	}
	assertParseDiagnosticCode(t, mixedBudget.String(), "SPL_QUERY_TOO_COMPLEX")
	lastMixedPredicate := " AND w" + strconv.Itoa(maxEvalPredicates/2) + "=1"
	if _, err := Parse(strings.TrimSuffix(mixedBudget.String(), lastMixedPredicate)); err != nil {
		t.Fatalf("Parse(exact cross-command predicate limit): %v", err)
	}

	nestedIf := "0"
	for range maxScalarNestingDepth {
		nestedIf = "if(true=true, 1, " + nestedIf + ")"
	}
	assertParseDiagnosticCode(t, "index=main | eval value="+nestedIf, "SPL_QUERY_TOO_COMPLEX")
	exactNestedIf := "0"
	for range maxScalarNestingDepth - 1 {
		exactNestedIf = "if(true=true, 1, " + exactNestedIf + ")"
	}
	if _, err := Parse("index=main | eval value=" + exactNestedIf); err != nil {
		t.Fatalf("Parse(exact nested-if depth): %v", err)
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
	for index := 0; index <= MaximumStatsMeasures; index++ {
		if index > 0 {
			measures.WriteByte(' ')
		}
		measures.WriteString("p95(f")
		measures.WriteString(strconv.Itoa(index))
		measures.WriteString(") AS p")
		measures.WriteString(strconv.Itoa(index))
	}
	assertParseDiagnosticCode(t, measures.String(), "SPL_QUERY_TOO_COMPLEX")
	lastMeasure := " p95(f" + strconv.Itoa(MaximumStatsMeasures) + ") AS p" + strconv.Itoa(MaximumStatsMeasures)
	if _, err := Parse(strings.TrimSuffix(measures.String(), lastMeasure)); err != nil {
		t.Fatalf("Parse(exact stats measure limit): %v", err)
	}

	var groups strings.Builder
	groups.WriteString("index=main | stats count BY ")
	for index := 0; index <= MaximumStatsGroupFields; index++ {
		if index > 0 {
			groups.WriteByte(' ')
		}
		groups.WriteString("f")
		groups.WriteString(strconv.Itoa(index))
	}
	assertParseDiagnosticCode(t, groups.String(), "SPL_QUERY_TOO_COMPLEX")
	lastGroup := " f" + strconv.Itoa(MaximumStatsGroupFields)
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
	for index := 0; index <= maxEvalPredicates; index++ {
		if index > 0 {
			where.WriteString(" AND ")
		}
		where.WriteString("f")
		where.WriteString(strconv.Itoa(index))
		where.WriteString("=1")
	}
	assertParseDiagnosticCode(t, where.String(), "SPL_QUERY_TOO_COMPLEX")
	lastComparison := " AND f" + strconv.Itoa(maxEvalPredicates) + "=1"
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

func TestParseStatsCountFieldPreservesInputAliasAndFunctionCase(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | stats COUNT(productId) count(action) AS actions count BY host`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*StatsCommand)
	if len(command.Aggregates) != 3 {
		t.Fatalf("aggregates = %#v", command.Aggregates)
	}
	product := command.Aggregates[0]
	if product.Function != AggregateFunctionCountValues ||
		product.Input != "productId" || product.Alias != "count(productId)" {
		t.Fatalf("product count aggregate = %#v", product)
	}
	actions := command.Aggregates[1]
	if actions.Function != AggregateFunctionCountValues ||
		actions.Input != "action" || actions.Alias != "actions" {
		t.Fatalf("action count aggregate = %#v", actions)
	}
	rows := command.Aggregates[2]
	if rows.Function != AggregateFunctionCount || rows.Input != "" || rows.Alias != "count" {
		t.Fatalf("row count aggregate = %#v", rows)
	}
	if len(command.GroupBy) != 1 || command.GroupBy[0].Name != "host" {
		t.Fatalf("group fields = %#v", command.GroupBy)
	}
}

func TestParseStatsCountFieldAbbreviationUsesCanonicalOutputName(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | stats C(productId) c(action) AS actions BY host`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*StatsCommand)
	if len(command.Aggregates) != 2 {
		t.Fatalf("aggregates = %#v, want two value counts", command.Aggregates)
	}
	product := command.Aggregates[0]
	if product.Function != AggregateFunctionCountValues ||
		product.Input != "productId" || product.Alias != "count(productId)" {
		t.Fatalf("product count aggregate = %#v", product)
	}
	actions := command.Aggregates[1]
	if actions.Function != AggregateFunctionCountValues ||
		actions.Input != "action" || actions.Alias != "actions" {
		t.Fatalf("action count aggregate = %#v", actions)
	}
	if len(command.GroupBy) != 1 || command.GroupBy[0].Name != "host" {
		t.Fatalf("group fields = %#v", command.GroupBy)
	}
}

func TestParseStatsCountFieldRequiresExactlyOneExactField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		code   string
	}{
		{name: "missing field", source: `index=main | stats count()`, code: "SPL_EXPECTED_FIELD"},
		{name: "multiple fields", source: `index=main | stats count(left,right)`, code: "SPL_EXPECTED_RIGHT_PAREN"},
		{name: "quoted field", source: `index=main | stats count("status")`, code: "SPL_EXPECTED_FIELD"},
		{name: "missing right parenthesis", source: `index=main | stats count(status`, code: "SPL_EXPECTED_RIGHT_PAREN"},
		{name: "bare abbreviation", source: `index=main | stats c`, code: "SPL_UNSUPPORTED_STATS_SYNTAX"},
		{name: "empty abbreviation", source: `index=main | stats c()`, code: "SPL_EXPECTED_FIELD"},
		{name: "abbreviated eval expression", source: `index=main | stats c(eval(status=200))`, code: "SPL_EXPECTED_RIGHT_PAREN"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Parse(test.source)
			if err == nil {
				t.Fatal("Parse succeeded, want error")
			}
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != test.code {
				t.Fatalf("diagnostic = %#v, want %s", err, test.code)
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
	if percentile.Function != AggregateFunctionPercentile || percentile.Percentile != 95 ||
		percentile.Input != "duration_ms" || percentile.Alias != "p95_ms" {
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
	if aggregate.Alias != "perc95(duration_ms)" {
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

func TestParseStatsMinAndMaxFieldsAliasesAndFunctionCase(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | stats MiN(amount) MaX(label) AS largest BY service`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*StatsCommand)
	if len(command.Aggregates) != 2 {
		t.Fatalf("aggregates = %#v", command.Aggregates)
	}
	minimum := command.Aggregates[0]
	if minimum.Function != AggregateFunctionMinimum ||
		minimum.Input != "amount" || minimum.Alias != "min(amount)" {
		t.Fatalf("min aggregate = %#v", minimum)
	}
	maximum := command.Aggregates[1]
	if maximum.Function != AggregateFunctionMaximum ||
		maximum.Input != "label" || maximum.Alias != "largest" {
		t.Fatalf("max aggregate = %#v", maximum)
	}
	if len(command.GroupBy) != 1 || command.GroupBy[0].Name != "service" {
		t.Fatalf("group fields = %#v", command.GroupBy)
	}
}

func TestParseStatsEarliestAndLatestFieldsAliasesAndFunctionCase(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | stats EaRLiEsT(amount) LaTeSt(label) AS newest BY service`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*StatsCommand)
	if len(command.Aggregates) != 2 {
		t.Fatalf("aggregates = %#v", command.Aggregates)
	}
	earliest := command.Aggregates[0]
	if earliest.Function != AggregateFunctionEarliest ||
		earliest.Input != "amount" || earliest.Alias != "earliest(amount)" {
		t.Fatalf("earliest aggregate = %#v", earliest)
	}
	latest := command.Aggregates[1]
	if latest.Function != AggregateFunctionLatest ||
		latest.Input != "label" || latest.Alias != "newest" {
		t.Fatalf("latest aggregate = %#v", latest)
	}
	if len(command.GroupBy) != 1 || command.GroupBy[0].Name != "service" {
		t.Fatalf("group fields = %#v", command.GroupBy)
	}
}

func TestParseStatsDistinctCountAliasesAndFunctionCase(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | stats DC(user) distinct_COUNT(device) AS devices BY service`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*StatsCommand)
	if len(command.Aggregates) != 2 {
		t.Fatalf("aggregates = %#v", command.Aggregates)
	}
	users := command.Aggregates[0]
	if users.Function != AggregateFunctionDistinctCount || users.Input != "user" || users.Alias != "dc(user)" {
		t.Fatalf("dc aggregate = %#v", users)
	}
	devices := command.Aggregates[1]
	if devices.Function != AggregateFunctionDistinctCount || devices.Input != "device" || devices.Alias != "devices" {
		t.Fatalf("distinct_count aggregate = %#v", devices)
	}
	if len(command.GroupBy) != 1 || command.GroupBy[0].Name != "service" {
		t.Fatalf("group fields = %#v", command.GroupBy)
	}

	canonical, err := Parse(`index=main | stats distinct_count(product)`)
	if err != nil {
		t.Fatalf("Parse distinct_count: %v", err)
	}
	aggregate := canonical.Commands[0].(*StatsCommand).Aggregates[0]
	if aggregate.Function != AggregateFunctionDistinctCount || aggregate.Alias != "dc(product)" {
		t.Fatalf("distinct_count default alias = %#v, want dc(product)", aggregate)
	}
}

func TestParseStatsValuesFieldAliasAndFunctionCase(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | stats VaLuEs(user) VALUES(device) AS devices BY service`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*StatsCommand)
	if len(command.Aggregates) != 2 {
		t.Fatalf("aggregates = %#v", command.Aggregates)
	}
	users := command.Aggregates[0]
	if users.Function != AggregateFunctionValues || users.Input != "user" || users.Alias != "values(user)" {
		t.Fatalf("values aggregate = %#v", users)
	}
	devices := command.Aggregates[1]
	if devices.Function != AggregateFunctionValues || devices.Input != "device" || devices.Alias != "devices" {
		t.Fatalf("aliased values aggregate = %#v", devices)
	}
	if len(command.GroupBy) != 1 || command.GroupBy[0].Name != "service" {
		t.Fatalf("group fields = %#v", command.GroupBy)
	}
}

func TestParseStatsValuesRequiresExactlyOneField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		code   string
	}{
		{name: "missing parentheses", source: `index=main | stats values`, code: "SPL_UNSUPPORTED_STATS_SYNTAX"},
		{name: "missing field", source: `index=main | stats values()`, code: "SPL_EXPECTED_FIELD"},
		{name: "multiple fields", source: `index=main | stats values(left,right)`, code: "SPL_EXPECTED_RIGHT_PAREN"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Parse(test.source)
			if err == nil {
				t.Fatal("Parse succeeded, want error")
			}
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != test.code {
				t.Fatalf("diagnostic = %#v, want %s", err, test.code)
			}
		})
	}
}

func TestParseStatsListFieldAliasAndFunctionCase(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | stats LiSt(user) LIST(device) AS devices BY service`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*StatsCommand)
	if len(command.Aggregates) != 2 {
		t.Fatalf("aggregates = %#v", command.Aggregates)
	}
	users := command.Aggregates[0]
	if users.Function != AggregateFunctionList || users.Input != "user" || users.Alias != "list(user)" {
		t.Fatalf("list aggregate = %#v", users)
	}
	devices := command.Aggregates[1]
	if devices.Function != AggregateFunctionList || devices.Input != "device" || devices.Alias != "devices" {
		t.Fatalf("aliased list aggregate = %#v", devices)
	}
	if len(command.GroupBy) != 1 || command.GroupBy[0].Name != "service" {
		t.Fatalf("group fields = %#v", command.GroupBy)
	}
}

func TestParseStatsListRequiresExactlyOneField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		code   string
	}{
		{name: "missing parentheses", source: `index=main | stats list`, code: "SPL_UNSUPPORTED_STATS_SYNTAX"},
		{name: "missing field", source: `index=main | stats list()`, code: "SPL_EXPECTED_FIELD"},
		{name: "multiple fields", source: `index=main | stats list(left,right)`, code: "SPL_EXPECTED_RIGHT_PAREN"},
	}
	for _, test := range tests {
		test := test
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

func TestParseStatsDistinctCountRequiresExactlyOneField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		code   string
	}{
		{name: "missing parentheses", source: `index=main | stats dc`, code: "SPL_UNSUPPORTED_STATS_SYNTAX"},
		{name: "missing field", source: `index=main | stats distinct_count()`, code: "SPL_EXPECTED_FIELD"},
		{name: "multiple fields", source: `index=main | stats dc(left,right)`, code: "SPL_EXPECTED_RIGHT_PAREN"},
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

func TestParseStatsMinAndMaxRequireExactlyOneField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		code   string
	}{
		{name: "min missing parentheses", source: `index=main | stats min`, code: "SPL_UNSUPPORTED_STATS_SYNTAX"},
		{name: "max missing field", source: `index=main | stats max()`, code: "SPL_EXPECTED_FIELD"},
		{name: "min multiple fields", source: `index=main | stats min(left,right)`, code: "SPL_EXPECTED_RIGHT_PAREN"},
		{name: "max eval expression", source: `index=main | stats max(eval(status=200))`, code: "SPL_EXPECTED_RIGHT_PAREN"},
		{name: "min quoted field", source: `index=main | stats min("status")`, code: "SPL_EXPECTED_FIELD"},
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

func TestParseStatsEarliestAndLatestRequireExactlyOneField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		code   string
	}{
		{name: "earliest missing parentheses", source: `index=main | stats earliest`, code: "SPL_UNSUPPORTED_STATS_SYNTAX"},
		{name: "latest missing field", source: `index=main | stats latest()`, code: "SPL_EXPECTED_FIELD"},
		{name: "earliest multiple fields", source: `index=main | stats earliest(left,right)`, code: "SPL_EXPECTED_RIGHT_PAREN"},
		{name: "latest eval expression", source: `index=main | stats latest(eval(status=200))`, code: "SPL_EXPECTED_RIGHT_PAREN"},
		{name: "earliest quoted field", source: `index=main | stats earliest("status")`, code: "SPL_EXPECTED_FIELD"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Parse(test.source)
			if err == nil {
				t.Fatal("Parse succeeded, want error")
			}
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != test.code {
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

func TestParseBinSpansFieldsAndOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		source      string
		commandText string
		commandName string
		field       string
		output      string
		spanText    string
		spanKind    BinSpanKind
		magnitude   uint64
		unit        TimeSpanUnit
	}{
		{
			name:        "field before span",
			source:      `index=main | bin _time span=5m`,
			commandText: `bin _time span=5m`,
			commandName: "bin",
			field:       "_time",
			output:      "_time",
			spanText:    "5m",
			spanKind:    BinSpanKindTime,
			magnitude:   5,
			unit:        TimeSpanUnitMinute,
		},
		{
			name:        "span before field with case insensitive syntax",
			source:      `index=main | BIN SPAN = 30S _time`,
			commandText: `BIN SPAN = 30S _time`,
			commandName: "bin",
			field:       "_time",
			output:      "_time",
			spanText:    "30S",
			spanKind:    BinSpanKindTime,
			magnitude:   30,
			unit:        TimeSpanUnitSecond,
		},
		{
			name:        "bucket alias",
			source:      `index=main | BuCkEt _time SpAn=2h`,
			commandText: `BuCkEt _time SpAn=2h`,
			commandName: "bucket",
			field:       "_time",
			output:      "_time",
			spanText:    "2h",
			spanKind:    BinSpanKindTime,
			magnitude:   2,
			unit:        TimeSpanUnitHour,
		},
		{
			name:        "generic numeric field",
			source:      `index=main | bin status span=100`,
			commandText: `bin status span=100`,
			commandName: "bin",
			field:       "status",
			output:      "status",
			spanText:    "100",
			spanKind:    BinSpanKindNumeric,
			magnitude:   100,
		},
		{
			name:        "unitless time span remains discriminated numeric",
			source:      `index=main | bin _time span=5`,
			commandText: `bin _time span=5`,
			commandName: "bin",
			field:       "_time",
			output:      "_time",
			spanText:    "5",
			spanKind:    BinSpanKindNumeric,
			magnitude:   5,
		},
		{
			name:        "numeric looking exact field",
			source:      `index=main | bucket span=25 300`,
			commandText: `bucket span=25 300`,
			commandName: "bucket",
			field:       "300",
			output:      "300",
			spanText:    "25",
			spanKind:    BinSpanKindNumeric,
			magnitude:   25,
		},
		{
			name:        "case preserving exact field",
			source:      `index=main | bin _TIME span=5m`,
			commandText: `bin _TIME span=5m`,
			commandName: "bin",
			field:       "_TIME",
			output:      "_TIME",
			spanText:    "5m",
			spanKind:    BinSpanKindTime,
			magnitude:   5,
			unit:        TimeSpanUnitMinute,
		},
		{
			name:        "as output",
			source:      `index=main | bucket span=10 latency AS latency_band`,
			commandText: `bucket span=10 latency AS latency_band`,
			commandName: "bucket",
			field:       "latency",
			output:      "latency_band",
			spanText:    "10",
			spanKind:    BinSpanKindNumeric,
			magnitude:   10,
		},
		{
			name:        "option keyword as output",
			source:      `index=main | bin duration span=10 AS span`,
			commandText: `bin duration span=10 AS span`,
			commandName: "bin",
			field:       "duration",
			output:      "span",
			spanText:    "10",
			spanKind:    BinSpanKindNumeric,
			magnitude:   10,
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
			command, ok := query.Commands[0].(*BinCommand)
			if !ok {
				t.Fatalf("command = %T, want *BinCommand", query.Commands[0])
			}
			if command.Name() != test.commandName {
				t.Fatalf("command name = %q, want %q", command.Name(), test.commandName)
			}
			if command.Field != test.field {
				t.Fatalf("field = %q, want %q", command.Field, test.field)
			}
			if command.Output != test.output {
				t.Fatalf("output = %q, want %q", command.Output, test.output)
			}
			if command.Span.Kind != test.spanKind ||
				command.Span.Magnitude != test.magnitude ||
				command.Span.Unit != test.unit {
				t.Fatalf("span = %#v", command.Span)
			}
			if got := test.source[command.FieldRange.Start.Offset:command.FieldRange.End.Offset]; got != test.field {
				t.Fatalf("field source = %q, want %q", got, test.field)
			}
			if got := test.source[command.OutputRange.Start.Offset:command.OutputRange.End.Offset]; got != test.output {
				t.Fatalf("output source = %q, want %q", got, test.output)
			}
			if got := test.source[command.Span.Range.Start.Offset:command.Span.Range.End.Offset]; got != test.spanText {
				t.Fatalf("span source = %q, want %q", got, test.spanText)
			}
			if got := test.source[command.Range.Start.Offset:command.Range.End.Offset]; got != test.commandText {
				t.Fatalf("command source = %q, want %q", got, test.commandText)
			}
		})
	}
}

func TestParseBinOptionNamesRemainExactFieldsWithoutEqual(t *testing.T) {
	t.Parallel()

	for _, field := range []string{"span", "bins", "minspan", "start", "end", "aligntime"} {
		field := field
		t.Run(field, func(t *testing.T) {
			t.Parallel()

			source := "index=main | bin " + field + " span=10"
			query, err := Parse(source)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			command := query.Commands[0].(*BinCommand)
			if command.Field != field || command.Output != field {
				t.Fatalf("field/output = %q/%q, want %q/%q", command.Field, command.Output, field, field)
			}
			if command.Span.Kind != BinSpanKindNumeric || command.Span.Magnitude != 10 {
				t.Fatalf("span = %#v, want numeric 10", command.Span)
			}
			if got := source[command.FieldRange.Start.Offset:command.FieldRange.End.Offset]; got != field {
				t.Fatalf("field source = %q, want %q", got, field)
			}
			if command.OutputRange != command.FieldRange {
				t.Fatalf("default output range = %#v, want field range %#v", command.OutputRange, command.FieldRange)
			}
		})
	}
}

func TestParseBinRejectsUnsupportedOrMalformedSyntax(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		source    string
		code      string
		locatedAt string
	}{
		{"missing arguments", `index=main | bin`, "SPL_UNSUPPORTED_BIN_SYNTAX", "bin"},
		{"missing span", `index=main | bin _time`, "SPL_UNSUPPORTED_BIN_SYNTAX", "_time"},
		{"missing field", `index=main | bin span=5m`, "SPL_UNSUPPORTED_BIN_SYNTAX", "5m"},
		{"quoted field", `index=main | bin "_time" span=5m`, "SPL_UNSUPPORTED_BIN_SYNTAX", `"_time"`},
		{"wildcard field", `index=main | bin _time* span=5m`, "SPL_UNSUPPORTED_BIN_SYNTAX", "_time*"},
		{"as before source field", `index=main | bin AS bucket_time span=5m`, "SPL_UNSUPPORTED_BIN_SYNTAX", "AS"},
		{"missing as output", `index=main | bin _time span=5m AS`, "SPL_UNSUPPORTED_BIN_SYNTAX", "AS"},
		{"quoted as output", `index=main | bin _time span=5m AS "bucket_time"`, "SPL_UNSUPPORTED_BIN_SYNTAX", `"bucket_time"`},
		{"wildcard as output", `index=main | bin _time span=5m AS bucket_*`, "SPL_UNSUPPORTED_BIN_SYNTAX", "bucket_*"},
		{"duplicate as output", `index=main | bin _time span=5m AS bucket_time AS other`, "SPL_UNSUPPORTED_BIN_SYNTAX", "AS"},
		{"field after as output", `index=main | bin _time span=5m AS bucket_time host`, "SPL_UNSUPPORTED_BIN_SYNTAX", "host"},
		{"bins option", `index=main | bin bins=10 _time`, "SPL_UNSUPPORTED_BIN_SYNTAX", "bins"},
		{"minspan option", `index=main | bin _time minspan=1m`, "SPL_UNSUPPORTED_BIN_SYNTAX", "minspan"},
		{"start option", `index=main | bin start=0 _time span=5m`, "SPL_UNSUPPORTED_BIN_SYNTAX", "start"},
		{"end option", `index=main | bin end=100 _time span=5m`, "SPL_UNSUPPORTED_BIN_SYNTAX", "end"},
		{"aligntime option", `index=main | bin _time span=5m aligntime=earliest`, "SPL_UNSUPPORTED_BIN_SYNTAX", "aligntime"},
		{"duplicate span", `index=main | bin span=5m _time span=10m`, "SPL_UNSUPPORTED_BIN_SYNTAX", "span"},
		{"duplicate field", `index=main | bin _time span=5m _time`, "SPL_UNSUPPORTED_BIN_SYNTAX", "_time"},
		{"second field", `index=main | bin _time host span=5m`, "SPL_UNSUPPORTED_BIN_SYNTAX", "host"},
		{"comma separated fields", `index=main | bin _time, host span=5m`, "SPL_UNSUPPORTED_BIN_SYNTAX", "host"},
		{"span without equal", `index=main | bin _time span 5m`, "SPL_EXPECTED_EQUAL", "span"},
		{"missing span value", `index=main | bin _time span=`, "SPL_INVALID_ARGUMENT", "span"},
		{"quoted span", `index=main | bin _time span="5m"`, "SPL_INVALID_ARGUMENT", `"5m"`},
		{"zero numeric span", `index=main | bin _time span=0`, "SPL_INVALID_ARGUMENT", "0"},
		{"zero time span", `index=main | bin _time span=0m`, "SPL_INVALID_ARGUMENT", "0m"},
		{"negative numeric span", `index=main | bin value span=-5`, "SPL_INVALID_ARGUMENT", "-5"},
		{"negative span", `index=main | bin _time span=-5m`, "SPL_INVALID_ARGUMENT", "-5m"},
		{"fractional span", `index=main | bin _time span=1.5m`, "SPL_INVALID_ARGUMENT", "1.5m"},
		{"fractional numeric span", `index=main | bin value span=1.5`, "SPL_INVALID_ARGUMENT", "1.5"},
		{"exponent numeric span", `index=main | bin value span=1e3`, "SPL_INVALID_ARGUMENT", "1e3"},
		{"compound span", `index=main | bin _time span=1h30m`, "SPL_INVALID_ARGUMENT", "1h30m"},
		{"calendar span", `index=main | bin _time span=1d`, "SPL_UNSUPPORTED_BIN_SYNTAX", "1d"},
		{"subsecond span", `index=main | bin _time span=500ms`, "SPL_UNSUPPORTED_BIN_SYNTAX", "500ms"},
		{"log span", `index=main | bin _time span=2log10`, "SPL_UNSUPPORTED_BIN_SYNTAX", "2log10"},
		{"duration overflow", `index=main | bin _time span=2562048h`, "SPL_NUMBER_OUT_OF_RANGE", "2562048h"},
		{"time integer overflow", `index=main | bucket span=18446744073709551616s _time`, "SPL_NUMBER_OUT_OF_RANGE", "18446744073709551616s"},
		{"numeric integer overflow", `index=main | bucket span=18446744073709551616 value`, "SPL_NUMBER_OUT_OF_RANGE", "18446744073709551616"},
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
			got := test.source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset]
			if got != test.locatedAt {
				t.Fatalf("diagnostic source = %q, want %q", got, test.locatedAt)
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
		{"log span keeps legacy diagnostic", `index=main | timechart span=2log10 count by level`, "SPL_INVALID_ARGUMENT", "2log10"},
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

func TestParseChartAcceptsBothTwoFieldPivotSpellings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		source      string
		over        string
		splitBy     string
		overSpelled bool
	}{
		{
			name:        "over then by",
			source:      `index=gradethis | chart count over path by status_class`,
			over:        "path",
			splitBy:     "status_class",
			overSpelled: true,
		},
		{
			name:        "by with comma",
			source:      `index=gradethis | chart count by path, status_class`,
			over:        "path",
			splitBy:     "status_class",
			overSpelled: false,
		},
		{
			name:        "by with whitespace only",
			source:      `index=gradethis | chart count by path status_class`,
			over:        "path",
			splitBy:     "status_class",
			overSpelled: false,
		},
		{
			name:        "by with comma and whitespace",
			source:      `index=gradethis | chart count by path , status_class`,
			over:        "path",
			splitBy:     "status_class",
			overSpelled: false,
		},
		{
			name:        "keyword and aggregate case insensitivity",
			source:      `index=gradethis | CHART COUNT OVER path BY level`,
			over:        "path",
			splitBy:     "level",
			overSpelled: true,
		},
		{
			name:        "dotted dynamic paths",
			source:      `index=gradethis | chart count over http.route by http.status_class`,
			over:        "http.route",
			splitBy:     "http.status_class",
			overSpelled: true,
		},
		{
			name:        "binned numeric row axis",
			source:      `index=gradethis | bin severity span=10 | chart count over severity by level`,
			over:        "severity",
			splitBy:     "level",
			overSpelled: true,
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
			command, ok := query.Commands[len(query.Commands)-1].(*ChartCommand)
			if !ok {
				t.Fatalf("last command = %T, want *ChartCommand", query.Commands[len(query.Commands)-1])
			}
			if command.Function != AggregateFunctionCount || command.Over.Name != test.over ||
				command.SplitBy.Name != test.splitBy || command.OverSpelledOver != test.overSpelled {
				t.Fatalf("chart = %#v", command)
			}
			if aggregateText := test.source[command.AggregateRange.Start.Offset:command.AggregateRange.End.Offset]; !strings.EqualFold(aggregateText, "count") {
				t.Fatalf("aggregate source = %q", aggregateText)
			}
			if overText := test.source[command.Over.Range.Start.Offset:command.Over.Range.End.Offset]; overText != test.over {
				t.Fatalf("row field source = %q, want %q", overText, test.over)
			}
			if splitText := test.source[command.SplitBy.Range.Start.Offset:command.SplitBy.Range.End.Offset]; splitText != test.splitBy {
				t.Fatalf("column field source = %q, want %q", splitText, test.splitBy)
			}
			commandText := test.source[command.Range.Start.Offset:command.Range.End.Offset]
			if !strings.HasPrefix(strings.ToLower(commandText), "chart") ||
				!strings.HasSuffix(commandText, test.splitBy) {
				t.Fatalf("command source = %q", commandText)
			}
			if command.Name() != "chart" {
				t.Fatalf("command name = %q", command.Name())
			}
			if command.SourceRange() != command.Range {
				t.Fatalf("source range = %#v, want %#v", command.SourceRange(), command.Range)
			}
		})
	}
}

func TestParseChartSpellingsProduceIdenticalAxes(t *testing.T) {
	t.Parallel()

	overForm, err := Parse(`index=gradethis | chart count over path by level`)
	if err != nil {
		t.Fatalf("Parse(OVER form): %v", err)
	}
	byForm, err := Parse(`index=gradethis | chart count by path, level`)
	if err != nil {
		t.Fatalf("Parse(BY form): %v", err)
	}
	over := overForm.Commands[0].(*ChartCommand)
	by := byForm.Commands[0].(*ChartCommand)
	if over.Function != by.Function || over.Over.Name != by.Over.Name || over.SplitBy.Name != by.SplitBy.Name {
		t.Fatalf("axes differ: %#v vs %#v", over, by)
	}
	if !over.OverSpelledOver || by.OverSpelledOver {
		t.Fatalf("spelling flags = %v/%v, want true/false", over.OverSpelledOver, by.OverSpelledOver)
	}
}

// TestParseChartSpellingsAgreeOnKeywordFieldNames pins that the two documented
// spellings stay interchangeable for a field whose name happens to be a chart
// keyword. OVER is only a misplaced keyword where a field cannot begin, so a
// comma-separated BY list accepts it exactly as the OVER form does.
func TestParseChartSpellingsAgreeOnKeywordFieldNames(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		over   string
		by     string
		row    string
		column string
	}{
		{"OVER as the row field", `chart count over over by level`, `chart count by over, level`, "over", "level"},
		{"OVER as the column field", `chart count over level by over`, `chart count by level, over`, "level", "over"},
		{"BY as the column field", `chart count over path by by`, `chart count by path, by`, "path", "by"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			for _, source := range []string{`index=gradethis | ` + test.over, `index=gradethis | ` + test.by} {
				parsed, err := Parse(source)
				if err != nil {
					t.Fatalf("Parse(%q): %v", source, err)
				}
				command, ok := parsed.Commands[0].(*ChartCommand)
				if !ok || command.Over.Name != test.row || command.SplitBy.Name != test.column {
					t.Fatalf("Parse(%q) axes = %#v, want %q/%q", source, parsed.Commands[0], test.row, test.column)
				}
			}
		})
	}

	// The genuinely ambiguous whitespace-separated form stays rejected, so the
	// documented BY-before-OVER boundary is unchanged.
	for _, source := range []string{
		`index=gradethis | chart count by path over level`,
		`index=gradethis | chart count by path over, level`,
		`index=gradethis | chart count by level over`,
	} {
		diagnostic, ok := errorFor(t, source).(*Diagnostic)
		if !ok || diagnostic.Code != "SPL_UNSUPPORTED_CHART_SYNTAX" {
			t.Fatalf("Parse(%q) diagnostic = %#v", source, diagnostic)
		}
	}
}

func errorFor(t *testing.T, source string) error {
	t.Helper()
	_, err := Parse(source)
	if err == nil {
		t.Fatalf("Parse(%q) succeeded", source)
	}
	return err
}

func TestParseChartRejectsUnsupportedAggregates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		source    string
		locatedAt string
	}{
		{"missing aggregate", `index=main | chart`, ""},
		{"sum", `index=main | chart sum(bytes) over path by level`, "sum"},
		{"average", `index=main | chart avg(bytes) over path by level`, "avg"},
		{"percentile", `index=main | chart p95(bytes) over path by level`, "p95"},
		{"count field argument", `index=main | chart count(level) over path by level`, "count"},
		{"count empty arguments", `index=main | chart count() over path by level`, "count"},
		{"second aggregate", `index=main | chart count avg(bytes) over path by level`, "avg"},
		{"comma separated aggregates", `index=main | chart count, sum(bytes) over path by level`, ","},
		{"sparkline", `index=main | chart sparkline(count) over path by level`, "sparkline"},
		{"eval aggregate", `index=main | chart eval(count/2) over path by level`, "eval"},
		{"parenthesized eval expression", `index=main | chart (count/2) over path by level`, "("},
		{"agg term", `index=main | chart agg=count over path by level`, "agg"},
		{"distinct count", `index=main | chart dc(level) over path by level`, "dc"},
		{"distinct count alias", `index=main | chart distinct_count(level) over path by level`, "distinct_count"},
		{"aggregate alias", `index=main | chart count AS total over path by level`, "AS"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Parse(test.source)
			diagnostic, ok := err.(*Diagnostic)
			if !ok || diagnostic.Code != "SPL_UNSUPPORTED_CHART_AGGREGATE" {
				t.Fatalf("diagnostic = %#v, want SPL_UNSUPPORTED_CHART_AGGREGATE", err)
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

func TestParseChartRejectsEveryOptionIncludingDocumentedDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		source    string
		locatedAt string
	}{
		{"limit default spelling", `index=main | chart count over path by level limit=10`, "limit"},
		{"limit top form", `index=main | chart count over path by level limit=top 10`, "limit"},
		{"useother default", `index=main | chart count over path by level useother=true`, "useother"},
		{"usenull default", `index=main | chart count over path by level usenull=true`, "usenull"},
		{"otherstr default", `index=main | chart count over path by level otherstr=OTHER`, "otherstr"},
		{"nullstr default", `index=main | chart count over path by level nullstr=NULL`, "nullstr"},
		{"cont default", `index=main | chart count over path by level cont=true`, "cont"},
		{"bins default", `index=main | chart count over path by level bins=300`, "bins"},
		{"dedup_splitvals", `index=main | chart count over path by level dedup_splitvals=false`, "dedup_splitvals"},
		{"span", `index=main | chart span=5m count over _time by level`, "span"},
		{"start", `index=main | chart count over severity by level start=0`, "start"},
		{"end", `index=main | chart count over severity by level end=100`, "end"},
		{"aligntime", `index=main | chart count over _time by level aligntime=earliest`, "aligntime"},
		{"format", `index=main | chart count over path by level format=$AGG$`, "format"},
		{"sep", `index=main | chart count over path by level sep=:`, "sep"},
		{"option before aggregate", `index=main | chart limit=10 count over path by level`, "limit"},
		{"option between aggregate and over", `index=main | chart count limit=10 over path by level`, "limit"},
		{"option in place of the row field", `index=main | chart count over limit=10 by level`, "limit"},
		{"option in place of the column field", `index=main | chart count over path by limit=10`, "limit"},
		{"option after a BY list", `index=main | chart count by path, level useother=false`, "useother"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Parse(test.source)
			diagnostic, ok := err.(*Diagnostic)
			if !ok || diagnostic.Code != "SPL_UNSUPPORTED_CHART_OPTION" {
				t.Fatalf("diagnostic = %#v, want SPL_UNSUPPORTED_CHART_OPTION", err)
			}
			got := test.source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset]
			if got != test.locatedAt {
				t.Fatalf("diagnostic source = %q, want %q", got, test.locatedAt)
			}
		})
	}
}

func TestParseChartRejectsUnsupportedOrMalformedSyntax(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		source    string
		code      string
		locatedAt string
	}{
		{"single OVER split", `index=main | chart count over path`, "SPL_UNSUPPORTED_CHART_SYNTAX", ""},
		{"single BY split", `index=main | chart count by path`, "SPL_UNSUPPORTED_CHART_SYNTAX", ""},
		{"no split at all", `index=main | chart count`, "SPL_UNSUPPORTED_CHART_SYNTAX", ""},
		{"identical OVER and BY fields", `index=main | chart count over path by path`, "SPL_UNSUPPORTED_CHART_SYNTAX", "path"},
		{"identical BY list fields", `index=main | chart count by path, path`, "SPL_UNSUPPORTED_CHART_SYNTAX", "path"},
		{"three BY fields", `index=main | chart count by path, level, host`, "SPL_UNSUPPORTED_CHART_SYNTAX", ","},
		{"three space separated BY fields", `index=main | chart count by path level host`, "SPL_UNSUPPORTED_CHART_SYNTAX", "host"},
		{"two column splits after OVER", `index=main | chart count over path by level, host`, "SPL_UNSUPPORTED_CHART_SYNTAX", ","},
		{"BY before OVER", `index=main | chart count by path over level`, "SPL_UNSUPPORTED_CHART_SYNTAX", "over"},
		{"repeated OVER", `index=main | chart count over path over level`, "SPL_UNSUPPORTED_CHART_SYNTAX", "over"},
		{"missing OVER or BY", `index=main | chart count path level`, "SPL_UNSUPPORTED_CHART_SYNTAX", "path"},
		{"wildcard row field", `index=main | chart count over path* by level`, "SPL_UNSUPPORTED_CHART_SYNTAX", "path*"},
		{"wildcard column field", `index=main | chart count over path by level*`, "SPL_UNSUPPORTED_CHART_SYNTAX", "level*"},
		{"wildcard BY list field", `index=main | chart count by path, level*`, "SPL_UNSUPPORTED_CHART_SYNTAX", "level*"},
		{"quoted row field", `index=main | chart count over "path" by level`, "SPL_UNSUPPORTED_CHART_SYNTAX", `"path"`},
		{"quoted column field", `index=main | chart count over path by "level"`, "SPL_UNSUPPORTED_CHART_SYNTAX", `"level"`},
		{"trailing where clause", `index=main | chart count over path by level where count > 100`, "SPL_UNSUPPORTED_CHART_SYNTAX", "where"},
		{"trailing in top clause", `index=main | chart count over path by level in top 5`, "SPL_UNSUPPORTED_CHART_SYNTAX", "in"},
		{"trailing token", `index=main | chart count by path, level extra`, "SPL_UNSUPPORTED_CHART_SYNTAX", "extra"},
		{"missing field after OVER", `index=main | chart count over`, "SPL_EXPECTED_FIELD", ""},
		{"missing field after BY", `index=main | chart count over path by`, "SPL_EXPECTED_FIELD", ""},
		{"missing field after BY list", `index=main | chart count by`, "SPL_EXPECTED_FIELD", ""},
		{"trailing comma promises a third field", `index=main | chart count by path, level,`, "SPL_UNSUPPORTED_CHART_SYNTAX", ","},
		{"trailing comma after one field", `index=main | chart count by path,`, "SPL_EXPECTED_FIELD", ""},
		{"leading comma", `index=main | chart count by , path`, "SPL_EXPECTED_FIELD", ","},
		{"empty comma", `index=main | chart count by path,, level`, "SPL_EXPECTED_FIELD", ","},
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

func TestParseChartSingleSplitSuggestsStats(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | chart count over path`,
		`index=main | chart count by path`,
		`index=main | chart count`,
	} {
		_, err := Parse(source)
		diagnostic, ok := err.(*Diagnostic)
		if !ok || diagnostic.Code != "SPL_UNSUPPORTED_CHART_SYNTAX" {
			t.Fatalf("Parse(%q) diagnostic = %#v", source, err)
		}
		if !slices.Contains(diagnostic.Suggestions, "stats count BY <field>") {
			t.Fatalf("Parse(%q) suggestions = %v, want the stats equivalent", source, diagnostic.Suggestions)
		}
	}
}

func TestParseChartCountsAsOnePipelineCommand(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | bin severity span=10 | chart count over severity by level`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(query.Commands) != 2 {
		t.Fatalf("command count = %d, want bin and chart", len(query.Commands))
	}
	if _, ok := query.Commands[1].(*ChartCommand); !ok {
		t.Fatalf("second command = %T, want *ChartCommand", query.Commands[1])
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
		{"other function", "index=main\n| stats median(bytes)", "SPL_UNSUPPORTED_STATS_AGGREGATE", 2, 9},
		{"second aggregate", `* | stats count, median(host)`, "SPL_UNSUPPORTED_STATS_AGGREGATE", 1, 18},
		{"space-separated aggregate", `* | stats count mode(host)`, "SPL_UNSUPPORTED_STATS_AGGREGATE", 1, 17},
		{"first remains unsupported", `* | stats first(host)`, "SPL_UNSUPPORTED_STATS_AGGREGATE", 1, 11},
		{"last remains unsupported", `* | stats last(host)`, "SPL_UNSUPPORTED_STATS_AGGREGATE", 1, 11},
		{"earliest_time remains unsupported", `* | stats earliest_time(host)`, "SPL_UNSUPPORTED_STATS_AGGREGATE", 1, 11},
		{"latest_time remains unsupported", `* | stats latest_time(host)`, "SPL_UNSUPPORTED_STATS_AGGREGATE", 1, 11},
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
		`index=main | stats count(productId) AS products by host`,
		`index=main | stats sum(bytes) by host`,
		`index=main | stats min(duration_ms) max(duration_ms) AS slowest by path`,
		`index=main | stats dc(user) distinct_count(device) AS devices by service`,
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
