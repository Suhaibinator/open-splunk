package spl

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
)

func TestParseAuthoredArithmeticPrecedenceAssociativityAndGrouping(t *testing.T) {
	t.Parallel()

	query, err := Parse(`| eval value="value=" . (1+2)*3-20/5/2`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	expression := query.Commands[0].(*EvalCommand).Assignments[0].Expression
	concat, ok := expression.(*ScalarCallExpr)
	if !ok || concat.Function != ScalarFunctionConcat || len(concat.Arguments) != 2 {
		t.Fatalf("expression = %#v, want two-operand concat", expression)
	}
	subtract, ok := concat.Arguments[1].(*ScalarBinaryExpr)
	if !ok || subtract.Op != ScalarBinaryOpSubtract {
		t.Fatalf("concat arithmetic = %#v, want subtraction", concat.Arguments[1])
	}
	multiply, ok := subtract.Left.(*ScalarBinaryExpr)
	if !ok || multiply.Op != ScalarBinaryOpMultiply {
		t.Fatalf("subtraction left = %#v, want multiplication", subtract.Left)
	}
	groupedAdd, ok := multiply.Left.(*ScalarBinaryExpr)
	if !ok || groupedAdd.Op != ScalarBinaryOpAdd {
		t.Fatalf("multiplication left = %#v, want grouped addition", multiply.Left)
	}
	if got := `| eval value="value=" . (1+2)*3-20/5/2`[groupedAdd.Range.Start.Offset:groupedAdd.Range.End.Offset]; got != `(1+2)` {
		t.Fatalf("grouped range = %q, want (1+2)", got)
	}
	outerDivide, ok := subtract.Right.(*ScalarBinaryExpr)
	if !ok || outerDivide.Op != ScalarBinaryOpDivide {
		t.Fatalf("subtraction right = %#v, want division", subtract.Right)
	}
	innerDivide, ok := outerDivide.Left.(*ScalarBinaryExpr)
	if !ok || innerDivide.Op != ScalarBinaryOpDivide {
		t.Fatalf("division left = %#v, want left-associated division", outerDivide.Left)
	}

	query, err = Parse(`| eval value=- - field`)
	if err != nil {
		t.Fatalf("Parse unary: %v", err)
	}
	outerUnary, ok := query.Commands[0].(*EvalCommand).Assignments[0].Expression.(*ScalarUnaryExpr)
	if !ok || outerUnary.Op != ScalarUnaryOpNegative {
		t.Fatalf("outer unary = %#v", query.Commands[0])
	}
	innerUnary, ok := outerUnary.Operand.(*ScalarUnaryExpr)
	if !ok || innerUnary.Op != ScalarUnaryOpNegative {
		t.Fatalf("inner unary = %#v", outerUnary.Operand)
	}
	assertSourceRangeText(t, `| eval value=- - field`, outerUnary.Range, `- - field`)
	assertSourceRangeText(t, `| eval value=- - field`, innerUnary.Range, `- field`)

	for _, test := range []struct {
		source string
		op     ScalarUnaryOp
	}{
		{`| eval value=+2`, ScalarUnaryOpPositive},
		{`| eval value=-2`, ScalarUnaryOpNegative},
	} {
		query, err := Parse(test.source)
		if err != nil {
			t.Fatalf("Parse(%q): %v", test.source, err)
		}
		unary, ok := query.Commands[0].(*EvalCommand).Assignments[0].Expression.(*ScalarUnaryExpr)
		if !ok || unary.Op != test.op {
			t.Fatalf("Parse(%q) expression = %#v, want unary %v", test.source, query.Commands[0], test.op)
		}
	}
}

func TestParseAuthoredWhereDisambiguatesScalarAndBooleanGroups(t *testing.T) {
	t.Parallel()

	query, err := Parse(`| where ((bytes+overhead)/1024)>10 AND (status=500 OR status==503)`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	root, ok := query.Commands[0].(*WhereCommand).Expression.(*WhereBoolExpr)
	if !ok || root.Op != BoolOpAnd {
		t.Fatalf("where root = %#v, want AND", query.Commands[0])
	}
	comparison, ok := root.Left.(*WhereComparisonExpr)
	if !ok || comparison.Op != CompareOpGreater {
		t.Fatalf("left = %#v, want scalar comparison", root.Left)
	}
	if _, isBinary := comparison.Left.(*ScalarBinaryExpr); !isBinary {
		t.Fatalf("left scalar = %#v, want grouped arithmetic", comparison.Left)
	}
	booleanGroup, ok := root.Right.(*WhereBoolExpr)
	if !ok || booleanGroup.Op != BoolOpOr {
		t.Fatalf("right = %#v, want Boolean group", root.Right)
	}

	for _, source := range []string{
		`| where (bytes+1)`,
	} {
		if _, err := Parse(source); err == nil {
			t.Fatalf("Parse(%q) succeeded; scalar truthiness/Boolean assignment must fail", source)
		}
	}
	assertAuthoredDiagnosticCode(t, `| eval value=(status=500 OR status=503)`, "SPL_UNSUPPORTED_EVAL_EXPRESSION")

	for _, source := range []string{
		`| where NOT-x>0`,
		`| where (x=1) AND-y>0`,
		`| where (x=1) OR-y>0`,
		`| where x IN (1) AND-y>0`,
		`| where x IN (1) OR-y>0`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse operator-adjacent Boolean boundary %q: %v", source, err)
		}
	}
}

func TestParseAuthoredQuotedScalarFieldsAndDestinations(t *testing.T) {
	t.Parallel()

	const source = `| eval 'error-rate'='request\'bytes'/1024 | where 'HTTP Status'==500`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	assignment := query.Commands[0].(*EvalCommand).Assignments[0]
	if assignment.Field != "error-rate" {
		t.Fatalf("destination = %q", assignment.Field)
	}
	assertSourceRangeText(t, source, assignment.FieldRange, `'error-rate'`)
	binary, ok := assignment.Expression.(*ScalarBinaryExpr)
	if !ok || binary.Op != ScalarBinaryOpDivide {
		t.Fatalf("assignment = %#v, want division", assignment.Expression)
	}
	field, ok := binary.Left.(*ScalarFieldExpr)
	if !ok || field.Field != "request'bytes" {
		t.Fatalf("decoded field = %#v", binary.Left)
	}
	assertSourceRangeText(t, source, field.Range, `'request\'bytes'`)
	where := query.Commands[1].(*WhereCommand).Expression.(*WhereComparisonExpr)
	whereField, ok := where.Left.(*ScalarFieldExpr)
	if !ok || whereField.Field != "HTTP Status" || where.Op != CompareOpEqual {
		t.Fatalf("where = %#v", where)
	}
	assertSourceRangeText(t, source, whereField.Range, `'HTTP Status'`)

	for _, source := range []string{
		`| eval value=''`,
		`| eval value=' leading'`,
		`| eval value='trailing '`,
		"| eval value='\u00a0leading'",
		`| eval value='wild*'`,
		`| eval value='wild?'`,
		`| eval value='__os_private'`,
		`| eval value='bad\n'`,
		"| eval value='bad\nfield'",
		`| eval value='` + strings.Repeat("x", eventfields.MaximumDynamicPathSegmentBytes+1) + `'`,
		`| eval value='` + strings.Repeat("x.", eventfields.MaximumDynamicPathSegments+1) + `x'`,
		"| eval value=`backtick`",
	} {
		if _, err := Parse(source); err == nil {
			t.Fatalf("Parse(%q) succeeded, want invalid quoted-field diagnostic", source)
		}
	}
	assertAuthoredDiagnosticCode(t, `| eval value='bad\q'`, "SPL_INVALID_FIELD_QUOTE_ESCAPE")
	assertAuthoredDiagnosticCode(t, string(append([]byte(`| eval value='bad`), []byte{0xff, '\''}...)), "SPL_INVALID_FIELD")
	assertAuthoredDiagnosticCode(t, `| eval value='unterminated`, "SPL_UNTERMINATED_FIELD_QUOTE")
	assertAuthoredDiagnosticCode(t, `| eval value=1+'unterminated`, "SPL_UNTERMINATED_FIELD_QUOTE")
	for _, diagnosticCase := range []struct {
		source string
		want   string
	}{
		{source: `| eval value='bad\q'`, want: `\q`},
		{source: `| eval value='unterminated`, want: `'`},
		{source: `| eval value=1+'unterminated`, want: `'`},
	} {
		_, diagnosticErr := Parse(diagnosticCase.source)
		var diagnostic *Diagnostic
		if !errors.As(diagnosticErr, &diagnostic) {
			t.Fatalf("Parse(%q) error = %v, want diagnostic", diagnosticCase.source, diagnosticErr)
		}
		assertSourceRangeText(t, diagnosticCase.source, diagnostic.Range, diagnosticCase.want)
	}
	assertAuthoredDiagnosticCode(t, `| eval value=''`, "SPL_EXPECTED_FIELD")
	quotedStats, quotedStatsErr := Parse(`| stats avg('request-bytes')`)
	if quotedStatsErr != nil {
		t.Fatalf("quoted stats field: %v", quotedStatsErr)
	}
	quotedAggregate := quotedStats.Commands[0].(*StatsCommand).Aggregates[0]
	if quotedAggregate.Input != "request-bytes" || !quotedAggregate.InputQuoted {
		t.Fatalf("quoted stats aggregate = %#v", quotedAggregate)
	}
	if _, err := Parse(`| eval value='pipe|comma,paren()=operator'`); err != nil {
		t.Fatalf("quoted field containing lexer punctuation: %v", err)
	}
	query, err = Parse(`| eval value='path\\\\leaf'`)
	if err != nil {
		t.Fatalf("quoted field with existing two-layer path escape: %v", err)
	}
	pathField, ok := query.Commands[0].(*EvalCommand).Assignments[0].Expression.(*ScalarFieldExpr)
	if !ok || pathField.Field != `path\\leaf` {
		t.Fatalf("quote-decoded path field = %#v, want two backslashes for path decoding", pathField)
	}

	for _, adjacent := range []struct {
		source string
		field  string
	}{
		{source: `| eval value=1+'request-bytes'`, field: "request-bytes"},
		{source: `| eval value=1+'pipe|comma,paren()=operator'`, field: "pipe|comma,paren()=operator"},
		{source: `| where 0%'field'>0`, field: "field"},
	} {
		adjacentQuery, err := Parse(adjacent.source)
		if err != nil {
			t.Fatalf("Parse operator-adjacent quote %q: %v", adjacent.source, err)
		}
		var arithmetic *ScalarBinaryExpr
		if eval, isEval := adjacentQuery.Commands[0].(*EvalCommand); isEval {
			arithmetic, _ = eval.Assignments[0].Expression.(*ScalarBinaryExpr)
		} else {
			comparison := adjacentQuery.Commands[0].(*WhereCommand).Expression.(*WhereComparisonExpr)
			arithmetic, _ = comparison.Left.(*ScalarBinaryExpr)
		}
		if arithmetic == nil {
			t.Fatalf("operator-adjacent quote AST = %#v, want binary arithmetic", adjacentQuery.Commands[0])
		}
		adjacentField, isField := arithmetic.Right.(*ScalarFieldExpr)
		if !isField || adjacentField.Field != adjacent.field {
			t.Fatalf("operator-adjacent quote AST = %#v, want field %q", adjacentQuery.Commands[0], adjacent.field)
		}
	}
	query, err = Parse(`| eval value=-'request-bytes'`)
	if err != nil {
		t.Fatalf("Parse unary operator-adjacent quote: %v", err)
	}
	unary, ok := query.Commands[0].(*EvalCommand).Assignments[0].Expression.(*ScalarUnaryExpr)
	if !ok {
		t.Fatalf("unary quoted operand AST = %#v", query.Commands[0])
	}
	field, fieldOK := unary.Operand.(*ScalarFieldExpr)
	if !fieldOK || field.Field != "request-bytes" {
		t.Fatalf("unary quoted operand AST = %#v", query.Commands[0])
	}
	assertAuthoredDiagnosticCode(t, `| where 0%'.'>0`, "SPL_INVALID_FIELD")

	segment := strings.Repeat(",", eventfields.MaximumDynamicPathSegmentBytes)
	largeQuotedField := strings.Join(
		makeRepeatedStrings(segment, eventfields.MaximumDynamicPathSegments+1),
		".",
	)
	if _, err := Parse(`| eval value=1+'` + largeQuotedField + `'`); err != nil {
		t.Fatalf("operator-adjacent quoted field should count as one syntax token: %v", err)
	}
	assertAuthoredDiagnosticCode(t, `1+'`+largeQuotedField+`'`, "SPL_QUERY_TOO_COMPLEX")
}

func TestParseAuthoredMembershipFormsAndCategoryBoundary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source  string
		negated bool
		count   int
	}{
		{`| where in(status, 400, 401+2)`, false, 2},
		{`| where status IN (400)`, false, 1},
		{`| where status NOT IN (200, 201, 204)`, true, 3},
		{`| where NOT in(status, 200)`, false, 1},
	}
	for _, test := range tests {
		query, err := Parse(test.source)
		if err != nil {
			t.Fatalf("Parse(%q): %v", test.source, err)
		}
		where := query.Commands[0].(*WhereCommand).Expression
		if not, ok := where.(*WhereNotExpr); ok {
			where = not.Operand
		}
		membership, ok := where.(*WhereMembershipExpr)
		if !ok || membership.Negated != test.negated || len(membership.Candidates) != test.count {
			t.Fatalf("Parse(%q) membership = %#v", test.source, where)
		}
		if test.source == `| where status IN (400)` {
			assertSourceRangeText(t, test.source, membership.Range, `status IN (400)`)
			assertSourceRangeText(t, test.source, membership.Value.SourceRange(), `status`)
			assertSourceRangeText(t, test.source, membership.Candidates[0].SourceRange(), `400`)
		}
	}

	for _, source := range []string{
		`| where status IN ()`,
		`| where in(status)`,
		`| where status IN (1,)`,
		`| where status IN (,1)`,
		`| eval result=in(status, 200)`,
		`| where (status IN (200))=true`,
	} {
		if _, err := Parse(source); err == nil {
			t.Fatalf("Parse(%q) succeeded, want membership category/syntax rejection", source)
		}
	}
	assertAuthoredDiagnosticCode(t, `| where (status IN (200))=true`, "SPL_UNSUPPORTED_MEMBERSHIP_SYNTAX")
	assertAuthoredDiagnosticCode(t, `| eval selected=status IN (200,201)`, "SPL_UNSUPPORTED_EVAL_EXPRESSION")
	assertAuthoredDiagnosticCode(t, `| eval selected=in(status,200)`, "SPL_UNSUPPORTED_EVAL_EXPRESSION")

	for _, source := range []string{
		`| eval class=if(environment IN ("production","staging"),"managed","other")`,
		`| eval class=case(environment IN ("production"),"managed",status NOT IN (200),"error")`,
		`| stats count(eval(status IN (500,502,503))) AS failures`,
		`| eventstats count(eval(status IN (500,502,503))) AS failures`,
		`| streamstats count(eval(status IN (500,502,503))) AS failures`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse shared membership surface %q: %v", source, err)
		}
	}
}

// equalDifferentialErrors reports whether two lexer/parser errors agree for
// differential testing. Both surfaces produce *Diagnostic, whose Error()
// string omits Range.End and Suggestions, so the diagnostics are compared as
// concrete structs to keep the full structural strength of the differential;
// any other error type must match exactly by type and message.
func equalDifferentialErrors(got, want error) bool {
	if (got == nil) != (want == nil) {
		return false
	}
	if got == nil {
		return true
	}
	//nolint:errorlint // Differential comparison of two lexer/parser surfaces:
	// the outer error shape must agree exactly, so unwrapping via errors.As
	// would wrongly equate a wrapped diagnostic with a bare one.
	gotDiagnostic, gotOK := got.(*Diagnostic)
	wantDiagnostic, wantOK := want.(*Diagnostic) //nolint:errorlint // See above.
	if gotOK != wantOK {
		return false
	}
	if gotOK {
		return reflect.DeepEqual(*gotDiagnostic, *wantDiagnostic)
	}
	return reflect.TypeOf(got) == reflect.TypeOf(want) &&
		got.Error() == want.Error()
}

func TestLexAuthoredScalarOperatorsAreContextSafeAndSourceExact(t *testing.T) {
	t.Parallel()

	// Outside a token-opening single quote, enabling quoted-field recognition
	// must not alter the legacy token stream for any ASCII byte.
	for value := range 128 {
		source := "a" + string(rune(value)) + "b"
		got, gotErr := lex(source)
		want, wantErr := lexWithQuotedFields(source, false)
		if !reflect.DeepEqual(got, want) || !equalDifferentialErrors(gotErr, wantErr) {
			t.Fatalf("ASCII %#02x token differential: got %#v, %v; want %#v, %v", value, got, gotErr, want, wantErr)
		}
	}

	operators := []struct {
		spelling string
		op       ScalarBinaryOp
	}{
		{`+`, ScalarBinaryOpAdd},
		{`-`, ScalarBinaryOpSubtract},
		{`*`, ScalarBinaryOpMultiply},
		{`/`, ScalarBinaryOpDivide},
		{`%`, ScalarBinaryOpRemainder},
	}
	for _, operator := range operators {
		for _, whitespace := range []string{"", " ", "\t", "\n", "\u00a0", "\u2003"} {
			source := `| eval value=left` + whitespace + operator.spelling + whitespace + `right`
			query, err := Parse(source)
			if err != nil {
				t.Fatalf("Parse(%q): %v", source, err)
			}
			binary, ok := query.Commands[0].(*EvalCommand).Assignments[0].Expression.(*ScalarBinaryExpr)
			if !ok || binary.Op != operator.op {
				t.Fatalf("Parse(%q) expression = %#v", source, query.Commands[0])
			}
			operatorOffset := strings.Index(source, operator.spelling)
			if operatorOffset < 0 || source[operatorOffset:operatorOffset+1] != operator.spelling {
				t.Fatalf("operator range setup failed for %q", source)
			}
		}
	}

	for _, source := range []string{
		`| eval value=1+`,
		`| eval value=*1`,
		`| eval value=1**2`,
		`| where value/ > 1`,
	} {
		assertAuthoredDiagnosticCode(t, source, "SPL_UNSUPPORTED_ARITHMETIC_SYNTAX")
	}
	assertAuthoredDiagnosticCode(t, `| where 0%.>0`, "SPL_UNSUPPORTED_ARITHMETIC_SYNTAX")

	for _, source := range []string{
		`| eval value=1e-foo`,
		`| eval value=1e+field`,
		`| eval value=1e--3`,
		`| eval value=1e+-3`,
	} {
		query, err := Parse(source)
		if err != nil {
			t.Fatalf("Parse exponent-like arithmetic %q: %v", source, err)
		}
		if _, ok := query.Commands[0].(*EvalCommand).Assignments[0].Expression.(*ScalarBinaryExpr); !ok {
			t.Fatalf("Parse(%q) expression = %#v, want binary arithmetic", source, query.Commands[0])
		}
	}
	for _, source := range []string{
		`| eval value=1e-3`,
		`| eval value=1e+3`,
		`| eval value=-1e-3`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse signed exponent %q: %v", source, err)
		}
	}
}

func TestLexAuthoredMultibyteInvalidQuoteEscapeKeepsExactPositions(t *testing.T) {
	t.Parallel()

	baseSource := `'\ϭ'`
	query, err := Parse(baseSource)
	if err != nil {
		t.Fatalf("Parse base-search invalid scalar escape spelling: %v", err)
	}
	if want := sourcePositionAtOffset(baseSource, len(baseSource)); query.Range.End != want {
		t.Fatalf("query end = %#v, want %#v", query.Range.End, want)
	}

	scalarSource := `| eval value='bad\ϭ'`
	_, err = Parse(scalarSource)
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_INVALID_FIELD_QUOTE_ESCAPE" {
		t.Fatalf("Parse scalar invalid escape = %v", err)
	}
	assertSourceRangeText(t, scalarSource, diagnostic.Range, `\ϭ`)
	if want := sourcePositionAtOffset(scalarSource, diagnostic.Range.End.Offset); diagnostic.Range.End != want {
		t.Fatalf("diagnostic end = %#v, want %#v", diagnostic.Range.End, want)
	}
}

func TestScanScalarWordBoundaryPreservesCompositeLegacyAndUTF8Ranges(t *testing.T) {
	t.Parallel()

	const prefix = "π\n"
	invalidComposite := prefix + "left+'bad" + string([]byte{0xff}) +
		" field'+right tail"
	tests := []struct {
		name         string
		source       string
		quotedFields bool
		wantText     string
		composite    bool
	}{
		{
			name: "escaped quote delimiter and newline",
			source: prefix + `left+'名, field\'s
next'+right tail`,
			quotedFields: true,
			wantText: `left+'名, field\'s
next'+right`,
			composite: true,
		},
		{
			name:         "invalid UTF-8 inside composite",
			source:       invalidComposite,
			quotedFields: true,
			wantText:     "left+'bad" + string([]byte{0xff}) + " field'+right",
			composite:    true,
		},
		{
			name:         "legacy quote does not suppress space",
			source:       prefix + "left+'space field'+right tail",
			quotedFields: false,
			wantText:     "left+'space",
		},
		{
			name:         "ordinary multibyte word",
			source:       prefix + "café+right tail",
			quotedFields: true,
			wantText:     "café+right",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			start := sourcePositionAtOffset(test.source, len(prefix))
			wantEnd := sourcePositionAtOffset(
				test.source,
				len(prefix)+len(test.wantText),
			)
			scan := scanScalarWordBoundary(test.source, start, test.quotedFields)
			if scan.composite != test.composite || scan.end != wantEnd {
				t.Fatalf(
					"boundary = composite %v end %#v, want %v %#v",
					scan.composite,
					scan.end,
					test.composite,
					wantEnd,
				)
			}

			wordLexer := lexer{
				source:       test.source,
				offset:       start.Offset,
				line:         start.Line,
				column:       start.Column,
				quotedFields: test.quotedFields,
			}
			tok, err := wordLexer.scanWord(start)
			if err != nil {
				t.Fatalf("scanWord: %v", err)
			}
			wantKind := tokenWord
			wantRaw := ""
			if test.composite {
				wantKind = tokenScalarComposite
				wantRaw = test.wantText
			}
			if tok.kind != wantKind || tok.text != test.wantText ||
				tok.raw != wantRaw || tok.sourceRange != (Range{Start: start, End: wantEnd}) {
				t.Fatalf("token = %#v, want kind %v text %q raw %q range %#v", tok, wantKind, test.wantText, wantRaw, Range{Start: start, End: wantEnd})
			}
		})
	}
}

func TestParseAuthoredExpressionBudgetsAndClosedKnowledgeProfile(t *testing.T) {
	t.Parallel()

	chain := func(operators int) string { return `| eval value=` + strings.Repeat(`1+`, operators) + `1` }
	if _, err := Parse(chain(MaximumArithmeticOperatorsPerQuery)); err != nil {
		t.Fatalf("maximum arithmetic chain: %v", err)
	}
	assertAuthoredDiagnosticCode(t, chain(MaximumArithmeticOperatorsPerQuery+1), "SPL_QUERY_TOO_COMPLEX")

	unary := func(operators int) string { return `| eval value=` + strings.Repeat(`- `, operators) + `field` }
	if _, err := Parse(unary(MaximumUnaryOperatorChain)); err != nil {
		t.Fatalf("maximum unary chain: %v", err)
	}
	assertAuthoredDiagnosticCode(t, unary(MaximumUnaryOperatorChain+1), "SPL_QUERY_TOO_COMPLEX")

	candidates := make([]string, MaximumMembershipCandidates+1)
	for index := range candidates {
		candidates[index] = "1"
	}
	assertAuthoredDiagnosticCode(t, `| where value IN (`+strings.Join(candidates, `,`)+`)`, "SPL_QUERY_TOO_COMPLEX")
	maximumList := strings.Join(candidates[:MaximumMembershipCandidates], `,`)
	maximumQuery := strings.Repeat(`| where value IN (`+maximumList+`) `, MaximumMembershipCandidatesPerQuery/MaximumMembershipCandidates)
	if _, err := Parse(maximumQuery); err != nil {
		t.Fatalf("maximum aggregate membership candidates: %v", err)
	}
	assertAuthoredDiagnosticCode(t, maximumQuery+`| where value IN (1)`, "SPL_QUERY_TOO_COMPLEX")

	for _, source := range []string{`1+2`, `(field)`, `'field'`} {
		if _, err := ParseScalarExpression(source); err == nil {
			t.Fatalf("ParseScalarExpression(%q) accepted authored-expression syntax", source)
		}
	}
	legacySigned, err := ParseScalarExpression(`-2`)
	if err != nil {
		t.Fatalf("ParseScalarExpression legacy signed literal: %v", err)
	}
	legacyLiteral, ok := legacySigned.(*ScalarLiteralExpr)
	if !ok || legacyLiteral.Value.Kind != LiteralKindInteger || legacyLiteral.Value.Text != "-2" {
		t.Fatalf("legacy signed expression = %#v, want signed Int literal", legacySigned)
	}
}

func TestParseAuthoredPreservesNonScalarTokenization(t *testing.T) {
	t.Parallel()

	query, err := Parse(`'HTTP Status'`)
	if err != nil {
		t.Fatalf("Parse legacy apostrophe terms: %v", err)
	}
	and, ok := query.Search.(*BinaryExpr)
	if !ok || and.Op != BoolOpAnd {
		t.Fatalf("search = %#v, want two legacy adjacent terms", query.Search)
	}
	left, leftOK := and.Left.(*TermExpr)
	right, rightOK := and.Right.(*TermExpr)
	if !leftOK || !rightOK || left.Value != `'HTTP` || right.Value != `Status'` {
		t.Fatalf("legacy terms = %#v AND %#v", and.Left, and.Right)
	}

	query, err = Parse(`| search 'HTTP Status'`)
	if err != nil {
		t.Fatalf("Parse pipeline search legacy apostrophe terms: %v", err)
	}
	pipelineSearch := query.Commands[0].(*SearchCommand).Expression
	and, ok = pipelineSearch.(*BinaryExpr)
	if !ok || and.Op != BoolOpAnd {
		t.Fatalf("pipeline search = %#v, want two legacy adjacent terms", pipelineSearch)
	}
	left, leftOK = and.Left.(*TermExpr)
	right, rightOK = and.Right.(*TermExpr)
	if !leftOK || !rightOK || left.Value != `'HTTP` || right.Value != `Status'` {
		t.Fatalf("pipeline legacy terms = %#v AND %#v", and.Left, and.Right)
	}

	for _, source := range []string{
		`O'Reilly`,
		`'legacy\q'`,
		`'legacy-unterminated`,
		`source=/var/log/app-1.log | sort -duration`,
		`status-code=500`,
		`field=50%`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
	}
	assertAuthoredDiagnosticCode(t, `status IN (500,503)`, "SPL_UNSUPPORTED_EXPRESSION")

	for _, source := range []string{
		`a+'plain'`,
		`a+'space field'/b`,
		`a+'pipe|comma,paren()=operator'`,
		`a+'bad\q'`,
		`| table a+'plain'`,
		`| table a+'pipe|head 1'`,
		`| stats avg(a+'plain')`,
		`| stats count BY a+'plain'`,
		`| streamstats count BY a+'plain'`,
	} {
		legacyTokens, lexErr := lexWithQuotedFields(source, false)
		if lexErr != nil {
			t.Fatalf("legacy lex(%q): %v", source, lexErr)
		}
		legacyParser := parser{source: source, tokens: legacyTokens, profile: expressionProfileAuthored}
		legacyQuery, legacyErr := legacyParser.parseQuery()
		query, err := Parse(source)
		if !reflect.DeepEqual(query, legacyQuery) || !equalDifferentialErrors(err, legacyErr) {
			t.Fatalf(
				"base-search composite differential for %q: got %#v, %v; legacy %#v, %v",
				source, query, err, legacyQuery, legacyErr,
			)
		}
	}
}

func makeRepeatedStrings(value string, count int) []string {
	values := make([]string, count)
	for index := range values {
		values[index] = value
	}
	return values
}

func TestAnalyzeScalarExpressionRejectsForgedAuthoredNodes(t *testing.T) {
	t.Parallel()

	literal := func() ScalarExpr {
		return &ScalarLiteralExpr{Value: Literal{Kind: LiteralKindInteger, Text: "1"}}
	}
	invalid := []ScalarExpr{
		&ScalarLiteralExpr{Value: Literal{Kind: LiteralKindInvalid}},
		&ScalarUnaryExpr{Op: ScalarUnaryOpInvalid, Operand: literal()},
		&ScalarUnaryExpr{Op: ScalarUnaryOpCount, Operand: literal()},
		&ScalarBinaryExpr{Op: ScalarBinaryOpInvalid, Left: literal(), Right: literal()},
		&ScalarBinaryExpr{Op: ScalarBinaryOpCount, Left: literal(), Right: literal()},
		&ScalarUnaryExpr{
			Op:      ScalarUnaryOpNegative,
			Operand: literal(),
			Range: Range{
				Start: Position{Offset: 2, Line: 1, Column: 3},
				End:   Position{Offset: 1, Line: 1, Column: 2},
			},
		},
		&ScalarBinaryExpr{
			Op: ScalarBinaryOpAdd, Left: literal(), Right: literal(),
			Range: Range{
				Start: Position{Offset: 0, Line: 2, Column: 1},
				End:   Position{Offset: 1, Line: 1, Column: 2},
			},
		},
		&ScalarIfExpr{Condition: &WhereMembershipExpr{Value: literal()}, True: literal(), False: literal()},
		&ScalarIfExpr{
			Condition: &WhereMembershipExpr{
				Value:      literal(),
				Candidates: make([]ScalarExpr, MaximumMembershipCandidates+1),
			},
			True:  literal(),
			False: literal(),
		},
		&ScalarIfExpr{
			Condition: &WhereMembershipExpr{Value: literal(), Candidates: []ScalarExpr{nil}},
			True:      literal(),
			False:     literal(),
		},
		&ScalarIfExpr{
			Condition: &WhereMembershipExpr{
				Value: literal(), Candidates: []ScalarExpr{literal()},
				Range: Range{
					Start: Position{Offset: 4, Line: 1, Column: 5},
					End:   Position{Offset: 3, Line: 1, Column: 4},
				},
			},
			True: literal(), False: literal(),
		},
	}
	for _, expression := range invalid {
		if _, err := AnalyzeScalarExpression(expression); err == nil {
			t.Fatalf("AnalyzeScalarExpression(%#v) succeeded", expression)
		}
	}

	unary := literal()
	for range MaximumUnaryOperatorChain + 1 {
		unary = &ScalarUnaryExpr{Op: ScalarUnaryOpNegative, Operand: unary}
	}
	if _, err := AnalyzeScalarExpression(unary); err == nil {
		t.Fatal("33-node unary chain succeeded")
	}

	legal := literal()
	for range MaximumArithmeticOperatorsPerQuery {
		legal = &ScalarBinaryExpr{Op: ScalarBinaryOpAdd, Left: legal, Right: literal()}
	}
	if _, err := AnalyzeScalarExpression(legal); err != nil {
		t.Fatalf("legal 256-operator forged chain: %v", err)
	}
	cycle := &ScalarBinaryExpr{Op: ScalarBinaryOpAdd, Right: literal()}
	cycle.Left = cycle
	if _, err := AnalyzeScalarExpression(cycle); err == nil {
		t.Fatal("cyclic arithmetic AST succeeded")
	}

	var condition WhereExpr
	for range MaximumMembershipCandidatesPerQuery/MaximumMembershipCandidates + 1 {
		membershipCandidates := make([]ScalarExpr, MaximumMembershipCandidates)
		for index := range membershipCandidates {
			membershipCandidates[index] = literal()
		}
		membership := WhereExpr(&WhereMembershipExpr{Value: literal(), Candidates: membershipCandidates})
		if condition == nil {
			condition = membership
		} else {
			condition = &WhereBoolExpr{Op: BoolOpAnd, Left: condition, Right: membership}
		}
	}
	overAggregateMembership := &ScalarIfExpr{Condition: condition, True: literal(), False: literal()}
	if _, err := AnalyzeScalarExpression(overAggregateMembership); err == nil {
		t.Fatal("forged 288-candidate membership tree succeeded")
	}

	condition = nil
	for range maxEvalPredicates + 1 {
		membership := WhereExpr(&WhereMembershipExpr{
			Value: literal(), Candidates: []ScalarExpr{literal()},
		})
		if condition == nil {
			condition = membership
		} else {
			condition = &WhereBoolExpr{Op: BoolOpAnd, Left: condition, Right: membership}
		}
	}
	overPredicateLimit := &ScalarIfExpr{Condition: condition, True: literal(), False: literal()}
	if _, err := AnalyzeScalarExpression(overPredicateLimit); err == nil {
		t.Fatal("forged 33-predicate tree succeeded")
	}
}

func assertAuthoredDiagnosticCode(t *testing.T, source, code string) {
	t.Helper()
	_, err := Parse(source)
	if err == nil {
		t.Fatalf("Parse(%q) succeeded, want %s", source, code)
	}
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != code {
		t.Fatalf("Parse(%q) error = %v, want %s", source, err, code)
	}
}
