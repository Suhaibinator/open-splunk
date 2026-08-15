package spl

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"
)

func FuzzParseV02ExpressionsAreDeterministicAndAtomic(f *testing.F) {
	seeds := []string{
		`| eval value=(left+right)*2-3/4%5`,
		`| eval value="value=" . 1+2`,
		`| eval value=- - 1e-3`,
		`| eval 'error-rate'='request-bytes'/1024`,
		`| eval value='owner\'s field'`,
		`| eval value='path\\\\leaf'`,
		`| eval value=left+'request-bytes'`,
		`| eval value='request-bytes'-right`,
		`| eval value=left*'request-bytes'`,
		`| eval value='request-bytes'/right`,
		`| eval value=left%'request-bytes'`,
		"| eval value=left+'名, field\\'s\nnext'+right",
		`| eval value='bad\q'`,
		`'\ϭ'`,
		`| eval value='unterminated`,
		`| where ((bytes+overhead)/1024)>10 AND (status=500 OR status==503)`,
		`| where in(status,400,401+2)`,
		`| where status NOT IN (200,201,204)`,
		`| where (status IN (200))=true`,
		`| where NOT-value>0`,
		`| where 0%.>0`,
		"| where ('HTTP Status'\r\n+1)>500",
		`status IN (500,503)`,
		"\x00\xff",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, source string) {
		v02FuzzCheckParse(t, source, true)
	})
}

func FuzzParseV02ExpressionFragmentsCanonicalize(f *testing.F) {
	for _, seed := range []struct {
		expression string
		predicate  bool
	}{
		{`1+2*3`, false},
		{`(1+2)*3`, false},
		{`- - 2`, false},
		{`"value=" . 1+2`, false},
		{`'request-bytes'/1024`, false},
		{`if(status IN (500,503),used/capacity,0.0)`, false},
		{`(left+right)*2>=10`, true},
		{`(status=500 OR status==503) AND attempts>1`, true},
		{`in(status,400,401,403)`, true},
		{`status NOT IN (200,201,204)`, true},
		{`('HTTP Status'+1)>500`, true},
		{`'bad\q'=1`, true},
	} {
		f.Add(seed.expression, seed.predicate)
	}
	f.Fuzz(func(t *testing.T, expression string, predicate bool) {
		prefix := `| eval value=`
		if predicate {
			prefix = `| where `
		}
		v02FuzzCheckParse(t, prefix+expression, true)
	})
}

func v02FuzzCheckParse(t *testing.T, source string, canonicalize bool) {
	t.Helper()

	first, firstErr := Parse(source)
	second, secondErr := Parse(source)
	if (firstErr == nil) != (secondErr == nil) {
		t.Fatalf("parse outcome changed across identical input: first=%v second=%v", firstErr, secondErr)
	}
	if firstErr != nil {
		if first != nil || second != nil {
			t.Fatalf("parse error published a partial query: first=%#v second=%#v", first, second)
		}
		firstDiagnostic := v02FuzzDiagnostic(t, firstErr)
		secondDiagnostic := v02FuzzDiagnostic(t, secondErr)
		if !reflect.DeepEqual(firstDiagnostic, secondDiagnostic) {
			t.Fatalf("diagnostic changed across identical input: first=%#v second=%#v", firstDiagnostic, secondDiagnostic)
		}
		v02FuzzValidateRange(t, source, firstDiagnostic.Range, "diagnostic")
		return
	}
	if first == nil || second == nil {
		t.Fatal("successful parse returned a nil query")
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("AST changed across identical reparses")
	}
	v02FuzzValidateNodeRanges(t, source, first)
	if !canonicalize {
		return
	}

	canonical, ok := v02FuzzCanonicalQuery(first)
	if !ok || len(canonical) > maxSPLSourceBytes {
		return
	}
	// The test renderer deliberately parenthesizes binary/Boolean shapes for
	// unambiguous idempotence. Do not demand a reparse when those renderer-
	// introduced groups alone would exceed the independent authored grouping
	// limit; the original AST has already passed deterministic/range checks.
	if strings.Count(canonical, "(") > maxScalarNestingDepth {
		return
	}
	canonicalTokens, lexErr := lex(canonical)
	if lexErr != nil || len(canonicalTokens)-1 > maxSPLTokens {
		return
	}
	reparsed, err := Parse(canonical)
	if err != nil {
		t.Fatalf("canonical source %q failed to reparse: %v", canonical, err)
	}
	v02FuzzValidateNodeRanges(t, canonical, reparsed)
	secondCanonical, ok := v02FuzzCanonicalQuery(reparsed)
	if !ok || secondCanonical != canonical {
		t.Fatalf("canonical form is not idempotent: first=%q second=%q", canonical, secondCanonical)
	}
}

func v02FuzzDiagnostic(t *testing.T, err error) *Diagnostic {
	t.Helper()
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic == nil {
		t.Fatalf("Parse returned non-diagnostic error %T: %v", err, err)
	}
	return diagnostic
}

func v02FuzzValidateNodeRanges(t *testing.T, source string, root Node) {
	t.Helper()
	nodeType := reflect.TypeFor[Node]()
	rangeType := reflect.TypeFor[Range]()
	seen := make(map[uintptr]struct{})
	var walk func(reflect.Value)
	walk = func(value reflect.Value) {
		if !value.IsValid() {
			return
		}
		if value.Kind() == reflect.Interface {
			if value.IsNil() {
				return
			}
			walk(value.Elem())
			return
		}
		if value.Kind() == reflect.Pointer {
			if value.IsNil() {
				return
			}
			pointer := value.Pointer()
			if _, exists := seen[pointer]; exists {
				return
			}
			seen[pointer] = struct{}{}
			if value.CanInterface() && value.Type().Implements(nodeType) {
				node := value.Interface().(Node)
				v02FuzzValidateRange(t, source, node.SourceRange(), value.Type().String())
			}
			walk(value.Elem())
			return
		}
		if value.Type() == rangeType && value.CanInterface() {
			sourceRange := value.Interface().(Range)
			if sourceRange != (Range{}) {
				v02FuzzValidateRange(t, source, sourceRange, "AST field")
			}
			return
		}
		switch value.Kind() {
		case reflect.Struct:
			for _, field := range value.Fields() {
				walk(field)
			}
		case reflect.Slice, reflect.Array:
			for index := 0; index < value.Len(); index++ {
				walk(value.Index(index))
			}
		}
	}
	walk(reflect.ValueOf(root))
}

func v02FuzzValidateRange(t *testing.T, source string, sourceRange Range, label string) {
	t.Helper()
	if sourceRange.Start.Offset < 0 ||
		sourceRange.End.Offset < sourceRange.Start.Offset ||
		sourceRange.End.Offset > len(source) {
		t.Fatalf("%s range %#v is outside a %d-byte source", label, sourceRange, len(source))
	}
	wantStart := sourcePositionAtOffset(source, sourceRange.Start.Offset)
	wantEnd := sourcePositionAtOffset(source, sourceRange.End.Offset)
	if sourceRange.Start != wantStart || sourceRange.End != wantEnd {
		t.Fatalf("%s range positions are inconsistent: got=%#v want=%#v..%#v", label, sourceRange, wantStart, wantEnd)
	}
}

func v02FuzzCanonicalQuery(query *Query) (string, bool) {
	if query == nil || query.Search != nil || len(query.Commands) != 1 {
		return "", false
	}
	switch command := query.Commands[0].(type) {
	case *EvalCommand:
		if command == nil || len(command.Assignments) != 1 {
			return "", false
		}
		destination, ok := v02FuzzCanonicalField(command.Assignments[0].Field)
		if !ok {
			return "", false
		}
		expression, ok := v02FuzzCanonicalScalar(command.Assignments[0].Expression)
		if !ok {
			return "", false
		}
		return `| eval ` + destination + `=` + expression, true
	case *WhereCommand:
		if command == nil {
			return "", false
		}
		expression, ok := v02FuzzCanonicalWhere(command.Expression)
		if !ok {
			return "", false
		}
		return `| where ` + expression, true
	default:
		return "", false
	}
}

func v02FuzzCanonicalScalar(expression ScalarExpr) (string, bool) {
	switch expression := expression.(type) {
	case *ScalarFieldExpr:
		if expression == nil {
			return "", false
		}
		return v02FuzzCanonicalField(expression.Field)
	case *ScalarLiteralExpr:
		if expression == nil {
			return "", false
		}
		if expression.Value.Kind == LiteralKindString {
			return v02FuzzCanonicalString(expression.Value.Text)
		}
		if expression.Value.Kind <= LiteralKindInvalid || expression.Value.Kind > LiteralKindNull {
			return "", false
		}
		return expression.Value.Text, true
	case *ScalarUnaryExpr:
		if expression == nil {
			return "", false
		}
		operand, ok := v02FuzzCanonicalScalar(expression.Operand)
		if !ok {
			return "", false
		}
		operator := ""
		switch expression.Op {
		case ScalarUnaryOpPositive:
			operator = "+"
		case ScalarUnaryOpNegative:
			operator = "-"
		default:
			return "", false
		}
		// Unary is right-associative, and every lower-precedence operand renderer
		// already carries its own parentheses. Avoid manufacturing one scalar
		// grouping level per legal unary operator in the canonical source.
		return operator + operand, true
	case *ScalarBinaryExpr:
		if expression == nil {
			return "", false
		}
		left, leftOK := v02FuzzCanonicalScalar(expression.Left)
		right, rightOK := v02FuzzCanonicalScalar(expression.Right)
		if !leftOK || !rightOK {
			return "", false
		}
		operator := ""
		switch expression.Op {
		case ScalarBinaryOpMultiply:
			operator = "*"
		case ScalarBinaryOpDivide:
			operator = "/"
		case ScalarBinaryOpRemainder:
			operator = "%"
		case ScalarBinaryOpAdd:
			operator = "+"
		case ScalarBinaryOpSubtract:
			operator = "-"
		default:
			return "", false
		}
		return `(` + left + ` ` + operator + ` ` + right + `)`, true
	case *ScalarCallExpr:
		if expression == nil {
			return "", false
		}
		arguments := make([]string, len(expression.Arguments))
		for index, argument := range expression.Arguments {
			canonical, ok := v02FuzzCanonicalScalar(argument)
			if !ok {
				return "", false
			}
			arguments[index] = canonical
		}
		if expression.Function == ScalarFunctionConcat {
			return `(` + strings.Join(arguments, ` . `) + `)`, len(arguments) >= 2
		}
		name, ok := v02FuzzCanonicalFunctionName(expression.Function)
		if !ok {
			return "", false
		}
		return name + `(` + strings.Join(arguments, `,`) + `)`, true
	case *ScalarIfExpr:
		if expression == nil {
			return "", false
		}
		condition, conditionOK := v02FuzzCanonicalWhere(expression.Condition)
		trueValue, trueOK := v02FuzzCanonicalScalar(expression.True)
		falseValue, falseOK := v02FuzzCanonicalScalar(expression.False)
		if !conditionOK || !trueOK || !falseOK {
			return "", false
		}
		return `if(` + condition + `,` + trueValue + `,` + falseValue + `)`, true
	case *ScalarCaseExpr:
		if expression == nil || len(expression.Branches) == 0 {
			return "", false
		}
		arguments := make([]string, 0, len(expression.Branches)*2)
		for _, branch := range expression.Branches {
			condition, conditionOK := v02FuzzCanonicalWhere(branch.Condition)
			value, valueOK := v02FuzzCanonicalScalar(branch.Value)
			if !conditionOK || !valueOK {
				return "", false
			}
			arguments = append(arguments, condition, value)
		}
		return `case(` + strings.Join(arguments, `,`) + `)`, true
	default:
		return "", false
	}
}

func v02FuzzCanonicalWhere(expression WhereExpr) (string, bool) {
	switch expression := expression.(type) {
	case *WhereBoolExpr:
		if expression == nil {
			return "", false
		}
		left, leftOK := v02FuzzCanonicalWhere(expression.Left)
		right, rightOK := v02FuzzCanonicalWhere(expression.Right)
		if !leftOK || !rightOK || expression.Op != BoolOpAnd && expression.Op != BoolOpOr {
			return "", false
		}
		return `(` + left + ` ` + expression.Op.String() + ` ` + right + `)`, true
	case *WhereNotExpr:
		if expression == nil {
			return "", false
		}
		operand, ok := v02FuzzCanonicalWhere(expression.Operand)
		if !ok {
			return "", false
		}
		return `(NOT ` + operand + `)`, true
	case *WhereComparisonExpr:
		if expression == nil || expression.Op <= CompareOpInvalid || expression.Op > CompareOpGreaterEqual {
			return "", false
		}
		left, leftOK := v02FuzzCanonicalScalar(expression.Left)
		right, rightOK := v02FuzzCanonicalScalar(expression.Right)
		if !leftOK || !rightOK {
			return "", false
		}
		return `(` + left + expression.Op.String() + right + `)`, true
	case *WhereMembershipExpr:
		if expression == nil || len(expression.Candidates) == 0 {
			return "", false
		}
		value, ok := v02FuzzCanonicalScalar(expression.Value)
		if !ok {
			return "", false
		}
		candidates := make([]string, len(expression.Candidates))
		for index, candidate := range expression.Candidates {
			canonical, candidateOK := v02FuzzCanonicalScalar(candidate)
			if !candidateOK {
				return "", false
			}
			candidates[index] = canonical
		}
		operator := ` IN `
		if expression.Negated {
			operator = ` NOT IN `
		}
		return `(` + value + operator + `(` + strings.Join(candidates, `,`) + `))`, true
	case *WhereScalarPredicateExpr:
		if expression == nil {
			return "", false
		}
		return v02FuzzCanonicalScalar(expression.Value)
	default:
		return "", false
	}
}

func v02FuzzCanonicalField(field string) (string, bool) {
	if field == "" || !utf8.ValidString(field) {
		return "", false
	}
	if classifyLiteral(field, false) == LiteralKindString &&
		IsExactUnquotedFieldName(field) && !unsupportedScalarIdentifier(field) &&
		!strings.EqualFold(field, "AND") && !strings.EqualFold(field, "OR") &&
		!strings.EqualFold(field, "NOT") {
		return field, true
	}
	if validateQuotedScalarField(token{text: field}) != nil {
		return "", false
	}
	escaped := strings.ReplaceAll(field, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `'`, `\'`)
	return `'` + escaped + `'`, true
}

func v02FuzzCanonicalString(value string) (string, bool) {
	if !utf8.ValidString(value) {
		return "", false
	}
	var canonical strings.Builder
	canonical.Grow(len(value) + 2)
	canonical.WriteByte('"')
	for _, character := range value {
		switch character {
		case '\\':
			canonical.WriteString(`\\`)
		case '"':
			canonical.WriteString(`\"`)
		case '\n':
			canonical.WriteString(`\n`)
		case '\r':
			canonical.WriteString(`\r`)
		case '\t':
			canonical.WriteString(`\t`)
		default:
			canonical.WriteRune(character)
		}
	}
	canonical.WriteByte('"')
	return canonical.String(), true
}

func v02FuzzCanonicalFunctionName(function ScalarFunction) (string, bool) {
	switch function {
	case ScalarFunctionToNumber:
		return "tonumber", true
	case ScalarFunctionReplace:
		return "replace", true
	case ScalarFunctionIsNull:
		return "isnull", true
	case ScalarFunctionIsNotNull:
		return "isnotnull", true
	case ScalarFunctionCoalesce:
		return "coalesce", true
	case ScalarFunctionLower:
		return "lower", true
	case ScalarFunctionUpper:
		return "upper", true
	case ScalarFunctionLength:
		return "len", true
	case ScalarFunctionSubstring:
		return "substr", true
	case ScalarFunctionToString:
		return "tostring", true
	case ScalarFunctionRound:
		return "round", true
	case ScalarFunctionCeil:
		return "ceil", true
	case ScalarFunctionFloor:
		return "floor", true
	case ScalarFunctionMVCount:
		return "mvcount", true
	case ScalarFunctionMVSort:
		return "mvsort", true
	case ScalarFunctionMatch:
		return "match", true
	case ScalarFunctionLike:
		return "like", true
	case ScalarFunctionNow:
		return "now", true
	case ScalarFunctionStrftime:
		return "strftime", true
	case ScalarFunctionStrptime:
		return "strptime", true
	case ScalarFunctionRelativeTime:
		return "relative_time", true
	default:
		return "", false
	}
}

var benchmarkParseV02Query *Query

func BenchmarkParseV02ArithmeticOperators(b *testing.B) {
	for _, operators := range []int{1, 32, 128, MaximumArithmeticOperatorsPerQuery} {
		b.Run(fmt.Sprintf("operators_%03d", operators), func(b *testing.B) {
			source := `| eval value=` + strings.Repeat(`1+`, operators) + `1`
			if _, err := Parse(source); err != nil {
				b.Fatalf("benchmark source: %v", err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(source)))
			b.ResetTimer()
			for range b.N {
				query, err := Parse(source)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkParseV02Query = query
			}
		})
	}
}

func BenchmarkParseV02MembershipCandidates(b *testing.B) {
	for _, candidates := range []int{1, 8, 16, MaximumMembershipCandidates} {
		b.Run(fmt.Sprintf("candidates_%02d", candidates), func(b *testing.B) {
			list := strings.TrimSuffix(strings.Repeat(`1,`, candidates), `,`)
			source := `| where value IN (` + list + `)`
			if _, err := Parse(source); err != nil {
				b.Fatalf("benchmark source: %v", err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(source)))
			b.ResetTimer()
			for range b.N {
				query, err := Parse(source)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkParseV02Query = query
			}
		})
	}
}
