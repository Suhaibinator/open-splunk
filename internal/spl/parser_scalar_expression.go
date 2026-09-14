package spl

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
)

func (p *parser) parseScalarExpression() (ScalarExpr, error) {
	if p.scalarDepth >= maxScalarNestingDepth {
		return nil, &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("scalar expression nesting exceeds %d levels", maxScalarNestingDepth),
			Range:   p.current().sourceRange,
		}
	}
	p.scalarDepth++
	defer func() { p.scalarDepth-- }()
	if p.profile == expressionProfileKnowledge {
		return p.parseScalarConcatenation(p.parseKnowledgeScalarPrimary)
	}
	if p.profile != expressionProfileAuthored {
		return nil, p.errorAtCurrent("SPL_UNSUPPORTED_EVAL_EXPRESSION", "unsupported scalar expression")
	}
	return p.parseScalarConcatenation(p.parseScalarAdditive)
}

func (p *parser) parseScalarConcatenation(parseOperand func() (ScalarExpr, error)) (ScalarExpr, error) {
	first, err := parseOperand()
	if err != nil {
		return nil, err
	}
	if !p.match(tokenConcat) {
		return first, nil
	}

	arguments := make([]ScalarExpr, 0, 4)
	arguments = append(arguments, first)
	for {
		argument, argumentErr := parseOperand()
		if argumentErr != nil {
			return nil, argumentErr
		}
		arguments = append(arguments, argument)
		if len(arguments) > MaximumConcatenationOperands {
			return nil, &Diagnostic{
				Code: "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf(
					"concatenation contains more than %d operands",
					MaximumConcatenationOperands,
				),
				Range: Range{
					Start: first.SourceRange().Start,
					End:   argument.SourceRange().End,
				},
			}
		}
		if !p.match(tokenConcat) {
			break
		}
	}
	for _, argument := range arguments {
		if scalarExpressionMayReturnBooleanValue(argument) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "concatenation cannot consume a Boolean result",
				Range:   argument.SourceRange(),
				Suggestions: []string{
					"convert the Boolean explicitly with tostring",
				},
			}
		}
	}
	if p.concatenationOperands >
		MaximumConcatenationOperandsPerQuery-len(arguments) {
		return nil, &Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"concatenation contains more than %d operand occurrences per query",
				MaximumConcatenationOperandsPerQuery,
			),
			Range: Range{
				Start: first.SourceRange().Start,
				End:   arguments[len(arguments)-1].SourceRange().End,
			},
		}
	}
	p.concatenationOperands += len(arguments)
	return &ScalarCallExpr{
		Function:  ScalarFunctionConcat,
		Arguments: arguments,
		Range: Range{
			Start: first.SourceRange().Start,
			End:   arguments[len(arguments)-1].SourceRange().End,
		},
	}, nil
}

func (p *parser) parseScalarAdditive() (ScalarExpr, error) {
	left, err := p.parseScalarMultiplicative()
	if err != nil {
		return nil, err
	}
	for {
		if err := p.prepareScalarToken(); err != nil {
			return nil, err
		}
		var op ScalarBinaryOp
		switch p.current().kind {
		case tokenPlus:
			op = ScalarBinaryOpAdd
		case tokenMinus:
			op = ScalarBinaryOpSubtract
		default:
			return left, nil
		}
		operatorRange := p.current().sourceRange
		p.advance()
		right, parseErr := p.parseScalarMultiplicative()
		if parseErr != nil {
			return nil, arithmeticOperandError(parseErr, operatorRange)
		}
		if countErr := p.countArithmeticOperator(operatorRange); countErr != nil {
			return nil, countErr
		}
		left = &ScalarBinaryExpr{
			Op:    op,
			Left:  left,
			Right: right,
			Range: Range{Start: left.SourceRange().Start, End: right.SourceRange().End},
		}
	}
}

func (p *parser) parseScalarMultiplicative() (ScalarExpr, error) {
	left, err := p.parseScalarUnary()
	if err != nil {
		return nil, err
	}
	for {
		if err := p.prepareScalarToken(); err != nil {
			return nil, err
		}
		var op ScalarBinaryOp
		switch p.current().kind {
		case tokenMultiply:
			op = ScalarBinaryOpMultiply
		case tokenDivide:
			op = ScalarBinaryOpDivide
		case tokenRemainder:
			op = ScalarBinaryOpRemainder
		default:
			return left, nil
		}
		operatorRange := p.current().sourceRange
		p.advance()
		right, parseErr := p.parseScalarUnary()
		if parseErr != nil {
			return nil, arithmeticOperandError(parseErr, operatorRange)
		}
		if countErr := p.countArithmeticOperator(operatorRange); countErr != nil {
			return nil, countErr
		}
		left = &ScalarBinaryExpr{
			Op:    op,
			Left:  left,
			Right: right,
			Range: Range{Start: left.SourceRange().Start, End: right.SourceRange().End},
		}
	}
}

func (p *parser) parseScalarUnary() (ScalarExpr, error) {
	if err := p.prepareScalarToken(); err != nil {
		return nil, err
	}
	var op ScalarUnaryOp
	switch p.current().kind {
	case tokenPlus:
		op = ScalarUnaryOpPositive
	case tokenMinus:
		op = ScalarUnaryOpNegative
	default:
		return p.parseAuthoredScalarPrimary()
	}
	operator := p.current()
	if p.unaryDepth >= MaximumUnaryOperatorChain {
		return nil, &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("unary operator chain exceeds %d operators", MaximumUnaryOperatorChain),
			Range:   operator.sourceRange,
		}
	}
	p.unaryDepth++
	defer func() { p.unaryDepth-- }()
	p.advance()
	operand, err := p.parseScalarUnary()
	if err != nil {
		return nil, arithmeticOperandError(err, operator.sourceRange)
	}
	if countErr := p.countArithmeticOperator(operator.sourceRange); countErr != nil {
		return nil, countErr
	}
	return &ScalarUnaryExpr{
		Op:      op,
		Operand: operand,
		Range:   Range{Start: operator.sourceRange.Start, End: operand.SourceRange().End},
	}, nil
}

func (p *parser) countArithmeticOperator(sourceRange Range) error {
	if p.arithmeticOperators >= MaximumArithmeticOperatorsPerQuery {
		return &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("search contains more than %d arithmetic operators", MaximumArithmeticOperatorsPerQuery),
			Range:   sourceRange,
		}
	}
	p.arithmeticOperators++
	return nil
}

func arithmeticOperandError(err error, operatorRange Range) error {
	var diagnostic *Diagnostic
	if errors.As(err, &diagnostic) && diagnostic.Code == "SPL_EXPECTED_SCALAR_EXPRESSION" {
		return &Diagnostic{
			Code:        "SPL_UNSUPPORTED_ARITHMETIC_SYNTAX",
			Message:     "arithmetic operator must be followed by a scalar operand",
			Range:       operatorRange,
			Suggestions: []string{"provide a numeric scalar operand after the operator"},
		}
	}
	return err
}

func (p *parser) parseKnowledgeScalarPrimary() (ScalarExpr, error) {
	tok := p.current()
	if tok.kind == tokenString {
		p.advance()
		literal := Literal{Kind: LiteralKindString, Text: tok.text, Quoted: true, Range: tok.sourceRange}
		return &ScalarLiteralExpr{Value: literal, Range: tok.sourceRange}, nil
	}
	if tok.kind != tokenWord || p.isKeyword("AND") || p.isKeyword("OR") || p.isKeyword("NOT") {
		return nil, p.errorAtCurrent("SPL_EXPECTED_SCALAR_EXPRESSION", "expected a field, literal, or supported function call")
	}
	p.advance()
	if p.match(tokenLeftParen) {
		if strings.EqualFold(tok.text, "if") {
			return p.parseScalarIf(tok)
		}
		if strings.EqualFold(tok.text, "case") {
			return p.parseScalarCase(tok)
		}
		return p.parseScalarCall(tok)
	}
	kind := classifyLiteral(tok.text, false)
	if kind != LiteralKindString {
		literal := Literal{Kind: kind, Text: tok.text, Range: tok.sourceRange}
		return &ScalarLiteralExpr{Value: literal, Range: tok.sourceRange}, nil
	}
	if unsupportedScalarIdentifier(tok.text) {
		return nil, &Diagnostic{
			Code:        "SPL_UNSUPPORTED_EVAL_EXPRESSION",
			Message:     fmt.Sprintf("unsupported unquoted scalar expression %q", tok.text),
			Range:       tok.sourceRange,
			Suggestions: []string{"use a supported field, literal, or function call"},
		}
	}
	return &ScalarFieldExpr{Field: tok.text, Range: tok.sourceRange}, nil
}

func (p *parser) parseAuthoredScalarPrimary() (ScalarExpr, error) {
	if err := p.prepareScalarToken(); err != nil {
		return nil, err
	}
	tok := p.current()
	if tok.scalarDiagnostic != nil {
		return nil, tok.scalarDiagnostic
	}
	if tok.kind == tokenWord && strings.HasPrefix(tok.text, "'") {
		return nil, unterminatedQuotedScalarField(tok)
	}
	if tok.kind == tokenMultiply || tok.kind == tokenDivide || tok.kind == tokenRemainder {
		return nil, &Diagnostic{
			Code:        "SPL_UNSUPPORTED_ARITHMETIC_SYNTAX",
			Message:     "binary arithmetic operator is missing its left operand",
			Range:       tok.sourceRange,
			Suggestions: []string{"provide a numeric scalar operand before the operator"},
		}
	}
	if tok.kind == tokenLeftParen {
		p.advance()
		if p.current().kind == tokenRightParen {
			return nil, p.errorAtCurrent("SPL_EXPECTED_SCALAR_EXPRESSION", "empty parenthesized scalar expression")
		}
		expression, err := p.parseScalarExpression()
		if err != nil {
			return nil, err
		}
		if !p.match(tokenRightParen) {
			if _, comparison := evalComparisonOperator(p.current().kind, p.profile); comparison ||
				p.isKeyword("IN") || p.isKeyword("NOT") || p.isKeyword("AND") || p.isKeyword("OR") {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: "a Boolean expression cannot be used as a grouped scalar value",
					Range:   Range{Start: tok.sourceRange.Start, End: p.current().sourceRange.End},
				}
			}
			return nil, p.errorAtCurrent("SPL_EXPECTED_RIGHT_PAREN", "expected ')' to close scalar expression")
		}
		setScalarExpressionRange(expression, Range{Start: tok.sourceRange.Start, End: p.previous().sourceRange.End})
		return expression, nil
	}
	if tok.kind == tokenString {
		p.advance()
		literal := Literal{Kind: LiteralKindString, Text: tok.text, Quoted: true, Range: tok.sourceRange}
		return &ScalarLiteralExpr{Value: literal, Range: tok.sourceRange}, nil
	}
	if tok.kind == tokenQuotedField {
		if err := validateQuotedFieldReference(tok); err != nil {
			return nil, err
		}
		p.advance()
		return &ScalarFieldExpr{Field: tok.text, Quoted: true, Range: tok.sourceRange}, nil
	}
	if tok.kind != tokenWord || p.isKeyword("AND") || p.isKeyword("OR") || p.isKeyword("NOT") {
		return nil, p.errorAtCurrent("SPL_EXPECTED_SCALAR_EXPRESSION", "expected a field, literal, supported function call, or grouped scalar expression")
	}
	p.advance()
	if p.match(tokenLeftParen) {
		if strings.EqualFold(tok.text, "in") {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "membership is a Boolean predicate and cannot be used as a scalar value",
				Range:   tok.sourceRange,
				Suggestions: []string{
					"use in(value, candidate) inside where, if, case, or count(eval(...))",
				},
			}
		}
		if strings.EqualFold(tok.text, "if") {
			return p.parseScalarIf(tok)
		}
		if strings.EqualFold(tok.text, "case") {
			return p.parseScalarCase(tok)
		}
		return p.parseScalarCall(tok)
	}
	kind := classifyLiteral(tok.text, false)
	if kind != LiteralKindString {
		literal := Literal{Kind: kind, Text: tok.text, Range: tok.sourceRange}
		return &ScalarLiteralExpr{Value: literal, Range: tok.sourceRange}, nil
	}
	if unsupportedScalarIdentifier(tok.text) {
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
			Message: fmt.Sprintf("unquoted scalar field %q contains reserved expression punctuation", tok.text),
			Range:   tok.sourceRange,
			Suggestions: []string{
				"single-quote the exact scalar field name",
			},
		}
	}
	return &ScalarFieldExpr{Field: tok.text, Range: tok.sourceRange}, nil
}

func validateQuotedScalarField(tok token) error {
	field := tok.text
	if field == "" {
		return &Diagnostic{
			Code:    "SPL_EXPECTED_FIELD",
			Message: "single-quoted scalar field cannot be empty",
			Range:   tok.sourceRange,
		}
	}
	if !utf8.ValidString(field) || strings.ContainsAny(field, "*?") ||
		strings.TrimFunc(field, unicode.IsSpace) != field {
		return &Diagnostic{
			Code:    "SPL_INVALID_FIELD",
			Message: "single-quoted scalar field is empty or contains unsupported whitespace or wildcard syntax",
			Range:   tok.sourceRange,
		}
	}
	for _, value := range field {
		if unicode.IsControl(value) {
			return &Diagnostic{
				Code:    "SPL_INVALID_FIELD",
				Message: "single-quoted scalar field contains a control character",
				Range:   tok.sourceRange,
			}
		}
	}
	segments, err := eventfields.ParseNormalizedSearchFieldPath(field)
	if err != nil || len(segments) == 0 {
		return &Diagnostic{
			Code:    "SPL_INVALID_FIELD",
			Message: "single-quoted scalar field is not a canonical bounded field path",
			Range:   tok.sourceRange,
		}
	}
	if !eventfields.IsCanonicalSPLField(field) && eventfields.IsReservedDynamicRoot(segments[0]) {
		return &Diagnostic{
			Code:    "SPL_INVALID_FIELD",
			Message: "single-quoted scalar field uses a reserved or compiler-private root",
			Range:   tok.sourceRange,
		}
	}
	return nil
}

func validateQuotedFieldReference(tok token) error {
	if IsExactQuotedFieldName(tok.text) ||
		IsStatsLiteralFieldReference(tok.text) {
		return nil
	}
	return validateQuotedScalarField(tok)
}

func unterminatedQuotedScalarField(tok token) *Diagnostic {
	end := advanceSourcePosition(tok.sourceRange.Start, "'")
	return &Diagnostic{
		Code:    "SPL_UNTERMINATED_FIELD_QUOTE",
		Message: "unterminated single-quoted field reference",
		Range:   Range{Start: tok.sourceRange.Start, End: end},
	}
}

func (p *parser) parseScalarIf(name token) (ScalarExpr, error) {
	invalidArity := func(sourceRange Range) error {
		return invalidEvalArity(sourceRange, "if requires exactly three arguments")
	}
	if p.current().kind == tokenRightParen || p.current().kind == tokenComma {
		return nil, invalidArity(name.sourceRange)
	}
	condition, err := p.parseWhereExpression()
	if err != nil {
		return nil, err
	}
	if !p.match(tokenComma) {
		if p.current().kind == tokenRightParen || p.atCommandEnd() {
			return nil, invalidArity(name.sourceRange)
		}
		if p.current().kind == tokenWord {
			return nil, &Diagnostic{
				Code: "SPL_UNSUPPORTED_WHERE_EXPRESSION",
				Message: fmt.Sprintf(
					"unsupported if predicate syntax at %q; explicit AND or OR is required between predicates",
					p.current().text,
				),
				Range:       p.current().sourceRange,
				Suggestions: []string{`if(first=value AND second=value, "yes", "no")`},
			}
		}
		return nil, p.errorAtCurrent("SPL_EXPECTED_COMMA", "expected ',' after if predicate")
	}
	if p.current().kind == tokenRightParen || p.current().kind == tokenComma {
		return nil, invalidArity(name.sourceRange)
	}
	trueValue, err := p.parseScalarExpression()
	if err != nil {
		return nil, err
	}
	if !p.match(tokenComma) {
		return nil, invalidArity(name.sourceRange)
	}
	if p.current().kind == tokenRightParen || p.current().kind == tokenComma {
		return nil, invalidArity(name.sourceRange)
	}
	falseValue, err := p.parseScalarExpression()
	if err != nil {
		return nil, err
	}
	if p.current().kind == tokenComma {
		return nil, invalidArity(name.sourceRange)
	}
	if !p.match(tokenRightParen) {
		return nil, p.errorAtCurrent("SPL_EXPECTED_RIGHT_PAREN", "expected ')' to close if function")
	}
	return &ScalarIfExpr{
		Condition: condition,
		True:      trueValue,
		False:     falseValue,
		Range:     Range{Start: name.sourceRange.Start, End: p.previous().sourceRange.End},
	}, nil
}

func (p *parser) parseScalarCase(name token) (ScalarExpr, error) {
	invalidArity := func(sourceRange Range) error {
		return invalidEvalArity(sourceRange, "case requires one or more condition/value pairs")
	}
	if p.current().kind == tokenRightParen || p.current().kind == tokenComma {
		return nil, invalidArity(name.sourceRange)
	}

	branches := make([]ScalarCaseBranch, 0, 2)
	for {
		condition, err := p.parseWhereExpression()
		if err != nil {
			return nil, err
		}
		if !p.match(tokenComma) {
			if p.current().kind == tokenRightParen || p.atCommandEnd() {
				return nil, invalidArity(name.sourceRange)
			}
			if p.current().kind == tokenWord {
				return nil, &Diagnostic{
					Code: "SPL_UNSUPPORTED_WHERE_EXPRESSION",
					Message: fmt.Sprintf(
						"unsupported case predicate syntax at %q; explicit AND or OR is required between predicates",
						p.current().text,
					),
					Range:       p.current().sourceRange,
					Suggestions: []string{`case(first=value AND second=value, "match")`},
				}
			}
			return nil, p.errorAtCurrent(
				"SPL_EXPECTED_COMMA",
				"expected ',' after case predicate",
			)
		}
		if p.current().kind == tokenRightParen || p.current().kind == tokenComma {
			return nil, invalidArity(name.sourceRange)
		}
		value, err := p.parseScalarExpression()
		if err != nil {
			return nil, err
		}
		branches = append(branches, ScalarCaseBranch{
			Condition: condition,
			Value:     value,
			Range: Range{
				Start: condition.SourceRange().Start,
				End:   value.SourceRange().End,
			},
		})
		if len(branches) > MaximumCaseBranches {
			return nil, &Diagnostic{
				Code: "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf(
					"case contains more than %d condition/value pairs",
					MaximumCaseBranches,
				),
				Range: name.sourceRange,
			}
		}
		if !p.match(tokenComma) {
			break
		}
		if p.current().kind == tokenRightParen || p.current().kind == tokenComma {
			return nil, invalidArity(name.sourceRange)
		}
	}
	if !p.match(tokenRightParen) {
		return nil, p.errorAtCurrent(
			"SPL_EXPECTED_RIGHT_PAREN",
			"expected ')' to close case function",
		)
	}
	return &ScalarCaseExpr{
		Branches: branches,
		Range:    Range{Start: name.sourceRange.Start, End: p.previous().sourceRange.End},
	}, nil
}
