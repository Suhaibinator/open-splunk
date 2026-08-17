package spl

import (
	"fmt"
	"sort"
)

const (
	maximumKnowledgeExpressionAnalysisNodes = maxSPLTokens * 4
	// A legal left-associative arithmetic chain contains up to 256 binary AST
	// nodes even though it consumes no scalar-grouping recursion while parsing.
	maximumKnowledgeExpressionAnalysisDepth = MaximumArithmeticOperatorsPerQuery + maxScalarNestingDepth*2
)

// ScalarExpressionAnalysis is the bounded semantic inventory of one parsed
// standalone scalar expression. InputFields are unique and binary sorted;
// Nodes counts every scalar and Boolean AST occurrence, including repeated
// references. Predicates counts comparison and direct-predicate leaves using
// the same definition as parser eval/where admission. The inventory contains
// no caller-owned mutable AST state.
type ScalarExpressionAnalysis struct {
	InputFields []string
	Nodes       uint32
	Predicates  uint32
}

// ParseScalarExpression parses one complete authored eval-language scalar
// expression without wrapping it in a synthetic search. Knowledge-object
// publication uses this entry point so the expression's 16 KiB and token
// budgets are identical to authored SPL rather than being reduced by wrapper
// text.
func ParseScalarExpression(source string) (ScalarExpr, error) {
	if len(source) > maxSPLSourceBytes {
		start := sourcePositionAtOffset(source, maxSPLSourceBytes)
		end := sourcePositionAtOffset(source, maxSPLSourceBytes+1)
		return nil, &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("scalar expression source exceeds %d UTF-8 bytes", maxSPLSourceBytes),
			Range:   Range{Start: start, End: end},
		}
	}
	// The standalone knowledge boundary uses the closed knowledge token stream;
	// authored single-quote/composite recognition is not enabled and cannot leak
	// through as a caller-selectable sub-feature.
	tokens, err := lexWithQuotedFields(source, false)
	if err != nil {
		return nil, err
	}
	if len(tokens)-1 > maxSPLTokens { // exclude EOF
		return nil, &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("scalar expression contains more than %d syntax tokens", maxSPLTokens),
			Range:   tokens[maxSPLTokens].sourceRange,
		}
	}
	expressionParser := parser{source: source, tokens: tokens, profile: expressionProfileKnowledge}
	expression, err := expressionParser.parseScalarExpression()
	if err != nil {
		return nil, err
	}
	if expressionParser.current().kind != tokenEOF {
		return nil, expressionParser.errorAtCurrent(
			"SPL_UNEXPECTED_TOKEN",
			fmt.Sprintf("unexpected token %q", expressionParser.current().text),
		)
	}
	return expression, nil
}

// AnalyzeScalarExpression validates and inventories a parsed scalar tree.
// Although ParseScalarExpression already bounds parser-produced trees, this
// second bounded walk is required because internal callers may construct ASTs
// directly. Cycles, typed nils, and unsupported nodes fail closed.
func AnalyzeScalarExpression(expression ScalarExpr) (ScalarExpressionAnalysis, error) {
	analyzer := scalarExpressionAnalyzer{fields: make(map[string]struct{})}
	if err := analyzer.visitScalar(expression, 1); err != nil {
		return ScalarExpressionAnalysis{}, err
	}
	fields := make([]string, 0, len(analyzer.fields))
	for field := range analyzer.fields {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	return ScalarExpressionAnalysis{
		InputFields: fields[:len(fields):len(fields)],
		Nodes:       uint32(analyzer.nodes),      // #nosec G115 -- enter bounds nodes to 4 * maxSPLTokens.
		Predicates:  uint32(analyzer.predicates), // #nosec G115 -- predicates cannot exceed the bounded node count.
	}, nil
}

// ScalarExpressionMayReturnBooleanFunction reports the exact authored-eval
// restriction used for a direct assignment: Boolean-returning functions are
// not storable values, including through coalesce/if/case value branches.
// Parse and Analyze must still be called separately for syntax and bounds.
func ScalarExpressionMayReturnBooleanFunction(expression ScalarExpr) bool {
	return scalarExpressionMayReturnBooleanFunction(expression)
}

type scalarExpressionAnalyzer struct {
	fields               map[string]struct{}
	nodes                int
	predicates           int
	arithmeticOperators  int
	membershipCandidates int
}

func (analyzer *scalarExpressionAnalyzer) enter(depth int) error {
	if depth > maximumKnowledgeExpressionAnalysisDepth {
		return fmt.Errorf("scalar expression analysis exceeds %d levels", maximumKnowledgeExpressionAnalysisDepth)
	}
	analyzer.nodes++
	if analyzer.nodes > maximumKnowledgeExpressionAnalysisNodes {
		return fmt.Errorf("scalar expression analysis exceeds %d nodes", maximumKnowledgeExpressionAnalysisNodes)
	}
	return nil
}

func (analyzer *scalarExpressionAnalyzer) visitScalar(expression ScalarExpr, depth int) error {
	if err := analyzer.enter(depth); err != nil {
		return err
	}
	switch expression := expression.(type) {
	case *ScalarFieldExpr:
		if expression == nil || expression.Field == "" {
			return fmt.Errorf("scalar expression contains an invalid field")
		}
		analyzer.fields[expression.Field] = struct{}{}
	case *ScalarLiteralExpr:
		if expression == nil || expression.Value.Kind <= LiteralKindInvalid || expression.Value.Kind > LiteralKindNull {
			return fmt.Errorf("scalar expression contains a nil literal")
		}
	case *ScalarUnaryExpr:
		if expression == nil || expression.Op <= ScalarUnaryOpInvalid || expression.Op >= ScalarUnaryOpCount {
			return fmt.Errorf("scalar expression contains an invalid unary arithmetic node")
		}
		if !validAnalysisRangeOrZero(expression.Range) {
			return fmt.Errorf("scalar expression contains an invalid unary arithmetic range")
		}
		chain := 0
		for current := expression; current != nil; {
			chain++
			if chain > MaximumUnaryOperatorChain {
				return fmt.Errorf("scalar expression unary chain exceeds %d operators", MaximumUnaryOperatorChain)
			}
			next, ok := current.Operand.(*ScalarUnaryExpr)
			if !ok {
				break
			}
			current = next
		}
		if err := analyzer.countArithmeticOperator(); err != nil {
			return err
		}
		return analyzer.visitScalar(expression.Operand, depth+1)
	case *ScalarBinaryExpr:
		if expression == nil || expression.Op <= ScalarBinaryOpInvalid || expression.Op >= ScalarBinaryOpCount {
			return fmt.Errorf("scalar expression contains an invalid binary arithmetic node")
		}
		if !validAnalysisRangeOrZero(expression.Range) {
			return fmt.Errorf("scalar expression contains an invalid binary arithmetic range")
		}
		if err := analyzer.countArithmeticOperator(); err != nil {
			return err
		}
		if err := analyzer.visitScalar(expression.Left, depth+1); err != nil {
			return err
		}
		return analyzer.visitScalar(expression.Right, depth+1)
	case *ScalarCallExpr:
		if expression == nil || expression.Function <= ScalarFunctionInvalid || expression.Function >= ScalarFunctionCount {
			return fmt.Errorf("scalar expression contains an invalid call")
		}
		for _, argument := range expression.Arguments {
			if err := analyzer.visitScalar(argument, depth+1); err != nil {
				return err
			}
		}
	case *ScalarIfExpr:
		if expression == nil {
			return fmt.Errorf("scalar expression contains a nil if")
		}
		if err := analyzer.visitWhere(expression.Condition, depth+1); err != nil {
			return err
		}
		if err := analyzer.visitScalar(expression.True, depth+1); err != nil {
			return err
		}
		return analyzer.visitScalar(expression.False, depth+1)
	case *ScalarCaseExpr:
		if expression == nil || len(expression.Branches) == 0 {
			return fmt.Errorf("scalar expression contains an invalid case")
		}
		for _, branch := range expression.Branches {
			if err := analyzer.visitWhere(branch.Condition, depth+1); err != nil {
				return err
			}
			if err := analyzer.visitScalar(branch.Value, depth+1); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("scalar expression contains an unsupported scalar node")
	}
	return nil
}

func (analyzer *scalarExpressionAnalyzer) visitWhere(expression WhereExpr, depth int) error {
	if err := analyzer.enter(depth); err != nil {
		return err
	}
	switch expression := expression.(type) {
	case *WhereBoolExpr:
		if expression == nil || expression.Op != BoolOpAnd && expression.Op != BoolOpOr {
			return fmt.Errorf("scalar expression contains an invalid Boolean node")
		}
		if err := analyzer.visitWhere(expression.Left, depth+1); err != nil {
			return err
		}
		return analyzer.visitWhere(expression.Right, depth+1)
	case *WhereNotExpr:
		if expression == nil {
			return fmt.Errorf("scalar expression contains a nil NOT node")
		}
		return analyzer.visitWhere(expression.Operand, depth+1)
	case *WhereComparisonExpr:
		if expression == nil || expression.Op <= CompareOpInvalid || expression.Op > CompareOpGreaterEqual {
			return fmt.Errorf("scalar expression contains an invalid comparison")
		}
		if err := analyzer.visitScalar(expression.Left, depth+1); err != nil {
			return err
		}
		if err := analyzer.visitScalar(expression.Right, depth+1); err != nil {
			return err
		}
		return analyzer.countPredicate()
	case *WhereMembershipExpr:
		if expression == nil || len(expression.Candidates) == 0 ||
			len(expression.Candidates) > MaximumMembershipCandidates {
			return fmt.Errorf("scalar expression contains an invalid membership predicate")
		}
		if !validAnalysisRangeOrZero(expression.Range) {
			return fmt.Errorf("scalar expression contains an invalid membership range")
		}
		if analyzer.membershipCandidates > MaximumMembershipCandidatesPerQuery-len(expression.Candidates) {
			return fmt.Errorf("scalar expression contains more than %d membership candidates", MaximumMembershipCandidatesPerQuery)
		}
		analyzer.membershipCandidates += len(expression.Candidates)
		if err := analyzer.visitScalar(expression.Value, depth+1); err != nil {
			return err
		}
		for _, candidate := range expression.Candidates {
			if err := analyzer.visitScalar(candidate, depth+1); err != nil {
				return err
			}
		}
		return analyzer.countPredicate()
	case *WhereScalarPredicateExpr:
		if expression == nil {
			return fmt.Errorf("scalar expression contains a nil predicate")
		}
		if err := analyzer.visitScalar(expression.Value, depth+1); err != nil {
			return err
		}
		return analyzer.countPredicate()
	default:
		return fmt.Errorf("scalar expression contains an unsupported Boolean node")
	}
}

func (analyzer *scalarExpressionAnalyzer) countArithmeticOperator() error {
	if analyzer.arithmeticOperators >= MaximumArithmeticOperatorsPerQuery {
		return fmt.Errorf("scalar expression contains more than %d arithmetic operators", MaximumArithmeticOperatorsPerQuery)
	}
	analyzer.arithmeticOperators++
	return nil
}

func (analyzer *scalarExpressionAnalyzer) countPredicate() error {
	if analyzer.predicates >= maxEvalPredicates {
		return fmt.Errorf("scalar expression contains more than %d predicate leaves", maxEvalPredicates)
	}
	analyzer.predicates++
	return nil
}

// validAnalysisRangeOrZero preserves the zero-range convention used by
// directly assembled internal fixtures while rejecting contradictory authored
// provenance on authored-only nodes in this defensive trust-boundary walk.
func validAnalysisRangeOrZero(sourceRange Range) bool {
	if sourceRange == (Range{}) {
		return true
	}
	if sourceRange.Start.Offset < 0 || sourceRange.Start.Line < 1 || sourceRange.Start.Column < 1 ||
		sourceRange.End.Line < 1 || sourceRange.End.Column < 1 ||
		sourceRange.End.Offset <= sourceRange.Start.Offset || sourceRange.End.Line < sourceRange.Start.Line {
		return false
	}
	return sourceRange.End.Line != sourceRange.Start.Line ||
		sourceRange.End.Column > sourceRange.Start.Column
}
