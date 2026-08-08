package spl

import (
	"fmt"
	"sort"
)

const (
	maximumKnowledgeExpressionAnalysisNodes = maxSPLTokens * 4
	maximumKnowledgeExpressionAnalysisDepth = maxScalarNestingDepth * 2
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
	tokens, err := lex(source)
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
	parser := parser{tokens: tokens}
	expression, err := parser.parseScalarExpression()
	if err != nil {
		return nil, err
	}
	if parser.current().kind != tokenEOF {
		return nil, parser.errorAtCurrent(
			"SPL_UNEXPECTED_TOKEN",
			fmt.Sprintf("unexpected token %q", parser.current().text),
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
		Nodes:       uint32(analyzer.nodes),
		Predicates:  uint32(analyzer.predicates),
	}, nil
}

type scalarExpressionAnalyzer struct {
	fields     map[string]struct{}
	nodes      int
	predicates int
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
		if expression == nil {
			return fmt.Errorf("scalar expression contains a nil literal")
		}
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
		analyzer.predicates++
		return nil
	case *WhereScalarPredicateExpr:
		if expression == nil {
			return fmt.Errorf("scalar expression contains a nil predicate")
		}
		if err := analyzer.visitScalar(expression.Value, depth+1); err != nil {
			return err
		}
		analyzer.predicates++
		return nil
	default:
		return fmt.Errorf("scalar expression contains an unsupported Boolean node")
	}
}
