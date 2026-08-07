package plan

import (
	"errors"
	"fmt"
	"sort"
)

const (
	maximumAnalysisNodes = 4_096
	maximumAnalysisDepth = 1_024
)

// Analysis is bounded metadata derived from a fully accepted logical plan.
// ReferencedFields contains sorted, unique logical row fields read by the
// pipeline. Write-only outputs are omitted unless a later operator reads them.
type Analysis struct {
	ReferencedFields []string
}

// Analyze derives safe public metadata from query. It fails closed for typed
// nils, unknown future nodes, malformed field references, excessive depth, and
// excessive work so forged plans cannot yield incomplete dependency metadata.
func Analyze(query *Query) (Analysis, error) {
	if query == nil {
		return Analysis{}, errors.New("analyze logical query: query is nil")
	}
	analyzer := queryAnalyzer{
		fields: make(map[string]struct{}),
	}
	for _, operator := range query.Operators {
		if err := analyzer.visitOperator(operator, 1); err != nil {
			return Analysis{}, err
		}
	}
	fields := make([]string, 0, len(analyzer.fields))
	for field := range analyzer.fields {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	return Analysis{ReferencedFields: fields}, nil
}

type queryAnalyzer struct {
	fields map[string]struct{}
	nodes  int
}

func (analyzer *queryAnalyzer) enter(depth int) error {
	if depth > maximumAnalysisDepth {
		return fmt.Errorf("analyze logical query: tree exceeds depth %d", maximumAnalysisDepth)
	}
	analyzer.nodes++
	if analyzer.nodes > maximumAnalysisNodes {
		return fmt.Errorf("analyze logical query: tree exceeds %d nodes", maximumAnalysisNodes)
	}
	return nil
}

func (analyzer *queryAnalyzer) addField(field FieldRef, depth int) error {
	if err := analyzer.validateField(field, depth); err != nil {
		return err
	}
	analyzer.fields[field.Name] = struct{}{}
	return nil
}

func (analyzer *queryAnalyzer) validateField(field FieldRef, depth int) error {
	if err := analyzer.enter(depth); err != nil {
		return err
	}
	if field.Name == "" {
		return errors.New("analyze logical query: field name is empty")
	}
	return nil
}

func (analyzer *queryAnalyzer) validateOutputName(name string, depth int) error {
	if err := analyzer.enter(depth); err != nil {
		return err
	}
	if name == "" {
		return errors.New("analyze logical query: output field name is empty")
	}
	return nil
}

func (analyzer *queryAnalyzer) addFields(fields []FieldRef, depth int) error {
	for _, field := range fields {
		if err := analyzer.addField(field, depth); err != nil {
			return err
		}
	}
	return nil
}

func (analyzer *queryAnalyzer) addChronologicalTimeDependency(
	function AggregateFunction,
	source FieldRef,
	depth int,
) error {
	if function != AggregateFunctionEarliest &&
		function != AggregateFunctionLatest {
		return nil
	}
	return analyzer.addField(FieldRef{
		Name:      "_time",
		Canonical: true,
		Range:     source.Range,
	}, depth)
}

func (analyzer *queryAnalyzer) visitAggregateMeasure(
	measure AggregateMeasure,
	depth int,
) error {
	if err := analyzer.validateOutputName(measure.Output, depth); err != nil {
		return err
	}
	switch measure.Function {
	case AggregateFunctionCountRows, AggregateFunctionCountPredicate:
	default:
		if err := analyzer.addField(measure.Input, depth); err != nil {
			return err
		}
	}
	if err := analyzer.addChronologicalTimeDependency(
		measure.Function,
		measure.Input,
		depth,
	); err != nil {
		return err
	}
	if measure.Predicate != nil {
		return analyzer.visitExpression(measure.Predicate, depth)
	}
	return nil
}

func (analyzer *queryAnalyzer) visitOperator(operator Operator, depth int) error {
	if err := analyzer.enter(depth); err != nil {
		return err
	}
	if operator == nil || isNilOperator(operator) {
		return fmt.Errorf("analyze logical query: operator %T is nil", operator)
	}
	switch operator := operator.(type) {
	case *Scan:
		return nil
	case *Filter:
		return analyzer.visitExpression(operator.Expression, depth+1)
	case *Project:
		return analyzer.addFields(operator.Fields, depth+1)
	case *Extend:
		for _, assignment := range operator.Assignments {
			if err := analyzer.validateField(assignment.Output, depth+1); err != nil {
				return err
			}
			if err := analyzer.visitScalarExpression(assignment.Expression, depth+1); err != nil {
				return err
			}
		}
		return nil
	case *TimeBucket:
		if err := analyzer.validateField(operator.Output, depth+1); err != nil {
			return err
		}
		return analyzer.addField(operator.Field, depth+1)
	case *NumericBucket:
		if err := analyzer.validateField(operator.Output, depth+1); err != nil {
			return err
		}
		return analyzer.addField(operator.Input, depth+1)
	case *Extract:
		for _, capture := range operator.Captures {
			if err := analyzer.validateField(capture.Output, depth+1); err != nil {
				return err
			}
		}
		return analyzer.addField(operator.Input, depth+1)
	case *ExtractJSON:
		if err := analyzer.validateField(operator.Output, depth+1); err != nil {
			return err
		}
		return analyzer.addField(operator.Input, depth+1)
	case *Rename:
		for _, assignment := range operator.Assignments {
			if err := analyzer.validateField(assignment.Destination, depth+1); err != nil {
				return err
			}
			if err := analyzer.addField(assignment.Source, depth+1); err != nil {
				return err
			}
		}
		return nil
	case *EventAggregate:
		if !validEventAggregateContract(operator) {
			return errors.New(
				"analyze logical query: event aggregate is invalid",
			)
		}
		if err := analyzer.visitAggregateMeasure(operator.Measure, depth+1); err != nil {
			return err
		}
		for _, field := range operator.GroupBy {
			if err := analyzer.addField(field, depth+1); err != nil {
				return err
			}
		}
		return nil
	case *StreamAggregate:
		if !validStreamAggregateContract(operator) {
			return errors.New(
				"analyze logical query: stream aggregate is invalid",
			)
		}
		if err := analyzer.visitAggregateMeasure(operator.Measure, depth+1); err != nil {
			return err
		}
		return analyzer.addFields(operator.GroupBy, depth+1)
	case *Aggregate:
		if err := analyzer.addFields(operator.GroupBy, depth+1); err != nil {
			return err
		}
		for _, measure := range operator.Measures {
			if err := analyzer.visitAggregateMeasure(measure, depth+1); err != nil {
				return err
			}
		}
		return nil
	case *Timechart:
		if err := analyzer.addField(operator.Time, depth+1); err != nil {
			return err
		}
		if !validTimechartMeasureContract(operator) {
			return errors.New("analyze logical query: timechart measure is invalid")
		}
		if err := analyzer.validateOutputName(operator.Measure.Output, depth+1); err != nil {
			return err
		}
		if operator.Measure.Function != AggregateFunctionCountRows {
			if err := analyzer.addField(operator.Measure.Input, depth+1); err != nil {
				return err
			}
		}
		if operator.Split != nil {
			return analyzer.addField(operator.Split.Field, depth+1)
		}
		return nil
	case *Chart:
		if !validChartContract(operator) {
			return errors.New("analyze logical query: chart contract is invalid")
		}
		if err := analyzer.addField(operator.Over, depth+1); err != nil {
			return err
		}
		if err := analyzer.addField(operator.SplitBy, depth+1); err != nil {
			return err
		}
		if operator.Measure.Function != AggregateFunctionCountRows {
			return analyzer.addField(operator.Measure.Input, depth+1)
		}
		return nil
	case *Window:
		if err := analyzer.validateOutputName(operator.Output, depth+1); err != nil {
			return err
		}
		return analyzer.addField(operator.Input, depth+1)
	case *Sort:
		for _, key := range operator.Keys {
			if err := analyzer.addField(key.Field, depth+1); err != nil {
				return err
			}
		}
		return nil
	case *Deduplicate:
		return analyzer.addFields(operator.Keys, depth+1)
	case *Limit:
		return nil
	default:
		return fmt.Errorf("analyze logical query: unsupported operator %T", operator)
	}
}

func (analyzer *queryAnalyzer) visitExpression(expression Expression, depth int) error {
	if err := analyzer.enter(depth); err != nil {
		return err
	}
	switch expression := expression.(type) {
	case *BooleanExpression:
		if expression == nil {
			return errors.New("analyze logical query: boolean expression is nil")
		}
		if err := analyzer.visitExpression(expression.Left, depth+1); err != nil {
			return err
		}
		return analyzer.visitExpression(expression.Right, depth+1)
	case *NotExpression:
		if expression == nil {
			return errors.New("analyze logical query: not expression is nil")
		}
		return analyzer.visitExpression(expression.Operand, depth+1)
	case *TextExpression:
		if expression == nil {
			return errors.New("analyze logical query: text expression is nil")
		}
		return analyzer.addField(FieldRef{Name: "_raw", Canonical: true}, depth+1)
	case *ComparisonExpression:
		if expression == nil {
			return errors.New("analyze logical query: comparison expression is nil")
		}
		return analyzer.addField(expression.Field, depth+1)
	case *EvalComparisonExpression:
		if expression == nil {
			return errors.New("analyze logical query: eval comparison expression is nil")
		}
		if err := analyzer.visitScalarExpression(expression.Left, depth+1); err != nil {
			return err
		}
		return analyzer.visitScalarExpression(expression.Right, depth+1)
	case *ScalarPredicateExpression:
		if expression == nil {
			return errors.New("analyze logical query: scalar predicate expression is nil")
		}
		return analyzer.visitScalarExpression(expression.Value, depth+1)
	default:
		return fmt.Errorf("analyze logical query: unsupported expression %T", expression)
	}
}

func (analyzer *queryAnalyzer) visitScalarExpression(expression ScalarExpression, depth int) error {
	if err := analyzer.enter(depth); err != nil {
		return err
	}
	switch expression := expression.(type) {
	case *ScalarFieldExpression:
		if expression == nil {
			return errors.New("analyze logical query: scalar field expression is nil")
		}
		return analyzer.addField(expression.Field, depth+1)
	case *ScalarLiteralExpression:
		if expression == nil {
			return errors.New("analyze logical query: scalar literal expression is nil")
		}
		return nil
	case *ScalarCallExpression:
		if expression == nil {
			return errors.New("analyze logical query: scalar call expression is nil")
		}
		for _, argument := range expression.Arguments {
			if err := analyzer.visitScalarExpression(argument, depth+1); err != nil {
				return err
			}
		}
		return nil
	case *ScalarIfExpression:
		if expression == nil {
			return errors.New("analyze logical query: scalar if expression is nil")
		}
		if err := analyzer.visitExpression(expression.Condition, depth+1); err != nil {
			return err
		}
		if err := analyzer.visitScalarExpression(expression.True, depth+1); err != nil {
			return err
		}
		return analyzer.visitScalarExpression(expression.False, depth+1)
	case *ScalarCaseExpression:
		if expression == nil {
			return errors.New("analyze logical query: scalar case expression is nil")
		}
		for _, branch := range expression.Branches {
			if err := analyzer.visitExpression(branch.Condition, depth+1); err != nil {
				return err
			}
			if err := analyzer.visitScalarExpression(branch.Value, depth+1); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("analyze logical query: unsupported scalar expression %T", expression)
	}
}
