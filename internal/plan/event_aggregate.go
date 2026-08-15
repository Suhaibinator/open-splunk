package plan

import (
	"reflect"
	"slices"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// validEventAggregateContract recognizes the deliberately narrow,
// row-preserving eventstats count/count(field)/count(eval(predicate)),
// pN/percN(field), min(field), max(field), earliest(field), latest(field),
// sum(field), avg(field), dc(field), values(field), or list(field) plan
// contract.
// Consumers that use event provenance metadata must fail closed when handed
// forged logical operators.
func validEventAggregateContract(operator *EventAggregate) bool {
	if operator == nil ||
		len(operator.GroupBy) > spl.MaximumStatsGroupFields ||
		operator.Measure.Output == "" ||
		operator.Measure.OutputLiteral ||
		operator.Measure.Sparkline != nil ||
		operator.Measure.InputExpression != nil {
		return false
	}
	if operator.Measure.Function != AggregateFunctionPercentile &&
		operator.Measure.Percentile != 0 {
		return false
	}
	switch operator.Measure.Function {
	case AggregateFunctionCountRows:
		if operator.Measure.Predicate != nil ||
			!emptyAggregateField(operator.Measure.Input) {
			return false
		}
	case AggregateFunctionCountValues, AggregateFunctionMinimum,
		AggregateFunctionMaximum,
		AggregateFunctionEarliest,
		AggregateFunctionLatest,
		AggregateFunctionSum,
		AggregateFunctionAverage,
		AggregateFunctionDistinctCount,
		AggregateFunctionValues,
		AggregateFunctionList:
		if operator.Measure.Predicate != nil ||
			!validResolvedEventAggregateField(operator.Measure.Input) {
			return false
		}
	case AggregateFunctionPercentile:
		if operator.Measure.Predicate != nil ||
			operator.Measure.Percentile < 1 ||
			operator.Measure.Percentile > 99 ||
			!validResolvedEventAggregateField(operator.Measure.Input) {
			return false
		}
	case AggregateFunctionCountPredicate:
		if !emptyAggregateField(operator.Measure.Input) ||
			!validEventAggregatePredicate(operator.Measure.Predicate) {
			return false
		}
	default:
		return false
	}
	if _, err := ResolveField(
		operator.Measure.Output,
		spl.Range{},
	); err != nil {
		return false
	}

	seen := make(map[string]struct{}, len(operator.GroupBy))
	for _, group := range operator.GroupBy {
		if !validResolvedEventAggregateField(group) {
			return false
		}
		if _, duplicate := seen[group.Name]; duplicate {
			return false
		}
		seen[group.Name] = struct{}{}
	}
	return true
}

type eventAggregatePredicateValidator struct {
	nodes                int
	arithmeticOperators  int
	membershipCandidates int
	active               map[any]struct{}
}

func validEventAggregatePredicate(expression Expression) bool {
	validator := eventAggregatePredicateValidator{
		active: make(map[any]struct{}),
	}
	return validator.validExpression(expression, 1)
}

func (validator *eventAggregatePredicateValidator) enter(
	node any,
	depth int,
) bool {
	if depth > maxConvertedExpressionDepth {
		return false
	}
	if !reflect.TypeOf(node).Comparable() {
		return false
	}
	if _, cyclic := validator.active[node]; cyclic {
		return false
	}
	validator.nodes++
	if validator.nodes > maxConvertedExpressionNodes {
		return false
	}
	validator.active[node] = struct{}{}
	return true
}

func (validator *eventAggregatePredicateValidator) chargeArithmeticOperator() bool {
	if validator.arithmeticOperators >= spl.MaximumArithmeticOperatorsPerQuery {
		return false
	}
	validator.arithmeticOperators++
	return true
}

func (validator *eventAggregatePredicateValidator) chargeMembershipCandidates(
	count int,
) bool {
	if count < 1 || count > spl.MaximumMembershipCandidates ||
		validator.membershipCandidates >
			spl.MaximumMembershipCandidatesPerQuery-count {
		return false
	}
	validator.membershipCandidates += count
	return true
}

func (validator *eventAggregatePredicateValidator) leave(node any) {
	delete(validator.active, node)
}

func (validator *eventAggregatePredicateValidator) validExpression(
	expression Expression,
	depth int,
) bool {
	if expression == nil || !validator.enter(expression, depth) {
		return false
	}
	defer validator.leave(expression)
	switch expression := expression.(type) {
	case *BooleanExpression:
		return expression != nil &&
			(expression.Op == BooleanOpAnd || expression.Op == BooleanOpOr) &&
			validator.validExpression(expression.Left, depth+1) &&
			validator.validExpression(expression.Right, depth+1)
	case *NotExpression:
		return expression != nil &&
			validator.validExpression(expression.Operand, depth+1)
	case *EvalComparisonExpression:
		return expression != nil &&
			validEventAggregateComparisonOp(expression.Op) &&
			validator.validScalar(expression.Left, depth+1) &&
			validator.validScalar(expression.Right, depth+1)
	case *ScalarPredicateExpression:
		if expression == nil {
			return false
		}
		return validator.validScalar(expression.Value, depth+1) &&
			scalarExpressionCanBeDirectPredicate(expression.Value)
	case *MembershipExpression:
		if expression == nil ||
			!validExpressionRangeOrZero(expression.Range) ||
			!validator.chargeMembershipCandidates(len(expression.Candidates)) ||
			!validator.validScalar(expression.Value, depth+1) {
			return false
		}
		for _, candidate := range expression.Candidates {
			if !validator.validScalar(candidate, depth+1) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func (validator *eventAggregatePredicateValidator) validScalar(
	expression ScalarExpression,
	depth int,
) bool {
	return validator.validScalarWithUnaryChain(expression, depth, 0)
}

func (validator *eventAggregatePredicateValidator) validScalarWithUnaryChain(
	expression ScalarExpression,
	depth int,
	unaryChain int,
) bool {
	if expression == nil || !validator.enter(expression, depth) {
		return false
	}
	defer validator.leave(expression)
	switch expression := expression.(type) {
	case *ScalarUnaryExpression:
		return expression != nil &&
			validScalarUnaryOp(expression.Op) &&
			validExpressionRangeOrZero(expression.Range) &&
			unaryChain < spl.MaximumUnaryOperatorChain &&
			validator.chargeArithmeticOperator() &&
			validator.validScalarWithUnaryChain(
				expression.Operand,
				depth+1,
				unaryChain+1,
			)
	case *ScalarBinaryExpression:
		return expression != nil &&
			validScalarBinaryOp(expression.Op) &&
			validExpressionRangeOrZero(expression.Range) &&
			validator.chargeArithmeticOperator() &&
			validator.validScalar(expression.Left, depth+1) &&
			validator.validScalar(expression.Right, depth+1)
	case *ScalarFieldExpression:
		return expression != nil &&
			validResolvedEventAggregateField(expression.Field)
	case *ScalarLiteralExpression:
		return expression != nil &&
			validEventAggregateLiteralKind(expression.Value.Kind)
	case *ScalarCallExpression:
		if expression == nil ||
			!validEventAggregateScalarFunction(expression.Function) {
			return false
		}
		for _, argument := range expression.Arguments {
			if !validator.validScalar(argument, depth+1) {
				return false
			}
		}
		return true
	case *ScalarIfExpression:
		return expression != nil &&
			validator.validExpression(expression.Condition, depth+1) &&
			validator.validScalar(expression.True, depth+1) &&
			validator.validScalar(expression.False, depth+1)
	case *ScalarCaseExpression:
		if expression == nil {
			return false
		}
		for _, branch := range expression.Branches {
			if !validator.validExpression(branch.Condition, depth+1) ||
				!validator.validScalar(branch.Value, depth+1) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func validEventAggregateScalarFunction(function ScalarFunction) bool {
	switch function {
	case ScalarFunctionToNumber,
		ScalarFunctionReplace,
		ScalarFunctionIsNull,
		ScalarFunctionIsNotNull,
		ScalarFunctionCoalesce,
		ScalarFunctionLower,
		ScalarFunctionUpper,
		ScalarFunctionLength,
		ScalarFunctionSubstring,
		ScalarFunctionToString,
		ScalarFunctionRound,
		ScalarFunctionCeil,
		ScalarFunctionFloor,
		ScalarFunctionMVCount,
		ScalarFunctionMVSort,
		ScalarFunctionMatch,
		ScalarFunctionLike,
		ScalarFunctionNow,
		ScalarFunctionStrftime,
		ScalarFunctionStrptime,
		ScalarFunctionRelativeTime,
		ScalarFunctionConcat:
		return true
	default:
		return false
	}
}

func validEventAggregateComparisonOp(op ComparisonOp) bool {
	switch op {
	case ComparisonOpEqual,
		ComparisonOpNotEqual,
		ComparisonOpLess,
		ComparisonOpLessEqual,
		ComparisonOpGreater,
		ComparisonOpGreaterEqual:
		return true
	default:
		return false
	}
}

func validEventAggregateLiteralKind(kind ValueKind) bool {
	switch kind {
	case ValueKindNull,
		ValueKindString,
		ValueKindInt64,
		ValueKindUint64,
		ValueKindFloat64,
		ValueKindBool:
		return true
	default:
		return false
	}
}

func validResolvedEventAggregateField(field FieldRef) bool {
	resolved, err := ResolveField(field.Name, field.Range)
	if err != nil {
		resolved, err = ResolveQuotedField(field.Name, field.Range)
		if err != nil {
			return false
		}
	}
	return resolved.Name == field.Name &&
		resolved.Canonical == field.Canonical &&
		(resolved.Path == nil) == (field.Path == nil) &&
		slices.Equal(resolved.Path, field.Path) &&
		resolved.Range == field.Range
}
