package clickhouse

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

// mathScalarSpec describes one numeric eval function lowered through the
// arithmetic operand model. Every operand is normalized exactly like an
// arithmetic leaf so Dynamic event fields, numeric text, tagged decimals and
// malformed semantics follow the same contract as `+`, and the result is an
// IEEE Float64 like every other arithmetic value.
type mathScalarSpec struct {
	name             string
	minimumArguments int
	maximumArguments int
	// body renders the ClickHouse expression over the bound operand names.
	// Domain errors (negative square roots, non-positive logarithms, complex
	// powers) lower to null rather than NaN so downstream comparisons and
	// statistics treat them as absent values.
	body func(operands []string) string
}

const nullFloat64SQL = "CAST(NULL AS Nullable(Float64))"

// normalizeMathResultSQL folds IEEE negative zero into positive zero, matching
// the arithmetic renderer so `-0` never leaks into ordering keys or text.
func normalizeMathResultSQL(sql string) string {
	return "plus(" + sql + ", toFloat64(0))"
}

var mathScalarSpecs = map[plan.ScalarFunction]mathScalarSpec{
	plan.ScalarFunctionAbs: {
		name: "abs", minimumArguments: 1, maximumArguments: 1,
		body: func(operands []string) string {
			return normalizeMathResultSQL("abs(" + operands[0] + ")")
		},
	},
	plan.ScalarFunctionSqrt: {
		name: "sqrt", minimumArguments: 1, maximumArguments: 1,
		body: func(operands []string) string {
			return normalizeMathResultSQL("if(" + operands[0] + " < 0, " + nullFloat64SQL +
				", sqrt(" + operands[0] + "))")
		},
	},
	plan.ScalarFunctionExp: {
		name: "exp", minimumArguments: 1, maximumArguments: 1,
		body: func(operands []string) string {
			return normalizeMathResultSQL("exp(" + operands[0] + ")")
		},
	},
	plan.ScalarFunctionLn: {
		name: "ln", minimumArguments: 1, maximumArguments: 1,
		body: func(operands []string) string {
			return normalizeMathResultSQL("if(" + operands[0] + " <= 0, " + nullFloat64SQL +
				", log(" + operands[0] + "))")
		},
	},
	plan.ScalarFunctionLog: {
		name: "log", minimumArguments: 1, maximumArguments: 2,
		body: func(operands []string) string {
			if len(operands) == 1 {
				return normalizeMathResultSQL("if(" + operands[0] + " <= 0, " + nullFloat64SQL +
					", log10(" + operands[0] + "))")
			}
			return normalizeMathResultSQL("if(" + operands[0] + " <= 0 OR " + operands[1] +
				" <= 0 OR " + operands[1] + " = 1, " + nullFloat64SQL +
				", divide(log(" + operands[0] + "), log(" + operands[1] + ")))")
		},
	},
	plan.ScalarFunctionPow: {
		name: "pow", minimumArguments: 2, maximumArguments: 2,
		body: func(operands []string) string {
			power := "pow(" + operands[0] + ", " + operands[1] + ")"
			return normalizeMathResultSQL("if(isNaN(" + power + "), " + nullFloat64SQL +
				", " + power + ")")
		},
	},
	plan.ScalarFunctionPi: {
		name: "pi", minimumArguments: 0, maximumArguments: 0,
		body: func([]string) string { return "pi()" },
	},
}

// compileMathScalar lowers abs/sqrt/exp/ln/log/pow/pi. Each call is charged as
// one arithmetic operator so the per-query operator ceiling and the atomic
// result contract for Dynamic operands apply exactly as they do to `+`.
func compileMathScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	if expression == nil {
		return compiledScalar{}, errors.New("compile ClickHouse math function: missing expression")
	}
	spec, ok := mathScalarSpecs[expression.Function]
	if !ok {
		return compiledScalar{}, fmt.Errorf(
			"compile ClickHouse math function: unsupported function %d",
			expression.Function,
		)
	}
	if len(expression.Arguments) < spec.minimumArguments ||
		len(expression.Arguments) > spec.maximumArguments {
		return compiledScalar{}, fmt.Errorf(
			"compile ClickHouse %s: expected %d to %d arguments",
			spec.name,
			spec.minimumArguments,
			spec.maximumArguments,
		)
	}
	if err := chargeCompiledArithmeticOperator(state.context, expression.Range); err != nil {
		return compiledScalar{}, err
	}

	parameters := make([]string, 0, len(expression.Arguments))
	values := make([]string, 0, len(expression.Arguments))
	var args []any
	alwaysNull := false
	materializeForPredicate := false
	for index, argument := range expression.Arguments {
		operand, err := compileNonBooleanScalarInputArgument(argument, state, spec.name)
		if err != nil {
			return compiledScalar{}, err
		}
		operand, err = normalizeArithmeticOperand(operand, argument.SourceRange())
		if err != nil {
			return compiledScalar{}, err
		}
		parameters = append(parameters, "__os_math_operand_"+strconv.Itoa(index+1))
		values = append(values, operand.valueSQL)
		args = append(args, operand.valueArgs...)
		alwaysNull = alwaysNull || operand.alwaysNull
		materializeForPredicate = materializeForPredicate || operand.materializeForPredicate
	}
	valueSQL := spec.body(parameters)
	if len(parameters) > 0 {
		valueSQL = bindSQLExpressions(parameters, values, valueSQL)
	}
	if err := validateExpressionNodeSQLBytes(valueSQL, spec.name, expression.Range); err != nil {
		return compiledScalar{}, err
	}
	return arithmeticCompiledScalar(valueSQL, args, alwaysNull, materializeForPredicate), nil
}
