package clickhouse

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	// MaximumArithmeticDynamicStringBytes bounds the lexical work performed for
	// one open-schema String operand. Fixed Strings are rejected before this
	// runtime conversion path is constructed.
	MaximumArithmeticDynamicStringBytes = 4 << 10
	// maxCompiledExpressionNodeSQLBytes independently bounds every arithmetic
	// and membership node before it can be embedded in a larger query.
	maxCompiledExpressionNodeSQLBytes = 64 << 10
	// ClickHouse's default AST-depth limit is 1,000. Negative-zero
	// canonicalization adds another function layer per direct operation, so
	// large legal trees switch to the constant-depth RPN fold well before the
	// backend limit. Ordinary expressions retain the smaller direct lowering.
	maximumDirectArithmeticOperators = 64

	// UnsupportedExpressionValueMarker classifies a Dynamic value that claims a
	// recognized semantic tag but does not satisfy that tag's bounded payload
	// contract. The marker deliberately contains no field name or payload text.
	UnsupportedExpressionValueMarker = "open-splunk: expression encountered a malformed semantic value"
)

func compileArithmeticUnary(
	expression *plan.ScalarUnaryExpression,
	state compileState,
) (compiledScalar, error) {
	if expression == nil {
		return compiledScalar{}, errors.New("compile ClickHouse arithmetic: missing unary expression")
	}
	return compileArithmeticTree(expression, state)
}

func compileArithmeticBinary(
	expression *plan.ScalarBinaryExpression,
	state compileState,
) (compiledScalar, error) {
	if expression == nil {
		return compiledScalar{}, errors.New("compile ClickHouse arithmetic: missing binary expression")
	}
	return compileArithmeticTree(expression, state)
}

// arithmeticTreeCompiler lowers one maximal arithmetic subtree into a single
// lambda. Leaf values are bound once in authored left-to-right order, while
// intermediate operations remain a linear expression tree inside the lambda.
// This keeps the public 256-operator ceiling executable instead of nesting one
// arrayMap materialization per operator and copying the complete prefix at
// every recursive return.
type arithmeticTreeCompiler struct {
	state                   compileState
	parameters              []string
	values                  []string
	args                    []any
	alwaysNull              bool
	materializeForPredicate bool
	singleUnaryRoot         bool
	operatorCount           int
}

func compileArithmeticTree(
	expression plan.ScalarExpression,
	state compileState,
) (compiledScalar, error) {
	compiler := arithmeticTreeCompiler{
		state:           state,
		singleUnaryRoot: isScalarUnaryExpression(expression),
	}
	body, err := compiler.compile(expression)
	if err != nil {
		return compiledScalar{}, err
	}
	if len(compiler.parameters) == 0 {
		return compiledScalar{}, errors.New("compile ClickHouse arithmetic: expression has no operand")
	}
	var bodySQL string
	var valueSQL string
	if compiler.operatorCount > maximumDirectArithmeticOperators {
		// The fold consumes the authored operands from one array literal. Do not
		// wrap this path in the ordinary one-parameter-per-leaf lambda: a legal
		// 256-operator expression has 257 leaves, and that very wide lambda can
		// exceed ClickHouse's parser/AST limits before arrayFold is reached.
		bodySQL = body.foldSQL(compiler.values)
		valueSQL = bodySQL
	} else {
		var rendered strings.Builder
		rendered.Grow(len(compiler.parameters) * 64)
		body.writeSQL(&rendered)
		bodySQL = rendered.String()
		valueSQL = bindSQLExpressions(compiler.parameters, compiler.values, bodySQL)
	}
	if err := validateExpressionNodeSQLBytes(
		valueSQL,
		"arithmetic",
		expression.SourceRange(),
	); err != nil {
		return compiledScalar{}, err
	}
	return arithmeticCompiledScalar(
		valueSQL,
		compiler.args,
		compiler.alwaysNull,
		compiler.materializeForPredicate,
	), nil
}

type arithmeticSQLNode struct {
	kind    uint8
	unary   plan.ScalarUnaryOp
	binary  plan.ScalarBinaryOp
	name    string
	operand int
	left    *arithmeticSQLNode
	right   *arithmeticSQLNode
}

const (
	arithmeticRPNUnaryNegative int16 = -1
	arithmeticRPNMultiply      int16 = -2
	arithmeticRPNDivide        int16 = -3
	arithmeticRPNRemainder     int16 = -4
	arithmeticRPNAdd           int16 = -5
	arithmeticRPNSubtract      int16 = -6
)

const (
	arithmeticSQLLeaf uint8 = iota + 1
	arithmeticSQLUnary
	arithmeticSQLBinary
)

func (node *arithmeticSQLNode) writeSQL(builder *strings.Builder) {
	if node == nil {
		panic("render ClickHouse arithmetic: nil SQL node")
	}
	switch node.kind {
	case arithmeticSQLLeaf:
		builder.WriteString(node.name)
	case arithmeticSQLUnary:
		if node.unary != plan.ScalarUnaryOpNegative {
			panic("render ClickHouse arithmetic: invalid unary SQL node")
		}
		builder.WriteString("plus(negate(")
		node.left.writeSQL(builder)
		builder.WriteString("), toFloat64(0))")
	case arithmeticSQLBinary:
		normalizeNegativeZero := node.binary != plan.ScalarBinaryOpDivide
		if normalizeNegativeZero {
			builder.WriteString("plus(")
		}
		switch node.binary {
		case plan.ScalarBinaryOpMultiply:
			builder.WriteString("multiply(")
		case plan.ScalarBinaryOpDivide:
			builder.WriteString("divideOrNull(")
		case plan.ScalarBinaryOpRemainder:
			builder.WriteString("moduloOrNull(")
		case plan.ScalarBinaryOpAdd:
			builder.WriteString("plus(")
		case plan.ScalarBinaryOpSubtract:
			builder.WriteString("minus(")
		default:
			panic("render ClickHouse arithmetic: invalid binary SQL node")
		}
		node.left.writeSQL(builder)
		builder.WriteString(", ")
		node.right.writeSQL(builder)
		builder.WriteByte(')')
		if normalizeNegativeZero {
			builder.WriteString(", toFloat64(0))")
		}
	default:
		panic("render ClickHouse arithmetic: invalid SQL node kind")
	}
}

func (node *arithmeticSQLNode) appendRPN(tokens []int16) []int16 {
	if node == nil {
		panic("render ClickHouse arithmetic RPN: nil SQL node")
	}
	switch node.kind {
	case arithmeticSQLLeaf:
		return append(tokens, int16(node.operand)) // #nosec G115 -- at most 257 operands.
	case arithmeticSQLUnary:
		tokens = node.left.appendRPN(tokens)
		return append(tokens, arithmeticRPNUnaryNegative)
	case arithmeticSQLBinary:
		tokens = node.left.appendRPN(tokens)
		tokens = node.right.appendRPN(tokens)
		var token int16
		switch node.binary {
		case plan.ScalarBinaryOpMultiply:
			token = arithmeticRPNMultiply
		case plan.ScalarBinaryOpDivide:
			token = arithmeticRPNDivide
		case plan.ScalarBinaryOpRemainder:
			token = arithmeticRPNRemainder
		case plan.ScalarBinaryOpAdd:
			token = arithmeticRPNAdd
		case plan.ScalarBinaryOpSubtract:
			token = arithmeticRPNSubtract
		default:
			panic("render ClickHouse arithmetic RPN: invalid binary operator")
		}
		return append(tokens, token)
	default:
		panic("render ClickHouse arithmetic RPN: invalid SQL node kind")
	}
}

func (node *arithmeticSQLNode) foldSQL(values []string) string {
	tokens := node.appendRPN(make([]int16, 0, len(values)*2))
	tokenText := make([]string, len(tokens))
	for index, token := range tokens {
		tokenText[index] = strconv.FormatInt(int64(token), 10)
	}
	const (
		stack       = "__os_arithmetic_stack"
		opcodeAlias = "__os_arithmetic_token"
	)
	operands := "CAST([" + strings.Join(values, ", ") +
		"], 'Array(Nullable(Float64))')"
	left := "arrayElement(" + stack + ", -2)"
	right := "arrayElement(" + stack + ", -1)"
	normalize := func(operation string) string {
		return "plus(" + operation + ", toFloat64(0))"
	}
	binary := "multiIf(" +
		opcodeAlias + " = " + strconv.FormatInt(int64(arithmeticRPNMultiply), 10) + ", " +
		normalize("multiply("+left+", "+right+")") + ", " +
		opcodeAlias + " = " + strconv.FormatInt(int64(arithmeticRPNDivide), 10) + ", " +
		"divideOrNull(" + left + ", " + right + "), " +
		opcodeAlias + " = " + strconv.FormatInt(int64(arithmeticRPNRemainder), 10) + ", " +
		normalize("moduloOrNull("+left+", "+right+")") + ", " +
		opcodeAlias + " = " + strconv.FormatInt(int64(arithmeticRPNAdd), 10) + ", " +
		normalize("plus("+left+", "+right+")") + ", " +
		opcodeAlias + " = " + strconv.FormatInt(int64(arithmeticRPNSubtract), 10) + ", " +
		normalize("minus("+left+", "+right+")") + ", " +
		"CAST(NULL AS Nullable(Float64)))"
	popOne := "arrayPopBack(" + stack + ")"
	popTwo := "arrayPopBack(" + popOne + ")"
	step := "if(" + opcodeAlias + " > 0, " +
		"arrayPushBack(" + stack + ", arrayElement(" + operands + ", toUInt16(" + opcodeAlias + "))), " +
		"if(" + opcodeAlias + " = " + strconv.FormatInt(int64(arithmeticRPNUnaryNegative), 10) + ", " +
		"arrayPushBack(" + popOne + ", " + normalize("negate("+right+")") + "), " +
		"arrayPushBack(" + popTwo + ", " + binary + ")))"
	fold := "arrayElement(arrayFold((" + stack + ", " + opcodeAlias + ") -> " + step +
		", CAST([" + strings.Join(tokenText, ", ") + "], 'Array(Int16)'), " +
		"CAST([], 'Array(Nullable(Float64))')), -1)"
	return fold
}

func (compiler *arithmeticTreeCompiler) compile(expression plan.ScalarExpression) (*arithmeticSQLNode, error) {
	switch node := expression.(type) {
	case *plan.ScalarUnaryExpression:
		if node == nil {
			return nil, errors.New("compile ClickHouse arithmetic: missing unary expression")
		}
		if !validCompiledScalarUnaryOp(node.Op) {
			return nil, fmt.Errorf("compile ClickHouse arithmetic: invalid unary operator %d", node.Op)
		}
		if nilScalarExpression(node.Operand) {
			return nil, errors.New("compile ClickHouse arithmetic: missing unary operand")
		}
		if err := chargeCompiledArithmeticOperator(compiler.state.context, node.Range); err != nil {
			return nil, err
		}
		compiler.operatorCount++
		operand, err := compiler.compile(node.Operand)
		if err != nil {
			return nil, err
		}
		if node.Op == plan.ScalarUnaryOpNegative {
			return &arithmeticSQLNode{
				kind: arithmeticSQLUnary, unary: node.Op, left: operand,
			}, nil
		}
		return operand, nil

	case *plan.ScalarBinaryExpression:
		if node == nil {
			return nil, errors.New("compile ClickHouse arithmetic: missing binary expression")
		}
		if !validCompiledScalarBinaryOp(node.Op) {
			return nil, fmt.Errorf("compile ClickHouse arithmetic: invalid binary operator %d", node.Op)
		}
		if nilScalarExpression(node.Left) || nilScalarExpression(node.Right) {
			return nil, errors.New("compile ClickHouse arithmetic: missing binary operand")
		}
		if err := chargeCompiledArithmeticOperator(compiler.state.context, node.Range); err != nil {
			return nil, err
		}
		compiler.operatorCount++
		left, err := compiler.compile(node.Left)
		if err != nil {
			return nil, err
		}
		right, err := compiler.compile(node.Right)
		if err != nil {
			return nil, err
		}
		return &arithmeticSQLNode{
			kind: arithmeticSQLBinary, binary: node.Op, left: left, right: right,
		}, nil

	default:
		return compiler.compileLeaf(expression)
	}
}

func (compiler *arithmeticTreeCompiler) compileLeaf(
	expression plan.ScalarExpression,
) (*arithmeticSQLNode, error) {
	if nilScalarExpression(expression) {
		return nil, errors.New("compile ClickHouse arithmetic: missing operand")
	}
	value, err := compileScalarValue(expression, compiler.state)
	if err != nil {
		return nil, err
	}
	value, err = normalizeArithmeticOperand(value, expression.SourceRange())
	if err != nil {
		return nil, err
	}
	name := "__os_arithmetic_operand_" + strconv.Itoa(len(compiler.parameters)+1)
	if compiler.singleUnaryRoot && len(compiler.parameters) == 0 {
		// Preserve the compact historical unary lowering spelling used by the
		// compiler's signed-literal regression fixtures.
		name = "__os_arithmetic_operand"
	}
	compiler.parameters = append(compiler.parameters, name)
	compiler.values = append(compiler.values, value.valueSQL)
	compiler.args = append(compiler.args, value.valueArgs...)
	compiler.alwaysNull = compiler.alwaysNull || value.alwaysNull
	compiler.materializeForPredicate = compiler.materializeForPredicate || value.materializeForPredicate
	return &arithmeticSQLNode{
		kind: arithmeticSQLLeaf, name: name, operand: len(compiler.parameters),
	}, nil
}

func isScalarUnaryExpression(expression plan.ScalarExpression) bool {
	_, ok := expression.(*plan.ScalarUnaryExpression)
	return ok
}

func arithmeticCompiledScalar(
	valueSQL string,
	valueArgs []any,
	alwaysNull bool,
	materializeForPredicate bool,
) compiledScalar {
	return compiledScalar{
		valueSQL:                valueSQL,
		valueArgs:               append([]any(nil), valueArgs...),
		maxStringBytes:          128,
		existsSQL:               "1",
		kind:                    fieldKindNumber,
		numberType:              "Float64",
		alwaysNull:              alwaysNull,
		comparisonAtomic:        false,
		ieeeComparison:          true,
		materializeForPredicate: materializeForPredicate,
	}
}

func normalizeArithmeticOperand(
	input compiledScalar,
	sourceRange spl.Range,
) (compiledScalar, error) {
	if input.ieeeComparison && input.kind == fieldKindNumber &&
		input.numberType == "Float64" && input.existsSQL == "1" {
		return input, nil
	}
	if compiledScalarIsAlwaysNull(input) {
		return arithmeticCompiledScalar(
			"accurateCastOrNull("+input.valueSQL+", 'Float64')",
			input.valueArgs,
			true,
			input.materializeForPredicate,
		), nil
	}

	switch input.kind {
	case fieldKindNumber:
		existsSQL := input.existsSQL
		if existsSQL == "" {
			existsSQL = "1"
		}
		if existsSQL == "1" && len(input.existsArgs) == 0 {
			return arithmeticCompiledScalar(
				"toFloat64("+input.valueSQL+")",
				input.valueArgs,
				input.alwaysNull,
				input.materializeForPredicate,
			), nil
		}
		valueSQL := bindSQLExpressions(
			[]string{"__os_arithmetic_value", "__os_arithmetic_present"},
			[]string{
				input.valueSQL,
				"toUInt8(ifNull(" + existsSQL + ", 0))",
			},
			"if(__os_arithmetic_present != 0, toFloat64(__os_arithmetic_value), "+
				"CAST(NULL AS Nullable(Float64)))",
		)
		args := make([]any, 0, len(input.valueArgs)+len(input.existsArgs))
		args = append(args, input.valueArgs...)
		args = append(args, input.existsArgs...)
		return arithmeticCompiledScalar(
			valueSQL,
			args,
			input.alwaysNull,
			input.materializeForPredicate,
		), nil
	case fieldKindDynamic:
		return normalizeDynamicArithmeticOperand(input), nil
	case fieldKindStringArray, fieldKindDynamicArray:
		return compiledScalar{}, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_ARITHMETIC_VALUE_TYPE",
			Message: "arithmetic requires a scalar numeric value; multivalue arithmetic is not supported",
			Range:   sourceRange,
		}
	case fieldKindString:
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_UNSUPPORTED_ARITHMETIC_VALUE_TYPE",
			Message: "arithmetic does not implicitly convert a fixed String; use tonumber, " +
				"or use period for String concatenation",
			Range: sourceRange,
		}
	case fieldKindTime:
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_UNSUPPORTED_ARITHMETIC_VALUE_TYPE",
			Message: "arithmetic does not implicitly convert time values; use relative_time " +
				"or strptime before arithmetic",
			Range: sourceRange,
		}
	default:
		return compiledScalar{}, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_ARITHMETIC_VALUE_TYPE",
			Message: "arithmetic requires a scalar numeric value",
			Range:   sourceRange,
		}
	}
}

func normalizeDynamicArithmeticOperand(input compiledScalar) compiledScalar {
	value := compiledScalar{
		valueSQL:       "__os_arithmetic_dynamic",
		dynamicTypeSQL: "dynamicType(__os_arithmetic_dynamic)",
		kind:           fieldKindDynamic,
	}
	typeSQL := dynamicScalarTypeSQL(value)
	stringSQL := "dynamicElement(__os_arithmetic_dynamic, 'String')"
	limit := strconv.Itoa(MaximumArithmeticDynamicStringBytes)
	boundedString := "if(length(" + stringSQL + ") <= " + limit + ", " +
		stringSQL + ", CAST('' AS String))"
	validString := "(" + typeSQL + " = 'String' AND length(" + stringSQL +
		") <= " + limit + " AND isValidUTF8(" + boundedString + ") AND match(" +
		boundedString + ", " + decimalNumericStringPattern + "))"
	stringValue := "toFloat64OrNull(" + canonicalNumericTextSQL(boundedString) + ")"

	validDecimal, decimalPayload := dynamicTaggedDecimalTextWithLimit(
		value,
		MaximumArithmeticDynamicStringBytes,
	)
	decimalValue := "toFloat64OrNull(" + canonicalNumericTextSQL(decimalPayload) + ")"
	malformedSemantic := dynamicMalformedSemanticScalarConditionSQL(value)
	unsupported := "CAST(throwIf(toUInt8(__os_arithmetic_malformed), '" +
		UnsupportedExpressionValueMarker + "') AS Nullable(Float64))"
	body := "multiIf(" +
		dynamicNumericTypePredicate(typeSQL) + ", accurateCastOrNull(__os_arithmetic_dynamic, 'Float64'), " +
		validString + ", " + stringValue + ", " +
		validDecimal + ", " + decimalValue + ", " +
		"__os_arithmetic_malformed != 0, " + unsupported + ", " +
		"CAST(NULL AS Nullable(Float64)))"
	body = bindSQLExpressions(
		[]string{"__os_arithmetic_malformed"},
		[]string{"toUInt8(" + malformedSemantic + ")"},
		body,
	)
	existsSQL := input.existsSQL
	if existsSQL == "" {
		existsSQL = "1"
	}
	valueSQL := bindSQLExpressions(
		[]string{"__os_arithmetic_dynamic", "__os_arithmetic_present"},
		[]string{
			input.valueSQL,
			"toUInt8(ifNull(" + existsSQL + ", 0))",
		},
		"if(__os_arithmetic_present != 0, "+body+", "+
			"CAST(NULL AS Nullable(Float64)))",
	)
	args := make([]any, 0, len(input.valueArgs)+len(input.existsArgs))
	args = append(args, input.valueArgs...)
	args = append(args, input.existsArgs...)
	return arithmeticCompiledScalar(
		valueSQL,
		args,
		input.alwaysNull,
		input.materializeForPredicate,
	)
}

func compileMembershipExpression(
	expression *plan.MembershipExpression,
	state compileState,
) (string, []any, error) {
	if expression == nil {
		return "", nil, errors.New("compile ClickHouse membership: missing expression")
	}
	if nilScalarExpression(expression.Value) {
		return "", nil, errors.New("compile ClickHouse membership: missing input value")
	}
	if len(expression.Candidates) < 1 ||
		len(expression.Candidates) > spl.MaximumMembershipCandidates {
		return "", nil, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"membership requires 1 through %d candidates",
				spl.MaximumMembershipCandidates,
			),
			Range: expression.Range,
		}
	}
	if err := chargeCompiledMembershipCandidates(
		state.context,
		len(expression.Candidates),
		expression.Range,
	); err != nil {
		return "", nil, err
	}

	left, err := compileMembershipOperand(expression.Value, state)
	if err != nil {
		return "", nil, err
	}
	candidates := make([]compiledScalar, 0, len(expression.Candidates))
	for _, candidateExpression := range expression.Candidates {
		if nilScalarExpression(candidateExpression) {
			return "", nil, errors.New("compile ClickHouse membership: missing candidate")
		}
		candidate, compileErr := compileMembershipOperand(candidateExpression, state)
		if compileErr != nil {
			return "", nil, compileErr
		}
		candidates = append(candidates, candidate)
	}

	parameters := make([]string, 0, 2+len(candidates)*2)
	values := make([]string, 0, cap(parameters))
	args := make([]any, 0)
	parameters = append(parameters, "__os_membership_left", "__os_membership_left_present")
	values = append(values, left.valueSQL, membershipExistsSQL(left))
	args = append(args, left.valueArgs...)
	args = append(args, left.existsArgs...)

	boundLeft := bindCompiledScalarForComparison(
		left,
		"__os_membership_left",
		"__os_membership_left_present",
	)
	derivedLeft := compiledScalar{}
	if boundLeft.kind == fieldKindDynamic {
		// Equality with numeric literals otherwise expands the bounded exact-
		// numeric classifier once per candidate. Materialize the left type,
		// eligibility, and ordering key inside the already-bound left-value
		// lambda so a 32-candidate list remains linear and the authored left
		// occurrence is classified exactly once.
		derivedLeft = boundLeft
		boundLeft.dynamicTypeSQL = "__os_membership_left_type"
		boundLeft.dynamicNumericEligibleSQL = "__os_membership_left_numeric"
		boundLeft.exactNumericKeySQL = "__os_membership_left_key"
	}
	comparisonParameters := make([]string, 0, len(candidates))
	comparisonValues := make([]string, 0, len(candidates))
	for index, candidate := range candidates {
		valueName := fmt.Sprintf("__os_membership_candidate_%d", index+1)
		presentName := fmt.Sprintf("__os_membership_candidate_%d_present", index+1)
		parameters = append(parameters, valueName, presentName)
		values = append(values, candidate.valueSQL, membershipExistsSQL(candidate))
		args = append(args, candidate.valueArgs...)
		args = append(args, candidate.existsArgs...)

		boundCandidate := bindCompiledScalarForComparison(
			candidate,
			valueName,
			presentName,
		)
		core, coreArgs := evalComparisonCore(boundLeft, boundCandidate, "=")
		if len(coreArgs) != 0 {
			return "", nil, errors.New(
				"compile ClickHouse membership: bound comparison retained arguments",
			)
		}
		comparisonName := fmt.Sprintf("__os_membership_equal_%d", index+1)
		comparisonParameters = append(comparisonParameters, comparisonName)
		comparisonValues = append(
			comparisonValues,
			"if((__os_membership_left_present != 0) AND ("+presentName+
				" != 0), "+core+", CAST(NULL AS Nullable(Bool)))",
		)
	}

	trueTerms := make([]string, 0, len(comparisonParameters))
	nullTerms := make([]string, 0, len(comparisonParameters))
	for _, comparison := range comparisonParameters {
		trueTerms = append(trueTerms, "ifNull("+comparison+", 0)")
		nullTerms = append(nullTerms, "isNull("+comparison+")")
	}
	result := "multiIf((" + strings.Join(trueTerms, " OR ") +
		"), CAST(1 AS Nullable(Bool)), (" + strings.Join(nullTerms, " OR ") +
		"), CAST(NULL AS Nullable(Bool)), CAST(0 AS Nullable(Bool)))"
	if expression.Negated {
		result = bindSQLExpressions(
			[]string{"__os_membership_result"},
			[]string{result},
			"if(isNull(__os_membership_result), CAST(NULL AS Nullable(Bool)), "+
				"NOT (__os_membership_result))",
		)
	}
	result = bindSQLExpressions(comparisonParameters, comparisonValues, result)
	if derivedLeft.kind == fieldKindDynamic {
		// String/Bool-only candidate lists need only the Dynamic type. Bind the
		// more expensive numeric eligibility and exact-order key expressions
		// only when a generated comparison actually references their aliases.
		// Constructing the exact key is intentionally lazy too: its SQL is large
		// even before ClickHouse evaluates it.
		derivedParameters := make([]string, 0, 3)
		derivedValues := make([]string, 0, 3)
		if strings.Contains(result, "__os_membership_left_type") {
			derivedParameters = append(derivedParameters, "__os_membership_left_type")
			derivedValues = append(derivedValues, dynamicScalarTypeSQL(derivedLeft))
		}
		if strings.Contains(result, "__os_membership_left_numeric") {
			derivedParameters = append(derivedParameters, "__os_membership_left_numeric")
			derivedValues = append(
				derivedValues,
				"toUInt8("+dynamicNumericValuePredicate(derivedLeft)+")",
			)
		}
		if strings.Contains(result, "__os_membership_left_key") {
			derivedParameters = append(derivedParameters, "__os_membership_left_key")
			derivedValues = append(derivedValues, exactNumericScalarKeySQL(derivedLeft))
		}
		if len(derivedParameters) != 0 {
			result = bindSQLExpressions(derivedParameters, derivedValues, result)
		}
	}
	result = bindSQLExpressions(parameters, values, result)
	if err := validateExpressionNodeSQLBytes(
		result,
		"membership",
		expression.Range,
	); err != nil {
		return "", nil, err
	}
	return result, args, nil
}

func validateMembershipStructure(
	consumer string,
	expression *plan.MembershipExpression,
) error {
	if expression == nil {
		return fmt.Errorf("compile ClickHouse %s: missing membership expression", consumer)
	}
	if nilScalarExpression(expression.Value) {
		return fmt.Errorf("compile ClickHouse %s: membership input is missing", consumer)
	}
	if len(expression.Candidates) < 1 ||
		len(expression.Candidates) > spl.MaximumMembershipCandidates {
		return fmt.Errorf(
			"compile ClickHouse %s: membership requires 1 through %d candidates",
			consumer,
			spl.MaximumMembershipCandidates,
		)
	}
	if err := validatePredicateScalarStructure(expression.Value); err != nil {
		return err
	}
	for _, candidate := range expression.Candidates {
		if nilScalarExpression(candidate) {
			return fmt.Errorf("compile ClickHouse %s: membership candidate is missing", consumer)
		}
		if err := validatePredicateScalarStructure(candidate); err != nil {
			return err
		}
	}
	return nil
}

func compileMembershipOperand(
	expression plan.ScalarExpression,
	state compileState,
) (compiledScalar, error) {
	value, err := compileScalarValue(expression, state)
	if err != nil {
		return compiledScalar{}, err
	}
	if isNativeMultivalueKind(value.kind) {
		return compiledScalar{}, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_MEMBERSHIP_VALUE_TYPE",
			Message: "membership requires scalar values; multivalue membership is not supported",
			Range:   expression.SourceRange(),
		}
	}
	if value.kind != fieldKindDynamic {
		return value, nil
	}

	bound := compiledScalar{
		valueSQL:       "__os_membership_semantic",
		dynamicTypeSQL: "dynamicType(__os_membership_semantic)",
		kind:           fieldKindDynamic,
	}
	malformed := dynamicMalformedSemanticScalarConditionSQL(bound)
	guardBody := bindSQLExpressions(
		[]string{"__os_membership_malformed"},
		[]string{"toUInt8(" + malformed + ")"},
		"if(__os_membership_malformed != 0, CAST(throwIf("+
			"toUInt8(__os_membership_malformed), '"+
			UnsupportedExpressionValueMarker+"') AS Dynamic), "+
			"__os_membership_semantic)",
	)
	guarded := bindSQLExpressions(
		[]string{"__os_membership_semantic"},
		[]string{value.valueSQL},
		guardBody,
	)
	value.valueSQL = guarded
	value.dynamicTypeSQL = ""
	value.exactNumericKeySQL = ""
	value.dynamicNumericEligibleSQL = ""
	value.comparisonAtomic = false
	return value, nil
}

func membershipExistsSQL(value compiledScalar) string {
	existsSQL := value.existsSQL
	if existsSQL == "" {
		existsSQL = "1"
	}
	return "toUInt8(ifNull(" + existsSQL + ", 0))"
}

func dynamicMalformedSemanticScalarConditionSQL(value compiledScalar) string {
	typeKey := "concat(char(0), 'open_splunk_type')"
	mapSQL := dynamicTaggedMapSQL(value)
	tagSQL := mapSQL + "[" + typeKey + "]"
	recognized := "(" + dynamicScalarTypeSQL(value) + " = 'Map(String, String)'" +
		" AND mapContains(" + mapSQL + ", " + typeKey + ")" +
		" AND (" + tagSQL + " = 'bytes/v1' OR " +
		tagSQL + " = 'timestamp/v1' OR " +
		tagSQL + " = 'duration/v1' OR " +
		tagSQL + " = 'decimal/v1'))"
	validEnvelope := dynamicTaggedScalarEnvelopeCondition(value)
	validDecimal, _ := dynamicTaggedDecimalTextWithLimit(
		value,
		MaximumArithmeticDynamicStringBytes,
	)
	decimal := dynamicTaggedEnvelopeCondition(value, "decimal/v1")
	valid := "((" + validEnvelope + " AND NOT (" + decimal + ")) OR (" +
		validDecimal + "))"
	return "((" + recognized + ") AND NOT (" + valid + "))"
}

func validateExpressionNodeSQLBytes(
	valueSQL string,
	operation string,
	sourceRange spl.Range,
) error {
	if len(valueSQL) <= maxCompiledExpressionNodeSQLBytes {
		return nil
	}
	return &plan.Diagnostic{
		Code: "SPL_QUERY_TOO_COMPLEX",
		Message: fmt.Sprintf(
			"%s compiled node exceeds %d bytes",
			operation,
			maxCompiledExpressionNodeSQLBytes,
		),
		Range: sourceRange,
	}
}

func chargeCompiledArithmeticOperator(
	context *compileContext,
	sourceRange spl.Range,
) error {
	if context == nil {
		return errors.New("compile ClickHouse arithmetic: query context is required")
	}
	if context.arithmeticOperators >= spl.MaximumArithmeticOperatorsPerQuery {
		return &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"search contains more than %d arithmetic operators",
				spl.MaximumArithmeticOperatorsPerQuery,
			),
			Range: sourceRange,
		}
	}
	context.arithmeticOperators++
	context.atomicResult = true
	return nil
}

func chargeCompiledMembershipCandidates(
	context *compileContext,
	count int,
	sourceRange spl.Range,
) error {
	if context == nil {
		return errors.New("compile ClickHouse membership: query context is required")
	}
	if count < 1 || count > spl.MaximumMembershipCandidates ||
		context.membershipCandidates > spl.MaximumMembershipCandidatesPerQuery-count {
		return &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"search contains more than %d membership candidates",
				spl.MaximumMembershipCandidatesPerQuery,
			),
			Range: sourceRange,
		}
	}
	context.membershipCandidates += count
	context.atomicResult = true
	return nil
}

func validCompiledScalarUnaryOp(op plan.ScalarUnaryOp) bool {
	return op == plan.ScalarUnaryOpPositive || op == plan.ScalarUnaryOpNegative
}

func validCompiledScalarBinaryOp(op plan.ScalarBinaryOp) bool {
	switch op {
	case plan.ScalarBinaryOpMultiply,
		plan.ScalarBinaryOpDivide,
		plan.ScalarBinaryOpRemainder,
		plan.ScalarBinaryOpAdd,
		plan.ScalarBinaryOpSubtract:
		return true
	default:
		return false
	}
}

func compiledUnaryArithmeticChainLength(expression *plan.ScalarUnaryExpression) int {
	length := 0
	current := expression
	for current != nil && length <= spl.MaximumUnaryOperatorChain {
		length++
		next, ok := current.Operand.(*plan.ScalarUnaryExpression)
		if !ok {
			break
		}
		current = next
	}
	return length
}
