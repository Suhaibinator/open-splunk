package plan

import (
	"errors"
	"fmt"
	"slices"

	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"github.com/Suhaibinator/open-splunk/internal/searchtimebounds"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
	"github.com/Suhaibinator/open-splunk/internal/splrelativetime"
	"github.com/Suhaibinator/open-splunk/internal/spltimeformat"
	"github.com/Suhaibinator/open-splunk/internal/splwildcard"
)

// scalarFunctionSpec is the single source for one supported eval function's
// diagnostic name, plan counterpart and fixed arity.
type scalarFunctionSpec struct {
	name       string
	plan       ScalarFunction
	arguments  int
	exactArity bool
}

// scalarFunctionSpecs enumerates every supported spl scalar function. Functions
// with variable arity carry no exactArity and validate their operand counts
// individually in convertScalarExpressionUnchecked.
var scalarFunctionSpecs = map[spl.ScalarFunction]scalarFunctionSpec{
	spl.ScalarFunctionConcat:       {name: "concatenation", plan: ScalarFunctionConcat},
	spl.ScalarFunctionNow:          {name: "now", plan: ScalarFunctionNow, arguments: 0, exactArity: true},
	spl.ScalarFunctionStrftime:     {name: "strftime", plan: ScalarFunctionStrftime, arguments: 2, exactArity: true},
	spl.ScalarFunctionStrptime:     {name: "strptime", plan: ScalarFunctionStrptime, arguments: 2, exactArity: true},
	spl.ScalarFunctionRelativeTime: {name: "relative_time", plan: ScalarFunctionRelativeTime, arguments: 2, exactArity: true},
	spl.ScalarFunctionToNumber:     {name: "tonumber", plan: ScalarFunctionToNumber, arguments: 1, exactArity: true},
	spl.ScalarFunctionToString:     {name: "tostring", plan: ScalarFunctionToString},
	spl.ScalarFunctionRound:        {name: "round", plan: ScalarFunctionRound},
	spl.ScalarFunctionCeil:         {name: "ceil", plan: ScalarFunctionCeil, arguments: 1, exactArity: true},
	spl.ScalarFunctionFloor:        {name: "floor", plan: ScalarFunctionFloor, arguments: 1, exactArity: true},
	spl.ScalarFunctionMVCount:      {name: "mvcount", plan: ScalarFunctionMVCount, arguments: 1, exactArity: true},
	spl.ScalarFunctionMVSort:       {name: "mvsort", plan: ScalarFunctionMVSort, arguments: 1, exactArity: true},
	spl.ScalarFunctionMatch:        {name: "match", plan: ScalarFunctionMatch, arguments: 2, exactArity: true},
	spl.ScalarFunctionLike:         {name: "like", plan: ScalarFunctionLike, arguments: 2, exactArity: true},
	spl.ScalarFunctionReplace:      {name: "replace", plan: ScalarFunctionReplace, arguments: 3, exactArity: true},
	spl.ScalarFunctionIsNull:       {name: "isnull", plan: ScalarFunctionIsNull, arguments: 1, exactArity: true},
	spl.ScalarFunctionIsNotNull:    {name: "isnotnull", plan: ScalarFunctionIsNotNull, arguments: 1, exactArity: true},
	spl.ScalarFunctionCoalesce:     {name: "coalesce", plan: ScalarFunctionCoalesce},
	spl.ScalarFunctionLower:        {name: "lower", plan: ScalarFunctionLower, arguments: 1, exactArity: true},
	spl.ScalarFunctionUpper:        {name: "upper", plan: ScalarFunctionUpper, arguments: 1, exactArity: true},
	spl.ScalarFunctionLength:       {name: "len", plan: ScalarFunctionLength, arguments: 1, exactArity: true},
	spl.ScalarFunctionSubstring:    {name: "substr", plan: ScalarFunctionSubstring},
	spl.ScalarFunctionSplit:        {name: "split", plan: ScalarFunctionSplit, arguments: 2, exactArity: true},
	spl.ScalarFunctionMVAppend:     {name: "mvappend", plan: ScalarFunctionMVAppend},
	spl.ScalarFunctionMVDedup:      {name: "mvdedup", plan: ScalarFunctionMVDedup, arguments: 1, exactArity: true},
	spl.ScalarFunctionMVIndex:      {name: "mvindex", plan: ScalarFunctionMVIndex},
	spl.ScalarFunctionMVJoin:       {name: "mvjoin", plan: ScalarFunctionMVJoin, arguments: 2, exactArity: true},
	spl.ScalarFunctionMVZip:        {name: "mvzip", plan: ScalarFunctionMVZip},
	spl.ScalarFunctionMVFind:       {name: "mvfind", plan: ScalarFunctionMVFind, arguments: 2, exactArity: true},
	spl.ScalarFunctionAbs:          {name: "abs", plan: ScalarFunctionAbs, arguments: 1, exactArity: true},
	spl.ScalarFunctionSqrt:         {name: "sqrt", plan: ScalarFunctionSqrt, arguments: 1, exactArity: true},
	spl.ScalarFunctionExp:          {name: "exp", plan: ScalarFunctionExp, arguments: 1, exactArity: true},
	spl.ScalarFunctionLn:           {name: "ln", plan: ScalarFunctionLn, arguments: 1, exactArity: true},
	spl.ScalarFunctionLog:          {name: "log", plan: ScalarFunctionLog},
	spl.ScalarFunctionPow:          {name: "pow", plan: ScalarFunctionPow, arguments: 2, exactArity: true},
	spl.ScalarFunctionPi:           {name: "pi", plan: ScalarFunctionPi, arguments: 0, exactArity: true},
	spl.ScalarFunctionTrim:         {name: "trim", plan: ScalarFunctionTrim},
	spl.ScalarFunctionLTrim:        {name: "ltrim", plan: ScalarFunctionLTrim},
	spl.ScalarFunctionRTrim:        {name: "rtrim", plan: ScalarFunctionRTrim},
	spl.ScalarFunctionURLDecode:    {name: "urldecode", plan: ScalarFunctionURLDecode, arguments: 1, exactArity: true},
	spl.ScalarFunctionMD5:          {name: "md5", plan: ScalarFunctionMD5, arguments: 1, exactArity: true},
	spl.ScalarFunctionSHA1:         {name: "sha1", plan: ScalarFunctionSHA1, arguments: 1, exactArity: true},
	spl.ScalarFunctionSHA256:       {name: "sha256", plan: ScalarFunctionSHA256, arguments: 1, exactArity: true},
	spl.ScalarFunctionSHA512:       {name: "sha512", plan: ScalarFunctionSHA512, arguments: 1, exactArity: true},
	spl.ScalarFunctionTypeOf:       {name: "typeof", plan: ScalarFunctionTypeOf, arguments: 1, exactArity: true},
	spl.ScalarFunctionCIDRMatch:    {name: "cidrmatch", plan: ScalarFunctionCIDRMatch, arguments: 2, exactArity: true},
}

func convertScalarExpressionUnchecked(expression spl.ScalarExpr) (ScalarExpression, error) {
	if expression == nil {
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
			Message: "scalar expression is missing",
		}
	}
	switch expression := expression.(type) {
	case *spl.ScalarFieldExpr:
		if expression == nil {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "scalar field expression is missing",
			}
		}
		var (
			field FieldRef
			err   error
		)
		if expression.Quoted {
			field, err = ResolveQuotedField(expression.Field, expression.Range)
		} else {
			field, err = ResolveField(expression.Field, expression.Range)
		}
		if err != nil {
			return nil, err
		}
		return &ScalarFieldExpression{Field: field, Range: expression.Range}, nil
	case *spl.ScalarLiteralExpr:
		if expression == nil {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "scalar literal expression is missing",
			}
		}
		value, err := convertValue(expression.Value)
		if err != nil {
			return nil, err
		}
		return &ScalarLiteralExpression{Value: value, Range: expression.Range}, nil
	case *spl.ScalarUnaryExpr:
		if expression == nil {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_ARITHMETIC_SYNTAX",
				Message: "unary arithmetic expression is missing",
			}
		}
		if !validExpressionRangeOrZero(expression.Range) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_ARITHMETIC_SYNTAX",
				Message: "unary arithmetic expression has an invalid source range",
				Range:   expression.Range,
			}
		}
		op := convertScalarUnaryOp(expression.Op)
		if op == ScalarUnaryOpInvalid {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_ARITHMETIC_SYNTAX",
				Message: "unary arithmetic expression has an invalid operator",
				Range:   expression.Range,
			}
		}
		if splScalarHasStaticallyUnsupportedArithmeticType(expression.Operand) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_ARITHMETIC_VALUE_TYPE",
				Message: "arithmetic cannot consume the statically known operand type",
				Range:   splScalarExpressionRange(expression.Operand, expression.Range),
			}
		}
		operand, err := convertScalarExpressionUnchecked(expression.Operand)
		if err != nil {
			return nil, err
		}
		return &ScalarUnaryExpression{
			Op:      op,
			Operand: operand,
			Range:   expression.Range,
		}, nil
	case *spl.ScalarBinaryExpr:
		if expression == nil {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_ARITHMETIC_SYNTAX",
				Message: "binary arithmetic expression is missing",
			}
		}
		if !validExpressionRangeOrZero(expression.Range) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_ARITHMETIC_SYNTAX",
				Message: "binary arithmetic expression has an invalid source range",
				Range:   expression.Range,
			}
		}
		op := convertScalarBinaryOp(expression.Op)
		if op == ScalarBinaryOpInvalid {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_ARITHMETIC_SYNTAX",
				Message: "binary arithmetic expression has an invalid operator",
				Range:   expression.Range,
			}
		}
		for _, operand := range []spl.ScalarExpr{expression.Left, expression.Right} {
			if splScalarHasStaticallyUnsupportedArithmeticType(operand) {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_ARITHMETIC_VALUE_TYPE",
					Message: "arithmetic cannot consume the statically known operand type",
					Range:   splScalarExpressionRange(operand, expression.Range),
				}
			}
		}
		left, err := convertScalarExpressionUnchecked(expression.Left)
		if err != nil {
			return nil, err
		}
		right, err := convertScalarExpressionUnchecked(expression.Right)
		if err != nil {
			return nil, err
		}
		return &ScalarBinaryExpression{
			Op:    op,
			Left:  left,
			Right: right,
			Range: expression.Range,
		}, nil
	case *spl.ScalarCallExpr:
		if expression == nil {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "scalar call expression is missing",
			}
		}
		spec, specKnown := scalarFunctionSpecs[expression.Function]
		expectedArguments := spec.arguments
		hasExactArity := spec.exactArity
		functionName := spec.name
		switch expression.Function {
		case spl.ScalarFunctionConcat:
			if len(expression.Arguments) < 2 {
				return nil, &Diagnostic{
					Code:    "SPL_INVALID_EVAL_ARITY",
					Message: "concatenation requires at least two operands",
					Range:   expression.Range,
				}
			}
			if len(expression.Arguments) > spl.MaximumConcatenationOperands {
				return nil, &Diagnostic{
					Code: "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf(
						"concatenation contains more than %d operands",
						spl.MaximumConcatenationOperands,
					),
					Range: expression.Range,
				}
			}
		case spl.ScalarFunctionRound:
			if len(expression.Arguments) < 1 || len(expression.Arguments) > 2 {
				return nil, &Diagnostic{
					Code:    "SPL_INVALID_EVAL_ARITY",
					Message: "round requires one or two arguments",
					Range:   expression.Range,
				}
			}
		case spl.ScalarFunctionCoalesce:
			if len(expression.Arguments) == 0 {
				return nil, &Diagnostic{
					Code:    "SPL_INVALID_EVAL_ARITY",
					Message: "coalesce requires at least one argument",
					Range:   expression.Range,
				}
			}
			if len(expression.Arguments) > spl.MaximumCoalesceArguments {
				return nil, &Diagnostic{
					Code: "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf(
						"coalesce contains more than %d arguments",
						spl.MaximumCoalesceArguments,
					),
					Range: expression.Range,
				}
			}
		case spl.ScalarFunctionSubstring:
			if len(expression.Arguments) < 2 || len(expression.Arguments) > 3 {
				return nil, &Diagnostic{
					Code:    "SPL_INVALID_EVAL_ARITY",
					Message: "substr requires two or three arguments",
					Range:   expression.Range,
				}
			}
		case spl.ScalarFunctionMVAppend:
			if len(expression.Arguments) == 0 {
				return nil, &Diagnostic{
					Code:    "SPL_INVALID_EVAL_ARITY",
					Message: "mvappend requires at least one argument",
					Range:   expression.Range,
				}
			}
			if len(expression.Arguments) > spl.MaximumMVAppendArguments {
				return nil, &Diagnostic{
					Code: "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf(
						"mvappend contains more than %d arguments",
						spl.MaximumMVAppendArguments,
					),
					Range: expression.Range,
				}
			}
		case spl.ScalarFunctionMVIndex:
			if len(expression.Arguments) < 2 || len(expression.Arguments) > 3 {
				return nil, &Diagnostic{
					Code:    "SPL_INVALID_EVAL_ARITY",
					Message: "mvindex requires two or three arguments",
					Range:   expression.Range,
				}
			}
		case spl.ScalarFunctionMVZip:
			if len(expression.Arguments) < 2 || len(expression.Arguments) > 3 {
				return nil, &Diagnostic{
					Code:    "SPL_INVALID_EVAL_ARITY",
					Message: "mvzip requires two or three arguments",
					Range:   expression.Range,
				}
			}
		case spl.ScalarFunctionToString,
			spl.ScalarFunctionLog,
			spl.ScalarFunctionTrim,
			spl.ScalarFunctionLTrim,
			spl.ScalarFunctionRTrim:
			if len(expression.Arguments) < 1 || len(expression.Arguments) > 2 {
				return nil, &Diagnostic{
					Code:    "SPL_INVALID_EVAL_ARITY",
					Message: functionName + " requires one or two arguments",
					Range:   expression.Range,
				}
			}
		}
		if hasExactArity && len(expression.Arguments) != expectedArguments {
			argumentNoun := "arguments"
			switch expectedArguments {
			case 0:
				return nil, &Diagnostic{
					Code:    "SPL_INVALID_EVAL_ARITY",
					Message: functionName + " requires no arguments",
					Range:   expression.Range,
				}
			case 1:
				argumentNoun = "argument"
			}
			return nil, &Diagnostic{
				Code: "SPL_INVALID_EVAL_ARITY",
				Message: fmt.Sprintf(
					"%s requires exactly %d %s",
					functionName,
					expectedArguments,
					argumentNoun,
				),
				Range: expression.Range,
			}
		}
		if expression.Function == spl.ScalarFunctionToNumber ||
			expression.Function == spl.ScalarFunctionReplace ||
			expression.Function == spl.ScalarFunctionLower ||
			expression.Function == spl.ScalarFunctionUpper ||
			expression.Function == spl.ScalarFunctionMVSort ||
			expression.Function == spl.ScalarFunctionLength ||
			expression.Function == spl.ScalarFunctionTrim ||
			expression.Function == spl.ScalarFunctionLTrim ||
			expression.Function == spl.ScalarFunctionRTrim ||
			expression.Function == spl.ScalarFunctionURLDecode ||
			expression.Function == spl.ScalarFunctionMD5 ||
			expression.Function == spl.ScalarFunctionSHA1 ||
			expression.Function == spl.ScalarFunctionSHA256 ||
			expression.Function == spl.ScalarFunctionSHA512 {
			for _, argument := range expression.Arguments {
				if splScalarMayReturnBooleanFunction(argument) {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
						Message: "search-mode scalar functions cannot consume a Boolean result",
						Range:   argument.SourceRange(),
					}
				}
			}
		}
		if expression.Function == spl.ScalarFunctionConcat {
			for _, argument := range expression.Arguments {
				if nilcheck.IsNil(argument) {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
						Message: "concatenation operand is missing",
						Range:   expression.Range,
					}
				}
				if splScalarMayReturnBooleanValue(argument) {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
						Message: "concatenation cannot consume a Boolean result",
						Range:   argument.SourceRange(),
					}
				}
			}
		}
		switch expression.Function {
		case spl.ScalarFunctionRound,
			spl.ScalarFunctionCeil,
			spl.ScalarFunctionFloor:
			if splScalarMayReturnBooleanFunction(expression.Arguments[0]) {
				numericFunctionName := ""
				switch expression.Function {
				case spl.ScalarFunctionRound:
					numericFunctionName = "round"
				case spl.ScalarFunctionCeil:
					numericFunctionName = "ceil"
				case spl.ScalarFunctionFloor:
					numericFunctionName = "floor"
				}
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: numericFunctionName + " cannot consume a Boolean result",
					Range:   expression.Arguments[0].SourceRange(),
				}
			}
		}
		switch expression.Function {
		case spl.ScalarFunctionAbs,
			spl.ScalarFunctionSqrt,
			spl.ScalarFunctionExp,
			spl.ScalarFunctionLn,
			spl.ScalarFunctionLog,
			spl.ScalarFunctionPow:
			// Math functions share the arithmetic operand model: Boolean and
			// statically non-numeric operands are rejected before lowering so
			// the diagnostic names the operand rather than the SQL shape.
			for _, argument := range expression.Arguments {
				if splScalarMayReturnBooleanFunction(argument) {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
						Message: functionName + " cannot consume a Boolean result",
						Range:   argument.SourceRange(),
					}
				}
				if splScalarHasStaticallyUnsupportedArithmeticType(argument) {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_ARITHMETIC_VALUE_TYPE",
						Message: functionName + " cannot consume the statically known operand type",
						Range:   splScalarExpressionRange(argument, expression.Range),
					}
				}
			}
		case spl.ScalarFunctionCIDRMatch:
			if _, prefixRange, ok := splQuotedStringLiteral(
				expression.Arguments[0],
				expression.Range,
			); !ok {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_CIDR_PREFIX",
					Message: "cidrmatch prefix must be a quoted string literal",
					Range:   prefixRange,
				}
			}
			if err := validateSPLCIDRPrefix(expression.Arguments[0]); err != nil {
				return nil, err
			}
			if splScalarMayReturnBooleanFunction(expression.Arguments[1]) {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: "cidrmatch cannot consume a Boolean result",
					Range:   expression.Arguments[1].SourceRange(),
				}
			}
		case spl.ScalarFunctionToString:
			if len(expression.Arguments) == 2 {
				if err := validateSPLToStringFormat(expression.Arguments[1], expression.Range); err != nil {
					return nil, err
				}
			}
		case spl.ScalarFunctionTrim,
			spl.ScalarFunctionLTrim,
			spl.ScalarFunctionRTrim:
			if len(expression.Arguments) == 2 {
				if err := validateSPLTrimCharacters(
					functionName,
					expression.Arguments[1],
					expression.Range,
				); err != nil {
					return nil, err
				}
			}
		}
		if expression.Function == spl.ScalarFunctionRound {
			if len(expression.Arguments) == 2 &&
				!spl.SupportedRoundPrecision(expression.Arguments[1]) {
				return nil, &Diagnostic{
					Code: "SPL_UNSUPPORTED_ROUND_PRECISION",
					Message: fmt.Sprintf(
						"round precision must be a literal integer from 0 through %d",
						spl.MaximumRoundPrecision,
					),
					Range: expression.Arguments[1].SourceRange(),
				}
			}
		}
		if expression.Function == spl.ScalarFunctionSubstring {
			if splScalarMayReturnBooleanFunction(expression.Arguments[0]) {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: "search-mode scalar functions cannot consume a Boolean result",
					Range:   expression.Arguments[0].SourceRange(),
				}
			}
			for index := 1; index < len(expression.Arguments); index++ {
				if nilcheck.IsNil(expression.Arguments[index]) {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
						Message: "scalar expression is missing",
						Range:   expression.Range,
					}
				}
				literal, ok := expression.Arguments[index].(*spl.ScalarLiteralExpr)
				if !ok || literal == nil ||
					literal.Value.Kind != spl.LiteralKindInteger {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_SUBSTRING_INDEX",
						Message: "substr start and length must be literal integers",
						Range:   expression.Arguments[index].SourceRange(),
					}
				}
			}
		}
		switch expression.Function {
		case spl.ScalarFunctionSplit, spl.ScalarFunctionMVJoin:
			if err := validateSPLMVDelimiter(
				functionName,
				expression.Arguments[1],
				expression.Range,
			); err != nil {
				return nil, err
			}
		case spl.ScalarFunctionMVZip:
			if len(expression.Arguments) == 3 {
				if err := validateSPLMVDelimiter(
					functionName,
					expression.Arguments[2],
					expression.Range,
				); err != nil {
					return nil, err
				}
			}
		}
		if expression.Function == spl.ScalarFunctionMVIndex {
			for index := 1; index < len(expression.Arguments); index++ {
				argument := expression.Arguments[index]
				if nilcheck.IsNil(argument) {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
						Message: "scalar expression is missing",
						Range:   expression.Range,
					}
				}
				literal, ok := argument.(*spl.ScalarLiteralExpr)
				if !ok || literal == nil ||
					literal.Value.Kind != spl.LiteralKindInteger {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_MV_INDEX",
						Message: "mvindex start and end must be literal signed 32-bit integers",
						Range:   argument.SourceRange(),
					}
				}
				if !spl.SupportedMVIndexLiteral(argument) {
					return nil, &Diagnostic{
						Code:    "SPL_NUMBER_OUT_OF_RANGE",
						Message: "mvindex start and end must fit a signed 32-bit integer",
						Range:   literal.Range,
					}
				}
			}
		}
		if expression.Function == spl.ScalarFunctionMVFind {
			pattern, patternRange, ok := splQuotedStringLiteral(
				expression.Arguments[1],
				expression.Range,
			)
			if !ok {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: "mvfind regular expression must be a quoted string literal",
					Range:   patternRange,
				}
			}
			if _, err := splregex.CompileMatchPattern(pattern.Value.Text); err != nil {
				if splregex.IsMatchComplexityError(err) {
					return nil, &Diagnostic{
						Code: "SPL_QUERY_TOO_COMPLEX",
						Message: fmt.Sprintf(
							"mvfind regular expression exceeds the %d-byte or %d-work-unit limit",
							splregex.MaximumMatchPatternBytes,
							splregex.MaximumMatchProgramWorkUnits,
						),
						Range: pattern.Range,
					}
				}
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_REGEX",
					Message: "mvfind regular expression is outside the supported RE2-compatible subset",
					Range:   pattern.Range,
				}
			}
		}
		if expression.Function == spl.ScalarFunctionMatch {
			if splScalarMayReturnBooleanFunction(expression.Arguments[0]) {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: "match cannot consume a Boolean result",
					Range:   expression.Arguments[0].SourceRange(),
				}
			}
			pattern, patternRange, ok := splQuotedStringLiteral(
				expression.Arguments[1],
				expression.Range,
			)
			if !ok {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: "match regular expression must be a quoted string literal",
					Range:   patternRange,
				}
			}
			if _, err := splregex.CompileMatchPattern(pattern.Value.Text); err != nil {
				if splregex.IsMatchComplexityError(err) {
					return nil, &Diagnostic{
						Code: "SPL_QUERY_TOO_COMPLEX",
						Message: fmt.Sprintf(
							"match regular expression exceeds the %d-byte or %d-work-unit limit",
							splregex.MaximumMatchPatternBytes,
							splregex.MaximumMatchProgramWorkUnits,
						),
						Range: pattern.Range,
					}
				}
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_REGEX",
					Message: "match regular expression is outside the supported RE2-compatible subset",
					Range:   pattern.Range,
				}
			}
		}
		if expression.Function == spl.ScalarFunctionLike {
			if splScalarMayReturnBooleanFunction(expression.Arguments[0]) {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: "like cannot consume a Boolean result",
					Range:   expression.Arguments[0].SourceRange(),
				}
			}
			pattern, patternRange, ok := splQuotedStringLiteral(
				expression.Arguments[1],
				expression.Range,
			)
			if !ok {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: "like pattern must be a quoted string literal",
					Range:   patternRange,
				}
			}
			if _, err := splwildcard.CompileLikePattern(pattern.Value.Text); err != nil {
				if splwildcard.IsLikeComplexityError(err) {
					return nil, &Diagnostic{
						Code: "SPL_QUERY_TOO_COMPLEX",
						Message: fmt.Sprintf(
							"like pattern exceeds the %d-byte or %d-work-unit limit",
							splwildcard.MaximumLikePatternBytes,
							splwildcard.MaximumLikePatternWorkUnits,
						),
						Range: pattern.Range,
					}
				}
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_LIKE_PATTERN",
					Message: "like pattern must be valid UTF-8 without NUL bytes or an unpaired terminal backslash",
					Range:   pattern.Range,
				}
			}
		}
		if expression.Function == spl.ScalarFunctionStrftime {
			if splScalarMayReturnBooleanFunction(expression.Arguments[0]) {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: "strftime cannot consume a Boolean time value",
					Range:   expression.Arguments[0].SourceRange(),
				}
			}
			format, formatRange, ok := splQuotedStringLiteral(
				expression.Arguments[1],
				expression.Range,
			)
			if !ok {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: "strftime format must be a quoted string literal",
					Range:   formatRange,
				}
			}
			if _, err := spltimeformat.CompileStrftimeFormat(format.Value.Text); err != nil {
				if errors.Is(err, spltimeformat.ErrStrftimeFormatTooLarge) {
					return nil, &Diagnostic{
						Code: "SPL_QUERY_TOO_COMPLEX",
						Message: fmt.Sprintf(
							"strftime format exceeds the %d-byte, %d-work-unit, or %d-output-byte limit",
							spltimeformat.MaximumStrftimeFormatBytes,
							spltimeformat.MaximumStrftimeWorkUnits,
							spltimeformat.MaximumStrftimeOutputBytes,
						),
						Range: format.Range,
					}
				}
				return nil, &Diagnostic{
					Code: "SPL_UNSUPPORTED_TIME_FORMAT",
					Message: "strftime format is outside the supported locale-stable " +
						"date/time variable subset",
					Range: format.Range,
				}
			}
		}
		if expression.Function == spl.ScalarFunctionStrptime {
			if splScalarMayReturnBooleanFunction(expression.Arguments[0]) {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: "strptime cannot consume a Boolean text value",
					Range:   expression.Arguments[0].SourceRange(),
				}
			}
			format, formatRange, ok := splQuotedStringLiteral(
				expression.Arguments[1],
				expression.Range,
			)
			if !ok {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: "strptime format must be a quoted string literal",
					Range:   formatRange,
				}
			}
			if _, err := spltimeformat.CompileStrptimeFormat(format.Value.Text); err != nil {
				if errors.Is(err, spltimeformat.ErrStrptimeFormatTooLarge) {
					return nil, &Diagnostic{
						Code: "SPL_QUERY_TOO_COMPLEX",
						Message: fmt.Sprintf(
							"strptime format exceeds the %d-byte or %d-work-unit limit",
							spltimeformat.MaximumStrptimeFormatBytes,
							spltimeformat.MaximumStrptimeWorkUnits,
						),
						Range: format.Range,
					}
				}
				return nil, &Diagnostic{
					Code: "SPL_UNSUPPORTED_TIME_FORMAT",
					Message: "strptime format is outside the supported deterministic " +
						"full-date parsing subset",
					Range: format.Range,
				}
			}
		}
		if expression.Function == spl.ScalarFunctionRelativeTime {
			if splScalarMayReturnBooleanFunction(expression.Arguments[0]) {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: "relative_time cannot consume a Boolean time value",
					Range:   expression.Arguments[0].SourceRange(),
				}
			}
			specifier, specifierRange, ok := splQuotedStringLiteral(
				expression.Arguments[1],
				expression.Range,
			)
			if !ok {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: "relative_time specifier must be a quoted string literal",
					Range:   specifierRange,
				}
			}
			if _, err := splrelativetime.CompileSpecifier(
				specifier.Value.Text,
			); err != nil {
				if errors.Is(err, splrelativetime.ErrSpecifierTooLarge) {
					return nil, &Diagnostic{
						Code: "SPL_QUERY_TOO_COMPLEX",
						Message: fmt.Sprintf(
							"relative_time specifier exceeds the %d-byte or %d-work-unit limit",
							splrelativetime.MaximumSpecifierBytes,
							splrelativetime.MaximumSpecifierWorkUnits,
						),
						Range: specifier.Range,
					}
				}
				if errors.Is(err, splrelativetime.ErrMagnitudeOutOfRange) {
					return nil, &Diagnostic{
						Code: "SPL_NUMBER_OUT_OF_RANGE",
						Message: "relative_time magnitude exceeds the supported " +
							searchtimebounds.YearRangeDescription + " timestamp span",
						Range: specifier.Range,
					}
				}
				return nil, &Diagnostic{
					Code: "SPL_UNSUPPORTED_RELATIVE_TIME_SPECIFIER",
					Message: "relative_time specifier is outside the supported " +
						"bounded offset-and-snap subset",
					Range: specifier.Range,
				}
			}
		}
		arguments := make([]ScalarExpression, 0, len(expression.Arguments))
		for _, argument := range expression.Arguments {
			converted, err := convertScalarExpressionUnchecked(argument)
			if err != nil {
				return nil, err
			}
			arguments = append(arguments, converted)
		}
		if !specKnown {
			return nil, &Diagnostic{Code: "SPL_UNSUPPORTED_EVAL_FUNCTION", Message: "unsupported scalar function", Range: expression.Range}
		}
		function := spec.plan
		return &ScalarCallExpression{Function: function, Arguments: arguments, Range: expression.Range}, nil
	case *spl.ScalarIfExpr:
		if expression == nil {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "if expression is missing",
			}
		}
		condition, err := convertWhereExpressionUnchecked(expression.Condition)
		if err != nil {
			return nil, err
		}
		trueValue, err := convertScalarExpressionUnchecked(expression.True)
		if err != nil {
			return nil, err
		}
		falseValue, err := convertScalarExpressionUnchecked(expression.False)
		if err != nil {
			return nil, err
		}
		return &ScalarIfExpression{
			Condition: condition,
			True:      trueValue,
			False:     falseValue,
			Range:     expression.Range,
		}, nil
	case *spl.ScalarCaseExpr:
		if expression == nil {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "case expression is missing",
			}
		}
		if len(expression.Branches) == 0 {
			return nil, &Diagnostic{
				Code:    "SPL_INVALID_EVAL_ARITY",
				Message: "case requires one or more condition/value pairs",
				Range:   expression.Range,
			}
		}
		if len(expression.Branches) > spl.MaximumCaseBranches {
			return nil, &Diagnostic{
				Code: "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf(
					"case contains more than %d condition/value pairs",
					spl.MaximumCaseBranches,
				),
				Range: expression.Range,
			}
		}
		branches := make([]ScalarCaseBranch, 0, len(expression.Branches))
		for _, branch := range expression.Branches {
			condition, err := convertWhereExpressionUnchecked(branch.Condition)
			if err != nil {
				return nil, err
			}
			value, err := convertScalarExpressionUnchecked(branch.Value)
			if err != nil {
				return nil, err
			}
			branches = append(branches, ScalarCaseBranch{
				Condition: condition,
				Value:     value,
				Range:     branch.Range,
			})
		}
		return &ScalarCaseExpression{
			Branches: branches,
			Range:    expression.Range,
		}, nil
	default:
		return nil, &Diagnostic{Code: "SPL_UNSUPPORTED_EVAL_EXPRESSION", Message: fmt.Sprintf("unsupported scalar expression type %T", expression), Range: safeSPLNodeRange(expression)}
	}
}

func splScalarMayReturnBooleanFunction(expression spl.ScalarExpr) bool {
	return spl.ScalarExpressionMayReturnBooleanFunction(expression)
}

func splScalarMayReturnBooleanValue(expression spl.ScalarExpr) bool {
	switch expression := expression.(type) {
	case *spl.ScalarFieldExpr, *spl.ScalarUnaryExpr, *spl.ScalarBinaryExpr:
		return false
	case *spl.ScalarLiteralExpr:
		return expression != nil &&
			expression.Value.Kind == spl.LiteralKindBool
	case *spl.ScalarCallExpr:
		if expression == nil {
			return false
		}
		if expression.Function.ReturnsBoolean() {
			return true
		}
		if expression.Function == spl.ScalarFunctionCoalesce {
			if slices.ContainsFunc(expression.Arguments, splScalarMayReturnBooleanValue) {
				return true
			}
		}
		return false
	case *spl.ScalarIfExpr:
		return expression != nil &&
			(splScalarMayReturnBooleanValue(expression.True) ||
				splScalarMayReturnBooleanValue(expression.False))
	case *spl.ScalarCaseExpr:
		if expression == nil {
			return false
		}
		for _, branch := range expression.Branches {
			if splScalarMayReturnBooleanValue(branch.Value) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func splScalarExpressionRange(
	expression spl.ScalarExpr,
	fallback spl.Range,
) spl.Range {
	if nilcheck.IsNil(expression) {
		return fallback
	}
	return expression.SourceRange()
}

func splScalarHasStaticallyUnsupportedArithmeticType(
	expression spl.ScalarExpr,
) bool {
	if nilcheck.IsNil(expression) ||
		splScalarMayReturnBooleanValue(expression) {
		return !nilcheck.IsNil(expression)
	}
	switch expression := expression.(type) {
	case *spl.ScalarFieldExpr:
		// Field type and canonical-time lineage are properties of the current
		// pipeline state. The backend validator owns that evidence so a field
		// overwritten by an earlier eval is not misclassified from its name.
		return false
	case *spl.ScalarLiteralExpr:
		return expression != nil && expression.Value.Kind == spl.LiteralKindString
	case *spl.ScalarCallExpr:
		if expression == nil {
			return false
		}
		switch expression.Function {
		case spl.ScalarFunctionReplace,
			spl.ScalarFunctionLower,
			spl.ScalarFunctionUpper,
			spl.ScalarFunctionMVSort,
			spl.ScalarFunctionSplit,
			spl.ScalarFunctionMVAppend,
			spl.ScalarFunctionMVDedup,
			spl.ScalarFunctionMVJoin,
			spl.ScalarFunctionMVZip,
			spl.ScalarFunctionSubstring,
			spl.ScalarFunctionToString,
			spl.ScalarFunctionStrftime,
			spl.ScalarFunctionConcat,
			spl.ScalarFunctionTrim,
			spl.ScalarFunctionLTrim,
			spl.ScalarFunctionRTrim,
			spl.ScalarFunctionURLDecode,
			spl.ScalarFunctionMD5,
			spl.ScalarFunctionSHA1,
			spl.ScalarFunctionSHA256,
			spl.ScalarFunctionSHA512,
			spl.ScalarFunctionTypeOf:
			return true
		case spl.ScalarFunctionMVIndex:
			return len(expression.Arguments) == 3
		case spl.ScalarFunctionCoalesce:
			foundValue := false
			for _, argument := range expression.Arguments {
				if literal, ok := argument.(*spl.ScalarLiteralExpr); ok &&
					literal != nil && literal.Value.Kind == spl.LiteralKindNull {
					continue
				}
				foundValue = true
				if !splScalarHasStaticallyUnsupportedArithmeticType(argument) {
					return false
				}
			}
			return foundValue
		default:
			return false
		}
	case *spl.ScalarIfExpr:
		return expression != nil &&
			splScalarHasStaticallyUnsupportedArithmeticType(expression.True) &&
			splScalarHasStaticallyUnsupportedArithmeticType(expression.False)
	case *spl.ScalarCaseExpr:
		if expression == nil || len(expression.Branches) == 0 {
			return false
		}
		for _, branch := range expression.Branches {
			if !splScalarHasStaticallyUnsupportedArithmeticType(branch.Value) {
				return false
			}
		}
		return true
	case *spl.ScalarUnaryExpr, *spl.ScalarBinaryExpr:
		return false
	default:
		return false
	}
}

func scalarFunctionReturnsBoolean(expression ScalarExpression) bool {
	switch expression := expression.(type) {
	case *ScalarFieldExpression, *ScalarUnaryExpression, *ScalarBinaryExpression:
		return false
	case *ScalarCallExpression:
		if expression == nil {
			return false
		}
		if expression.Function.ReturnsBoolean() {
			return true
		}
		if expression.Function == ScalarFunctionCoalesce {
			return coalesceScalarExpressionReturnsBoolean(expression.Arguments)
		}
		return false
	case *ScalarLiteralExpression:
		return expression != nil && expression.Value.Kind == ValueKindBool
	case *ScalarIfExpression:
		return expression != nil &&
			scalarFunctionReturnsBoolean(expression.True) &&
			scalarFunctionReturnsBoolean(expression.False)
	case *ScalarCaseExpression:
		return expression != nil &&
			caseScalarFunctionReturnsBoolean(expression.Branches)
	default:
		return false
	}
}

func scalarExpressionCanBeDirectPredicate(expression ScalarExpression) bool {
	switch expression := expression.(type) {
	case *ScalarFieldExpression,
		*ScalarLiteralExpression,
		*ScalarUnaryExpression,
		*ScalarBinaryExpression:
		return false
	case *ScalarCallExpression:
		if expression == nil {
			return false
		}
		if expression.Function.ReturnsBoolean() {
			return true
		}
		if expression.Function == ScalarFunctionCoalesce {
			return coalesceScalarExpressionReturnsBoolean(expression.Arguments)
		}
		return false
	case *ScalarIfExpression:
		return expression != nil && scalarFunctionReturnsBoolean(expression)
	case *ScalarCaseExpression:
		return expression != nil && scalarFunctionReturnsBoolean(expression)
	default:
		return false
	}
}

func coalesceScalarExpressionReturnsBoolean(arguments []ScalarExpression) bool {
	foundBoolean := false
	for _, argument := range arguments {
		if literal, ok := argument.(*ScalarLiteralExpression); ok &&
			literal != nil &&
			literal.Value.Kind == ValueKindNull {
			continue
		}
		if !scalarFunctionReturnsBoolean(argument) {
			return false
		}
		foundBoolean = true
	}
	return foundBoolean
}

func caseScalarFunctionReturnsBoolean(branches []ScalarCaseBranch) bool {
	foundBoolean := false
	for _, branch := range branches {
		if literal, ok := branch.Value.(*ScalarLiteralExpression); ok &&
			literal != nil &&
			literal.Value.Kind == ValueKindNull {
			continue
		}
		if !scalarFunctionReturnsBoolean(branch.Value) {
			return false
		}
		foundBoolean = true
	}
	return foundBoolean
}

func convertComparisonOp(op spl.CompareOp) ComparisonOp {
	switch op {
	case spl.CompareOpEqual:
		return ComparisonOpEqual
	case spl.CompareOpNotEqual:
		return ComparisonOpNotEqual
	case spl.CompareOpLess:
		return ComparisonOpLess
	case spl.CompareOpLessEqual:
		return ComparisonOpLessEqual
	case spl.CompareOpGreater:
		return ComparisonOpGreater
	case spl.CompareOpGreaterEqual:
		return ComparisonOpGreaterEqual
	default:
		return ComparisonOpInvalid
	}
}

func convertScalarUnaryOp(op spl.ScalarUnaryOp) ScalarUnaryOp {
	switch op {
	case spl.ScalarUnaryOpPositive:
		return ScalarUnaryOpPositive
	case spl.ScalarUnaryOpNegative:
		return ScalarUnaryOpNegative
	default:
		return ScalarUnaryOpInvalid
	}
}

func convertScalarBinaryOp(op spl.ScalarBinaryOp) ScalarBinaryOp {
	switch op {
	case spl.ScalarBinaryOpMultiply:
		return ScalarBinaryOpMultiply
	case spl.ScalarBinaryOpDivide:
		return ScalarBinaryOpDivide
	case spl.ScalarBinaryOpRemainder:
		return ScalarBinaryOpRemainder
	case spl.ScalarBinaryOpAdd:
		return ScalarBinaryOpAdd
	case spl.ScalarBinaryOpSubtract:
		return ScalarBinaryOpSubtract
	default:
		return ScalarBinaryOpInvalid
	}
}
