package clickhouse

import (
	"errors"
	"fmt"
	"net/netip"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// textTransform describes one String → String eval function that applies the
// same member expression to a scalar String, to every member of a Splunk
// multivalue Array(String), and to every String member of a native
// Array(Dynamic) list. lower/upper, trim/ltrim/rtrim, urldecode, and the
// digest functions share this lowering so multivalue semantics, native list
// sealing, and the per-expression SQL ceiling are defined exactly once.
type textTransform struct {
	functionName string
	// memberSQL renders the transform over one String operand. Constant
	// parameters (such as trim characters) are inlined as validated literals
	// because the corresponding ClickHouse functions reject bound values.
	memberSQL func(operand string) string
	// resultBytes bounds the transformed value from the input bound and the
	// physical result kind.
	resultBytes         func(inputBound uint64, kind fieldKind) uint64
	unsupportedTypeCode string
	unsupportedMessage  string
	aliasPrefix         string
	maximumSQLBytes     int
}

func compileTextCaseScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	functionName := "lower"
	clickHouseFunction := "lowerUTF8"
	if expression.Function == plan.ScalarFunctionUpper {
		functionName = "upper"
		clickHouseFunction = "upperUTF8"
	}
	input, err := compileUnaryNonBooleanScalarInput(expression, state, functionName)
	if err != nil {
		return compiledScalar{}, err
	}
	return compileTextTransform(expression, input, state, textTransform{
		functionName: functionName,
		memberSQL: func(operand string) string {
			return clickHouseFunction + "(" + operand + ")"
		},
		resultBytes: func(inputBound uint64, _ fieldKind) uint64 {
			// Unicode case conversion can expand UTF-8 bytes.
			return saturatingStringByteProduct(inputBound, 4)
		},
		unsupportedTypeCode: "SPL_UNSUPPORTED_TEXT_CASE_VALUE_TYPE",
		unsupportedMessage:  "%s requires a String or multivalue String input",
		aliasPrefix:         "__os_text_case_mv_",
		maximumSQLBytes:     maxCompiledTextCaseScalarSQLBytes,
	})
}

// defaultTrimCharacters mirrors Splunk, which strips spaces and tabs when no
// character set is given; ClickHouse's one-argument trim strips only spaces.
const defaultTrimCharacters = " \t"

// digestHexBytes is the lowercase hexadecimal length of each digest.
var digestHexBytes = map[plan.ScalarFunction]uint64{
	plan.ScalarFunctionMD5:    32,
	plan.ScalarFunctionSHA1:   40,
	plan.ScalarFunctionSHA256: 64,
	plan.ScalarFunctionSHA512: 128,
}

func textTransformFunctionName(function plan.ScalarFunction) (string, bool) {
	switch function {
	case plan.ScalarFunctionTrim:
		return "trim", true
	case plan.ScalarFunctionLTrim:
		return "ltrim", true
	case plan.ScalarFunctionRTrim:
		return "rtrim", true
	case plan.ScalarFunctionURLDecode:
		return "urldecode", true
	case plan.ScalarFunctionMD5:
		return "md5", true
	case plan.ScalarFunctionSHA1:
		return "sha1", true
	case plan.ScalarFunctionSHA256:
		return "sha256", true
	case plan.ScalarFunctionSHA512:
		return "sha512", true
	default:
		return "", false
	}
}

// compileTextTransformScalar lowers trim/ltrim/rtrim, urldecode, and the
// digest functions through the shared text transform template.
func compileTextTransformScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	if expression == nil {
		return compiledScalar{}, errors.New("compile ClickHouse text transform: missing expression")
	}
	functionName, ok := textTransformFunctionName(expression.Function)
	if !ok {
		return compiledScalar{}, fmt.Errorf(
			"compile ClickHouse text transform: unsupported function %d",
			expression.Function,
		)
	}
	transform := textTransform{
		functionName:        functionName,
		unsupportedTypeCode: "SPL_UNSUPPORTED_TEXT_TRANSFORM_VALUE_TYPE",
		unsupportedMessage:  "%s requires a String or multivalue String input",
		aliasPrefix:         "__os_text_transform_mv_",
		maximumSQLBytes:     maxCompiledTextTransformScalarSQLBytes,
	}
	identityBytes := func(inputBound uint64, _ fieldKind) uint64 { return inputBound }
	switch expression.Function {
	case plan.ScalarFunctionTrim, plan.ScalarFunctionLTrim, plan.ScalarFunctionRTrim:
		if len(expression.Arguments) != 1 && len(expression.Arguments) != 2 {
			return compiledScalar{}, fmt.Errorf(
				"compile ClickHouse %s: expected one or two arguments",
				functionName,
			)
		}
		characters := defaultTrimCharacters
		if len(expression.Arguments) == 2 {
			literal, isLiteral := scalarQuotedStringLiteral(expression.Arguments[1])
			if !isLiteral {
				return compiledScalar{}, fmt.Errorf(
					"compile ClickHouse %s: characters must be a quoted string literal",
					functionName,
				)
			}
			if literal == "" || !utf8.ValidString(literal) {
				return compiledScalar{}, fmt.Errorf(
					"compile ClickHouse %s: characters must be a non-empty valid UTF-8 string",
					functionName,
				)
			}
			if len(literal) > spl.MaximumTrimCharactersBytes {
				return compiledScalar{}, fmt.Errorf(
					"compile ClickHouse %s: characters exceed the %d-byte limit",
					functionName,
					spl.MaximumTrimCharactersBytes,
				)
			}
			characters = literal
		}
		clickHouseFunction := map[plan.ScalarFunction]string{
			plan.ScalarFunctionTrim:  "trimBoth",
			plan.ScalarFunctionLTrim: "trimLeft",
			plan.ScalarFunctionRTrim: "trimRight",
		}[expression.Function]
		// ClickHouse only accepts a constant trim character set, so the
		// validated literal is inlined instead of bound.
		charactersSQL := quoteStringLiteral(characters)
		transform.memberSQL = func(operand string) string {
			return clickHouseFunction + "(" + operand + ", " + charactersSQL + ")"
		}
		transform.resultBytes = identityBytes
	case plan.ScalarFunctionURLDecode:
		if len(expression.Arguments) != 1 {
			return compiledScalar{}, errors.New("compile ClickHouse urldecode: expected one argument")
		}
		transform.memberSQL = func(operand string) string {
			// Percent-decoding can assemble arbitrary bytes. Keep the input
			// unchanged when the decoded text is not valid UTF-8 so every
			// String value stays inside the UTF-8 text contract.
			const alias = "__os_urldecode_value"
			return bindSQLExpressions(
				[]string{alias},
				[]string{operand},
				"if(isValidUTF8(decodeURLComponent("+alias+")), "+
					"decodeURLComponent("+alias+"), "+alias+")",
			)
		}
		transform.resultBytes = identityBytes
	default:
		if len(expression.Arguments) != 1 {
			return compiledScalar{}, fmt.Errorf(
				"compile ClickHouse %s: expected one argument",
				functionName,
			)
		}
		clickHouseFunction := map[plan.ScalarFunction]string{
			plan.ScalarFunctionMD5:    "MD5",
			plan.ScalarFunctionSHA1:   "SHA1",
			plan.ScalarFunctionSHA256: "SHA256",
			plan.ScalarFunctionSHA512: "SHA512",
		}[expression.Function]
		hexBytes := digestHexBytes[expression.Function]
		transform.memberSQL = func(operand string) string {
			return "lower(hex(" + clickHouseFunction + "(" + operand + ")))"
		}
		transform.resultBytes = func(inputBound uint64, kind fieldKind) uint64 {
			if kind == fieldKindString || kind == fieldKindInvalid {
				return hexBytes
			}
			// Lists hash every member, so an empty-string member still
			// produces a full digest. Bound by the native member ceiling.
			return max(
				inputBound,
				saturatingStringByteProduct(hexBytes, uint64(spl.MaximumNativeMVValues)),
			)
		}
	}
	input, err := compileNonBooleanScalarInputArgument(expression.Arguments[0], state, functionName)
	if err != nil {
		return compiledScalar{}, err
	}
	return compileTextTransform(expression, input, state, transform)
}

func compileTextTransform(
	expression *plan.ScalarCallExpression,
	input compiledScalar,
	state compileState,
	transform textTransform,
) (compiledScalar, error) {
	valueSQL := ""
	valueArgs := append([]any(nil), input.valueArgs...)
	existsSQL := "1"
	var existsArgs []any
	presentSQL := ""
	resultKind := fieldKindString
	dynamicDomain := dynamicScalarDomainAny
	requiresRuntimeValidation := input.requiresRuntimeValidation
	switch input.kind {
	case fieldKindDynamic:
		// Dynamic event fields can be either scalar String or Splunk
		// multivalue Array(String). Bind the input once through a single-element
		// higher-order expression so nested calls grow linearly instead of
		// duplicating the complete child SQL in each runtime-type branch.
		valueSQL = "arrayElement(arrayMap(value -> multiIf(" +
			"dynamicType(value) = 'String', " +
			"CAST(" + transform.memberSQL("dynamicElement(value, 'String')") +
			" AS Dynamic), " +
			"dynamicType(value) = 'Array(String)', " +
			"CAST(arrayMap(element -> " + transform.memberSQL("element") +
			", dynamicElement(value, 'Array(String)')) AS Dynamic), " +
			"dynamicType(value) = 'Array(Dynamic)' AND " +
			"arrayAll(element -> dynamicType(element) = 'String', " +
			"dynamicElement(value, 'Array(Dynamic)')), " +
			"CAST(arrayMap(element -> " +
			transform.memberSQL("assumeNotNull(dynamicElement(element, 'String'))") +
			", dynamicElement(value, 'Array(Dynamic)')) AS Dynamic), " +
			"CAST(NULL AS Dynamic)), [" + input.valueSQL + "]), 1)"
		resultKind = fieldKindDynamic
		dynamicDomain = dynamicScalarDomainText
	case fieldKindStringArray:
		// A fixed Array(String) can originate from an aggregate over _raw.
		// Bind it once and validate every member before calling the UTF-8
		// function; invalid arrays become the canonical empty/absent MV value.
		mapped := "arrayMap(element -> " + transform.memberSQL("element") + ", values)"
		body := "if(arrayAll(element -> isValidUTF8(element), values), " +
			mapped + ", CAST([], 'Array(String)'))"
		if input.optionalMultivaluePresentSQL != "" {
			// New native String arrays are sealed and share the 1,000-member /
			// 1-MiB contract. The transform can change UTF-8 byte counts, so
			// validate the mapped payload before publishing it as sealed.
			body = bindSQLExpressions(
				[]string{"mapped"},
				[]string{mapped},
				stringMVLimitsGuardSQL(
					"mapped",
					"arrayExists(element -> NOT isValidUTF8(element), values)",
				),
			)
			existsSQL, existsArgs = scalarExistsSQL(input)
			presentSQL = input.optionalMultivaluePresentSQL
			requiresRuntimeValidation = true
			markNativeMVRuntimeValidation(state)
		}
		valueSQL = "arrayElement(arrayMap(values -> " + body + ", [" +
			input.valueSQL + "]), 1)"
		resultKind = fieldKindStringArray
	case fieldKindDynamicArray:
		normalized, normalizeErr := compileNativeMVState(input, false)
		if normalizeErr != nil {
			return compiledScalar{}, normalizeErr
		}
		stateAlias := transform.aliasPrefix + "state"
		valuesAlias := transform.aliasPrefix + "values"
		mappedAlias := transform.aliasPrefix + "mapped"
		values := "tupleElement(" + stateAlias + ", 1)"
		mapped := "arrayMap(element -> CAST(" +
			transform.memberSQL("assumeNotNull(dynamicElement(element, 'String'))") +
			" AS Dynamic), " + valuesAlias + ")"
		invalid := "tupleElement(" + stateAlias + ", 4) != 0 OR " +
			"arrayExists(element -> dynamicType(element) != 'String', " + valuesAlias + ")"
		body := bindSQLExpressions(
			[]string{mappedAlias},
			[]string{mapped},
			nativeMVPreflightSQL(
				mappedAlias,
				invalid,
				"length("+mappedAlias+")",
				nativeMVArrayPayloadBytesSQL(mappedAlias),
				emptyNativeMVSQL(),
			),
		)
		body = bindSQLExpressions([]string{valuesAlias}, []string{values}, body)
		valueSQL = bindSQLExpressions(
			[]string{stateAlias},
			[]string{normalized.sql},
			body,
		)
		valueArgs = append([]any(nil), normalized.args...)
		existsSQL, presentSQL, existsArgs = nativeMVPreservedStateSQL(input, normalized)
		resultKind = fieldKindDynamicArray
		dynamicDomain = dynamicScalarDomainText
		requiresRuntimeValidation = true
		markNativeMVRuntimeValidation(state)
	case fieldKindString, fieldKindInvalid:
		inputSQL, inputArgs := compiledTextEligibleStringScalar(input)
		valueArgs = inputArgs
		valueSQL = transform.memberSQL(inputSQL)
	default:
		return compiledScalar{}, &plan.Diagnostic{
			Code:    transform.unsupportedTypeCode,
			Message: fmt.Sprintf(transform.unsupportedMessage, transform.functionName),
			Range:   expression.Range,
		}
	}
	if len(valueSQL) > transform.maximumSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"%s scalar SQL exceeds %d bytes",
				transform.functionName,
				transform.maximumSQLBytes,
			),
			Range: expression.Range,
		}
	}
	maxStringBytes := transform.resultBytes(compiledScalarStringByteBound(input), resultKind)
	if isNativeMultivalueKind(resultKind) && presentSQL != "" {
		maxStringBytes = min(maxStringBytes, uint64(spl.MaximumNativeMVPayloadBytes))
	}
	return compiledScalar{
		valueSQL:                     valueSQL,
		valueArgs:                    valueArgs,
		maxStringBytes:               maxStringBytes,
		existsSQL:                    existsSQL,
		existsArgs:                   existsArgs,
		optionalMultivaluePresentSQL: presentSQL,
		dynamicDomain:                dynamicDomain,
		kind:                         resultKind,
		alwaysNull:                   input.alwaysNull,
		materializeForPredicate:      input.materializeForPredicate,
		requiresRuntimeValidation:    requiresRuntimeValidation,
	}, nil
}

// typeOfKindName is the Splunk type name for values whose kind is fixed at
// compile time. Time values are epoch numbers in SPL.
func typeOfKindName(kind fieldKind) (string, bool) {
	switch kind {
	case fieldKindString:
		return "String", true
	case fieldKindNumber, fieldKindTime:
		return "Number", true
	case fieldKindBool:
		return "Boolean", true
	default:
		return "", false
	}
}

// compileTypeOfScalar lowers typeof(x) to Splunk's type names: Number,
// String, Boolean, or Invalid for a missing or null value. Dynamic event
// fields are auto-typed the way Splunk auto-types extracted fields, so numeric
// text and tagged decimals report Number.
func compileTypeOfScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	input, err := compileUnaryScalarInput(expression, state, "typeof")
	if err != nil {
		return compiledScalar{}, err
	}
	result := compiledScalar{
		maxStringBytes:          7,
		existsSQL:               "1",
		kind:                    fieldKindString,
		materializeForPredicate: input.materializeForPredicate,
	}
	if compiledScalarIsAlwaysNull(input) || input.kind == fieldKindInvalid {
		result.valueSQL = "CAST('Invalid' AS String)"
		return result, nil
	}
	if isNativeMultivalueKind(input.kind) {
		return compiledScalar{}, unsupportedMultivalueUsage("typeof", expression.Range)
	}
	if name, ok := typeOfKindName(input.kind); ok {
		presenceSQL, presenceArgs := compiledScalarPresenceSQL(input)
		result.valueSQL = "if(ifNull(" + presenceSQL + ", 0), '" + name + "', 'Invalid')"
		result.valueArgs = presenceArgs
		return result, nil
	}
	if input.kind != fieldKindDynamic {
		return compiledScalar{}, fmt.Errorf(
			"compile ClickHouse typeof: unsupported input kind %d",
			input.kind,
		)
	}
	const (
		valueAlias  = "__os_typeof_value"
		existsAlias = "__os_typeof_exists"
	)
	bound := compiledScalar{
		valueSQL:       valueAlias,
		dynamicTypeSQL: "dynamicType(" + valueAlias + ")",
		kind:           fieldKindDynamic,
	}
	typeSQL := dynamicScalarTypeSQL(bound)
	stringSQL := "dynamicElement(" + valueAlias + ", 'String')"
	limit := fmt.Sprint(MaximumArithmeticDynamicStringBytes)
	boundedString := "if(length(" + stringSQL + ") <= " + limit + ", " +
		stringSQL + ", CAST('' AS String))"
	numericText := "(" + typeSQL + " = 'String' AND length(" + stringSQL +
		") <= " + limit + " AND isValidUTF8(" + boundedString + ") AND match(" +
		boundedString + ", " + decimalNumericStringPattern + "))"
	taggedDecimal, _ := dynamicTaggedDecimalTextWithLimit(bound, MaximumArithmeticDynamicStringBytes)
	body := "multiIf(" +
		existsAlias + " = 0 OR isNull(" + valueAlias + "), 'Invalid', " +
		typeSQL + " = 'Bool', 'Boolean', " +
		dynamicNumericTypePredicate(typeSQL) + " OR " + numericText + " OR " +
		taggedDecimal + ", 'Number', " +
		typeSQL + " = 'String', 'String', " +
		"CAST(NULL AS Nullable(String)))"
	existsSQL := input.existsSQL
	if existsSQL == "" {
		existsSQL = "1"
	}
	result.valueSQL = bindSQLExpressions(
		[]string{valueAlias, existsAlias},
		[]string{input.valueSQL, "toUInt8(ifNull(" + existsSQL + ", 0))"},
		body,
	)
	result.valueArgs = append(append([]any(nil), input.valueArgs...), input.existsArgs...)
	if len(result.valueSQL) > maxCompiledTypeOfScalarSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"typeof scalar SQL exceeds %d bytes",
				maxCompiledTypeOfScalarSQLBytes,
			),
			Range: expression.Range,
		}
	}
	return result, nil
}

// compileCIDRMatchScalar lowers cidrmatch(cidr, ip). The prefix is a validated
// literal bound as a parameter; the address is any scalar text. Text that is
// not a syntactically valid IPv4 or IPv6 address is false rather than an
// execution error, and a null address stays null.
func compileCIDRMatchScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	if expression == nil || len(expression.Arguments) != 2 {
		return compiledScalar{}, errors.New("compile ClickHouse cidrmatch: expected two arguments")
	}
	prefixText, ok := scalarQuotedStringLiteral(expression.Arguments[0])
	if !ok {
		return compiledScalar{}, errors.New("compile ClickHouse cidrmatch: prefix must be a quoted string literal")
	}
	prefix, err := netip.ParsePrefix(prefixText)
	if err != nil {
		return compiledScalar{}, fmt.Errorf("compile ClickHouse cidrmatch: invalid prefix: %w", err)
	}
	input, err := compileNonBooleanScalarInputArgument(expression.Arguments[1], state, "cidrmatch")
	if err != nil {
		return compiledScalar{}, err
	}
	if input.alwaysNull || input.kind == fieldKindInvalid {
		return compiledScalar{
			valueSQL:       "CAST(NULL AS Nullable(Bool))",
			maxStringBytes: 1,
			existsSQL:      "1",
			kind:           fieldKindBool,
			alwaysNull:     true,
		}, nil
	}
	if isNativeMultivalueKind(input.kind) {
		return compiledScalar{}, unsupportedMultivalueUsage("cidrmatch", expression.Range)
	}
	inputSQL, inputArgs := compiledTextEligibleStringScalar(input)
	const (
		addressAlias = "__os_cidrmatch_address"
		prefixAlias  = "__os_cidrmatch_prefix"
	)
	// isIPAddressInRange throws on malformed text, so validate the address
	// first and feed the range test a safe placeholder when it is invalid.
	valid := "(isIPv4String(assumeNotNull(" + addressAlias + ")) OR " +
		"isIPv6String(assumeNotNull(" + addressAlias + ")))"
	body := "if(isNull(" + addressAlias + "), CAST(NULL AS Nullable(Bool)), " +
		"CAST(" + valid + " AND isIPAddressInRange(if(" + valid +
		", assumeNotNull(" + addressAlias + "), '0.0.0.0'), " + prefixAlias +
		") AS Nullable(Bool)))"
	valueSQL := bindSQLExpressions(
		[]string{addressAlias, prefixAlias},
		[]string{inputSQL, "CAST(? AS String)"},
		body,
	)
	if len(valueSQL) > maxCompiledCIDRMatchScalarSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"cidrmatch scalar SQL exceeds %d bytes",
				maxCompiledCIDRMatchScalarSQLBytes,
			),
			Range: expression.Range,
		}
	}
	return compiledScalar{
		valueSQL:                valueSQL,
		valueArgs:               append(inputArgs, prefix.Masked().String()),
		maxStringBytes:          5,
		existsSQL:               "1",
		kind:                    fieldKindBool,
		materializeForPredicate: input.materializeForPredicate,
	}, nil
}
