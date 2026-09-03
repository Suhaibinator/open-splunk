package clickhouse

import (
	"errors"
	"fmt"
	"net/netip"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"fortio.org/safecast"

	"github.com/Suhaibinator/open-splunk/internal/ianatimezone"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchtimebounds"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
	"github.com/Suhaibinator/open-splunk/internal/splrelativetime"
	"github.com/Suhaibinator/open-splunk/internal/spltimeformat"
	"github.com/Suhaibinator/open-splunk/internal/splwildcard"
)

func compileScalarValue(expression plan.ScalarExpression, state compileState) (compiledScalar, error) {
	switch expression := expression.(type) {
	case *plan.ScalarUnaryExpression:
		return compileArithmeticUnary(expression, state)
	case *plan.ScalarBinaryExpression:
		return compileArithmeticBinary(expression, state)
	case *plan.ScalarFieldExpression:
		if expression == nil {
			return compiledScalar{}, errors.New("compile ClickHouse scalar expression: missing field expression")
		}
		field, ok, err := resolveCompiledField(expression.Field, state)
		if err != nil {
			return compiledScalar{}, err
		}
		if !ok {
			return compiledScalar{
				valueSQL:       "CAST(NULL AS Nullable(String))",
				maxStringBytes: 1,
				existsSQL:      "0",
				kind:           fieldKindString,
				alwaysNull:     true,
			}, nil
		}
		return compiledScalarFromField(field), nil
	case *plan.ScalarLiteralExpression:
		if expression == nil {
			return compiledScalar{}, errors.New("compile ClickHouse scalar expression: missing literal expression")
		}
		value := expression.Value
		kind := fieldKindString
		numberType := ""
		valueSQL := ""
		maxStringBytes := uint64(64)
		var argument any
		switch value.Kind {
		case plan.ValueKindString:
			valueSQL, argument = "CAST(? AS String)", value.String
			maxStringBytes = max(uint64(1), uint64(len(value.String)))
		case plan.ValueKindInt64:
			kind, numberType = fieldKindNumber, "Int64"
			valueSQL, argument = "CAST(? AS Int64)", value.Int64
		case plan.ValueKindUint64:
			kind, numberType = fieldKindNumber, "UInt64"
			valueSQL, argument = "CAST(? AS UInt64)", value.Uint64
		case plan.ValueKindFloat64:
			kind, numberType = fieldKindNumber, "Float64"
			valueSQL, argument = "CAST(? AS Float64)", value.Float64
		case plan.ValueKindBool:
			kind = fieldKindBool
			valueSQL, argument = "CAST(? AS Bool)", value.Bool
			maxStringBytes = 5
		case plan.ValueKindNull:
			return compiledScalar{
				valueSQL:         "CAST(NULL AS Nullable(String))",
				maxStringBytes:   1,
				existsSQL:        "1",
				kind:             fieldKindInvalid,
				literal:          &value,
				alwaysNull:       true,
				comparisonAtomic: true,
			}, nil
		default:
			return compiledScalar{}, errors.New("compile ClickHouse scalar expression: invalid literal")
		}
		return compiledScalar{
			valueSQL:         valueSQL,
			valueArgs:        []any{argument},
			maxStringBytes:   maxStringBytes,
			existsSQL:        "1",
			kind:             kind,
			numberType:       numberType,
			literal:          &value,
			comparisonAtomic: true,
		}, nil
	case *plan.ScalarCallExpression:
		if expression == nil {
			return compiledScalar{}, errors.New("compile ClickHouse scalar expression: missing call expression")
		}
		switch expression.Function {
		case plan.ScalarFunctionNow:
			return compileNowScalar(expression, state)
		case plan.ScalarFunctionStrftime:
			return compileStrftimeScalar(expression, state)
		case plan.ScalarFunctionStrptime:
			return compileStrptimeScalar(expression, state)
		case plan.ScalarFunctionRelativeTime:
			return compileRelativeTimeScalar(expression, state)
		case plan.ScalarFunctionReplace:
			return compileReplaceScalar(expression, state)
		case plan.ScalarFunctionToNumber:
			return compileToNumberScalar(expression, state)
		case plan.ScalarFunctionToString:
			return compileToStringScalar(expression, state)
		case plan.ScalarFunctionConcat:
			return compileConcatenationScalar(expression, state)
		case plan.ScalarFunctionSplit:
			return compileBoundedNativeMVScalar(expression, state, compileSplitScalar)
		case plan.ScalarFunctionMVAppend:
			return compileBoundedNativeMVScalar(expression, state, compileMVAppendScalar)
		case plan.ScalarFunctionMVDedup:
			return compileBoundedNativeMVScalar(expression, state, compileMVDedupScalar)
		case plan.ScalarFunctionMVIndex:
			return compileBoundedNativeMVScalar(expression, state, compileMVIndexScalar)
		case plan.ScalarFunctionMVJoin:
			return compileBoundedNativeMVScalar(expression, state, compileMVJoinScalar)
		case plan.ScalarFunctionMVZip:
			return compileBoundedNativeMVScalar(expression, state, compileMVZipScalar)
		case plan.ScalarFunctionMVFind:
			return compileBoundedNativeMVScalar(expression, state, compileMVFindScalar)
		case plan.ScalarFunctionRound:
			return compileRoundScalar(expression, state)
		case plan.ScalarFunctionCeil:
			return compileIntegralRoundingScalar(expression, state, "ceil")
		case plan.ScalarFunctionFloor:
			return compileIntegralRoundingScalar(expression, state, "floor")
		case plan.ScalarFunctionMVCount:
			return compileMVCountScalar(expression, state)
		case plan.ScalarFunctionMVSort:
			return compileMVSortScalar(expression, state)
		case plan.ScalarFunctionMatch:
			return compileMatchScalar(expression, state)
		case plan.ScalarFunctionLike:
			return compileLikeScalar(expression, state)
		case plan.ScalarFunctionIsNull, plan.ScalarFunctionIsNotNull:
			return compileNullTestScalar(expression, state)
		case plan.ScalarFunctionCoalesce:
			return compileCoalesceScalar(expression, state)
		case plan.ScalarFunctionLower, plan.ScalarFunctionUpper:
			return compileTextCaseScalar(expression, state)
		case plan.ScalarFunctionTrim,
			plan.ScalarFunctionLTrim,
			plan.ScalarFunctionRTrim,
			plan.ScalarFunctionURLDecode,
			plan.ScalarFunctionMD5,
			plan.ScalarFunctionSHA1,
			plan.ScalarFunctionSHA256,
			plan.ScalarFunctionSHA512:
			return compileTextTransformScalar(expression, state)
		case plan.ScalarFunctionTypeOf:
			return compileTypeOfScalar(expression, state)
		case plan.ScalarFunctionCIDRMatch:
			return compileCIDRMatchScalar(expression, state)
		case plan.ScalarFunctionAbs,
			plan.ScalarFunctionSqrt,
			plan.ScalarFunctionExp,
			plan.ScalarFunctionLn,
			plan.ScalarFunctionLog,
			plan.ScalarFunctionPow,
			plan.ScalarFunctionPi:
			return compileMathScalar(expression, state)
		case plan.ScalarFunctionLength:
			return compileTextLengthScalar(expression, state)
		case plan.ScalarFunctionSubstring:
			return compileSubstringScalar(expression, state)
		default:
			return compiledScalar{}, fmt.Errorf("compile ClickHouse scalar expression: unsupported function %d", expression.Function)
		}
	case *plan.ScalarIfExpression:
		return compileIfScalar(expression, state)
	case *plan.ScalarCaseExpression:
		return compileCaseScalar(expression, state)
	default:
		return compiledScalar{}, fmt.Errorf("compile ClickHouse scalar expression: unsupported expression %T", expression)
	}
}

func compileNowScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	if expression == nil {
		return compiledScalar{}, errors.New("compile ClickHouse now: missing expression")
	}
	if len(expression.Arguments) != 0 {
		return compiledScalar{}, errors.New("compile ClickHouse now: now requires no arguments")
	}
	if state.context == nil {
		return compiledScalar{}, errors.New("compile ClickHouse now: search-start anchor is required")
	}
	return compiledScalar{
		valueSQL:         "CAST(? AS Int64)",
		valueArgs:        []any{state.context.searchStartUnix},
		maxStringBytes:   20,
		existsSQL:        "1",
		kind:             fieldKindNumber,
		numberType:       "Int64",
		numericIntegral:  true,
		comparisonAtomic: true,
	}, nil
}

func compileStrftimeScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	if expression == nil {
		return compiledScalar{}, errors.New("compile ClickHouse strftime: missing expression")
	}
	if len(expression.Arguments) != 2 {
		return compiledScalar{}, errors.New(
			"compile ClickHouse strftime: expected two arguments",
		)
	}
	format, ok := scalarQuotedStringLiteral(expression.Arguments[1])
	if !ok {
		return compiledScalar{}, errors.New(
			"compile ClickHouse strftime: format must be a quoted string literal",
		)
	}
	if state.context == nil {
		return compiledScalar{}, errors.New(
			"compile ClickHouse strftime: search timezone is required",
		)
	}
	if err := validateCompileContextSearchTimezone(state.context); err != nil {
		return compiledScalar{}, err
	}

	compiledFormat, cached := state.context.strftimeBudget.formats[expression]
	if !cached {
		var err error
		compiledFormat, err = compileStrftimeFormatForBackend(
			format,
			expression.Arguments[1].SourceRange(),
		)
		if err != nil {
			return compiledScalar{}, err
		}
		state.context.strftimeBudget.formats[expression] = compiledFormat
	}
	if compiledFormat.WorkUnits >
		MaximumStrftimeQueryWorkUnits-state.context.strftimeBudget.workUnits {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"search strftime formats require more than %d work units",
				MaximumStrftimeQueryWorkUnits,
			),
			Range: expression.Range,
		}
	}
	if compiledFormat.MaximumOutputBytes >
		MaximumStrftimeQueryOutputBytes-state.context.strftimeBudget.outputBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"search strftime results may exceed %d bytes per row",
				MaximumStrftimeQueryOutputBytes,
			),
			Range: expression.Range,
		}
	}
	state.context.strftimeBudget.workUnits += compiledFormat.WorkUnits
	state.context.strftimeBudget.outputBytes += compiledFormat.MaximumOutputBytes

	input, err := compileNonBooleanScalarInputArgument(
		expression.Arguments[0],
		state,
		"strftime",
	)
	if err != nil {
		return compiledScalar{}, err
	}
	if isNativeMultivalueKind(input.kind) {
		return compiledScalar{}, unsupportedMultivalueUsage(
			"strftime",
			expression.Range,
		)
	}
	if input.alwaysNull ||
		input.kind == fieldKindInvalid ||
		input.kind == fieldKindDynamic &&
			input.dynamicDomain == dynamicScalarDomainText {
		return compiledScalar{
			valueSQL:                "CAST(NULL AS Nullable(String))",
			maxStringBytes:          1,
			existsSQL:               "1",
			kind:                    fieldKindString,
			alwaysNull:              true,
			materializeForPredicate: input.materializeForPredicate,
		}, nil
	}
	if input.kind == fieldKindString || input.kind == fieldKindBool {
		return compiledScalar{}, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_STRFTIME_VALUE_TYPE",
			Message: "strftime requires a numeric Unix-seconds value or time field",
			Range:   expression.Arguments[0].SourceRange(),
		}
	}
	if err := chargeUnixTimestampDynamicDecimalBudget(
		input,
		state.context,
		"strftime",
		expression.Range,
	); err != nil {
		return compiledScalar{}, err
	}

	timestampSQL, err := unixTimestampScalarSQL(input, "strftime")
	if err != nil {
		return compiledScalar{}, err
	}
	formattedSQL, formatArgs, err := compileStrftimeParts(
		compiledFormat.Parts,
	)
	if err != nil {
		return compiledScalar{}, err
	}
	timestampBinding := "arrayElement(arrayMap(timestamp -> if(isNull(timestamp), " +
		"CAST(NULL AS Nullable(String)), " + formattedSQL + "), [" +
		"toTimeZone(" + timestampSQL + ", ?)]), 1)"
	valueSQL := "arrayElement(arrayMap(value -> " + timestampBinding +
		", [" + input.valueSQL + "]), 1)"
	if len(valueSQL) > maxCompiledStrftimeScalarSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"strftime scalar SQL exceeds %d bytes",
				maxCompiledStrftimeScalarSQLBytes,
			),
			Range: expression.Range,
		}
	}
	valueArgs := make(
		[]any,
		0,
		len(formatArgs)+1+len(input.valueArgs),
	)
	valueArgs = append(valueArgs, formatArgs...)
	valueArgs = append(valueArgs, state.context.searchTimezone)
	valueArgs = append(valueArgs, input.valueArgs...)
	return compiledScalar{
		valueSQL:                valueSQL,
		valueArgs:               valueArgs,
		maxStringBytes:          max(uint64(1), compiledFormat.MaximumOutputBytes),
		existsSQL:               "1",
		kind:                    fieldKindString,
		materializeForPredicate: input.materializeForPredicate,
	}, nil
}

// errSearchTimezoneInvalid is a package-level sentinel so compileContext can
// cache the check outcome as a plain bool: storing the error itself would put
// an error value inside compileState's type closure and undermine the
// reflect.DeepEqual state seals that compare compiled preludes.
var errSearchTimezoneInvalid = errors.New(
	"compile ClickHouse date/time function: search timezone is invalid",
)

func validateCompileContextSearchTimezone(context *compileContext) error {
	if context.searchTimezoneChecked {
		if context.searchTimezoneInvalid {
			return errSearchTimezoneInvalid
		}
		return nil
	}
	context.searchTimezoneChecked = true
	location, err := ianatimezone.Load(context.searchTimezone)
	if err != nil {
		context.searchTimezoneInvalid = true
		return errSearchTimezoneInvalid
	}
	localMinimum := time.Date(
		searchtimebounds.MinimumYear,
		time.January,
		1,
		0,
		0,
		0,
		0,
		location,
	)
	// ClickHouse clamps some localized DateTime64 values at its 1900 floor
	// and can report a wall-clock remainder instead of a true UTC offset.
	// Derive the earliest safe local civil instant from the same IANA rules
	// used by search admission. Pinned integration coverage with a historical
	// second-offset zone detects drift against ClickHouse's bundled tzdb.
	context.searchLocalMinimumUnixNanoseconds =
		localMinimum.Unix() * 1_000_000_000
	return nil
}

func compileStrptimeScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	if expression == nil {
		return compiledScalar{}, errors.New(
			"compile ClickHouse strptime: missing expression",
		)
	}
	if len(expression.Arguments) != 2 {
		return compiledScalar{}, errors.New(
			"compile ClickHouse strptime: expected two arguments",
		)
	}
	format, ok := scalarQuotedStringLiteral(expression.Arguments[1])
	if !ok {
		return compiledScalar{}, errors.New(
			"compile ClickHouse strptime: format must be a quoted string literal",
		)
	}
	if state.context == nil {
		return compiledScalar{}, errors.New(
			"compile ClickHouse strptime: search timezone is required",
		)
	}
	if err := validateCompileContextSearchTimezone(state.context); err != nil {
		return compiledScalar{}, err
	}

	compiledFormat, cached := state.context.strptimeBudget.formats[expression]
	if !cached {
		var err error
		compiledFormat, err = compileStrptimeFormatForBackend(
			format,
			expression.Arguments[1].SourceRange(),
		)
		if err != nil {
			return compiledScalar{}, err
		}
		if state.context.strptimeBudget.formats == nil {
			state.context.strptimeBudget.formats =
				make(map[*plan.ScalarCallExpression]spltimeformat.StrptimeFormat)
		}
		state.context.strptimeBudget.formats[expression] = compiledFormat
	}
	if compiledFormat.WorkUnits >
		MaximumStrptimeQueryWorkUnits-state.context.strptimeBudget.workUnits {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"search strptime formats require more than %d work units",
				MaximumStrptimeQueryWorkUnits,
			),
			Range: expression.Range,
		}
	}
	state.context.strptimeBudget.workUnits += compiledFormat.WorkUnits

	input, err := compileNonBooleanScalarInputArgument(
		expression.Arguments[0],
		state,
		"strptime",
	)
	if err != nil {
		return compiledScalar{}, err
	}
	if isNativeMultivalueKind(input.kind) {
		return compiledScalar{}, unsupportedMultivalueUsage(
			"strptime",
			expression.Range,
		)
	}
	if input.alwaysNull || input.kind == fieldKindInvalid {
		return compiledScalar{
			valueSQL:                "CAST(NULL AS Nullable(Float64))",
			maxStringBytes:          1,
			existsSQL:               "1",
			kind:                    fieldKindNumber,
			numberType:              "Float64",
			alwaysNull:              true,
			materializeForPredicate: input.materializeForPredicate,
		}, nil
	}
	if input.kind != fieldKindString && input.kind != fieldKindDynamic {
		return compiledScalar{}, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_STRPTIME_VALUE_TYPE",
			Message: "strptime requires a String timestamp value",
			Range:   expression.Arguments[0].SourceRange(),
		}
	}

	inputBytes := min(
		compiledScalarStringByteBound(input),
		MaximumStrptimeInputBytes,
	)
	if inputBytes >
		MaximumStrptimeQueryInputBytes-state.context.strptimeBudget.inputBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"search strptime inputs require more than %d bytes of date parsing per row",
				MaximumStrptimeQueryInputBytes,
			),
			Range: expression.Range,
		}
	}
	state.context.strptimeBudget.inputBytes += inputBytes

	patterns, err := compileStrptimePatterns(compiledFormat.Parts)
	if err != nil {
		return compiledScalar{}, err
	}
	inputSQL, inputArgs := compiledTextEligibleStringScalar(input)
	parserSQL := "parseDateTime64InJodaSyntaxOrNull(value, ?, ?)"
	parserArgs := []any{
		patterns.primaryJoda,
		state.context.searchTimezone,
	}
	if patterns.fallbackJoda != "" {
		parserSQL = "if(notEmpty(arrayElement(groups, " +
			strconv.Itoa(patterns.optionalFractionGroup) + ")), " +
			parserSQL +
			", parseDateTime64InJodaSyntaxOrNull(value, ?, ?))"
		parserArgs = []any{
			patterns.primaryJoda,
			state.context.searchTimezone,
			patterns.fallbackJoda,
			state.context.searchTimezone,
		}
	}
	maximumDateGroup := max(
		patterns.yearGroup,
		patterns.monthGroup,
		patterns.dayGroup,
	)
	civilDateSQL := "toUInt32OrZero(arrayElement(groups, " +
		strconv.Itoa(patterns.yearGroup) + ")) * 10000 + " +
		"toUInt32OrZero(arrayElement(groups, " +
		strconv.Itoa(patterns.monthGroup) + ")) * 100 + " +
		"toUInt32OrZero(arrayElement(groups, " +
		strconv.Itoa(patterns.dayGroup) + "))"
	parserSQL = "if(ifNull(length(value) <= " +
		strconv.FormatUint(MaximumStrptimeInputBytes, 10) +
		", 0), arrayElement(arrayMap(groups -> if(length(groups) >= " +
		strconv.Itoa(maximumDateGroup) + " AND (" + civilDateSQL + ") >= " +
		strconv.Itoa(minimumStrptimeCivilDate) + " AND (" + civilDateSQL +
		") <= " + strconv.Itoa(maximumStrptimeCivilDate) + ", " + parserSQL +
		", NULL), [extractGroups(ifNull(value, CAST('' AS String)), ?)]), 1), NULL)"
	microsecondsSQL := "toUnixTimestamp64Micro(" + parserSQL + ")"
	epochSQL := "arrayElement(arrayMap(microseconds -> if(" +
		"isNull(microseconds), CAST(NULL AS Nullable(Float64)), " +
		"toFloat64(microseconds) / 1000000), [" + microsecondsSQL + "]), 1)"
	valueSQL := "arrayElement(arrayMap(value -> " + epochSQL +
		", [" + inputSQL + "]), 1)"
	if len(valueSQL) > maxCompiledStrptimeScalarSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"strptime scalar SQL exceeds %d bytes",
				maxCompiledStrptimeScalarSQLBytes,
			),
			Range: expression.Range,
		}
	}
	valueArgs := make([]any, 0, 1+len(parserArgs)+len(inputArgs))
	valueArgs = append(valueArgs, parserArgs...)
	valueArgs = append(valueArgs, patterns.civilRegex)
	valueArgs = append(valueArgs, inputArgs...)
	return compiledScalar{
		valueSQL:                valueSQL,
		valueArgs:               valueArgs,
		maxStringBytes:          64,
		existsSQL:               "1",
		kind:                    fieldKindNumber,
		numberType:              "Float64",
		materializeForPredicate: input.materializeForPredicate,
	}, nil
}

func compileRelativeTimeScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	if expression == nil {
		return compiledScalar{}, errors.New(
			"compile ClickHouse relative_time: missing expression",
		)
	}
	if len(expression.Arguments) != 2 {
		return compiledScalar{}, errors.New(
			"compile ClickHouse relative_time: expected two arguments",
		)
	}
	specifierText, ok := scalarQuotedStringLiteral(expression.Arguments[1])
	if !ok {
		return compiledScalar{}, errors.New(
			"compile ClickHouse relative_time: specifier must be a quoted string literal",
		)
	}
	if state.context == nil {
		return compiledScalar{}, errors.New(
			"compile ClickHouse relative_time: search timezone is required",
		)
	}
	if err := validateCompileContextSearchTimezone(state.context); err != nil {
		return compiledScalar{}, err
	}

	specifier, cached := state.context.relativeTimeBudget.specifiers[expression]
	if !cached {
		var err error
		specifier, err = compileRelativeTimeSpecifierForBackend(
			specifierText,
			expression.Arguments[1].SourceRange(),
		)
		if err != nil {
			return compiledScalar{}, err
		}
		if state.context.relativeTimeBudget.specifiers == nil {
			state.context.relativeTimeBudget.specifiers =
				make(map[*plan.ScalarCallExpression]splrelativetime.Specifier)
		}
		state.context.relativeTimeBudget.specifiers[expression] = specifier
	}
	if specifier.WorkUnits >
		MaximumRelativeTimeQueryWorkUnits-
			state.context.relativeTimeBudget.workUnits {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"search relative_time specifiers require more than %d work units",
				MaximumRelativeTimeQueryWorkUnits,
			),
			Range: expression.Range,
		}
	}
	operationCount := specifier.OperationCount()
	if operationCount >
		MaximumRelativeTimeQueryOperations-
			state.context.relativeTimeBudget.operations {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"search relative_time specifiers contain more than %d operations",
				MaximumRelativeTimeQueryOperations,
			),
			Range: expression.Range,
		}
	}
	state.context.relativeTimeBudget.workUnits += specifier.WorkUnits
	state.context.relativeTimeBudget.operations += operationCount

	input, err := compileNonBooleanScalarInputArgument(
		expression.Arguments[0],
		state,
		"relative_time",
	)
	if err != nil {
		return compiledScalar{}, err
	}
	if isNativeMultivalueKind(input.kind) {
		return compiledScalar{}, unsupportedMultivalueUsage(
			"relative_time",
			expression.Range,
		)
	}
	if input.alwaysNull ||
		input.kind == fieldKindInvalid ||
		input.kind == fieldKindDynamic &&
			input.dynamicDomain == dynamicScalarDomainText {
		return compiledScalar{
			valueSQL:                "CAST(NULL AS Nullable(Float64))",
			maxStringBytes:          1,
			existsSQL:               "1",
			kind:                    fieldKindNumber,
			numberType:              "Float64",
			alwaysNull:              true,
			materializeForPredicate: input.materializeForPredicate,
		}, nil
	}
	if input.kind != fieldKindTime &&
		input.kind != fieldKindNumber &&
		input.kind != fieldKindDynamic {
		return compiledScalar{}, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_RELATIVE_TIME_VALUE_TYPE",
			Message: "relative_time requires a numeric Unix-seconds value or time field",
			Range:   expression.Arguments[0].SourceRange(),
		}
	}
	if err := chargeUnixTimestampDynamicDecimalBudget(
		input,
		state.context,
		"relative_time",
		expression.Range,
	); err != nil {
		return compiledScalar{}, err
	}

	timestampSQL, err := unixTimestampScalarSQL(input, "relative_time")
	if err != nil {
		return compiledScalar{}, err
	}
	programSQL := compileRelativeTimeInputTimestampSQL(
		timestampSQL,
		input.valueSQL,
	)
	programArgs := make(
		[]any,
		0,
		1+len(input.valueArgs)+operationCount,
	)
	programArgs = append(programArgs, state.context.searchTimezone)
	programArgs = append(programArgs, input.valueArgs...)
	for index := range operationCount {
		operation, found := specifier.Operation(index)
		if !found {
			return compiledScalar{}, errors.New(
				"compile ClickHouse relative_time: validated operation is missing",
			)
		}
		programSQL, programArgs, err = compileRelativeTimeOperation(
			programSQL,
			programArgs,
			operation,
			state.context.searchLocalMinimumUnixNanoseconds,
		)
		if err != nil {
			return compiledScalar{}, err
		}
	}

	valueSQL := "arrayElement(arrayMap(value -> if(isNull(value), " +
		"CAST(NULL AS Nullable(Float64)), " +
		"toFloat64(toUnixTimestamp64Nano(value)) / 1000000000), [" +
		programSQL + "]), 1)"
	if len(valueSQL) > maxCompiledRelativeTimeScalarSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"relative_time scalar SQL exceeds %d bytes",
				maxCompiledRelativeTimeScalarSQLBytes,
			),
			Range: expression.Range,
		}
	}
	return compiledScalar{
		valueSQL:                valueSQL,
		valueArgs:               programArgs,
		maxStringBytes:          64,
		existsSQL:               "1",
		kind:                    fieldKindNumber,
		numberType:              "Float64",
		materializeForPredicate: input.materializeForPredicate,
	}, nil
}

func compileRelativeTimeSpecifierForBackend(
	source string,
	sourceRange spl.Range,
) (splrelativetime.Specifier, error) {
	specifier, err := splrelativetime.CompileSpecifier(source)
	if err == nil {
		return specifier, nil
	}
	if errors.Is(err, splrelativetime.ErrSpecifierTooLarge) {
		return splrelativetime.Specifier{}, &plan.Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: "relative_time specifier exceeds its resource limit",
			Range:   sourceRange,
		}
	}
	if errors.Is(err, splrelativetime.ErrMagnitudeOutOfRange) {
		return splrelativetime.Specifier{}, &plan.Diagnostic{
			Code: "SPL_NUMBER_OUT_OF_RANGE",
			Message: "relative_time magnitude exceeds the supported " +
				searchtimebounds.YearRangeDescription + " timestamp span",
			Range: sourceRange,
		}
	}
	return splrelativetime.Specifier{}, &plan.Diagnostic{
		Code: "SPL_UNSUPPORTED_RELATIVE_TIME_SPECIFIER",
		Message: "relative_time specifier is outside the supported bounded " +
			"offset-and-snap subset",
		Range: sourceRange,
	}
}

func compileRelativeTimeInputTimestampSQL(
	timestampSQL string,
	inputSQL string,
) string {
	return "arrayElement(arrayMap(value -> arrayElement(arrayMap(timestamp -> " +
		"if(" + relativeTimeTimestampRangeCondition("timestamp") +
		", toTimeZone(timestamp, ?), NULL), [" + timestampSQL +
		"]), 1), [" + inputSQL + "]), 1)"
}

func compileRelativeTimeOperation(
	inputSQL string,
	inputArgs []any,
	operation splrelativetime.Operation,
	localMinimumUnixNanoseconds int64,
) (string, []any, error) {
	switch operation.Kind {
	case splrelativetime.OperationOffset:
		if operation.Magnitude == 0 {
			return inputSQL, inputArgs, nil
		}
		var (
			compiledSQL string
			err         error
		)
		switch operation.Unit {
		case splrelativetime.UnitSecond:
			compiledSQL = compileRelativeTimeElapsedOffsetSQL(
				inputSQL,
				operation,
				1_000_000_000,
			)
		case splrelativetime.UnitMinute:
			compiledSQL = compileRelativeTimeElapsedOffsetSQL(
				inputSQL,
				operation,
				60*1_000_000_000,
			)
		case splrelativetime.UnitHour:
			compiledSQL = compileRelativeTimeElapsedOffsetSQL(
				inputSQL,
				operation,
				60*60*1_000_000_000,
			)
		case splrelativetime.UnitDay:
			compiledSQL = compileRelativeTimeCalendarDayOffsetSQL(
				inputSQL,
				operation,
				1,
				localMinimumUnixNanoseconds,
			)
		case splrelativetime.UnitWeek:
			compiledSQL = compileRelativeTimeCalendarDayOffsetSQL(
				inputSQL,
				operation,
				7,
				localMinimumUnixNanoseconds,
			)
		case splrelativetime.UnitMonth:
			compiledSQL = compileRelativeTimeCalendarMonthOffsetSQL(
				inputSQL,
				operation,
				1,
				localMinimumUnixNanoseconds,
			)
		case splrelativetime.UnitQuarter:
			compiledSQL = compileRelativeTimeCalendarMonthOffsetSQL(
				inputSQL,
				operation,
				3,
				localMinimumUnixNanoseconds,
			)
		case splrelativetime.UnitYear:
			compiledSQL = compileRelativeTimeCalendarMonthOffsetSQL(
				inputSQL,
				operation,
				12,
				localMinimumUnixNanoseconds,
			)
		default:
			err = errors.New(
				"compile ClickHouse relative_time: invalid offset unit",
			)
		}
		if err != nil {
			return "", nil, err
		}
		args := make([]any, 0, 1+len(inputArgs))
		args = append(args, operation.Magnitude)
		args = append(args, inputArgs...)
		return compiledSQL, args, nil
	case splrelativetime.OperationSnap:
		compiledSQL, err := compileRelativeTimeSnapSQL(
			inputSQL,
			operation,
			localMinimumUnixNanoseconds,
		)
		if err != nil {
			return "", nil, err
		}
		return compiledSQL, inputArgs, nil
	default:
		return "", nil, errors.New(
			"compile ClickHouse relative_time: invalid operation",
		)
	}
}

func compileRelativeTimeElapsedOffsetSQL(
	inputSQL string,
	operation splrelativetime.Operation,
	nanosecondsPerUnit uint64,
) string {
	operator := "+"
	if operation.Negative {
		operator = "-"
	}
	targetTicks := "toInt256(toUnixTimestamp64Nano(value)) " + operator +
		" toInt256(?) * toInt256(" +
		strconv.FormatUint(nanosecondsPerUnit, 10) + ")"
	candidate := "arrayElement(arrayMap(ticks -> if(isNotNull(value) AND ticks >= " +
		"toInt256(" +
		strconv.FormatInt(minimumRelativeTimeUnixNanoseconds, 10) +
		") AND ticks <= toInt256(" +
		strconv.FormatInt(maximumRelativeTimeUnixNanoseconds, 10) +
		"), toTimeZone(fromUnixTimestamp64Nano(" +
		"accurateCastOrNull(ticks, 'Int64'), 'UTC'), timezoneOf(value)), " +
		"NULL), [" + targetTicks + "]), 1)"
	return boundedRelativeTimeTimestampSQL(
		inputSQL,
		candidate,
		relativeTimeOffsetResultDirection(operation),
	)
}

func compileRelativeTimeCalendarDayOffsetSQL(
	inputSQL string,
	operation splrelativetime.Operation,
	daysPerUnit uint64,
	localMinimumUnixNanoseconds int64,
) string {
	operator := "+"
	if operation.Negative {
		operator = "-"
	}
	currentDay := "toInt64(toDaysSinceYearZero(value))"
	targetDay := currentDay + " " + operator + " toInt64(?) * " +
		strconv.FormatUint(daysPerUnit, 10)
	valid := relativeTimeLocalCivilLowerBoundCondition(
		localMinimumUnixNanoseconds,
	) + " AND " +
		relativeTimeCalendarDayRangeCondition("target_day")
	adjusted := "addDays(value, if(" + valid + ", target_day - " +
		currentDay + ", 0))"
	candidate := "arrayElement(arrayMap(target_day -> " +
		"if(isNotNull(value) AND " + valid + ", " + adjusted +
		", NULL), [" + targetDay + "]), 1)"
	return boundedRelativeTimeTimestampSQL(
		inputSQL,
		candidate,
		relativeTimeOffsetResultDirection(operation),
	)
}

func compileRelativeTimeCalendarMonthOffsetSQL(
	inputSQL string,
	operation splrelativetime.Operation,
	monthsPerUnit uint64,
	localMinimumUnixNanoseconds int64,
) string {
	operator := "+"
	if operation.Negative {
		operator = "-"
	}
	currentMonth := "toInt64(toYear(value)) * 12 + " +
		"toInt64(toMonth(value)) - 1"
	targetMonth := currentMonth + " " + operator + " toInt64(?) * " +
		strconv.FormatUint(monthsPerUnit, 10)
	valid := relativeTimeLocalCivilLowerBoundCondition(
		localMinimumUnixNanoseconds,
	) + " AND " +
		relativeTimeCalendarMonthRangeCondition("target_month")
	adjusted := "addMonths(value, if(" + valid + ", target_month - (" +
		currentMonth + "), 0))"
	candidate := "arrayElement(arrayMap(target_month -> " +
		"if(isNotNull(value) AND " + valid + ", " + adjusted +
		", NULL), [" + targetMonth + "]), 1)"
	return boundedRelativeTimeTimestampSQL(
		inputSQL,
		candidate,
		relativeTimeOffsetResultDirection(operation),
	)
}

func compileRelativeTimeSnapSQL(
	inputSQL string,
	operation splrelativetime.Operation,
	localMinimumUnixNanoseconds int64,
) (string, error) {
	body := ""
	switch operation.Unit {
	case splrelativetime.UnitSecond:
		body = compileRelativeTimeSubdaySnapCandidateSQL(
			1_000_000_000,
			false,
		)
	case splrelativetime.UnitMinute:
		body = compileRelativeTimeSubdaySnapCandidateSQL(
			60*1_000_000_000,
			true,
		)
	case splrelativetime.UnitHour:
		body = compileRelativeTimeSubdaySnapCandidateSQL(
			60*60*1_000_000_000,
			true,
		)
	case splrelativetime.UnitDay:
		body = relativeTimeLocallyRepresentableCandidateSQL(
			"dateTrunc('day', value)",
			localMinimumUnixNanoseconds,
		)
	case splrelativetime.UnitWeek:
		return compileRelativeTimeWeekSnapSQL(
			inputSQL,
			operation.Weekday,
			localMinimumUnixNanoseconds,
		), nil
	case splrelativetime.UnitMonth:
		body = relativeTimeLocallyRepresentableCandidateSQL(
			"toDateTime64(dateTrunc('month', value), 0, timezoneOf(value))",
			localMinimumUnixNanoseconds,
		)
	case splrelativetime.UnitQuarter:
		body = relativeTimeLocallyRepresentableCandidateSQL(
			"toDateTime64(dateTrunc('quarter', value), 0, timezoneOf(value))",
			localMinimumUnixNanoseconds,
		)
	case splrelativetime.UnitYear:
		body = relativeTimeLocallyRepresentableCandidateSQL(
			"toDateTime64(dateTrunc('year', value), 0, timezoneOf(value))",
			localMinimumUnixNanoseconds,
		)
	default:
		return "", errors.New(
			"compile ClickHouse relative_time: invalid snap unit",
		)
	}
	return boundedRelativeTimeTimestampSQL(
		inputSQL,
		body,
		relativeTimeResultNotAfter,
	), nil
}

func compileRelativeTimeSubdaySnapCandidateSQL(
	nanosecondsPerUnit uint64,
	timezoneAligned bool,
) string {
	step := "toInt256(" +
		strconv.FormatUint(nanosecondsPerUnit, 10) + ")"
	alignedTicks := "ticks"
	if timezoneAligned {
		alignedTicks += " + toInt256(timeZoneOffset(value)) * " +
			"toInt256(1000000000)"
	}
	remainder := "modulo(modulo(" + alignedTicks + ", " + step + ") + " + step +
		", " + step + ")"
	target := "ticks - " + remainder
	return "arrayElement(arrayMap(ticks -> " +
		"toTimeZone(fromUnixTimestamp64Nano(" +
		"accurateCastOrNull(" + target + ", 'Int64'), 'UTC'), " +
		"timezoneOf(value)), [" +
		"toInt256(toUnixTimestamp64Nano(value))]), 1)"
}

func compileRelativeTimeWeekSnapSQL(
	inputSQL string,
	weekday uint8,
	localMinimumUnixNanoseconds int64,
) string {
	currentDay := "toInt64(toDaysSinceYearZero(value))"
	daysBack := "modulo(toInt64(modulo(toDayOfWeek(value), 7)) - " +
		strconv.FormatUint(uint64(weekday), 10) + " + 7, 7)"
	targetDay := currentDay + " - " + daysBack
	valid := relativeTimeLocalCivilLowerBoundCondition(
		localMinimumUnixNanoseconds,
	) + " AND " +
		relativeTimeCalendarDayRangeCondition("target_day")
	adjusted := "addDays(dateTrunc('day', value), if(" + valid +
		", target_day - " + currentDay + ", 0))"
	candidate := "arrayElement(arrayMap(target_day -> " +
		"if(isNotNull(value) AND " + valid + ", " + adjusted +
		", NULL), [" + targetDay + "]), 1)"
	return boundedRelativeTimeTimestampSQL(
		inputSQL,
		candidate,
		relativeTimeResultNotAfter,
	)
}

func relativeTimeCalendarDayRangeCondition(valueSQL string) string {
	return valueSQL + " >= toInt64(toDaysSinceYearZero(toDate32('" +
		strconv.Itoa(searchtimebounds.MinimumYear) +
		"-01-01'))) AND " + valueSQL +
		" <= toInt64(toDaysSinceYearZero(toDate32('" +
		strconv.Itoa(searchtimebounds.MaximumYear) + "-01-01')))"
}

func relativeTimeCalendarMonthRangeCondition(valueSQL string) string {
	return valueSQL + " >= " +
		strconv.Itoa(searchtimebounds.MinimumYear*12) + " AND " +
		valueSQL + " <= " +
		strconv.Itoa(searchtimebounds.MaximumYear*12)
}

func relativeTimeTimestampRangeCondition(valueSQL string) string {
	ticks := "toUnixTimestamp64Nano(" + valueSQL + ")"
	return "isNotNull(" + valueSQL + ") AND " + ticks + " >= " +
		strconv.FormatInt(minimumRelativeTimeUnixNanoseconds, 10) +
		" AND " + ticks + " <= " +
		strconv.FormatInt(maximumRelativeTimeUnixNanoseconds, 10)
}

func relativeTimeLocalCivilLowerBoundCondition(
	localMinimumUnixNanoseconds int64,
) string {
	return "toUnixTimestamp64Nano(value) >= " +
		strconv.FormatInt(localMinimumUnixNanoseconds, 10)
}

func relativeTimeLocallyRepresentableCandidateSQL(
	candidateSQL string,
	localMinimumUnixNanoseconds int64,
) string {
	return "if(isNotNull(value) AND " +
		relativeTimeLocalCivilLowerBoundCondition(
			localMinimumUnixNanoseconds,
		) +
		", " + candidateSQL + ", NULL)"
}

func relativeTimeOffsetResultDirection(
	operation splrelativetime.Operation,
) relativeTimeResultDirection {
	if operation.Negative {
		return relativeTimeResultBefore
	}
	return relativeTimeResultAfter
}

func relativeTimeResultDirectionCondition(
	direction relativeTimeResultDirection,
) string {
	resultTicks := "toUnixTimestamp64Nano(result)"
	inputTicks := "toUnixTimestamp64Nano(value)"
	switch direction {
	case relativeTimeResultBefore:
		return resultTicks + " < " + inputTicks
	case relativeTimeResultAfter:
		return resultTicks + " > " + inputTicks
	case relativeTimeResultNotAfter:
		return resultTicks + " <= " + inputTicks
	default:
		return "0"
	}
}

func boundedRelativeTimeTimestampSQL(
	inputSQL string,
	candidateSQL string,
	direction relativeTimeResultDirection,
) string {
	return "arrayElement(arrayMap(value -> " +
		"arrayElement(arrayMap(result -> if(" +
		relativeTimeTimestampRangeCondition("result") + " AND " +
		relativeTimeResultDirectionCondition(direction) +
		", result, NULL), [" + candidateSQL + "]), 1), [" +
		inputSQL + "]), 1)"
}

func compileStrptimeFormatForBackend(
	format string,
	sourceRange spl.Range,
) (spltimeformat.StrptimeFormat, error) {
	compiled, err := spltimeformat.CompileStrptimeFormat(format)
	if err == nil {
		return compiled, nil
	}
	if errors.Is(err, spltimeformat.ErrStrptimeFormatTooLarge) {
		return spltimeformat.StrptimeFormat{}, &plan.Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: "strptime format exceeds its resource limit",
			Range:   sourceRange,
		}
	}
	return spltimeformat.StrptimeFormat{}, &plan.Diagnostic{
		Code: "SPL_UNSUPPORTED_TIME_FORMAT",
		Message: "strptime format is outside the supported deterministic " +
			"full-date parsing subset",
		Range: sourceRange,
	}
}

type compiledStrptimePatterns struct {
	primaryJoda           string
	fallbackJoda          string
	civilRegex            string
	yearGroup             int
	monthGroup            int
	dayGroup              int
	optionalFractionGroup int
}

func compileStrptimePatterns(
	parts []spltimeformat.Part,
) (compiledStrptimePatterns, error) {
	primaryJoda, err := compileStrptimeJodaPattern(parts)
	if err != nil {
		return compiledStrptimePatterns{}, err
	}
	compiled := compiledStrptimePatterns{primaryJoda: primaryJoda}
	optionalFraction := hasOptionalTerminalStrptimeFraction(parts)
	if optionalFraction {
		fallbackParts := slices.Clone(parts[:len(parts)-1])
		lastLiteral := &fallbackParts[len(fallbackParts)-1]
		lastLiteral.Literal = strings.TrimSuffix(lastLiteral.Literal, ".")
		compiled.fallbackJoda, err = compileStrptimeJodaPattern(fallbackParts)
		if err != nil {
			return compiledStrptimePatterns{}, err
		}
	}

	var pattern strings.Builder
	pattern.WriteByte('^')
	groupCount := 0
	appendDateGroup := func(fragment string, target *int) {
		groupCount++
		*target = groupCount
		pattern.WriteByte('(')
		pattern.WriteString(fragment)
		pattern.WriteByte(')')
	}
	for index, part := range parts {
		switch part.Directive {
		case spltimeformat.DirectiveLiteral:
			literal := part.Literal
			if optionalFraction && index == len(parts)-2 {
				literal = strings.TrimSuffix(literal, ".")
			}
			pattern.WriteString(regexp.QuoteMeta(literal))
		case spltimeformat.DirectivePercent:
			pattern.WriteByte('%')
		case spltimeformat.DirectiveYear:
			appendDateGroup(`[0-9]{4}`, &compiled.yearGroup)
		case spltimeformat.DirectiveMonthNumber:
			appendDateGroup(`[0-9]{1,2}`, &compiled.monthGroup)
		case spltimeformat.DirectiveDay:
			appendDateGroup(`[0-9]{1,2}`, &compiled.dayGroup)
		case spltimeformat.DirectiveISODate:
			appendDateGroup(`[0-9]{4}`, &compiled.yearGroup)
			pattern.WriteByte('-')
			appendDateGroup(`[0-9]{1,2}`, &compiled.monthGroup)
			pattern.WriteByte('-')
			appendDateGroup(`[0-9]{1,2}`, &compiled.dayGroup)
		case spltimeformat.DirectiveHour24,
			spltimeformat.DirectiveHour12,
			spltimeformat.DirectiveMinute,
			spltimeformat.DirectiveSecond:
			pattern.WriteString(`[0-9]{1,2}`)
		case spltimeformat.DirectiveAMPM:
			pattern.WriteString(`(?:[Aa][Mm]|[Pp][Mm])`)
		case spltimeformat.DirectiveTime24:
			pattern.WriteString(
				`[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}`,
			)
		case spltimeformat.DirectiveTimezoneOffset:
			pattern.WriteString(`[+-][0-9]{4}`)
		case spltimeformat.DirectiveSubseconds:
			isOptional := optionalFraction && index == len(parts)-1
			if isOptional {
				groupCount++
				compiled.optionalFractionGroup = groupCount
			}
			appendStrptimeFractionPattern(
				&pattern,
				part.Width,
				isOptional,
			)
		case spltimeformat.DirectiveMicroseconds:
			appendStrptimeFractionPattern(
				&pattern,
				6,
				optionalFraction && index == len(parts)-1,
			)
		default:
			return compiledStrptimePatterns{}, fmt.Errorf(
				"compile ClickHouse strptime: unsupported directive %d",
				part.Directive,
			)
		}
	}
	pattern.WriteByte('$')
	if compiled.yearGroup == 0 ||
		compiled.monthGroup == 0 ||
		compiled.dayGroup == 0 {
		return compiledStrptimePatterns{}, errors.New(
			"compile ClickHouse strptime: format is missing a complete date",
		)
	}
	compiled.civilRegex = pattern.String()
	return compiled, nil
}

func hasOptionalTerminalStrptimeFraction(parts []spltimeformat.Part) bool {
	if len(parts) < 2 {
		return false
	}
	if parts[len(parts)-1].Directive != spltimeformat.DirectiveSubseconds {
		return false
	}
	literal := parts[len(parts)-2]
	return literal.Directive == spltimeformat.DirectiveLiteral &&
		strings.HasSuffix(literal.Literal, ".")
}

func appendStrptimeFractionPattern(
	pattern *strings.Builder,
	width uint8,
	optional bool,
) {
	if optional {
		pattern.WriteString(`(\.`)
	}
	pattern.WriteString(`[0-9]{1,`)
	pattern.WriteString(strconv.Itoa(int(width)))
	pattern.WriteByte('}')
	if optional {
		pattern.WriteString(`)?`)
	}
}

func compileStrptimeJodaPattern(parts []spltimeformat.Part) (string, error) {
	var pattern strings.Builder
	for _, part := range parts {
		if appendStrftimeJodaPart(&pattern, part) {
			continue
		}
		if part.Directive == spltimeformat.DirectiveTimezoneOffset {
			pattern.WriteByte('Z')
			continue
		}
		return "", fmt.Errorf(
			"compile ClickHouse strptime: unsupported directive %d",
			part.Directive,
		)
	}
	return pattern.String(), nil
}

func compileStrftimeFormatForBackend(
	format string,
	sourceRange spl.Range,
) (spltimeformat.StrftimeFormat, error) {
	compiled, err := spltimeformat.CompileStrftimeFormat(format)
	if err == nil {
		return compiled, nil
	}
	if errors.Is(err, spltimeformat.ErrStrftimeFormatTooLarge) {
		return spltimeformat.StrftimeFormat{}, &plan.Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: "strftime format exceeds its resource limit",
			Range:   sourceRange,
		}
	}
	return spltimeformat.StrftimeFormat{}, &plan.Diagnostic{
		Code: "SPL_UNSUPPORTED_TIME_FORMAT",
		Message: "strftime format is outside the supported locale-stable " +
			"date/time variable subset",
		Range: sourceRange,
	}
}

func chargeUnixTimestampDynamicDecimalBudget(
	input compiledScalar,
	context *compileContext,
	functionName string,
	sourceRange spl.Range,
) error {
	if input.kind != fieldKindDynamic ||
		input.dynamicDomain != dynamicScalarDomainAny {
		return nil
	}
	if context == nil {
		return fmt.Errorf(
			"compile ClickHouse %s: query context is required",
			functionName,
		)
	}
	inputBytes := uint64(MaximumUnixTimestampDynamicDecimalBytes)
	if inputBytes >
		MaximumUnixTimestampQueryDynamicDecimalBytes-
			context.unixTimestampBudget.dynamicDecimalBytes {
		return &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"search Dynamic decimal timestamp inputs require more than %d bytes of parsing",
				MaximumUnixTimestampQueryDynamicDecimalBytes,
			),
			Range: sourceRange,
		}
	}
	context.unixTimestampBudget.dynamicDecimalBytes += inputBytes
	return nil
}

func unixTimestampScalarSQL(
	input compiledScalar,
	functionName string,
) (string, error) {
	switch input.kind {
	case fieldKindTime:
		return "value", nil
	case fieldKindNumber:
		nanoseconds := ""
		if fixedNumberTypeIsInteger(input.numberType) {
			nanoseconds = "accurateCastOrNull(toInt256(value) * " +
				"toInt256(1000000000), 'Int64')"
		} else {
			nanoseconds = "accurateCastOrNull(floor(toFloat64(value) * " +
				"1000000000), 'Int64')"
		}
		return "fromUnixTimestamp64Nano(" + nanoseconds + ", 'UTC')", nil
	case fieldKindDynamic:
		if input.dynamicDomain == dynamicScalarDomainText {
			return "fromUnixTimestamp64Nano(" +
				"CAST(NULL AS Nullable(Int64)), 'UTC')", nil
		}
		dynamic := compiledScalar{
			valueSQL:       "value",
			dynamicTypeSQL: "dynamicType(value)",
			kind:           fieldKindDynamic,
			dynamicDomain:  input.dynamicDomain,
		}
		typeSQL := dynamicScalarTypeSQL(dynamic)
		integerNanoseconds := "accurateCastOrNull(toInt256(" +
			"accurateCastOrNull(value, 'Int64')) * " +
			"toInt256(1000000000), 'Int64')"
		numeric := finiteDynamicFloatOrNullSQL("value")
		if input.dynamicDomain == dynamicScalarDomainAny {
			taggedDecimal, taggedPayload := dynamicTaggedDecimalText(dynamic)
			payloadLimit := strconv.Itoa(
				MaximumUnixTimestampDynamicDecimalBytes,
			)
			boundedTaggedPayload := "if(length(" + taggedPayload + ") <= " +
				payloadLimit + ", " + taggedPayload +
				", CAST('' AS String))"
			numeric = "multiIf(" +
				dynamicNumericTypePredicate(typeSQL) + ", " +
				numeric + ", " +
				taggedDecimal + ", " +
				finiteFloatOrNullSQL(boundedTaggedPayload) +
				", CAST(NULL AS Nullable(Float64)))"
		}
		floatingNanoseconds := "accurateCastOrNull(floor(ifNotFinite(" + numeric +
			", CAST(NULL AS Nullable(Float64))) * 1000000000), 'Int64')"
		return "fromUnixTimestamp64Nano(if(" +
			dynamicIntegerTypePredicate(typeSQL) + ", " +
			integerNanoseconds + ", " + floatingNanoseconds +
			"), 'UTC')", nil
	default:
		return "", fmt.Errorf(
			"compile ClickHouse %s: unsupported scalar value type",
			functionName,
		)
	}
}

func compileStrftimeParts(
	parts []spltimeformat.Part,
) (string, []any, error) {
	fragments := make([]string, 0, len(parts))
	args := make([]any, 0, len(parts))
	var joda strings.Builder
	flushJoda := func() {
		if joda.Len() == 0 {
			return
		}
		fragments = append(
			fragments,
			"formatDateTimeInJodaSyntax(timestamp, ?)",
		)
		args = append(args, joda.String())
		joda.Reset()
	}
	for _, part := range parts {
		if appendStrftimeJodaPart(&joda, part) {
			continue
		}
		flushJoda()
		switch part.Directive {
		case spltimeformat.DirectiveDaySpace:
			fragments = append(
				fragments,
				"leftPad(toString(toDayOfMonth(timestamp)), 2, ' ')",
			)
		case spltimeformat.DirectiveWeekdayNumber:
			fragments = append(
				fragments,
				"toString(modulo(toDayOfWeek(timestamp), 7))",
			)
		case spltimeformat.DirectiveISOWeekYearShort:
			fragments = append(
				fragments,
				"substring(formatDateTimeInJodaSyntax(timestamp, ?), -2)",
			)
			args = append(args, "xxxx")
		case spltimeformat.DirectiveEpochSeconds:
			fragments = append(
				fragments,
				"arrayElement(arrayMap(nanoseconds -> toString(if(nanoseconds < 0, "+
					"-intDiv(-nanoseconds + 999999999, 1000000000), "+
					"intDiv(nanoseconds, 1000000000))), "+
					"[toUnixTimestamp64Nano(timestamp)]), 1)",
			)
		case spltimeformat.DirectiveTimezoneOffset:
			fragments = append(
				fragments,
				"formatDateTime(timestamp, ?)",
			)
			args = append(args, "%z")
		case spltimeformat.DirectiveTimezoneOffsetColon:
			fragments = append(
				fragments,
				"arrayElement(arrayMap(offset -> concat(substring(offset, 1, 3), "+
					"':', substring(offset, 4, 2)), "+
					"[formatDateTime(timestamp, ?)]), 1)",
			)
			args = append(args, "%z")
		default:
			return "", nil, fmt.Errorf(
				"compile ClickHouse strftime: unsupported directive %d",
				part.Directive,
			)
		}
	}
	flushJoda()
	switch len(fragments) {
	case 0:
		return "CAST('' AS String)", args, nil
	case 1:
		return fragments[0], args, nil
	default:
		return "concat(" + strings.Join(fragments, ", ") + ")", args, nil
	}
}

func appendStrftimeJodaPart(builder *strings.Builder, part spltimeformat.Part) bool {
	switch part.Directive {
	case spltimeformat.DirectiveLiteral:
		appendJodaLiteral(builder, part.Literal)
	case spltimeformat.DirectivePercent:
		builder.WriteByte('%')
	case spltimeformat.DirectiveYear:
		builder.WriteString("yyyy")
	case spltimeformat.DirectiveYearShort:
		builder.WriteString("yy")
	case spltimeformat.DirectiveISOWeekYear:
		builder.WriteString("xxxx")
	case spltimeformat.DirectiveMonthNumber:
		builder.WriteString("MM")
	case spltimeformat.DirectiveMonthShort:
		builder.WriteString("MMM")
	case spltimeformat.DirectiveMonthLong:
		builder.WriteString("MMMM")
	case spltimeformat.DirectiveDay:
		builder.WriteString("dd")
	case spltimeformat.DirectiveDayOfYear:
		builder.WriteString("DDD")
	case spltimeformat.DirectiveISOWeek:
		builder.WriteString("ww")
	case spltimeformat.DirectiveWeekdayShort:
		builder.WriteString("EEE")
	case spltimeformat.DirectiveWeekdayLong:
		builder.WriteString("EEEE")
	case spltimeformat.DirectiveHour24:
		builder.WriteString("HH")
	case spltimeformat.DirectiveHour12:
		builder.WriteString("hh")
	case spltimeformat.DirectiveMinute:
		builder.WriteString("mm")
	case spltimeformat.DirectiveSecond:
		builder.WriteString("ss")
	case spltimeformat.DirectiveAMPM:
		builder.WriteString("a")
	case spltimeformat.DirectiveTime24:
		builder.WriteString("HH:mm:ss")
	case spltimeformat.DirectiveISODate:
		builder.WriteString("yyyy-MM-dd")
	case spltimeformat.DirectiveSubseconds:
		builder.WriteString(strings.Repeat("S", int(part.Width)))
	case spltimeformat.DirectiveMicroseconds:
		builder.WriteString("SSSSSS")
	default:
		return false
	}
	return true
}

func appendJodaLiteral(builder *strings.Builder, literal string) {
	if literal == "" {
		return
	}
	if !strings.ContainsAny(
		literal,
		"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'",
	) {
		builder.WriteString(literal)
		return
	}
	builder.WriteByte('\'')
	builder.WriteString(strings.ReplaceAll(literal, "'", "''"))
	builder.WriteByte('\'')
}

func compileCoalesceScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	if expression == nil {
		return compiledScalar{}, errors.New("compile ClickHouse coalesce: missing expression")
	}
	if len(expression.Arguments) == 0 {
		return compiledScalar{}, errors.New("compile ClickHouse coalesce: requires at least one argument")
	}
	if len(expression.Arguments) > spl.MaximumCoalesceArguments {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"coalesce contains more than %d arguments",
				spl.MaximumCoalesceArguments,
			),
			Range: expression.Range,
		}
	}

	values := make([]compiledScalar, 0, len(expression.Arguments))
	alwaysNull := true
	ieeeComparison := false
	materializeForPredicate := false
	sqlBytes := len("coalesce()")
	for _, argument := range expression.Arguments {
		if nilScalarExpression(argument) {
			return compiledScalar{}, errors.New("compile ClickHouse coalesce: missing argument")
		}
		value, err := compileScalarValue(argument, state)
		if err != nil {
			return compiledScalar{}, err
		}
		values = append(values, value)
		alwaysNull = alwaysNull && compiledScalarIsAlwaysNull(value)
		ieeeComparison = ieeeComparison || value.ieeeComparison
		materializeForPredicate = materializeForPredicate || value.materializeForPredicate
		sqlBytes += len(value.valueSQL) + len(", ")
		if err := validateCoalesceScalarSQLBytes(sqlBytes, expression.Range); err != nil {
			return compiledScalar{}, err
		}
	}

	textEligibleSQL, ok := coalesceTextEligibility(values)
	if !ok {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_UNSUPPORTED_COALESCE_VALUE_TYPE",
			Message: "coalesce values carry incompatible text provenance; use matching text sources " +
				"or normalize each value before coalescing",
			Range: expression.Range,
		}
	}
	semanticBytesSQL, semanticBytesArgs, stringOrBytes, stringOrBytesNullable :=
		coalesceSemanticBytes(values)
	values, kind, numberType, err := normalizeCoalesceValues(values, expression.Range)
	if err != nil {
		return compiledScalar{}, err
	}

	valueSQL := make([]string, 0, len(values))
	args := make([]any, 0)
	for _, value := range values {
		valueSQL = append(valueSQL, value.valueSQL)
		args = append(args, value.valueArgs...)
	}
	sqlBytes = len("coalesce()") + len(strings.Join(valueSQL, ", "))
	if err := validateCoalesceScalarSQLBytes(sqlBytes, expression.Range); err != nil {
		return compiledScalar{}, err
	}
	return compiledScalar{
		valueSQL:                    "coalesce(" + strings.Join(valueSQL, ", ") + ")",
		valueArgs:                   args,
		maxStringBytes:              maximumCompiledScalarStringByteBound(values...),
		existsSQL:                   "1",
		textEligibleSQL:             textEligibleSQL,
		semanticBytesSQL:            semanticBytesSQL,
		semanticBytesArgs:           semanticBytesArgs,
		semanticBytesByUTF8Validity: kind == fieldKindString && semanticBytesValidityOnly(values...),
		textEligibleBySemanticBytes: kind == fieldKindString && stringOrBytes,
		stringOrBytes:               kind == fieldKindString && stringOrBytes,
		stringOrBytesNullable:       kind == fieldKindString && stringOrBytesNullable,
		kind:                        kind,
		numberType:                  numberType,
		alwaysNull:                  alwaysNull,
		ieeeComparison:              ieeeComparison,
		materializeForPredicate:     materializeForPredicate,
	}, nil
}

func validateCoalesceScalarSQLBytes(size int, sourceRange spl.Range) error {
	if size <= maxCompiledCoalesceScalarSQLBytes {
		return nil
	}
	return &plan.Diagnostic{
		Code:    "SPL_QUERY_TOO_COMPLEX",
		Message: fmt.Sprintf("coalesce scalar SQL exceeds %d bytes", maxCompiledCoalesceScalarSQLBytes),
		Range:   sourceRange,
	}
}

func coalesceTextEligibility(values []compiledScalar) (string, bool) {
	textEligibleSQL := ""
	found := false
	for _, value := range values {
		if compiledScalarIsAlwaysNull(value) {
			continue
		}
		if !found {
			textEligibleSQL = value.textEligibleSQL
			found = true
			continue
		}
		if value.textEligibleSQL != textEligibleSQL {
			return "", false
		}
	}
	return textEligibleSQL, true
}

func compiledScalarSemanticBytes(value compiledScalar) (string, []any) {
	if !value.stringOrBytes || compiledScalarIsAlwaysNull(value) ||
		value.semanticBytesSQL == "" {
		return "toUInt8(0)", nil
	}
	return "toUInt8(ifNull(" + value.semanticBytesSQL + ", 0))",
		append([]any(nil), value.semanticBytesArgs...)
}

func semanticBytesTextEligibilitySQL(valueSQL, semanticBytesSQL string) string {
	return "(ifNull(" + semanticBytesSQL + ", 0) = 0 AND isNotNull(" + valueSQL +
		") AND isValidUTF8(assumeNotNull(" + valueSQL + ")))"
}

func coalesceSemanticBytes(values []compiledScalar) (string, []any, bool, bool) {
	parts := make([]string, 0, len(values)*2+1)
	args := make([]any, 0)
	stringOrBytes := false
	nullable := true
	for _, value := range values {
		if compiledScalarIsAlwaysNull(value) {
			continue
		}
		if !value.stringOrBytesNullable {
			nullable = false
		}
		if !value.stringOrBytes {
			continue
		}
		stringOrBytes = true
		flagSQL, flagArgs := compiledScalarSemanticBytes(value)
		parts = append(parts, "isNotNull("+value.valueSQL+")", flagSQL)
		args = append(args, value.valueArgs...)
		args = append(args, flagArgs...)
	}
	if !stringOrBytes {
		return "", nil, false, false
	}
	parts = append(parts, "toUInt8(0)")
	return "multiIf(" + strings.Join(parts, ", ") + ")", args, true, nullable
}

func semanticBytesValidityOnly(values ...compiledScalar) bool {
	found := false
	for _, value := range values {
		if !value.stringOrBytes || compiledScalarIsAlwaysNull(value) {
			continue
		}
		found = true
		if !value.semanticBytesByUTF8Validity {
			return false
		}
	}
	return found
}

func normalizeCoalesceValues(
	values []compiledScalar,
	sourceRange spl.Range,
) ([]compiledScalar, fieldKind, string, error) {
	return normalizeConditionalValues(values, sourceRange, unsupportedCoalesceValueTypes)
}

func normalizeConditionalValues(
	values []compiledScalar,
	sourceRange spl.Range,
	unsupportedValueTypes func(spl.Range, compiledScalar, compiledScalar) error,
) ([]compiledScalar, fieldKind, string, error) {
	target := compiledScalar{}
	found := false
	for _, value := range values {
		if compiledScalarIsAlwaysNull(value) {
			continue
		}
		if !found {
			target = value
			found = true
			continue
		}
		if !coalesceFixedTypesMatch(target, value) {
			return nil, fieldKindInvalid, "",
				unsupportedValueTypes(sourceRange, target, value)
		}
	}
	if !found {
		normalized := append([]compiledScalar(nil), values...)
		target = typedNullIfBranch(fieldKindString, "")
		for index, value := range normalized {
			if coalesceFixedTypesMatch(value, target) {
				continue
			}
			typed := target
			if len(value.valueArgs) > 0 {
				typed.valueSQL = "CAST(" + value.valueSQL +
					" AS Nullable(String))"
				typed.valueArgs = append([]any(nil), value.valueArgs...)
				typed.materializeForPredicate = value.materializeForPredicate
			}
			normalized[index] = typed
		}
		return normalized, fieldKindString, "", nil
	}
	if !supportedCoalesceFixedType(target) {
		return nil, fieldKindInvalid, "",
			unsupportedValueTypes(sourceRange, target, compiledScalar{})
	}

	normalized := append([]compiledScalar(nil), values...)
	for index, value := range normalized {
		if !compiledScalarIsAlwaysNull(value) {
			continue
		}
		if coalesceFixedTypesMatch(value, target) {
			// Keep a typed null-producing expression intact. Preserving it retains
			// source-order bindings and avoids an evaluation-elision contract.
			continue
		}
		typed, ok := typedNullIfBranchFor(target)
		if !ok {
			return nil, fieldKindInvalid, "",
				unsupportedValueTypes(sourceRange, target, value)
		}
		if len(value.valueArgs) > 0 {
			typed.valueSQL = "CAST(" + value.valueSQL + " AS Nullable(" +
				coalesceFixedTypeSQL(target) + "))"
			typed.valueArgs = append([]any(nil), value.valueArgs...)
			typed.materializeForPredicate = value.materializeForPredicate
		}
		normalized[index] = typed
	}
	return normalized, target.kind, target.numberType, nil
}

func coalesceFixedTypeSQL(value compiledScalar) string {
	switch value.kind {
	case fieldKindBool:
		return "Bool"
	case fieldKindNumber:
		return value.numberType
	default:
		return "String"
	}
}

func supportedCoalesceFixedType(value compiledScalar) bool {
	switch value.kind {
	case fieldKindString, fieldKindBool:
		return true
	case fieldKindNumber:
		return value.numberType != "" && supportedIfNumberType(value.numberType)
	default:
		return false
	}
}

func coalesceFixedTypesMatch(left, right compiledScalar) bool {
	if !supportedCoalesceFixedType(left) || !supportedCoalesceFixedType(right) {
		return false
	}
	if left.kind != right.kind {
		return false
	}
	return left.kind != fieldKindNumber || left.numberType == right.numberType
}

func unsupportedCoalesceValueTypes(
	sourceRange spl.Range,
	left, right compiledScalar,
) error {
	leftType := describeIfBranchType(left)
	rightType := describeIfBranchType(right)
	if right.kind == fieldKindInvalid && !compiledScalarIsAlwaysNull(right) {
		rightType = "unsupported"
	}
	return &plan.Diagnostic{
		Code: "SPL_UNSUPPORTED_COALESCE_VALUE_TYPE",
		Message: fmt.Sprintf(
			"coalesce values have unsupported or unstable types %s and %s; use matching fixed String, Bool, or numeric values",
			leftType,
			rightType,
		),
		Range: sourceRange,
	}
}

func compileCaseScalar(
	expression *plan.ScalarCaseExpression,
	state compileState,
) (compiledScalar, error) {
	if expression == nil {
		return compiledScalar{}, errors.New("compile ClickHouse case: missing expression")
	}
	if len(expression.Branches) == 0 {
		return compiledScalar{}, errors.New(
			"compile ClickHouse case: requires at least one condition/value pair",
		)
	}
	if len(expression.Branches) > spl.MaximumCaseBranches {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"case contains more than %d condition/value pairs",
				spl.MaximumCaseBranches,
			),
			Range: expression.Range,
		}
	}

	conditionSQL := make([]string, 0, len(expression.Branches))
	conditionArgs := make([][]any, 0, len(expression.Branches))
	values := make([]compiledScalar, 0, len(expression.Branches))
	alwaysNull := true
	ieeeComparison := false
	materializeForPredicate := false
	sqlBytes := len("multiIf()")
	for _, branch := range expression.Branches {
		if nilPlanExpression(branch.Condition) {
			return compiledScalar{}, errors.New("compile ClickHouse case: missing condition")
		}
		if nilScalarExpression(branch.Value) {
			return compiledScalar{}, errors.New("compile ClickHouse case: missing value")
		}
		if err := validateCaseCondition(branch.Condition); err != nil {
			return compiledScalar{}, err
		}
		compiledConditionSQL, compiledConditionArgs, err := compileExpression(
			branch.Condition,
			state,
		)
		if err != nil {
			return compiledScalar{}, err
		}
		compiledValue, err := compileScalarValue(branch.Value, state)
		if err != nil {
			return compiledScalar{}, err
		}
		conditionSQL = append(conditionSQL, compiledConditionSQL)
		conditionArgs = append(conditionArgs, compiledConditionArgs)
		values = append(values, compiledValue)
		alwaysNull = alwaysNull && compiledScalarIsAlwaysNull(compiledValue)
		ieeeComparison = ieeeComparison || compiledValue.ieeeComparison
		materializeForPredicate = materializeForPredicate ||
			len(predicateMaterializationFields(branch.Condition, state)) > 0 ||
			compiledValue.materializeForPredicate
		sqlBytes += len("ifNull(, 0), , ") +
			len(compiledConditionSQL) +
			len(compiledValue.valueSQL)
		if err := validateCaseScalarSQLBytes(sqlBytes, expression.Range); err != nil {
			return compiledScalar{}, err
		}
	}

	textEligibleSQL, ok := coalesceTextEligibility(values)
	if !ok {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_UNSUPPORTED_CASE_VALUE_TYPE",
			Message: "case values carry incompatible text provenance; use matching text sources " +
				"or normalize each value before the conditional",
			Range: expression.Range,
		}
	}
	semanticParts := make([]string, 0, len(values)*2+1)
	semanticBytesArgs := make([]any, 0)
	stringOrBytes := false
	for index, value := range values {
		flagSQL, flagArgs := compiledScalarSemanticBytes(value)
		semanticParts = append(
			semanticParts,
			"ifNull("+conditionSQL[index]+", 0)",
			flagSQL,
		)
		semanticBytesArgs = append(semanticBytesArgs, conditionArgs[index]...)
		semanticBytesArgs = append(semanticBytesArgs, flagArgs...)
		stringOrBytes = stringOrBytes || value.stringOrBytes
	}
	semanticBytesSQL := ""
	if stringOrBytes {
		semanticParts = append(semanticParts, "toUInt8(0)")
		semanticBytesSQL = "multiIf(" + strings.Join(semanticParts, ", ") + ")"
	}
	values, kind, numberType, err := normalizeCaseValues(values, expression.Range)
	if err != nil {
		return compiledScalar{}, err
	}
	defaultValue := typedNullIfBranch(kind, numberType)

	parts := make([]string, 0, len(values)*2+1)
	args := make([]any, 0)
	for index, value := range values {
		parts = append(
			parts,
			"ifNull("+conditionSQL[index]+", 0)",
			value.valueSQL,
		)
		args = append(args, conditionArgs[index]...)
		args = append(args, value.valueArgs...)
	}
	parts = append(parts, defaultValue.valueSQL)
	valueSQL := "multiIf(" + strings.Join(parts, ", ") + ")"
	if err := validateCaseScalarSQLBytes(len(valueSQL), expression.Range); err != nil {
		return compiledScalar{}, err
	}
	return compiledScalar{
		valueSQL:                    valueSQL,
		valueArgs:                   args,
		maxStringBytes:              maximumCompiledScalarStringByteBound(values...),
		existsSQL:                   "1",
		textEligibleSQL:             textEligibleSQL,
		semanticBytesSQL:            semanticBytesSQL,
		semanticBytesArgs:           semanticBytesArgs,
		semanticBytesByUTF8Validity: kind == fieldKindString && semanticBytesValidityOnly(values...),
		textEligibleBySemanticBytes: kind == fieldKindString && stringOrBytes,
		stringOrBytes:               kind == fieldKindString && stringOrBytes,
		// case has an implicit NULL default even when its selected String
		// branches carry no Bytes provenance. Preserve that physical nullability
		// for a later concatenation that introduces byte capability.
		stringOrBytesNullable:   kind == fieldKindString,
		kind:                    kind,
		numberType:              numberType,
		alwaysNull:              alwaysNull,
		ieeeComparison:          ieeeComparison,
		materializeForPredicate: materializeForPredicate,
	}, nil
}

func validateCaseScalarSQLBytes(size int, sourceRange spl.Range) error {
	if size <= maxCompiledCaseScalarSQLBytes {
		return nil
	}
	return &plan.Diagnostic{
		Code:    "SPL_QUERY_TOO_COMPLEX",
		Message: fmt.Sprintf("case scalar SQL exceeds %d bytes", maxCompiledCaseScalarSQLBytes),
		Range:   sourceRange,
	}
}

func normalizeCaseValues(
	values []compiledScalar,
	sourceRange spl.Range,
) ([]compiledScalar, fieldKind, string, error) {
	return normalizeConditionalValues(values, sourceRange, unsupportedCaseValueTypes)
}

func unsupportedCaseValueTypes(
	sourceRange spl.Range,
	left, right compiledScalar,
) error {
	leftType := describeIfBranchType(left)
	rightType := describeIfBranchType(right)
	if right.kind == fieldKindInvalid && !compiledScalarIsAlwaysNull(right) {
		rightType = "unsupported"
	}
	return &plan.Diagnostic{
		Code: "SPL_UNSUPPORTED_CASE_VALUE_TYPE",
		Message: fmt.Sprintf(
			"case values have unsupported or unstable types %s and %s; use matching fixed String, Bool, or numeric values",
			leftType,
			rightType,
		),
		Range: sourceRange,
	}
}

func compileIfScalar(expression *plan.ScalarIfExpression, state compileState) (compiledScalar, error) {
	if expression == nil {
		return compiledScalar{}, errors.New("compile ClickHouse if: missing if expression")
	}
	if nilPlanExpression(expression.Condition) {
		return compiledScalar{}, errors.New("compile ClickHouse if: missing condition")
	}
	if nilScalarExpression(expression.True) {
		return compiledScalar{}, errors.New("compile ClickHouse if: missing true branch")
	}
	if nilScalarExpression(expression.False) {
		return compiledScalar{}, errors.New("compile ClickHouse if: missing false branch")
	}
	if err := validateIfCondition(expression.Condition); err != nil {
		return compiledScalar{}, err
	}

	conditionSQL, conditionArgs, err := compileExpression(expression.Condition, state)
	if err != nil {
		return compiledScalar{}, err
	}
	if sizeErr := validateIfScalarSQLBytes(len(conditionSQL), expression.Range); sizeErr != nil {
		return compiledScalar{}, sizeErr
	}
	trueValue, err := compileScalarValue(expression.True, state)
	if err != nil {
		return compiledScalar{}, err
	}
	if sizeErr := validateIfScalarSQLBytes(len(trueValue.valueSQL), expression.Range); sizeErr != nil {
		return compiledScalar{}, sizeErr
	}
	falseValue, err := compileScalarValue(expression.False, state)
	if err != nil {
		return compiledScalar{}, err
	}
	if sizeErr := validateIfScalarSQLBytes(len(falseValue.valueSQL), expression.Range); sizeErr != nil {
		return compiledScalar{}, sizeErr
	}
	alwaysNull := compiledScalarIsAlwaysNull(trueValue) &&
		compiledScalarIsAlwaysNull(falseValue)
	textEligibleSQL, ok := ifBranchTextEligibility(trueValue, falseValue)
	if !ok {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_UNSUPPORTED_IF_BRANCH_TYPE",
			Message: "if branches carry incompatible text provenance; use matching text sources " +
				"or normalize each branch before the conditional",
			Range: expression.Range,
		}
	}
	trueSemanticSQL, trueSemanticArgs := compiledScalarSemanticBytes(trueValue)
	falseSemanticSQL, falseSemanticArgs := compiledScalarSemanticBytes(falseValue)
	stringOrBytes := trueValue.stringOrBytes || falseValue.stringOrBytes
	semanticBytesSQL := ""
	semanticBytesArgs := make([]any, 0)
	if stringOrBytes {
		semanticBytesSQL = "if(ifNull(" + conditionSQL + ", 0), " +
			trueSemanticSQL + ", " + falseSemanticSQL + ")"
		semanticBytesArgs = append(semanticBytesArgs, conditionArgs...)
		semanticBytesArgs = append(semanticBytesArgs, trueSemanticArgs...)
		semanticBytesArgs = append(semanticBytesArgs, falseSemanticArgs...)
	}
	stringOrBytesNullable := compiledScalarIsAlwaysNull(trueValue) ||
		trueValue.stringOrBytesNullable || compiledScalarIsAlwaysNull(falseValue) ||
		falseValue.stringOrBytesNullable
	trueValue, falseValue, kind, numberType, err := normalizeIfBranches(
		trueValue,
		falseValue,
		expression.Range,
	)
	if err != nil {
		return compiledScalar{}, err
	}

	valueSQLBytes := len("if(ifNull(, 0), , )") +
		len(conditionSQL) + len(trueValue.valueSQL) + len(falseValue.valueSQL)
	if err := validateIfScalarSQLBytes(valueSQLBytes, expression.Range); err != nil {
		return compiledScalar{}, err
	}
	args := make([]any, 0, len(conditionArgs)+len(trueValue.valueArgs)+len(falseValue.valueArgs))
	args = append(args, conditionArgs...)
	args = append(args, trueValue.valueArgs...)
	args = append(args, falseValue.valueArgs...)
	return compiledScalar{
		valueSQL: "if(ifNull(" + conditionSQL + ", 0), " +
			trueValue.valueSQL + ", " + falseValue.valueSQL + ")",
		valueArgs: args,
		maxStringBytes: maximumCompiledScalarStringByteBound(
			trueValue,
			falseValue,
		),
		existsSQL:         "1",
		textEligibleSQL:   textEligibleSQL,
		semanticBytesSQL:  semanticBytesSQL,
		semanticBytesArgs: semanticBytesArgs,
		semanticBytesByUTF8Validity: kind == fieldKindString &&
			semanticBytesValidityOnly(trueValue, falseValue),
		textEligibleBySemanticBytes: kind == fieldKindString && stringOrBytes,
		stringOrBytes:               kind == fieldKindString && stringOrBytes,
		stringOrBytesNullable:       kind == fieldKindString && stringOrBytesNullable,
		kind:                        kind,
		numberType:                  numberType,
		alwaysNull:                  alwaysNull,
		ieeeComparison:              trueValue.ieeeComparison || falseValue.ieeeComparison,
		materializeForPredicate: len(predicateMaterializationFields(expression.Condition, state)) > 0 ||
			trueValue.materializeForPredicate ||
			falseValue.materializeForPredicate,
	}, nil
}

func validateIfScalarSQLBytes(size int, sourceRange spl.Range) error {
	if size <= maxCompiledIfScalarSQLBytes {
		return nil
	}
	return &plan.Diagnostic{
		Code:    "SPL_QUERY_TOO_COMPLEX",
		Message: fmt.Sprintf("if scalar SQL exceeds %d bytes", maxCompiledIfScalarSQLBytes),
		Range:   sourceRange,
	}
}

func ifBranchTextEligibility(trueValue, falseValue compiledScalar) (string, bool) {
	switch {
	case compiledScalarIsAlwaysNull(trueValue):
		return falseValue.textEligibleSQL, true
	case compiledScalarIsAlwaysNull(falseValue):
		return trueValue.textEligibleSQL, true
	case trueValue.textEligibleSQL == falseValue.textEligibleSQL:
		return trueValue.textEligibleSQL, true
	default:
		return "", false
	}
}

func validateIfCondition(expression plan.Expression) error {
	validator := predicateComplexityValidator{
		active: make(map[any]struct{}),
	}
	if err := validator.validateExpression(expression, 1); err != nil {
		return err
	}
	return validateIfConditionStructure(expression)
}

func validateIfConditionStructure(expression plan.Expression) error {
	if nilPlanExpression(expression) {
		return errors.New("compile ClickHouse if: missing condition")
	}
	switch expression := expression.(type) {
	case *plan.BooleanExpression:
		if expression.Op != plan.BooleanOpAnd && expression.Op != plan.BooleanOpOr {
			return errors.New("compile ClickHouse if: condition has an invalid Boolean operator")
		}
		if err := validateIfConditionStructure(expression.Left); err != nil {
			return err
		}
		return validateIfConditionStructure(expression.Right)
	case *plan.NotExpression:
		return validateIfConditionStructure(expression.Operand)
	case *plan.EvalComparisonExpression:
		if nilScalarExpression(expression.Left) || nilScalarExpression(expression.Right) {
			return errors.New("compile ClickHouse if: comparison condition has a missing scalar operand")
		}
		if !validComparisonOp(expression.Op) {
			return errors.New("compile ClickHouse if: condition has an invalid comparison operator")
		}
		if err := validatePredicateScalarStructure(expression.Left); err != nil {
			return err
		}
		if err := validatePredicateScalarStructure(expression.Right); err != nil {
			return err
		}
		return nil
	case *plan.MembershipExpression:
		return validateMembershipStructure("if", expression)
	case *plan.ScalarPredicateExpression:
		if nilScalarExpression(expression.Value) {
			return errors.New("compile ClickHouse if: scalar condition is missing")
		}
		if err := validatePredicateScalarStructure(expression.Value); err != nil {
			return err
		}
		if !scalarExpressionReturnsBoolean(expression.Value) {
			return errors.New("compile ClickHouse if: scalar condition must return Boolean")
		}
		return nil
	default:
		return fmt.Errorf(
			"compile ClickHouse if: condition must be an eval/where predicate, got %T",
			expression,
		)
	}
}

func validateCaseCondition(expression plan.Expression) error {
	validator := predicateComplexityValidator{
		active: make(map[any]struct{}),
	}
	if err := validator.validateExpression(expression, 1); err != nil {
		return err
	}
	return validateCaseConditionStructure(expression)
}

func validateCaseConditionStructure(expression plan.Expression) error {
	if nilPlanExpression(expression) {
		return errors.New("compile ClickHouse case: missing condition")
	}
	switch expression := expression.(type) {
	case *plan.BooleanExpression:
		if expression.Op != plan.BooleanOpAnd && expression.Op != plan.BooleanOpOr {
			return errors.New("compile ClickHouse case: condition has an invalid Boolean operator")
		}
		if err := validateCaseConditionStructure(expression.Left); err != nil {
			return err
		}
		return validateCaseConditionStructure(expression.Right)
	case *plan.NotExpression:
		return validateCaseConditionStructure(expression.Operand)
	case *plan.EvalComparisonExpression:
		if nilScalarExpression(expression.Left) || nilScalarExpression(expression.Right) {
			return errors.New(
				"compile ClickHouse case: comparison condition has a missing scalar operand",
			)
		}
		if !validComparisonOp(expression.Op) {
			return errors.New(
				"compile ClickHouse case: condition has an invalid comparison operator",
			)
		}
		if err := validatePredicateScalarStructure(expression.Left); err != nil {
			return err
		}
		return validatePredicateScalarStructure(expression.Right)
	case *plan.MembershipExpression:
		return validateMembershipStructure("case", expression)
	case *plan.ScalarPredicateExpression:
		if nilScalarExpression(expression.Value) {
			return errors.New("compile ClickHouse case: scalar condition is missing")
		}
		if err := validatePredicateScalarStructure(expression.Value); err != nil {
			return err
		}
		if !scalarExpressionReturnsBoolean(expression.Value) {
			return errors.New("compile ClickHouse case: scalar condition must return Boolean")
		}
		return nil
	default:
		return fmt.Errorf(
			"compile ClickHouse case: condition must be an eval/where predicate, got %T",
			expression,
		)
	}
}

type predicateComplexityValidator struct {
	nodes                int
	arithmeticOperators  int
	membershipCandidates int
	active               map[any]struct{}
}

func validateCompiledPredicateComplexity(expression plan.Expression) error {
	validator := predicateComplexityValidator{
		active: make(map[any]struct{}),
	}
	return validator.validateExpression(expression, 1)
}

func validateCompiledScalarComplexity(expression plan.ScalarExpression) error {
	validator := predicateComplexityValidator{
		active: make(map[any]struct{}),
	}
	return validator.validateScalar(expression, 1)
}

func (v *predicateComplexityValidator) validateExpression(
	expression plan.Expression,
	depth int,
) error {
	if nilPlanExpression(expression) {
		return nil
	}
	if err := v.enter(expression, depth, expression.SourceRange()); err != nil {
		return err
	}
	defer v.leave(expression)

	switch expression := expression.(type) {
	case *plan.BooleanExpression:
		if err := v.validateExpression(expression.Left, depth+1); err != nil {
			return err
		}
		return v.validateExpression(expression.Right, depth+1)
	case *plan.NotExpression:
		return v.validateExpression(expression.Operand, depth+1)
	case *plan.EvalComparisonExpression:
		if err := v.validateScalar(expression.Left, depth+1); err != nil {
			return err
		}
		return v.validateScalar(expression.Right, depth+1)
	case *plan.MembershipExpression:
		if len(expression.Candidates) < 1 ||
			len(expression.Candidates) > spl.MaximumMembershipCandidates {
			return predicateComplexityError(
				fmt.Sprintf(
					"membership requires 1 through %d candidates",
					spl.MaximumMembershipCandidates,
				),
				expression.Range,
			)
		}
		if v.membershipCandidates >
			spl.MaximumMembershipCandidatesPerQuery-len(expression.Candidates) {
			return predicateComplexityError(
				fmt.Sprintf(
					"predicate contains more than %d membership candidates",
					spl.MaximumMembershipCandidatesPerQuery,
				),
				expression.Range,
			)
		}
		v.membershipCandidates += len(expression.Candidates)
		if err := v.validateScalar(expression.Value, depth+1); err != nil {
			return err
		}
		for _, candidate := range expression.Candidates {
			if err := v.validateScalar(candidate, depth+1); err != nil {
				return err
			}
		}
		return nil
	case *plan.ScalarPredicateExpression:
		return v.validateScalar(expression.Value, depth+1)
	default:
		return nil
	}
}

func (v *predicateComplexityValidator) validateScalar(
	expression plan.ScalarExpression,
	depth int,
) error {
	if nilScalarExpression(expression) {
		return nil
	}
	if err := v.enter(expression, depth, expression.SourceRange()); err != nil {
		return err
	}
	defer v.leave(expression)

	switch expression := expression.(type) {
	case *plan.ScalarUnaryExpression:
		if !validCompiledScalarUnaryOp(expression.Op) {
			return errors.New("compile ClickHouse predicate: invalid unary arithmetic operator")
		}
		v.arithmeticOperators++
		if v.arithmeticOperators > spl.MaximumArithmeticOperatorsPerQuery {
			return predicateComplexityError(
				fmt.Sprintf(
					"predicate contains more than %d arithmetic operators",
					spl.MaximumArithmeticOperatorsPerQuery,
				),
				expression.Range,
			)
		}
		if compiledUnaryArithmeticChainLength(expression) > spl.MaximumUnaryOperatorChain {
			return predicateComplexityError(
				fmt.Sprintf(
					"unary arithmetic nesting exceeds %d operators",
					spl.MaximumUnaryOperatorChain,
				),
				expression.Range,
			)
		}
		return v.validateScalar(expression.Operand, depth+1)
	case *plan.ScalarBinaryExpression:
		if !validCompiledScalarBinaryOp(expression.Op) {
			return errors.New("compile ClickHouse predicate: invalid binary arithmetic operator")
		}
		v.arithmeticOperators++
		if v.arithmeticOperators > spl.MaximumArithmeticOperatorsPerQuery {
			return predicateComplexityError(
				fmt.Sprintf(
					"predicate contains more than %d arithmetic operators",
					spl.MaximumArithmeticOperatorsPerQuery,
				),
				expression.Range,
			)
		}
		if err := v.validateScalar(expression.Left, depth+1); err != nil {
			return err
		}
		return v.validateScalar(expression.Right, depth+1)
	case *plan.ScalarCallExpression:
		if len(expression.Arguments) > maxCompiledPredicateNodes {
			return predicateComplexityError(
				"predicate scalar call exceeds the structural node budget",
				expression.Range,
			)
		}
		for _, argument := range expression.Arguments {
			if err := v.validateScalar(argument, depth+1); err != nil {
				return err
			}
		}
	case *plan.ScalarIfExpression:
		if err := v.validateExpression(expression.Condition, depth+1); err != nil {
			return err
		}
		if err := v.validateScalar(expression.True, depth+1); err != nil {
			return err
		}
		return v.validateScalar(expression.False, depth+1)
	case *plan.ScalarCaseExpression:
		if len(expression.Branches) > spl.MaximumCaseBranches {
			return predicateComplexityError(
				fmt.Sprintf(
					"case contains more than %d condition/value pairs",
					spl.MaximumCaseBranches,
				),
				expression.Range,
			)
		}
		for _, branch := range expression.Branches {
			if err := v.validateExpression(branch.Condition, depth+1); err != nil {
				return err
			}
			if err := v.validateScalar(branch.Value, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *predicateComplexityValidator) enter(
	node any,
	depth int,
	sourceRange spl.Range,
) error {
	if depth > maxCompiledPredicateDepth {
		return predicateComplexityError(
			fmt.Sprintf("predicate nesting exceeds %d levels", maxCompiledPredicateDepth),
			sourceRange,
		)
	}
	if _, cyclic := v.active[node]; cyclic {
		return predicateComplexityError(
			"predicate expression graph contains a cycle",
			sourceRange,
		)
	}
	// Count occurrences rather than unique pointers. Later compilation walks
	// every occurrence, so memoizing a shared DAG here would let a tiny forged
	// graph expand exponentially after validation.
	v.nodes++
	if v.nodes > maxCompiledPredicateNodes {
		return predicateComplexityError(
			fmt.Sprintf("predicate contains more than %d structural nodes", maxCompiledPredicateNodes),
			sourceRange,
		)
	}
	v.active[node] = struct{}{}
	return nil
}

func (v *predicateComplexityValidator) leave(node any) {
	delete(v.active, node)
}

func predicateComplexityError(message string, sourceRange spl.Range) error {
	return &plan.Diagnostic{
		Code:    "SPL_QUERY_TOO_COMPLEX",
		Message: message,
		Range:   sourceRange,
	}
}

func validComparisonOp(op plan.ComparisonOp) bool {
	switch op {
	case plan.ComparisonOpEqual,
		plan.ComparisonOpNotEqual,
		plan.ComparisonOpLess,
		plan.ComparisonOpLessEqual,
		plan.ComparisonOpGreater,
		plan.ComparisonOpGreaterEqual:
		return true
	default:
		return false
	}
}

func validatePredicateScalarStructure(expression plan.ScalarExpression) error {
	if nilScalarExpression(expression) {
		return errors.New("compile ClickHouse predicate: missing scalar expression")
	}
	switch expression := expression.(type) {
	case *plan.ScalarUnaryExpression:
		if !validCompiledScalarUnaryOp(expression.Op) {
			return errors.New("compile ClickHouse predicate: invalid unary arithmetic operator")
		}
		return validatePredicateScalarStructure(expression.Operand)
	case *plan.ScalarBinaryExpression:
		if !validCompiledScalarBinaryOp(expression.Op) {
			return errors.New("compile ClickHouse predicate: invalid binary arithmetic operator")
		}
		if err := validatePredicateScalarStructure(expression.Left); err != nil {
			return err
		}
		return validatePredicateScalarStructure(expression.Right)
	case *plan.ScalarFieldExpression:
		return validateCanonicalFieldRef("predicate", "scalar", expression.Field)
	case *plan.ScalarLiteralExpression:
		switch expression.Value.Kind {
		case plan.ValueKindNull,
			plan.ValueKindString,
			plan.ValueKindInt64,
			plan.ValueKindUint64,
			plan.ValueKindFloat64,
			plan.ValueKindBool:
			return nil
		default:
			return errors.New("compile ClickHouse predicate: scalar literal has an invalid kind")
		}
	case *plan.ScalarCallExpression:
		expectedArguments := 0
		hasExactArity := false
		switch expression.Function {
		case plan.ScalarFunctionNow:
			hasExactArity = true
		case plan.ScalarFunctionToNumber,
			plan.ScalarFunctionIsNull,
			plan.ScalarFunctionIsNotNull,
			plan.ScalarFunctionLower,
			plan.ScalarFunctionUpper,
			plan.ScalarFunctionLength,
			plan.ScalarFunctionCeil,
			plan.ScalarFunctionFloor,
			plan.ScalarFunctionMVCount,
			plan.ScalarFunctionMVSort:
			expectedArguments = 1
			hasExactArity = true
		case plan.ScalarFunctionToString:
			if len(expression.Arguments) < 1 || len(expression.Arguments) > 2 {
				return errors.New(
					"compile ClickHouse predicate: tostring requires one or two arguments",
				)
			}
			if len(expression.Arguments) == 2 {
				format, ok := scalarQuotedStringLiteral(expression.Arguments[1])
				if !ok || !slices.Contains(spl.SupportedToStringFormats, format) {
					return errors.New(
						"compile ClickHouse predicate: tostring format must be a supported quoted string literal",
					)
				}
			}
		case plan.ScalarFunctionMVDedup:
			expectedArguments = 1
			hasExactArity = true
		case plan.ScalarFunctionSplit, plan.ScalarFunctionMVJoin:
			if len(expression.Arguments) != 2 {
				return fmt.Errorf(
					"compile ClickHouse predicate: scalar function %d requires exactly two arguments",
					expression.Function,
				)
			}
			delimiter, ok := scalarQuotedStringLiteral(expression.Arguments[1])
			if !ok || !utf8.ValidString(delimiter) ||
				len(delimiter) > spl.MaximumMVDelimiterBytes {
				return errors.New(
					"compile ClickHouse predicate: multivalue delimiter must be a bounded quoted UTF-8 string literal",
				)
			}
		case plan.ScalarFunctionMVAppend:
			if len(expression.Arguments) == 0 ||
				len(expression.Arguments) > spl.MaximumMVAppendArguments {
				return fmt.Errorf(
					"compile ClickHouse predicate: mvappend requires one through %d arguments",
					spl.MaximumMVAppendArguments,
				)
			}
		case plan.ScalarFunctionMVIndex:
			if len(expression.Arguments) < 2 || len(expression.Arguments) > 3 {
				return errors.New(
					"compile ClickHouse predicate: mvindex requires two or three arguments",
				)
			}
			for index := 1; index < len(expression.Arguments); index++ {
				if _, ok := signedMVIndexLiteral(expression.Arguments[index]); !ok {
					return errors.New(
						"compile ClickHouse predicate: mvindex indexes must be signed 32-bit integer literals",
					)
				}
			}
		case plan.ScalarFunctionMVZip:
			if len(expression.Arguments) < 2 || len(expression.Arguments) > 3 {
				return errors.New(
					"compile ClickHouse predicate: mvzip requires two or three arguments",
				)
			}
			if len(expression.Arguments) == 3 {
				delimiter, ok := scalarQuotedStringLiteral(expression.Arguments[2])
				if !ok || !utf8.ValidString(delimiter) ||
					len(delimiter) > spl.MaximumMVDelimiterBytes {
					return errors.New(
						"compile ClickHouse predicate: mvzip delimiter must be a bounded quoted UTF-8 string literal",
					)
				}
			}
		case plan.ScalarFunctionMVFind:
			if len(expression.Arguments) != 2 {
				return errors.New(
					"compile ClickHouse predicate: mvfind requires exactly two arguments",
				)
			}
			if _, ok := scalarQuotedStringLiteral(expression.Arguments[1]); !ok {
				return errors.New(
					"compile ClickHouse predicate: mvfind regular expression must be a quoted string literal",
				)
			}
		case plan.ScalarFunctionAbs,
			plan.ScalarFunctionSqrt,
			plan.ScalarFunctionExp,
			plan.ScalarFunctionLn,
			plan.ScalarFunctionURLDecode,
			plan.ScalarFunctionMD5,
			plan.ScalarFunctionSHA1,
			plan.ScalarFunctionSHA256,
			plan.ScalarFunctionSHA512,
			plan.ScalarFunctionTypeOf:
			expectedArguments = 1
			hasExactArity = true
		case plan.ScalarFunctionPow:
			expectedArguments = 2
			hasExactArity = true
		case plan.ScalarFunctionPi:
			hasExactArity = true
		case plan.ScalarFunctionLog:
			if len(expression.Arguments) < 1 || len(expression.Arguments) > 2 {
				return errors.New(
					"compile ClickHouse predicate: log requires one or two arguments",
				)
			}
		case plan.ScalarFunctionTrim,
			plan.ScalarFunctionLTrim,
			plan.ScalarFunctionRTrim:
			if len(expression.Arguments) < 1 || len(expression.Arguments) > 2 {
				return errors.New(
					"compile ClickHouse predicate: trim requires one or two arguments",
				)
			}
			if len(expression.Arguments) == 2 {
				characters, ok := scalarQuotedStringLiteral(expression.Arguments[1])
				if !ok || characters == "" || !utf8.ValidString(characters) ||
					len(characters) > spl.MaximumTrimCharactersBytes {
					return errors.New(
						"compile ClickHouse predicate: trim characters must be a bounded non-empty quoted UTF-8 string literal",
					)
				}
			}
		case plan.ScalarFunctionCIDRMatch:
			if len(expression.Arguments) != 2 {
				return errors.New(
					"compile ClickHouse predicate: cidrmatch requires exactly two arguments",
				)
			}
			prefix, ok := scalarQuotedStringLiteral(expression.Arguments[0])
			if !ok {
				return errors.New(
					"compile ClickHouse predicate: cidrmatch prefix must be a quoted string literal",
				)
			}
			if _, err := netip.ParsePrefix(prefix); err != nil {
				return errors.New(
					"compile ClickHouse predicate: cidrmatch prefix must be a CIDR block",
				)
			}
		case plan.ScalarFunctionMatch:
			if len(expression.Arguments) != 2 {
				return errors.New(
					"compile ClickHouse predicate: match requires exactly two arguments",
				)
			}
			if nilScalarExpression(expression.Arguments[1]) {
				return errors.New(
					"compile ClickHouse predicate: match has a missing regular expression",
				)
			}
			_, ok := scalarQuotedStringLiteral(expression.Arguments[1])
			if !ok {
				return errors.New(
					"compile ClickHouse predicate: match regular expression must be a string literal",
				)
			}
		case plan.ScalarFunctionLike:
			if len(expression.Arguments) != 2 {
				return errors.New(
					"compile ClickHouse predicate: like requires exactly two arguments",
				)
			}
			if nilScalarExpression(expression.Arguments[1]) {
				return errors.New(
					"compile ClickHouse predicate: like has a missing pattern",
				)
			}
			_, ok := scalarQuotedStringLiteral(expression.Arguments[1])
			if !ok {
				return errors.New(
					"compile ClickHouse predicate: like pattern must be a string literal",
				)
			}
		case plan.ScalarFunctionStrftime:
			if len(expression.Arguments) != 2 {
				return errors.New(
					"compile ClickHouse predicate: strftime requires exactly two arguments",
				)
			}
			if nilScalarExpression(expression.Arguments[1]) {
				return errors.New(
					"compile ClickHouse predicate: strftime has a missing format",
				)
			}
			_, ok := scalarQuotedStringLiteral(expression.Arguments[1])
			if !ok {
				return errors.New(
					"compile ClickHouse predicate: strftime format must be a quoted string literal",
				)
			}
		case plan.ScalarFunctionStrptime:
			if len(expression.Arguments) != 2 {
				return errors.New(
					"compile ClickHouse predicate: strptime requires exactly two arguments",
				)
			}
			if nilScalarExpression(expression.Arguments[1]) {
				return errors.New(
					"compile ClickHouse predicate: strptime has a missing format",
				)
			}
			_, ok := scalarQuotedStringLiteral(expression.Arguments[1])
			if !ok {
				return errors.New(
					"compile ClickHouse predicate: strptime format must be a quoted string literal",
				)
			}
		case plan.ScalarFunctionRelativeTime:
			if len(expression.Arguments) != 2 {
				return errors.New(
					"compile ClickHouse predicate: relative_time requires exactly two arguments",
				)
			}
			if nilScalarExpression(expression.Arguments[1]) {
				return errors.New(
					"compile ClickHouse predicate: relative_time has a missing specifier",
				)
			}
			_, ok := scalarQuotedStringLiteral(expression.Arguments[1])
			if !ok {
				return errors.New(
					"compile ClickHouse predicate: relative_time specifier must be a quoted string literal",
				)
			}
		case plan.ScalarFunctionRound:
			if len(expression.Arguments) < 1 || len(expression.Arguments) > 2 {
				return errors.New(
					"compile ClickHouse predicate: round requires one or two arguments",
				)
			}
			if len(expression.Arguments) == 2 {
				if nilScalarExpression(expression.Arguments[1]) {
					return errors.New(
						"compile ClickHouse predicate: round has a missing precision",
					)
				}
				if _, err := roundPrecisionLiteral(
					expression.Arguments[1],
				); err != nil {
					return fmt.Errorf(
						"compile ClickHouse predicate: %w",
						err,
					)
				}
			}
		case plan.ScalarFunctionReplace:
			expectedArguments = 3
			hasExactArity = true
		case plan.ScalarFunctionSubstring:
			if len(expression.Arguments) < 2 || len(expression.Arguments) > 3 {
				return errors.New(
					"compile ClickHouse predicate: substr requires two or three arguments",
				)
			}
			for index := 1; index < len(expression.Arguments); index++ {
				if nilScalarExpression(expression.Arguments[index]) {
					return errors.New(
						"compile ClickHouse predicate: substr has a missing index",
					)
				}
				if _, ok := scalarIntegerLiteral(expression.Arguments[index]); !ok {
					return errors.New(
						"compile ClickHouse predicate: substr indexes must be literal integers",
					)
				}
			}
		case plan.ScalarFunctionCoalesce:
			if len(expression.Arguments) == 0 {
				return errors.New(
					"compile ClickHouse predicate: coalesce requires at least one argument",
				)
			}
			if len(expression.Arguments) > spl.MaximumCoalesceArguments {
				return fmt.Errorf(
					"compile ClickHouse predicate: coalesce contains more than %d arguments",
					spl.MaximumCoalesceArguments,
				)
			}
		case plan.ScalarFunctionConcat:
			if len(expression.Arguments) < 2 {
				return errors.New(
					"compile ClickHouse predicate: concatenation requires at least two operands",
				)
			}
			if len(expression.Arguments) > spl.MaximumConcatenationOperands {
				return fmt.Errorf(
					"compile ClickHouse predicate: concatenation contains more than %d operands",
					spl.MaximumConcatenationOperands,
				)
			}
		default:
			return fmt.Errorf(
				"compile ClickHouse predicate: unsupported scalar function %d",
				expression.Function,
			)
		}
		if hasExactArity && len(expression.Arguments) != expectedArguments {
			if expectedArguments == 0 {
				return errors.New(
					"compile ClickHouse predicate: now requires no arguments",
				)
			}
			return fmt.Errorf(
				"compile ClickHouse predicate: scalar function %d requires %d arguments",
				expression.Function,
				expectedArguments,
			)
		}
		for _, argument := range expression.Arguments {
			if err := validatePredicateScalarStructure(argument); err != nil {
				return err
			}
		}
		return nil
	case *plan.ScalarIfExpression:
		if err := validateIfConditionStructure(expression.Condition); err != nil {
			return err
		}
		if err := validatePredicateScalarStructure(expression.True); err != nil {
			return err
		}
		return validatePredicateScalarStructure(expression.False)
	case *plan.ScalarCaseExpression:
		if len(expression.Branches) == 0 {
			return errors.New(
				"compile ClickHouse predicate: case requires at least one condition/value pair",
			)
		}
		if len(expression.Branches) > spl.MaximumCaseBranches {
			return fmt.Errorf(
				"compile ClickHouse predicate: case contains more than %d condition/value pairs",
				spl.MaximumCaseBranches,
			)
		}
		for _, branch := range expression.Branches {
			if err := validateCaseConditionStructure(branch.Condition); err != nil {
				return err
			}
			if err := validatePredicateScalarStructure(branch.Value); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf(
			"compile ClickHouse predicate: unsupported scalar expression %T",
			expression,
		)
	}
}

func scalarExpressionReturnsBoolean(expression plan.ScalarExpression) bool {
	switch expression := expression.(type) {
	case *plan.ScalarUnaryExpression, *plan.ScalarBinaryExpression:
		return false
	case *plan.ScalarCallExpression:
		if expression == nil {
			return false
		}
		if expression.Function.ReturnsBoolean() {
			return true
		}
		if expression.Function == plan.ScalarFunctionCoalesce {
			return coalescePlanScalarReturnsBoolean(expression.Arguments)
		}
		return false
	case *plan.ScalarLiteralExpression:
		return expression != nil && expression.Value.Kind == plan.ValueKindBool
	case *plan.ScalarIfExpression:
		return expression != nil &&
			scalarExpressionReturnsBoolean(expression.True) &&
			scalarExpressionReturnsBoolean(expression.False)
	case *plan.ScalarCaseExpression:
		return expression != nil &&
			casePlanScalarReturnsBoolean(expression.Branches)
	default:
		return false
	}
}

func coalescePlanScalarReturnsBoolean(arguments []plan.ScalarExpression) bool {
	foundBoolean := false
	for _, argument := range arguments {
		if literal, ok := argument.(*plan.ScalarLiteralExpression); ok &&
			literal != nil &&
			literal.Value.Kind == plan.ValueKindNull {
			continue
		}
		if !scalarExpressionReturnsBoolean(argument) {
			return false
		}
		foundBoolean = true
	}
	return foundBoolean
}

func casePlanScalarReturnsBoolean(branches []plan.ScalarCaseBranch) bool {
	foundBoolean := false
	for _, branch := range branches {
		if literal, ok := branch.Value.(*plan.ScalarLiteralExpression); ok &&
			literal != nil &&
			literal.Value.Kind == plan.ValueKindNull {
			continue
		}
		if !scalarExpressionReturnsBoolean(branch.Value) {
			return false
		}
		foundBoolean = true
	}
	return foundBoolean
}

func nilPlanExpression(expression plan.Expression) bool {
	if expression == nil {
		return true
	}
	switch expression := expression.(type) {
	case *plan.BooleanExpression:
		return expression == nil
	case *plan.NotExpression:
		return expression == nil
	case *plan.TextExpression:
		return expression == nil
	case *plan.ComparisonExpression:
		return expression == nil
	case *plan.EvalComparisonExpression:
		return expression == nil
	case *plan.MembershipExpression:
		return expression == nil
	case *plan.ScalarPredicateExpression:
		return expression == nil
	default:
		return false
	}
}

func nilScalarExpression(expression plan.ScalarExpression) bool {
	if expression == nil {
		return true
	}
	switch expression := expression.(type) {
	case *plan.ScalarUnaryExpression:
		return expression == nil
	case *plan.ScalarBinaryExpression:
		return expression == nil
	case *plan.ScalarFieldExpression:
		return expression == nil
	case *plan.ScalarLiteralExpression:
		return expression == nil
	case *plan.ScalarCallExpression:
		return expression == nil
	case *plan.ScalarIfExpression:
		return expression == nil
	case *plan.ScalarCaseExpression:
		return expression == nil
	default:
		return false
	}
}

func normalizeIfBranches(
	trueValue, falseValue compiledScalar,
	sourceRange spl.Range,
) (compiledScalar, compiledScalar, fieldKind, string, error) {
	trueNull := compiledScalarIsAlwaysNull(trueValue)
	falseNull := compiledScalarIsAlwaysNull(falseValue)
	if trueNull && falseNull {
		trueValue = typedNullIfBranch(fieldKindString, "")
		falseValue = typedNullIfBranch(fieldKindString, "")
		return trueValue, falseValue, fieldKindString, "", nil
	}
	if trueNull {
		normalized, ok := typedNullIfBranchFor(falseValue)
		if !ok {
			return compiledScalar{}, compiledScalar{}, fieldKindInvalid, "",
				unsupportedIfBranchTypes(sourceRange, trueValue, falseValue)
		}
		trueValue = normalized
	}
	if falseNull {
		normalized, ok := typedNullIfBranchFor(trueValue)
		if !ok {
			return compiledScalar{}, compiledScalar{}, fieldKindInvalid, "",
				unsupportedIfBranchTypes(sourceRange, trueValue, falseValue)
		}
		falseValue = normalized
	}

	switch {
	case trueValue.kind == fieldKindString && falseValue.kind == fieldKindString:
		return trueValue, falseValue, fieldKindString, "", nil
	case trueValue.kind == fieldKindBool && falseValue.kind == fieldKindBool:
		return trueValue, falseValue, fieldKindBool, "", nil
	case trueValue.kind == fieldKindNumber &&
		falseValue.kind == fieldKindNumber &&
		trueValue.numberType != "" &&
		trueValue.numberType == falseValue.numberType &&
		supportedIfNumberType(trueValue.numberType):
		return trueValue, falseValue, fieldKindNumber, trueValue.numberType, nil
	default:
		return compiledScalar{}, compiledScalar{}, fieldKindInvalid, "",
			unsupportedIfBranchTypes(sourceRange, trueValue, falseValue)
	}
}

func compiledScalarIsAlwaysNull(value compiledScalar) bool {
	return value.alwaysNull ||
		(value.literal != nil && value.literal.Kind == plan.ValueKindNull)
}

func typedNullIfBranchFor(value compiledScalar) (compiledScalar, bool) {
	switch value.kind {
	case fieldKindString, fieldKindBool:
		return typedNullIfBranch(value.kind, ""), true
	case fieldKindNumber:
		if value.numberType == "" || !supportedIfNumberType(value.numberType) {
			return compiledScalar{}, false
		}
		return typedNullIfBranch(value.kind, value.numberType), true
	default:
		return compiledScalar{}, false
	}
}

func typedNullIfBranch(kind fieldKind, numberType string) compiledScalar {
	typeSQL := "String"
	switch kind {
	case fieldKindBool:
		typeSQL = "Bool"
	case fieldKindNumber:
		typeSQL = numberType
	}
	return compiledScalar{
		valueSQL:       "CAST(NULL AS Nullable(" + typeSQL + "))",
		maxStringBytes: 1,
		existsSQL:      "1",
		kind:           kind,
		numberType:     numberType,
		alwaysNull:     true,
	}
}

func supportedIfNumberType(numberType string) bool {
	switch numberType {
	case "UInt8", "Int64", "UInt64", "Float64":
		return true
	default:
		return false
	}
}

func unsupportedIfBranchTypes(
	sourceRange spl.Range,
	trueValue, falseValue compiledScalar,
) error {
	return &plan.Diagnostic{
		Code: "SPL_UNSUPPORTED_IF_BRANCH_TYPE",
		Message: fmt.Sprintf(
			"if branches have unsupported or unstable types %s and %s; use matching fixed String, Bool, or numeric branches",
			describeIfBranchType(trueValue),
			describeIfBranchType(falseValue),
		),
		Range: sourceRange,
	}
}

func describeIfBranchType(value compiledScalar) string {
	if compiledScalarIsAlwaysNull(value) {
		return "Null"
	}
	switch value.kind {
	case fieldKindDynamic:
		return "Dynamic"
	case fieldKindString:
		return "String"
	case fieldKindNumber:
		if value.numberType != "" {
			return value.numberType
		}
		return "Number"
	case fieldKindBool:
		return "Bool"
	case fieldKindTime:
		return "Time"
	case fieldKindStringArray:
		return "StringArray"
	case fieldKindDynamicArray:
		return "DynamicArray"
	default:
		return "Invalid"
	}
}

func compileNullTestScalar(expression *plan.ScalarCallExpression, state compileState) (compiledScalar, error) {
	if len(expression.Arguments) != 1 {
		return compiledScalar{}, errors.New("compile ClickHouse null predicate: expected one argument")
	}
	input, err := compileScalarValue(expression.Arguments[0], state)
	if err != nil {
		return compiledScalar{}, err
	}

	presenceSQL, presenceArgs := compiledScalarPresenceSQL(input)
	valueSQL := "CAST(ifNull(" + presenceSQL + ", 0) AS Bool)"
	if expression.Function == plan.ScalarFunctionIsNull {
		valueSQL = "CAST(NOT ifNull(" + presenceSQL + ", 0) AS Bool)"
	}
	return compiledScalar{
		valueSQL:                valueSQL,
		valueArgs:               presenceArgs,
		maxStringBytes:          5,
		existsSQL:               "1",
		kind:                    fieldKindBool,
		materializeForPredicate: input.materializeForPredicate,
	}, nil
}

// compiledScalarPresenceSQL implements SPL's distinction between a missing or
// null value and every present, non-null scalar. Dynamic object parents can be
// represented only by their flattened descendants, so their bounded metadata
// probe also establishes presence. Keep every argument with valueSQL because
// null predicates are complete scalar values whose existsSQL is the constant 1.
func compiledScalarPresenceSQL(value compiledScalar) (string, []any) {
	existsSQL := value.existsSQL
	if existsSQL == "" {
		existsSQL = "1"
	}
	if isNativeMultivalueKind(value.kind) {
		// Native eval/spath/nomv values carry an authoritative list-presence
		// sidecar independent of physical cardinality. It is false for both a
		// missing field and an explicit null, and true for every list, including
		// a present-empty list. The sidecar reuses existsArgs by contract.
		if value.optionalMultivaluePresentSQL != "" {
			presentSQL := value.optionalMultivaluePresentSQL
			presentArgs := append([]any(nil), value.existsArgs...)
			if value.requiresRuntimeValidation {
				// A wrapper such as isnull() consumes only logical presence. Force
				// the guarded list expression through ignore() as well, otherwise
				// ClickHouse could prune unsupported-member or resource checks whose
				// output cardinality does not affect the sidecar.
				presentSQL = bindSQLExpressions(
					[]string{"validated_mv", "mv_present"},
					[]string{value.valueSQL, presentSQL},
					"toUInt8(ifNull(mv_present, 0)) + ignore(validated_mv) != 0",
				)
				presentArgs = append(
					append([]any(nil), value.valueArgs...),
					presentArgs...,
				)
			}
			return presentSQL, presentArgs
		}
		// Fixed multivalue results are physically non-null Array(String), but
		// their canonical empty representation is logically absent in SPL.
		// Calculated arrays without a separate existence predicate must test
		// their members instead of treating isNotNull([]) as presence. Projected
		// arrays already carry a notEmpty(alias) existence predicate and retain
		// the ordinary physical-null check below.
		if existsSQL == "0" {
			return "0", nil
		}
		if existsSQL == "1" {
			return "notEmpty(" + value.valueSQL + ")",
				append([]any(nil), value.valueArgs...)
		}
	}
	presenceSQL := "((" + existsSQL + ") AND isNotNull(" + value.valueSQL + "))"
	args := make([]any, 0, len(value.existsArgs)+len(value.valueArgs)+len(value.descendantArgs))
	args = append(args, value.existsArgs...)
	args = append(args, value.valueArgs...)
	if value.descendantSQL != "" {
		presenceSQL = "(" + presenceSQL + " OR (" + value.descendantSQL + "))"
		args = append(args, value.descendantArgs...)
	}
	return presenceSQL, args
}

// logicalFieldPresenceSQL is the shared non-null SPL presence contract for a
// resolved field. In particular, native multivalue fields must consult their
// sealed list-presence sidecar rather than physical Array nullability or
// cardinality so missing, explicit null, and present-empty remain distinct.
func logicalFieldPresenceSQL(field fieldState) (string, []any) {
	return compiledScalarPresenceSQL(compiledScalarFromField(field))
}

func compileReplaceScalar(expression *plan.ScalarCallExpression, state compileState) (compiledScalar, error) {
	if len(expression.Arguments) != 3 {
		return compiledScalar{}, errors.New("compile ClickHouse replace: expected three arguments")
	}
	if scalarExpressionMayReturnBooleanFunction(expression.Arguments[0]) {
		return compiledScalar{}, booleanScalarConsumerError("replace")
	}
	input, err := compileScalarValue(expression.Arguments[0], state)
	if err != nil {
		return compiledScalar{}, err
	}
	if isNativeMultivalueKind(input.kind) {
		return compiledScalar{}, unsupportedMultivalueUsage("replace", expression.Range)
	}
	pattern, ok := scalarStringLiteral(expression.Arguments[1])
	if !ok {
		return compiledScalar{}, errors.New("compile ClickHouse replace: regular expression must be a string literal")
	}
	if pattern == "" {
		return compiledScalar{}, errors.New("compile ClickHouse replace: empty regular expressions are not supported")
	}
	if err := splregex.ValidateReplacePattern(pattern); err != nil {
		return compiledScalar{}, fmt.Errorf("compile ClickHouse replace: regular expression is outside the supported RE2 subset: %w", err)
	}
	replacement, ok := scalarStringLiteral(expression.Arguments[2])
	if !ok {
		return compiledScalar{}, errors.New("compile ClickHouse replace: replacement must be a string literal")
	}
	inputSQL, inputArgs := compiledStringScalar(input)
	replacementFactor := uint64(len(replacement)) + 1
	return compiledScalar{
		valueSQL:                    "replaceRegexpAll(" + inputSQL + ", ?, ?)",
		valueArgs:                   append(inputArgs, pattern, replacement),
		maxStringBytes:              saturatingStringByteProduct(compiledScalarStringByteBound(input), replacementFactor),
		existsSQL:                   "1",
		textEligibleSQL:             input.textEligibleSQL,
		semanticBytesSQL:            input.semanticBytesSQL,
		semanticBytesArgs:           append([]any(nil), input.semanticBytesArgs...),
		semanticBytesByUTF8Validity: input.semanticBytesByUTF8Validity,
		textEligibleBySemanticBytes: input.textEligibleBySemanticBytes,
		stringOrBytes:               input.stringOrBytes,
		stringOrBytesNullable:       input.stringOrBytesNullable,
		kind:                        fieldKindString,
		materializeForPredicate:     input.materializeForPredicate,
	}, nil
}

func compileBinaryTextPredicateOperands(
	expression *plan.ScalarCallExpression,
	state compileState,
	functionName string,
	patternDescription string,
) (compiledScalar, string, error) {
	if len(expression.Arguments) != 2 {
		return compiledScalar{}, "", fmt.Errorf(
			"compile ClickHouse %s: expected two arguments",
			functionName,
		)
	}
	if scalarExpressionMayReturnBooleanFunction(expression.Arguments[0]) {
		return compiledScalar{}, "", booleanScalarConsumerError(functionName)
	}
	input, err := compileScalarValue(expression.Arguments[0], state)
	if err != nil {
		return compiledScalar{}, "", err
	}
	if isNativeMultivalueKind(input.kind) {
		return compiledScalar{}, "", unsupportedMultivalueUsage(
			functionName,
			expression.Range,
		)
	}
	pattern, ok := scalarQuotedStringLiteral(expression.Arguments[1])
	if !ok {
		return compiledScalar{}, "", fmt.Errorf(
			"compile ClickHouse %s: %s must be a string literal",
			functionName,
			patternDescription,
		)
	}
	return input, pattern, nil
}

func compileBoundedTextPredicateResult(
	input compiledScalar,
	pattern string,
	functionName string,
	maximumInputBytes uint64,
	maximumSQLBytes int,
	sourceRange spl.Range,
) (compiledScalar, error) {
	if input.alwaysNull || input.kind == fieldKindInvalid {
		return compiledScalar{
			valueSQL:       "CAST(NULL AS Nullable(Bool))",
			maxStringBytes: 1,
			existsSQL:      "1",
			kind:           fieldKindBool,
			alwaysNull:     true,
		}, nil
	}
	if compiledScalarStringByteBound(input) > maximumInputBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"%s input may exceed %d bytes after scalar evaluation",
				functionName,
				maximumInputBytes,
			),
			Range: sourceRange,
		}
	}
	inputSQL, inputArgs := compiledTextEligibleStringScalar(input)
	valueSQL := "CAST(" + functionName + "(" + inputSQL + ", ?) AS Nullable(Bool))"
	if len(valueSQL) > maximumSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"%s scalar SQL exceeds %d bytes",
				functionName,
				maximumSQLBytes,
			),
			Range: sourceRange,
		}
	}
	return compiledScalar{
		valueSQL:                valueSQL,
		valueArgs:               append(inputArgs, pattern),
		maxStringBytes:          5,
		existsSQL:               "1",
		kind:                    fieldKindBool,
		materializeForPredicate: input.materializeForPredicate,
	}, nil
}

func compileMatchScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	input, pattern, err := compileBinaryTextPredicateOperands(
		expression,
		state,
		"match",
		"regular expression",
	)
	if err != nil {
		return compiledScalar{}, err
	}
	compiledPattern := splregex.MatchPattern{}
	if state.context != nil {
		compiledPattern = state.context.patternBudgets.match.patterns[expression]
	}
	if compiledPattern.ProgramWorkUnits == 0 {
		compiledPattern, err = compileMatchPatternForBackend(pattern, expression.Range)
		if err != nil {
			return compiledScalar{}, err
		}
		if state.context != nil {
			state.context.patternBudgets.match.patterns[expression] = compiledPattern
		}
	}
	if state.context != nil {
		if compiledPattern.ProgramWorkUnits >
			splregex.MaximumMatchQueryProgramWorkUnits-state.context.patternBudgets.match.programWorkUnits {
			return compiledScalar{}, &plan.Diagnostic{
				Code: "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf(
					"search match programs require more than %d work units",
					splregex.MaximumMatchQueryProgramWorkUnits,
				),
				Range: expression.Range,
			}
		}
		state.context.patternBudgets.match.programWorkUnits += compiledPattern.ProgramWorkUnits
	}
	return compileBoundedTextPredicateResult(
		input,
		compiledPattern.Pattern,
		"match",
		MaximumMatchInputBytes,
		maxCompiledMatchScalarSQLBytes,
		expression.Range,
	)
}

func compileLikeScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	input, pattern, err := compileBinaryTextPredicateOperands(
		expression,
		state,
		"like",
		"pattern",
	)
	if err != nil {
		return compiledScalar{}, err
	}
	compiledPattern := splwildcard.LikePattern{}
	if state.context != nil {
		compiledPattern = state.context.patternBudgets.like.patterns[expression]
	}
	if compiledPattern.WorkUnits == 0 {
		compiledPattern, err = compileLikePatternForBackend(pattern, expression.Range)
		if err != nil {
			return compiledScalar{}, err
		}
		if state.context != nil {
			state.context.patternBudgets.like.patterns[expression] = compiledPattern
		}
	}
	if state.context != nil {
		if compiledPattern.WorkUnits >
			splwildcard.MaximumLikeQueryPatternWorkUnits-state.context.patternBudgets.like.workUnits {
			return compiledScalar{}, &plan.Diagnostic{
				Code: "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf(
					"search like patterns require more than %d work units",
					splwildcard.MaximumLikeQueryPatternWorkUnits,
				),
				Range: expression.Range,
			}
		}
		state.context.patternBudgets.like.workUnits += compiledPattern.WorkUnits
		if !input.alwaysNull && input.kind != fieldKindInvalid {
			inputBytes := compiledScalarStringByteBound(input)
			if inputBytes >
				MaximumLikeQueryInputBytes-state.context.patternBudgets.like.inputBytes {
				return compiledScalar{}, &plan.Diagnostic{
					Code: "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf(
						"search like inputs require more than %d bytes of wildcard scanning per row",
						MaximumLikeQueryInputBytes,
					),
					Range: expression.Range,
				}
			}
			state.context.patternBudgets.like.inputBytes += inputBytes
		}
	}
	return compileBoundedTextPredicateResult(
		input,
		compiledPattern.Pattern,
		"like",
		MaximumLikeInputBytes,
		maxCompiledLikeScalarSQLBytes,
		expression.Range,
	)
}

func compileTextLengthScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	input, err := compileUnaryNonBooleanScalarInput(expression, state, "len")
	if err != nil {
		return compiledScalar{}, err
	}

	valueSQL := ""
	valueArgs := append([]any(nil), input.valueArgs...)
	switch input.kind {
	case fieldKindDynamic:
		// dynamicElement returns Nullable(String), with null for every other
		// runtime type. It therefore preserves len's scalar-only boundary while
		// referencing the open event field exactly once.
		valueSQL = "lengthUTF8(dynamicElement(" + input.valueSQL + ", 'String'))"
	case fieldKindStringArray, fieldKindDynamicArray:
		return compiledScalar{}, unsupportedMultivalueUsage("len", expression.Range)
	case fieldKindString, fieldKindInvalid:
		inputSQL, inputArgs := compiledTextEligibleStringScalar(input)
		valueArgs = inputArgs
		valueSQL = "lengthUTF8(" + inputSQL + ")"
	default:
		return compiledScalar{}, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_TEXT_LENGTH_VALUE_TYPE",
			Message: "len requires a String input",
			Range:   expression.Range,
		}
	}
	if len(valueSQL) > maxCompiledTextLengthScalarSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"len scalar SQL exceeds %d bytes",
				maxCompiledTextLengthScalarSQLBytes,
			),
			Range: expression.Range,
		}
	}
	return compiledScalar{
		valueSQL:                valueSQL,
		valueArgs:               valueArgs,
		existsSQL:               "1",
		kind:                    fieldKindNumber,
		numberType:              "UInt64",
		alwaysNull:              input.alwaysNull,
		materializeForPredicate: input.materializeForPredicate,
	}, nil
}

func compileSubstringScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	if expression == nil {
		return compiledScalar{}, errors.New(
			"compile ClickHouse substr: missing expression",
		)
	}
	if len(expression.Arguments) < 2 || len(expression.Arguments) > 3 {
		return compiledScalar{}, errors.New(
			"compile ClickHouse substr: expected two or three arguments",
		)
	}
	input, err := compileNonBooleanScalarInputArgument(
		expression.Arguments[0],
		state,
		"substr",
	)
	if err != nil {
		return compiledScalar{}, err
	}
	inputSQL := ""
	inputArgs := append([]any(nil), input.valueArgs...)
	switch input.kind {
	case fieldKindDynamic:
		// dynamicElement returns Nullable(String), so unsupported runtime
		// numbers, Booleans, arrays, and objects fail closed without generic
		// Dynamic conversion branches.
		inputSQL = "dynamicElement(" + input.valueSQL + ", 'String')"
	case fieldKindStringArray, fieldKindDynamicArray:
		return compiledScalar{}, unsupportedMultivalueUsage(
			"substr",
			expression.Range,
		)
	case fieldKindString, fieldKindInvalid:
		inputSQL, inputArgs = compiledTextEligibleStringScalar(input)
	default:
		return compiledScalar{}, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_SUBSTRING_VALUE_TYPE",
			Message: "substr requires a String input",
			Range:   expression.Range,
		}
	}

	start, err := compileSubstringIntegerLiteral(
		expression.Arguments[1],
	)
	if err != nil {
		return compiledScalar{}, err
	}
	var length *plan.Value
	if len(expression.Arguments) == 3 {
		compiledLength, lengthErr := compileSubstringIntegerLiteral(
			expression.Arguments[2],
		)
		if lengthErr != nil {
			return compiledScalar{}, lengthErr
		}
		length = &compiledLength
	}

	valueSQL, indexArgs := compileSQLiteSubstringUTF8SQL(
		inputSQL,
		start,
		length,
	)
	valueArgs := append(inputArgs, indexArgs...)
	if len(valueSQL) > maxCompiledSubstringScalarSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"substr scalar SQL exceeds %d bytes",
				maxCompiledSubstringScalarSQLBytes,
			),
			Range: expression.Range,
		}
	}
	return compiledScalar{
		valueSQL:                valueSQL,
		valueArgs:               valueArgs,
		maxStringBytes:          compiledScalarStringByteBound(input),
		existsSQL:               "1",
		kind:                    fieldKindString,
		alwaysNull:              input.alwaysNull,
		materializeForPredicate: input.materializeForPredicate,
	}, nil
}

func compileSubstringIntegerLiteral(
	expression plan.ScalarExpression,
) (plan.Value, error) {
	if nilScalarExpression(expression) {
		return plan.Value{}, errors.New(
			"compile ClickHouse substr: missing index",
		)
	}
	value, ok := scalarIntegerLiteral(expression)
	if !ok {
		return plan.Value{}, errors.New(
			"compile ClickHouse substr: index must be a literal integer",
		)
	}
	return value, nil
}

func scalarIntegerLiteral(
	expression plan.ScalarExpression,
) (plan.Value, bool) {
	literal, ok := expression.(*plan.ScalarLiteralExpression)
	if !ok || literal == nil {
		return plan.Value{}, false
	}
	switch literal.Value.Kind {
	case plan.ValueKindInt64, plan.ValueKindUint64:
		return literal.Value, true
	default:
		return plan.Value{}, false
	}
}

func compileSQLiteSubstringUTF8SQL(
	inputSQL string,
	start plan.Value,
	length *plan.Value,
) (string, []any) {
	startSign := substringIntegerSign(start)
	if length == nil {
		if startSign == 0 {
			// SQLite treats position zero as immediately before the first
			// character; with no explicit length that is the whole String.
			return inputSQL, nil
		}
		if !nativeSubstringIntegerSafe(start) {
			return compileGenericSQLiteSubstringUTF8SQL(
				inputSQL,
				start,
				nil,
			)
		}
		startSQL, startArgs := compileNativeSubstringInteger(start)
		return "substringUTF8(" + inputSQL + ", " + startSQL + ")", startArgs
	}

	lengthSign := substringIntegerSign(*length)
	if lengthSign == 0 {
		return compileNativeSubstringUTF8(inputSQL, 1, 0)
	}
	if startSign >= 0 {
		startValue := nonnegativeSubstringInteger(start)
		if lengthSign > 0 {
			lengthValue := nonnegativeSubstringInteger(*length)
			if startValue == 0 {
				// SQLite counts the virtual zero position against a positive
				// length, so only length-1 real characters are returned.
				if lengthValue-1 <= maximumNativeSubstringInteger {
					return compileNativeSubstringUTF8(
						inputSQL,
						1,
						lengthValue-1,
					)
				}
				return compileGenericSQLiteSubstringUTF8SQL(
					inputSQL,
					start,
					length,
				)
			}
			if !nativeSubstringIntegerSafe(start) ||
				!nativeSubstringIntegerSafe(*length) {
				return compileGenericSQLiteSubstringUTF8SQL(
					inputSQL,
					start,
					length,
				)
			}
			startSQL, startArgs := compileNativeSubstringInteger(start)
			lengthSQL, lengthArgs := compileNativeSubstringInteger(*length)
			return "substringUTF8(" + inputSQL + ", " + startSQL +
					", " + lengthSQL + ")",
				append(startArgs, lengthArgs...)
		}

		// For a non-negative start and negative length, both SQLite interval
		// endpoints are compile-time constants. Clip the lower endpoint here;
		// native substringUTF8 clips the upper endpoint at the row's end.
		end := max(startValue, uint64(1))
		magnitude := negativeSubstringIntegerMagnitude(*length)
		begin := uint64(1)
		if startValue > 1 && magnitude < startValue-1 {
			begin = startValue - magnitude
		}
		if begin > maximumNativeSubstringInteger ||
			end-begin > maximumNativeSubstringInteger {
			return compileGenericSQLiteSubstringUTF8SQL(
				inputSQL,
				start,
				length,
			)
		}
		return compileNativeSubstringUTF8(inputSQL, begin, end-begin)
	}

	// A negative start with an explicit non-zero length has a clipped interval
	// which depends on the row's UTF-8 code-point count.
	return compileGenericSQLiteSubstringUTF8SQL(inputSQL, start, length)
}

func compileNativeSubstringUTF8(
	inputSQL string,
	start, length uint64,
) (string, []any) {
	return "substringUTF8(" + inputSQL +
			", CAST(? AS UInt64), CAST(? AS UInt64))",
		[]any{start, length}
}

func compileNativeSubstringInteger(value plan.Value) (string, []any) {
	if value.Kind == plan.ValueKindInt64 {
		return "CAST(? AS Int64)", []any{value.Int64}
	}
	return "CAST(? AS UInt64)", []any{value.Uint64}
}

func nativeSubstringIntegerSafe(value plan.Value) bool {
	return value.Kind == plan.ValueKindInt64 ||
		value.Uint64 <= maximumNativeSubstringInteger
}

func compileInt128SubstringInteger(value plan.Value) []any {
	if value.Kind == plan.ValueKindInt64 {
		return []any{value.Int64}
	}
	return []any{value.Uint64}
}

func substringIntegerSign(value plan.Value) int {
	if value.Kind == plan.ValueKindUint64 {
		if value.Uint64 == 0 {
			return 0
		}
		return 1
	}
	switch {
	case value.Int64 < 0:
		return -1
	case value.Int64 > 0:
		return 1
	default:
		return 0
	}
}

func nonnegativeSubstringInteger(value plan.Value) uint64 {
	if value.Kind == plan.ValueKindUint64 {
		return value.Uint64
	}
	if value.Int64 < 0 {
		return 0
	}

	return safecast.MustConv[uint64](value.Int64)
}

func negativeSubstringIntegerMagnitude(value plan.Value) uint64 {
	if value.Int64 >= 0 {
		return 0
	}
	// Subtract before negating so MinInt64 stays representable in its signed
	// domain. The result is non-negative and therefore fits UInt64.
	magnitudeMinusOne := -(value.Int64 + 1)

	return safecast.MustConv[uint64](magnitudeMinusOne) + 1
}

func compileGenericSQLiteSubstringUTF8SQL(
	inputSQL string,
	start plan.Value,
	length *plan.Value,
) (string, []any) {
	startArgs := compileInt128SubstringInteger(start)
	outerParameters := "value, start"
	outerArguments := "[" + inputSQL + "], [CAST(? AS Int128)]"

	positionSQL := "if(start < 0, n + start + 1, start)"
	beginSQL := positionSQL
	endSQL := "n + 1"
	indexArgs := startArgs
	if length != nil {
		lengthArgs := compileInt128SubstringInteger(*length)
		outerParameters += ", span"
		outerArguments += ", [CAST(? AS Int128)]"
		indexArgs = append(indexArgs, lengthArgs...)
		beginSQL = "if(span < 0, (" + positionSQL + ") + span, " +
			positionSQL + ")"
		endSQL = "if(span < 0, " + positionSQL + ", (" +
			positionSQL + ") + span)"
	}
	clippedBeginSQL := "clamp(" + beginSQL +
		", CAST(1 AS Int128), n + 1)"
	clippedEndSQL := "clamp(" + endSQL +
		", CAST(1 AS Int128), n + 1)"
	substringSQL := "substringUTF8(value, toInt64(" +
		clippedBeginSQL + "), toUInt64(" +
		clippedEndSQL + " - " + clippedBeginSQL + "))"
	lengthBindingSQL := "arrayElement(arrayMap(n -> " +
		substringSQL +
		", [CAST(lengthUTF8(value) AS Int128)]), 1)"
	return "arrayElement(arrayMap((" + outerParameters + ") -> " +
		lengthBindingSQL + ", " + outerArguments + "), 1)", indexArgs
}

func compileUnaryNonBooleanScalarInput(
	expression *plan.ScalarCallExpression,
	state compileState,
	functionName string,
) (compiledScalar, error) {
	input, err := compileUnaryScalarInput(expression, state, functionName)
	if err != nil {
		return compiledScalar{}, err
	}
	if scalarExpressionMayReturnBooleanFunction(expression.Arguments[0]) {
		return compiledScalar{}, booleanScalarConsumerError(functionName)
	}
	return input, nil
}

func compileUnaryScalarInput(
	expression *plan.ScalarCallExpression,
	state compileState,
	functionName string,
) (compiledScalar, error) {
	if expression == nil {
		return compiledScalar{}, fmt.Errorf(
			"compile ClickHouse %s: missing expression",
			functionName,
		)
	}
	if len(expression.Arguments) != 1 {
		return compiledScalar{}, fmt.Errorf(
			"compile ClickHouse %s: expected one argument",
			functionName,
		)
	}
	return compileScalarInputArgument(
		expression.Arguments[0],
		state,
		functionName,
	)
}

func compileScalarInputArgument(
	argument plan.ScalarExpression,
	state compileState,
	functionName string,
) (compiledScalar, error) {
	if nilScalarExpression(argument) {
		return compiledScalar{}, fmt.Errorf(
			"compile ClickHouse %s: missing scalar expression",
			functionName,
		)
	}
	return compileScalarValue(argument, state)
}

func compileNonBooleanScalarInputArgument(
	argument plan.ScalarExpression,
	state compileState,
	functionName string,
) (compiledScalar, error) {
	input, err := compileScalarInputArgument(argument, state, functionName)
	if err != nil {
		return compiledScalar{}, err
	}
	if scalarExpressionMayReturnBooleanFunction(argument) {
		return compiledScalar{}, booleanScalarConsumerError(functionName)
	}
	return input, nil
}

func compiledTextEligibleStringScalar(input compiledScalar) (string, []any) {
	inputSQL, inputArgs := compiledStringScalar(input)
	if input.textEligibleSQL == "" {
		return inputSQL, inputArgs
	}
	// _raw and conditionals derived from it carry a provenance guard.
	// Ingestion verifies the UTF-8 declaration, so this avoids both undefined
	// UTF-8 function behavior and a redundant byte scan.
	return "if(ifNull(" + input.textEligibleSQL + ", 0), " +
		inputSQL + ", CAST(NULL AS Nullable(String)))", inputArgs
}

func compileToStringScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	if expression != nil && len(expression.Arguments) == 2 {
		return compileToStringFormatScalar(expression, state)
	}
	input, err := compileUnaryScalarInput(expression, state, "tostring")
	if err != nil {
		return compiledScalar{}, err
	}
	return compileLexicalStringScalar(
		input,
		state,
		scalarStringConversion{
			operation:           "tostring",
			unsupportedTypeCode: "SPL_UNSUPPORTED_TOSTRING_VALUE_TYPE",
			allowBoolean:        true,
			maximumSQLBytes:     maxCompiledToStringScalarSQLBytes,
		},
		expression.Range,
	)
}

type scalarStringConversion struct {
	operation           string
	unsupportedTypeCode string
	allowBoolean        bool
	maximumSQLBytes     int
}

// compileLexicalStringScalar implements the exact scalar-to-String spelling
// shared by explicit tostring and period concatenation. Dynamic inputs are
// bound once and domain-specialized; only the unrestricted domain reserves
// bounded decimal/v1 parsing work.
func compileLexicalStringScalar(
	input compiledScalar,
	state compileState,
	conversion scalarStringConversion,
	sourceRange spl.Range,
) (compiledScalar, error) {
	valueSQL := ""
	valueArgs := append([]any(nil), input.valueArgs...)
	textEligibleSQL := ""
	switch input.kind {
	case fieldKindDynamic:
		switch input.dynamicDomain {
		case dynamicScalarDomainText:
			// Text-case producers can only contain String, String arrays, or
			// null. A direct extraction avoids a redundant singleton-array
			// binding and runtime dispatch while rejecting multivalue variants.
			valueSQL = "dynamicElement(" + input.valueSQL + ", 'String')"
		case dynamicScalarDomainNumeric:
			typeSQL := "dynamicType(value)"
			valueSQL = "arrayElement(arrayMap(value -> if(" +
				dynamicNumericTypePredicate(typeSQL) + ", toString(value), " +
				"CAST(NULL AS Nullable(String))), [" +
				input.valueSQL + "]), 1)"
		default:
			if err := reserveStringConversionDynamicDecimal(
				state.context,
				conversion.operation,
				sourceRange,
			); err != nil {
				return compiledScalar{}, err
			}
			typeSQL := "dynamicType(value)"
			dynamicValue := compiledScalar{
				valueSQL:       "value",
				dynamicTypeSQL: typeSQL,
				kind:           fieldKindDynamic,
			}
			decimalCondition, decimalPayload := dynamicTaggedDecimalText(
				dynamicValue,
			)
			branches := typeSQL +
				" = 'String', dynamicElement(value, 'String'), "
			if conversion.allowBoolean {
				branches += typeSQL +
					" = 'Bool', if(dynamicElement(value, 'Bool'), " +
					"CAST('True' AS String), CAST('False' AS String)), "
			}
			valueSQL = "arrayElement(arrayMap(value -> multiIf(" +
				branches + decimalCondition + ", " + decimalPayload + ", " +
				dynamicNumericTypePredicate(typeSQL) + ", toString(value), " +
				"CAST(NULL AS Nullable(String))), [" +
				input.valueSQL + "]), 1)"
		}
	case fieldKindString:
		valueSQL = input.valueSQL
		textEligibleSQL = input.textEligibleSQL
	case fieldKindNumber:
		valueSQL = "toString(" + input.valueSQL + ")"
	case fieldKindBool:
		if !conversion.allowBoolean {
			return compiledScalar{}, booleanScalarConsumerError(
				conversion.operation,
			)
		}
		// transform preserves nullable Boolean null while evaluating its input
		// once, without allocating a singleton array per row.
		valueSQL = "transform(" + input.valueSQL + ", [true, false], " +
			"['True', 'False'], CAST(NULL AS Nullable(String)))"
	case fieldKindInvalid:
		valueSQL = "CAST(NULL AS Nullable(String))"
	case fieldKindStringArray, fieldKindDynamicArray:
		return compiledScalar{}, unsupportedMultivalueUsage(
			conversion.operation,
			sourceRange,
		)
	default:
		supportedTypes := "scalar String and number"
		if conversion.allowBoolean {
			supportedTypes = "scalar String, number, and Boolean"
		}
		return compiledScalar{}, &plan.Diagnostic{
			Code: conversion.unsupportedTypeCode,
			Message: conversion.operation +
				" supports " + supportedTypes + " input",
			Range: sourceRange,
		}
	}
	if len(valueSQL) > conversion.maximumSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"%s scalar SQL exceeds %d bytes",
				conversion.operation,
				conversion.maximumSQLBytes,
			),
			Range: sourceRange,
		}
	}
	return compiledScalar{
		valueSQL:                    valueSQL,
		valueArgs:                   valueArgs,
		maxStringBytes:              compiledScalarStringByteBound(input),
		existsSQL:                   "1",
		textEligibleSQL:             textEligibleSQL,
		semanticBytesSQL:            input.semanticBytesSQL,
		semanticBytesArgs:           append([]any(nil), input.semanticBytesArgs...),
		semanticBytesByUTF8Validity: input.semanticBytesByUTF8Validity,
		textEligibleBySemanticBytes: input.textEligibleBySemanticBytes,
		stringOrBytes:               input.kind == fieldKindString && input.stringOrBytes,
		stringOrBytesNullable:       input.stringOrBytesNullable,
		kind:                        fieldKindString,
		alwaysNull:                  input.alwaysNull,
		materializeForPredicate:     input.materializeForPredicate,
	}, nil
}

func reserveStringConversionDynamicDecimal(
	context *compileContext,
	operation string,
	sourceRange spl.Range,
) error {
	if context == nil {
		return fmt.Errorf(
			"compile ClickHouse %s: query context is required",
			operation,
		)
	}
	reservation := uint64(MaximumStringConversionDynamicDecimalBytes)
	used := context.stringConversionBudget.dynamicDecimalBytes
	if used > MaximumStringConversionQueryDynamicDecimalBytes ||
		reservation > MaximumStringConversionQueryDynamicDecimalBytes-used {
		return &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"search Dynamic decimal String conversions require more than %d bytes of parsing",
				MaximumStringConversionQueryDynamicDecimalBytes,
			),
			Range: sourceRange,
		}
	}
	context.stringConversionBudget.dynamicDecimalBytes += reservation
	return nil
}

func compileConcatenationScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	return compileConcatenationScalarWithNullPolicy(expression, state, false)
}

func compileConcatenationScalarWithNullPolicy(
	expression *plan.ScalarCallExpression,
	state compileState,
	nullAsEmpty bool,
) (compiledScalar, error) {
	if expression == nil {
		return compiledScalar{}, errors.New(
			"compile ClickHouse concatenation: missing expression",
		)
	}
	if len(expression.Arguments) < 2 {
		return compiledScalar{}, errors.New(
			"compile ClickHouse concatenation: requires at least two operands",
		)
	}
	if len(expression.Arguments) > spl.MaximumConcatenationOperands {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"concatenation contains more than %d operands",
				spl.MaximumConcatenationOperands,
			),
			Range: expression.Range,
		}
	}
	if state.context == nil {
		return compiledScalar{}, errors.New(
			"compile ClickHouse concatenation: query context is required",
		)
	}
	if slices.ContainsFunc(expression.Arguments, nilScalarExpression) {
		return compiledScalar{}, errors.New(
			"compile ClickHouse concatenation: missing operand",
		)
	}
	if err := reserveConcatenationOperands(
		state.context,
		len(expression.Arguments),
		expression.Range,
	); err != nil {
		return compiledScalar{}, err
	}

	operands := make([]compiledScalar, 0, len(expression.Arguments))
	outputBytes := uint64(0)
	alwaysNull := false
	materializeForPredicate := false
	for _, argument := range expression.Arguments {
		input, err := compileScalarValue(argument, state)
		if err != nil {
			return compiledScalar{}, err
		}
		operand, err := compileLexicalStringScalar(
			input,
			state,
			scalarStringConversion{
				operation:           "concatenation",
				unsupportedTypeCode: "SPL_UNSUPPORTED_CONCATENATION_VALUE_TYPE",
				maximumSQLBytes:     maxCompiledConcatenationScalarSQLBytes,
			},
			argument.SourceRange(),
		)
		if err != nil {
			return compiledScalar{}, err
		}
		if nullAsEmpty {
			// A missing/null provenance-bearing String contributes an ordinary
			// empty String. Its dormant source provenance must not taint the
			// concatenation result as Bytes when every remaining contribution is
			// text. Command operands are exact fields or literals, so a guarded
			// field value is already a compiler-owned column without value args.
			if operand.textEligibleSQL != "" {
				operand.textEligibleSQL = "(isNull(" + operand.valueSQL + ") OR ifNull(" +
					operand.textEligibleSQL + ", 0))"
			}
			if operand.stringOrBytes && operand.semanticBytesSQL != "" {
				operand.semanticBytesSQL = "toUInt8(if(isNotNull(" + operand.valueSQL +
					"), ifNull(" + operand.semanticBytesSQL + ", 0), 0))"
				operand.semanticBytesArgs = append(
					append([]any(nil), operand.valueArgs...),
					operand.semanticBytesArgs...,
				)
				operand.stringOrBytesNullable = false
			}
			operand.valueSQL = "ifNull(" + operand.valueSQL + ", CAST('' AS String))"
			operand.alwaysNull = false
		}
		outputBytes = saturatingStringByteSum(
			outputBytes,
			compiledScalarStringByteBound(operand),
		)
		if outputBytes > MaximumConcatenationOutputBytes {
			return compiledScalar{}, &plan.Diagnostic{
				Code: "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf(
					"concatenation output may exceed %d bytes",
					MaximumConcatenationOutputBytes,
				),
				Range: expression.Range,
			}
		}
		alwaysNull = alwaysNull || operand.alwaysNull
		materializeForPredicate = materializeForPredicate ||
			operand.materializeForPredicate
		operands = append(operands, operand)
	}
	if err := reserveConcatenationOutput(
		state.context,
		outputBytes,
		expression.Range,
	); err != nil {
		return compiledScalar{}, err
	}

	var sql strings.Builder
	sql.WriteString("concat(")
	args := make([]any, 0)
	for index, operand := range operands {
		separatorBytes := 0
		if index > 0 {
			separatorBytes = 2
		}
		if sql.Len() >
			maxCompiledConcatenationScalarSQLBytes-
				separatorBytes-len(operand.valueSQL)-1 {
			return compiledScalar{}, &plan.Diagnostic{
				Code: "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf(
					"concatenation scalar SQL exceeds %d bytes",
					maxCompiledConcatenationScalarSQLBytes,
				),
				Range: expression.Range,
			}
		}
		if index > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(operand.valueSQL)
		args = append(args, operand.valueArgs...)
	}
	sql.WriteByte(')')
	valueSQL := sql.String()
	semanticGuards := make([]string, 0, len(operands))
	semanticGuardArgs := make([]any, 0)
	stringOrBytes := false
	stringOrBytesNullable := false
	for _, operand := range operands {
		stringOrBytesNullable = stringOrBytesNullable ||
			operand.stringOrBytesNullable || operand.alwaysNull
		if !operand.stringOrBytes {
			continue
		}
		stringOrBytes = true
		flagSQL, flagArgs := compiledScalarSemanticBytes(operand)
		semanticGuards = append(semanticGuards, flagSQL+" != 0")
		semanticGuardArgs = append(semanticGuardArgs, flagArgs...)
	}
	semanticBytesSQL := ""
	semanticBytesArgs := make([]any, 0)
	if stringOrBytes {
		// ClickHouse propagates Nullable from any concat operand, including an
		// ordinary String producer that carries no semantic-Bytes provenance.
		// Normalize every byte-capable concatenation to Nullable(String) so the
		// sealed result descriptor has one exact, conservative physical type.
		valueSQL = "CAST(" + valueSQL + " AS Nullable(String))"
		stringOrBytesNullable = true
		semanticBytesSQL = "toUInt8(if(isNotNull(" + valueSQL + "), " +
			"(" + strings.Join(semanticGuards, " OR ") + "), 0))"
		semanticBytesArgs = append(semanticBytesArgs, args...)
		semanticBytesArgs = append(semanticBytesArgs, semanticGuardArgs...)
	}
	return compiledScalar{
		valueSQL:                    valueSQL,
		valueArgs:                   args,
		maxStringBytes:              outputBytes,
		existsSQL:                   "1",
		textEligibleSQL:             concatenationTextEligibility(operands),
		semanticBytesSQL:            semanticBytesSQL,
		semanticBytesArgs:           semanticBytesArgs,
		semanticBytesByUTF8Validity: semanticBytesValidityOnly(operands...),
		textEligibleBySemanticBytes: stringOrBytes,
		stringOrBytes:               stringOrBytes,
		stringOrBytesNullable:       stringOrBytesNullable,
		kind:                        fieldKindString,
		alwaysNull:                  alwaysNull,
		materializeForPredicate:     materializeForPredicate,
	}, nil
}

func reserveConcatenationOperands(
	context *compileContext,
	operands int,
	sourceRange spl.Range,
) error {
	used := context.concatenationBudget.operands
	if used > spl.MaximumConcatenationOperandsPerQuery ||
		operands > spl.MaximumConcatenationOperandsPerQuery-used {
		return &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"concatenation contains more than %d operand occurrences per query",
				spl.MaximumConcatenationOperandsPerQuery,
			),
			Range: sourceRange,
		}
	}
	context.concatenationBudget.operands += operands
	return nil
}

func reserveConcatenationOutput(
	context *compileContext,
	outputBytes uint64,
	sourceRange spl.Range,
) error {
	used := context.concatenationBudget.outputBytes
	if used > MaximumConcatenationQueryOutputBytes ||
		outputBytes > MaximumConcatenationQueryOutputBytes-used {
		return &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"concatenation outputs may exceed %d bytes per query row",
				MaximumConcatenationQueryOutputBytes,
			),
			Range: sourceRange,
		}
	}
	context.concatenationBudget.outputBytes += outputBytes
	return nil
}

func concatenationTextEligibility(operands []compiledScalar) string {
	seen := make(map[string]struct{}, len(operands))
	guards := make([]string, 0, len(operands))
	for _, operand := range operands {
		if operand.textEligibleSQL == "" {
			continue
		}
		if _, duplicate := seen[operand.textEligibleSQL]; duplicate {
			continue
		}
		seen[operand.textEligibleSQL] = struct{}{}
		guards = append(guards, operand.textEligibleSQL)
	}
	switch len(guards) {
	case 0:
		return ""
	case 1:
		return guards[0]
	default:
		return "(" + strings.Join(guards, ") AND (") + ")"
	}
}

func compileRoundScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	if expression == nil {
		return compiledScalar{}, errors.New(
			"compile ClickHouse round: missing expression",
		)
	}
	if len(expression.Arguments) < 1 || len(expression.Arguments) > 2 {
		return compiledScalar{}, errors.New(
			"compile ClickHouse round: requires one or two arguments",
		)
	}
	input, err := compileNonBooleanScalarInputArgument(
		expression.Arguments[0],
		state,
		"round",
	)
	if err != nil {
		return compiledScalar{}, err
	}

	operation := numericRoundingOperation{
		functionName:        "round",
		unsupportedTypeCode: "SPL_UNSUPPORTED_ROUND_VALUE_TYPE",
	}
	if len(expression.Arguments) == 2 {
		precision, precisionErr := roundPrecisionLiteral(
			expression.Arguments[1],
		)
		if precisionErr != nil {
			return compiledScalar{}, precisionErr
		}
		operation.precision = &precision
	}

	return compileNumericRoundingInput(
		input,
		operation,
		expression.Range,
	)
}

func compileIntegralRoundingScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
	functionName string,
) (compiledScalar, error) {
	input, err := compileUnaryNonBooleanScalarInput(
		expression,
		state,
		functionName,
	)
	if err != nil {
		return compiledScalar{}, err
	}
	unsupportedTypeCode := "SPL_UNSUPPORTED_CEIL_VALUE_TYPE"
	if functionName == "floor" {
		unsupportedTypeCode = "SPL_UNSUPPORTED_FLOOR_VALUE_TYPE"
	}
	return compileNumericRoundingInput(
		input,
		numericRoundingOperation{
			functionName:        functionName,
			unsupportedTypeCode: unsupportedTypeCode,
		},
		expression.Range,
	)
}

type numericRoundingOperation struct {
	functionName        string
	unsupportedTypeCode string
	precision           *uint8
}

// compileNumericRoundingInput implements the common numeric contract for
// round, ceil, and floor. Dynamic input is bound once; integer variants and
// integral semantic Decimals stay exact, while other numeric variants are
// converted through finite Float64 before applying the requested function.
func compileNumericRoundingInput(
	input compiledScalar,
	operation numericRoundingOperation,
	sourceRange spl.Range,
) (compiledScalar, error) {
	numericIntegral := operation.functionName != "round" ||
		operation.precision == nil ||
		*operation.precision == 0
	if input.alwaysNull {
		return compiledScalar{
			valueSQL:        "CAST(NULL AS Nullable(Float64))",
			existsSQL:       "1",
			kind:            fieldKindNumber,
			numberType:      "Float64",
			numericIntegral: numericIntegral,
			alwaysNull:      true,
			ieeeComparison:  input.ieeeComparison,
		}, nil
	}
	if input.numericIntegral &&
		(operation.functionName == "ceil" || operation.functionName == "floor") {
		return input, nil
	}
	fixedArgumentsSQL := ""
	dynamicArgumentsSQL := ""
	var operationArgs []any
	if operation.precision != nil {
		fixedArgumentsSQL = ", CAST(? AS UInt8)"
		dynamicArgumentsSQL = ", precision"
		operationArgs = []any{*operation.precision}
	}
	valueSQL := ""
	valueArgs := append([]any(nil), input.valueArgs...)
	resultKind := fieldKindNumber
	numberType := input.numberType
	dynamicDomain := dynamicScalarDomainAny
	alwaysNull := input.alwaysNull
	switch input.kind {
	case fieldKindDynamic:
		if input.dynamicDomain == dynamicScalarDomainText {
			valueSQL = "CAST(NULL AS Nullable(Float64))"
			valueArgs = nil
			numberType = "Float64"
			alwaysNull = true
			break
		}
		typeSQL := "dynamicType(value)"
		integerCondition := dynamicIntegerTypePredicate(typeSQL)
		body := ""
		if input.dynamicDomain == dynamicScalarDomainNumeric {
			rounded := operation.functionName + "(" +
				finiteDynamicFloatOrNullSQL("value") +
				dynamicArgumentsSQL + ")"
			body = "multiIf(" +
				integerCondition + ", value, " +
				"CAST(" + rounded + " AS Dynamic))"
		} else {
			dynamicValue := compiledScalar{
				valueSQL:       "value",
				dynamicTypeSQL: typeSQL,
				kind:           fieldKindDynamic,
			}
			decimalCondition, decimalPayload := dynamicTaggedDecimalText(
				dynamicValue,
			)
			exactTaggedInteger := dynamicTaggedDecimalIntegralSQL(
				dynamicValue,
			)
			numericValue := "multiIf(" +
				decimalCondition + ", " +
				finiteFloatOrNullSQL(decimalPayload) + ", " +
				dynamicNumericTypePredicate(typeSQL) + ", " +
				finiteDynamicFloatOrNullSQL("value") + ", " +
				"CAST(NULL AS Nullable(Float64)))"
			rounded := operation.functionName + "(" + numericValue + dynamicArgumentsSQL + ")"
			body = "arrayElement(arrayMap(exact_value -> multiIf(" +
				integerCondition + ", value, (" +
				"isNotNull(exact_value)), CAST(assumeNotNull(exact_value) AS Dynamic), " +
				"CAST(" + rounded + " AS Dynamic)), [" +
				exactTaggedInteger + "]), 1)"
		}
		if len(operationArgs) == 0 {
			valueSQL = "arrayElement(arrayMap(value -> " + body + ", [" +
				input.valueSQL + "]), 1)"
		} else {
			valueSQL = "arrayElement(arrayMap((value, precision) -> " + body +
				", [" + input.valueSQL + "], [CAST(? AS UInt8)]), 1)"
		}
		valueArgs = append(valueArgs, operationArgs...)
		resultKind = fieldKindDynamic
		numberType = ""
		dynamicDomain = dynamicScalarDomainNumeric
	case fieldKindNumber:
		if fixedNumberTypeIsInteger(input.numberType) {
			valueSQL = input.valueSQL
			break
		}
		valueSQL = operation.functionName + "(" + input.valueSQL + fixedArgumentsSQL + ")"
		valueArgs = append(valueArgs, operationArgs...)
	case fieldKindInvalid:
		valueSQL = "CAST(NULL AS Nullable(Float64))"
		numberType = "Float64"
		alwaysNull = true
	case fieldKindStringArray, fieldKindDynamicArray:
		return compiledScalar{}, unsupportedMultivalueUsage(
			operation.functionName,
			sourceRange,
		)
	default:
		return compiledScalar{}, &plan.Diagnostic{
			Code:    operation.unsupportedTypeCode,
			Message: operation.functionName + " requires a numeric input",
			Range:   sourceRange,
		}
	}
	if len(valueSQL) > maxCompiledNumericRoundingScalarSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"%s scalar SQL exceeds %d bytes",
				operation.functionName,
				maxCompiledNumericRoundingScalarSQLBytes,
			),
			Range: sourceRange,
		}
	}
	return compiledScalar{
		valueSQL:                valueSQL,
		valueArgs:               valueArgs,
		existsSQL:               "1",
		dynamicDomain:           dynamicDomain,
		numericIntegral:         numericIntegral,
		kind:                    resultKind,
		numberType:              numberType,
		alwaysNull:              alwaysNull,
		ieeeComparison:          input.ieeeComparison,
		materializeForPredicate: input.materializeForPredicate,
	}, nil
}

func compileMVSortScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	input, err := compileUnaryNonBooleanScalarInput(expression, state, "mvsort")
	if err != nil {
		return compiledScalar{}, err
	}
	if input.mvSortedLexicographic {
		return input, nil
	}

	emptyArray := "CAST([], 'Array(String)')"
	if input.alwaysNull {
		return compiledScalar{
			valueSQL:              emptyArray,
			existsSQL:             "0",
			kind:                  fieldKindStringArray,
			alwaysNull:            true,
			mvSortedLexicographic: true,
		}, nil
	}

	valueSQL := ""
	valueArgs := append([]any(nil), input.valueArgs...)
	existsSQL := "1"
	var existsArgs []any
	presentSQL := ""
	resultKind := fieldKindStringArray
	dynamicDomain := dynamicScalarDomainAny
	requiresRuntimeValidation := input.requiresRuntimeValidation
	switch input.kind {
	case fieldKindStringArray:
		valueSQL = "arrayElement(arrayMap(values -> " +
			boundedMVSortStringArraySQL(
				"values",
				emptyArray,
				"Array(String)",
				false,
			) +
			", [" + input.valueSQL + "]), 1)"
		if input.optionalMultivaluePresentSQL != "" {
			existsSQL, existsArgs = scalarExistsSQL(input)
			presentSQL = input.optionalMultivaluePresentSQL
		}
	case fieldKindDynamicArray:
		normalized, normalizeErr := compileNativeMVState(input, false)
		if normalizeErr != nil {
			return compiledScalar{}, normalizeErr
		}
		stateAlias := "__os_mvsort_native_state"
		valuesAlias := "__os_mvsort_native_values"
		sortedAlias := "__os_mvsort_native_sorted"
		values := "tupleElement(" + stateAlias + ", 1)"
		stringsSQL := "arrayMap(element -> assumeNotNull(dynamicElement(element, 'String')), " +
			valuesAlias + ")"
		sorted := "arrayMap(element -> CAST(element AS Dynamic), arraySort(" + stringsSQL + "))"
		invalid := "tupleElement(" + stateAlias + ", 4) != 0 OR " +
			"arrayExists(element -> dynamicType(element) != 'String', " + valuesAlias + ")"
		body := bindSQLExpressions(
			[]string{sortedAlias},
			[]string{sorted},
			nativeMVPreflightSQL(
				sortedAlias,
				invalid,
				"length("+sortedAlias+")",
				nativeMVArrayPayloadBytesSQL(sortedAlias),
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
	case fieldKindDynamic:
		nullDynamic := "CAST(NULL AS Dynamic)"
		stringArray := "arrayElement(arrayMap(values -> " +
			boundedMVSortStringArraySQL(
				"values",
				nullDynamic,
				"Dynamic",
				true,
			) +
			", [dynamicElement(value, 'Array(String)')]), 1)"
		dynamicArray := "arrayElement(arrayMap(values -> " +
			boundedMVSortDynamicArraySQL("values") +
			", [dynamicElement(value, 'Array(Dynamic)')]), 1)"
		body := "multiIf(" +
			"dynamicType(value) = 'Array(String)', " + stringArray + ", " +
			"dynamicType(value) = 'Array(Dynamic)', " + dynamicArray + ", " +
			nullDynamic + ")"
		bound := "arrayElement(arrayMap(value -> " + body +
			", [" + input.valueSQL + "]), 1)"
		dynamicExistsSQL := input.existsSQL
		if dynamicExistsSQL == "" {
			dynamicExistsSQL = "1"
		}
		if dynamicExistsSQL == "1" {
			valueSQL = bound
		} else {
			valueSQL = "if(" + dynamicExistsSQL + ", " + bound + ", " + nullDynamic + ")"
			valueArgs = append(
				append([]any(nil), input.existsArgs...),
				input.valueArgs...,
			)
		}
		resultKind = fieldKindDynamic
		dynamicDomain = dynamicScalarDomainText
	case fieldKindInvalid:
		valueSQL = emptyArray
		valueArgs = nil
	default:
		return compiledScalar{}, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_MVSORT_VALUE_TYPE",
			Message: "mvsort requires a multivalue String input",
			Range:   expression.Range,
		}
	}
	if len(valueSQL) > maxCompiledMVSortScalarSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"mvsort scalar SQL exceeds %d bytes",
				maxCompiledMVSortScalarSQLBytes,
			),
			Range: expression.Range,
		}
	}
	return compiledScalar{
		valueSQL:                     valueSQL,
		valueArgs:                    valueArgs,
		maxStringBytes:               input.maxStringBytes,
		existsSQL:                    existsSQL,
		existsArgs:                   existsArgs,
		optionalMultivaluePresentSQL: presentSQL,
		dynamicDomain:                dynamicDomain,
		kind:                         resultKind,
		mvSortedLexicographic:        true,
		materializeForPredicate:      input.materializeForPredicate,
		requiresRuntimeValidation:    requiresRuntimeValidation,
	}, nil
}

func boundedMVSortStringArraySQL(
	valuesSQL string,
	invalidSQL string,
	resultType string,
	requireNonEmpty bool,
) string {
	conditions := []string{
		"length(" + valuesSQL + ") <= toUInt64(" +
			strconv.FormatUint(uint64(MaximumMVSortValues), 10) + ")",
		stringArrayPayloadBytesSQL(valuesSQL) + " <= toUInt128(" +
			strconv.FormatUint(uint64(MaximumMVSortBytes), 10) + ")",
		"arrayAll(element -> isValidUTF8(element), " + valuesSQL + ")",
	}
	if requireNonEmpty {
		conditions = append([]string{"notEmpty(" + valuesSQL + ")"}, conditions...)
	}
	return "if(" + strings.Join(conditions, " AND ") +
		", CAST(arraySort(" + valuesSQL + ") AS " + resultType +
		"), " + invalidSQL + ")"
}

func boundedMVSortDynamicArraySQL(valuesSQL string) string {
	nullableStringValue := "dynamicElement(element, 'String')"
	stringValue := "assumeNotNull(" + nullableStringValue + ")"
	overLimitBytes := strconv.FormatUint(uint64(MaximumMVSortBytes)+1, 10)
	payloadBytes := "arrayFold((bytes, element) -> bytes + toUInt128(ifNull(" +
		"length(" + nullableStringValue + "), toUInt64(" + overLimitBytes + "))), " +
		valuesSQL + ", toUInt128(0))"
	conditions := []string{
		"notEmpty(" + valuesSQL + ")",
		"length(" + valuesSQL + ") <= toUInt64(" +
			strconv.FormatUint(uint64(MaximumMVSortValues), 10) + ")",
		"arrayAll(element -> dynamicType(element) = 'String', " + valuesSQL + ")",
		payloadBytes + " <= toUInt128(" +
			strconv.FormatUint(uint64(MaximumMVSortBytes), 10) + ")",
		"arrayAll(element -> isValidUTF8(ifNull(" + nullableStringValue +
			", '')), " + valuesSQL + ")",
	}
	stringsSQL := "arrayMap(element -> " + stringValue + ", " + valuesSQL + ")"
	return "if(" + strings.Join(conditions, " AND ") +
		", CAST(arraySort(" + stringsSQL + ") AS Dynamic), CAST(NULL AS Dynamic))"
}

func compileMVCountScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	input, err := compileUnaryScalarInput(expression, state, "mvcount")
	if err != nil {
		return compiledScalar{}, err
	}
	if input.mvCountOneOrNull {
		return input, nil
	}
	nullUInt64 := "CAST(NULL AS Nullable(UInt64))"
	if input.alwaysNull {
		return compiledScalar{
			valueSQL:         nullUInt64,
			existsSQL:        "1",
			kind:             fieldKindNumber,
			numberType:       "UInt64",
			numericIntegral:  true,
			mvCountOneOrNull: true,
			alwaysNull:       true,
		}, nil
	}

	valueSQL := ""
	valueArgs := append([]any(nil), input.valueArgs...)
	switch input.kind {
	case fieldKindStringArray:
		valueSQL = "nullIf(toUInt64(length(" + input.valueSQL +
			")), toUInt64(0))"
	case fieldKindDynamicArray:
		// Explicit native null members are retained in the typed list but do
		// not contribute to mvcount, matching the open Dynamic Array(Dynamic)
		// path below and stats count(field).
		valueSQL = "nullIf(toUInt64(arrayCount(element -> dynamicType(element) != 'None', " +
			input.valueSQL + ")), toUInt64(0))"
	case fieldKindDynamic:
		existsSQL := input.existsSQL
		if existsSQL == "" {
			existsSQL = "1"
		}
		body := ""
		switch input.dynamicDomain {
		case dynamicScalarDomainText:
			typeSQL := "dynamicType(value)"
			body = "multiIf(" +
				typeSQL + " = 'String', toUInt64(1), " +
				typeSQL + " = 'Array(String)', nullIf(toUInt64(length(" +
				"dynamicElement(value, 'Array(String)'))), toUInt64(0)), " +
				nullUInt64 + ")"
		case dynamicScalarDomainNumeric:
			body = "if(dynamicType(" + input.valueSQL + ") = 'None', " +
				nullUInt64 + ", toUInt64(1))"
			if existsSQL == "1" {
				valueSQL = body
			} else {
				valueSQL = "if(" + existsSQL + ", " + body + ", " + nullUInt64 + ")"
				valueArgs = append(append([]any(nil), input.existsArgs...), input.valueArgs...)
			}
		default:
			typeSQL := "dynamicType(value)"
			dynamicValue := compiledScalar{
				valueSQL:       "value",
				dynamicTypeSQL: typeSQL,
				kind:           fieldKindDynamic,
			}
			dynamicCount := "nullIf(" +
				dynamicNonNullArrayCardinalitySQL("value") +
				", toUInt64(0))"
			otherArrayCount := "nullIf(toUInt64(length(value)), toUInt64(0))"
			scalar := "(" +
				typeSQL + " = 'String' OR " +
				typeSQL + " = 'Bool' OR " +
				dynamicNumericTypePredicate(typeSQL) + " OR " +
				dynamicTaggedScalarEnvelopeCondition(dynamicValue) + ")"
			body = "multiIf(" +
				typeSQL + " = 'None', " + nullUInt64 + ", " +
				typeSQL + " = 'Array(Dynamic)', " + dynamicCount + ", " +
				"startsWith(" + typeSQL + ", 'Array('), " +
				otherArrayCount + ", " +
				scalar + ", toUInt64(1), " +
				nullUInt64 + ")"
		}
		if input.dynamicDomain != dynamicScalarDomainNumeric {
			bound := "arrayElement(arrayMap(value -> " + body +
				", [" + input.valueSQL + "]), 1)"
			if existsSQL == "1" {
				valueSQL = bound
			} else {
				valueSQL = "if(" + existsSQL + ", " + bound + ", " + nullUInt64 + ")"
				valueArgs = append(append([]any(nil), input.existsArgs...), input.valueArgs...)
			}
		}
	case fieldKindInvalid:
		valueSQL = nullUInt64
		valueArgs = nil
	default:
		valueSQL = "if(isNotNull(" + input.valueSQL +
			"), toUInt64(1), " + nullUInt64 + ")"
	}
	if len(valueSQL) > maxCompiledMVCountScalarSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"mvcount scalar SQL exceeds %d bytes",
				maxCompiledMVCountScalarSQLBytes,
			),
			Range: expression.Range,
		}
	}
	return compiledScalar{
		valueSQL:                valueSQL,
		valueArgs:               valueArgs,
		existsSQL:               "1",
		kind:                    fieldKindNumber,
		numberType:              "UInt64",
		numericIntegral:         true,
		mvCountOneOrNull:        true,
		materializeForPredicate: input.materializeForPredicate,
	}, nil
}

func roundPrecisionLiteral(
	expression plan.ScalarExpression,
) (uint8, error) {
	if nilScalarExpression(expression) {
		return 0, errors.New(
			"compile ClickHouse round: missing precision",
		)
	}
	value, ok := scalarIntegerLiteral(expression)
	if !ok {
		return 0, errors.New(
			"compile ClickHouse round: precision must be a literal integer",
		)
	}
	switch value.Kind {
	case plan.ValueKindInt64:
		if value.Int64 < 0 ||
			value.Int64 > spl.MaximumRoundPrecision {
			return 0, fmt.Errorf(
				"compile ClickHouse round: precision must be from 0 through %d",
				spl.MaximumRoundPrecision,
			)
		}

		return safecast.MustConv[uint8](value.Int64), nil
	case plan.ValueKindUint64:
		if value.Uint64 > spl.MaximumRoundPrecision {
			return 0, fmt.Errorf(
				"compile ClickHouse round: precision must be from 0 through %d",
				spl.MaximumRoundPrecision,
			)
		}

		return safecast.MustConv[uint8](value.Uint64), nil
	default:
		return 0, errors.New(
			"compile ClickHouse round: precision must be a literal integer",
		)
	}
}

func compileToNumberScalar(expression *plan.ScalarCallExpression, state compileState) (compiledScalar, error) {
	if len(expression.Arguments) != 1 {
		return compiledScalar{}, errors.New("compile ClickHouse tonumber: expected one argument")
	}
	if scalarExpressionMayReturnBooleanFunction(expression.Arguments[0]) {
		return compiledScalar{}, booleanScalarConsumerError("tonumber")
	}
	input, err := compileScalarValue(expression.Arguments[0], state)
	if err != nil {
		return compiledScalar{}, err
	}
	if isNativeMultivalueKind(input.kind) {
		return compiledScalar{}, unsupportedMultivalueUsage("tonumber", expression.Range)
	}
	inputSQL, inputArgs := compiledStringScalar(input)
	return compiledScalar{
		valueSQL:                "ifNotFinite(toFloat64OrNull(" + inputSQL + "), CAST(NULL AS Nullable(Float64)))",
		valueArgs:               inputArgs,
		existsSQL:               "1",
		kind:                    fieldKindNumber,
		numberType:              "Float64",
		materializeForPredicate: input.materializeForPredicate,
	}, nil
}

func compiledStringScalar(value compiledScalar) (string, []any) {
	if value.kind == fieldKindDynamic {
		return "if(" + value.existsSQL + ", dynamicElement(" + value.valueSQL + ", 'String'), CAST(NULL AS Nullable(String)))",
			append(append([]any(nil), value.existsArgs...), value.valueArgs...)
	}
	if value.existsSQL != "" && value.existsSQL != "1" {
		return "if(" + value.existsSQL + ", toString(" + value.valueSQL + "), CAST(NULL AS Nullable(String)))",
			append(append([]any(nil), value.existsArgs...), value.valueArgs...)
	}
	if value.kind == fieldKindString {
		return value.valueSQL, append([]any(nil), value.valueArgs...)
	}
	if value.kind == fieldKindTime {
		return "toString(" + numericScalarSQL(value, false) + ")", append([]any(nil), value.valueArgs...)
	}
	return "toString(" + value.valueSQL + ")", append([]any(nil), value.valueArgs...)
}

func scalarStringLiteral(expression plan.ScalarExpression) (string, bool) {
	literal, ok := expression.(*plan.ScalarLiteralExpression)
	if !ok || literal == nil || literal.Value.Kind != plan.ValueKindString {
		return "", false
	}
	return literal.Value.String, true
}

func scalarQuotedStringLiteral(expression plan.ScalarExpression) (string, bool) {
	literal, ok := expression.(*plan.ScalarLiteralExpression)
	if !ok || literal == nil ||
		literal.Value.Kind != plan.ValueKindString ||
		!literal.Value.Quoted {
		return "", false
	}
	return literal.Value.String, true
}

func scalarExpressionMayReturnBooleanFunction(expression plan.ScalarExpression) bool {
	switch expression := expression.(type) {
	case *plan.ScalarUnaryExpression, *plan.ScalarBinaryExpression:
		// Arithmetic has a fixed numeric result. Its operands are validated by
		// arithmetic lowering so a nested Boolean receives the source-located
		// unsupported-arithmetic diagnostic instead of being mistaken for a
		// directly assigned Boolean result here.
		return false
	case *plan.ScalarCallExpression:
		if expression == nil {
			return false
		}
		if expression.Function.ReturnsBoolean() {
			return true
		}
		if expression.Function == plan.ScalarFunctionCoalesce {
			if slices.ContainsFunc(expression.Arguments, scalarExpressionMayReturnBooleanFunction) {
				return true
			}
		}
		return false
	case *plan.ScalarIfExpression:
		return expression != nil &&
			(scalarExpressionMayReturnBooleanFunction(expression.True) ||
				scalarExpressionMayReturnBooleanFunction(expression.False))
	case *plan.ScalarCaseExpression:
		if expression == nil {
			return false
		}
		for _, branch := range expression.Branches {
			if scalarExpressionMayReturnBooleanFunction(branch.Value) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func scalarExpressionRequiresNativeMVValidation(
	expression plan.ScalarExpression,
	state compileState,
) bool {
	if nilScalarExpression(expression) {
		return false
	}
	switch expression := expression.(type) {
	case *plan.ScalarUnaryExpression:
		return scalarExpressionRequiresNativeMVValidation(expression.Operand, state)
	case *plan.ScalarBinaryExpression:
		return scalarExpressionRequiresNativeMVValidation(expression.Left, state) ||
			scalarExpressionRequiresNativeMVValidation(expression.Right, state)
	case *plan.ScalarCallExpression:
		switch expression.Function {
		case plan.ScalarFunctionSplit,
			plan.ScalarFunctionMVAppend,
			plan.ScalarFunctionMVDedup,
			plan.ScalarFunctionMVIndex,
			plan.ScalarFunctionMVJoin,
			plan.ScalarFunctionMVZip,
			plan.ScalarFunctionMVFind:
			return true
		case plan.ScalarFunctionLower,
			plan.ScalarFunctionUpper,
			plan.ScalarFunctionMVSort:
			// These established transforms gain a runtime guard only for a
			// compiler-sealed native-list input. Detect that direct field form
			// precisely instead of forcing every ordinary scalar lower/upper or
			// open-Dynamic mvsort through a materialized validation fence. Nested
			// native producers are found by the recursive argument walk below.
			if len(expression.Arguments) == 1 &&
				sealedNativeMVFieldExpression(expression.Arguments[0], state) {
				return true
			}
		case plan.ScalarFunctionTrim,
			plan.ScalarFunctionLTrim,
			plan.ScalarFunctionRTrim,
			plan.ScalarFunctionURLDecode,
			plan.ScalarFunctionMD5,
			plan.ScalarFunctionSHA1,
			plan.ScalarFunctionSHA256,
			plan.ScalarFunctionSHA512:
			// Per-member text transforms follow the lower/upper rule; the
			// optional trim character set is a literal and never a list.
			if len(expression.Arguments) >= 1 &&
				sealedNativeMVFieldExpression(expression.Arguments[0], state) {
				return true
			}
		}
		for _, argument := range expression.Arguments {
			if scalarExpressionRequiresNativeMVValidation(argument, state) {
				return true
			}
		}
		return false
	case *plan.ScalarIfExpression:
		return expressionRequiresNativeMVValidation(expression.Condition, state) ||
			scalarExpressionRequiresNativeMVValidation(expression.True, state) ||
			scalarExpressionRequiresNativeMVValidation(expression.False, state)
	case *plan.ScalarCaseExpression:
		for _, branch := range expression.Branches {
			if expressionRequiresNativeMVValidation(branch.Condition, state) ||
				scalarExpressionRequiresNativeMVValidation(branch.Value, state) {
				return true
			}
		}
	}
	return false
}

func sealedNativeMVFieldExpression(
	expression plan.ScalarExpression,
	state compileState,
) bool {
	fieldExpression, ok := expression.(*plan.ScalarFieldExpression)
	if !ok || fieldExpression == nil {
		return false
	}
	field, resolved, err := resolveCompiledField(fieldExpression.Field, state)
	return err == nil && resolved && isNativeMultivalueKind(field.kind) &&
		(field.kind == fieldKindDynamicArray ||
			field.optionalMultivaluePresentSQL != "")
}

func expressionRequiresNativeMVValidation(
	expression plan.Expression,
	state compileState,
) bool {
	if nilPlanExpression(expression) {
		return false
	}
	switch expression := expression.(type) {
	case *plan.BooleanExpression:
		return expressionRequiresNativeMVValidation(expression.Left, state) ||
			expressionRequiresNativeMVValidation(expression.Right, state)
	case *plan.NotExpression:
		return expressionRequiresNativeMVValidation(expression.Operand, state)
	case *plan.EvalComparisonExpression:
		return scalarExpressionRequiresNativeMVValidation(expression.Left, state) ||
			scalarExpressionRequiresNativeMVValidation(expression.Right, state)
	case *plan.MembershipExpression:
		if scalarExpressionRequiresNativeMVValidation(expression.Value, state) {
			return true
		}
		for _, candidate := range expression.Candidates {
			if scalarExpressionRequiresNativeMVValidation(candidate, state) {
				return true
			}
		}
	case *plan.ScalarPredicateExpression:
		return scalarExpressionRequiresNativeMVValidation(expression.Value, state)
	}
	return false
}
