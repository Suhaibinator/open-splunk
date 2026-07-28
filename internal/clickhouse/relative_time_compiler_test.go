package clickhouse

import (
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchtimebounds"
	"github.com/Suhaibinator/open-splunk/internal/splrelativetime"
)

func TestCompileRelativeTimeUsesPinnedTimezoneAndBoundedOperationOrder(t *testing.T) {
	t.Parallel()

	scope := testChartScope()
	scope.SearchTimezone = "America/Los_Angeles"
	compiled := compileSPLWithScope(
		t,
		`index=gradethis`+
			` | eval elapsed=relative_time(_time, "-1234567s"),`+
			` calendar=relative_time(_time, "-1d"),`+
			` snapped=relative_time(_time, "-1mon@mon+7d")`+
			` | table elapsed,calendar,snapped`,
		scope,
	)
	for _, required := range []string{
		"toTimeZone(",
		"toUnixTimestamp64Nano(",
		"fromUnixTimestamp64Nano(",
		"addDays(",
		"addMonths(",
		"dateTrunc('month'",
		"arrayMap(",
		`AS "elapsed"`,
		`AS "calendar"`,
		`AS "snapped"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("relative_time SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	for _, forbidden := range []string{
		"timezone()",
		"serverTimeZone",
		"-1234567",
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("relative_time SQL contains unpinned value %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if countArgument(compiled.Args, scope.SearchTimezone) != 3 {
		t.Fatalf("relative_time timezone args = %#v", compiled.Args)
	}
	if countArgument(compiled.Args, uint64(1_234_567)) != 1 {
		t.Fatalf("relative_time magnitude was not parameterized: %#v", compiled.Args)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s", got, want, compiled.SQL)
	}
}

func TestCompileRelativeTimePreservesUnsnappedFractionAndExplicitSnapDistinction(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis`+
			` | eval unchanged=relative_time(_time, "+0s"),`+
			` shifted=relative_time(_time, "-1h"),`+
			` snapped=relative_time(_time, "-1h@h")`+
			` | table unchanged,shifted,snapped`,
	)
	if strings.Count(
		compiled.SQL,
		"modulo(modulo(ticks + toInt256(timeZoneOffset(value)) * "+
			"toInt256(1000000000), toInt256(3600000000000))",
	) != 1 {
		t.Fatalf("only the explicit @h program should snap:\n%s", compiled.SQL)
	}
	if strings.Count(compiled.SQL, "toUnixTimestamp64Nano(") < 3 ||
		!strings.Contains(compiled.SQL, "toFloat64(") {
		t.Fatalf("unsnapped output lost fractional Unix-time conversion:\n%s", compiled.SQL)
	}
}

func TestCompileRelativeTimeSnapsSecondsByEpochTicks(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis`+
			` | eval snapped=relative_time(timestamp, "@s")`+
			` | table snapped`,
	)
	for _, required := range []string{
		"modulo(modulo(ticks, toInt256(1000000000))",
		"fromUnixTimestamp64Nano(",
		"timezoneOf(value)",
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("second snap SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, "dateTrunc('second'") {
		t.Fatalf(
			"second snap used lower-bound-unsafe dateTrunc:\n%s",
			compiled.SQL,
		)
	}
}

func TestCompileRelativeTimeSnapsMinutesAndHoursByPinnedTimezoneOffset(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis`+
			` | eval minute=relative_time(timestamp, "@m"),`+
			` hour=relative_time(timestamp, "@h")`+
			` | table minute,hour`,
	)
	for _, required := range []string{
		"timeZoneOffset(value)",
		"toInt256(60000000000)",
		"toInt256(3600000000000)",
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("subday snap SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	for _, forbidden := range []string{
		"dateTrunc('minute'",
		"dateTrunc('hour'",
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf(
				"subday snap used historical-offset-unsafe %q:\n%s",
				forbidden,
				compiled.SQL,
			)
		}
	}
}

func TestCompileRelativeTimeGuardsPolicyBoundsAndOperationDirection(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis`+
			` | eval forward=relative_time(_time, "+1d"),`+
			` backward=relative_time(_time, "-1mon"),`+
			` snapped=relative_time(_time, "@d")`+
			` | table forward,backward,snapped`,
	)
	for _, required := range []string{
		strconv.FormatInt(searchtimebounds.MinimumUnixSeconds*1_000_000_000, 10),
		strconv.FormatInt(searchtimebounds.MaximumUnixSeconds*1_000_000_000, 10),
		"toUnixTimestamp64Nano(result) > toUnixTimestamp64Nano(value)",
		"toUnixTimestamp64Nano(result) < toUnixTimestamp64Nano(value)",
		"toUnixTimestamp64Nano(result) <= toUnixTimestamp64Nano(value)",
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("relative_time SQL missing guard %q:\n%s", required, compiled.SQL)
		}
	}
}

func TestCompileRelativeTimeUsesCanonicalIANALocalLowerBoundary(t *testing.T) {
	t.Parallel()

	scope := testChartScope()
	scope.SearchTimezone = "Europe/Dublin"
	compiled := compileSPLWithScope(
		t,
		`index=gradethis`+
			` | eval day=relative_time(_time, "+1d"),`+
			` month=relative_time(_time, "@mon")`+
			` | table day,month`,
		scope,
	)
	location := mustLocation(t, scope.SearchTimezone)
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
	boundaryTicks := strconv.FormatInt(
		localMinimum.Unix()*1_000_000_000,
		10,
	)
	if !strings.Contains(compiled.SQL, boundaryTicks) {
		t.Fatalf(
			"relative_time SQL missing canonical local lower boundary %s:\n%s",
			boundaryTicks,
			compiled.SQL,
		)
	}
	if strings.Contains(compiled.SQL, "timeZoneOffset(value)") {
		t.Fatalf(
			"calendar lower guard reused ClickHouse's clamped offset:\n%s",
			compiled.SQL,
		)
	}
}

func TestCompileRelativeTimeSupportsTimeNumericDynamicNestedAndPredicateContexts(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis`+
			` | eval event=relative_time(_time, "@s"),`+
			` anchored=relative_time(now(), "-2h@h"),`+
			` dynamic=relative_time(timestamp, "+1mon"),`+
			` parsed=relative_time(strptime(source, "%F %T"), "+1d"),`+
			` rendered=strftime(relative_time(_time, "@d"), "%F %T"),`+
			` conditional=if(relative_time(_time, "@d")>=0, 1, 0),`+
			` branched=case(relative_time(_time, "@h")>=0, 1, 1=1, 0)`+
			` | where relative_time(other_time, "@h")>=0`+
			` | table event,anchored,dynamic,parsed,rendered,conditional,branched`,
	)
	for _, required := range []string{
		`"__os_fields"."timestamp"`,
		`"__os_fields"."other_time"`,
		"parseDateTime64InJodaSyntaxOrNull(",
		"formatDateTimeInJodaSyntax(",
		`AS "event"`,
		`AS "anchored"`,
		`AS "dynamic"`,
		`AS "parsed"`,
		`AS "rendered"`,
		`AS "conditional"`,
		`AS "branched"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("composed relative_time SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, "now(") ||
		strings.Contains(compiled.SQL, "now64(") {
		t.Fatalf("relative_time(now()) depends on ClickHouse wall clock:\n%s", compiled.SQL)
	}
}

func TestCompileRelativeTimeConvertsDynamicIntegersWithoutFloat64Loss(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis`+
			` | eval shifted=relative_time(timestamp, "+0s")`+
			` | table shifted`,
	)
	for _, required := range []string{
		"dynamicType(value) IN ('Int8', 'Int16', 'Int32', 'Int64'",
		"accurateCastOrNull(value, 'Int64')",
		"toInt256(1000000000)",
		"ifNotFinite(",
		"<= 4096",
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf(
				"Dynamic integer relative_time conversion missing %q:\n%s",
				required,
				compiled.SQL,
			)
		}
	}
}

func TestCompileRelativeTimeSpecializesKnownDynamicDomains(t *testing.T) {
	t.Parallel()

	expression := &plan.ScalarCallExpression{
		Function: plan.ScalarFunctionRelativeTime,
		Arguments: []plan.ScalarExpression{
			&plan.ScalarFieldExpression{
				Field: plan.FieldRef{
					Name: "timestamp", Path: []string{"timestamp"},
				},
			},
			&plan.ScalarLiteralExpression{
				Value: plan.Value{
					Kind: plan.ValueKindString, String: "@s", Quoted: true,
				},
			},
		},
	}
	context := newCompileContext(time.Unix(0, 0), "UTC")
	context.unixTimestampBudget.dynamicDecimalBytes =
		MaximumUnixTimestampQueryDynamicDecimalBytes
	state := compileState{
		context: context,
		visible: map[string]fieldState{
			"timestamp": {
				valueSQL:      `"timestamp"`,
				existsSQL:     "1",
				kind:          fieldKindDynamic,
				dynamicDomain: dynamicScalarDomainNumeric,
			},
		},
	}
	var numeric compiledScalar
	for range 17 {
		var err error
		numeric, err = compileRelativeTimeScalar(expression, state)
		if err != nil {
			t.Fatalf("compile numeric-domain relative_time: %v", err)
		}
	}
	if context.unixTimestampBudget.dynamicDecimalBytes !=
		MaximumUnixTimestampQueryDynamicDecimalBytes {
		t.Fatalf(
			"numeric Dynamic consumed decimal budget: %+v",
			context.unixTimestampBudget,
		)
	}
	for _, forbidden := range []string{
		"open_splunk_type",
		"Map(String, String)",
		"match(",
	} {
		if strings.Contains(numeric.valueSQL, forbidden) {
			t.Fatalf(
				"numeric Dynamic relative_time retained %q:\n%s",
				forbidden,
				numeric.valueSQL,
			)
		}
	}

	textContext := newCompileContext(time.Unix(0, 0), "UTC")
	textContext.unixTimestampBudget.dynamicDecimalBytes =
		MaximumUnixTimestampQueryDynamicDecimalBytes
	state.context = textContext
	textField := state.visible["timestamp"]
	textField.valueSQL = `lowerUTF8("timestamp")`
	textField.dynamicDomain = dynamicScalarDomainText
	textField.materializeForPredicate = true
	state.visible["timestamp"] = textField
	text, err := compileRelativeTimeScalar(expression, state)
	if err != nil {
		t.Fatalf("compile text-domain relative_time: %v", err)
	}
	if text.valueSQL != "CAST(NULL AS Nullable(Float64))" ||
		!text.alwaysNull {
		t.Fatalf("text Dynamic relative_time did not specialize to null:\n%s", text.valueSQL)
	}
	for _, forbidden := range []string{
		"timestamp",
		"lowerUTF8",
		"Map(String, String)",
	} {
		if strings.Contains(text.valueSQL, forbidden) {
			t.Fatalf(
				"text Dynamic relative_time retained %q producer work:\n%s",
				forbidden,
				text.valueSQL,
			)
		}
	}
	if !text.materializeForPredicate {
		t.Fatal("text Dynamic relative_time lost predicate materialization")
	}
}

func TestCompileRelativeTimePreservesNullAndRejectsUnsupportedInputTypes(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval absent=relative_time(null, "+1h") | table absent`,
	)
	if !strings.Contains(
		compiled.SQL,
		`CAST(NULL AS Nullable(Float64)) AS "absent"`,
	) {
		t.Fatalf("relative_time null did not remain a typed numeric null:\n%s", compiled.SQL)
	}

	for _, test := range []struct {
		source string
		code   string
	}{
		{
			source: `index=gradethis | eval shifted=relative_time("0", "+1h")`,
			code:   "SPL_UNSUPPORTED_RELATIVE_TIME_VALUE_TYPE",
		},
		{
			source: `index=gradethis | eval shifted=relative_time(true, "+1h")`,
			code:   "SPL_UNSUPPORTED_RELATIVE_TIME_VALUE_TYPE",
		},
		{
			source: `index=gradethis | stats values(_time) AS times | eval shifted=relative_time(times, "+1h")`,
			code:   "SPL_UNSUPPORTED_MULTIVALUE_USAGE",
		},
	} {
		logical := buildPlan(t, test.source)
		_, err := (Compiler{}).Compile(logical)
		var diagnostic *plan.Diagnostic
		if !errors.As(err, &diagnostic) || diagnostic.Code != test.code {
			t.Fatalf("Compile(%q) error = %#v, want %s", test.source, err, test.code)
		}
	}
}

func TestCompileRelativeTimeRejectsMissingOrInvalidSearchTimezone(t *testing.T) {
	t.Parallel()

	for _, timezone := range []string{"", "Local", "OpenSplunk/Invalid"} {
		logical := buildPlan(
			t,
			`index=gradethis | eval shifted=relative_time(_time, "+1h")`,
		)
		logical.SearchTimezone = timezone
		_, err := (Compiler{}).Compile(logical)
		if err == nil || !strings.Contains(err.Error(), "search timezone") {
			t.Fatalf(
				"Compile timezone %q error = %v, want explicit rejection",
				timezone,
				err,
			)
		}
	}
}

func TestCompileRelativeTimeRejectsForgedPlans(t *testing.T) {
	t.Parallel()

	base := buildPlan(t, `index=gradethis`)
	value := func() plan.ScalarExpression {
		return &plan.ScalarFieldExpression{
			Field: plan.FieldRef{Name: "_time", Path: []string{"_time"}},
		}
	}
	specifier := func(text string, quoted bool) plan.ScalarExpression {
		return &plan.ScalarLiteralExpression{
			Value: plan.Value{
				Kind: plan.ValueKindString, String: text, Quoted: quoted,
			},
		}
	}
	var typedNil *plan.ScalarLiteralExpression
	boolean := &plan.ScalarCallExpression{
		Function:  plan.ScalarFunctionIsNull,
		Arguments: []plan.ScalarExpression{value()},
	}
	for _, test := range []struct {
		name       string
		expression *plan.ScalarCallExpression
		want       string
		wantCode   string
	}{
		{
			name: "zero arguments",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionRelativeTime,
			},
			want: "expected two arguments",
		},
		{
			name: "one argument",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionRelativeTime,
				Arguments: []plan.ScalarExpression{value()},
			},
			want: "expected two arguments",
		},
		{
			name: "three arguments",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionRelativeTime,
				Arguments: []plan.ScalarExpression{
					value(), specifier("@d", true), specifier("+1h", true),
				},
			},
			want: "expected two arguments",
		},
		{
			name: "typed nil input",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionRelativeTime,
				Arguments: []plan.ScalarExpression{
					typedNil, specifier("@d", true),
				},
			},
			want: "missing scalar expression",
		},
		{
			name: "typed nil specifier",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionRelativeTime,
				Arguments: []plan.ScalarExpression{
					value(), typedNil,
				},
			},
			want: "specifier must be a quoted string literal",
		},
		{
			name: "nonliteral specifier",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionRelativeTime,
				Arguments: []plan.ScalarExpression{
					value(), value(),
				},
			},
			want: "specifier must be a quoted string literal",
		},
		{
			name: "unquoted specifier",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionRelativeTime,
				Arguments: []plan.ScalarExpression{
					value(), specifier("@d", false),
				},
			},
			want: "specifier must be a quoted string literal",
		},
		{
			name: "Boolean input",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionRelativeTime,
				Arguments: []plan.ScalarExpression{
					boolean, specifier("@d", true),
				},
			},
			want: "cannot consume a Boolean",
		},
		{
			name: "invalid specifier",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionRelativeTime,
				Arguments: []plan.ScalarExpression{
					value(), specifier("-1d@d+2h+1s", true),
				},
			},
			want:     "offset-and-snap",
			wantCode: "SPL_UNSUPPORTED_RELATIVE_TIME_SPECIFIER",
		},
		{
			name: "magnitude out of range",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionRelativeTime,
				Arguments: []plan.ScalarExpression{
					value(), specifier("+363y", true),
				},
			},
			want:     "timestamp span",
			wantCode: "SPL_NUMBER_OUT_OF_RANGE",
		},
		{
			name: "oversized specifier",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionRelativeTime,
				Arguments: []plan.ScalarExpression{
					value(),
					specifier(
						"+1"+strings.Repeat(
							"s",
							splrelativetime.MaximumSpecifierBytes+1,
						),
						true,
					),
				},
			},
			want:     "resource limit",
			wantCode: "SPL_QUERY_TOO_COMPLEX",
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := compileForgedScalarAssignment(t, base, test.expression)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf(
					"Compile forged relative_time error = %v, want %q",
					err,
					test.want,
				)
			}
			if test.wantCode != "" {
				var diagnostic *plan.Diagnostic
				if !errors.As(err, &diagnostic) ||
					diagnostic.Code != test.wantCode {
					t.Fatalf(
						"Compile error = %#v, want %s",
						err,
						test.wantCode,
					)
				}
			}
		})
	}
}

func TestCompileRelativeTimeNestedSQLGrowsLinearlyAndEvaluatesInputOnce(t *testing.T) {
	t.Parallel()

	expression := "timestamp"
	for range 20 {
		expression = `relative_time(` + expression + `, "+1s")`
	}
	compiled := compileSPL(
		t,
		`index=gradethis | eval shifted=`+expression+` | table shifted`,
	)
	if len(compiled.SQL) > maxCompiledRelativeTimeScalarSQLBytes {
		t.Fatalf(
			"nested relative_time SQL = %d bytes, want at most %d",
			len(compiled.SQL),
			maxCompiledRelativeTimeScalarSQLBytes,
		)
	}
	if strings.Count(compiled.SQL, `"__os_fields"."timestamp"`) != 1 {
		t.Fatalf("nested Dynamic input was duplicated:\n%s", compiled.SQL)
	}
}

func TestNewCompileContextAllocatesRelativeTimeCacheLazily(t *testing.T) {
	t.Parallel()

	context := newCompileContext(time.Unix(0, 0), "UTC")
	if context.relativeTimeBudget.specifiers != nil {
		t.Fatal("newCompileContext eagerly allocated the relative_time cache")
	}
}

func TestCompileRelativeTimeBoundsQueryWideSpecifierWork(t *testing.T) {
	t.Parallel()

	input := &plan.ScalarFieldExpression{
		Field: plan.FieldRef{Name: "_time", Path: []string{"_time"}},
	}
	specifier := &plan.ScalarLiteralExpression{
		Value: plan.Value{
			Kind: plan.ValueKindString,
			String: "+" + strings.Repeat(
				"0",
				splrelativetime.MaximumSpecifierWorkUnits-3,
			) + "1s",
			Quoted: true,
		},
	}
	shared := &plan.ScalarCallExpression{
		Function: plan.ScalarFunctionRelativeTime,
		Arguments: []plan.ScalarExpression{
			input,
			specifier,
		},
	}
	arguments := make([]plan.ScalarExpression, 17)
	for index := range arguments {
		arguments[index] = shared
	}
	err := compileForgedScalarAssignment(
		t,
		buildPlan(t, `index=gradethis`),
		&plan.ScalarCallExpression{
			Function:  plan.ScalarFunctionCoalesce,
			Arguments: arguments,
		},
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"relative_time specifiers require more than",
	) {
		t.Fatalf("Compile query-wide relative_time error = %v", err)
	}
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) ||
		diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
		t.Fatalf("Compile error = %#v, want SPL_QUERY_TOO_COMPLEX", err)
	}
}

func TestCompileRelativeTimeBoundsQueryWideOperationCount(t *testing.T) {
	t.Parallel()

	expression := &plan.ScalarCallExpression{
		Function: plan.ScalarFunctionRelativeTime,
		Arguments: []plan.ScalarExpression{
			&plan.ScalarLiteralExpression{
				Value: plan.Value{Kind: plan.ValueKindInt64, Int64: 0},
			},
			&plan.ScalarLiteralExpression{
				Value: plan.Value{
					Kind: plan.ValueKindString, String: "@s", Quoted: true,
				},
			},
		},
	}
	context := newCompileContext(time.Unix(0, 0), "UTC")
	context.relativeTimeBudget.operations = MaximumRelativeTimeQueryOperations
	_, err := compileRelativeTimeScalar(
		expression,
		compileState{context: context},
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"relative_time specifiers contain more than",
	) {
		t.Fatalf("Compile query-wide relative_time error = %v", err)
	}
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) ||
		diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
		t.Fatalf("Compile error = %#v, want SPL_QUERY_TOO_COMPLEX", err)
	}
}

func TestCompileRelativeTimeBoundsQueryWideDynamicDecimalParsing(t *testing.T) {
	t.Parallel()

	shared := &plan.ScalarCallExpression{
		Function: plan.ScalarFunctionRelativeTime,
		Arguments: []plan.ScalarExpression{
			&plan.ScalarFieldExpression{
				Field: plan.FieldRef{
					Name: "timestamp", Path: []string{"timestamp"},
				},
			},
			&plan.ScalarLiteralExpression{
				Value: plan.Value{
					Kind: plan.ValueKindString, String: "@s", Quoted: true,
				},
			},
		},
	}
	occurrences := int(
		MaximumUnixTimestampQueryDynamicDecimalBytes/
			uint64(MaximumUnixTimestampDynamicDecimalBytes),
	) + 1
	arguments := make([]plan.ScalarExpression, occurrences)
	for index := range arguments {
		arguments[index] = shared
	}
	err := compileForgedScalarAssignment(
		t,
		buildPlan(t, `index=gradethis`),
		&plan.ScalarCallExpression{
			Function:  plan.ScalarFunctionCoalesce,
			Arguments: arguments,
		},
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"Dynamic decimal timestamp inputs require more than",
	) {
		t.Fatalf("Compile Dynamic decimal budget error = %v", err)
	}
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) ||
		diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
		t.Fatalf("Compile error = %#v, want SPL_QUERY_TOO_COMPLEX", err)
	}
}
