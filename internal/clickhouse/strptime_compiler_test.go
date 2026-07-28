package clickhouse

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spltimeformat"
)

func TestCompileStrptimeUsesBoundedParameterizedJodaParserAndPinnedTimezone(t *testing.T) {
	t.Parallel()

	scope := testChartScope()
	scope.SearchTimezone = "America/Los_Angeles"
	compiled := compileSPLWithScope(
		t,
		`index=gradethis | eval epoch=strptime(source, "%F %T.%6N") | table epoch`,
		scope,
	)
	for _, required := range []string{
		"parseDateTime64InJodaSyntaxOrNull(",
		"toUnixTimestamp64Micro(",
		"length(value) <= 4096",
		"extractGroups(ifNull(value, CAST('' AS String)), ?)",
		"19710101",
		"22991231",
		"arrayMap(",
		`AS "epoch"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("strptime SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	for _, forbidden := range []string{
		"parseDateTime64BestEffortOrNull(value",
		"timezone()",
		"serverTimeZone",
		"31536000000000",
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("strptime SQL contains unpinned parser behavior %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if countArgument(compiled.Args, "yyyy-MM-dd HH:mm:ss.SSSSSS") != 1 ||
		countArgument(compiled.Args, "yyyy-MM-dd HH:mm:ss") != 1 ||
		countArgument(compiled.Args, scope.SearchTimezone) != 2 {
		t.Fatalf("strptime parser arguments = %#v", compiled.Args)
	}
	wantPrefix := []any{
		"yyyy-MM-dd HH:mm:ss",
		scope.SearchTimezone,
		"yyyy-MM-dd HH:mm:ss.SSSSSS",
		scope.SearchTimezone,
		`^([0-9]{4})-([0-9]{1,2})-([0-9]{1,2}) [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}(?:\.[0-9]{1,6})?$`,
	}
	for index, want := range wantPrefix {
		if index >= len(compiled.Args) {
			t.Fatalf("strptime args = %#v, want prefix %#v", compiled.Args, wantPrefix)
		}
		if compiled.Args[index] != want {
			t.Fatalf(
				"strptime argument %d = %#v, want %#v; args = %#v",
				index,
				compiled.Args[index],
				want,
				compiled.Args,
			)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s", got, want, compiled.SQL)
	}
}

func TestCompileStrptimeUsesOneParserWithoutOptionalFractionFallback(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval epoch=strptime(source, "%F %T") | table epoch`,
	)
	if strings.Count(compiled.SQL, "parseDateTime64InJodaSyntaxOrNull(") != 1 {
		t.Fatalf("ordinary strptime parser was duplicated:\n%s", compiled.SQL)
	}
	if countArgument(compiled.Args, "yyyy-MM-dd HH:mm:ss") != 1 {
		t.Fatalf("ordinary strptime parser arguments = %#v", compiled.Args)
	}
}

func TestCompileStrptimeSupportsFixedDynamicPredicateAndNestedStrftimeInputs(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis`+
			` | eval literal=strptime("2026-07-27", "%F"),`+
			` fixed=strptime(source, "%F"),`+
			` dynamic=strptime(timestamp, "%F"),`+
			` roundtrip=strptime(strftime(_time, "%F %T"), "%F %T")`+
			` | where strptime(other_timestamp, "%F")>=0`+
			` | table literal,fixed,dynamic,roundtrip`,
	)
	for _, required := range []string{
		`dynamicElement("__os_fields"."timestamp", 'String')`,
		`dynamicElement("__os_fields"."other_timestamp", 'String')`,
		"formatDateTimeInJodaSyntax(",
		"parseDateTime64InJodaSyntaxOrNull(",
		`AS "literal"`,
		`AS "fixed"`,
		`AS "dynamic"`,
		`AS "roundtrip"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("composed strptime SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
}

func TestCompileStrptimePreservesBinaryTextProvenanceAndNullInput(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval raw_epoch=strptime(_raw, "%F"), absent=strptime(null, "%F") | table raw_epoch,absent`,
	)
	if !strings.Contains(
		compiled.SQL,
		`if(ifNull("__os_raw_encoding" = 1, 0), "_raw", CAST(NULL AS Nullable(String)))`,
	) {
		t.Fatalf("strptime lost _raw UTF-8 provenance guard:\n%s", compiled.SQL)
	}
	if !strings.Contains(compiled.SQL, `CAST(NULL AS Nullable(Float64)) AS "absent"`) {
		t.Fatalf("strptime null did not remain a typed numeric null:\n%s", compiled.SQL)
	}
}

func TestCompileStrptimeRejectsFixedNonStringAndMultivalueInputs(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		code   string
	}{
		{
			source: `index=gradethis | eval epoch=strptime(123, "%F")`,
			code:   "SPL_UNSUPPORTED_STRPTIME_VALUE_TYPE",
		},
		{
			source: `index=gradethis | eval epoch=strptime(true, "%F")`,
			code:   "SPL_UNSUPPORTED_STRPTIME_VALUE_TYPE",
		},
		{
			source: `index=gradethis | eval epoch=strptime(_time, "%F")`,
			code:   "SPL_UNSUPPORTED_STRPTIME_VALUE_TYPE",
		},
		{
			source: `index=gradethis | stats values(source) AS sources | eval epoch=strptime(sources, "%F")`,
			code:   "SPL_UNSUPPORTED_MULTIVALUE_USAGE",
		},
	} {
		logical := buildPlan(t, test.source)
		_, err := (Compiler{}).Compile(logical)
		var diagnostic *plan.Diagnostic
		if !errors.As(err, &diagnostic) || diagnostic.Code != test.code {
			t.Fatalf("Compile(%q) error = %#v, want %s", test.source, err, test.code)
		}
		if diagnostic.Range.Start.Offset >= diagnostic.Range.End.Offset {
			t.Fatalf("Compile(%q) diagnostic range = %#v", test.source, diagnostic.Range)
		}
	}
}

func TestCompileStrptimeRejectsMissingOrInvalidSearchTimezone(t *testing.T) {
	t.Parallel()

	for _, timezone := range []string{"", "Local", "OpenSplunk/Invalid"} {
		logical := buildPlan(t, `index=gradethis | eval epoch=strptime(source, "%F")`)
		logical.SearchTimezone = timezone
		_, err := (Compiler{}).Compile(logical)
		if err == nil || !strings.Contains(err.Error(), "search timezone") {
			t.Fatalf("Compile timezone %q error = %v, want explicit rejection", timezone, err)
		}
	}
}

func TestCompileStrptimeRejectsForgedPlans(t *testing.T) {
	t.Parallel()

	base := buildPlan(t, `index=gradethis`)
	value := func() plan.ScalarExpression {
		return &plan.ScalarFieldExpression{
			Field: plan.FieldRef{Name: "source", Path: []string{"source"}},
		}
	}
	format := func(text string, quoted bool) plan.ScalarExpression {
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
			name:       "zero arguments",
			expression: &plan.ScalarCallExpression{Function: plan.ScalarFunctionStrptime},
			want:       "expected two arguments",
		},
		{
			name: "one argument",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionStrptime,
				Arguments: []plan.ScalarExpression{value()},
			},
			want: "expected two arguments",
		},
		{
			name: "three arguments",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionStrptime,
				Arguments: []plan.ScalarExpression{
					value(), format("%F", true), format("%T", true),
				},
			},
			want: "expected two arguments",
		},
		{
			name: "typed nil input",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionStrptime,
				Arguments: []plan.ScalarExpression{typedNil, format("%F", true)},
			},
			want: "missing scalar expression",
		},
		{
			name: "typed nil format",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionStrptime,
				Arguments: []plan.ScalarExpression{value(), typedNil},
			},
			want: "format must be a quoted string literal",
		},
		{
			name: "nonliteral format",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionStrptime,
				Arguments: []plan.ScalarExpression{value(), value()},
			},
			want: "format must be a quoted string literal",
		},
		{
			name: "unquoted format",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionStrptime,
				Arguments: []plan.ScalarExpression{value(), format("%F", false)},
			},
			want: "format must be a quoted string literal",
		},
		{
			name: "Boolean input",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionStrptime,
				Arguments: []plan.ScalarExpression{boolean, format("%F", true)},
			},
			want: "cannot consume a Boolean",
		},
		{
			name: "invalid format",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionStrptime,
				Arguments: []plan.ScalarExpression{value(), format("%Y-%m", true)},
			},
			want:     "full-date",
			wantCode: "SPL_UNSUPPORTED_TIME_FORMAT",
		},
		{
			name: "formatter output amplification is unsupported",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionStrptime,
				Arguments: []plan.ScalarExpression{
					value(), format(strings.Repeat("%F", 1700), true),
				},
			},
			want:     "full-date",
			wantCode: "SPL_UNSUPPORTED_TIME_FORMAT",
		},
		{
			name: "oversized format",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionStrptime,
				Arguments: []plan.ScalarExpression{
					value(),
					format(
						strings.Repeat(
							"x",
							spltimeformat.MaximumStrptimeFormatBytes+1,
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
				t.Fatalf("Compile forged strptime error = %v, want %q", err, test.want)
			}
			if test.wantCode != "" {
				var diagnostic *plan.Diagnostic
				if !errors.As(err, &diagnostic) || diagnostic.Code != test.wantCode {
					t.Fatalf("Compile error = %#v, want %s", err, test.wantCode)
				}
			}
		})
	}
}

func TestCompileStrptimeNestedTextSQLGrowsLinearlyAndEvaluatesInputOnce(t *testing.T) {
	t.Parallel()

	expression := "timestamp"
	for range 20 {
		expression = "lower(" + expression + ")"
	}
	compiled := compileSPL(
		t,
		`index=gradethis | eval epoch=strptime(`+expression+`, "%F %T") | table epoch`,
	)
	if len(compiled.SQL) > maxCompiledStrptimeScalarSQLBytes {
		t.Fatalf(
			"nested strptime SQL = %d bytes, want at most %d",
			len(compiled.SQL),
			maxCompiledStrptimeScalarSQLBytes,
		)
	}
	if strings.Count(compiled.SQL, `"__os_fields"."timestamp"`) != 1 {
		t.Fatalf("nested Dynamic input was duplicated:\n%s", compiled.SQL)
	}
	if strings.Count(compiled.SQL, "parseDateTime64InJodaSyntaxOrNull(") != 1 {
		t.Fatalf("strptime parser was duplicated:\n%s", compiled.SQL)
	}
}

func TestNewCompileContextAllocatesStrptimeCacheLazily(t *testing.T) {
	t.Parallel()

	context := newCompileContext(time.Unix(0, 0), "UTC")
	if context.strptimeBudget.formats != nil {
		t.Fatal("newCompileContext eagerly allocated the strptime format cache")
	}
}

func TestCompileStrptimeBoundsQueryWideFormatWorkAndInputBytes(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		format    string
		arguments int
		want      string
	}{
		{
			name:      "format work",
			format:    "%F" + strings.Repeat("x", spltimeformat.MaximumStrptimeWorkUnits-2),
			arguments: 5,
			want:      "strptime formats require more than",
		},
		{
			name:      "input bytes",
			format:    "%F",
			arguments: 17,
			want:      "strptime inputs require more than",
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			input := &plan.ScalarFieldExpression{
				Field: plan.FieldRef{Name: "source", Path: []string{"source"}},
			}
			format := &plan.ScalarLiteralExpression{
				Value: plan.Value{
					Kind: plan.ValueKindString, String: test.format, Quoted: true,
				},
			}
			shared := &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionStrptime,
				Arguments: []plan.ScalarExpression{input, format},
			}
			arguments := make([]plan.ScalarExpression, test.arguments)
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
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Compile query-wide strptime error = %v, want %q", err, test.want)
			}
			var diagnostic *plan.Diagnostic
			if !errors.As(err, &diagnostic) ||
				diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
				t.Fatalf("Compile error = %#v, want SPL_QUERY_TOO_COMPLEX", err)
			}
		})
	}
}
