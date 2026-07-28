package clickhouse

import (
	"errors"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spltimeformat"
)

func TestCompileStrftimeUsesPinnedTimezoneAndPortableDirectiveLowering(t *testing.T) {
	t.Parallel()

	scope := testChartScope()
	scope.SearchTimezone = "America/Los_Angeles"
	compiled := compileSPLWithScope(
		t,
		`index=gradethis | eval rendered=strftime(_time, "%Y-%m-%dT%H:%M:%S.%Q%:z") | table rendered`,
		scope,
	)
	for _, required := range []string{
		"formatDateTimeInJodaSyntax(",
		"formatDateTime(",
		"arrayMap(",
		`AS "rendered"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("strftime SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, "timezone()") ||
		strings.Contains(compiled.SQL, "serverTimeZone") ||
		!containsArgument(compiled.Args, scope.SearchTimezone) {
		t.Fatalf(
			"strftime did not bind the search timezone: args=%#v\nSQL: %s",
			compiled.Args,
			compiled.SQL,
		)
	}
}

func TestCompileStrftimeSupportsFixedTimeNumericNowAndPredicateContexts(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis`+
			` | eval epoch=strftime(0, "%s"), admitted=strftime(now(), "%F %T"), event=strftime(_time, "%9N")`+
			` | where strftime(_time, "%Y")="2026"`+
			` | table epoch,admitted,event`,
	)
	for _, required := range []string{
		"fromUnixTimestamp64Nano(",
		"toUnixTimestamp64Nano(",
		`AS "epoch"`,
		`AS "admitted"`,
		`AS "event"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("composed strftime SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, "now(") || strings.Contains(compiled.SQL, "now64(") {
		t.Fatalf("strftime(now()) depends on ClickHouse wall clock:\n%s", compiled.SQL)
	}
}

func TestCompileStrftimeRejectsMissingOrInvalidSearchTimezone(t *testing.T) {
	t.Parallel()

	for _, timezone := range []string{"", "Local", "OpenSplunk/Invalid"} {
		logical := buildPlan(t, `index=gradethis | eval rendered=strftime(_time, "%F")`)
		logical.SearchTimezone = timezone
		_, err := (Compiler{}).Compile(logical)
		if err == nil || !strings.Contains(err.Error(), "search timezone") {
			t.Fatalf("Compile timezone %q error = %v, want explicit rejection", timezone, err)
		}
	}
}

func TestCompileStrftimeRejectsFixedStringBooleanAndMultivalueInputs(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
		code   string
	}{
		{
			name:   "fixed string",
			source: `index=gradethis | eval rendered=strftime("0", "%F")`,
		},
		{
			name:   "multivalue",
			source: `index=gradethis | stats values(_time) AS times | eval rendered=strftime(times, "%F")`,
			code:   "SPL_UNSUPPORTED_MULTIVALUE_USAGE",
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			query, err := (Compiler{}).Compile(buildPlan(t, test.source))
			if err == nil {
				t.Fatalf("Compile unexpectedly succeeded: %#v", query)
			}
			if test.code != "" {
				var diagnostic *plan.Diagnostic
				if !errors.As(err, &diagnostic) || diagnostic.Code != test.code {
					t.Fatalf("Compile error = %#v, want %s", err, test.code)
				}
			}
		})
	}
}

func TestCompileStrftimeRejectsForgedPlans(t *testing.T) {
	t.Parallel()

	base := buildPlan(t, `index=gradethis`)
	value := func() plan.ScalarExpression {
		return &plan.ScalarFieldExpression{
			Field: plan.FieldRef{Name: "_time", Path: []string{"_time"}},
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
			expression: &plan.ScalarCallExpression{Function: plan.ScalarFunctionStrftime},
			want:       "expected two arguments",
		},
		{
			name: "one argument",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionStrftime,
				Arguments: []plan.ScalarExpression{value()},
			},
			want: "expected two arguments",
		},
		{
			name: "three arguments",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionStrftime,
				Arguments: []plan.ScalarExpression{
					value(), format("%F", true), format("%T", true),
				},
			},
			want: "expected two arguments",
		},
		{
			name: "typed nil format",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionStrftime,
				Arguments: []plan.ScalarExpression{value(), typedNil},
			},
			want: "format must be a quoted string literal",
		},
		{
			name: "nonliteral format",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionStrftime,
				Arguments: []plan.ScalarExpression{value(), value()},
			},
			want: "format must be a quoted string literal",
		},
		{
			name: "unquoted format",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionStrftime,
				Arguments: []plan.ScalarExpression{value(), format("%F", false)},
			},
			want: "format must be a quoted string literal",
		},
		{
			name: "Boolean input",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionStrftime,
				Arguments: []plan.ScalarExpression{boolean, format("%F", true)},
			},
			want: "cannot consume a Boolean",
		},
		{
			name: "invalid format",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionStrftime,
				Arguments: []plan.ScalarExpression{value(), format("%Z", true)},
			},
			want:     "locale-stable",
			wantCode: "SPL_UNSUPPORTED_TIME_FORMAT",
		},
		{
			name: "oversized format",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionStrftime,
				Arguments: []plan.ScalarExpression{
					value(),
					format(
						strings.Repeat("x", spltimeformat.MaximumStrftimeFormatBytes+1),
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
				t.Fatalf("Compile forged strftime error = %v, want %q", err, test.want)
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

func TestCompileStrftimeNestedNumericSQLGrowsLinearly(t *testing.T) {
	t.Parallel()

	expression := "now()"
	for range 20 {
		expression = "floor(" + expression + ")"
	}
	compiled := compileSPL(
		t,
		`index=gradethis | eval rendered=strftime(`+expression+`, "%F %T.%9N") | table rendered`,
	)
	if len(compiled.SQL) > maxCompiledStrftimeScalarSQLBytes {
		t.Fatalf(
			"nested strftime SQL = %d bytes, want at most %d",
			len(compiled.SQL),
			maxCompiledStrftimeScalarSQLBytes,
		)
	}
	if got := countArgument(compiled.Args, buildPlan(t, `index=gradethis`).SearchStart.Unix()); got > 1 {
		t.Fatalf("nested input was duplicated: args=%#v", compiled.Args)
	}
}

func TestCompileStrftimeBoundsQueryWideFormatWorkAndOutput(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		format string
		want   string
	}{
		{
			name:   "work",
			format: strings.Repeat("x", spltimeformat.MaximumStrftimeWorkUnits),
			want:   "strftime formats require more than",
		},
		{
			name:   "output",
			format: strings.Repeat("%N", 1800),
			want:   "strftime results may exceed",
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			timeValue := &plan.ScalarFieldExpression{
				Field: plan.FieldRef{Name: "_time", Path: []string{"_time"}},
			}
			formatValue := &plan.ScalarLiteralExpression{
				Value: plan.Value{
					Kind: plan.ValueKindString, String: test.format, Quoted: true,
				},
			}
			shared := &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionStrftime,
				Arguments: []plan.ScalarExpression{
					timeValue,
					formatValue,
				},
			}
			arguments := make([]plan.ScalarExpression, 5)
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
				t.Fatalf("Compile query-wide format error = %v, want %q", err, test.want)
			}
			var diagnostic *plan.Diagnostic
			if !errors.As(err, &diagnostic) ||
				diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
				t.Fatalf("Compile error = %#v, want SPL_QUERY_TOO_COMPLEX", err)
			}
		})
	}
}
