package clickhouse

import (
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestCompileNativeMultivalueEvalFunctionsUseBoundedTypedTransport(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t,
		`index=gradethis `+
			`| eval parts=split("a😀a", ""), `+
			`appended=mvappend(parts, 7, 7.0, true, null), `+
			`unique=mvdedup(appended), `+
			`first=mvindex(unique, -1), `+
			`window=mvindex(unique, 0, 2), `+
			`joined=mvjoin(unique, "|"), `+
			`zipped=mvzip(parts, window, "::"), `+
			`found=mvfind(unique, "(?i)^a") `+
			`| table parts appended unique first window joined zipped found`,
	)

	for _, required := range []string{
		`substringUTF8(`,
		`arrayConcat(`,
		`concat(multiIf(startsWith(dynamicType(`,
		`arrayFilter((member, occurrence) -> occurrence = toUInt32(1)`,
		`arrayEnumerateUniq(`,
		`arrayElement(`,
		`arraySlice(`,
		`arrayStringConcat(`,
		`least(length(`,
		`arrayFirstIndex(canonical -> match(`,
		emptyNativeMVSQL(),
		UnsupportedNativeMVValueMarker,
		NativeMVMembersLimitMarker,
		NativeMVPayloadLimitMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("native multivalue SQL is missing %q:\n%s", required, compiled.SQL)
		}
	}
	if !compiled.RequiresAtomicResult() || !compiled.HasValidExecutionSeal() {
		t.Fatalf(
			"native multivalue query = atomic %t sealed %t",
			compiled.RequiresAtomicResult(),
			compiled.HasValidExecutionSeal(),
		)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d", got, want)
	}

	var optional []int
	for _, output := range compiled.OptionalMultivalueOutputs {
		optional = append(optional, int(output.OutputIndex))
	}
	if want := []int{0, 1, 2, 4, 6}; !slices.Equal(optional, want) {
		t.Fatalf("optional native multivalue outputs = %v, want %v", optional, want)
	}
	if !strings.Contains(compiled.SQL, `AS "__os_result_multivalue_present_4"`) {
		t.Fatalf("Array(Dynamic) range output lost the sealed presence sidecar:\n%s", compiled.SQL)
	}
}

func TestCompileNativeMultivalueCompositionPreservesTypesAndPresentationIndependence(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t,
		`index=gradethis `+
			`| eval values=mvappend(split("b,a,b", ","), 1, 1.0, false, null) `+
			`| eval values=mvdedup(values), selected=mvindex(values, -3, -1) `+
			`| eval rendered=mvjoin(selected, ","), found=mvfind(selected, "^(1|false)$") `+
			`| table values selected rendered found`,
	)

	if got := len(compiled.OptionalMultivalueOutputs); got != 2 {
		t.Fatalf("optional multivalue output count = %d, want 2", got)
	}
	if len(compiled.OutputPresentations) != 0 {
		t.Fatalf("native multivalue eval unexpectedly attached presentation metadata: %#v", compiled.OutputPresentations)
	}
	for _, required := range []string{
		`dynamicType(`,
		`startsWith(`,
		`'Decimal'`,
		nullNativeMVSQL(),
		`toInt64(-3)`,
		`toInt64(-1)`,
		`toInt64(`,
		`CAST(NULL AS Nullable(Int64))`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("composed native multivalue SQL is missing %q:\n%s", required, compiled.SQL)
		}
	}
	if !compiled.RequiresAtomicResult() || !compiled.HasValidExecutionSeal() {
		t.Fatalf(
			"composed native multivalue query = atomic %t sealed %t",
			compiled.RequiresAtomicResult(),
			compiled.HasValidExecutionSeal(),
		)
	}
}

func TestCompileNativeDynamicArrayFlowsThroughMVExpandAndStatsBy(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t,
		`index=gradethis `+
			`| eval users=mvdedup(mvappend("alice", "bob", "alice", 7, true, null)) `+
			`| mvexpand users `+
			`| stats count BY users`,
	)

	for _, required := range []string{
		emptyNativeMVSQL(),
		`__os_mvexpand_values_`,
		nullNativeMVSQL(),
		`arrayExists(member -> NOT (`,
		`ARRAY JOIN`,
		`GROUP BY`,
		UnsupportedNativeMVValueMarker,
		UnsupportedMVExpandValueMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("native Array(Dynamic) pipeline is missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got, want := compiled.OutputFields, []string{"users", "count"}; !slices.Equal(got, want) {
		t.Fatalf("native Array(Dynamic) stats output = %v, want %v", got, want)
	}
	if len(compiled.OptionalMultivalueOutputs) != 0 {
		t.Fatalf("expanded scalar leaked native-list transport: %#v", compiled.OptionalMultivalueOutputs)
	}
	if !compiled.RequiresAtomicResult() || !compiled.HasValidExecutionSeal() {
		t.Fatalf(
			"native Array(Dynamic) pipeline = atomic %t sealed %t",
			compiled.RequiresAtomicResult(),
			compiled.HasValidExecutionSeal(),
		)
	}
}

func TestCompileSplitAndZipPreflightSharedNativeMultivalueLimits(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source    string
		construct string
	}{
		{`index=gradethis | eval values=split(message, "") | table values`, `substringUTF8(`},
		{`index=gradethis | eval left=split(message, ","), right=split(source, ","), values=mvzip(left, right) | table values`, `arrayMap((left_member, right_member) -> concat(`},
		{`index=gradethis | eval values=mvappend(split(message, ","), split(source, ",")) | table values`, `arrayConcat(`},
	} {
		compiled := compileSPL(t, test.source)
		for _, marker := range []string{
			UnsupportedNativeMVValueMarker,
			NativeMVMembersLimitMarker,
			NativeMVPayloadLimitMarker,
		} {
			if !strings.Contains(compiled.SQL, marker) {
				t.Fatalf("Compile(%q) is missing runtime marker %q:\n%s", test.source, marker, compiled.SQL)
			}
		}
		for _, guard := range []string{
			`length(`,
			`throwIf(toUInt8(`,
		} {
			if !strings.Contains(compiled.SQL, guard) {
				t.Fatalf("Compile(%q) is missing preflight guard %q:\n%s", test.source, guard, compiled.SQL)
			}
		}
		memberGuard := strings.Index(compiled.SQL, NativeMVMembersLimitMarker)
		construction := strings.Index(compiled.SQL, test.construct)
		if memberGuard < 0 || construction < 0 || memberGuard > construction {
			t.Fatalf(
				"Compile(%q) does not preflight the member limit before %q:\n%s",
				test.source,
				test.construct,
				compiled.SQL,
			)
		}
		if !compiled.RequiresAtomicResult() || !compiled.HasValidExecutionSeal() {
			t.Fatalf(
				"Compile(%q) = atomic %t sealed %t",
				test.source,
				compiled.RequiresAtomicResult(),
				compiled.HasValidExecutionSeal(),
			)
		}
	}
}

func TestCompileNativeMultivalueRejectsFixedScalarListConsumers(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | eval value=mvdedup("scalar")`,
		`index=gradethis | eval value=mvindex(7, 0)`,
		`index=gradethis | eval value=mvjoin(true, ",")`,
		`index=gradethis | eval value=mvzip(split("a", ","), 7)`,
		`index=gradethis | eval value=mvfind("scalar", "x")`,
	} {
		_, err := (Compiler{}).Compile(buildPlan(t, source))
		if err == nil || !strings.Contains(err.Error(), "SPL_UNSUPPORTED_MULTIVALUE_USAGE") {
			t.Fatalf("Compile(%q) error = %v, want SPL_UNSUPPORTED_MULTIVALUE_USAGE", source, err)
		}
	}
}

func TestCompileNativeMultivalueEvalRejectsForgedPlans(t *testing.T) {
	t.Parallel()

	base := buildPlan(t, `index=gradethis`)
	quoted := func(value string) plan.ScalarExpression {
		return &plan.ScalarLiteralExpression{Value: plan.Value{
			Kind: plan.ValueKindString, String: value, Quoted: true,
		}}
	}
	unquoted := func(value string) plan.ScalarExpression {
		return &plan.ScalarLiteralExpression{Value: plan.Value{
			Kind: plan.ValueKindString, String: value,
		}}
	}
	integer := func(value int64) plan.ScalarExpression {
		return &plan.ScalarLiteralExpression{Value: plan.Value{
			Kind: plan.ValueKindInt64, Int64: value,
		}}
	}
	value := quoted("value")
	values := &plan.ScalarCallExpression{
		Function: plan.ScalarFunctionSplit,
		Arguments: []plan.ScalarExpression{
			quoted("a,b"), quoted(","),
		},
	}
	tooMany := make([]plan.ScalarExpression, spl.MaximumMVAppendArguments+1)
	for index := range tooMany {
		tooMany[index] = quoted("x")
	}
	for _, test := range []struct {
		name       string
		expression plan.ScalarExpression
		want       string
	}{
		{
			name:       "split arity",
			expression: &plan.ScalarCallExpression{Function: plan.ScalarFunctionSplit},
			want:       "expected two arguments",
		},
		{
			name: "split forged delimiter",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionSplit,
				Arguments: []plan.ScalarExpression{
					value, unquoted(","),
				},
			},
			want: "delimiter must be a bounded quoted string literal",
		},
		{
			name: "mvappend zero arguments",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionMVAppend,
			},
			want: "expected one through 32 arguments",
		},
		{
			name: "mvappend too many arguments",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionMVAppend, Arguments: tooMany,
			},
			want: "expected one through 32 arguments",
		},
		{
			name: "mvdedup arity",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionMVDedup,
			},
			want: "expected one argument",
		},
		{
			name: "mvindex forged start",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionMVIndex,
				Arguments: []plan.ScalarExpression{
					values, quoted("0"),
				},
			},
			want: "start must be a signed 32-bit integer literal",
		},
		{
			name: "mvindex arity",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionMVIndex,
				Arguments: []plan.ScalarExpression{
					values, integer(0), integer(1), integer(2),
				},
			},
			want: "expected two or three arguments",
		},
		{
			name: "mvjoin forged delimiter",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionMVJoin,
				Arguments: []plan.ScalarExpression{
					values, unquoted("|"),
				},
			},
			want: "delimiter must be a bounded quoted string literal",
		},
		{
			name: "mvzip arity",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionMVZip,
				Arguments: []plan.ScalarExpression{values},
			},
			want: "expected two or three arguments",
		},
		{
			name: "mvzip forged delimiter",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionMVZip,
				Arguments: []plan.ScalarExpression{
					values, values, unquoted("::"),
				},
			},
			want: "delimiter must be a bounded quoted string literal",
		},
		{
			name: "mvfind forged pattern",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionMVFind,
				Arguments: []plan.ScalarExpression{
					values, unquoted("x"),
				},
			},
			want: "pattern must be a quoted string literal",
		},
		{
			name: "mvfind unsupported regex",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionMVFind,
				Arguments: []plan.ScalarExpression{
					values, quoted("(?=secret)"),
				},
			},
			want: "outside the supported RE2 subset",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := compileForgedScalarAssignment(t, base, test.expression)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Compile forged native multivalue expression error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestCompileMVAppendRetainsPresentNullableScalarAsExplicitNull(t *testing.T) {
	t.Parallel()

	normalized, err := compileNativeMVState(compiledScalar{
		valueSQL:  `"nullable_value"`,
		existsSQL: `"value_present"`,
		kind:      fieldKindString,
	}, true)
	if err != nil {
		t.Fatalf("compileNativeMVState: %v", err)
	}
	if !strings.Contains(normalized.sql,
		`if(field_exists != 0, [CAST(value AS Dynamic)], `+emptyNativeMVSQL()+`)`) {
		t.Fatalf("nullable scalar normalization drops an explicit null:\n%s", normalized.sql)
	}
	if strings.Contains(normalized.sql,
		`field_exists != 0 AND isNotNull(value), [CAST(value AS Dynamic)]`) {
		t.Fatalf("nullable scalar normalization still skips an explicit null:\n%s", normalized.sql)
	}
}

func TestCompileMVAppendRetainsExplicitNullNativeMVAndListConsumersPropagateMissing(t *testing.T) {
	t.Parallel()

	nullableMV, err := compileNativeMVState(compiledScalar{
		valueSQL:                     `"nullable_mv"`,
		existsSQL:                    `"mv_exists"`,
		optionalMultivaluePresentSQL: `"mv_present"`,
		kind:                         fieldKindDynamicArray,
	}, true)
	if err != nil {
		t.Fatalf("compileNativeMVState nullable MV: %v", err)
	}
	for _, required := range []string{
		`field_exists != 0, ` + nullNativeMVSQL(),
		`toUInt8(field_exists != 0)`,
	} {
		if !strings.Contains(nullableMV.sql, required) {
			t.Fatalf("nullable MV normalization is missing %q:\n%s", required, nullableMV.sql)
		}
	}

	retained := compileSPL(t,
		`index=gradethis `+
			`| eval ranged=mvindex(split("a", ","), 1, 2), appended=mvappend(ranged) `+
			`| table appended`,
	)
	if !strings.Contains(retained.SQL, nullNativeMVSQL()) {
		t.Fatalf("mvappend dropped an explicit-null MV argument:\n%s", retained.SQL)
	}

	missing := compileSPL(t,
		`index=gradethis | fields host `+
			`| eval deduped=mvdedup(absent), indexed=mvindex(deduped, 0), `+
			`joined=mvjoin(deduped, ","), found=mvfind(deduped, "x"), `+
			`zipped=mvzip(deduped, deduped) `+
			`| table deduped indexed joined found zipped`,
	)
	if !strings.Contains(missing.SQL,
		`tuple(`+emptyNativeMVSQL()+`, toUInt8(0), toUInt8(0), toUInt8(0))`) {
		t.Fatalf("list consumers did not propagate a statically missing input:\n%s", missing.SQL)
	}
}

func TestNativeMVCanonicalTextNormalizesTaggedAndPhysicalDecimals(t *testing.T) {
	t.Parallel()

	textSQL := nativeMVCanonicalTextSQL("member")
	for _, required := range []string{
		`startsWith(dynamicType(member), 'Decimal')`,
		`canonical_decimal_adjusted_exponent`,
		`decimal/v1`,
	} {
		if !strings.Contains(textSQL, required) {
			t.Fatalf("native canonical Decimal SQL is missing %q:\n%s", required, textSQL)
		}
	}
	keySQL := nativeMVDedupKeySQL("member")
	for _, logicalType := range []string{"Decimal", "Signed", "Unsigned", "Double"} {
		if !strings.Contains(keySQL, `CAST('`+logicalType+`' AS String)`) {
			t.Fatalf("native dedup key does not normalize %s widths:\n%s", logicalType, keySQL)
		}
	}
	for _, zeroNormalization := range []string{
		`startsWith(dynamicType(member), 'Float')`,
		`toFloat64(member) = toFloat64(0)`,
		`CAST('0' AS String)`,
	} {
		if !strings.Contains(keySQL, zeroNormalization) {
			t.Fatalf("native dedup key does not normalize signed floating zero with %q:\n%s", zeroNormalization, keySQL)
		}
	}
}

func TestCompileNativeMVUsesLinearDedupAndGuardedConstruction(t *testing.T) {
	t.Parallel()

	deduped := compileSPL(t,
		`index=gradethis | eval values=mvdedup(mvappend(-0.0, 0.0, "x", "x")) | table values`,
	)
	for _, required := range []string{
		`arrayEnumerateUniq(__os_mvdedup_keys)`,
		`arrayFilter((member, occurrence) -> occurrence = toUInt32(1)`,
		`toFloat64(member) = toFloat64(0)`,
	} {
		if !strings.Contains(deduped.SQL, required) {
			t.Fatalf("linear native dedup SQL is missing %q:\n%s", required, deduped.SQL)
		}
	}
	if strings.Contains(deduped.SQL, `indexOf(__os_mvdedup_keys`) {
		t.Fatalf("native dedup still uses repeated indexOf:\n%s", deduped.SQL)
	}

	appended := compileSPL(t,
		`index=gradethis | eval values=mvappend(split(message, ","), split(source, ",")) | table values`,
	)
	for _, eagerNestedArrayFold := range []string{
		`arrayFold((count, members)`,
	} {
		if strings.Contains(appended.SQL, eagerNestedArrayFold) {
			t.Fatalf("mvappend still constructs a nested input array through %q:\n%s", eagerNestedArrayFold, appended.SQL)
		}
	}
	for _, required := range []string{
		`toUInt128(length(tupleElement(__os_mvappend_state_0, 1))) + toUInt128(length(tupleElement(__os_mvappend_state_1, 1)))`,
		`arrayFold((bytes, members) -> bytes + arrayFold((member_bytes, member) ->`,
	} {
		if !strings.Contains(appended.SQL, required) {
			t.Fatalf("mvappend checked-sum preflight is missing %q:\n%s", required, appended.SQL)
		}
	}
	memberGuard := strings.Index(appended.SQL, NativeMVMembersLimitMarker)
	payloadFold := strings.Index(appended.SQL, `arrayFold((bytes, members)`)
	payloadGuard := strings.Index(appended.SQL, NativeMVPayloadLimitMarker)
	construction := strings.Index(appended.SQL, `arrayConcat(`)
	if memberGuard < 0 || payloadFold < 0 || payloadGuard < 0 || construction < 0 ||
		memberGuard > payloadFold || payloadGuard > construction {
		t.Fatalf("mvappend preflight/construction order is invalid:\n%s", appended.SQL)
	}

	zipped := compileSPL(t,
		`index=gradethis | eval left=split(message, ","), right=split(source, ","), values=mvzip(left, right) | table values`,
	)
	for _, required := range []string{
		`members), [arraySlice(tupleElement(__os_mvzip_left, 1), 1, least(length(`,
		`arraySlice(tupleElement(__os_mvzip_right, 1), 1, least(length(`,
	} {
		if !strings.Contains(zipped.SQL, required) {
			t.Fatalf("mvzip shortest-prefix canonicalization is missing %q:\n%s", required, zipped.SQL)
		}
	}
	if strings.Contains(zipped.SQL, `right_member), arraySlice(arrayElement(__os_mvzip_canonical_arrays`) {
		t.Fatalf("mvzip still slices after canonicalizing complete arrays:\n%s", zipped.SQL)
	}
}

func TestCompileSplitCanonicalInputBindsTextEligibilityArguments(t *testing.T) {
	t.Parallel()

	canonical, err := compileCanonicalScalarState(compiledScalar{
		valueSQL:          `if(? = 'value', CAST('ok' AS Nullable(String)), NULL)`,
		valueArgs:         []any{"value"},
		existsSQL:         `? = 'present'`,
		existsArgs:        []any{"present"},
		descendantSQL:     `? = 'descendant'`,
		descendantArgs:    []any{"descendant"},
		textEligibleSQL:   `? = 'text'`,
		semanticBytesArgs: []any{"text"},
		kind:              fieldKindString,
	})
	if err != nil {
		t.Fatalf("compileCanonicalScalarState: %v", err)
	}
	if got, want := canonical.args, []any{"value", "present", "descendant", "text"}; !slices.Equal(got, want) {
		t.Fatalf("canonical input args = %#v, want %#v", got, want)
	}
	if got, want := strings.Count(canonical.sql, "?"), len(canonical.args); got != want {
		t.Fatalf("canonical input placeholders = %d, args = %d:\n%s", got, want, canonical.sql)
	}
}

func TestCompileNestedNativeMVValidationSurvivesProjection(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t,
		`index=gradethis `+
			`| eval found=mvfind(mvdedup(split(message, ",")), "api")+1 `+
			`| fields event_id`,
	)
	for _, required := range []string{
		`__os_eval_mv_validation_`,
		`AS MATERIALIZED`,
		`ignore("found")`,
		UnsupportedNativeMVValueMarker,
		NativeMVMembersLimitMarker,
		NativeMVPayloadLimitMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("nested native-MV validation lost %q after projection:\n%s", required, compiled.SQL)
		}
	}
	if !slices.Contains(compiled.OutputFields, "event_id") ||
		slices.Contains(compiled.OutputFields, "found") {
		t.Fatalf("projected output fields = %v, want event_id without found", compiled.OutputFields)
	}
}
