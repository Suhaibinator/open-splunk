package clickhouse

import (
	"math/big"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/jsonnumber"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestExactNumericOrderingSQLStaysBounded(t *testing.T) {
	t.Parallel()

	dynamicExtrema, _ := statsExtremaDynamicCandidatesSQL(fieldState{
		valueSQL:       "value",
		dynamicTypeSQL: "dynamicType(value)",
		existsSQL:      "present",
		descendantSQL:  "descendant",
		kind:           fieldKindDynamic,
	})
	for _, test := range []struct {
		name    string
		sql     string
		maximum int
	}{
		{
			name:    "ordering key",
			sql:     exactNumericOrderingKeySQL("value"),
			maximum: 4 << 10,
		},
		{
			name:    "decimal envelope",
			sql:     decimalEnvelopeTextSQL("value"),
			maximum: 2 << 10,
		},
		{
			name:    "scalar extrema candidate",
			sql:     statsExtremaScalarCandidateSQL("value", "number", "0"),
			maximum: 16 << 10,
		},
		{
			name:    "array extrema candidates",
			sql:     statsExtremaCandidatesSQL("values"),
			maximum: 12 << 10,
		},
		{
			name:    "dynamic extrema candidates",
			sql:     dynamicExtrema,
			maximum: 64 << 10,
		},
		{
			name:    "dynamic sort",
			sql:     dynamicSortValue("value", true),
			maximum: 7 << 10,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := len(test.sql); got > test.maximum {
				t.Fatalf("generated SQL is %d bytes, ceiling is %d", got, test.maximum)
			}
		})
	}
}

func TestTrustedFiniteFloatOrderingKeySQLHasNarrowTrustBoundary(t *testing.T) {
	t.Parallel()

	trusted := trustedFiniteFloatOrderingKeySQL("finite_number")
	if got, maximum := len(trusted), 3<<10; got > maximum {
		t.Fatalf("trusted finite Float64 key is %d bytes, ceiling is %d", got, maximum)
	}
	if generic := exactNumericOrderingKeySQL("toString(finite_number)"); len(trusted) >= len(generic) {
		t.Fatalf(
			"trusted finite Float64 key is %d bytes, generic key is %d",
			len(trusted),
			len(generic),
		)
	}
	for _, required := range []string{
		"toString(finite_number)",
		"toInt16OrZero(",
		"trimLeft(",
		"trimRight(",
		"translate(",
		"tuple(toUInt8(1),",
	} {
		if !strings.Contains(trusted, required) {
			t.Errorf("trusted finite Float64 key is missing %q:\n%s", required, trusted)
		}
	}
	for _, forbidden := range []string{
		"isValidUTF8(",
		"match(",
		"replaceRegexp",
		"[if(__os_trusted_float_order_negative",
		"__os_exact_order_bounded",
		"__os_exact_order_exponent_eligible",
		strconv.Itoa(MaximumExactNumericOrderingTextBytes),
		strconv.Itoa(jsonnumber.MaximumExponentMagnitude),
	} {
		if strings.Contains(trusted, forbidden) {
			t.Errorf("trusted finite Float64 key contains generic guard %q:\n%s", forbidden, trusted)
		}
	}
}

func TestStatsExtremaPublicationCandidateUsesTrustedFloatKeyOnly(t *testing.T) {
	t.Parallel()

	candidate := statsExtremaPublicationCandidateSQL(
		statsExtremaPublicationCandidateInput{
			publicationValueSQL: "value",
			orderingValueSQL:    "value",
			numberSQL:           "number",
			exactTextSQL:        "exact_text",
			lexicalPublicationKindSQL: "toUInt8(" +
				strconv.Itoa(int(statsExtremaPublicationLexical)) + ")",
			eligibleSQL: "eligible",
		},
	)
	if !strings.Contains(candidate, "__os_trusted_float_order_text") {
		t.Fatalf("extrema publication candidate lacks trusted Float64 key:\n%s", candidate)
	}
	if !strings.Contains(
		candidate,
		"toString(ifNull(__os_stats_extrema_number, toFloat64(0)))",
	) {
		t.Fatalf("trusted Float64 key is not fed by the finite-or-null number binding:\n%s", candidate)
	}
	if !strings.Contains(candidate, "isValidUTF8(__os_exact_order_bounded)") {
		t.Fatalf("attacker-controlled exact-text key lost its generic guards:\n%s", candidate)
	}
	if got := strings.Count(candidate, "__os_trusted_float_order_text) ->"); got != 1 {
		t.Fatalf("trusted Float64 key definitions = %d, want 1:\n%s", got, candidate)
	}
}

func TestDynamicExtremaBytesNormalizationSeparatesEncodedAndRawRepresentations(t *testing.T) {
	t.Parallel()

	field := fieldState{
		valueSQL:       "value",
		dynamicTypeSQL: "dynamicType(value)",
		storedTypeSQL:  "stored_type",
		kind:           fieldKindDynamic,
	}
	normalized := dynamicExtremaNormalizedTupleSQL(
		field,
		compileDynamicMeasureScalar(field),
	)
	for _, required := range []string{
		`'bytes/v1'`,
		`tryBase64Decode(`,
		`modulo(length(__os_raw_base64_payload), 4) = 2`,
		`modulo(length(__os_raw_base64_payload), 4) = 3`,
		`replaceRegexpOne(base64Encode(__os_stats_extrema_dynamic_ordering), '=+$', '')`,
		`stored_type) = toUInt8(` + strconv.Itoa(int(eventfields.StoredValueTypeBytes)) + `)`,
		`CAST('' AS String)`,
	} {
		if !strings.Contains(normalized, required) {
			t.Fatalf("Dynamic Bytes extrema normalization is missing %q:\n%s", required, normalized)
		}
	}
	if strings.Contains(normalized, `tryBase64Decode(__os_stats_extrema_dynamic_lexical)`) {
		t.Fatalf("RawStd payload is decoded without padding:\n%s", normalized)
	}

	storedType := statsExtremaStoredTypeSQL("winner")
	for _, tag := range []string{"'decimal/v1'", "'bytes/v1'"} {
		if !strings.Contains(storedType, tag) {
			t.Fatalf("extrema stored type does not distinguish %s:\n%s", tag, storedType)
		}
	}
}

func TestExactNumericLiteralKeyNormalizesEquivalentSpellings(t *testing.T) {
	t.Parallel()

	for _, spellings := range [][]string{
		{"0", "0.0", "-0", "+.000", "0e100000000000000000000"},
		{"1", "1.0", "01.000e+0", ".1e1", "10e-1"},
		{"-1", "-1.0", "-01.000e+0", "-.1e1", "-10e-1"},
		{"9007199254740992.75", "900719925474099275e-2"},
	} {
		want := parseExactNumericLiteralKey(spellings[0])
		if !want.eligible {
			t.Fatalf("reference spelling %q is ineligible", spellings[0])
		}
		for _, spelling := range spellings[1:] {
			if got := parseExactNumericLiteralKey(spelling); got != want {
				t.Errorf("key(%q) = %#v, want key(%q) = %#v", spelling, got, spellings[0], want)
			}
		}
	}
}

func TestExactNumericLiteralKeyMatchesBigRatOrder(t *testing.T) {
	t.Parallel()

	spellings := []string{
		"-1e10000",
		"-9007199254740993",
		"-9007199254740992.75",
		"-1.001",
		"-1",
		"-0.100000000000000001",
		"-0",
		"0.000",
		"1e-10000",
		"0.1",
		"0.100000000000000001",
		"1",
		"1.001",
		"9007199254740992.75",
		"9007199254740993",
		"1e10000",
	}
	type keyed struct {
		text string
		key  exactNumericLiteralKey
		rat  *big.Rat
	}
	values := make([]keyed, 0, len(spellings))
	for _, spelling := range spellings {
		key := parseExactNumericLiteralKey(spelling)
		if !key.eligible {
			t.Fatalf("key(%q) is ineligible", spelling)
		}
		values = append(values, keyed{
			text: spelling,
			key:  key,
			rat:  exactNumericReferenceRat(t, spelling),
		})
	}
	for left := range values {
		for right := range values {
			keyOrder := compareExactNumericLiteralKeys(
				values[left].key,
				values[right].key,
			)
			rationalOrder := values[left].rat.Cmp(values[right].rat)
			if (keyOrder == 0) != (rationalOrder == 0) ||
				(keyOrder < 0) != (rationalOrder < 0) {
				t.Fatalf(
					"key comparison %q/%q = %d, exact comparison = %d",
					values[left].text,
					values[right].text,
					keyOrder,
					rationalOrder,
				)
			}
		}
	}
	sort.SliceStable(values, func(left, right int) bool {
		return compareExactNumericLiteralKeys(values[left].key, values[right].key) < 0
	})
	for index := 1; index < len(values); index++ {
		if values[index-1].rat.Cmp(values[index].rat) > 0 {
			t.Fatalf(
				"key ordered %q before %q, but exact values are %s > %s",
				values[index-1].text,
				values[index].text,
				values[index-1].rat.RatString(),
				values[index].rat.RatString(),
			)
		}
	}
}

func TestExactNumericLiteralKeyRejectsBoundedNonzeroWork(t *testing.T) {
	t.Parallel()

	for _, spelling := range []string{
		"1e10001",
		"1e-10001",
		strings.Repeat("9", MaximumExactNumericOrderingTextBytes+1),
		".",
		"1e",
		"NaN",
	} {
		if got := parseExactNumericLiteralKey(spelling); got.eligible {
			t.Errorf("key(%q) = %#v, want ineligible", spelling, got)
		}
	}
	if got := parseExactNumericLiteralKey("0e10001"); !got.eligible ||
		got.signClass != 1 || got.decimalOrder != 0 || got.coefficient != "" {
		t.Fatalf("oversized-exponent zero key = %#v, want normalized zero", got)
	}
}

func TestBindSQLExpressionsRejectsMismatchedBindings(t *testing.T) {
	t.Parallel()

	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("mismatched SQL bindings did not panic")
		}
	}()
	_ = bindSQLExpressions([]string{"left"}, nil, "left")
}

func TestMaximumExactNumericPredicatesStayWithinCompiledQueryCeiling(t *testing.T) {
	t.Parallel()

	for _, predicate := range []string{
		`ratio>9007199254740992.75`,
		`ratio>other_ratio`,
	} {
		t.Run(predicate, func(t *testing.T) {
			t.Parallel()
			var source strings.Builder
			source.WriteString(`index=gradethis | where `)
			for index := range 32 {
				if index > 0 {
					source.WriteString(` AND `)
				}
				source.WriteString(predicate)
			}
			compiled := compileSPL(t, source.String())
			if got := len(compiled.SQL); got > maxCompiledQueryBytes {
				t.Fatalf(
					"32 exact predicates compiled to %d bytes, ceiling is %d",
					got,
					maxCompiledQueryBytes,
				)
			}
		})
	}
}

func TestRepeatedExactNumericPredicatesComposeWithCalculatedFieldFence(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | spath input=payload output=selected path=value`+
			` | eval label=if(isnull(selected), "missing", "present") | where `+
			`label="missing" AND ratio>other_ratio AND ratio>other_ratio`,
	)
	for _, alias := range []string{
		"__os_filter_bound_",
		"__os_filter_exact_key_",
		"__os_filter_exact_numeric_",
	} {
		if !strings.Contains(compiled.SQL, alias) {
			t.Fatalf("combined predicate SQL missing %q:\n%s", alias, compiled.SQL)
		}
	}
}

func TestRepeatedExactNumericPredicateFieldsSkipConflictingForgedPaths(t *testing.T) {
	t.Parallel()

	literal := plan.Value{Kind: plan.ValueKindInt64, Int64: 1, SourceText: "1"}
	expression := &plan.BooleanExpression{
		Op: plan.BooleanOpAnd,
		Left: &plan.ComparisonExpression{
			Field: plan.FieldRef{Name: "ratio", Path: []string{"ratio"}},
			Op:    plan.ComparisonOpGreater,
			Value: literal,
		},
		Right: &plan.ComparisonExpression{
			Field: plan.FieldRef{Name: "ratio", Path: []string{"other_ratio"}},
			Op:    plan.ComparisonOpGreater,
			Value: literal,
		},
	}
	state := compileState{
		visible:      make(map[string]fieldState),
		allowDynamic: true,
	}
	if got := repeatedExactNumericPredicateFields(expression, state); len(got) != 0 {
		t.Fatalf("conflicting forged paths selected for materialization: %#v", got)
	}
}

func compareExactNumericLiteralKeys(left, right exactNumericLiteralKey) int {
	if left.signClass != right.signClass {
		return int(left.signClass) - int(right.signClass)
	}
	if left.decimalOrder < right.decimalOrder {
		return -1
	}
	if left.decimalOrder > right.decimalOrder {
		return 1
	}
	return strings.Compare(left.coefficient, right.coefficient)
}

func exactNumericReferenceRat(t *testing.T, spelling string) *big.Rat {
	t.Helper()

	sign := 1
	if strings.HasPrefix(spelling, "-") {
		sign = -1
		spelling = spelling[1:]
	} else {
		spelling = strings.TrimPrefix(spelling, "+")
	}
	exponent := 0
	if position := strings.IndexAny(spelling, "eE"); position >= 0 {
		parsed, err := strconv.Atoi(spelling[position+1:])
		if err != nil {
			t.Fatalf("parse reference exponent %q: %v", spelling, err)
		}
		exponent = parsed
		spelling = spelling[:position]
	}
	fractionDigits := 0
	if position := strings.IndexByte(spelling, '.'); position >= 0 {
		fractionDigits = len(spelling) - position - 1
		spelling = spelling[:position] + spelling[position+1:]
	}
	numerator := new(big.Int)
	if _, ok := numerator.SetString(spelling, 10); !ok {
		t.Fatalf("parse reference coefficient %q", spelling)
	}
	if sign < 0 {
		numerator.Neg(numerator)
	}
	scale := exponent - fractionDigits
	if scale >= 0 {
		numerator.Mul(numerator, new(big.Int).Exp(
			big.NewInt(10),
			big.NewInt(int64(scale)),
			nil,
		))
		return new(big.Rat).SetInt(numerator)
	}
	denominator := new(big.Int).Exp(
		big.NewInt(10),
		big.NewInt(int64(-scale)),
		nil,
	)
	return new(big.Rat).SetFrac(numerator, denominator)
}
