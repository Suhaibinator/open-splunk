package clickhouse

import (
	"math/big"
	"sort"
	"strconv"
	"strings"
	"testing"

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
			sql:     statsExtremaScalarCandidateSQL("value", "number"),
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
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := len(test.sql); got > test.maximum {
				t.Fatalf("generated SQL is %d bytes, ceiling is %d", got, test.maximum)
			}
		})
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
		predicate := predicate
		t.Run(predicate, func(t *testing.T) {
			t.Parallel()
			var source strings.Builder
			source.WriteString(`index=gradethis | where `)
			for index := 0; index < 32; index++ {
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
