package spl_test

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"slices"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

const referenceMaximumNumericTextBytes = 4 << 10

var (
	errReferenceStaticType       = errors.New("statically unsupported arithmetic value type")
	errReferenceMalformedScalar  = errors.New("sanitized unsupported semantic scalar")
	errReferenceMembershipArity  = errors.New("membership candidate count is outside 1..32")
	errReferenceInvalidOperation = errors.New("invalid reference operation")
)

type referenceScalarKind uint8

const (
	referenceMissing referenceScalarKind = iota
	referenceNull
	referenceSint
	referenceUint
	referenceFloat
	referenceString
	referenceBool
	referenceBinaryScalar
	referenceObject
	referenceList
	referenceDecimal
	referenceUnsupportedTag
)

type referenceScalar struct {
	kind    referenceScalarKind
	fixed   bool
	present bool
	sint    int64
	uint    uint64
	float   float64
	text    string
	boolean bool
	invalid bool
}

type referenceTruth uint8

const (
	referenceTruthNull referenceTruth = iota
	referenceTruthFalse
	referenceTruthTrue
)

type referenceResult struct {
	value referenceScalar
	err   error
}

type referenceTrace struct {
	labels []string
}

type referenceExpression func(*referenceTrace) referenceResult

type referenceUnaryOperation uint8

const (
	referenceUnaryPlus referenceUnaryOperation = iota + 1
	referenceUnaryMinus
)

type referenceBinaryOperation uint8

const (
	referenceAdd referenceBinaryOperation = iota + 1
	referenceSubtract
	referenceMultiply
	referenceDivide
	referenceRemainder
)

type referenceComparisonOperation uint8

const (
	referenceEqual referenceComparisonOperation = iota + 1
	referenceNotEqual
	referenceLess
	referenceLessEqual
	referenceGreater
	referenceGreaterEqual
)

func TestExpressionV02ReferenceArithmeticFixtures(t *testing.T) {
	t.Parallel()

	negativeZero := math.Copysign(0, -1)
	constant := func(value referenceScalar) referenceExpression {
		return referenceConstant("", value)
	}
	binary := func(op referenceBinaryOperation, left, right referenceScalar) referenceExpression {
		return referenceBinary(op, constant(left), constant(right))
	}

	allOperators := referenceBinary(
		referenceRemainder,
		referenceBinary(
			referenceDivide,
			referenceBinary(
				referenceMultiply,
				referenceBinary(
					referenceSubtract,
					referenceBinary(referenceAdd, constant(referenceFixedSint(8)), constant(referenceFixedSint(2))),
					constant(referenceFixedSint(1)),
				),
				constant(referenceFixedSint(3)),
			),
			constant(referenceFixedSint(9)),
		),
		constant(referenceFixedSint(4)),
	)

	tests := []struct {
		name             string
		expression       referenceExpression
		wantNull         bool
		wantFloat        float64
		wantNaN          bool
		wantPositiveInf  bool
		wantNegativeZero bool
		wantError        error
	}{
		{name: "unary plus", expression: referenceUnary(referenceUnaryPlus, constant(referenceFixedSint(2))), wantFloat: 2},
		{name: "unary minus", expression: referenceUnary(referenceUnaryMinus, constant(referenceFixedSint(2))), wantFloat: -2},
		{name: "all binary operators", expression: allOperators, wantFloat: 3},
		{name: "missing becomes present null", expression: binary(referenceAdd, referenceMissingValue(), referenceFixedSint(1)), wantNull: true},
		{name: "explicit null", expression: binary(referenceAdd, referenceNullValue(), referenceFixedSint(1)), wantNull: true},
		{name: "Dynamic numeric String", expression: binary(referenceAdd, referenceDynamicString("2.5"), referenceFixedSint(1)), wantFloat: 3.5},
		{name: "fixed numeric-looking String rejected", expression: binary(referenceAdd, referenceFixedString("2.5"), referenceFixedSint(1)), wantError: errReferenceStaticType},
		{name: "String whitespace", expression: binary(referenceAdd, referenceDynamicString(" 2.5 "), referenceFixedSint(1)), wantNull: true},
		{name: "String textual nonfinite", expression: binary(referenceAdd, referenceDynamicString("NaN"), referenceFixedSint(1)), wantNull: true},
		{name: "String incomplete exponent", expression: binary(referenceAdd, referenceDynamicString("1e"), referenceFixedSint(1)), wantNull: true},
		{name: "String invalid UTF-8", expression: binary(referenceAdd, referenceDynamicString(string([]byte{0xff, '1'})), referenceFixedSint(1)), wantNull: true},
		{name: "String over byte bound", expression: binary(referenceAdd, referenceDynamicString(strings.Repeat("0", referenceMaximumNumericTextBytes+1)), referenceFixedSint(1)), wantNull: true},
		{name: "String underflow converts to zero", expression: binary(referenceAdd, referenceDynamicString("1e-400"), referenceFixedSint(1)), wantFloat: 1},
		{name: "String overflow is null", expression: binary(referenceAdd, referenceDynamicString("1e400"), referenceFixedSint(1)), wantNull: true},
		{name: "integer precision boundary", expression: binary(referenceAdd, referenceDynamicUint(9_007_199_254_740_993), referenceFixedSint(0)), wantFloat: 9_007_199_254_740_992},
		{name: "valid decimal", expression: binary(referenceAdd, referenceDecimalValue("-123.5"), referenceFixedFloat(0.5)), wantFloat: -123},
		{name: "decimal underflow", expression: binary(referenceAdd, referenceDecimalValue("1e-400"), referenceFixedSint(1)), wantFloat: 1},
		{name: "malformed decimal", expression: binary(referenceAdd, referenceMalformedDecimal("malformed-secret-1e"), referenceFixedSint(1)), wantError: errReferenceMalformedScalar},
		{name: "oversized decimal", expression: binary(referenceAdd, referenceDecimalValue(strings.Repeat("1", referenceMaximumNumericTextBytes+1)), referenceFixedSint(1)), wantError: errReferenceMalformedScalar},
		{name: "Dynamic Bool becomes null", expression: binary(referenceAdd, referenceDynamicTrue(), referenceFixedSint(1)), wantNull: true},
		{name: "fixed Bool rejected", expression: binary(referenceAdd, referenceFixedBool(true), referenceFixedSint(1)), wantError: errReferenceStaticType},
		{name: "positive zero division", expression: binary(referenceDivide, referenceFixedSint(1), referenceFixedFloat(0)), wantNull: true},
		{name: "negative zero division", expression: binary(referenceDivide, referenceFixedSint(1), referenceFixedFloat(negativeZero)), wantNull: true},
		{name: "zero remainder", expression: binary(referenceRemainder, referenceFixedSint(1), referenceFixedSint(0)), wantNull: true},
		{name: "negative remainder", expression: binary(referenceRemainder, referenceFixedSint(-5), referenceFixedSint(2)), wantFloat: -1},
		{name: "negative divisor remainder", expression: binary(referenceRemainder, referenceFixedSint(5), referenceFixedSint(-2)), wantFloat: 1},
		{name: "both negative remainder", expression: binary(referenceRemainder, referenceFixedSint(-5), referenceFixedSint(-2)), wantFloat: -1},
		{name: "finite remainder infinity", expression: binary(referenceRemainder, referenceFixedSint(5), referenceFixedFloat(math.Inf(1))), wantFloat: 5},
		{name: "infinity remainder finite", expression: binary(referenceRemainder, referenceFixedFloat(math.Inf(1)), referenceFixedSint(5)), wantNaN: true},
		{name: "negative zero normalized by addition", expression: binary(referenceAdd, referenceFixedFloat(negativeZero), referenceFixedSint(0)), wantFloat: 0},
		{name: "division retains negative zero", expression: binary(referenceDivide, referenceFixedSint(0), referenceFixedSint(-2)), wantFloat: 0, wantNegativeZero: true},
		{name: "finite overflow", expression: binary(referenceMultiply, referenceFixedFloat(1e308), referenceFixedFloat(1e308)), wantPositiveInf: true},
		{name: "invalid IEEE operation", expression: binary(referenceSubtract, referenceFixedFloat(math.Inf(1)), referenceFixedFloat(math.Inf(1))), wantNaN: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			result := test.expression(&referenceTrace{})
			if test.wantError != nil {
				if !errors.Is(result.err, test.wantError) {
					t.Fatalf("error = %v, want %v", result.err, test.wantError)
				}
				return
			}
			if result.err != nil {
				t.Fatalf("evaluate: %v", result.err)
			}
			if test.wantNull {
				if result.value.kind != referenceNull || !result.value.present {
					t.Fatalf("result = %#v, want present null", result.value)
				}
				return
			}
			if result.value.kind != referenceFloat || !result.value.present {
				t.Fatalf("result = %#v, want present Float64", result.value)
			}
			if test.wantNaN {
				if !math.IsNaN(result.value.float) {
					t.Fatalf("result = %v, want NaN", result.value.float)
				}
				return
			}
			if test.wantPositiveInf {
				if !math.IsInf(result.value.float, 1) {
					t.Fatalf("result = %v, want +Inf", result.value.float)
				}
				return
			}
			negativeZero := result.value.float == 0 && math.Signbit(result.value.float)
			if result.value.float != test.wantFloat || negativeZero != test.wantNegativeZero {
				t.Fatalf(
					"result = %v (negative-zero=%v), want %v (negative-zero=%v)",
					result.value.float,
					negativeZero,
					test.wantFloat,
					test.wantNegativeZero,
				)
			}
		})
	}
}

func TestExpressionV02ReferenceMembershipFixtures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		value      referenceScalar
		candidates []referenceScalar
		negated    bool
		want       referenceTruth
		wantError  error
	}{
		{name: "String case-sensitive", value: referenceDynamicString("Prod"), candidates: []referenceScalar{referenceFixedString("prod")}, want: referenceTruthFalse},
		{name: "wildcard is literal", value: referenceDynamicString("500"), candidates: []referenceScalar{referenceFixedString("5*")}, want: referenceTruthFalse},
		{name: "earlier match before null", value: referenceDynamicSint(1), candidates: []referenceScalar{referenceFixedSint(1), referenceNullValue()}, want: referenceTruthTrue},
		{name: "no match plus null", value: referenceDynamicSint(2), candidates: []referenceScalar{referenceFixedSint(1), referenceNullValue()}, want: referenceTruthNull},
		{name: "null input", value: referenceNullValue(), candidates: []referenceScalar{referenceFixedSint(1), referenceFixedSint(2)}, want: referenceTruthNull},
		{name: "NOT true", value: referenceDynamicSint(1), candidates: []referenceScalar{referenceFixedSint(1), referenceFixedSint(2)}, negated: true, want: referenceTruthFalse},
		{name: "NOT null", value: referenceDynamicSint(2), candidates: []referenceScalar{referenceFixedSint(1), referenceNullValue()}, negated: true, want: referenceTruthNull},
		{name: "NaN never matches", value: referenceDynamicFloat(math.NaN()), candidates: []referenceScalar{referenceDynamicFloat(math.NaN())}, want: referenceTruthFalse},
		{name: "signed and unsigned exact", value: referenceDynamicSint(42), candidates: []referenceScalar{referenceDynamicUint(42)}, want: referenceTruthTrue},
		{name: "numeric String against number", value: referenceDynamicSint(25), candidates: []referenceScalar{referenceDynamicString("25.0")}, want: referenceTruthTrue},
		{name: "decimal against number", value: referenceDynamicUint(9_007_199_254_740_993), candidates: []referenceScalar{referenceDecimalValue("9007199254740993.0")}, want: referenceTruthTrue},
		{name: "fixed Float comparison is native", value: referenceFixedFloat(9_007_199_254_740_992), candidates: []referenceScalar{referenceDynamicUint(9_007_199_254_740_993)}, want: referenceTruthTrue},
		{name: "Dynamic Float uses exact published key", value: referenceDynamicFloat(9_007_199_254_740_992), candidates: []referenceScalar{referenceDynamicUint(9_007_199_254_740_993)}, want: referenceTruthFalse},
		{name: "Bool equality", value: referenceDynamicTrue(), candidates: []referenceScalar{referenceDynamicTrue()}, want: referenceTruthTrue},
		{name: "Bool and number are incomparable", value: referenceDynamicTrue(), candidates: []referenceScalar{referenceFixedSint(1)}, want: referenceTruthNull},
		{name: "object comparison is null", value: referenceDynamicObject(), candidates: []referenceScalar{referenceDynamicObject()}, want: referenceTruthNull},
		{name: "later malformed candidate fails", value: referenceDynamicSint(1), candidates: []referenceScalar{referenceFixedSint(1), referenceMalformedDecimal("secret-1e")}, wantError: errReferenceMalformedScalar},
		{name: "zero candidates", value: referenceDynamicSint(1), wantError: errReferenceMembershipArity},
		{name: "thirty-three candidates", value: referenceDynamicSint(1), candidates: make([]referenceScalar, 33), wantError: errReferenceMembershipArity},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			candidates := make([]referenceExpression, len(test.candidates))
			for index, candidate := range test.candidates {
				candidates[index] = referenceConstant(fmt.Sprintf("candidate-%d", index), candidate)
			}
			truth, err := referenceMembership(
				referenceConstant("value", test.value),
				candidates,
				test.negated,
				&referenceTrace{},
			)
			if test.wantError != nil {
				if !errors.Is(err, test.wantError) {
					t.Fatalf("error = %v, want %v", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("evaluate membership: %v", err)
			}
			if truth != test.want {
				t.Fatalf("membership = %v, want %v", truth, test.want)
			}
		})
	}
}

func TestExpressionV02ReferenceNaNComparisons(t *testing.T) {
	t.Parallel()

	nan := referenceDynamicFloat(math.NaN())
	tests := []struct {
		name string
		op   referenceComparisonOperation
		want referenceTruth
	}{
		{name: "equality false", op: referenceEqual, want: referenceTruthFalse},
		{name: "inequality true", op: referenceNotEqual, want: referenceTruthTrue},
		{name: "less false", op: referenceLess, want: referenceTruthFalse},
		{name: "less equal false", op: referenceLessEqual, want: referenceTruthFalse},
		{name: "greater false", op: referenceGreater, want: referenceTruthFalse},
		{name: "greater equal false", op: referenceGreaterEqual, want: referenceTruthFalse},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := referenceCompare(nan, nan, test.op)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("comparison = %v, want %v", got, test.want)
			}
		})
	}
}

func TestExpressionV02ReferenceEvaluationOrderAndOccurrence(t *testing.T) {
	t.Parallel()

	trace := &referenceTrace{}
	expression := referenceBinary(
		referenceMultiply,
		referenceBinary(
			referenceAdd,
			referenceConstant("left", referenceDynamicSint(2)),
			referenceConstant("middle", referenceDynamicSint(3)),
		),
		referenceConstant("right", referenceDynamicSint(4)),
	)
	result := expression(trace)
	if result.err != nil || result.value.kind != referenceFloat || result.value.float != 20 {
		t.Fatalf("arithmetic result = %#v error=%v", result.value, result.err)
	}
	if !slices.Equal(trace.labels, []string{"left", "middle", "right"}) {
		t.Fatalf("arithmetic evaluation order = %v", trace.labels)
	}

	trace = &referenceTrace{}
	truth, err := referenceMembership(
		referenceConstant("value", referenceDynamicSint(1)),
		[]referenceExpression{
			referenceConstant("match", referenceFixedSint(1)),
			referenceConstant("null", referenceNullValue()),
			referenceConstant("malformed", referenceMalformedDecimal("secret-1e")),
		},
		false,
		trace,
	)
	if !errors.Is(err, errReferenceMalformedScalar) || truth != referenceTruthNull {
		t.Fatalf("membership = %v error=%v, want sanitized failure", truth, err)
	}
	if !slices.Equal(trace.labels, []string{"value", "match", "null", "malformed"}) {
		t.Fatalf("membership evaluation order = %v", trace.labels)
	}
}

func TestExpressionV02ReferenceArithmeticRandomized(t *testing.T) {
	t.Parallel()

	const (
		seed       = int64(0x502a11)
		iterations = 20_000
	)
	random := rand.New(rand.NewSource(seed)) // #nosec G404 -- deterministic test data, not security randomness.
	operations := []referenceBinaryOperation{
		referenceAdd, referenceSubtract, referenceMultiply, referenceDivide, referenceRemainder,
	}
	for iteration := range iterations {
		leftInteger := int64(random.Intn(2_000_001) - 1_000_000)
		rightInteger := int64(random.Intn(2_000_001) - 1_000_000)
		operation := operations[random.Intn(len(operations))]
		left := randomReferenceNumericEncoding(random, leftInteger)
		right := randomReferenceNumericEncoding(random, rightInteger)

		result := referenceBinary(
			operation,
			referenceConstant("left", left),
			referenceConstant("right", right),
		)(&referenceTrace{})
		if result.err != nil {
			t.Fatalf("seed=%d iteration=%d: %v", seed, iteration, result.err)
		}
		if (operation == referenceDivide || operation == referenceRemainder) && rightInteger == 0 {
			if result.value.kind != referenceNull || !result.value.present {
				t.Fatalf("seed=%d iteration=%d: zero divisor result = %#v", seed, iteration, result.value)
			}
			continue
		}

		leftFloat, rightFloat := float64(leftInteger), float64(rightInteger)
		var want float64
		switch operation {
		case referenceAdd:
			want = leftFloat + rightFloat
		case referenceSubtract:
			want = leftFloat - rightFloat
		case referenceMultiply:
			want = leftFloat * rightFloat
		case referenceDivide:
			want = leftFloat / rightFloat
		case referenceRemainder:
			want = math.Mod(leftFloat, rightFloat)
		}
		if operation != referenceDivide && want == 0 {
			want = 0
		}
		if result.value.kind != referenceFloat ||
			math.Float64bits(result.value.float) != math.Float64bits(want) {
			t.Fatalf(
				"seed=%d iteration=%d: %d op=%d %d => %v (%x), want %v (%x)",
				seed,
				iteration,
				leftInteger,
				operation,
				rightInteger,
				result.value.float,
				math.Float64bits(result.value.float),
				want,
				math.Float64bits(want),
			)
		}
	}
}

func TestExpressionV02ReferenceMembershipRandomized(t *testing.T) {
	t.Parallel()

	const (
		seed       = int64(0x5021a5)
		iterations = 20_000
	)
	random := rand.New(rand.NewSource(seed)) // #nosec G404 -- deterministic test data, not security randomness.
	for iteration := range iterations {
		target := int64(random.Intn(2_001) - 1_000)
		candidateCount := random.Intn(32) + 1
		negated := random.Intn(2) == 0
		candidates := make([]referenceExpression, 0, candidateCount)
		matched, sawNull := false, false
		for candidateIndex := range candidateCount {
			candidateValue := int64(random.Intn(2_001) - 1_000)
			var candidate referenceScalar
			switch random.Intn(7) {
			case 0:
				candidate = referenceDynamicSint(candidateValue)
				matched = matched || candidateValue == target
			case 1:
				if candidateValue >= 0 {
					candidate = referenceDynamicUint(uint64(candidateValue))
					matched = matched || candidateValue == target
				} else {
					candidate = referenceDynamicObject()
					sawNull = true
				}
			case 2:
				candidate = referenceDecimalValue(strconv.FormatInt(candidateValue, 10) + ".0")
				matched = matched || candidateValue == target
			case 3:
				candidate = referenceDynamicString(strconv.FormatInt(candidateValue, 10) + "e0")
				matched = matched || candidateValue == target
			case 4:
				candidate = referenceNullValue()
				sawNull = true
			case 5:
				candidate = referenceDynamicObject()
				sawNull = true
			default:
				candidate = referenceDynamicString("not-numeric")
				sawNull = true
			}
			candidates = append(
				candidates,
				referenceConstant(fmt.Sprintf("candidate-%d", candidateIndex), candidate),
			)
		}

		want := referenceTruthFalse
		if matched {
			want = referenceTruthTrue
		} else if sawNull {
			want = referenceTruthNull
		}
		if negated {
			want = referenceNot(want)
		}
		got, err := referenceMembership(
			referenceConstant("value", referenceDynamicSint(target)),
			candidates,
			negated,
			&referenceTrace{},
		)
		if err != nil {
			t.Fatalf("seed=%d iteration=%d: %v", seed, iteration, err)
		}
		if got != want {
			t.Fatalf(
				"seed=%d iteration=%d target=%d negated=%v: membership=%v, want %v",
				seed,
				iteration,
				target,
				negated,
				got,
				want,
			)
		}
	}
}

func referenceConstant(label string, value referenceScalar) referenceExpression {
	return func(trace *referenceTrace) referenceResult {
		if trace != nil && label != "" {
			trace.labels = append(trace.labels, label)
		}
		return referenceResult{value: value}
	}
}

func referenceUnary(op referenceUnaryOperation, operand referenceExpression) referenceExpression {
	return func(trace *referenceTrace) referenceResult {
		result := operand(trace)
		if result.err != nil {
			return result
		}
		number, eligible, err := referenceArithmeticNumber(result.value)
		if err != nil {
			return referenceResult{err: err}
		}
		if !eligible {
			return referenceResult{value: referenceNullValue()}
		}
		switch op {
		case referenceUnaryPlus:
			return referenceResult{value: referenceFloatValue(number)}
		case referenceUnaryMinus:
			return referenceResult{value: referenceFloatValue(referenceNormalizeZero(-number))}
		default:
			return referenceResult{err: errReferenceInvalidOperation}
		}
	}
}

func referenceBinary(
	op referenceBinaryOperation,
	leftExpression, rightExpression referenceExpression,
) referenceExpression {
	return func(trace *referenceTrace) referenceResult {
		leftResult := leftExpression(trace)
		if leftResult.err != nil {
			return leftResult
		}
		rightResult := rightExpression(trace)
		if rightResult.err != nil {
			return rightResult
		}
		left, leftEligible, err := referenceArithmeticNumber(leftResult.value)
		if err != nil {
			return referenceResult{err: err}
		}
		right, rightEligible, err := referenceArithmeticNumber(rightResult.value)
		if err != nil {
			return referenceResult{err: err}
		}
		if !leftEligible || !rightEligible {
			return referenceResult{value: referenceNullValue()}
		}

		var value float64
		switch op {
		case referenceAdd:
			value = referenceNormalizeZero(left + right)
		case referenceSubtract:
			value = referenceNormalizeZero(left - right)
		case referenceMultiply:
			value = referenceNormalizeZero(left * right)
		case referenceDivide:
			if right == 0 {
				return referenceResult{value: referenceNullValue()}
			}
			value = left / right
		case referenceRemainder:
			if right == 0 {
				return referenceResult{value: referenceNullValue()}
			}
			value = referenceNormalizeZero(math.Mod(left, right))
		default:
			return referenceResult{err: errReferenceInvalidOperation}
		}
		return referenceResult{value: referenceFloatValue(value)}
	}
}

func referenceArithmeticNumber(value referenceScalar) (float64, bool, error) {
	if value.kind == referenceDecimal && (value.invalid || !referenceValidDecimalPayload(value.text)) {
		return 0, false, errReferenceMalformedScalar
	}
	if value.kind == referenceDecimal && len(value.text) > referenceMaximumNumericTextBytes {
		return 0, false, errReferenceMalformedScalar
	}
	switch value.kind {
	case referenceMissing, referenceNull:
		return 0, false, nil
	case referenceSint:
		return float64(value.sint), true, nil
	case referenceUint:
		return float64(value.uint), true, nil
	case referenceFloat:
		return value.float, true, nil
	case referenceString:
		if value.fixed {
			return 0, false, errReferenceStaticType
		}
		return referenceParseFloat(value.text, false)
	case referenceDecimal:
		return referenceParseFloat(value.text, true)
	case referenceBool, referenceBinaryScalar, referenceObject, referenceList, referenceUnsupportedTag:
		if value.fixed {
			return 0, false, errReferenceStaticType
		}
		return 0, false, nil
	default:
		return 0, false, errReferenceInvalidOperation
	}
}

func referenceParseFloat(text string, semanticDecimal bool) (float64, bool, error) {
	if len(text) > referenceMaximumNumericTextBytes || !utf8.ValidString(text) {
		if semanticDecimal {
			return 0, false, errReferenceMalformedScalar
		}
		return 0, false, nil
	}
	if semanticDecimal {
		if !referenceValidDecimalPayload(text) {
			return 0, false, errReferenceMalformedScalar
		}
	} else if !referenceValidNumericString(text) {
		return 0, false, nil
	}
	value, err := strconv.ParseFloat(text, 64)
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, false, nil
	}
	if err != nil && value != 0 {
		return 0, false, nil
	}
	return value, true, nil
}

func referenceMembership(
	valueExpression referenceExpression,
	candidateExpressions []referenceExpression,
	negated bool,
	trace *referenceTrace,
) (referenceTruth, error) {
	if len(candidateExpressions) == 0 || len(candidateExpressions) > 32 {
		return referenceTruthNull, errReferenceMembershipArity
	}
	valueResult := valueExpression(trace)
	if valueResult.err != nil {
		return referenceTruthNull, valueResult.err
	}
	matched, sawNull := false, false
	for _, candidateExpression := range candidateExpressions {
		candidateResult := candidateExpression(trace)
		if candidateResult.err != nil {
			return referenceTruthNull, candidateResult.err
		}
		comparison, err := referenceCompare(valueResult.value, candidateResult.value, referenceEqual)
		if err != nil {
			return referenceTruthNull, err
		}
		switch comparison {
		case referenceTruthTrue:
			matched = true
		case referenceTruthNull:
			sawNull = true
		}
	}
	truth := referenceTruthFalse
	if matched {
		truth = referenceTruthTrue
	} else if sawNull {
		truth = referenceTruthNull
	}
	if negated {
		truth = referenceNot(truth)
	}
	return truth, nil
}

func referenceCompare(
	left, right referenceScalar,
	op referenceComparisonOperation,
) (referenceTruth, error) {
	if err := referenceValidateSemanticScalar(left); err != nil {
		return referenceTruthNull, err
	}
	if err := referenceValidateSemanticScalar(right); err != nil {
		return referenceTruthNull, err
	}
	if left.kind == referenceMissing || left.kind == referenceNull ||
		right.kind == referenceMissing || right.kind == referenceNull {
		return referenceTruthNull, nil
	}
	if left.kind == referenceString && right.kind == referenceString {
		return referenceApplyOrdering(strings.Compare(left.text, right.text), op), nil
	}
	if left.kind == referenceBool || right.kind == referenceBool {
		if left.kind != referenceBool || right.kind != referenceBool ||
			(op != referenceEqual && op != referenceNotEqual) {
			return referenceTruthNull, nil
		}
		equal := left.boolean == right.boolean
		if op == referenceNotEqual {
			equal = !equal
		}
		return referenceTruthFromBool(equal), nil
	}
	if referenceIncomparableKind(left.kind) || referenceIncomparableKind(right.kind) {
		return referenceTruthNull, nil
	}

	leftNumber, leftOK, err := referenceComparisonNumber(left)
	if err != nil {
		return referenceTruthNull, err
	}
	rightNumber, rightOK, err := referenceComparisonNumber(right)
	if err != nil {
		return referenceTruthNull, err
	}
	if !leftOK || !rightOK {
		return referenceTruthNull, nil
	}
	if math.IsNaN(leftNumber.float) || math.IsNaN(rightNumber.float) {
		switch op {
		case referenceNotEqual:
			return referenceTruthTrue, nil
		case referenceEqual, referenceLess, referenceLessEqual, referenceGreater, referenceGreaterEqual:
			return referenceTruthFalse, nil
		default:
			return referenceTruthNull, errReferenceInvalidOperation
		}
	}
	if leftNumber.nativeFloat || rightNumber.nativeFloat || leftNumber.nonFinite || rightNumber.nonFinite {
		leftFloat, leftValid := leftNumber.asFloat()
		rightFloat, rightValid := rightNumber.asFloat()
		if !leftValid || !rightValid {
			return referenceTruthNull, nil
		}
		return referenceApplyFloatComparison(leftFloat, rightFloat, op), nil
	}
	return referenceApplyOrdering(leftNumber.rat.Cmp(rightNumber.rat), op), nil
}

type referenceNumber struct {
	float       float64
	rat         *big.Rat
	nativeFloat bool
	nonFinite   bool
}

func (number referenceNumber) asFloat() (float64, bool) {
	if number.nativeFloat || number.nonFinite {
		return number.float, true
	}
	value, _ := number.rat.Float64()
	if math.IsInf(value, 0) {
		return 0, false
	}
	return value, true
}

func referenceComparisonNumber(value referenceScalar) (referenceNumber, bool, error) {
	switch value.kind {
	case referenceSint:
		return referenceNumber{rat: new(big.Rat).SetInt64(value.sint)}, true, nil
	case referenceUint:
		integer := new(big.Int).SetUint64(value.uint)
		return referenceNumber{rat: new(big.Rat).SetInt(integer)}, true, nil
	case referenceFloat:
		if math.IsNaN(value.float) || math.IsInf(value.float, 0) {
			return referenceNumber{float: value.float, nativeFloat: value.fixed, nonFinite: true}, true, nil
		}
		if value.fixed {
			return referenceNumber{float: value.float, nativeFloat: true}, true, nil
		}
		text := strconv.FormatFloat(value.float, 'g', -1, 64)
		rat, ok := referenceNumericRat(text)
		return referenceNumber{float: value.float, rat: rat}, ok, nil
	case referenceDecimal:
		if value.invalid || !referenceValidDecimalPayload(value.text) ||
			len(value.text) > referenceMaximumNumericTextBytes {
			return referenceNumber{}, false, errReferenceMalformedScalar
		}
		rat, ok := referenceNumericRat(value.text)
		return referenceNumber{rat: rat}, ok, nil
	case referenceString:
		if value.fixed || !referenceValidNumericString(value.text) ||
			len(value.text) > referenceMaximumNumericTextBytes || !utf8.ValidString(value.text) {
			return referenceNumber{}, false, nil
		}
		rat, ok := referenceNumericRat(value.text)
		return referenceNumber{rat: rat}, ok, nil
	default:
		return referenceNumber{}, false, nil
	}
}

func referenceValidateSemanticScalar(value referenceScalar) error {
	if value.kind == referenceDecimal &&
		(value.invalid || len(value.text) > referenceMaximumNumericTextBytes ||
			!utf8.ValidString(value.text) || !referenceValidDecimalPayload(value.text)) {
		return errReferenceMalformedScalar
	}
	return nil
}

func referenceIncomparableKind(kind referenceScalarKind) bool {
	switch kind {
	case referenceBinaryScalar, referenceObject, referenceList, referenceUnsupportedTag:
		return true
	default:
		return false
	}
}

func referenceApplyFloatComparison(left, right float64, op referenceComparisonOperation) referenceTruth {
	switch op {
	case referenceEqual:
		return referenceTruthFromBool(left == right)
	case referenceNotEqual:
		return referenceTruthFromBool(left != right)
	case referenceLess:
		return referenceTruthFromBool(left < right)
	case referenceLessEqual:
		return referenceTruthFromBool(left <= right)
	case referenceGreater:
		return referenceTruthFromBool(left > right)
	case referenceGreaterEqual:
		return referenceTruthFromBool(left >= right)
	default:
		return referenceTruthNull
	}
}

func referenceApplyOrdering(order int, op referenceComparisonOperation) referenceTruth {
	switch op {
	case referenceEqual:
		return referenceTruthFromBool(order == 0)
	case referenceNotEqual:
		return referenceTruthFromBool(order != 0)
	case referenceLess:
		return referenceTruthFromBool(order < 0)
	case referenceLessEqual:
		return referenceTruthFromBool(order <= 0)
	case referenceGreater:
		return referenceTruthFromBool(order > 0)
	case referenceGreaterEqual:
		return referenceTruthFromBool(order >= 0)
	default:
		return referenceTruthNull
	}
}

func referenceTruthFromBool(value bool) referenceTruth {
	if value {
		return referenceTruthTrue
	}
	return referenceTruthFalse
}

func referenceNot(value referenceTruth) referenceTruth {
	switch value {
	case referenceTruthTrue:
		return referenceTruthFalse
	case referenceTruthFalse:
		return referenceTruthTrue
	default:
		return referenceTruthNull
	}
}

func referenceNormalizeZero(value float64) float64 {
	if value == 0 {
		return 0
	}
	return value
}

func referenceValidNumericString(text string) bool {
	if text == "" || len(text) > referenceMaximumNumericTextBytes || !utf8.ValidString(text) {
		return false
	}
	position := 0
	if text[position] == '+' || text[position] == '-' {
		position++
		if position == len(text) {
			return false
		}
	}
	digits := 0
	for position < len(text) && referenceASCIIDigit(text[position]) {
		position++
		digits++
	}
	if position < len(text) && text[position] == '.' {
		position++
		for position < len(text) && referenceASCIIDigit(text[position]) {
			position++
			digits++
		}
	}
	if digits == 0 {
		return false
	}
	if position < len(text) && (text[position] == 'e' || text[position] == 'E') {
		position++
		if position < len(text) && (text[position] == '+' || text[position] == '-') {
			position++
		}
		exponentStart := position
		for position < len(text) && referenceASCIIDigit(text[position]) {
			position++
		}
		if position == exponentStart {
			return false
		}
	}
	return position == len(text)
}

func referenceValidDecimalPayload(text string) bool {
	if text == "" || len(text) > referenceMaximumNumericTextBytes || !utf8.ValidString(text) {
		return false
	}
	position := 0
	if text[position] == '-' {
		position++
		if position == len(text) {
			return false
		}
	}
	if text[position] == '0' {
		position++
		if position < len(text) && referenceASCIIDigit(text[position]) {
			return false
		}
	} else {
		if text[position] < '1' || text[position] > '9' {
			return false
		}
		for position < len(text) && referenceASCIIDigit(text[position]) {
			position++
		}
	}
	if position < len(text) && text[position] == '.' {
		position++
		fractionStart := position
		for position < len(text) && referenceASCIIDigit(text[position]) {
			position++
		}
		if position == fractionStart {
			return false
		}
	}
	if position < len(text) && (text[position] == 'e' || text[position] == 'E') {
		position++
		if position < len(text) && (text[position] == '+' || text[position] == '-') {
			position++
		}
		if position == len(text) {
			return false
		}
		if text[position] == '0' {
			position++
			if position < len(text) && referenceASCIIDigit(text[position]) {
				return false
			}
		} else {
			if text[position] < '1' || text[position] > '9' {
				return false
			}
			for position < len(text) && referenceASCIIDigit(text[position]) {
				position++
			}
		}
	}
	return position == len(text)
}

func referenceNumericRat(text string) (*big.Rat, bool) {
	if !referenceValidNumericString(text) {
		return nil, false
	}
	negative := strings.HasPrefix(text, "-")
	text = strings.TrimPrefix(strings.TrimPrefix(text, "+"), "-")
	mantissa, exponentText := text, ""
	if exponentPosition := strings.IndexAny(text, "eE"); exponentPosition >= 0 {
		mantissa, exponentText = text[:exponentPosition], text[exponentPosition+1:]
	}
	integer, fraction, hasFraction := strings.Cut(mantissa, ".")
	if integer == "" {
		integer = "0"
	}
	digits := integer
	if hasFraction {
		digits += fraction
	}
	coefficient := new(big.Int)
	if _, ok := coefficient.SetString(digits, 10); !ok {
		return nil, false
	}
	if coefficient.Sign() == 0 {
		return new(big.Rat), true
	}
	if negative {
		coefficient.Neg(coefficient)
	}
	exponent := int64(0)
	if exponentText != "" {
		parsed, err := strconv.ParseInt(exponentText, 10, 32)
		if err != nil || parsed < -10_000 || parsed > 10_000 {
			return nil, false
		}
		exponent = parsed
	}
	scale := int64(len(fraction)) - exponent
	denominator := big.NewInt(1)
	if scale > 0 {
		denominator.Exp(big.NewInt(10), big.NewInt(scale), nil)
	} else if scale < 0 {
		coefficient.Mul(coefficient, new(big.Int).Exp(big.NewInt(10), big.NewInt(-scale), nil))
	}
	return new(big.Rat).SetFrac(coefficient, denominator), true
}

func referenceASCIIDigit(value byte) bool {
	return value >= '0' && value <= '9'
}

func randomReferenceNumericEncoding(random *rand.Rand, value int64) referenceScalar {
	switch random.Intn(5) {
	case 0:
		return referenceDynamicSint(value)
	case 1:
		if value >= 0 {
			return referenceDynamicUint(uint64(value))
		}
		return referenceDynamicSint(value)
	case 2:
		return referenceDynamicFloat(float64(value))
	case 3:
		return referenceDynamicString(strconv.FormatInt(value, 10) + ".0")
	default:
		return referenceDecimalValue(strconv.FormatInt(value, 10) + ".0")
	}
}

func referenceMissingValue() referenceScalar {
	return referenceScalar{kind: referenceMissing}
}

func referenceNullValue() referenceScalar {
	return referenceScalar{kind: referenceNull, present: true}
}

func referenceFixedSint(value int64) referenceScalar {
	return referenceScalar{kind: referenceSint, fixed: true, present: true, sint: value}
}

func referenceDynamicSint(value int64) referenceScalar {
	return referenceScalar{kind: referenceSint, present: true, sint: value}
}

func referenceDynamicUint(value uint64) referenceScalar {
	return referenceScalar{kind: referenceUint, present: true, uint: value}
}

func referenceFixedFloat(value float64) referenceScalar {
	return referenceScalar{kind: referenceFloat, fixed: true, present: true, float: value}
}

func referenceDynamicFloat(value float64) referenceScalar {
	return referenceScalar{kind: referenceFloat, present: true, float: value}
}

func referenceFloatValue(value float64) referenceScalar {
	return referenceScalar{kind: referenceFloat, fixed: true, present: true, float: value}
}

func referenceFixedString(value string) referenceScalar {
	return referenceScalar{kind: referenceString, fixed: true, present: true, text: value}
}

func referenceDynamicString(value string) referenceScalar {
	return referenceScalar{kind: referenceString, present: true, text: value}
}

func referenceFixedBool(value bool) referenceScalar {
	return referenceScalar{kind: referenceBool, fixed: true, present: true, boolean: value}
}

func referenceDynamicTrue() referenceScalar {
	return referenceScalar{kind: referenceBool, present: true, boolean: true}
}

func referenceDynamicObject() referenceScalar {
	return referenceScalar{kind: referenceObject, present: true}
}

func referenceDecimalValue(value string) referenceScalar {
	return referenceScalar{kind: referenceDecimal, present: true, text: value}
}

func referenceMalformedDecimal(value string) referenceScalar {
	return referenceScalar{kind: referenceDecimal, present: true, text: value, invalid: true}
}
