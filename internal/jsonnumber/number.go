// Package jsonnumber defines the bounded numeric representation shared by
// JSON ingestion and runtime lexical extraction.
package jsonnumber

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/bits"
	"strconv"
	"strings"
)

const (
	// MaximumFloat64TextBytes bounds the exact rational comparison. Longer
	// JSON numbers remain exact Decimal values without allocating attacker-
	// controlled big integers.
	MaximumFloat64TextBytes = 19
	// Nineteen bytes also keeps the pinned server's Float64 parser inside its
	// correctly-rounded significant-digit window. Longer dyadic spellings are
	// still preserved exactly as Decimal.
	// MaximumFloat64DecimalScale and MaximumFloat64Magnitude match the exact
	// fixed-decimal formatter available on the pinned ClickHouse release.
	// Values outside either publication bound remain Decimal even when binary64
	// could represent them, keeping ingestion and runtime spath typing identical.
	MaximumFloat64DecimalScale = 60
	MaximumFloat64Magnitude    = 1e60
	// MaximumExponentMagnitude bounds rational construction without expanding
	// an attacker-controlled decimal exponent.
	MaximumExponentMagnitude = 10_000
)

// Kind is the canonical scalar representation for one valid JSON number.
type Kind uint8

const (
	KindSint64 Kind = iota + 1
	KindUint64
	KindFloat64
	KindDecimal
)

// Classification contains exactly one value selected by Kind.
type Classification struct {
	Kind    Kind
	Sint64  int64
	Uint64  uint64
	Float64 float64
	Decimal string
}

// Classify maps one valid JSON-number lexeme to its bounded canonical scalar.
// Callers retain Decimal whenever exact Float64 publication cannot be proven.
func Classify(text string) Classification {
	if !strings.ContainsAny(text, ".eE") {
		if value, err := strconv.ParseInt(text, 10, 64); err == nil {
			return Classification{Kind: KindSint64, Sint64: value}
		}
		if !strings.HasPrefix(text, "-") {
			if value, err := strconv.ParseUint(text, 10, 64); err == nil {
				return Classification{Kind: KindUint64, Uint64: value}
			}
		}
		return Classification{Kind: KindDecimal, Decimal: text}
	}

	if len(text) > MaximumFloat64TextBytes {
		return decimalClassification(text)
	}
	floatValue, err := strconv.ParseFloat(text, 64)
	if err == nil && !math.IsInf(floatValue, 0) && !math.IsNaN(floatValue) &&
		(floatValue == 0 || math.Abs(floatValue) < MaximumFloat64Magnitude) &&
		exactFloat64DecimalScale(floatValue) <= MaximumFloat64DecimalScale {
		exact, exactErr := ParseDecimalRat(text)
		if exactErr == nil && exact.Cmp(new(big.Rat).SetFloat64(floatValue)) == 0 {
			return Classification{Kind: KindFloat64, Float64: floatValue}
		}
	}
	return decimalClassification(text)
}

func decimalClassification(text string) Classification {
	return Classification{Kind: KindDecimal, Decimal: NormalizeDecimalLexeme(text)}
}

func exactFloat64DecimalScale(value float64) int {
	raw := math.Float64bits(value) & ((uint64(1) << 63) - 1)
	if raw == 0 {
		return 0
	}
	exponentBits := int((raw >> 52) & 0x7ff)
	fraction := raw & ((uint64(1) << 52) - 1)
	significand := fraction
	binaryExponent := -1074
	if exponentBits != 0 {
		significand |= uint64(1) << 52
		binaryExponent = exponentBits - 1023 - 52
	}
	binaryExponent += bits.TrailingZeros64(significand)
	if binaryExponent >= 0 {
		return 0
	}
	return -binaryExponent
}

// ParseDecimalRat parses the complete JSON decimal without routing through
// Float64. Exponents are bounded before powers of ten are constructed.
func ParseDecimalRat(text string) (*big.Rat, error) {
	if !validJSONNumber(text) {
		return nil, fmt.Errorf("invalid JSON number %q", text)
	}

	mantissa := text
	exponentText := ""
	if exponentPosition := strings.IndexAny(text, "eE"); exponentPosition >= 0 {
		mantissa = text[:exponentPosition]
		exponentText = text[exponentPosition+1:]
	}
	exponent := int64(0)
	var err error
	if exponentText != "" {
		exponent, err = strconv.ParseInt(exponentText, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("decimal exponent is too large: %w", err)
		}
		if exponent < -MaximumExponentMagnitude || exponent > MaximumExponentMagnitude {
			return nil, errors.New("decimal exponent exceeds supported conversion range")
		}
	}
	negative := strings.HasPrefix(mantissa, "-")
	mantissa = strings.TrimPrefix(mantissa, "-")
	integer, fraction, hasFraction := strings.Cut(mantissa, ".")
	digits := integer
	if hasFraction {
		digits += fraction
	}
	numerator := new(big.Int)
	if _, ok := numerator.SetString(digits, 10); !ok {
		return nil, fmt.Errorf("invalid JSON number %q", text)
	}
	if negative {
		numerator.Neg(numerator)
	}
	if numerator.Sign() == 0 {
		return new(big.Rat), nil
	}
	scale := int64(len(fraction)) - exponent
	denominator := big.NewInt(1)
	if scale > 0 {
		denominator.Exp(big.NewInt(10), big.NewInt(scale), nil)
	} else if scale < 0 {
		numerator.Mul(numerator, new(big.Int).Exp(big.NewInt(10), big.NewInt(-scale), nil))
	}
	return new(big.Rat).SetFrac(numerator, denominator), nil
}

// validJSONNumber recognizes the complete JSON number grammar without regular
// expressions or numeric conversion. Callers apply their own representation
// bounds after this single linear scan.
func validJSONNumber(text string) bool {
	if text == "" {
		return false
	}

	position := 0
	if text[position] == '-' {
		position++
		if position == len(text) {
			return false
		}
	}

	switch text[position] {
	case '0':
		position++
		if position < len(text) && asciiDigit(text[position]) {
			return false
		}
	default:
		if text[position] < '1' || text[position] > '9' {
			return false
		}
		for position < len(text) && asciiDigit(text[position]) {
			position++
		}
	}

	if position < len(text) && text[position] == '.' {
		position++
		fractionStart := position
		for position < len(text) && asciiDigit(text[position]) {
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
		exponentStart := position
		for position < len(text) && asciiDigit(text[position]) {
			position++
		}
		if position == exponentStart {
			return false
		}
	}

	return position == len(text)
}

func asciiDigit(value byte) bool {
	return value >= '0' && value <= '9'
}

// NormalizeDecimalLexeme preserves coefficient spelling while canonicalizing
// exponent case, sign, and leading zeros for the decimal/v1 transport.
func NormalizeDecimalLexeme(text string) string {
	mantissa, exponent, found := strings.Cut(strings.ToLower(text), "e")
	if !found {
		return mantissa
	}
	sign := ""
	if strings.HasPrefix(exponent, "+") {
		exponent = strings.TrimPrefix(exponent, "+")
	} else if strings.HasPrefix(exponent, "-") {
		sign = "-"
		exponent = strings.TrimPrefix(exponent, "-")
	}
	exponent = strings.TrimLeft(exponent, "0")
	if exponent == "" {
		exponent, sign = "0", ""
	}
	return mantissa + "e" + sign + exponent
}
