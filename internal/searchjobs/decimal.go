package searchjobs

import (
	"errors"
	"math/big"
	"strings"
)

// CanonicalDecimal returns the deterministic transport and comparison spelling
// for one validated decimal. Numerically equivalent coefficient, scale, and
// exponent spellings collapse to one value without rounding through float64.
// The result uses plain notation for ordinary magnitudes and normalized
// scientific notation outside that compact range.
func CanonicalDecimal(source string) (string, error) {
	lower := strings.ToLower(source)
	mantissa, exponent, hasExponent := strings.Cut(lower, "e")
	negative := false
	if mantissa != "" {
		switch mantissa[0] {
		case '-':
			negative = true
			mantissa = mantissa[1:]
		case '+':
			mantissa = mantissa[1:]
		}
	}
	integer, fraction, hasFraction := strings.Cut(mantissa, ".")
	if integer == "" || !decimalDigits(integer) || (hasFraction && (fraction == "" || !decimalDigits(fraction))) {
		return "", errors.New("search result decimal is invalid")
	}
	explicitExponent := new(big.Int)
	if hasExponent {
		exponentNegative := false
		if exponent != "" {
			switch exponent[0] {
			case '-':
				exponentNegative = true
				exponent = exponent[1:]
			case '+':
				exponent = exponent[1:]
			}
		}
		if exponent == "" || !decimalDigits(exponent) {
			return "", errors.New("search result decimal is invalid")
		}
		exponent = strings.TrimLeft(exponent, "0")
		if exponent == "" {
			exponent = "0"
		}
		if _, ok := explicitExponent.SetString(exponent, 10); !ok {
			return "", errors.New("search result decimal is invalid")
		}
		if exponentNegative {
			explicitExponent.Neg(explicitExponent)
		}
	}

	digits := integer + fraction
	firstNonzero := strings.IndexFunc(digits, func(character rune) bool { return character != '0' })
	if firstNonzero < 0 {
		return "0", nil
	}
	lastNonzero := len(digits) - 1
	for digits[lastNonzero] == '0' {
		lastNonzero--
	}
	coefficient := digits[firstNonzero : lastNonzero+1]
	scientificExponent := new(big.Int).Set(explicitExponent)
	scientificExponent.Add(scientificExponent, big.NewInt(int64(len(integer)-firstNonzero-1)))

	sign := ""
	if negative {
		sign = "-"
	}
	if scientificExponent.IsInt64() {
		exponentValue := scientificExponent.Int64()
		if exponentValue >= -6 && exponentValue < 21 {
			decimalPoint := int(exponentValue) + 1
			switch {
			case decimalPoint <= 0:
				return sign + "0." + strings.Repeat("0", -decimalPoint) + coefficient, nil
			case decimalPoint >= len(coefficient):
				return sign + coefficient + strings.Repeat("0", decimalPoint-len(coefficient)), nil
			default:
				return sign + coefficient[:decimalPoint] + "." + coefficient[decimalPoint:], nil
			}
		}
	}
	if len(coefficient) == 1 {
		return sign + coefficient + "e" + scientificExponent.String(), nil
	}
	return sign + coefficient[:1] + "." + coefficient[1:] + "e" + scientificExponent.String(), nil
}

func decimalDigits(value string) bool {
	for index := range len(value) {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
	}
	return true
}
