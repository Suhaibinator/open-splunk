package searchjobs

import (
	"errors"
	"strconv"
	"strings"
)

// CanonicalDecimal returns the deterministic transport and comparison spelling
// for one validated decimal. Numerically equivalent coefficient, scale, and
// exponent spellings collapse to one value without rounding through float64.
// The result uses plain notation for ordinary magnitudes and normalized
// scientific notation outside that compact range.
func CanonicalDecimal(source string) (string, error) {
	if !validDecimal(source) {
		return "", errors.New("search result decimal is invalid")
	}
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
	integer, fraction, _ := strings.Cut(mantissa, ".")
	exponentSign := 0
	exponentMagnitude := "0"
	if hasExponent {
		exponentSign = 1
		switch exponent[0] {
		case '-':
			exponentSign = -1
			exponent = exponent[1:]
		case '+':
			exponent = exponent[1:]
		}
		exponent = strings.TrimLeft(exponent, "0")
		if exponent == "" {
			exponentSign = 0
		} else {
			exponentMagnitude = exponent
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
	scientificSign, scientificMagnitude := addDecimalExponent(
		exponentSign,
		exponentMagnitude,
		len(integer)-firstNonzero-1,
	)

	sign := ""
	if negative {
		sign = "-"
	}
	if exponentValue, small := smallDecimalExponent(scientificSign, scientificMagnitude); small {
		if exponentValue >= -6 && exponentValue < 21 {
			decimalPoint := exponentValue + 1
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
	formattedExponent := scientificMagnitude
	if scientificSign < 0 {
		formattedExponent = "-" + formattedExponent
	}
	if len(coefficient) == 1 {
		return sign + coefficient + "e" + formattedExponent, nil
	}
	return sign + coefficient[:1] + "." + coefficient[1:] + "e" + formattedExponent, nil
}

// addDecimalExponent adds a bounded machine-sized adjustment to an arbitrary
// precision signed decimal exponent in linear time. This avoids feeding an
// attacker-sized exponent to big.Int merely to add the mantissa's offset.
func addDecimalExponent(sign int, magnitude string, adjustment int) (int, string) {
	if adjustment == 0 {
		return sign, magnitude
	}
	adjustmentSign := 1
	if adjustment < 0 {
		adjustmentSign = -1
		adjustment = -adjustment
	}
	adjustmentMagnitude := strconv.Itoa(adjustment)
	if sign == 0 {
		return adjustmentSign, adjustmentMagnitude
	}
	if sign == adjustmentSign {
		return sign, addDecimalMagnitudes(magnitude, adjustmentMagnitude)
	}
	switch compareDecimalMagnitudes(magnitude, adjustmentMagnitude) {
	case 0:
		return 0, "0"
	case 1:
		return sign, subtractDecimalMagnitudes(magnitude, adjustmentMagnitude)
	default:
		return adjustmentSign, subtractDecimalMagnitudes(adjustmentMagnitude, magnitude)
	}
}

func smallDecimalExponent(sign int, magnitude string) (int, bool) {
	if len(magnitude) > 2 {
		return 0, false
	}
	value := 0
	for index := range len(magnitude) {
		value = value*10 + int(magnitude[index]-'0')
	}
	if sign < 0 {
		value = -value
	}
	return value, true
}

func compareDecimalMagnitudes(left, right string) int {
	if len(left) < len(right) {
		return -1
	}
	if len(left) > len(right) {
		return 1
	}
	return strings.Compare(left, right)
}

func addDecimalMagnitudes(left, right string) string {
	length := max(len(left), len(right))
	result := make([]byte, length+1)
	leftIndex, rightIndex, resultIndex, carry := len(left)-1, len(right)-1, length, 0
	for resultIndex > 0 {
		digit := carry
		if leftIndex >= 0 {
			digit += int(left[leftIndex] - '0')
			leftIndex--
		}
		if rightIndex >= 0 {
			digit += int(right[rightIndex] - '0')
			rightIndex--
		}
		result[resultIndex] = byte(digit%10) + '0'
		carry = digit / 10
		resultIndex--
	}
	if carry == 0 {
		return string(result[1:])
	}
	result[0] = byte(carry) + '0'
	return string(result)
}

// subtractDecimalMagnitudes returns left-right for normalized left >= right.
func subtractDecimalMagnitudes(left, right string) string {
	result := make([]byte, len(left))
	leftIndex, rightIndex, borrow := len(left)-1, len(right)-1, 0
	for leftIndex >= 0 {
		digit := int(left[leftIndex]-'0') - borrow
		if rightIndex >= 0 {
			digit -= int(right[rightIndex] - '0')
			rightIndex--
		}
		if digit < 0 {
			digit += 10
			borrow = 1
		} else {
			borrow = 0
		}
		result[leftIndex] = byte(digit) + '0'
		leftIndex--
	}
	trimmed := strings.TrimLeft(string(result), "0")
	if trimmed == "" {
		return "0"
	}
	return trimmed
}
