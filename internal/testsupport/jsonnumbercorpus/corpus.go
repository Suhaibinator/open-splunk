// Package jsonnumbercorpus owns the implementation-independent numeric cases
// used to prove collector and runtime spath classification stay identical.
package jsonnumbercorpus

import (
	"math"
	"strconv"
)

// Case is one valid JSON-number lexeme and its public Dynamic representation.
// Value is the exact scalar text for integers, the decimal/v1 payload for a
// Map, or the decimal UInt64 bit pattern for Float64 (which preserves -0).
type Case struct {
	Name        string
	EventID     string
	Lexeme      string
	DynamicType string
	Value       string
}

// Cases returns a fresh copy of the shared bounded numeric compatibility
// corpus. Expectations are deliberately independent of either implementation.
func Cases() []Case {
	return []Case{
		{Name: "signed integer", EventID: "parity-signed", Lexeme: "-9", DynamicType: "Int64", Value: "-9"},
		{Name: "maximum unsigned integer", EventID: "parity-uint-max", Lexeme: "18446744073709551615", DynamicType: "UInt64", Value: "18446744073709551615"},
		{Name: "integer overflow", EventID: "parity-integer-overflow", Lexeme: "18446744073709551616", DynamicType: "Map(String, String)", Value: "18446744073709551616"},
		floatCase("exact fraction", "parity-exact-fraction", "0.5", 0.5),
		floatCase("exact exponent", "parity-exact-exponent", "1e0", 1),
		floatCase("exact fractional exponent", "parity-fractional-exponent", "1.5e1", 15),
		floatCase("normalized exponent parser trap", "parity-parser-trap", "9.7e2", 970),
		floatCase("negative exponent parser trap", "parity-negative-parser-trap", "-0.0186E4", -186),
		floatCase("negative fractional zero", "parity-negative-zero", "-0.0", math.Copysign(0, -1)),
		floatCase("positive maximum exponent zero", "parity-zero-exp-positive", "0e10000", 0),
		floatCase("negative exponent positive zero", "parity-zero-exp-negative", "0e-10000", 0),
		floatCase("positive exponent negative zero", "parity-negative-zero-exp-positive", "-0e10000", math.Copysign(0, -1)),
		floatCase("negative exponent negative zero", "parity-negative-zero-exp-negative", "-0e-10000", math.Copysign(0, -1)),
		{Name: "over positive zero exponent bound", EventID: "parity-zero-exp-over", Lexeme: "0e10001", DynamicType: "Map(String, String)", Value: "0e10001"},
		{Name: "over negative zero exponent bound", EventID: "parity-negative-zero-exp-over", Lexeme: "-0e-10001", DynamicType: "Map(String, String)", Value: "-0e-10001"},
		{Name: "inexact fraction", EventID: "parity-inexact", Lexeme: "0.1", DynamicType: "Map(String, String)", Value: "0.1"},
		{Name: "coefficient preserving exponent", EventID: "parity-coefficient", Lexeme: "1.2300e00", DynamicType: "Map(String, String)", Value: "1.2300e0"},
		{Name: "inexact shortest round trip", EventID: "parity-inexact-high", Lexeme: "0.10000000000000001", DynamicType: "Map(String, String)", Value: "0.10000000000000001"},
		{Name: "rounded wide fraction", EventID: "parity-wide-rounded", Lexeme: "9007199254740993.0", DynamicType: "Map(String, String)", Value: "9007199254740993.0"},
		floatCase("exact wide fraction", "parity-wide-exact", "9007199254740994.0", 9007199254740994),
		{Name: "underflow", EventID: "parity-underflow", Lexeme: "1E-0400", DynamicType: "Map(String, String)", Value: "1e-400"},
		{Name: "overflow", EventID: "parity-overflow", Lexeme: "1E+0400", DynamicType: "Map(String, String)", Value: "1e400"},
		floatCase("exact Float text bound", "parity-text-bound", "72057594037927936.0", math.Ldexp(1, 56)),
		{Name: "over exact Float text bound", EventID: "parity-text-over", Lexeme: "144115188075855872.0", DynamicType: "Map(String, String)", Value: "144115188075855872.0"},
		{Name: "long exact dyadic remains Decimal", EventID: "parity-long-dyadic", Lexeme: "867361737988403547205962240695953369140625e-60", DynamicType: "Map(String, String)", Value: "867361737988403547205962240695953369140625e-60"},
	}
}

func floatCase(name, eventID, lexeme string, value float64) Case {
	return Case{
		Name:        name,
		EventID:     eventID,
		Lexeme:      lexeme,
		DynamicType: "Float64",
		Value:       strconv.FormatUint(math.Float64bits(value), 10),
	}
}
