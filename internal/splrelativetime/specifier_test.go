package splrelativetime

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestCompileSpecifierNormalizesDocumentedOperationOrder(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		source     string
		operations []Operation
	}{
		{
			name:   "offset then snap",
			source: "-2h@h",
			operations: []Operation{
				{
					Kind:      OperationOffset,
					Unit:      UnitHour,
					Magnitude: 2,
					Negative:  true,
				},
				{Kind: OperationSnap, Unit: UnitHour},
			},
		},
		{
			name:   "snap then offset",
			source: "@d-2h",
			operations: []Operation{
				{Kind: OperationSnap, Unit: UnitDay},
				{
					Kind:      OperationOffset,
					Unit:      UnitHour,
					Magnitude: 2,
					Negative:  true,
				},
			},
		},
		{
			name:   "offset snap and post offset",
			source: "-mon@month+7days",
			operations: []Operation{
				{
					Kind:      OperationOffset,
					Unit:      UnitMonth,
					Magnitude: 1,
					Negative:  true,
				},
				{Kind: OperationSnap, Unit: UnitMonth},
				{
					Kind:      OperationOffset,
					Unit:      UnitDay,
					Magnitude: 7,
				},
			},
		},
		{
			name:   "implicit magnitude without snap",
			source: "+d",
			operations: []Operation{
				{
					Kind:      OperationOffset,
					Unit:      UnitDay,
					Magnitude: 1,
				},
			},
		},
		{
			name:   "zero offset",
			source: "+0seconds",
			operations: []Operation{
				{
					Kind: OperationOffset,
					Unit: UnitSecond,
				},
			},
		},
		{
			name:   "negative zero offset is canonicalized",
			source: "-0seconds",
			operations: []Operation{
				{
					Kind: OperationOffset,
					Unit: UnitSecond,
				},
			},
		},
		{
			name:   "sunday week default",
			source: "@week",
			operations: []Operation{
				{Kind: OperationSnap, Unit: UnitWeek, Weekday: 0},
			},
		},
		{
			name:   "sunday week seven",
			source: "@w7",
			operations: []Operation{
				{Kind: OperationSnap, Unit: UnitWeek, Weekday: 0},
			},
		},
		{
			name:   "monday week",
			source: "@w1",
			operations: []Operation{
				{Kind: OperationSnap, Unit: UnitWeek, Weekday: 1},
			},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			compiled, err := CompileSpecifier(test.source)
			if err != nil {
				t.Fatalf("CompileSpecifier(%q): %v", test.source, err)
			}
			operations := specifierOperations(compiled)
			if !reflect.DeepEqual(operations, test.operations) {
				t.Fatalf(
					"CompileSpecifier(%q) operations = %#v, want %#v",
					test.source,
					operations,
					test.operations,
				)
			}
			if compiled.WorkUnits != len(test.source) {
				t.Fatalf(
					"CompileSpecifier(%q) work = %d, want %d",
					test.source,
					compiled.WorkUnits,
					len(test.source),
				)
			}
		})
	}
}

func TestCompileSpecifierAcceptsDocumentedUnitAliases(t *testing.T) {
	t.Parallel()

	for unit, aliases := range map[Unit][]string{
		UnitSecond: {
			"s", "sec", "secs", "second", "seconds",
		},
		UnitMinute: {
			"m", "min", "mins", "minute", "minutes",
		},
		UnitHour: {
			"h", "hr", "hrs", "hour", "hours",
		},
		UnitDay: {
			"d", "day", "days",
		},
		UnitWeek: {
			"w", "week", "weeks",
		},
		UnitMonth: {
			"mon", "month", "months",
		},
		UnitQuarter: {
			"q", "qtr", "qtrs", "quarter", "quarters",
		},
		UnitYear: {
			"y", "yr", "yrs", "year", "years",
		},
	} {
		unit := unit
		for _, alias := range aliases {
			alias := alias
			t.Run(alias, func(t *testing.T) {
				t.Parallel()

				compiled, err := CompileSpecifier("-3" + alias)
				if err != nil {
					t.Fatalf("CompileSpecifier(%q): %v", "-3"+alias, err)
				}
				want := Operation{
					Kind:      OperationOffset,
					Unit:      unit,
					Magnitude: 3,
					Negative:  true,
				}
				operation, ok := compiled.Operation(0)
				if !ok || operation != want {
					t.Fatalf("offset = %#v/%t, want %#v", operation, ok, want)
				}
				if compiled.OperationCount() != 1 {
					t.Fatalf(
						"operation count = %d, want offset only",
						compiled.OperationCount(),
					)
				}
				if _, ok := compiled.Operation(1); ok {
					t.Fatal("out-of-range operation lookup unexpectedly succeeded")
				}
			})
		}
	}
}

func specifierOperations(specifier Specifier) []Operation {
	operations := make([]Operation, 0, specifier.OperationCount())
	for index := range specifier.OperationCount() {
		operation, ok := specifier.Operation(index)
		if !ok {
			panic("validated specifier rejected an in-range operation")
		}
		operations = append(operations, operation)
	}
	return operations
}

func TestCompileSpecifierRejectsAmbiguousUnsupportedOrUnsafeSyntax(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		"",
		"now",
		"1d",
		"+d@",
		"@",
		"@w8",
		"+1M",
		"+1ms",
		"+1d+2h",
		"-1d@d+2h-3m",
		"-1d@d@h",
		" @d",
		"@d ",
		"@ day",
		"+-1d",
		"@w01",
		"@month1",
		"+1d\x00",
		string([]byte{'+', '1', 'd', 0xff}),
	} {
		if _, err := CompileSpecifier(source); !errors.Is(
			err,
			ErrInvalidSpecifier,
		) {
			t.Errorf(
				"CompileSpecifier(%q) error = %v, want ErrInvalidSpecifier",
				source,
				err,
			)
		}
	}
}

func TestCompileSpecifierPinsExactMagnitudeBounds(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		accepted   string
		outOfRange string
	}{
		{"seconds", "+11423635200s", "+11423635201s"},
		{"minutes", "+190393920m", "+190393921m"},
		{"hours", "+3173232h", "+3173233h"},
		{"days", "+132218d", "+132219d"},
		{"weeks", "+18888w", "+18889w"},
		{"months", "+4344mon", "+4345mon"},
		{"quarters", "+1448q", "+1449q"},
		{"years", "+362y", "+363y"},
		{"uint64 overflow", "", "+18446744073709551616s"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if test.accepted != "" {
				if _, err := CompileSpecifier(test.accepted); err != nil {
					t.Fatalf("CompileSpecifier(%q): %v", test.accepted, err)
				}
			}
			if _, err := CompileSpecifier(test.outOfRange); !errors.Is(
				err,
				ErrMagnitudeOutOfRange,
			) {
				t.Fatalf(
					"CompileSpecifier(%q) error = %v, want ErrMagnitudeOutOfRange",
					test.outOfRange,
					err,
				)
			}
		})
	}
}

func TestCompileSpecifierPinsIndependentResourceLimits(t *testing.T) {
	t.Parallel()

	if MaximumSpecifierBytes != 1<<10 ||
		MaximumSpecifierWorkUnits != 1<<10 ||
		MaximumOperations != 3 {
		t.Fatalf(
			"relative-time limits = %d bytes/%d work/%d operations",
			MaximumSpecifierBytes,
			MaximumSpecifierWorkUnits,
			MaximumOperations,
		)
	}
	if _, err := CompileSpecifier(
		"+" + strings.Repeat("0", MaximumSpecifierBytes) + "s",
	); !errors.Is(err, ErrSpecifierTooLarge) {
		t.Fatalf("oversized specifier error = %v, want ErrSpecifierTooLarge", err)
	}
}

var (
	compiledSpecifierSink      Specifier
	compiledSpecifierErrorSink error
)

func TestCompileSpecifierDoesNotAllocateForBoundedPrograms(t *testing.T) {
	allocations := testing.AllocsPerRun(100, func() {
		compiledSpecifierSink, compiledSpecifierErrorSink =
			CompileSpecifier("-1d@d+2h")
	})
	if compiledSpecifierErrorSink != nil {
		t.Fatalf("CompileSpecifier: %v", compiledSpecifierErrorSink)
	}
	if allocations != 0 {
		t.Fatalf("CompileSpecifier allocations = %g, want 0", allocations)
	}
}
