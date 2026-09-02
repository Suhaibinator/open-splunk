package splrelativetime

import (
	"errors"
	"strconv"
	"strings"
	"testing"
)

func FuzzCompileSpecifier(f *testing.F) {
	for _, seed := range []string{
		"-1d",
		"+1h",
		"-2h@h",
		"@d-2h",
		"-mon@month+7days",
		"@w0",
		"@w7",
		"@w1+1d",
		"-7d@w6",
		"@q",
		"-1y@y+1mon",
		"-0d",
		"+d",
		"-1d@",
		"@",
		"@d@d",
		"+1d-1d",
		"-1d+1d",
		"+5",
		"-1x",
		"@w8",
		"-99999999999999999999d",
		"-11423635201s",
		"-11423635200s",
		"-1D",
		"−1d",
		"",
		"\x00",
		"@d+1h-1m",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, source string) {
		first, firstErr := CompileSpecifier(source)
		second, secondErr := CompileSpecifier(source)
		if !errors.Is(firstErr, secondErr) || !errors.Is(secondErr, firstErr) || first != second {
			t.Fatalf("CompileSpecifier changed across identical input: first=%#v/%v second=%#v/%v",
				first, firstErr, second, secondErr)
		}
		if firstErr != nil {
			if first != (Specifier{}) {
				t.Fatalf("error published a partial specifier: %#v", first)
			}
			matched := 0
			for _, sentinel := range []error{ErrInvalidSpecifier, ErrMagnitudeOutOfRange, ErrSpecifierTooLarge} {
				if errors.Is(firstErr, sentinel) {
					matched++
				}
			}
			if matched != 1 {
				t.Fatalf("error %v matches %d published sentinels, want exactly one", firstErr, matched)
			}
			return
		}

		if first.WorkUnits != len(source) || first.WorkUnits > MaximumSpecifierWorkUnits {
			t.Fatalf("work units %d for a %d-byte specifier", first.WorkUnits, len(source))
		}
		count := first.OperationCount()
		if count < 1 || count > MaximumOperations {
			t.Fatalf("operation count %d outside [1, %d]", count, MaximumOperations)
		}
		if _, ok := first.Operation(count); ok {
			t.Fatalf("operation %d is readable past the count", count)
		}
		if _, ok := first.Operation(-1); ok {
			t.Fatal("operation -1 is readable")
		}
		operations := make([]Operation, 0, count)
		for index := range count {
			operation, ok := first.Operation(index)
			if !ok {
				t.Fatalf("operation %d of %d is unreadable", index, count)
			}
			operations = append(operations, operation)
		}
		relativeTimeFuzzCheckShape(t, source, operations)

		// The canonical spelling of the operations re-parses to the same
		// program, so the normalized form is a faithful representation.
		canonical := relativeTimeFuzzRender(operations)
		again, err := CompileSpecifier(canonical)
		if err != nil {
			t.Fatalf("canonical %q of %q does not compile: %v", canonical, source, err)
		}
		if again.OperationCount() != count {
			t.Fatalf("canonical %q has %d operations, %q has %d", canonical, again.OperationCount(), source, count)
		}
		for index, want := range operations {
			if got, _ := again.Operation(index); got != want {
				t.Fatalf("canonical %q operation %d = %#v, %q has %#v", canonical, index, got, source, want)
			}
		}
	})
}

// relativeTimeFuzzCheckShape pins the documented program grammar on the
// normalized operations: an optional offset, an optional snap, and an
// optional post-snap offset, with weekday only on a week snap, magnitude
// only on an offset, a positive sign on zero, and every magnitude inside the
// representable span for its unit.
func relativeTimeFuzzCheckShape(t *testing.T, source string, operations []Operation) {
	t.Helper()
	snapIndex := -1
	for index, operation := range operations {
		if operation.Unit < UnitSecond || operation.Unit > UnitYear {
			t.Fatalf("%q operation %d has unit %d", source, index, operation.Unit)
		}
		switch operation.Kind {
		case OperationSnap:
			if snapIndex >= 0 {
				t.Fatalf("%q snaps twice: %#v", source, operations)
			}
			snapIndex = index
			if operation.Magnitude != 0 || operation.Negative {
				t.Fatalf("%q snap carries an offset: %#v", source, operation)
			}
			if operation.Weekday != 0 && operation.Unit != UnitWeek {
				t.Fatalf("%q snap carries a weekday on a non-week unit: %#v", source, operation)
			}
			if operation.Weekday > 6 {
				t.Fatalf("%q snap weekday %d is not normalized", source, operation.Weekday)
			}
		case OperationOffset:
			if operation.Weekday != 0 {
				t.Fatalf("%q offset carries a weekday: %#v", source, operation)
			}
			if operation.Magnitude == 0 && operation.Negative {
				t.Fatalf("%q offset is negative zero: %#v", source, operation)
			}
			if operation.Magnitude > maximumMagnitude(operation.Unit) {
				t.Fatalf("%q offset exceeds its unit span: %#v", source, operation)
			}
			if snapIndex < 0 && index != 0 {
				t.Fatalf("%q has two offsets before any snap: %#v", source, operations)
			}
			if snapIndex >= 0 && index != snapIndex+1 {
				t.Fatalf("%q has two offsets after its snap: %#v", source, operations)
			}
		default:
			t.Fatalf("%q operation %d has kind %d", source, index, operation.Kind)
		}
	}
	if snapIndex < 0 && len(operations) != 1 {
		t.Fatalf("%q has %d operations without a snap", source, len(operations))
	}
}

var relativeTimeFuzzUnits = map[Unit]string{
	UnitSecond:  "s",
	UnitMinute:  "m",
	UnitHour:    "h",
	UnitDay:     "d",
	UnitWeek:    "w",
	UnitMonth:   "mon",
	UnitQuarter: "q",
	UnitYear:    "y",
}

func relativeTimeFuzzRender(operations []Operation) string {
	var rendered strings.Builder
	for _, operation := range operations {
		if operation.Kind == OperationSnap {
			rendered.WriteByte('@')
			if operation.Unit == UnitWeek && operation.Weekday != 0 {
				rendered.WriteByte('w')
				rendered.WriteByte('0' + operation.Weekday)
				continue
			}
			rendered.WriteString(relativeTimeFuzzUnits[operation.Unit])
			continue
		}
		if operation.Negative {
			rendered.WriteByte('-')
		} else {
			rendered.WriteByte('+')
		}
		rendered.WriteString(strconv.FormatUint(operation.Magnitude, 10))
		rendered.WriteString(relativeTimeFuzzUnits[operation.Unit])
	}
	return rendered.String()
}
