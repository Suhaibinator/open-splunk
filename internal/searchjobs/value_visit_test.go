package searchjobs

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestVisitDetachedEmitsExactDepthFirstSequence(t *testing.T) {
	t.Parallel()

	timestamp := time.Date(2026, time.July, 22, 1, 2, 3, 4, time.UTC)
	decimal, err := DecimalValue("+0012.3400e-2")
	if err != nil {
		t.Fatal(err)
	}
	nestedObject, err := ObjectValue(ObjectField{Name: "child", Value: NullValue()})
	if err != nil {
		t.Fatal(err)
	}
	value, err := ObjectValue(
		ObjectField{Name: "null", Value: NullValue()},
		ObjectField{Name: "string", Value: StringValue("hello")},
		ObjectField{Name: "signed", Value: SignedValue(-2)},
		ObjectField{Name: "unsigned", Value: UnsignedValue(3)},
		ObjectField{Name: "double", Value: DoubleValue(4.5)},
		ObjectField{Name: "bool", Value: BoolValue(true)},
		ObjectField{Name: "bytes", Value: BytesValue([]byte{6, 7})},
		ObjectField{Name: "time", Value: TimeValue(timestamp)},
		ObjectField{Name: "duration", Value: DurationValue(-8 * time.Second)},
		ObjectField{Name: "decimal", Value: decimal},
		ObjectField{Name: "nested", Value: ListValue(StringValue("item"), nestedObject)},
	)
	if err != nil {
		t.Fatal(err)
	}

	var got []ValueVisitToken
	if err := value.VisitDetached(func(token ValueVisitToken) error {
		got = append(got, token)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	want := []ValueVisitToken{
		{Kind: ValueVisitObjectBegin, Length: 11},
		{Kind: ValueVisitObjectField, StringValue: "null"},
		{Kind: ValueVisitNull},
		{Kind: ValueVisitObjectField, StringValue: "string"},
		{Kind: ValueVisitString, StringValue: "hello"},
		{Kind: ValueVisitObjectField, StringValue: "signed"},
		{Kind: ValueVisitSigned, SignedValue: -2},
		{Kind: ValueVisitObjectField, StringValue: "unsigned"},
		{Kind: ValueVisitUnsigned, UnsignedValue: 3},
		{Kind: ValueVisitObjectField, StringValue: "double"},
		{Kind: ValueVisitDouble, DoubleValue: 4.5},
		{Kind: ValueVisitObjectField, StringValue: "bool"},
		{Kind: ValueVisitBool, BoolValue: true},
		{Kind: ValueVisitObjectField, StringValue: "bytes"},
		{Kind: ValueVisitBytes, BytesValue: []byte{6, 7}},
		{Kind: ValueVisitObjectField, StringValue: "time"},
		{Kind: ValueVisitTime, TimeValue: timestamp},
		{Kind: ValueVisitObjectField, StringValue: "duration"},
		{Kind: ValueVisitDuration, DurationValue: -8 * time.Second},
		{Kind: ValueVisitObjectField, StringValue: "decimal"},
		{Kind: ValueVisitDecimal, StringValue: "+0012.3400e-2"},
		{Kind: ValueVisitObjectField, StringValue: "nested"},
		{Kind: ValueVisitListBegin, Length: 2},
		{Kind: ValueVisitString, StringValue: "item"},
		{Kind: ValueVisitObjectBegin, Length: 1},
		{Kind: ValueVisitObjectField, StringValue: "child"},
		{Kind: ValueVisitNull},
		{Kind: ValueVisitObjectEnd},
		{Kind: ValueVisitListEnd},
		{Kind: ValueVisitObjectEnd},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("VisitDetached tokens = %#v, want %#v", got, want)
	}
}

func TestVisitDetachedByteTokenCannotMutateValue(t *testing.T) {
	t.Parallel()

	value := BytesValue([]byte{1, 2, 3})
	var retained []byte
	if err := value.VisitDetached(func(token ValueVisitToken) error {
		retained = token.BytesValue
		retained[0] = 9
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	retained[1] = 8

	got, ok := value.Bytes()
	if !ok || !reflect.DeepEqual(got, []byte{1, 2, 3}) {
		t.Fatalf("value bytes after callback mutation = %v, %v", got, ok)
	}

	if err := value.VisitDetached(func(token ValueVisitToken) error {
		if !reflect.DeepEqual(token.BytesValue, []byte{1, 2, 3}) {
			t.Fatalf("second visit bytes = %v", token.BytesValue)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestVisitDetachedStopsAtCallbackError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("stop")
	var kinds []ValueVisitTokenKind
	err := ListValue(StringValue("first"), SignedValue(2)).VisitDetached(func(token ValueVisitToken) error {
		kinds = append(kinds, token.Kind)
		if token.Kind == ValueVisitString {
			return wantErr
		}
		return nil
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("VisitDetached error = %v, want %v", err, wantErr)
	}
	if want := []ValueVisitTokenKind{ValueVisitListBegin, ValueVisitString}; !reflect.DeepEqual(kinds, want) {
		t.Fatalf("visited kinds = %v, want %v", kinds, want)
	}
}

func TestVisitDetachedEnforcesMaximumDepth(t *testing.T) {
	t.Parallel()

	value := NullValue()
	for range maximumValueVisitDepth {
		value = Value{kind: ValueKindList, listValue: []Value{value}}
	}
	var visits int
	if err := value.VisitDetached(func(ValueVisitToken) error {
		visits++
		return nil
	}); err != nil {
		t.Fatalf("VisitDetached at maximum depth: %v", err)
	}
	if want := maximumValueVisitDepth*2 + 1; visits != want {
		t.Fatalf("visits at maximum depth = %d, want %d", visits, want)
	}

	tooDeep := Value{kind: ValueKindList, listValue: []Value{value}}
	if err := tooDeep.VisitDetached(func(ValueVisitToken) error { return nil }); err == nil {
		t.Fatal("VisitDetached succeeded beyond maximum depth")
	}
}

func TestVisitDetachedRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value Value
	}{
		{name: "invalid kind", value: Value{}},
		{name: "mixed cell kind", value: Value{kind: ValueKindMixed}},
		{name: "invalid decimal", value: Value{kind: ValueKindDecimal, decimalValue: "1."}},
		{name: "empty object field", value: Value{kind: ValueKindObject, objectValue: []ObjectField{{Value: NullValue()}}}},
		{name: "duplicate object field", value: Value{kind: ValueKindObject, objectValue: []ObjectField{
			{Name: "same", Value: NullValue()},
			{Name: "same", Value: NullValue()},
		}}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var visits int
			if err := test.value.VisitDetached(func(ValueVisitToken) error {
				visits++
				return nil
			}); err == nil {
				t.Fatal("VisitDetached succeeded")
			}
			if visits != 0 {
				t.Fatalf("VisitDetached emitted %d tokens for invalid value", visits)
			}
		})
	}

	if err := NullValue().VisitDetached(nil); err == nil {
		t.Fatal("VisitDetached(nil) succeeded")
	}
}
