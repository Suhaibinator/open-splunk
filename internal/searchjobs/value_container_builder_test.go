package searchjobs

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestImmutableContainerBuildersObserveCancellation(t *testing.T) {
	items := make([]Value, 4096)
	for index := range items {
		items[index] = UnsignedValue(uint64(index))
	}
	nested := ListValue(items...)
	for _, object := range []bool{false, true} {
		ctx := &cancelAfterChecksContext{after: 4}
		calls := 0
		var value Value
		var err error
		if object {
			value, err = ObjectValueFromFieldsContext(ctx, 100, func(int) (ObjectField, error) { calls++; return ObjectField{Name: "nested", Value: nested}, nil })
		} else {
			value, err = ListValueFromItemsContext(ctx, 100, func(int) (Value, error) { calls++; return nested, nil })
		}
		if !errors.Is(err, context.Canceled) || value.Kind() != ValueKindInvalid || calls != 1 {
			t.Fatalf("object=%v calls=%d value=%v err=%v", object, calls, value.Kind(), err)
		}
	}
}

func TestImmutableContainerBuildersDetachAndValidate(t *testing.T) {
	original := []ObjectField{{Name: "a", Value: ListValue(UnsignedValue(1))}, {Name: "b", Value: StringValue("text")}}
	got, err := ObjectValueFromFieldsContext(context.Background(), len(original), func(index int) (ObjectField, error) { return original[index], nil })
	if err != nil {
		t.Fatal(err)
	}
	want, err := ObjectValue(original...)
	if err != nil {
		t.Fatal(err)
	}
	original[0] = ObjectField{Name: "changed", Value: UnsignedValue(99)}
	gotFields, _ := got.Object()
	wantFields, _ := want.Object()
	if !reflect.DeepEqual(gotFields, wantFields) {
		t.Fatal("builder retained caller fields or changed nested semantics")
	}
	if _, err := ObjectValueFromFields(2, func(int) (ObjectField, error) { return ObjectField{Name: "same", Value: NullValue()}, nil }); err == nil {
		t.Fatal("duplicate fields accepted")
	}
	deep := NullValue()
	for range 32 {
		deep = ListValue(deep)
	}
	if _, err := ListValueFromItemsContext(context.Background(), 1, func(int) (Value, error) { return deep, nil }); err == nil {
		t.Fatal("list depth exceeded")
	}
	if _, err := ObjectValueFromFieldsContext(context.Background(), 1, func(int) (ObjectField, error) { return ObjectField{Name: "deep", Value: deep}, nil }); err == nil {
		t.Fatal("object depth exceeded")
	}
}

func TestImmutableContainerBuilderChecksFinalCallbackCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, err := ListValueFromItemsContext(ctx, 1, func(int) (Value, error) { cancel(); return UnsignedValue(1), nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("last callback cancellation=%v", err)
	}
}
