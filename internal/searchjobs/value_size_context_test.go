package searchjobs

import (
	"context"
	"errors"
	"testing"
)

func TestValueRetainedSizeContextCancelsNestedTraversal(t *testing.T) {
	items := make([]Value, 4096)
	for i := range items {
		items[i] = UnsignedValue(uint64(i))
	}
	value, err := ObjectValue(ObjectField{Name: "items", Value: ListValue(items...)})
	if err != nil {
		t.Fatal(err)
	}
	ctx := &cancelAfterChecksContext{after: 3}
	if size, err := value.RetainedSizeBytesContext(ctx); !errors.Is(err, context.Canceled) || size != 0 {
		t.Fatalf("nested measurement completed after cancellation: size=%d err=%v", size, err)
	}
	if ctx.checks != 3 {
		t.Fatalf("measurement kept traversing after cancellation: checks=%d", ctx.checks)
	}
	want, err := value.RetainedSizeBytes()
	if err != nil {
		t.Fatal(err)
	}
	got, err := value.RetainedSizeBytesContext(context.Background())
	if err != nil || got != want {
		t.Fatalf("context accounting changed: got=%d want=%d err=%v", got, want, err)
	}
}

func TestValueRetainedSizeContextChecksCompletion(t *testing.T) {
	ctx := &cancelAfterChecksContext{after: 2}
	if size, err := UnsignedValue(1).RetainedSizeBytesContext(ctx); !errors.Is(err, context.Canceled) || size != 0 {
		t.Fatalf("completed measurement ignored cancellation: size=%d err=%v", size, err)
	}
	var missingContext context.Context
	if _, err := NullValue().RetainedSizeBytesContext(missingContext); err == nil {
		t.Fatal("nil context accepted")
	}
}
