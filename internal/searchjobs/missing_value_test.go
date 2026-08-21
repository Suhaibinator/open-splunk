package searchjobs

import "testing"

func TestMissingValueRetainsDistinctConcreteKind(t *testing.T) {
	t.Parallel()

	missing := MissingValue()
	if missing.Kind() != ValueKindMissing || !missing.IsMissing() || missing.IsNull() {
		t.Fatalf("MissingValue() = kind %v missing %t null %t", missing.Kind(), missing.IsMissing(), missing.IsNull())
	}
	if NullValue().IsMissing() {
		t.Fatal("NullValue() reports missing")
	}
	if payload, retained, err := measureValue(missing, 0); err != nil || payload != 0 || retained != retainedValueBase {
		t.Fatalf("measureValue(missing) = (%d, %d, %v), want (0, %d, nil)", payload, retained, err, retainedValueBase)
	}
	if list := ListValue(missing, NullValue()); list.Kind() != ValueKindList {
		t.Fatalf("list containing missing = kind %v", list.Kind())
	}
}

func TestMissingValueFollowsNullableSchemaRules(t *testing.T) {
	t.Parallel()

	sink := &resultSink{}
	for _, test := range []struct {
		name    string
		column  Column
		value   Value
		wantErr bool
	}{
		{name: "nullable fixed column", column: Column{Name: "value", Kind: ValueKindString, Nullable: true}, value: MissingValue()},
		{name: "nullable mixed column", column: Column{Name: "value", Kind: ValueKindMixed, Nullable: true}, value: MissingValue()},
		{name: "missing-only column", column: Column{Name: "value", Kind: ValueKindMissing}, value: MissingValue()},
		{name: "nonnullable fixed column", column: Column{Name: "value", Kind: ValueKindString}, value: MissingValue(), wantErr: true},
		{name: "nonnullable mixed column", column: Column{Name: "value", Kind: ValueKindMixed}, value: MissingValue(), wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := sink.measureRowCellsLocked([]Column{test.column}, []Value{test.value})
			if (err != nil) != test.wantErr {
				t.Fatalf("measureRowCellsLocked() error = %v, wantErr %t", err, test.wantErr)
			}
			if schemaErr := validateSchema(Schema{Columns: []Column{test.column}}, []string{"value"}); schemaErr != nil {
				t.Fatalf("validateSchema(missing-capable column): %v", schemaErr)
			}
		})
	}
}
