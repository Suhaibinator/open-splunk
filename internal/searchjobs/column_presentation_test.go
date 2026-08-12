package searchjobs

import (
	"strings"
	"testing"
)

func TestFlatMultivalueColumnPresentationCloneAndRetainedBytes(t *testing.T) {
	t.Parallel()

	schema := Schema{Columns: []Column{{
		Name:                       "users",
		Kind:                       ValueKindList,
		Multivalue:                 true,
		FlatMultivalueDelimiter:    " / ",
		HasFlatMultivalueDelimiter: true,
	}}}
	if err := validateSchema(schema, []string{"users"}); err != nil {
		t.Fatal(err)
	}
	cloned := cloneSchema(schema)
	if len(cloned.Columns) != 1 || cloned.Columns[0] != schema.Columns[0] {
		t.Fatalf("clone = %#v", cloned)
	}
	withDelimiter, err := retainedSchemaSize(schema)
	if err != nil {
		t.Fatal(err)
	}
	withoutDelimiter := schema
	withoutDelimiter.Columns = append([]Column(nil), schema.Columns...)
	withoutDelimiter.Columns[0].FlatMultivalueDelimiter = ""
	withoutDelimiter.Columns[0].HasFlatMultivalueDelimiter = false
	base, err := retainedSchemaSize(withoutDelimiter)
	if err != nil {
		t.Fatal(err)
	}
	if withDelimiter != base+uint64(len(" / ")) {
		t.Fatalf("retained bytes = %d, want %d", withDelimiter, base+uint64(len(" / ")))
	}
}

func TestFlatMultivalueColumnPresentationValidation(t *testing.T) {
	t.Parallel()

	validEmpty := Column{
		Name:                       "users",
		Kind:                       ValueKindList,
		Multivalue:                 true,
		HasFlatMultivalueDelimiter: true,
	}
	if !validEmpty.ValidFlatMultivaluePresentation() {
		t.Fatal("present empty delimiter was rejected")
	}
	tests := []Column{
		{Name: "users", Kind: ValueKindList, Multivalue: true, FlatMultivalueDelimiter: ","},
		{Name: "users", Kind: ValueKindString, Multivalue: true, FlatMultivalueDelimiter: ",", HasFlatMultivalueDelimiter: true},
		{Name: "users", Kind: ValueKindList, FlatMultivalueDelimiter: ",", HasFlatMultivalueDelimiter: true},
		{Name: "users", Kind: ValueKindList, Multivalue: true, FlatMultivalueDelimiter: string([]byte{0xff}), HasFlatMultivalueDelimiter: true},
		{Name: "users", Kind: ValueKindList, Multivalue: true, FlatMultivalueDelimiter: strings.Repeat("x", MaximumFlatMultivalueDelimiterBytes+1), HasFlatMultivalueDelimiter: true},
	}
	for _, column := range tests {
		if column.ValidFlatMultivaluePresentation() {
			t.Fatalf("invalid presentation accepted: %#v", column)
		}
		if err := validateSchema(Schema{Columns: []Column{column}}, []string{"users"}); err == nil {
			t.Fatalf("invalid schema accepted: %#v", column)
		}
	}
}

func TestStatsSparklineColumnPresentationValidation(t *testing.T) {
	t.Parallel()
	valid := Column{Name: "trend", Kind: ValueKindList, Multivalue: true, StatsSparkline: true}
	if !valid.ValidFlatMultivaluePresentation() {
		t.Fatal("valid stats sparkline presentation rejected")
	}
	for _, column := range []Column{
		{Name: "trend", Kind: ValueKindString, StatsSparkline: true},
		{Name: "trend", Kind: ValueKindList, StatsSparkline: true},
		{Name: "trend", Kind: ValueKindList, Multivalue: true, StatsSparkline: true, HasFlatMultivalueDelimiter: true},
	} {
		if column.ValidFlatMultivaluePresentation() {
			t.Fatalf("invalid stats sparkline presentation accepted: %#v", column)
		}
	}
}
