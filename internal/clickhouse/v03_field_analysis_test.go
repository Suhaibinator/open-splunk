package clickhouse

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileFieldCatalogProfilesMakeMVOutputAsTypedList(t *testing.T) {
	t.Parallel()
	logical := buildPlan(
		t,
		`index=gradethis | makemv delim="," allowempty=true tags`,
	)
	if err := plan.ValidateFieldAnalysisEligibility(logical); err != nil {
		t.Fatalf("ValidateFieldAnalysisEligibility(makemv): %v", err)
	}
	compiled, err := (Compiler{}).CompileFieldCatalog(
		logical,
		FieldCatalogSpec{MaximumFields: 64},
	)
	if err != nil {
		t.Fatalf("CompileFieldCatalog(makemv): %v", err)
	}

	if !containsArgument(compiled.Args, uint8(eventfields.StoredValueTypeList)) {
		t.Fatalf("catalog did not bind the semantic List type: %#v", compiled.Args)
	}
	for _, fragment := range []string{
		"Array(String)",
		"__os_makemv_value_present_",
		quoteIdentifier(fieldCatalogKnownTypes),
	} {
		if !strings.Contains(compiled.SQL, fragment) {
			t.Errorf("catalog SQL is missing typed multivalue fragment %q", fragment)
		}
	}
	if !containsArgument(compiled.Args, "tags") {
		t.Fatalf("catalog omitted the exact makemv output name: %#v", compiled.Args)
	}
}

func TestCompileFieldSummaryClassifiesMakeMVOutputAsUnsupportedList(t *testing.T) {
	t.Parallel()
	logical := buildPlan(
		t,
		`index=gradethis | makemv delim="," allowempty=true tags`,
	)
	compiled, err := (Compiler{}).CompileFieldSummary(
		logical,
		fieldSummaryTestSpec("tags"),
	)
	if err != nil {
		t.Fatalf("CompileFieldSummary(makemv): %v", err)
	}
	if !compiled.FieldKnown {
		t.Fatal("makemv output was not retained as an exact known field")
	}
	if !containsArgument(compiled.Args, uint8(eventfields.StoredValueTypeList)) {
		t.Fatalf("summary did not bind the semantic List type: %#v", compiled.Args)
	}

	storedType := quoteIdentifier(fieldSummaryStoredType)
	listCode := "toUInt8(10)"
	for _, fragment := range []string{
		storedType + " = " + listCode,
		storedType + " IN (" + listCode + ", toUInt8(11))",
		quoteIdentifier(fieldSummaryRowUnsupported),
	} {
		if !strings.Contains(compiled.SQL, fragment) {
			t.Errorf("summary SQL is missing typed container fragment %q", fragment)
		}
	}
	if strings.Contains(
		compiled.SQL,
		"toString("+quoteIdentifier(fieldSummaryRawValue)+")",
	) {
		t.Fatal("field summary silently scalarized Array(String) instead of classifying it as a container")
	}
}

func TestStringArrayFieldAnalysisTypeContract(t *testing.T) {
	t.Parallel()
	field := fieldState{kind: fieldKindStringArray, valueSQL: "array_value"}
	storedType, err := fixedFieldStoredType(field)
	if err != nil || storedType != eventfields.StoredValueTypeList {
		t.Fatalf("fixedFieldStoredType(StringArray) = (%d, %v), want List", storedType, err)
	}
	predicate := fieldSummaryFixedTypePredicate(field, "stored_type")
	if predicate != "stored_type = toUInt8(10)" {
		t.Fatalf("StringArray summary predicate = %q", predicate)
	}
	if encoding := fieldSummaryFixedEncoding(field, "stored_type", "array_value"); encoding != "CAST('' AS String)" {
		t.Fatalf("StringArray summary encoding = %q", encoding)
	}
}
