package clickhouse

import (
	"reflect"
	"strconv"
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
)

func TestCompileKnowledgeRegexExtractionProducesOneAtomicRowTuple(t *testing.T) {
	t.Parallel()

	operation := knowledgeRegexTestOperation(t,
		opensplunk.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
	)
	compiled, err := compileKnowledgeRegexExtraction(operation)
	if err != nil {
		t.Fatalf("compileKnowledgeRegexExtraction: %v", err)
	}
	if strings.Count(compiled.sql, "extractGroups(") != 1 {
		t.Fatalf("extractGroups occurrences = %d, want 1:\n%s", strings.Count(compiled.sql, "extractGroups("), compiled.sql)
	}
	for _, required := range []string{
		`tupleElement(__os_ko_regex_selector, 1) != 0`,
		`"__os_raw_encoding" = 1`,
		`isValidUTF8("_raw")`,
		`toUInt128(arraySum(value -> toUInt128(length(value)), __os_ko_regex_groups))`,
		`toUInt8(__os_ko_regex_matched)`,
		`arrayElement(__os_ko_regex_groups, 1)`,
		`arrayElement(__os_ko_regex_groups, 2)`,
	} {
		if !strings.Contains(compiled.sql, required) {
			t.Fatalf("compiled SQL omits %q:\n%s", required, compiled.sql)
		}
	}
	if strings.Count(compiled.sql, "notEmpty(__os_ko_regex_groups)") != 1 {
		t.Fatalf("capture presence is not shared atomically:\n%s", compiled.sql)
	}
	if strings.Count(compiled.sql, "?") != len(compiled.args) || len(compiled.args) < 2 ||
		compiled.args[0] != operation.Pattern() {
		t.Fatalf("arguments = %#v for SQL:\n%s", compiled.args, compiled.sql)
	}
	if compiled.programWorkUnits != operation.ProgramWorkUnits() ||
		compiled.operation.Overwrite() != knowledgeprogram.ReplaceExisting ||
		compiled.operation.Origin().ObjectID() != operation.Origin().ObjectID() {
		t.Fatalf("retained operation = %#v / work=%d", compiled.operation, compiled.programWorkUnits)
	}
	if len(compiled.captures) != 2 {
		t.Fatalf("capture metadata = %#v", compiled.captures)
	}
	for index, want := range []struct {
		name     string
		group    uint16
		location string
	}{
		{name: "first", group: 1, location: "field_extraction.regex.output_fields[0]"},
		{name: "second", group: 2, location: "field_extraction.regex.output_fields[1]"},
	} {
		capture := compiled.captures[index]
		if capture.name != want.name || capture.group != want.group ||
			capture.tupleElement != knowledgeRegexFirstCaptureElement+index ||
			capture.definitionLocation != want.location {
			t.Fatalf("capture %d = %#v, want %+v", index, capture, want)
		}
		result := `"compiled_regex"`
		if !strings.Contains(capture.presentSQL(result), strconv.Itoa(capture.tupleElement)) ||
			!strings.Contains(capture.valueSQL(result), strconv.Itoa(capture.tupleElement)) {
			t.Fatalf("capture %d accessors are not positional", index)
		}
	}
	result := `"compiled_regex"`
	if compiled.selectorInputBytesSQL(result) != `toUInt128(tupleElement("compiled_regex", 1))` ||
		compiled.selectorQueryUnitsSQL(result) != `toUInt128(tupleElement("compiled_regex", 2))` ||
		compiled.capturedBytesSQL(result) != `toUInt128(tupleElement("compiled_regex", 3))` {
		t.Fatal("charge accessors do not pin the row-tuple prefix")
	}
	for _, secret := range []string{
		operation.Origin().ObjectID(), operation.Origin().Name(), operation.Origin().OwnerID(),
	} {
		if strings.Contains(compiled.sql, secret) {
			t.Fatalf("compiled SQL leaked provenance %q", secret)
		}
	}
}

func TestCompileKnowledgeRegexExtractionRetainsOverwriteAndDetachedSelectorArgs(t *testing.T) {
	t.Parallel()

	operation := knowledgeRegexTestOperation(t,
		opensplunk.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
	)
	first, err := compileKnowledgeRegexExtraction(operation)
	if err != nil {
		t.Fatalf("compileKnowledgeRegexExtraction: %v", err)
	}
	if first.operation.Overwrite() != knowledgeprogram.PreserveExisting {
		t.Fatalf("overwrite = %v", first.operation.Overwrite())
	}
	var exact []string
	for _, argument := range first.args[1:] {
		if values, ok := argument.([]string); ok {
			exact = values
			break
		}
	}
	if len(exact) == 0 {
		t.Fatalf("selector exact-set argument is absent: %#v", first.args)
	}
	exact[0] = "mutated"
	second, err := compileKnowledgeRegexExtraction(operation)
	if err != nil {
		t.Fatalf("compileKnowledgeRegexExtraction(second): %v", err)
	}
	if reflect.DeepEqual(first.args, second.args) || strings.Contains(second.sql, "mutated") {
		t.Fatalf("compiled selector arguments alias across emissions: first=%#v second=%#v", first.args, second.args)
	}
	for _, argument := range second.args[1:] {
		if values, ok := argument.([]string); ok && len(values) > 0 && values[0] == "mutated" {
			t.Fatalf("fresh selector arguments retained caller mutation: %#v", second.args)
		}
	}
}

func TestCompileKnowledgeRegexExtractionRejectsDisagreeingAuthority(t *testing.T) {
	t.Parallel()

	operation := knowledgeRegexTestOperation(t,
		opensplunk.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
	)
	base := knowledgeRegexAuthorityForTest(operation)
	tests := []struct {
		name   string
		mutate func(*knowledgeRegexExtractionAuthority)
	}{
		{name: "zero provenance", mutate: func(value *knowledgeRegexExtractionAuthority) { value.origin = knowledgeprogram.Origin{} }},
		{name: "zero retained operation", mutate: func(value *knowledgeRegexExtractionAuthority) { value.operation = knowledgeprogram.RegexExtraction{} }},
		{name: "zero selector", mutate: func(value *knowledgeRegexExtractionAuthority) { value.selector = knowledgeprogram.Selector{} }},
		{name: "other input", mutate: func(value *knowledgeRegexExtractionAuthority) { value.input = "message" }},
		{name: "invalid overwrite", mutate: func(value *knowledgeRegexExtractionAuthority) { value.overwrite = knowledgeprogram.OverwriteInvalid }},
		{name: "noncanonical pattern", mutate: func(value *knowledgeRegexExtractionAuthority) {
			value.pattern = strings.TrimPrefix(value.pattern, "(?-s)")
		}},
		{name: "work disagreement", mutate: func(value *knowledgeRegexExtractionAuthority) { value.workUnits++ }},
		{name: "capture name disagreement", mutate: func(value *knowledgeRegexExtractionAuthority) { value.captures[0].name = "wrong" }},
		{name: "capture group disagreement", mutate: func(value *knowledgeRegexExtractionAuthority) { value.captures[0].group = 2 }},
		{name: "capture location disagreement", mutate: func(value *knowledgeRegexExtractionAuthority) {
			value.captures[0].definitionLocation = "field_extraction.regex.output_fields[9]"
		}},
		{name: "unnamed capture", mutate: func(value *knowledgeRegexExtractionAuthority) {
			value.pattern = `(?-s)(unnamed)(?P<first>[a-z]+)(?P<second>[0-9]+)`
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authority := base
			authority.captures = append([]knowledgeRegexCaptureAuthority(nil), base.captures...)
			test.mutate(&authority)
			if _, err := compileKnowledgeRegexExtractionAuthority(authority); err == nil {
				t.Fatal("disagreeing authority compiled")
			}
		})
	}
	if _, err := compileKnowledgeRegexExtraction(knowledgeprogram.RegexExtraction{}); err == nil {
		t.Fatal("zero operation compiled")
	}
}

func knowledgeRegexTestOperation(
	t *testing.T,
	overwrite opensplunk.KnowledgeOverwriteBehavior,
) knowledgeprogram.RegexExtraction {
	t.Helper()
	program := knowledgePreparationProgram(t, []*opensplunk.KnowledgeObjectDefinition{
		{
			AppId: "app", Name: "regex-object", SharingScope: opensplunk.SharingScope_SHARING_SCOPE_APP,
			Selector: &opensplunk.KnowledgeSelector{
				IndexPatterns: []*opensplunk.KnowledgeSelectorPattern{{Value: "main"}, {Value: "prod*"}},
				HostPatterns:  []*opensplunk.KnowledgeSelectorPattern{{Value: "api-1"}},
			},
			Body: &opensplunk.KnowledgeObjectDefinition_FieldExtraction{
				FieldExtraction: &opensplunk.FieldExtractionDefinition{
					InputField:        "_raw",
					OverwriteBehavior: overwrite,
					Extraction: &opensplunk.FieldExtractionDefinition_Regex{
						Regex: &opensplunk.RegexFieldExtractionDefinition{
							Pattern:      `(?P<first>[a-z]+)-(?P<second>[0-9]+)`,
							OutputFields: []string{"first", "second"},
						},
					},
				},
			},
		},
	})
	operations := program.RegexExtractions()
	if len(operations) != 1 {
		t.Fatalf("regex operations = %d", len(operations))
	}
	return operations[0]
}

func knowledgeRegexAuthorityForTest(
	operation knowledgeprogram.RegexExtraction,
) knowledgeRegexExtractionAuthority {
	return knowledgeRegexExtractionAuthorityFromOperation(operation)
}
