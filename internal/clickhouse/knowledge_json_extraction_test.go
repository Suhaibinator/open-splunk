package clickhouse

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/splpath"
)

func TestCompileKnowledgeJSONExtractionPinsGatingScalarDomainAndArguments(t *testing.T) {
	t.Parallel()

	extraction := knowledgeJSONExtractionFixture(
		t,
		knowledgeprogram.ReplaceExisting,
		&opensplunk.KnowledgeSelector{
			IndexPatterns: []*opensplunk.KnowledgeSelectorPattern{
				{Value: "prod*"},
				{Value: "audit"},
			},
		},
	)
	compiled, err := compileKnowledgeJSONExtraction(extraction)
	if err != nil {
		t.Fatalf("compileKnowledgeJSONExtraction: %v", err)
	}

	steps := extraction.Steps()
	if compiled.operation.Output() != "knowledge_value" ||
		compiled.operation.Overwrite() != knowledgeprogram.ReplaceExisting ||
		compiled.evaluationWorkUnits != uint32(splpath.EvaluationWorkUnits(steps)) {
		t.Fatalf("compiled metadata = %#v", compiled)
	}
	result := quoteIdentifier("compiled_json")
	if compiled.producedSQL(result) != `toUInt8(tupleElement("compiled_json", 1))` ||
		compiled.valueSQL(result) != `tupleElement("compiled_json", 2)` ||
		compiled.selectorInputBytesSQL(result) != `toUInt128(tupleElement("compiled_json", 3))` ||
		compiled.selectorQueryUnitsSQL(result) != `toUInt128(tupleElement("compiled_json", 4))` {
		t.Fatal("knowledge JSON tuple accessors do not pin the closed positional contract")
	}
	for _, required := range []string{
		"if(tupleElement(__os_ko_json_selector, 1) != 0",
		`isNotNull("_raw") AND "__os_raw_encoding" = 1 AND isValidUTF8("_raw")`,
		`assumeNotNull("_raw")`,
		"tuple(toUInt8(tupleElement(__os_ko_json_candidate, 1)), tupleElement(__os_ko_json_candidate, 2)",
		"toUInt128(tupleElement(__os_ko_json_selector, 2))",
		"toUInt128(tupleElement(__os_ko_json_selector, 3))",
		"notEmpty(__os_ko_json_raw) AND (__os_ko_json_number_selected != 0 OR __os_ko_json_raw IN ('null', 'true', 'false') OR startsWith(__os_ko_json_raw, char(34)))",
		SpathInputLimitMarker,
		SpathJSONTokenLimitMarker,
	} {
		if !strings.Contains(compiled.sql, required) {
			t.Fatalf("knowledge JSON SQL omits %q:\n%s", required, compiled.sql)
		}
	}
	if strings.Contains(compiled.sql, UnsupportedSpathValueMarker) {
		t.Fatalf("knowledge JSON container path retained authored spath throw:\n%s", compiled.sql)
	}
	if got := strings.Count(compiled.sql, "?"); got != len(compiled.args) {
		t.Fatalf("placeholder count = %d, args = %d", got, len(compiled.args))
	}
	for function, want := range map[string]int{
		"JSONExtractRaw(":    1,
		"JSONExtractString(": 1,
		"JSONExtract(":       1,
		"JSONType(":          1,
	} {
		if got := strings.Count(compiled.sql, function); got != want {
			t.Fatalf("%s occurrences = %d, want %d", function, got, want)
		}
	}

	runtime, ok := extraction.Selector().RuntimeProgram(knowledge.DimensionIndex)
	if !ok || runtime.WildcardRE2 == "" || len(runtime.ExactLiterals) != 1 {
		t.Fatalf("selector runtime program = %#v / %v", runtime, ok)
	}
	wantArgs := []any{
		"payload", "items", int64(3), "value",
		"payload", "items", int64(3), "value",
		"payload", "items",
		spathJSONNumberPattern,
		spathJSONTokenPattern,
		spathJSONTokenPattern,
		runtime.WildcardRE2,
		runtime.ExactLiterals,
	}
	if !reflect.DeepEqual(compiled.args, wantArgs) {
		t.Fatalf("knowledge JSON args = %#v, want %#v", compiled.args, wantArgs)
	}

	again, err := compileKnowledgeJSONExtraction(extraction)
	if err != nil {
		t.Fatalf("compileKnowledgeJSONExtraction(again): %v", err)
	}
	compiled.args[len(compiled.args)-1].([]string)[0] = "mutated"
	if !slices.Equal(again.args[len(again.args)-1].([]string), []string{"audit"}) {
		t.Fatal("knowledge JSON selector arguments alias a prior compilation")
	}
}

func TestCompileKnowledgeJSONExtractionMaximalPathFitsPerObjectSQLBound(t *testing.T) {
	t.Parallel()

	path := maximalKnowledgeJSONPath(t)
	extraction := knowledgeJSONExtractionFixturePath(
		t,
		knowledgeprogram.ReplaceExisting,
		nil,
		path,
	)
	compiled, err := compileKnowledgeJSONExtraction(extraction)
	if err != nil {
		t.Fatalf("compile maximal knowledge JSON path: %v", err)
	}
	if len(compiled.sql) > maxCompiledKnowledgeJSONExtractionSQLBytes {
		t.Fatalf(
			"maximal path SQL bytes = %d, exceeds %d",
			len(compiled.sql),
			maxCompiledKnowledgeJSONExtractionSQLBytes,
		)
	}
	if got := strings.Count(compiled.sql, "?"); got != len(compiled.args) {
		t.Fatalf("maximal path placeholder count = %d, args = %d", got, len(compiled.args))
	}
}

func TestCompileKnowledgeJSONExtractionCarriesButDoesNotApplyOverwrite(t *testing.T) {
	t.Parallel()

	preserve := knowledgeJSONExtractionFixture(t, knowledgeprogram.PreserveExisting, nil)
	replace := knowledgeJSONExtractionFixture(t, knowledgeprogram.ReplaceExisting, nil)
	preserved, err := compileKnowledgeJSONExtraction(preserve)
	if err != nil {
		t.Fatalf("compile preserve: %v", err)
	}
	replaced, err := compileKnowledgeJSONExtraction(replace)
	if err != nil {
		t.Fatalf("compile replace: %v", err)
	}
	if preserved.operation.Overwrite() != knowledgeprogram.PreserveExisting ||
		replaced.operation.Overwrite() != knowledgeprogram.ReplaceExisting {
		t.Fatalf("overwrite metadata = %v / %v", preserved.operation.Overwrite(), replaced.operation.Overwrite())
	}
	if preserved.operation.Output() != replaced.operation.Output() || preserved.sql != replaced.sql ||
		!reflect.DeepEqual(preserved.args, replaced.args) {
		t.Fatal("row-local extraction changed when only destination merge authority changed")
	}
	if strings.Contains(preserved.sql, "destination") || strings.Contains(preserved.sql, "existing") {
		t.Fatalf("row-local extraction opened a destination merge:\n%s", preserved.sql)
	}
}

func TestCompileKnowledgeJSONExtractionRejectsDisagreeingAuthority(t *testing.T) {
	t.Parallel()

	valid := knowledgeJSONExtractionFixture(t, knowledgeprogram.PreserveExisting, nil)
	base := knowledgeJSONExtractionAuthorityFromOperation(valid)
	tests := []struct {
		name   string
		mutate func(*knowledgeJSONExtractionAuthority)
	}{
		{name: "zero provenance", mutate: func(value *knowledgeJSONExtractionAuthority) { value.origin = knowledgeprogram.Origin{} }},
		{name: "zero retained operation", mutate: func(value *knowledgeJSONExtractionAuthority) { value.operation = knowledgeprogram.JSONExtraction{} }},
		{name: "zero selector", mutate: func(value *knowledgeJSONExtractionAuthority) { value.selector = knowledgeprogram.Selector{} }},
		{name: "other input", mutate: func(value *knowledgeJSONExtractionAuthority) { value.input = "message" }},
		{name: "invalid overwrite", mutate: func(value *knowledgeJSONExtractionAuthority) { value.overwrite = knowledgeprogram.OverwriteInvalid }},
		{name: "path disagreement", mutate: func(value *knowledgeJSONExtractionAuthority) { value.path = "payload.other" }},
		{name: "step disagreement", mutate: func(value *knowledgeJSONExtractionAuthority) { value.steps[0].Key = "other" }},
		{name: "output disagreement", mutate: func(value *knowledgeJSONExtractionAuthority) { value.output = "other" }},
		{name: "output location disagreement", mutate: func(value *knowledgeJSONExtractionAuthority) {
			value.outputLocation = "field_extraction.json.output_field[1]"
		}},
		{name: "work disagreement", mutate: func(value *knowledgeJSONExtractionAuthority) { value.workUnits++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authority := base
			authority.steps = slices.Clone(base.steps)
			test.mutate(&authority)
			if _, err := compileKnowledgeJSONExtractionAuthority(authority); err == nil {
				t.Fatal("disagreeing knowledge JSON authority compiled")
			}
		})
	}
	if _, err := compileKnowledgeJSONExtraction(knowledgeprogram.JSONExtraction{}); err == nil {
		t.Fatal("zero operation compiled")
	}
}

func knowledgeJSONExtractionFixture(
	t *testing.T,
	overwrite knowledgeprogram.OverwriteBehavior,
	selector *opensplunk.KnowledgeSelector,
) knowledgeprogram.JSONExtraction {
	t.Helper()
	return knowledgeJSONExtractionFixturePath(
		t,
		overwrite,
		selector,
		"payload.items{2}.value",
	)
}

func knowledgeJSONExtractionFixturePath(
	t *testing.T,
	overwrite knowledgeprogram.OverwriteBehavior,
	selector *opensplunk.KnowledgeSelector,
	path string,
) knowledgeprogram.JSONExtraction {
	t.Helper()
	protobufOverwrite := opensplunk.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING
	if overwrite == knowledgeprogram.ReplaceExisting {
		protobufOverwrite = opensplunk.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING
	}
	program := knowledgePreparationProgram(t, []*opensplunk.KnowledgeObjectDefinition{
		{
			AppId: "app", Name: "json-fixture", SharingScope: opensplunk.SharingScope_SHARING_SCOPE_APP,
			Selector: selector,
			Body: &opensplunk.KnowledgeObjectDefinition_FieldExtraction{
				FieldExtraction: &opensplunk.FieldExtractionDefinition{
					InputField:        "_raw",
					OverwriteBehavior: protobufOverwrite,
					Extraction: &opensplunk.FieldExtractionDefinition_Json{
						Json: &opensplunk.JsonFieldExtractionDefinition{
							Path: path, OutputField: "knowledge_value",
						},
					},
				},
			},
		},
	})
	extractions := program.JSONExtractions()
	if len(extractions) != 1 {
		t.Fatalf("JSON extraction count = %d, want 1", len(extractions))
	}
	return extractions[0]
}

func maximalKnowledgeJSONPath(t *testing.T) string {
	t.Helper()
	const (
		longKeyBytes  = 240
		shortKeyBytes = 216
	)
	steps := make([]string, splpath.MaximumPathSteps)
	for index := range steps {
		keyBytes := longKeyBytes
		if index >= splpath.MaximumPathSteps-2 {
			keyBytes = shortKeyBytes
		}
		steps[index] = strings.Repeat("k", keyBytes)
		if index >= splpath.MaximumPathSteps-splpath.MaximumArraySelectors {
			steps[index] += "{2147483646}"
		}
	}
	path := strings.Join(steps, ".")
	if len(path) != splpath.MaximumPathBytes {
		t.Fatalf("maximal path bytes = %d, want %d", len(path), splpath.MaximumPathBytes)
	}
	parsed, err := splpath.ParseJSON(path)
	if err != nil {
		t.Fatalf("ParseJSON(maximal path): %v", err)
	}
	arraySelectors := 0
	for _, step := range parsed {
		if step.Selector == splpath.ArraySelectorFixed {
			arraySelectors++
		}
	}
	if len(parsed) != splpath.MaximumPathSteps || arraySelectors != splpath.MaximumArraySelectors {
		t.Fatalf("maximal path shape = %d steps / %d array selectors", len(parsed), arraySelectors)
	}
	return path
}
