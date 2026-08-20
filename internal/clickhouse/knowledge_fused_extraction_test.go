package clickhouse

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
)

func TestCompileKnowledgeExtractionStagePreservesOrderFrozenBindingsAndCharges(t *testing.T) {
	program := knowledgePreparationProgram(t, []*opensplunk.KnowledgeObjectDefinition{
		knowledgeJSONStageDefinition("a-json", "json_a", "payload.a", "east"),
		knowledgeRegexStageDefinition("b-regex", `(?P<first>[a-z]+)-(?P<second>[0-9]+)`, []string{"first", "second"}, "west"),
		knowledgeJSONStageDefinition("c-json", "json_c", "payload.c", "north"),
	})
	state := knowledgeExtractionStageState()
	state.rexCapturedBytesSQL = quoteIdentifier("__os_prior_capture_bytes")
	prior := compiledKnowledgeSelectorChargeColumns{
		inputBytes: quoteIdentifier("__os_prior_selector_input"),
		queryUnits: quoteIdentifier("__os_prior_selector_query"),
	}
	compiled, err := compileKnowledgeExtractionStage(state, program, 12, prior)
	if err != nil {
		t.Fatalf("compileKnowledgeExtractionStage: %v", err)
	}
	if compiled.emittedOperatorCount != 3 || compiled.emittedOutputCount != 4 ||
		compiled.emittedRegexPrograms != 1 || len(compiled.arrayJoinBindings) != 7 {
		t.Fatalf("emission proof = %#v", compiled)
	}
	charges := program.Charges()
	if compiled.emittedOperatorCount != charges.GeneratedOperators ||
		compiled.emittedOutputCount != charges.ExtractionOutputs ||
		compiled.emittedRegexPrograms != charges.RegexPrograms ||
		compiled.emittedRegexWorkUnits != charges.RegexWorkUnits ||
		compiled.emittedJSONEvaluationWork != charges.JSONEvaluationWork {
		t.Fatalf("emission charges = %#v, program = %#v", compiled, charges)
	}
	wantKinds := []knowledgeprogram.OperatorKind{
		knowledgeprogram.OperatorConditionalExtractJSON,
		knowledgeprogram.OperatorConditionalExtract,
		knowledgeprogram.OperatorConditionalExtractJSON,
	}
	gotKinds := make([]knowledgeprogram.OperatorKind, len(compiled.emittedOperations))
	for index, operation := range compiled.emittedOperations {
		gotKinds[index] = operation.kind
	}
	if !slices.Equal(gotKinds, wantKinds) {
		t.Fatalf("emitted order = %v, want %v", gotKinds, wantKinds)
	}
	bindings := strings.Join(compiled.arrayJoinBindings, " ")
	if strings.Count(bindings, "extractGroups(") != 1 ||
		strings.Count(bindings, `AS "__os_ko_extract_binding_12_`) != 3 ||
		strings.Count(bindings, `AS "__os_ko_extract_previous_authority_12_`) != 4 {
		t.Fatalf("object bindings are not singleton and exact:\n%s", bindings)
	}
	inner := strings.Join(compiled.bindingProjection, " ")
	for _, destination := range []string{`AS "json_a"`, `AS "first"`, `AS "second"`, `AS "json_c"`} {
		if strings.Contains(inner, destination) {
			t.Fatalf("frozen binding layer publishes %s:\n%s", destination, inner)
		}
	}
	projection := strings.Join(compiled.projection, " ")
	for _, required := range []string{
		"toUInt128(" + prior.inputBytes + ")",
		"toUInt128(" + prior.queryUnits + ")",
		"toUInt128(" + state.rexCapturedBytesSQL + ")",
		compiled.selectorCharges.inputBytes,
		compiled.selectorCharges.queryUnits,
		compiled.capturedBytes,
	} {
		if !strings.Contains(projection, required) {
			t.Fatalf("projection omits %q:\n%s", required, projection)
		}
	}
	if compiled.state.rexCapturedBytesSQL != compiled.capturedBytes ||
		slices.Contains(compiled.state.privateColumns, compiled.selectorCharges.inputBytes) ||
		slices.Contains(compiled.state.privateColumns, compiled.selectorCharges.queryUnits) {
		t.Fatalf("successor accounting state = %#v", compiled.state)
	}
	if got := strings.Count(bindings, "?"); got != len(compiled.suffixArgs) {
		t.Fatalf("binding placeholders = %d, suffix args = %d", got, len(compiled.suffixArgs))
	}
	objects, err := compileKnowledgeExtractionObjects(program, 12)
	if err != nil {
		t.Fatalf("compile extraction objects: %v", err)
	}
	wantArgs := make([]any, 0, len(compiled.suffixArgs))
	for _, object := range objects {
		wantArgs = append(wantArgs, object.args...)
	}
	for index, destination := range []string{"json_a", "first", "second", "json_c"} {
		previous, _, previousErr := compileKnowledgeExtractionPrevious(
			destination,
			state,
			12,
			index,
		)
		if previousErr != nil {
			t.Fatalf("compile previous %q: %v", destination, previousErr)
		}
		wantArgs = append(wantArgs, previous.args...)
	}
	if !reflect.DeepEqual(compiled.suffixArgs, wantArgs) {
		t.Fatalf("suffix argument partition = %#v, want %#v", compiled.suffixArgs, wantArgs)
	}
	composedSQL := "SELECT " + projection + " FROM (SELECT " + inner +
		" FROM (SELECT ? AS host) ARRAY JOIN " + strings.Join(compiled.arrayJoinBindings, ", ") + ")"
	composedArgs := append([]any{"input-first"}, compiled.suffixArgs...)
	if strings.Count(composedSQL, "?") != len(composedArgs) || composedArgs[0] != "input-first" {
		t.Fatalf("composed argument order = %#v", composedArgs)
	}
}

func TestCompileKnowledgeExtractionStageGroupsDisjointDestinations(t *testing.T) {
	program := knowledgePreparationProgram(t, []*opensplunk.KnowledgeObjectDefinition{
		knowledgeJSONStageDefinition("a-east", "shared", "payload.value", "east"),
		knowledgeRegexStageDefinition("b-west", `(?P<shared>[a-z]+)`, []string{"shared"}, "west"),
	})
	compiled, err := compileKnowledgeExtractionStage(
		knowledgeExtractionStageState(), program, 4, compiledKnowledgeSelectorChargeColumns{},
	)
	if err != nil {
		t.Fatalf("compileKnowledgeExtractionStage: %v", err)
	}
	projection := strings.Join(compiled.projection, " ")
	if strings.Count(projection, ` AS "shared"`) != 1 ||
		!strings.Contains(projection, `"__os_ko_extract_binding_4_0"`) ||
		!strings.Contains(projection, `"__os_ko_extract_binding_4_1"`) {
		t.Fatalf("disjoint destination was not grouped:\n%s", projection)
	}
	bindings := strings.Join(compiled.arrayJoinBindings, " ")
	if len(compiled.arrayJoinBindings) != 3 ||
		strings.Count(bindings, `AS "__os_ko_extract_previous_authority_4_0"`) != 1 {
		t.Fatalf("shared prior destination was not bound exactly once:\n%s", bindings)
	}
	if !strings.Contains(projection, `"__os_ko_extract_previous_authority_4_0"`) ||
		!strings.Contains(projection, ` = 0`) {
		t.Fatalf("preserve policy does not use immutable prior presence:\n%s", projection)
	}
}

func TestCompileKnowledgeExtractionStageCarriesPriorCaptureAndRebasesFrozenFields(t *testing.T) {
	program := knowledgePreparationProgram(t, []*opensplunk.KnowledgeObjectDefinition{
		knowledgeJSONStageDefinition("json-only", "json_value", "payload.value", "east"),
	})
	state := knowledgeExtractionStageState()
	state.rexCapturedBytesSQL = quoteIdentifier("__os_prior_capture_bytes")
	state.visible["derived"] = fieldState{
		valueSQL:       quoteIdentifier("__os_prior_derived"),
		existsSQL:      "1",
		kind:           fieldKindString,
		maxStringBytes: 64,
	}
	state.publicOrder = append(state.publicOrder, "derived")
	compiled, err := compileKnowledgeExtractionStage(
		state,
		program,
		6,
		compiledKnowledgeSelectorChargeColumns{},
	)
	if err != nil {
		t.Fatalf("compileKnowledgeExtractionStage: %v", err)
	}
	projection := strings.Join(compiled.projection, " ")
	if compiled.capturedBytes != state.rexCapturedBytesSQL ||
		compiled.state.rexCapturedBytesSQL != state.rexCapturedBytesSQL ||
		!strings.Contains(projection, state.rexCapturedBytesSQL) {
		t.Fatalf("JSON-only stage dropped prior capture authority: %#v\n%s", compiled, projection)
	}
	if !strings.Contains(strings.Join(compiled.bindingProjection, " "),
		quoteIdentifier("__os_prior_derived")+" AS "+quoteIdentifier("derived")) ||
		!strings.Contains(projection, quoteIdentifier("derived")) ||
		strings.Contains(projection, quoteIdentifier("__os_prior_derived")) {
		t.Fatalf("outer projection did not consume the rebased inner alias:\n%s", projection)
	}
}

func TestCompileKnowledgeExtractionStageRejectsNonEventInput(t *testing.T) {
	program := knowledgePreparationProgram(t, []*opensplunk.KnowledgeObjectDefinition{
		knowledgeJSONStageDefinition("json", "value", "payload.value", "east"),
	})
	for _, test := range []struct {
		name   string
		mutate func(*compileState)
	}{
		{name: "not event rows", mutate: func(state *compileState) { state.eventRows = false }},
		{name: "dynamic fields unavailable", mutate: func(state *compileState) { state.allowDynamic = false }},
		{name: "blocked field", mutate: func(state *compileState) { state.blocked["value"] = struct{}{} }},
		{name: "missing raw", mutate: func(state *compileState) { delete(state.visible, "_raw") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := knowledgeExtractionStageState()
			test.mutate(&state)
			if _, err := compileKnowledgeExtractionStage(
				state,
				program,
				1,
				compiledKnowledgeSelectorChargeColumns{},
			); err == nil {
				t.Fatal("incomplete event input compiled")
			}
		})
	}
}

func knowledgeExtractionStageState() compileState {
	state := knowledgeFusedFieldState()
	state.visible["_raw"] = canonicalState("_raw")
	state.publicOrder = append([]string{"_raw"}, state.publicOrder...)
	return state
}

func knowledgeJSONStageDefinition(name, output, path, index string) *opensplunk.KnowledgeObjectDefinition {
	return &opensplunk.KnowledgeObjectDefinition{
		AppId: "app", Name: name, SharingScope: opensplunk.SharingScope_SHARING_SCOPE_APP,
		Selector: &opensplunk.KnowledgeSelector{IndexPatterns: []*opensplunk.KnowledgeSelectorPattern{{Value: index}}},
		Body: &opensplunk.KnowledgeObjectDefinition_FieldExtraction{FieldExtraction: &opensplunk.FieldExtractionDefinition{
			InputField: "_raw", OverwriteBehavior: opensplunk.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
			Extraction: &opensplunk.FieldExtractionDefinition_Json{Json: &opensplunk.JsonFieldExtractionDefinition{Path: path, OutputField: output}},
		}},
	}
}

func knowledgeRegexStageDefinition(name, pattern string, outputs []string, index string) *opensplunk.KnowledgeObjectDefinition {
	return &opensplunk.KnowledgeObjectDefinition{
		AppId: "app", Name: name, SharingScope: opensplunk.SharingScope_SHARING_SCOPE_APP,
		Selector: &opensplunk.KnowledgeSelector{IndexPatterns: []*opensplunk.KnowledgeSelectorPattern{{Value: index}}},
		Body: &opensplunk.KnowledgeObjectDefinition_FieldExtraction{FieldExtraction: &opensplunk.FieldExtractionDefinition{
			InputField: "_raw", OverwriteBehavior: opensplunk.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
			Extraction: &opensplunk.FieldExtractionDefinition_Regex{Regex: &opensplunk.RegexFieldExtractionDefinition{Pattern: pattern, OutputFields: outputs}},
		}},
	}
}
