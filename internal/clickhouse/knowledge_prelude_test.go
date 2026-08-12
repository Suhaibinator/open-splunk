package clickhouse

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
)

func TestCompileKnowledgePreludeDistinguishesAbsentEmptyAndDerivesMixedProof(t *testing.T) {
	input := knowledgeExtractionStageState()
	if _, err := compileKnowledgePrelude(compileState{}, preparedKnowledgeCompilation{}); err != nil {
		t.Fatalf("legacy absent prelude changed zero-state behavior: %v", err)
	}
	absent, err := compileKnowledgePrelude(input, preparedKnowledgeCompilation{})
	if err != nil {
		t.Fatalf("compile absent prelude: %v", err)
	}
	if absent.present || absent.prefixLength != 0 || len(absent.stages) != 0 ||
		absent.selectorCharges != (compiledKnowledgeSelectorChargeColumns{}) ||
		absent.capturedBytes != "" {
		t.Fatalf("absent prelude = %#v", absent)
	}
	delete(absent.state.visible, "host")
	if _, ok := input.visible["host"]; !ok {
		t.Fatal("absent prelude state aliases its input map")
	}

	emptyProgram, err := knowledgeprogram.Prepare(knowledgeprogram.Input{})
	if err != nil {
		t.Fatalf("prepare empty program: %v", err)
	}
	empty, err := compileKnowledgePrelude(input, knowledgePreludePreparationForTest(emptyProgram))
	if err != nil {
		t.Fatalf("compile empty prelude: %v", err)
	}
	if !empty.present || empty.prefixLength != 0 || len(empty.stages) != 0 ||
		len(empty.proof.operatorKinds) != 0 || len(empty.proof.extractions) != 0 ||
		len(empty.proof.aliases) != 0 || len(empty.proof.calculated) != 0 ||
		empty.proof.objectCount != 0 || empty.proof.charges != (knowledgeprogram.Charges{}) ||
		empty.selectorCharges != (compiledKnowledgeSelectorChargeColumns{}) ||
		empty.capturedBytes != "" {
		t.Fatalf("empty prelude = %#v", empty)
	}
	if _, err := compileKnowledgePrelude(
		compileState{},
		knowledgePreludePreparationForTest(emptyProgram),
	); err == nil {
		t.Fatal("present empty prelude accepted a non-Scan input state")
	}

	program := knowledgePreludeProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{
		knowledgeJSONStageDefinition("a-json", "json_value", "payload.value", "east"),
		knowledgeRegexStageDefinition("b-regex", `(?P<regex_value>[a-z]+)`, []string{"regex_value"}, "west"),
		knowledgePreludeAliasDefinition("c-alias", "host", "alias_value"),
		knowledgePreludeCalculatedDefinition("d-calculated", "calculated_value", "lower(source)"),
	})
	compiled, err := compileKnowledgePrelude(input, knowledgePreludePreparationForTest(program))
	if err != nil {
		t.Fatalf("compile mixed prelude: %v", err)
	}
	if !compiled.present || compiled.prefixLength != 4 || len(compiled.stages) != 3 {
		t.Fatalf("mixed prelude shape = %#v", compiled)
	}
	wantStageKinds := []compiledKnowledgePreludeStageKind{
		compiledKnowledgePreludeStageExtraction,
		compiledKnowledgePreludeStageAlias,
		compiledKnowledgePreludeStageCalculated,
	}
	wantOffsets := []int{0, 2, 3}
	wantSpans := [][]knowledgeprogram.OperatorKind{
		{
			knowledgeprogram.OperatorConditionalExtractJSON,
			knowledgeprogram.OperatorConditionalExtract,
		},
		{knowledgeprogram.OperatorCopyFieldAlias},
		{knowledgeprogram.OperatorParallelExtend},
	}
	for index, stage := range compiled.stages {
		if stage.kind != wantStageKinds[index] || stage.operatorOffset != wantOffsets[index] ||
			!slices.Equal(stage.operatorKinds, wantSpans[index]) {
			t.Fatalf("stage %d = %#v", index, stage)
		}
		if strings.Count(strings.Join(stage.arrayJoinBindings, " "), "?") != len(stage.suffixArgs) {
			t.Fatalf("stage %d placeholder/argument mismatch", index)
		}
	}
	if compiled.proof.objectCount != program.ObjectCount() ||
		compiled.proof.charges != program.Charges() ||
		!slices.Equal(compiled.proof.operatorKinds, program.OperatorKinds()) ||
		len(compiled.proof.extractions) != 2 || len(compiled.proof.aliases) != 1 ||
		len(compiled.proof.calculated) != 1 {
		t.Fatalf("mixed lowering proof = %#v, program charges = %#v", compiled.proof, program.Charges())
	}
	if compiled.selectorCharges.inputBytes == "" || compiled.selectorCharges.queryUnits == "" ||
		compiled.aliasCopyCharges.eventBytes == "" || compiled.aliasCopyCharges.queryUnits == "" ||
		compiled.capturedBytes == "" || compiled.capturedBytes != compiled.state.rexCapturedBytesSQL {
		t.Fatalf("mixed accounting = %#v", compiled)
	}
	for _, output := range []string{"json_value", "regex_value", "alias_value", "calculated_value"} {
		if _, ok := compiled.state.visible[output]; !ok {
			t.Fatalf("final state omits %q", output)
		}
	}
	if slices.Contains(compiled.state.privateColumns, compiled.selectorCharges.inputBytes) ||
		slices.Contains(compiled.state.privateColumns, compiled.selectorCharges.queryUnits) ||
		slices.Contains(compiled.state.privateColumns, compiled.aliasCopyCharges.eventBytes) ||
		slices.Contains(compiled.state.privateColumns, compiled.aliasCopyCharges.queryUnits) {
		t.Fatalf("runtime charges entered private field metadata: %#v", compiled.state.privateColumns)
	}

	relation := "SELECT ? AS input_seed"
	args := []any{"input-first"}
	for _, stage := range compiled.stages {
		relation = "SELECT " + strings.Join(stage.projection, ", ") +
			" FROM (SELECT " + strings.Join(stage.bindingProjection, ", ") +
			" FROM (" + relation + ") ARRAY JOIN " +
			strings.Join(stage.arrayJoinBindings, ", ") + ")"
		args = append(args, stage.suffixArgs...)
	}
	if strings.Count(relation, "?") != len(args) || args[0] != "input-first" {
		t.Fatalf("composed placeholder/argument order = %#v", args)
	}

	fresh, err := compileKnowledgePrelude(input, knowledgePreludePreparationForTest(program))
	if err != nil {
		t.Fatalf("compile fresh mixed prelude: %v", err)
	}
	mutated := false
	for _, argument := range compiled.stages[0].suffixArgs {
		if values, ok := argument.([]string); ok && len(values) > 0 {
			values[0] = "mutated"
			mutated = true
			break
		}
	}
	if !mutated {
		t.Fatal("fixture has no mutable selector argument")
	}
	if reflect.DeepEqual(compiled.stages[0].suffixArgs, fresh.stages[0].suffixArgs) {
		t.Fatal("fresh prelude retained a prior stage-argument mutation")
	}
}

func TestCompileKnowledgePreludeSupportsEveryOptionalStageCombination(t *testing.T) {
	extraction := knowledgeJSONStageDefinition("a-json", "json_value", "payload.value", "east")
	alias := knowledgePreludeAliasDefinition("b-alias", "host", "alias_value")
	calculated := knowledgePreludeCalculatedDefinition("c-calculated", "calculated_value", "lower(source)")
	tests := []struct {
		name        string
		definitions []*opensplunkv1.KnowledgeObjectDefinition
		stageKinds  []compiledKnowledgePreludeStageKind
	}{
		{name: "extraction", definitions: []*opensplunkv1.KnowledgeObjectDefinition{extraction}, stageKinds: []compiledKnowledgePreludeStageKind{compiledKnowledgePreludeStageExtraction}},
		{name: "alias", definitions: []*opensplunkv1.KnowledgeObjectDefinition{alias}, stageKinds: []compiledKnowledgePreludeStageKind{compiledKnowledgePreludeStageAlias}},
		{name: "calculated", definitions: []*opensplunkv1.KnowledgeObjectDefinition{calculated}, stageKinds: []compiledKnowledgePreludeStageKind{compiledKnowledgePreludeStageCalculated}},
		{name: "extraction alias", definitions: []*opensplunkv1.KnowledgeObjectDefinition{extraction, alias}, stageKinds: []compiledKnowledgePreludeStageKind{compiledKnowledgePreludeStageExtraction, compiledKnowledgePreludeStageAlias}},
		{name: "extraction calculated", definitions: []*opensplunkv1.KnowledgeObjectDefinition{extraction, calculated}, stageKinds: []compiledKnowledgePreludeStageKind{compiledKnowledgePreludeStageExtraction, compiledKnowledgePreludeStageCalculated}},
		{name: "alias calculated", definitions: []*opensplunkv1.KnowledgeObjectDefinition{alias, calculated}, stageKinds: []compiledKnowledgePreludeStageKind{compiledKnowledgePreludeStageAlias, compiledKnowledgePreludeStageCalculated}},
		{name: "all", definitions: []*opensplunkv1.KnowledgeObjectDefinition{extraction, alias, calculated}, stageKinds: []compiledKnowledgePreludeStageKind{compiledKnowledgePreludeStageExtraction, compiledKnowledgePreludeStageAlias, compiledKnowledgePreludeStageCalculated}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			program := knowledgePreludeProgram(t, test.definitions)
			compiled, err := compileKnowledgePrelude(
				knowledgeExtractionStageState(),
				knowledgePreludePreparationForTest(program),
			)
			if err != nil {
				t.Fatalf("compileKnowledgePrelude: %v", err)
			}
			gotKinds := make([]compiledKnowledgePreludeStageKind, len(compiled.stages))
			for index, stage := range compiled.stages {
				gotKinds[index] = stage.kind
			}
			if !slices.Equal(gotKinds, test.stageKinds) ||
				compiled.proof.charges != program.Charges() ||
				compiled.proof.objectCount != program.ObjectCount() ||
				(len(program.Aliases()) != 0) !=
					(compiled.aliasCopyCharges != (compiledKnowledgeAliasCopyChargeColumns{})) {
				t.Fatalf("compiled optional stages = %#v", compiled)
			}
		})
	}
}

func TestCompileKnowledgePreludeRejectsForgedPreparationAndPriorWork(t *testing.T) {
	program := knowledgePreludeProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{
		knowledgeJSONStageDefinition("json", "json_value", "payload.value", "east"),
	})
	base := knowledgePreludePreparationForTest(program)
	tests := []struct {
		name   string
		mutate func(*preparedKnowledgeCompilation)
	}{
		{name: "missing present marker", mutate: func(value *preparedKnowledgeCompilation) { value.present = false }},
		{name: "wrong prefix", mutate: func(value *preparedKnowledgeCompilation) { value.prefixLength++ }},
		{name: "wrong kind", mutate: func(value *preparedKnowledgeCompilation) {
			value.operatorKinds[0] = knowledgeprogram.OperatorParallelExtend
		}},
		{name: "wrong charges", mutate: func(value *preparedKnowledgeCompilation) { value.programCharges.GeneratedFields++ }},
		{name: "wrong commitment", mutate: func(value *preparedKnowledgeCompilation) { value.programCommitment[0] ^= 0xff }},
		{name: "zero program", mutate: func(value *preparedKnowledgeCompilation) { value.program = knowledgeprogram.Program{} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := base
			candidate.operatorKinds = slices.Clone(base.operatorKinds)
			test.mutate(&candidate)
			if _, err := compileKnowledgePrelude(knowledgeExtractionStageState(), candidate); err == nil {
				t.Fatal("forged preparation compiled")
			}
		})
	}

	state := knowledgeExtractionStageState()
	state.rexCapturedBytesSQL = quoteIdentifier("__os_unproved_capture")
	if _, err := compileKnowledgePrelude(state, base); err == nil {
		t.Fatal("prelude with prior capture work compiled")
	}
	malformedAbsent := preparedKnowledgeCompilation{prefixLength: 1}
	if _, err := compileKnowledgePrelude(knowledgeExtractionStageState(), malformedAbsent); err == nil {
		t.Fatal("malformed absent preparation compiled")
	}
}

func knowledgePreludePreparationForTest(
	program knowledgeprogram.Program,
) preparedKnowledgeCompilation {
	commitment, ok := program.Commitment()
	if !ok {
		return preparedKnowledgeCompilation{}
	}
	kinds := program.OperatorKinds()
	return preparedKnowledgeCompilation{
		present:           true,
		prefixLength:      len(kinds),
		operatorKinds:     slices.Clone(kinds),
		program:           program.Clone(),
		programCharges:    program.Charges(),
		programCommitment: commitment,
	}
}

func knowledgePreludeProgram(
	t *testing.T,
	definitions []*opensplunkv1.KnowledgeObjectDefinition,
) knowledgeprogram.Program {
	t.Helper()
	objects := make([]*opensplunkv1.KnowledgeSnapshotObject, len(definitions))
	stageOrdinals := make(map[opensplunkv1.KnowledgeSearchStage]uint32)
	for index, definition := range definitions {
		normalized, err := knowledgedefinition.Normalize(definition)
		if err != nil {
			t.Fatalf("Normalize(%d): %v", index, err)
		}
		var stage opensplunkv1.KnowledgeSearchStage
		switch normalized.ObjectType {
		case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION:
			stage = opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION
		case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS:
			stage = opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS
		case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD:
			stage = opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD
		default:
			t.Fatalf("unexpected object type %v", normalized.ObjectType)
		}
		objects[index] = &opensplunkv1.KnowledgeSnapshotObject{
			ResolutionOrdinal: uint32(index),
			Stage:             stage,
			StageOrdinal:      stageOrdinals[stage],
			KnowledgeObjectId: "prelude-" + normalized.Name,
			Version:           1,
			ObjectType:        normalized.ObjectType,
			Name:              normalized.Name,
			AppId:             normalized.AppID,
			OwnerId:           "owner",
			SharingScope:      normalized.SharingScope,
			Definition:        normalized.Definition,
			DefinitionSha256:  slices.Clone(normalized.Digest[:]),
		}
		stageOrdinals[stage]++
	}
	program, err := knowledgeprogram.Prepare(knowledgeprogram.Input{Objects: objects})
	if err != nil {
		t.Fatalf("knowledgeprogram.Prepare: %v", err)
	}
	return program
}

func knowledgePreludeAliasDefinition(
	name, source, destination string,
) *opensplunkv1.KnowledgeObjectDefinition {
	return &opensplunkv1.KnowledgeObjectDefinition{
		AppId: "app", Name: name, SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{
			FieldAlias: &opensplunkv1.FieldAliasDefinition{
				SourceField: source, DestinationField: destination,
				OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
			},
		},
	}
}

func knowledgePreludeCalculatedDefinition(
	name, destination, expression string,
) *opensplunkv1.KnowledgeObjectDefinition {
	return &opensplunkv1.KnowledgeObjectDefinition{
		AppId: "app", Name: name, SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Body: &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{
			CalculatedField: &opensplunkv1.CalculatedFieldDefinition{
				DestinationField: destination, Expression: expression,
				OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
			},
		},
	}
}
