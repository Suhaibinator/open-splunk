package knowledgeprogram

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"google.golang.org/protobuf/proto"
)

func TestPrepareValidEmptyIsDistinctFromAbsent(t *testing.T) {
	program, err := Prepare(Input{})
	if err != nil {
		t.Fatalf("Prepare(empty): %v", err)
	}
	if program.IsZero() {
		t.Fatal("Prepare(empty) returned absent program")
	}
	commitment, ok := program.Commitment()
	if !ok || commitment == ([32]byte{}) {
		t.Fatalf("empty commitment = %x/%t", commitment, ok)
	}
	if (Program{}).Equal(program) || !program.Equal(program.Clone()) {
		t.Fatal("absent/empty equality is invalid")
	}
	if charges := program.Charges(); charges != (Charges{}) {
		t.Fatalf("empty charges = %+v", charges)
	}
	if program.RetainedBytes() == 0 || program.RetainedBytes() != program.Clone().RetainedBytes() ||
		(Program{}).RetainedBytes() != 0 {
		t.Fatalf("empty retained bytes = %d", program.RetainedBytes())
	}
}

func TestPrepareBuildsCanonicalTypedProgramAndDetaches(t *testing.T) {
	input := mixedProgramInput(t)
	program, err := Prepare(input)
	if err != nil {
		t.Fatalf("Prepare(mixed): %v", err)
	}
	charges := program.Charges()
	if charges.GeneratedOperators != 4 || charges.GeneratedFields != 5 ||
		charges.RegexPrograms != 1 || charges.ExtractionOutputs != 3 ||
		charges.JSONEvaluationWork == 0 || charges.ScalarExpressions != 1 ||
		charges.ScalarExpressionNodes == 0 {
		t.Fatalf("charges = %+v", charges)
	}
	regex := program.RegexExtractions()
	json := program.JSONExtractions()
	aliases := program.Aliases()
	calculated := program.CalculatedFields()
	if len(regex) != 1 || len(regex[0].Captures()) != 2 || regex[0].Input() != "_raw" ||
		len(json) != 1 || json[0].Output() != "json_value" || len(json[0].Steps()) != 2 ||
		len(aliases) != 1 || aliases[0].Source() != "source_field" ||
		len(calculated) != 1 || calculated[0].Destination() != "calculated_value" {
		t.Fatalf("typed program = regex=%#v json=%#v aliases=%#v calculated=%#v", regex, json, aliases, calculated)
	}
	if regex[0].Origin().ResolutionOrdinal() != 0 || calculated[0].Origin().StageOrdinal() != 0 ||
		regex[0].Origin().DefinitionLocation() != "field_extraction.regex.pattern" ||
		regex[0].Captures()[0].DefinitionLocation() != "field_extraction.regex.output_fields[0]" ||
		json[0].OutputDefinitionLocation() != "field_extraction.json.output_field" {
		t.Fatalf("origins = regex=%+v calculated=%+v", regex[0].Origin(), calculated[0].Origin())
	}
	if _, ok := regex[0].Selector().RuntimeProgram(knowledge.DimensionIndex); !ok {
		t.Fatal("index selector program is absent")
	}

	want, _ := program.Commitment()
	input.Objects[0].KnowledgeObjectId = "mutated"
	input.Objects[0].Definition.GetFieldExtraction().GetRegex().OutputFields[0] = "mutated"
	regex[0].captures[0].name = "mutated"
	json[0].steps[0].Key = "mutated"
	calculated[0].inputFields[0] = "mutated"
	got, _ := program.Commitment()
	if got != want || program.RegexExtractions()[0].Captures()[0].Name() != "first" ||
		program.JSONExtractions()[0].Steps()[0].Key != "payload" ||
		program.CalculatedFields()[0].InputFields()[0] == "mutated" {
		t.Fatal("program aliases caller or accessor mutation")
	}
	empty, err := Prepare(Input{})
	if err != nil {
		t.Fatalf("Prepare(empty): %v", err)
	}
	if program.RetainedBytes() <= empty.RetainedBytes() ||
		program.RetainedBytes() != program.Clone().RetainedBytes() {
		t.Fatalf("retained bytes = mixed:%d empty:%d", program.RetainedBytes(), empty.RetainedBytes())
	}
}

func TestPrepareRejectsInvalidAuthorityAndCommitmentIsSensitive(t *testing.T) {
	base := mixedProgramInput(t)
	first, err := Prepare(base)
	if err != nil {
		t.Fatalf("Prepare(base): %v", err)
	}
	firstCommitment, _ := first.Commitment()

	tests := []struct {
		name   string
		mutate func(Input)
	}{
		{name: "bad digest", mutate: func(input Input) { input.Objects[0].DefinitionSha256[0] ^= 0xff }},
		{name: "bad ordinal", mutate: func(input Input) { input.Objects[1].ResolutionOrdinal = 9 }},
		{name: "bad order", mutate: func(input Input) { input.Objects[0], input.Objects[1] = input.Objects[1], input.Objects[0] }},
		{name: "unknown field", mutate: func(input Input) { input.Objects[0].ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01}) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := cloneProgramInput(base)
			test.mutate(input)
			if _, err := Prepare(input); err == nil {
				t.Fatal("Prepare(invalid) succeeded")
			}
		})
	}

	changed := cloneProgramInput(base)
	changed.Objects[3].Definition.GetCalculatedField().Expression = "upper(alias_value)"
	refreshDefinition(t, changed.Objects[3])
	changed.Dependencies[0].Source.DefinitionSha256 = bytes.Clone(changed.Objects[3].DefinitionSha256)
	second, err := Prepare(changed)
	if err != nil {
		t.Fatalf("Prepare(changed): %v", err)
	}
	secondCommitment, _ := second.Commitment()
	if firstCommitment == secondCommitment || first.Equal(second) {
		t.Fatal("semantic change preserved program commitment")
	}
}

func TestPrepareRequiresExactDerivedDependencies(t *testing.T) {
	definitions := []*opensplunkv1.KnowledgeObjectDefinition{
		{
			AppId: "app-a", Name: "extract-chain", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
				InputField: "_raw",
				Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{Regex: &opensplunkv1.RegexFieldExtractionDefinition{
					Pattern: `(?P<extracted_value>x)`, OutputFields: []string{"extracted_value"},
				}},
			}},
		},
		{
			AppId: "app-a", Name: "alias-chain", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{FieldAlias: &opensplunkv1.FieldAliasDefinition{
				SourceField: "extracted_value", DestinationField: "alias_value",
			}},
		},
		{
			AppId: "app-a", Name: "calculated-chain", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Body: &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{CalculatedField: &opensplunkv1.CalculatedFieldDefinition{
				DestinationField: "calculated_value", Expression: "lower(alias_value)",
			}},
		},
	}
	base := inputFromDefinitions(t, definitions)
	compiled, err := Compile(base.Objects)
	if err != nil {
		t.Fatalf("Compile(chain): %v", err)
	}
	base.Dependencies = compiled.Dependencies()
	prepared, err := Prepare(cloneProgramInput(base))
	if err != nil {
		t.Fatalf("Prepare(exact dependencies): %v", err)
	}
	if !prepared.Equal(compiled) {
		t.Fatal("Prepare(exact dependencies) disagrees with Compile")
	}

	tests := []struct {
		name   string
		mutate func(*Input)
	}{
		{
			name: "missing",
			mutate: func(input *Input) {
				input.Dependencies = input.Dependencies[:1]
			},
		},
		{
			name: "extra",
			mutate: func(input *Input) {
				input.Dependencies = append(
					input.Dependencies,
					testDependency(input.Objects[2], input.Objects[0], 2, 2),
				)
			},
		},
		{
			name: "wrong depth",
			mutate: func(input *Input) {
				input.Dependencies[1].TopologicalDepth = 1
			},
		},
		{
			name: "wrong source version",
			mutate: func(input *Input) {
				input.Dependencies[0].Source.Version++
			},
		},
		{
			name: "wrong target digest",
			mutate: func(input *Input) {
				input.Dependencies[0].Target.GetObject().DefinitionSha256[0] ^= 0xff
			},
		},
		{
			name: "wrong role",
			mutate: func(input *Input) {
				input.Dependencies[0].Role = opensplunkv1.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_UNSPECIFIED
			},
		},
		{
			name: "reordered",
			mutate: func(input *Input) {
				input.Dependencies[0], input.Dependencies[1] = input.Dependencies[1], input.Dependencies[0]
			},
		},
		{
			name: "nested unknown field",
			mutate: func(input *Input) {
				input.Dependencies[0].Target.GetObject().ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := cloneProgramInput(base)
			test.mutate(&input)
			if _, err := Prepare(input); !errors.Is(err, ErrInvalidProgram) {
				t.Fatalf("Prepare(invalid dependency closure) error = %v, want ErrInvalidProgram", err)
			}
		})
	}
}

func TestPrepareValidatesParallelStageSemantics(t *testing.T) {
	alias := func(name, selector, source, destination string) *opensplunkv1.KnowledgeObjectDefinition {
		return &opensplunkv1.KnowledgeObjectDefinition{
			AppId: "app-a", Name: name, SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Selector: &opensplunkv1.KnowledgeSelector{HostPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{Value: selector}}},
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{FieldAlias: &opensplunkv1.FieldAliasDefinition{
				SourceField: source, DestinationField: destination,
			}},
		}
	}
	tests := []struct {
		name        string
		definitions []*opensplunkv1.KnowledgeObjectDefinition
		wantError   bool
	}{
		{
			name: "overlapping destination",
			definitions: []*opensplunkv1.KnowledgeObjectDefinition{
				alias("alias-a", "web", "source_a", "shared_output"),
				alias("alias-b", "web", "source_b", "shared_output"),
			},
			wantError: true,
		},
		{
			name: "overlapping same-stage chain",
			definitions: []*opensplunkv1.KnowledgeObjectDefinition{
				alias("alias-a", "web", "source_a", "intermediate"),
				alias("alias-b", "web", "intermediate", "final_output"),
			},
			wantError: true,
		},
		{
			name: "provably disjoint destination",
			definitions: []*opensplunkv1.KnowledgeObjectDefinition{
				alias("alias-a", "api", "source_a", "shared_output"),
				alias("alias-b", "web", "source_b", "shared_output"),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := inputFromDefinitions(t, test.definitions)
			_, compileErr := Compile(input.Objects)
			_, prepareErr := Prepare(input)
			if (compileErr != nil) != test.wantError || (prepareErr != nil) != test.wantError {
				t.Fatalf("Compile/Prepare errors = %v/%v, wantError %t", compileErr, prepareErr, test.wantError)
			}
			if test.wantError && (!errors.Is(compileErr, ErrInvalidProgram) || !errors.Is(prepareErr, ErrInvalidProgram)) {
				t.Fatalf("Compile/Prepare errors = %v/%v, want ErrInvalidProgram", compileErr, prepareErr)
			}
		})
	}
}

func TestPrepareRejectsUnnamedRegexCapture(t *testing.T) {
	definition := &opensplunkv1.KnowledgeObjectDefinition{
		AppId: "app-a", Name: "extract-a", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
			InputField: "_raw",
			Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{Regex: &opensplunkv1.RegexFieldExtractionDefinition{
				Pattern: `([a])(?P<named>b)`, OutputFields: []string{"named"},
			}},
		}},
	}
	if _, err := Prepare(inputFromDefinitions(t, []*opensplunkv1.KnowledgeObjectDefinition{definition})); err == nil {
		t.Fatal("Prepare accepted a regex with an unnamed capture")
	}
}

func TestPrepareRejectsDirectBooleanCalculatedExpression(t *testing.T) {
	definition := &opensplunkv1.KnowledgeObjectDefinition{
		AppId: "app-a", Name: "calculated-a", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Body: &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{CalculatedField: &opensplunkv1.CalculatedFieldDefinition{
			DestinationField: "calculated_value", Expression: "isnull(host)",
		}},
	}
	if _, err := Prepare(inputFromDefinitions(t, []*opensplunkv1.KnowledgeObjectDefinition{definition})); err == nil {
		t.Fatal("Prepare accepted a direct Boolean calculated expression")
	}
}

func TestPrepareEnforcesScalarExpressionAggregateBoundary(t *testing.T) {
	definitions := make([]*opensplunkv1.KnowledgeObjectDefinition, MaximumScalarExpressions+1)
	for index := range definitions {
		definitions[index] = &opensplunkv1.KnowledgeObjectDefinition{
			AppId: "app-a", Name: fmt.Sprintf("calculated-%02d", index), SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Body: &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{CalculatedField: &opensplunkv1.CalculatedFieldDefinition{
				DestinationField: fmt.Sprintf("calculated_%02d", index), Expression: "host",
			}},
		}
	}
	if _, err := Prepare(inputFromDefinitions(t, definitions[:MaximumScalarExpressions])); err != nil {
		t.Fatalf("Prepare(exact scalar-expression boundary): %v", err)
	}
	if _, err := Prepare(inputFromDefinitions(t, definitions)); err == nil {
		t.Fatal("Prepare accepted one scalar expression beyond the aggregate boundary")
	}
}

func TestPreparePreservesJSONBeforeRegexOperatorOrder(t *testing.T) {
	definitions := []*opensplunkv1.KnowledgeObjectDefinition{
		{
			AppId: "app-a", Name: "a-json", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
				InputField: "_raw",
				Extraction: &opensplunkv1.FieldExtractionDefinition_Json{Json: &opensplunkv1.JsonFieldExtractionDefinition{
					Path: "payload.value", OutputField: "json_value",
				}},
			}},
		},
		{
			AppId: "app-a", Name: "b-regex", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
				InputField: "_raw",
				Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{Regex: &opensplunkv1.RegexFieldExtractionDefinition{
					Pattern: `(?P<regex_value>x)`, OutputFields: []string{"regex_value"},
				}},
			}},
		},
	}
	program, err := Prepare(inputFromDefinitions(t, definitions))
	if err != nil {
		t.Fatalf("Prepare(JSON before regex): %v", err)
	}
	kinds := program.OperatorKinds()
	if len(kinds) != 2 || kinds[0] != OperatorConditionalExtractJSON || kinds[1] != OperatorConditionalExtract {
		t.Fatalf("operator kinds = %v, want JSON then regex", kinds)
	}
}

func mixedProgramInput(t *testing.T) Input {
	t.Helper()
	selector := &opensplunkv1.KnowledgeSelector{IndexPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{Value: "main*"}}}
	regexDefinition := &opensplunkv1.KnowledgeObjectDefinition{
		AppId: "app-a", Name: "extract-a", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Selector: proto.Clone(selector).(*opensplunkv1.KnowledgeSelector),
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
			InputField: "_raw", OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
			Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{Regex: &opensplunkv1.RegexFieldExtractionDefinition{Pattern: `(?P<first>[a-z]+)-(?P<second>[0-9]+)`, OutputFields: []string{"first", "second"}}},
		}},
	}
	jsonDefinition := &opensplunkv1.KnowledgeObjectDefinition{
		AppId: "app-a", Name: "extract-json", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
			InputField: "_raw", OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
			Extraction: &opensplunkv1.FieldExtractionDefinition_Json{Json: &opensplunkv1.JsonFieldExtractionDefinition{Path: "payload.value", OutputField: "json_value"}},
		}},
	}
	aliasDefinition := &opensplunkv1.KnowledgeObjectDefinition{
		AppId: "app-a", Name: "alias-a", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{FieldAlias: &opensplunkv1.FieldAliasDefinition{SourceField: "source_field", DestinationField: "alias_value", OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING}},
	}
	calculatedDefinition := &opensplunkv1.KnowledgeObjectDefinition{
		AppId: "app-a", Name: "calculated-a", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Body: &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{CalculatedField: &opensplunkv1.CalculatedFieldDefinition{DestinationField: "calculated_value", Expression: "lower(alias_value)", OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING}},
	}
	definitions := []*opensplunkv1.KnowledgeObjectDefinition{regexDefinition, jsonDefinition, aliasDefinition, calculatedDefinition}
	objects := make([]*opensplunkv1.KnowledgeSnapshotObject, len(definitions))
	stageOrdinals := map[opensplunkv1.KnowledgeSearchStage]uint32{}
	for index, definition := range definitions {
		normalized, err := knowledgedefinition.Normalize(definition)
		if err != nil {
			t.Fatalf("Normalize(definition %d): %v", index, err)
		}
		stage, _, err := stageForType(normalized.ObjectType)
		if err != nil {
			t.Fatal(err)
		}
		objects[index] = &opensplunkv1.KnowledgeSnapshotObject{
			ResolutionOrdinal: uint32(index), Stage: stage, StageOrdinal: stageOrdinals[stage],
			KnowledgeObjectId: "object-" + normalized.Name, Version: 1, ObjectType: normalized.ObjectType,
			Name: normalized.Name, AppId: normalized.AppID, OwnerId: "owner-a", SharingScope: normalized.SharingScope,
			Definition: normalized.Definition, DefinitionSha256: bytes.Clone(normalized.Digest[:]),
		}
		stageOrdinals[stage]++
	}
	return Input{
		Objects: objects,
		Dependencies: []*opensplunkv1.KnowledgeObjectDependency{
			testDependency(objects[3], objects[2], 1, 0),
		},
	}
}

func inputFromDefinitions(t *testing.T, definitions []*opensplunkv1.KnowledgeObjectDefinition) Input {
	t.Helper()
	objects := make([]*opensplunkv1.KnowledgeSnapshotObject, len(definitions))
	stageOrdinals := map[opensplunkv1.KnowledgeSearchStage]uint32{}
	for index, definition := range definitions {
		normalized, err := knowledgedefinition.Normalize(definition)
		if err != nil {
			t.Fatalf("Normalize(definition %d): %v", index, err)
		}
		stage, _, err := stageForType(normalized.ObjectType)
		if err != nil {
			t.Fatal(err)
		}
		objects[index] = &opensplunkv1.KnowledgeSnapshotObject{
			ResolutionOrdinal: uint32(index),
			Stage:             stage,
			StageOrdinal:      stageOrdinals[stage],
			KnowledgeObjectId: "object-" + normalized.Name,
			Version:           1,
			ObjectType:        normalized.ObjectType,
			Name:              normalized.Name,
			AppId:             normalized.AppID,
			OwnerId:           "owner-a",
			SharingScope:      normalized.SharingScope,
			Definition:        normalized.Definition,
			DefinitionSha256:  bytes.Clone(normalized.Digest[:]),
		}
		stageOrdinals[stage]++
	}
	return Input{Objects: objects}
}

func testDependency(
	source, target *opensplunkv1.KnowledgeSnapshotObject,
	depth, ordinal uint32,
) *opensplunkv1.KnowledgeObjectDependency {
	return &opensplunkv1.KnowledgeObjectDependency{
		Source: &opensplunkv1.KnowledgeObjectVersionReference{
			KnowledgeObjectId: source.GetKnowledgeObjectId(),
			Version:           source.GetVersion(),
			DefinitionSha256:  bytes.Clone(source.GetDefinitionSha256()),
		},
		Target: &opensplunkv1.KnowledgeDependencyTarget{
			Target: &opensplunkv1.KnowledgeDependencyTarget_Object{
				Object: &opensplunkv1.KnowledgeObjectVersionReference{
					KnowledgeObjectId: target.GetKnowledgeObjectId(),
					Version:           target.GetVersion(),
					DefinitionSha256:  bytes.Clone(target.GetDefinitionSha256()),
				},
			},
		},
		Role:             opensplunkv1.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
		SourceStage:      source.GetStage(),
		TargetStage:      target.GetStage(),
		TopologicalDepth: depth,
		CanonicalOrdinal: ordinal,
	}
}

func cloneProgramInput(input Input) Input {
	result := Input{Objects: make([]*opensplunkv1.KnowledgeSnapshotObject, len(input.Objects)), Dependencies: cloneDependencies(input.Dependencies)}
	for index, object := range input.Objects {
		result.Objects[index] = proto.Clone(object).(*opensplunkv1.KnowledgeSnapshotObject)
	}
	return result
}

func refreshDefinition(t *testing.T, object *opensplunkv1.KnowledgeSnapshotObject) {
	t.Helper()
	normalized, err := knowledgedefinition.Normalize(object.Definition)
	if err != nil {
		t.Fatalf("Normalize(refreshed): %v", err)
	}
	object.Definition = normalized.Definition
	object.DefinitionSha256 = bytes.Clone(normalized.Digest[:])
}
