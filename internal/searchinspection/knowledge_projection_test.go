package searchinspection

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestProjectLogicalPlanProjectsAllKnowledgeOperatorsAndRedactsProvenance(
	t *testing.T,
) {
	t.Parallel()

	const source = `index=main status=200`
	authoredRange := spl.Range{
		Start: spl.Position{Offset: 0, Line: 1, Column: 1},
		End: spl.Position{
			Offset: len(source), Line: 1, Column: len(source) + 1,
		},
	}
	authored := &plan.Query{
		Operators: []plan.Operator{
			&plan.Scan{
				Indexes: []string{"main"},
				Range:   authoredRange,
			},
			&plan.Filter{
				Expression: &plan.ComparisonExpression{
					Field: plan.FieldRef{Name: "status"},
					Value: plan.Value{
						Kind:   plan.ValueKindString,
						String: "secret-authored-literal",
					},
					Range: authoredRange,
				},
				Range: authoredRange,
			},
		},
		EffectiveIndexes: []string{"main"},
	}
	program := knowledgeProjectionProgram(t)
	logical, err := plan.InjectKnowledgePrelude(authored, program)
	if err != nil {
		t.Fatalf("InjectKnowledgePrelude: %v", err)
	}

	projected, err := projectLogicalPlan(context.Background(), logical, source)
	if err != nil {
		t.Fatalf("projectLogicalPlan: %v", err)
	}
	assertKnowledgeProjection(t, projected, authoredRange)

	// Every returned collection and authored range must be detached from the
	// immutable plan and from the other projected stages.
	projected.Stages[0].SourceRange.Start.Line = 99
	if projected.Stages[len(projected.Stages)-1].SourceRange.Start.Line != 1 {
		t.Fatal("authored stage source ranges alias each other")
	}
	projected.Stages[1].InputFields[0] = "mutated_input"
	projected.Stages[2].OutputFields[0] = "mutated_output"
	projected.Stages[3].KnowledgeObjects[0].Ordinal = 99
	projected.Stages[3].OutputProvenance[0].Field = "mutated_provenance"
	projected.ReferencedFields[0] = "mutated_reference"

	again, err := projectLogicalPlan(context.Background(), logical, source)
	if err != nil {
		t.Fatalf("projectLogicalPlan(second): %v", err)
	}
	assertKnowledgeProjection(t, again, authoredRange)
}

func assertKnowledgeProjection(
	t *testing.T,
	projected LogicalPlan,
	authoredRange spl.Range,
) {
	t.Helper()

	wantOperators := []string{
		"Scan",
		"ConditionalExtractJSON",
		"ConditionalExtract",
		"CopyFieldAlias",
		"ParallelExtend",
		"Filter",
	}
	if len(projected.Stages) != len(wantOperators) {
		t.Fatalf("stages = %d, want %d: %#v", len(projected.Stages), len(wantOperators), projected.Stages)
	}
	for index, want := range wantOperators {
		stage := projected.Stages[index]
		if stage.Index != uint32(index) || stage.Operator != want {
			t.Fatalf("stage %d = %#v, want operator %q", index, stage, want)
		}
		generated := index > 0 && index < len(wantOperators)-1
		if generated {
			if stage.SourceRange != nil {
				t.Fatalf("generated stage %d has authored source range %#v", index, stage.SourceRange)
			}
		} else {
			wantRange := sourceRangeProjection(authoredRange)
			if stage.SourceRange == nil || *stage.SourceRange != wantRange {
				t.Fatalf("authored stage %d source range = %#v, want %#v", index, stage.SourceRange, wantRange)
			}
			if len(stage.KnowledgeObjects) != 0 || len(stage.OutputProvenance) != 0 {
				t.Fatalf("authored stage %d invented knowledge provenance: %#v", index, stage)
			}
		}
	}

	assertStringsEqual(t, projected.Stages[1].InputFields, []string{"_raw", "source"})
	assertStringsEqual(t, projected.Stages[1].OutputFields, []string{"json_value"})
	assertStringsEqual(t, projected.Stages[2].InputFields, []string{"_raw", "index", "sourcetype"})
	assertStringsEqual(t, projected.Stages[2].OutputFields, []string{"regex_first", "regex_second"})
	assertStringsEqual(t, projected.Stages[3].InputFields, []string{"host", "source_alpha", "source_beta"})
	// Two selector-disjoint alias objects legitimately target one output. The
	// public stage field inventory is unique while occurrence provenance retains
	// both redacted writers.
	assertStringsEqual(t, projected.Stages[3].OutputFields, []string{"shared_alias"})
	assertStringsEqual(t, projected.Stages[4].InputFields, []string{"service", "source"})
	assertStringsEqual(t, projected.Stages[4].OutputFields, []string{"calculated_out"})
	assertStringsEqual(t, projected.Stages[5].InputFields, []string{"status"})

	extractionType := opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION
	extractionStage := opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION
	aliasType := opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS
	aliasStage := opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS
	calculatedType := opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD
	calculatedStage := opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD
	wantObjects := [][]RedactedObjectProvenance{
		nil,
		{{Ordinal: 0, ObjectType: extractionType, Stage: extractionStage}},
		{{Ordinal: 1, ObjectType: extractionType, Stage: extractionStage}},
		{
			{Ordinal: 2, ObjectType: aliasType, Stage: aliasStage},
			{Ordinal: 3, ObjectType: aliasType, Stage: aliasStage},
		},
		{{Ordinal: 4, ObjectType: calculatedType, Stage: calculatedStage}},
		nil,
	}
	for index, want := range wantObjects {
		if !slices.Equal(projected.Stages[index].KnowledgeObjects, want) {
			t.Fatalf(
				"stage %d knowledge objects = %#v, want %#v",
				index,
				projected.Stages[index].KnowledgeObjects,
				want,
			)
		}
	}

	wantOutputs := [][]OutputProvenance{
		nil,
		{{Field: "json_value", ObjectOrdinal: wantObjects[1][0].Ordinal}},
		{
			{Field: "regex_first", ObjectOrdinal: wantObjects[2][0].Ordinal},
			{Field: "regex_second", ObjectOrdinal: wantObjects[2][0].Ordinal},
		},
		{
			{Field: "shared_alias", ObjectOrdinal: wantObjects[3][0].Ordinal},
			{Field: "shared_alias", ObjectOrdinal: wantObjects[3][1].Ordinal},
		},
		{{Field: "calculated_out", ObjectOrdinal: wantObjects[4][0].Ordinal}},
		nil,
	}
	for index, want := range wantOutputs {
		if !slices.Equal(projected.Stages[index].OutputProvenance, want) {
			t.Fatalf(
				"stage %d output provenance = %#v, want %#v",
				index,
				projected.Stages[index].OutputProvenance,
				want,
			)
		}
	}

	if projected.Output.Kind != OutputKindOpen {
		t.Fatalf("output shape = %#v, want open", projected.Output)
	}
	assertStringsEqual(t, projected.ReferencedFields, []string{
		"_raw",
		"host",
		"index",
		"service",
		"source",
		"source_alpha",
		"source_beta",
		"sourcetype",
		"status",
	})

	rendered := fmt.Sprintf("%#v", projected)
	for _, secret := range []string{
		"secret-app-id",
		"secret-owner-id",
		"secret-object-",
		"json_extract",
		"regex_extract",
		"alias_alpha",
		"alias_beta",
		"calculated_value",
		"secret-authored-literal",
		"secret-calculated-literal",
		"secret-calculated-source",
		"secret-host-alpha",
		"secret-host-beta",
		"secret-json-path",
		"secret-json-source",
		"secret-regex-literal",
		"secret-regex-sourcetype",
		"field_extraction.regex.pattern",
	} {
		if strings.Contains(rendered, secret) {
			t.Fatalf("safe knowledge projection contains %q: %s", secret, rendered)
		}
	}
}

func knowledgeProjectionProgram(t *testing.T) knowledgeprogram.Program {
	t.Helper()

	definitions := []*opensplunk.KnowledgeObjectDefinition{
		{
			AppId:        "secret-app-id",
			Name:         "json_extract",
			SharingScope: opensplunk.SharingScope_SHARING_SCOPE_APP,
			Selector: knowledgeProjectionSelector(
				"source",
				"secret-json-source",
			),
			Body: &opensplunk.KnowledgeObjectDefinition_FieldExtraction{
				FieldExtraction: &opensplunk.FieldExtractionDefinition{
					InputField:        "_raw",
					OverwriteBehavior: opensplunk.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
					Extraction: &opensplunk.FieldExtractionDefinition_Json{
						Json: &opensplunk.JsonFieldExtractionDefinition{
							Path:        "secret-json-path.token",
							OutputField: "json_value",
						},
					},
				},
			},
		},
		{
			AppId:        "secret-app-id",
			Name:         "regex_extract",
			SharingScope: opensplunk.SharingScope_SHARING_SCOPE_APP,
			Selector: knowledgeProjectionSelector(
				"index+sourcetype",
				"main",
			),
			Body: &opensplunk.KnowledgeObjectDefinition_FieldExtraction{
				FieldExtraction: &opensplunk.FieldExtractionDefinition{
					InputField:        "_raw",
					OverwriteBehavior: opensplunk.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
					Extraction: &opensplunk.FieldExtractionDefinition_Regex{
						Regex: &opensplunk.RegexFieldExtractionDefinition{
							Pattern: `secret-regex-literal-(?P<regex_second>[0-9]+)-(?P<regex_first>[a-z]+)`,
							OutputFields: []string{
								"regex_second",
								"regex_first",
							},
						},
					},
				},
			},
		},
		knowledgeProjectionAliasDefinition(
			"alias_alpha",
			"source_alpha",
			"secret-host-alpha",
		),
		knowledgeProjectionAliasDefinition(
			"alias_beta",
			"source_beta",
			"secret-host-beta",
		),
		{
			AppId:        "secret-app-id",
			Name:         "calculated_value",
			SharingScope: opensplunk.SharingScope_SHARING_SCOPE_APP,
			Selector: knowledgeProjectionSelector(
				"source",
				"secret-calculated-source",
			),
			Body: &opensplunk.KnowledgeObjectDefinition_CalculatedField{
				CalculatedField: &opensplunk.CalculatedFieldDefinition{
					DestinationField:  "calculated_out",
					Expression:        `if(service="secret-calculated-literal", 1, 0)`,
					OverwriteBehavior: opensplunk.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
				},
			},
		},
	}

	objects := make([]*opensplunk.KnowledgeSnapshotObject, len(definitions))
	stageOrdinals := make(map[opensplunk.KnowledgeSearchStage]uint32)
	for index, definition := range definitions {
		normalized, err := knowledgedefinition.Normalize(definition)
		if err != nil {
			t.Fatalf("Normalize(%q): %v", definition.GetName(), err)
		}
		stage := knowledgeProjectionStage(normalized.ObjectType)
		objects[index] = &opensplunk.KnowledgeSnapshotObject{
			ResolutionOrdinal: uint32(index),
			Stage:             stage,
			StageOrdinal:      stageOrdinals[stage],
			KnowledgeObjectId: "secret-object-" + normalized.Name,
			Version:           uint64(index + 11),
			ObjectType:        normalized.ObjectType,
			Name:              normalized.Name,
			AppId:             normalized.AppID,
			OwnerId:           "secret-owner-id",
			SharingScope:      normalized.SharingScope,
			Definition:        normalized.Definition,
			DefinitionSha256:  slices.Clone(normalized.Digest[:]),
		}
		stageOrdinals[stage]++
	}

	program, err := knowledgeprogram.Prepare(knowledgeprogram.Input{Objects: objects})
	if err != nil {
		t.Fatalf("Prepare(disjoint all-four program): %v", err)
	}
	if program.ObjectCount() != uint32(len(objects)) ||
		program.Charges().GeneratedOperators != 4 {
		t.Fatalf(
			"program inventory = objects %d charges %+v",
			program.ObjectCount(),
			program.Charges(),
		)
	}
	return program
}

func knowledgeProjectionAliasDefinition(
	name string,
	source string,
	host string,
) *opensplunk.KnowledgeObjectDefinition {
	return &opensplunk.KnowledgeObjectDefinition{
		AppId:        "secret-app-id",
		Name:         name,
		SharingScope: opensplunk.SharingScope_SHARING_SCOPE_APP,
		Selector:     knowledgeProjectionSelector("host", host),
		Body: &opensplunk.KnowledgeObjectDefinition_FieldAlias{
			FieldAlias: &opensplunk.FieldAliasDefinition{
				SourceField:       source,
				DestinationField:  "shared_alias",
				OverwriteBehavior: opensplunk.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
			},
		},
	}
}

func knowledgeProjectionSelector(
	dimension string,
	value string,
) *opensplunk.KnowledgeSelector {
	pattern := []*opensplunk.KnowledgeSelectorPattern{{Value: value}}
	selector := &opensplunk.KnowledgeSelector{}
	switch dimension {
	case "index+sourcetype":
		selector.IndexPatterns = pattern
		selector.SourcetypePatterns = []*opensplunk.KnowledgeSelectorPattern{{
			Value: "secret-regex-sourcetype",
		}}
	case "host":
		selector.HostPatterns = pattern
	case "source":
		selector.SourcePatterns = pattern
	default:
		panic("unsupported knowledge projection selector dimension")
	}
	return selector
}

func knowledgeProjectionStage(
	objectType opensplunk.KnowledgeObjectType,
) opensplunk.KnowledgeSearchStage {
	switch objectType {
	case opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION:
		return opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION
	case opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS:
		return opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS
	case opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD:
		return opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD
	default:
		panic("unsupported knowledge projection object type")
	}
}
