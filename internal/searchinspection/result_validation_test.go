package searchinspection

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
)

func TestValidateResultAcceptsServiceOutput(t *testing.T) {
	snapshot := validInspectionSnapshot()
	service := newInspectionTestService(t, inspectionTestConfig{
		Searches: &inspectionSearches{
			snapshots: []searchjobs.ExecutionSnapshot{snapshot},
		},
		Compiler: &inspectionCompiler{},
		Explainer: &inspectionExplainer{
			result: inspectionExplainResult(
				"open-splunk-explain-result-validator-service",
			),
		},
	})
	result, err := service.Inspect(
		context.Background(),
		searchjobs.AccessScope{
			TenantID: snapshot.TenantID,
			OwnerID:  snapshot.OwnerID,
		},
		Request{SearchJobID: snapshot.ID},
	)
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if err := ValidateResult(result); err != nil {
		t.Fatalf("ValidateResult(service output) error = %v", err)
	}
}

func TestValidateResultAcceptsCanonicalResultAndExactBounds(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Result)
	}{
		{name: "ordinary"},
		{
			name: "maximum generated SQL",
			mutate: func(result *Result) {
				result.GeneratedSQL = "SELECT " + strings.Repeat(
					"x",
					maximumGeneratedSQLBytes-len("SELECT "),
				)
			},
		},
		{
			name: "maximum stages",
			mutate: func(result *Result) {
				result.Plan.Stages = validResultStages(
					int(maximumAuthoredPlanStages),
				)
			},
		},
		{
			name: "maximum fields in one stage",
			mutate: func(result *Result) {
				result.Plan.Stages[0].InputFields =
					resultValidationFieldNames(
						int(maximumStageFields),
						"input",
						12,
					)
			},
		},
		{
			name: "static output preserves plan order",
			mutate: func(result *Result) {
				result.Plan.Output = OutputShape{
					Kind:   OutputKindStatic,
					Fields: []string{"z_field", "a_field"},
				}
			},
		},
		{
			name: "dynamic output",
			mutate: func(result *Result) {
				result.Plan.Output = OutputShape{
					Kind:             OutputKindDynamic,
					Fields:           []string{"_time"},
					MaxDynamicFields: maximumDynamicFields,
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := validResultForValidation(t)
			if test.mutate != nil {
				test.mutate(&result)
			}
			before := fmt.Sprintf("%#v", result)
			if err := ValidateResult(result); err != nil {
				t.Fatalf("ValidateResult() error = %v", err)
			}
			if after := fmt.Sprintf("%#v", result); after != before {
				t.Fatal("ValidateResult() mutated its input")
			}
		})
	}
}

func TestValidateResultRejectsMalformedLogicalProjection(t *testing.T) {
	private := "private-result-value-7f2c"
	tests := []struct {
		name   string
		mutate func(*Result)
	}{
		{
			name: "empty stages",
			mutate: func(result *Result) {
				result.Plan.Stages = nil
			},
		},
		{
			name: "too many authored stages",
			mutate: func(result *Result) {
				result.Plan.Stages = validResultStages(
					int(maximumAuthoredPlanStages) + 1,
				)
			},
		},
		{
			name: "noncontiguous stage index",
			mutate: func(result *Result) {
				result.Plan.Stages[0].Index = 1
			},
		},
		{
			name: "unsupported operator",
			mutate: func(result *Result) {
				result.Plan.Stages[0].Operator = private
			},
		},
		{
			name: "too many stage input fields",
			mutate: func(result *Result) {
				result.Plan.Stages[0].InputFields =
					resultValidationFieldNames(
						int(maximumStageFields)+1,
						"input",
						12,
					)
			},
		},
		{
			name: "too many stage output fields",
			mutate: func(result *Result) {
				result.Plan.Stages[0].OutputFields =
					resultValidationFieldNames(
						int(maximumStageFields)+1,
						"output",
						12,
					)
			},
		},
		{
			name: "unsorted stage inputs",
			mutate: func(result *Result) {
				result.Plan.Stages[0].InputFields =
					[]string{"z_field", "a_field"}
			},
		},
		{
			name: "duplicate stage outputs",
			mutate: func(result *Result) {
				result.Plan.Stages[0].OutputFields =
					[]string{"same", "same"}
			},
		},
		{
			name: "unsorted referenced fields",
			mutate: func(result *Result) {
				result.Plan.ReferencedFields =
					[]string{"z_field", "a_field"}
			},
		},
		{
			name: "duplicate referenced fields",
			mutate: func(result *Result) {
				result.Plan.ReferencedFields =
					[]string{"same", "same"}
			},
		},
		{
			name: "too many referenced fields",
			mutate: func(result *Result) {
				result.Plan.ReferencedFields =
					resultValidationFieldNames(
						int(maximumStageFields)+1,
						"referenced",
						16,
					)
			},
		},
		{
			name: "field has invalid escape",
			mutate: func(result *Result) {
				result.Plan.ReferencedFields = []string{`bad\q`}
			},
		},
		{
			name: "field has control",
			mutate: func(result *Result) {
				result.Plan.ReferencedFields =
					[]string{"bad\u0085field"}
			},
		},
		{
			name: "field has invalid UTF-8",
			mutate: func(result *Result) {
				result.Plan.ReferencedFields =
					[]string{string([]byte{'b', 'a', 'd', 0xff})}
			},
		},
		{
			name: "too many field occurrences",
			mutate: func(result *Result) {
				fields := resultValidationFieldNames(
					int(maximumStageFields),
					"field",
					12,
				)
				result.Plan.Stages = validResultStages(16)
				for index := range result.Plan.Stages {
					result.Plan.Stages[index].InputFields =
						fields
				}
				result.Plan.ReferencedFields = []string{"overflow"}
			},
		},
		{
			name: "field strings exceed aggregate bytes",
			mutate: func(result *Result) {
				fields := resultValidationFieldNames(
					int(maximumStageFields),
					"field",
					250,
				)
				result.Plan.Stages = validResultStages(5)
				for index := range result.Plan.Stages {
					result.Plan.Stages[index].InputFields =
						fields
				}
			},
		},
		{
			name: "zero source line",
			mutate: func(result *Result) {
				result.Plan.Stages[0].SourceRange.Start.Line = 0
			},
		},
		{
			name: "zero source column",
			mutate: func(result *Result) {
				result.Plan.Stages[0].SourceRange.End.Column = 0
			},
		},
		{
			name: "source offset exceeds retained source bound",
			mutate: func(result *Result) {
				result.Plan.Stages[0].SourceRange.End =
					SourcePosition{
						ByteOffset: maximumProjectionSourceBytes + 1,
						Line:       1,
						Column:     maximumProjectionSourceBytes + 2,
					}
			},
		},
		{
			name: "source byte range is backward",
			mutate: func(result *Result) {
				result.Plan.Stages[0].SourceRange =
					&SourceRange{
						Start: SourcePosition{
							ByteOffset: 2, Line: 1, Column: 3,
						},
						End: SourcePosition{
							ByteOffset: 1, Line: 1, Column: 2,
						},
					}
			},
		},
		{
			name: "source coordinate is backward",
			mutate: func(result *Result) {
				result.Plan.Stages[0].SourceRange =
					&SourceRange{
						Start: SourcePosition{
							ByteOffset: 1, Line: 2, Column: 1,
						},
						End: SourcePosition{
							ByteOffset: 2, Line: 1, Column: 3,
						},
					}
			},
		},
		{
			name: "same source byte has two coordinates",
			mutate: func(result *Result) {
				result.Plan.Stages[0].SourceRange =
					&SourceRange{
						Start: SourcePosition{
							ByteOffset: 1, Line: 1, Column: 2,
						},
						End: SourcePosition{
							ByteOffset: 1, Line: 2, Column: 1,
						},
					}
			},
		},
		{
			name: "invalid output kind",
			mutate: func(result *Result) {
				result.Plan.Output.Kind = OutputKindInvalid
			},
		},
		{
			name: "open output has fields",
			mutate: func(result *Result) {
				result.Plan.Output = OutputShape{
					Kind: OutputKindOpen, Fields: []string{"field"},
				}
			},
		},
		{
			name: "open output has dynamic bound",
			mutate: func(result *Result) {
				result.Plan.Output = OutputShape{
					Kind: OutputKindOpen, MaxDynamicFields: 1,
				}
			},
		},
		{
			name: "static output is empty",
			mutate: func(result *Result) {
				result.Plan.Output = OutputShape{
					Kind: OutputKindStatic,
				}
			},
		},
		{
			name: "static output has dynamic bound",
			mutate: func(result *Result) {
				result.Plan.Output = OutputShape{
					Kind:             OutputKindStatic,
					Fields:           []string{"field"},
					MaxDynamicFields: 1,
				}
			},
		},
		{
			name: "static output has duplicate fields",
			mutate: func(result *Result) {
				result.Plan.Output = OutputShape{
					Kind:   OutputKindStatic,
					Fields: []string{"same", "same"},
				}
			},
		},
		{
			name: "static output has too many fields",
			mutate: func(result *Result) {
				result.Plan.Output = OutputShape{
					Kind: OutputKindStatic,
					Fields: resultValidationFieldNames(
						int(maximumFinalOutputFields)+1,
						"output",
						16,
					),
				}
			},
		},
		{
			name: "dynamic output is empty",
			mutate: func(result *Result) {
				result.Plan.Output = OutputShape{
					Kind: OutputKindDynamic, MaxDynamicFields: 1,
				}
			},
		},
		{
			name: "dynamic output has zero bound",
			mutate: func(result *Result) {
				result.Plan.Output = OutputShape{
					Kind: OutputKindDynamic, Fields: []string{"_time"},
				}
			},
		},
		{
			name: "dynamic output bound is too large",
			mutate: func(result *Result) {
				result.Plan.Output = OutputShape{
					Kind:             OutputKindDynamic,
					Fields:           []string{"_time"},
					MaxDynamicFields: maximumDynamicFields + 1,
				}
			},
		},
		{
			name: "dynamic output has too many fixed fields",
			mutate: func(result *Result) {
				result.Plan.Output = OutputShape{
					Kind: OutputKindDynamic,
					Fields: resultValidationFieldNames(
						int(maximumStageFields)+1,
						"fixed",
						12,
					),
					MaxDynamicFields: 1,
				}
			},
		},
		{
			name: "dynamic output has duplicate fixed fields",
			mutate: func(result *Result) {
				result.Plan.Output = OutputShape{
					Kind:             OutputKindDynamic,
					Fields:           []string{"same", "same"},
					MaxDynamicFields: 1,
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := validResultForValidation(t)
			test.mutate(&result)
			assertInvalidInspectionResult(t, result, private)
		})
	}
}

func TestValidateResultAcceptsGeneratedKnowledgeProvenance(t *testing.T) {
	t.Run("legacy has authored ranges and no summary", func(t *testing.T) {
		result := validResultForValidation(t)
		if err := ValidateResult(result); err != nil {
			t.Fatalf("ValidateResult(legacy) error = %v", err)
		}
	})

	t.Run("enabled empty has authored ranges and an empty summary", func(t *testing.T) {
		result := validResultForValidation(t)
		result.KnowledgeSnapshot = resultValidationKnowledgeSummary(nil)
		if err := ValidateResult(result); err != nil {
			t.Fatalf("ValidateResult(enabled empty) error = %v", err)
		}
	})

	operatorPairs := []struct {
		operator   string
		objectType opensplunk.KnowledgeObjectType
		stage      opensplunk.KnowledgeSearchStage
	}{
		{
			operator:   "ConditionalExtract",
			objectType: opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION,
			stage:      opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION,
		},
		{
			operator:   "ConditionalExtractJSON",
			objectType: opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION,
			stage:      opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION,
		},
		{
			operator:   "CopyFieldAlias",
			objectType: opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
			stage:      opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS,
		},
		{
			operator:   "ParallelExtend",
			objectType: opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD,
			stage:      opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD,
		},
	}
	for _, pair := range operatorPairs {
		t.Run(pair.operator+" exact type and stage", func(t *testing.T) {
			object := RedactedObjectProvenance{
				ObjectType: pair.objectType,
				Stage:      pair.stage,
			}
			result := resultValidationKnowledgeResult(
				t,
				resultValidationGeneratedStage(
					pair.operator,
					[]RedactedObjectProvenance{object},
					[]OutputProvenance{{Field: "generated", ObjectOrdinal: object.Ordinal}},
				),
			)
			if err := ValidateResult(result); err != nil {
				t.Fatalf("ValidateResult(%s) error = %v", pair.operator, err)
			}
		})
	}

	t.Run("complete ordered generated prefix", func(t *testing.T) {
		result := resultValidationFourOperatorResult(t)
		if err := ValidateResult(result); err != nil {
			t.Fatalf("ValidateResult(four operators) error = %v", err)
		}
	})

	t.Run("same field has distinct selector disjoint origins", func(t *testing.T) {
		objects := []RedactedObjectProvenance{
			{
				Ordinal:    0,
				ObjectType: opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
				Stage:      opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS,
			},
			{
				Ordinal:    1,
				ObjectType: opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
				Stage:      opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS,
			},
		}
		result := resultValidationKnowledgeResult(
			t,
			resultValidationGeneratedStage(
				"CopyFieldAlias",
				objects,
				[]OutputProvenance{
					{Field: "shared", ObjectOrdinal: objects[0].Ordinal},
					{Field: "shared", ObjectOrdinal: objects[1].Ordinal},
				},
			),
		)
		if err := ValidateResult(result); err != nil {
			t.Fatalf("ValidateResult(shared destination) error = %v", err)
		}
	})
}

func TestValidateResultRejectsGeneratedRangeAndPrefixForgery(t *testing.T) {
	private := "private-result-value-7f2c"
	tests := []struct {
		name   string
		mutate func(*Result)
	}{
		{
			name: "authored stage has nil range",
			mutate: func(result *Result) {
				result.Plan.Stages[0].SourceRange = nil
			},
		},
		{
			name: "generated stage has authored range",
			mutate: func(result *Result) {
				sourceRange := *result.Plan.Stages[0].SourceRange
				result.Plan.Stages[1].SourceRange = &sourceRange
			},
		},
		{
			name: "generated stage replaces scan",
			mutate: func(result *Result) {
				result.Plan.Stages[0], result.Plan.Stages[1] =
					result.Plan.Stages[1], result.Plan.Stages[0]
				resultValidationResetStageIndexes(result.Plan.Stages)
			},
		},
		{
			name: "generated stage follows authored suffix",
			mutate: func(result *Result) {
				generated := result.Plan.Stages[1]
				copy(result.Plan.Stages[1:], result.Plan.Stages[2:])
				result.Plan.Stages[len(result.Plan.Stages)-1] = generated
				resultValidationResetStageIndexes(result.Plan.Stages)
			},
		},
		{
			name: "authored stage carries knowledge provenance",
			mutate: func(result *Result) {
				result.Plan.Stages[0].KnowledgeObjects = slices.Clone(
					result.Plan.Stages[1].KnowledgeObjects,
				)
				result.Plan.Stages[0].OutputProvenance = slices.Clone(
					result.Plan.Stages[1].OutputProvenance,
				)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := resultValidationTwoOriginAliasResult(t)
			test.mutate(&result)
			assertInvalidInspectionResult(t, result, private)
		})
	}

	for _, operators := range [][]string{
		{"CopyFieldAlias", "CopyFieldAlias"},
		{"ParallelExtend", "ParallelExtend"},
		{"ParallelExtend", "CopyFieldAlias"},
		{"CopyFieldAlias", "ConditionalExtract"},
	} {
		name := strings.Join(operators, " then ")
		t.Run(name, func(t *testing.T) {
			stages := make([]PlanStage, len(operators))
			for index, operator := range operators {
				contract, ok := inspectionKnowledgeOperator(operator)
				if !ok {
					t.Fatalf("missing knowledge operator contract for %q", operator)
				}
				object := RedactedObjectProvenance{
					Ordinal:    uint32(index),
					ObjectType: contract.objectType,
					Stage:      contract.stage,
				}
				stages[index] = resultValidationGeneratedStage(
					operator,
					[]RedactedObjectProvenance{object},
					[]OutputProvenance{{
						Field:         fmt.Sprintf("generated_%d", index),
						ObjectOrdinal: object.Ordinal,
					}},
				)
			}
			assertInvalidInspectionResult(
				t,
				resultValidationKnowledgeResult(t, stages...),
				private,
			)
		})
	}
}

func TestValidateResultRejectsGeneratedProvenanceForgery(t *testing.T) {
	private := "private-result-value-7f2c"
	tests := []struct {
		name   string
		mutate func(*PlanStage)
	}{
		{
			name: "missing object",
			mutate: func(stage *PlanStage) {
				stage.KnowledgeObjects = stage.KnowledgeObjects[:1]
			},
		},
		{
			name: "extra unused object",
			mutate: func(stage *PlanStage) {
				stage.KnowledgeObjects = append(
					stage.KnowledgeObjects,
					RedactedObjectProvenance{
						Ordinal:    2,
						ObjectType: opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
						Stage:      opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS,
					},
				)
			},
		},
		{
			name: "duplicate object",
			mutate: func(stage *PlanStage) {
				stage.KnowledgeObjects = append(
					stage.KnowledgeObjects,
					stage.KnowledgeObjects[1],
				)
			},
		},
		{
			name: "reordered objects",
			mutate: func(stage *PlanStage) {
				stage.KnowledgeObjects[0], stage.KnowledgeObjects[1] =
					stage.KnowledgeObjects[1], stage.KnowledgeObjects[0]
			},
		},
		{
			name: "missing output provenance",
			mutate: func(stage *PlanStage) {
				stage.OutputProvenance = stage.OutputProvenance[:1]
			},
		},
		{
			name: "extra output provenance",
			mutate: func(stage *PlanStage) {
				stage.OutputProvenance = append(
					stage.OutputProvenance,
					OutputProvenance{
						Field:         "field_c",
						ObjectOrdinal: stage.KnowledgeObjects[0].Ordinal,
					},
				)
			},
		},
		{
			name: "duplicate output provenance",
			mutate: func(stage *PlanStage) {
				stage.OutputProvenance = append(
					stage.OutputProvenance,
					stage.OutputProvenance[1],
				)
			},
		},
		{
			name: "reordered output provenance",
			mutate: func(stage *PlanStage) {
				stage.OutputProvenance[0], stage.OutputProvenance[1] =
					stage.OutputProvenance[1], stage.OutputProvenance[0]
			},
		},
		{
			name: "output object ordinal is absent from inventory",
			mutate: func(stage *PlanStage) {
				stage.OutputProvenance[0].ObjectOrdinal = 99
			},
		},
		{
			name: "operator type mismatches provenance",
			mutate: func(stage *PlanStage) {
				stage.Operator = "ParallelExtend"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := resultValidationTwoOriginAliasResult(t)
			test.mutate(&result.Plan.Stages[1])
			assertInvalidInspectionResult(t, result, private)
		})
	}

	t.Run("global provenance ordinals are reordered", func(t *testing.T) {
		result := resultValidationFourOperatorResult(t)
		first := &result.Plan.Stages[1]
		second := &result.Plan.Stages[2]
		first.KnowledgeObjects[0].Ordinal = 1
		first.OutputProvenance[0].ObjectOrdinal = 1
		second.KnowledgeObjects[0].Ordinal = 0
		second.OutputProvenance[0].ObjectOrdinal = 0
		assertInvalidInspectionResult(t, result, private)
	})
}

func TestValidateResultBindsKnowledgeSummaryToLogicalProvenance(t *testing.T) {
	private := "private-result-value-7f2c"

	t.Run("nonempty exact binding", func(t *testing.T) {
		result := resultValidationTwoOriginAliasResult(t)
		if err := ValidateResult(result); err != nil {
			t.Fatalf("ValidateResult(nonempty) error = %v", err)
		}
	})

	tests := []struct {
		name   string
		result func(*testing.T) Result
		mutate func(*Result)
	}{
		{
			name:   "nonempty plan has missing summary",
			result: resultValidationTwoOriginAliasResult,
			mutate: func(result *Result) {
				result.KnowledgeSnapshot = nil
			},
		},
		{
			name:   "legacy plan has nonempty summary",
			result: validResultForValidation,
			mutate: func(result *Result) {
				objects := resultValidationAliasObjects(2)
				result.KnowledgeSnapshot = resultValidationKnowledgeSummary(objects)
			},
		},
		{
			name:   "nonempty plan has enabled empty summary",
			result: resultValidationTwoOriginAliasResult,
			mutate: func(result *Result) {
				result.KnowledgeSnapshot = resultValidationKnowledgeSummary(nil)
			},
		},
		{
			name: "summary has extra object",
			result: func(t *testing.T) Result {
				object := resultValidationAliasObjects(1)
				return resultValidationKnowledgeResult(
					t,
					resultValidationGeneratedStage(
						"CopyFieldAlias",
						object,
						[]OutputProvenance{{Field: "field_a", ObjectOrdinal: object[0].Ordinal}},
					),
				)
			},
			mutate: func(result *Result) {
				result.KnowledgeSnapshot = resultValidationKnowledgeSummary(
					resultValidationAliasObjects(2),
				)
			},
		},
		{
			name:   "summary type and stage mismatch",
			result: resultValidationTwoOriginAliasResult,
			mutate: func(result *Result) {
				object := result.KnowledgeSnapshot.Objects[0]
				object.ObjectType = opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD
				object.Stage = opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := test.result(t)
			test.mutate(&result)
			assertInvalidInspectionResult(t, result, private)
		})
	}
}

func TestValidateResultGeneratedProvenanceExactBounds(t *testing.T) {
	t.Run("combined stage N and N plus one", func(t *testing.T) {
		generated := make([]PlanStage, maximumProjectedKnowledgeObjects)
		for index := range generated {
			object := RedactedObjectProvenance{
				Ordinal:    uint32(index),
				ObjectType: opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION,
				Stage:      opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION,
			}
			generated[index] = resultValidationGeneratedStage(
				"ConditionalExtract",
				[]RedactedObjectProvenance{object},
				[]OutputProvenance{{
					Field:         fmt.Sprintf("generated_%03d", index),
					ObjectOrdinal: object.Ordinal,
				}},
			)
		}
		result := resultValidationKnowledgeResult(t, generated...)
		authoredRange := *result.Plan.Stages[len(result.Plan.Stages)-1].SourceRange
		authoredSuffix := result.Plan.Stages[len(result.Plan.Stages)-1]
		result.Plan.Stages[len(result.Plan.Stages)-1] = PlanStage{
			Operator:     "AutomaticLookupGroup",
			InputFields:  []string{"service_key"},
			OutputFields: []string{"service_owner"},
		}
		result.Plan.Stages = append(result.Plan.Stages, authoredSuffix)
		for len(result.Plan.Stages) < int(maximumPlanStages) {
			result.Plan.Stages = append(result.Plan.Stages, PlanStage{
				Operator:    "Limit",
				SourceRange: &authoredRange,
			})
		}
		resultValidationResetStageIndexes(result.Plan.Stages)
		if err := ValidateResult(result); err != nil {
			t.Fatalf("ValidateResult(exact combined stage ceiling) error = %v", err)
		}

		over := cloneResultValidationFixture(result)
		over.Plan.Stages = append(over.Plan.Stages, PlanStage{
			Index:       uint32(len(over.Plan.Stages)),
			Operator:    "Limit",
			SourceRange: &authoredRange,
		})
		assertInvalidInspectionResult(t, over, "private-result-value-7f2c")
	})

	t.Run("object N and N plus one", func(t *testing.T) {
		objects := resultValidationAliasObjects(
			int(maximumProjectedKnowledgeObjects),
		)
		outputs := make([]OutputProvenance, len(objects))
		for index, object := range objects {
			outputs[index] = OutputProvenance{
				Field:         fmt.Sprintf("object_%03d", index),
				ObjectOrdinal: object.Ordinal,
			}
		}
		result := resultValidationKnowledgeResult(
			t,
			resultValidationGeneratedStage(
				"CopyFieldAlias",
				objects,
				outputs,
			),
		)
		if err := ValidateResult(result); err != nil {
			t.Fatalf("ValidateResult(exact object ceiling) error = %v", err)
		}

		overObjects := slices.Clone(objects)
		overObjects = append(overObjects, RedactedObjectProvenance{
			Ordinal:    maximumProjectedKnowledgeObjects,
			ObjectType: opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
			Stage:      opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS,
		})
		overObjectOutputs := make([]OutputProvenance, len(overObjects))
		for index, object := range overObjects {
			overObjectOutputs[index] = OutputProvenance{
				Field:         fmt.Sprintf("object_%03d", index),
				ObjectOrdinal: object.Ordinal,
			}
		}
		overObjectPlan := resultValidationKnowledgeResult(
			t,
			resultValidationGeneratedStage(
				"CopyFieldAlias",
				overObjects,
				overObjectOutputs,
			),
		).Plan
		if got, ok := validInspectionLogicalPlan(overObjectPlan); ok || got != nil {
			t.Fatalf(
				"validInspectionLogicalPlan accepted %d knowledge objects",
				len(overObjects),
			)
		}
	})

	t.Run("output N and N plus one", func(t *testing.T) {
		stages := make([]PlanStage, maximumProjectedKnowledgeObjects)
		for index := range stages {
			object := RedactedObjectProvenance{
				Ordinal:    uint32(index),
				ObjectType: opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION,
				Stage:      opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION,
			}
			stages[index] = resultValidationGeneratedStage(
				"ConditionalExtract",
				[]RedactedObjectProvenance{object},
				[]OutputProvenance{
					{Field: fmt.Sprintf("field_%03d_a", index), ObjectOrdinal: object.Ordinal},
					{Field: fmt.Sprintf("field_%03d_b", index), ObjectOrdinal: object.Ordinal},
				},
			)
		}
		result := resultValidationKnowledgeResult(t, stages...)
		if err := ValidateResult(result); err != nil {
			t.Fatalf("ValidateResult(exact output ceiling) error = %v", err)
		}

		overOutputs := cloneResultValidationFixture(result)
		last := &overOutputs.Plan.Stages[len(overOutputs.Plan.Stages)-2]
		extra := OutputProvenance{
			Field:         "field_zzz",
			ObjectOrdinal: last.KnowledgeObjects[0].Ordinal,
		}
		last.OutputFields = append(last.OutputFields, extra.Field)
		last.OutputProvenance = append(last.OutputProvenance, extra)
		assertInvalidInspectionResult(
			t,
			overOutputs,
			"private-result-value-7f2c",
		)
	})
}

func TestValidateResultRejectsMalformedSensitiveAndPhysicalValues(
	t *testing.T,
) {
	private := "private-result-value-7f2c"
	tests := []struct {
		name   string
		mutate func(*Result)
	}{
		{
			name: "empty generated SQL",
			mutate: func(result *Result) {
				result.GeneratedSQL = ""
			},
		},
		{
			name: "blank generated SQL",
			mutate: func(result *Result) {
				result.GeneratedSQL = " \n\t"
			},
		},
		{
			name: "oversized generated SQL",
			mutate: func(result *Result) {
				result.GeneratedSQL = strings.Repeat(
					"x",
					maximumGeneratedSQLBytes+1,
				)
			},
		},
		{
			name: "generated SQL has invalid UTF-8",
			mutate: func(result *Result) {
				result.GeneratedSQL =
					string([]byte{'S', 'E', 'L', 'E', 'C', 'T', 0xff})
			},
		},
		{
			name: "generated SQL has unsupported control",
			mutate: func(result *Result) {
				result.GeneratedSQL = "SELECT '" + private + "\x00'"
			},
		},
		{
			name: "empty EXPLAIN text",
			mutate: func(result *Result) {
				result.ExplainText = ""
			},
		},
		{
			name: "malformed EXPLAIN text",
			mutate: func(result *Result) {
				result.ExplainText = private
			},
		},
		{
			name: "oversized EXPLAIN text",
			mutate: func(result *Result) {
				result.ExplainText = strings.Repeat(
					"x",
					maximumInspectionExplainTextBytes+1,
				)
			},
		},
		{
			name: "oversized EXPLAIN line",
			mutate: func(result *Result) {
				result.ExplainText = strings.Repeat(
					"x",
					maximumInspectionExplainLineBytes+1,
				)
			},
		},
		{
			name: "too many EXPLAIN lines",
			mutate: func(result *Result) {
				result.ExplainText = strings.Repeat(
					"x\n",
					maximumInspectionExplainLines,
				) + "x"
			},
		},
		{
			name: "empty diagnostic query ID",
			mutate: func(result *Result) {
				result.DiagnosticQueryID = ""
			},
		},
		{
			name: "malformed diagnostic query ID",
			mutate: func(result *Result) {
				result.DiagnosticQueryID = private
			},
		},
		{
			name: "oversized diagnostic query ID",
			mutate: func(result *Result) {
				result.DiagnosticQueryID =
					"open-splunk-explain-" +
						strings.Repeat(
							"x",
							maximumInspectionDiagnosticQueryIDBytes,
						)
			},
		},
		{
			name: "physical projection does not match raw plan",
			mutate: func(result *Result) {
				result.PhysicalPlan.NodeTypes[0] = "Filter"
			},
		},
		{
			name: "too many physical nodes",
			mutate: func(result *Result) {
				result.PhysicalPlan.NodeTypes = make(
					[]string,
					maximumInspectionPhysicalNodes+1,
				)
			},
		},
		{
			name: "too many physical reads",
			mutate: func(result *Result) {
				result.PhysicalPlan.Reads = make(
					[]queryexec.ExplainRead,
					maximumInspectionPhysicalReads+1,
				)
			},
		},
		{
			name: "too many physical columns",
			mutate: func(result *Result) {
				result.PhysicalPlan.Reads = []queryexec.ExplainRead{{
					Columns: make(
						[]string,
						maximumInspectionPhysicalHeaders+1,
					),
				}}
			},
		},
		{
			name: "too many physical indexes",
			mutate: func(result *Result) {
				result.PhysicalPlan.Reads = []queryexec.ExplainRead{{
					Indexes: make(
						[]queryexec.ExplainIndex,
						maximumInspectionPhysicalIndexes+1,
					),
				}}
			},
		},
		{
			name: "too many physical index keys",
			mutate: func(result *Result) {
				result.PhysicalPlan.Reads = []queryexec.ExplainRead{{
					Indexes: []queryexec.ExplainIndex{{
						Type: "PrimaryKey",
						Keys: make(
							[]string,
							maximumInspectionPhysicalIndexKeys+1,
						),
					}},
				}}
			},
		},
		{
			name: "physical selected parts exceed initial",
			mutate: func(result *Result) {
				result.PhysicalPlan.Reads = []queryexec.ExplainRead{{
					Indexes: []queryexec.ExplainIndex{{
						Type:          "PrimaryKey",
						InitialParts:  1,
						SelectedParts: 2,
					}},
				}}
			},
		},
		{
			name: "physical selected granules exceed initial",
			mutate: func(result *Result) {
				result.PhysicalPlan.Reads = []queryexec.ExplainRead{{
					Indexes: []queryexec.ExplainIndex{{
						Type:             "PrimaryKey",
						InitialGranules:  1,
						SelectedGranules: 2,
					}},
				}}
			},
		},
		{
			name: "oversized physical metadata",
			mutate: func(result *Result) {
				result.PhysicalPlan.NodeTypes = []string{
					strings.Repeat(
						"x",
						maximumInspectionPhysicalMetadataBytes+1,
					),
				}
			},
		},
		{
			name: "aggregate physical metadata is oversized",
			mutate: func(result *Result) {
				result.PhysicalPlan.NodeTypes = make(
					[]string,
					maximumInspectionPhysicalNodes,
				)
				value := strings.Repeat(
					"x",
					maximumInspectionPhysicalMetadataBytes,
				)
				for index := range result.PhysicalPlan.NodeTypes {
					result.PhysicalPlan.NodeTypes[index] = value
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := validResultForValidation(t)
			test.mutate(&result)
			assertInvalidInspectionResult(t, result, private)
		})
	}
}

func validResultForValidation(t *testing.T) Result {
	t.Helper()
	explained := queryexec.ExplainResult{
		Text:    `[{"Plan":{"Node Type":"ReadNothing"}}]`,
		QueryID: "open-splunk-explain-result-validator",
	}
	physical, err := queryexec.ParseExplainPlan(explained)
	if err != nil {
		t.Fatalf("ParseExplainPlan(valid fixture): %v", err)
	}
	return Result{
		Plan: LogicalPlan{
			Stages: []PlanStage{{
				Operator: "Scan",
				SourceRange: &SourceRange{
					Start: SourcePosition{
						Line: 1, Column: 1,
					},
					End: SourcePosition{
						ByteOffset: 10, Line: 1, Column: 11,
					},
				},
			}},
			ReferencedFields: []string{"host", "status"},
			Output:           OutputShape{Kind: OutputKindOpen},
		},
		PhysicalPlan:      physical,
		GeneratedSQL:      "SELECT event_time FROM events",
		ExplainText:       explained.Text,
		DiagnosticQueryID: explained.QueryID,
	}
}

func validResultStages(count int) []PlanStage {
	stages := make([]PlanStage, count)
	for index := range stages {
		operator := "Limit"
		if index == 0 {
			operator = "Scan"
		}
		stages[index] = PlanStage{
			Index:    uint32(index),
			Operator: operator,
			SourceRange: &SourceRange{
				Start: SourcePosition{Line: 1, Column: 1},
				End: SourcePosition{
					ByteOffset: 1, Line: 1, Column: 2,
				},
			},
		}
	}
	return stages
}

func resultValidationFourOperatorResult(t *testing.T) Result {
	t.Helper()
	objects := []RedactedObjectProvenance{
		{
			Ordinal:    0,
			ObjectType: opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION,
			Stage:      opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION,
		},
		{
			Ordinal:    1,
			ObjectType: opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION,
			Stage:      opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION,
		},
		{
			Ordinal:    2,
			ObjectType: opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
			Stage:      opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS,
		},
		{
			Ordinal:    3,
			ObjectType: opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD,
			Stage:      opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD,
		},
	}
	operators := []string{
		"ConditionalExtract",
		"ConditionalExtractJSON",
		"CopyFieldAlias",
		"ParallelExtend",
	}
	stages := make([]PlanStage, len(operators))
	for index, operator := range operators {
		stages[index] = resultValidationGeneratedStage(
			operator,
			[]RedactedObjectProvenance{objects[index]},
			[]OutputProvenance{{
				Field:         fmt.Sprintf("generated_%d", index),
				ObjectOrdinal: objects[index].Ordinal,
			}},
		)
	}
	return resultValidationKnowledgeResult(t, stages...)
}

func resultValidationTwoOriginAliasResult(t *testing.T) Result {
	t.Helper()
	objects := resultValidationAliasObjects(2)
	return resultValidationKnowledgeResult(
		t,
		resultValidationGeneratedStage(
			"CopyFieldAlias",
			objects,
			[]OutputProvenance{
				{Field: "field_a", ObjectOrdinal: objects[0].Ordinal},
				{Field: "field_b", ObjectOrdinal: objects[1].Ordinal},
			},
		),
	)
}

func resultValidationAliasObjects(count int) []RedactedObjectProvenance {
	objects := make([]RedactedObjectProvenance, count)
	for index := range objects {
		objects[index] = RedactedObjectProvenance{
			Ordinal:    uint32(index),
			ObjectType: opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
			Stage:      opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS,
		}
	}
	return objects
}

func resultValidationGeneratedStage(
	operator string,
	objects []RedactedObjectProvenance,
	outputs []OutputProvenance,
) PlanStage {
	stage := PlanStage{
		Operator:         operator,
		KnowledgeObjects: slices.Clone(objects),
		OutputProvenance: slices.Clone(outputs),
	}
	slices.SortFunc(stage.OutputProvenance, compareOutputProvenance)
	stage.OutputFields = make([]string, 0, len(outputs))
	for _, output := range stage.OutputProvenance {
		stage.OutputFields = append(stage.OutputFields, output.Field)
	}
	slices.Sort(stage.OutputFields)
	stage.OutputFields = slices.Compact(stage.OutputFields)
	return stage
}

func resultValidationKnowledgeResult(
	t *testing.T,
	generated ...PlanStage,
) Result {
	t.Helper()
	result := validResultForValidation(t)
	stages := make([]PlanStage, 0, len(generated)+2)
	stages = append(stages, result.Plan.Stages[0])
	stages = append(stages, generated...)
	stages = append(stages, PlanStage{
		Operator: "Limit",
		SourceRange: &SourceRange{
			Start: SourcePosition{Line: 1, Column: 1},
			End: SourcePosition{
				ByteOffset: 1,
				Line:       1,
				Column:     2,
			},
		},
	})
	resultValidationResetStageIndexes(stages)
	result.Plan.Stages = stages

	objects := make([]RedactedObjectProvenance, 0)
	for _, stage := range generated {
		objects = append(objects, stage.KnowledgeObjects...)
	}
	result.KnowledgeSnapshot = resultValidationKnowledgeSummary(objects)
	return result
}

func resultValidationResetStageIndexes(stages []PlanStage) {
	for index := range stages {
		stages[index].Index = uint32(index)
	}
}

func resultValidationKnowledgeSummary(
	objects []RedactedObjectProvenance,
) *opensplunk.KnowledgeSnapshotSummary {
	prefixCount := min(len(objects), knowledgesnapshot.MaximumSummaryObjects)
	prefix := make(
		[]*opensplunk.KnowledgeSnapshotObjectSummary,
		prefixCount,
	)
	for index, object := range objects[:prefixCount] {
		prefix[index] = &opensplunk.KnowledgeSnapshotObjectSummary{
			ResolutionOrdinal: object.Ordinal,
			ObjectType:        object.ObjectType,
			Stage:             object.Stage,
			Disclosure: &opensplunk.KnowledgeSnapshotObjectSummary_Redacted{
				Redacted: true,
			},
		}
	}
	return &opensplunk.KnowledgeSnapshotSummary{
		Ref: &opensplunk.KnowledgeSnapshotRef{
			SnapshotSha256:          bytes.Repeat([]byte{0x11}, sha256.Size),
			TenantCatalogRevision:   1,
			TenantCatalogStateToken: bytes.Repeat([]byte{0x22}, sha256.Size),
			ObjectCount:             uint32(len(objects)),
			LookupAssetCount:        0,
		},
		Objects:          prefix,
		ObjectsTruncated: len(objects) > knowledgesnapshot.MaximumSummaryObjects,
	}
}

func resultValidationFieldNames(
	count int,
	prefix string,
	width int,
) []string {
	fields := make([]string, count)
	for index := range fields {
		head := fmt.Sprintf("%s_%06d_", prefix, index)
		fields[index] = head + strings.Repeat("x", max(0, width-len(head)))
	}
	return fields
}

func assertInvalidInspectionResult(
	t *testing.T,
	result Result,
	sensitive string,
) {
	t.Helper()
	before := cloneResultValidationFixture(result)
	beforeSummary := before.KnowledgeSnapshot
	before.KnowledgeSnapshot = nil
	err := ValidateResult(result)
	if !errors.Is(err, ErrInspectionFailed) {
		t.Fatalf(
			"ValidateResult() error = %v, want ErrInspectionFailed",
			err,
		)
	}
	if strings.Contains(err.Error(), sensitive) {
		t.Fatalf("ValidateResult() leaked sensitive result content: %v", err)
	}
	resultSummary := result.KnowledgeSnapshot
	result.KnowledgeSnapshot = nil
	if !reflect.DeepEqual(result, before) ||
		!proto.Equal(resultSummary, beforeSummary) {
		t.Fatal("ValidateResult() mutated its rejected input")
	}
}

func cloneResultValidationFixture(result Result) Result {
	cloned := result
	cloned.Plan.Stages = slices.Clone(result.Plan.Stages)
	for index := range cloned.Plan.Stages {
		stage := &cloned.Plan.Stages[index]
		stage.InputFields = slices.Clone(stage.InputFields)
		stage.OutputFields = slices.Clone(stage.OutputFields)
		stage.KnowledgeObjects = slices.Clone(stage.KnowledgeObjects)
		stage.OutputProvenance = slices.Clone(stage.OutputProvenance)
		if stage.SourceRange != nil {
			sourceRange := *stage.SourceRange
			stage.SourceRange = &sourceRange
		}
	}
	cloned.Plan.ReferencedFields = slices.Clone(result.Plan.ReferencedFields)
	cloned.Plan.Output.Fields = slices.Clone(result.Plan.Output.Fields)
	if result.KnowledgeSnapshot != nil {
		cloned.KnowledgeSnapshot = proto.Clone(
			result.KnowledgeSnapshot,
		).(*opensplunk.KnowledgeSnapshotSummary)
	}
	return cloned
}
