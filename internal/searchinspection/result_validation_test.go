package searchinspection

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
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
					int(maximumPlanStages),
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
			name: "too many stages",
			mutate: func(result *Result) {
				result.Plan.Stages = validResultStages(
					int(maximumPlanStages) + 1,
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
					SourceRange{
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
					SourceRange{
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
					SourceRange{
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
				SourceRange: SourceRange{
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
		stages[index] = PlanStage{
			Index:    uint32(index),
			Operator: "Limit",
			SourceRange: SourceRange{
				Start: SourcePosition{Line: 1, Column: 1},
				End: SourcePosition{
					ByteOffset: 1, Line: 1, Column: 2,
				},
			},
		}
	}
	return stages
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
	before := Result{
		Plan: LogicalPlan{
			Stages: append([]PlanStage(nil), result.Plan.Stages...),
			ReferencedFields: append(
				[]string(nil),
				result.Plan.ReferencedFields...,
			),
			Output: result.Plan.Output,
		},
		PhysicalPlan:      result.PhysicalPlan,
		GeneratedSQL:      result.GeneratedSQL,
		ExplainText:       result.ExplainText,
		DiagnosticQueryID: result.DiagnosticQueryID,
	}
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
	if !reflect.DeepEqual(result, before) {
		t.Fatal("ValidateResult() mutated its rejected input")
	}
}
