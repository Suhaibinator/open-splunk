package plan

import (
	"errors"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestValidateFieldAnalysisEligibilityAcceptsFinalEventRelations(t *testing.T) {
	base := validFieldAnalysisQuery()
	operators := []Operator{
		&Filter{},
		&Extend{Assignments: []ExtendAssignment{{Output: FieldRef{Name: "computed"}}}},
		&Rename{Assignments: []RenameAssignment{{Source: FieldRef{Name: "source"}, Destination: FieldRef{Name: "origin"}}}},
		&Project{Mode: ProjectModeInclude, Fields: []FieldRef{{Name: "host"}}},
		&Project{Mode: ProjectModeExclude, Fields: []FieldRef{{Name: "trace_id"}}},
		&Project{Mode: ProjectModeTable, Fields: []FieldRef{{Name: "host"}, {Name: "status"}}},
		&Sort{},
		&Deduplicate{},
		&Limit{},
	}
	for _, operator := range operators {
		t.Run(operator.LogicalName(), func(t *testing.T) {
			query := &Query{Operators: append(append([]Operator(nil), base.Operators...), operator)}
			if err := ValidateFieldAnalysisEligibility(query); err != nil {
				t.Fatalf("ValidateFieldAnalysisEligibility(%T) error = %v", operator, err)
			}
		})
	}

	query := &Query{Operators: []Operator{
		base.Operators[0],
		&Filter{},
		&Extend{Assignments: []ExtendAssignment{{Output: FieldRef{Name: "computed"}}}},
		&Rename{Assignments: []RenameAssignment{{Source: FieldRef{Name: "computed"}, Destination: FieldRef{Name: "renamed"}}}},
		&Project{Mode: ProjectModeTable, Fields: []FieldRef{{Name: "renamed"}}},
		&Sort{},
		&Deduplicate{},
		&Limit{},
	}}
	if err := ValidateFieldAnalysisEligibility(query); err != nil {
		t.Fatalf("complete event pipeline error = %v", err)
	}
}

func TestValidateFieldAnalysisEligibilityRejectsTransformingAndForgedRelations(t *testing.T) {
	base := validFieldAnalysisQuery()
	tests := []struct {
		name  string
		query *Query
	}{
		{name: "nil query"},
		{name: "empty query", query: &Query{}},
		{name: "dynamic output", query: &Query{Operators: base.Operators, DynamicOutput: &DynamicSeriesOutput{FixedFields: []string{"_time"}, MaxSeries: 1}}},
		{name: "first is not scan", query: &Query{Operators: []Operator{&Filter{}}}},
		{name: "second scan", query: &Query{Operators: []Operator{base.Operators[0], &Scan{}}}},
		{name: "invalid project", query: &Query{Operators: []Operator{base.Operators[0], &Project{Mode: ProjectModeInvalid}}}},
		{name: "aggregate", query: &Query{Operators: []Operator{base.Operators[0], &Aggregate{}}}},
		{name: "timechart", query: &Query{Operators: []Operator{base.Operators[0], &Timechart{}}}},
		{name: "window", query: &Query{Operators: []Operator{base.Operators[0], &Window{}}}},
		{name: "future operator", query: &Query{Operators: []Operator{base.Operators[0], &fieldAnalysisFutureOperator{}}}},
	}
	var typedNil *Filter
	tests = append(tests, struct {
		name  string
		query *Query
	}{name: "typed nil", query: &Query{Operators: []Operator{base.Operators[0], typedNil}}})

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateFieldAnalysisEligibility(test.query)
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_UNSUPPORTED_FIELD_ANALYSIS_PIPELINE" {
				t.Fatalf("error = %v, want field-analysis pipeline diagnostic", err)
			}
		})
	}
}

func validFieldAnalysisQuery() *Query {
	return &Query{Operators: []Operator{&Scan{Range: spl.Range{Start: spl.Position{Offset: 1}}}}}
}

type fieldAnalysisFutureOperator struct{}

func (*fieldAnalysisFutureOperator) operator()              {}
func (*fieldAnalysisFutureOperator) LogicalName() string    { return "Future" }
func (*fieldAnalysisFutureOperator) SourceRange() spl.Range { return spl.Range{} }
