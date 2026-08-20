package plan

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestPipelineAnalysisAccountsForEveryReadWithoutInventingPrivateDependencies(t *testing.T) {
	t.Parallel()

	const source = `index=gradethis` +
		` | regex message!="(?i)reject_界"` +
		` | sort 0 +event_id` +
		` | accum bytes AS running` +
		` | strcat allrequired=true host "private_join_💥" route endpoint` +
		` | addinfo` +
		` | fillnull value="private_fill_界" optional` +
		` | addtotals fieldname=total bytes running` +
		` | delta running AS step p=2` +
		` | makemv delim="💥界" allowempty=true tags` +
		` | mvexpand tags limit=3` +
		` | reverse` +
		` | table event_id tags running endpoint optional total step info_sid`

	scope := testScope([]string{"gradethis"}, nil)
	scope.SearchJobID = "pipeline-analysis-job"
	logical, err := Build(mustParse(t, source), scope)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	analysis, err := AnalyzeStages(logical)
	if err != nil {
		t.Fatalf("AnalyzeStages() error = %v", err)
	}

	wantStages := []struct {
		operator string
		reads    []string
	}{
		{operator: "Scan"},
		{operator: "Filter", reads: []string{"index"}},
		{operator: "RegexFilter", reads: []string{"message"}},
		{operator: "Sort", reads: []string{"event_id"}},
		{operator: "StreamAggregate", reads: []string{"bytes"}},
		{operator: "Strcat", reads: []string{"host", "route"}},
		{operator: "Extend"},
		{operator: "FillNull", reads: []string{"optional"}},
		{operator: "RowTotal", reads: []string{"bytes", "running"}},
		{operator: "OrderedDelta", reads: []string{"running"}},
		{operator: "MakeMultivalue", reads: []string{"tags"}},
		{operator: "ExpandMultivalue", reads: []string{"tags"}},
		{operator: "Reverse"},
		{operator: "Project", reads: []string{
			"endpoint", "event_id", "info_sid", "optional", "running", "step", "tags", "total",
		}},
	}
	if len(logical.Operators) != len(wantStages) || len(analysis.Stages) != len(wantStages) {
		names := make([]string, len(logical.Operators))
		for index, operator := range logical.Operators {
			names[index] = operator.LogicalName()
		}
		t.Fatalf("stage counts = logical %d, analysis %d, want %d; operators = %v", len(logical.Operators), len(analysis.Stages), len(wantStages), names)
	}
	for index, want := range wantStages {
		if got := logical.Operators[index].LogicalName(); got != want.operator {
			t.Fatalf("operator %d = %q, want %q", index, got, want.operator)
		}
		if !slices.Equal(analysis.Stages[index].ReferencedFields, want.reads) {
			t.Fatalf("stage %d (%s) reads = %v, want %v", index, want.operator, analysis.Stages[index].ReferencedFields, want.reads)
		}
	}

	wantReads := []string{
		"bytes", "endpoint", "event_id", "host", "index", "info_sid", "message", "optional",
		"route", "running", "step", "tags", "total",
	}
	if !slices.Equal(analysis.ReferencedFields, wantReads) {
		t.Fatalf("whole-query reads = %v, want %v", analysis.ReferencedFields, wantReads)
	}
	for _, field := range analysis.ReferencedFields {
		if strings.HasPrefix(strings.ToLower(field), "__os_") {
			t.Fatalf("analysis exposed compiler-private dependency %q", field)
		}
	}
	if rendered := strings.Join(analysis.ReferencedFields, "\x00"); strings.Contains(rendered, "private_") || strings.Contains(rendered, "💥") {
		t.Fatalf("analysis leaked authored literal material: %q", rendered)
	}
}

func TestPipelineFieldAnalysisRejectsOnlyTheRowGeneratingExpansionAtItsExactRange(t *testing.T) {
	t.Parallel()

	const eventSource = `index=gradethis` +
		` | regex message="ok" | reverse | accum bytes AS running` +
		` | strcat host "/" route endpoint | addinfo` +
		` | fillnull value="0" optional | addtotals fieldname=total bytes running` +
		` | delta running AS step p=2 | makemv delim="," tags`
	scope := testScope([]string{"gradethis"}, nil)
	scope.SearchJobID = "pipeline-field-analysis-job"
	eventLogical, err := Build(mustParse(t, eventSource), scope)
	if err != nil {
		t.Fatalf("Build(event pipeline) error = %v", err)
	}
	if err := ValidateFieldAnalysisEligibility(eventLogical); err != nil {
		t.Fatalf("event-preserving pipeline commands rejected for field analysis: %v", err)
	}

	const expansionSuffix = ` | mvexpand tags limit=2 | reverse`
	expandedSource := eventSource + expansionSuffix
	expandedLogical, err := Build(mustParse(t, expandedSource), scope)
	if err != nil {
		t.Fatalf("Build(expanding pipeline) error = %v", err)
	}
	var expansion *ExpandMultivalue
	for _, operator := range expandedLogical.Operators {
		if candidate, ok := operator.(*ExpandMultivalue); ok {
			expansion = candidate
			break
		}
	}
	if expansion == nil {
		t.Fatal("built pipeline omitted ExpandMultivalue")
	}

	err = ValidateFieldAnalysisEligibility(expandedLogical)
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != fieldAnalysisPipelineDiagnosticCode {
		t.Fatalf("field analysis error = %v, want %s", err, fieldAnalysisPipelineDiagnosticCode)
	}
	if diagnostic.Range != expansion.Range {
		t.Fatalf("diagnostic range = %#v, want expansion range %#v", diagnostic.Range, expansion.Range)
	}
	if got := expandedSource[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset]; got != "mvexpand tags limit=2" {
		t.Fatalf("diagnostic located %q, want exact mvexpand command", got)
	}
}
