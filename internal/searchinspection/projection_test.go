package searchinspection

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestProjectLogicalPlanCoversEveryCurrentOperator(t *testing.T) {
	source := strings.Repeat("x", 256)
	sourceRange := testSourceRange()
	chartOver, err := plan.ResolveField("host", sourceRange)
	if err != nil {
		t.Fatalf("resolve chart row field: %v", err)
	}
	chartSplit, err := plan.ResolveField("level", sourceRange)
	if err != nil {
		t.Fatalf("resolve chart column field: %v", err)
	}
	query := &plan.Query{
		Operators: []plan.Operator{
			&plan.Scan{Range: sourceRange},
			&plan.Filter{
				Expression: &plan.ComparisonExpression{
					Field: plan.FieldRef{Name: "status"},
					Value: plan.Value{Kind: plan.ValueKindString, String: "private-literal"},
					Range: sourceRange,
				},
				Range: sourceRange,
			},
			&plan.Project{
				Mode:   plan.ProjectModeTable,
				Fields: []plan.FieldRef{{Name: "host"}, {Name: "status"}},
				Range:  sourceRange,
			},
			&plan.Extend{
				Assignments: []plan.ExtendAssignment{{
					Output: plan.FieldRef{Name: "doubled"},
					Expression: &plan.ScalarFieldExpression{
						Field: plan.FieldRef{Name: "bytes"},
						Range: sourceRange,
					},
				}},
				Range: sourceRange,
			},
			&plan.TimeBucket{
				Field: plan.FieldRef{Name: "_time"}, Output: plan.FieldRef{Name: "bucket"},
				Span: time.Minute, Range: sourceRange,
			},
			&plan.NumericBucket{
				Input: plan.FieldRef{Name: "bytes"}, Output: plan.FieldRef{Name: "size_bin"},
				Span: 10, Range: sourceRange,
			},
			&plan.Extract{
				Input:   plan.FieldRef{Name: "_raw"},
				Pattern: "private-regex",
				Captures: []plan.ExtractCapture{
					{Output: plan.FieldRef{Name: "user"}, Group: 1},
				},
				Range: sourceRange,
			},
			&plan.ExtractJSON{
				Input: plan.FieldRef{Name: "_raw"}, Output: plan.FieldRef{Name: "json_value"},
				Path: "private-json-path", Range: sourceRange,
			},
			&plan.Rename{
				Assignments: []plan.RenameAssignment{{
					Source: plan.FieldRef{Name: "host"}, Destination: plan.FieldRef{Name: "server"},
				}},
				Range: sourceRange,
			},
			&plan.Aggregate{
				GroupBy: []plan.FieldRef{{Name: "host"}},
				Measures: []plan.AggregateMeasure{
					{Function: plan.AggregateFunctionCountRows, Output: "events"},
					{Function: plan.AggregateFunctionSum, Input: plan.FieldRef{Name: "bytes"}, Output: "total_bytes"},
				},
				Range: sourceRange,
			},
			&plan.Timechart{
				Time: plan.FieldRef{Name: "_time", Canonical: true},
				Measure: plan.AggregateMeasure{
					Function: plan.AggregateFunctionCountRows,
					Output:   "count",
				},
				Split: &plan.TimechartSplit{
					Field:        plan.FieldRef{Name: "host", Canonical: true},
					SeriesLimit:  10,
					IncludeNull:  true,
					IncludeOther: true,
					NullLabel:    "NULL",
					OtherLabel:   "OTHER",
				},
				Range: sourceRange,
			},
			&plan.Chart{
				Over: chartOver, SplitBy: chartSplit,
				Measure:      plan.AggregateMeasure{Function: plan.AggregateFunctionCountRows, Output: "count"},
				RowLimit:     10_000,
				SeriesLimit:  10,
				IncludeNull:  true,
				IncludeOther: true,
				NullLabel:    "NULL",
				OtherLabel:   "OTHER",
				Range:        sourceRange,
			},
			&plan.Window{
				Input: plan.FieldRef{Name: "events"}, Output: "percent", Range: sourceRange,
			},
			&plan.Sort{
				Keys:  []plan.SortKey{{Field: plan.FieldRef{Name: "events"}, Descending: true}},
				Range: sourceRange,
			},
			&plan.Deduplicate{
				Count: 1, Keys: []plan.FieldRef{{Name: "host"}}, Range: sourceRange,
			},
			&plan.Limit{Count: 10, Range: sourceRange},
		},
		OutputFields: []string{"host", "events"},
	}
	if _, err := plan.Analyze(query); err != nil {
		t.Fatalf("Analyze fixture: %v", err)
	}

	projected, err := projectLogicalPlan(context.Background(), query, source)
	if err != nil {
		t.Fatal(err)
	}
	wantOperators := []string{
		"Scan", "Filter", "Project", "Extend", "TimeBucket", "NumericBucket",
		"Extract", "ExtractJSON", "Rename", "Aggregate", "Timechart", "Chart",
		"Window", "Sort", "Deduplicate", "Limit",
	}
	if len(projected.Stages) != len(wantOperators) {
		t.Fatalf("stages = %d, want %d", len(projected.Stages), len(wantOperators))
	}
	for index, want := range wantOperators {
		stage := projected.Stages[index]
		canonical := query.Operators[index]
		if canonical.LogicalName() != want {
			t.Fatalf(
				"fixture operator %d canonical name = %q, want %q",
				index,
				canonical.LogicalName(),
				want,
			)
		}
		if stage.Index != uint32(index) ||
			stage.Operator != canonical.LogicalName() {
			t.Fatalf(
				"stage %d = %#v, want canonical operator %q",
				index,
				stage,
				canonical.LogicalName(),
			)
		}
		wantRange := sourceRangeProjection(canonical.SourceRange())
		if stage.SourceRange == nil || *stage.SourceRange != wantRange {
			t.Fatalf(
				"stage %d source range = %#v, want canonical range %#v",
				index,
				stage.SourceRange,
				wantRange,
			)
		}
	}

	assertStringsEqual(t, projected.Stages[1].InputFields, []string{"status"})
	assertStringsEqual(t, projected.Stages[2].InputFields, []string{"host", "status"})
	assertStringsEqual(t, projected.Stages[2].OutputFields, []string{"host", "status"})
	assertStringsEqual(t, projected.Stages[3].InputFields, []string{"bytes"})
	assertStringsEqual(t, projected.Stages[3].OutputFields, []string{"doubled"})
	assertStringsEqual(t, projected.Stages[6].InputFields, []string{"_raw"})
	assertStringsEqual(t, projected.Stages[6].OutputFields, []string{"user"})
	assertStringsEqual(t, projected.Stages[9].InputFields, []string{"bytes", "host"})
	assertStringsEqual(t, projected.Stages[9].OutputFields, []string{"events", "host", "total_bytes"})
	assertStringsEqual(t, projected.Stages[10].InputFields, []string{"_time", "host"})
	assertStringsEqual(t, projected.Stages[10].OutputFields, []string{"_time"})
	assertStringsEqual(t, projected.Stages[11].InputFields, []string{"host", "level"})
	assertStringsEqual(t, projected.Stages[11].OutputFields, []string{"host"})
	assertStringsEqual(t, projected.ReferencedFields, []string{
		"_raw", "_time", "bytes", "events", "host", "level", "status",
	})
	if projected.Output.Kind != OutputKindStatic ||
		!slices.Equal(projected.Output.Fields, []string{"host", "events"}) {
		t.Fatalf("output = %#v", projected.Output)
	}

	rendered := fmt.Sprintf("%#v", projected)
	for _, secret := range []string{
		"private-literal", "private-regex", "private-json-path",
	} {
		if strings.Contains(rendered, secret) {
			t.Fatalf("safe projection contains %q: %s", secret, rendered)
		}
	}
}

func TestProjectLogicalPlanOutputShapesAndDetachment(t *testing.T) {
	source := "index=main"
	sourceRange := spl.Range{
		Start: spl.Position{Offset: 0, Line: 1, Column: 1},
		End:   spl.Position{Offset: len(source), Line: 1, Column: len(source) + 1},
	}
	tests := []struct {
		name  string
		query *plan.Query
		want  OutputShape
	}{
		{
			name:  "open",
			query: &plan.Query{Operators: []plan.Operator{&plan.Scan{Range: sourceRange}}},
			want:  OutputShape{Kind: OutputKindOpen},
		},
		{
			name: "static",
			query: &plan.Query{
				Operators:    []plan.Operator{&plan.Scan{Range: sourceRange}},
				OutputFields: []string{"host", "events"},
			},
			want: OutputShape{Kind: OutputKindStatic, Fields: []string{"host", "events"}},
		},
		{
			name: "dynamic",
			query: &plan.Query{
				Operators: []plan.Operator{&plan.Scan{Range: sourceRange}},
				DynamicOutput: &plan.DynamicSeriesOutput{
					FixedFields: []string{"_time"}, MaxSeries: 12,
				},
			},
			want: OutputShape{
				Kind: OutputKindDynamic, Fields: []string{"_time"}, MaxDynamicFields: 12,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			projected, err := projectLogicalPlan(context.Background(), test.query, source)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(projected.Output, test.want) {
				t.Fatalf("output = %#v, want %#v", projected.Output, test.want)
			}
			if len(projected.Output.Fields) > 0 {
				test.query.OutputFields = []string{"mutated"}
				if test.query.DynamicOutput != nil {
					test.query.DynamicOutput.FixedFields[0] = "mutated"
				}
				if projected.Output.Fields[0] == "mutated" {
					t.Fatal("projection aliases the logical query")
				}
			}
		})
	}
}

func TestProjectLogicalPlanProjectsStaticTimechartCount(t *testing.T) {
	snapshot := validInspectionSnapshot()
	snapshot.SPL = "index=" + snapshot.EffectiveIndexes[0] +
		" | timechart span=5m count"
	logical, err := buildInspectionAuthoredPlan(snapshot)
	if err != nil {
		t.Fatalf("BuildExecutionPlan: %v", err)
	}

	projected, err := projectLogicalPlan(
		context.Background(),
		logical,
		snapshot.SPL,
	)
	if err != nil {
		t.Fatal(err)
	}
	stage := projected.Stages[len(projected.Stages)-1]
	if stage.Operator != "Timechart" {
		t.Fatalf("last stage = %#v, want Timechart", stage)
	}
	assertStringsEqual(t, stage.InputFields, []string{"_time"})
	assertStringsEqual(t, stage.OutputFields, []string{"_time", "count"})
	if projected.Output.Kind != OutputKindStatic ||
		!slices.Equal(projected.Output.Fields, []string{"_time", "count"}) ||
		projected.Output.MaxDynamicFields != 0 {
		t.Fatalf("output = %#v, want static _time,count", projected.Output)
	}
}

func TestProjectLogicalPlanCoversProjectModesFromAcceptedSPL(t *testing.T) {
	snapshot := validInspectionSnapshot()
	tests := []struct {
		name        string
		command     string
		mode        plan.ProjectMode
		stageOutput []string
		outputKind  OutputKind
	}{
		{
			name:        "include",
			command:     "fields host, status",
			mode:        plan.ProjectModeInclude,
			stageOutput: []string{"host", "status"},
			outputKind:  OutputKindOpen,
		},
		{
			name:       "exclude",
			command:    "fields - host, status",
			mode:       plan.ProjectModeExclude,
			outputKind: OutputKindOpen,
		},
		{
			name:        "table",
			command:     "table host, status",
			mode:        plan.ProjectModeTable,
			stageOutput: []string{"host", "status"},
			outputKind:  OutputKindStatic,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := snapshot
			fixture.SPL = "index=" + snapshot.EffectiveIndexes[0] +
				" | " + test.command
			logical, err := buildInspectionAuthoredPlan(fixture)
			if err != nil {
				t.Fatalf("BuildExecutionPlan: %v", err)
			}
			project, ok := logical.Operators[len(logical.Operators)-1].(*plan.Project)
			if !ok || project.Mode != test.mode {
				t.Fatalf(
					"last operator = %#v, want Project mode %d",
					logical.Operators[len(logical.Operators)-1],
					test.mode,
				)
			}
			projected, err := projectLogicalPlan(
				context.Background(),
				logical,
				fixture.SPL,
			)
			if err != nil {
				t.Fatal(err)
			}
			stage := projected.Stages[len(projected.Stages)-1]
			assertStringsEqual(t, stage.InputFields, []string{"host", "status"})
			assertStringsEqual(t, stage.OutputFields, test.stageOutput)
			if projected.Output.Kind != test.outputKind {
				t.Fatalf(
					"output kind = %d, want %d",
					projected.Output.Kind,
					test.outputKind,
				)
			}
		})
	}
}

func TestProjectLogicalPlanOmitsAcceptedTextAndCountPredicateValues(t *testing.T) {
	snapshot := validInspectionSnapshot()
	snapshot.SPL = "index=" + snapshot.EffectiveIndexes[0] +
		` "private-text-filter" | stats count(eval(status=418)) AS matches`
	logical, err := buildInspectionAuthoredPlan(snapshot)
	if err != nil {
		t.Fatalf("BuildExecutionPlan: %v", err)
	}
	projected, err := projectLogicalPlan(
		context.Background(),
		logical,
		snapshot.SPL,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(projected.ReferencedFields, "_raw") ||
		!slices.Contains(projected.ReferencedFields, "status") {
		t.Fatalf(
			"referenced fields = %v, want text and predicate inputs",
			projected.ReferencedFields,
		)
	}
	rendered := fmt.Sprintf("%#v", projected)
	for _, sensitive := range []string{"private-text-filter", "418"} {
		if strings.Contains(rendered, sensitive) {
			t.Fatalf("safe projection contains predicate value %q", sensitive)
		}
	}
}

func TestProjectLogicalPlanFailsClosedAndReturnsNoPartialPlan(t *testing.T) {
	source := strings.Repeat("x", 32)
	validRange := testSourceRange()
	var typedNil *plan.Filter
	tests := []struct {
		name  string
		query *plan.Query
	}{
		{name: "nil query"},
		{name: "empty plan", query: &plan.Query{}},
		{name: "typed nil", query: &plan.Query{Operators: []plan.Operator{typedNil}}},
		{
			name: "backward range",
			query: &plan.Query{Operators: []plan.Operator{&plan.Scan{Range: spl.Range{
				Start: spl.Position{Offset: 2, Line: 1, Column: 3},
				End:   spl.Position{Offset: 1, Line: 1, Column: 2},
			}}}},
		},
		{
			name: "range past source",
			query: &plan.Query{Operators: []plan.Operator{&plan.Scan{Range: spl.Range{
				Start: spl.Position{Offset: 0, Line: 1, Column: 1},
				End:   spl.Position{Offset: len(source) + 1, Line: 1, Column: len(source) + 2},
			}}}},
		},
		{
			name: "invalid output field",
			query: &plan.Query{
				Operators:    []plan.Operator{&plan.Scan{Range: validRange}},
				OutputFields: []string{"bad\x00field"},
			},
		},
		{
			name: "invalid UTF-8 output field",
			query: &plan.Query{
				Operators: []plan.Operator{&plan.Scan{Range: validRange}},
				OutputFields: []string{
					string([]byte{'b', 'a', 'd', 0xff}),
				},
			},
		},
		{
			name: "C1 control output field",
			query: &plan.Query{
				Operators:    []plan.Operator{&plan.Scan{Range: validRange}},
				OutputFields: []string{"bad\u0080field"},
			},
		},
		{
			name: "duplicate static output fields",
			query: &plan.Query{
				Operators:    []plan.Operator{&plan.Scan{Range: validRange}},
				OutputFields: []string{"host", "host"},
			},
		},
		{
			name: "duplicate dynamic output fields",
			query: &plan.Query{
				Operators: []plan.Operator{&plan.Scan{Range: validRange}},
				DynamicOutput: &plan.DynamicSeriesOutput{
					FixedFields: []string{"_time", "_time"},
					MaxSeries:   1,
				},
			},
		},
		{
			name: "invalid Project mode",
			query: &plan.Query{Operators: []plan.Operator{&plan.Project{
				Mode:   plan.ProjectModeInvalid,
				Fields: []plan.FieldRef{{Name: "host"}},
				Range:  validRange,
			}}},
		},
		{
			name: "both static and dynamic output",
			query: &plan.Query{
				Operators:    []plan.Operator{&plan.Scan{Range: validRange}},
				OutputFields: []string{"host"},
				DynamicOutput: &plan.DynamicSeriesOutput{
					FixedFields: []string{"_time"}, MaxSeries: 1,
				},
			},
		},
		{
			name: "dynamic output over bound",
			query: &plan.Query{
				Operators: []plan.Operator{&plan.Scan{Range: validRange}},
				DynamicOutput: &plan.DynamicSeriesOutput{
					FixedFields: []string{"_time"}, MaxSeries: maximumDynamicFields + 1,
				},
			},
		},
		{
			name: "too many authored stages",
			query: &plan.Query{Operators: repeatLimitOperators(
				int(maximumAuthoredPlanStages)+1,
				validRange,
			)},
		},
		{
			name: "too many absolute stages",
			query: &plan.Query{Operators: repeatLimitOperators(
				int(maximumPlanStages)+1,
				validRange,
			)},
		},
		{
			name: "too many stage outputs",
			query: &plan.Query{Operators: []plan.Operator{&plan.Project{
				Mode: plan.ProjectModeTable,
				Fields: repeatFieldRefs(
					int(maximumStageFields)+1,
					"field",
				),
				Range: validRange,
			}}},
		},
		{
			name: "oversized source",
			query: &plan.Query{Operators: []plan.Operator{
				&plan.Scan{Range: validRange},
			}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testSource := source
			if test.name == "oversized source" {
				testSource = strings.Repeat(
					"x",
					maximumProjectionSourceBytes+1,
				)
			}
			projected, err := projectLogicalPlan(
				context.Background(),
				test.query,
				testSource,
			)
			if !errors.Is(err, searchjobs.ErrInvalidResult) {
				t.Fatalf("error = %v, want ErrInvalidResult", err)
			}
			if !reflect.DeepEqual(projected, LogicalPlan{}) {
				t.Fatalf("partial projection published: %#v", projected)
			}
		})
	}
}

func TestProjectLogicalPlanAcceptsAccumulatedOutputExpansion(t *testing.T) {
	var source strings.Builder
	source.WriteString("| table _raw")
	for index := 0; index < 1_008; index++ {
		fmt.Fprintf(&source, " f%d", index)
	}
	for command := 0; command < 2; command++ {
		source.WriteString(` | rex "`)
		for capture := 0; capture < 16; capture++ {
			fmt.Fprintf(
				&source,
				"(?<r%d_%d>x)",
				command,
				capture,
			)
		}
		source.WriteByte('"')
	}
	snapshot := validInspectionSnapshot()
	snapshot.SPL = source.String()
	logical, err := buildInspectionAuthoredPlan(snapshot)
	if err != nil {
		t.Fatalf("BuildExecutionPlan: %v", err)
	}
	if len(logical.OutputFields) != 1_041 {
		t.Fatalf(
			"accumulated output fields = %d, want 1041",
			len(logical.OutputFields),
		)
	}
	compiled, err := (clickhouse.Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if !validGeneratedSQL(compiled) {
		t.Fatalf(
			"accepted expansion compiled to invalid SQL (%d bytes)",
			len(compiled.SQL),
		)
	}
	projected, err := projectLogicalPlan(
		context.Background(),
		logical,
		snapshot.SPL,
	)
	if err != nil {
		t.Fatal(err)
	}
	if projected.Output.Kind != OutputKindStatic ||
		len(projected.Output.Fields) != len(logical.OutputFields) {
		t.Fatalf(
			"projected accumulated output = kind %d fields %d",
			projected.Output.Kind,
			len(projected.Output.Fields),
		)
	}
	occurrences := len(projected.ReferencedFields) +
		len(projected.Output.Fields)
	for _, stage := range projected.Stages {
		occurrences += len(stage.InputFields) + len(stage.OutputFields)
	}
	const previousOccurrenceCeiling = 4_096
	if occurrences <= previousOccurrenceCeiling {
		t.Fatalf(
			"expanded projection has %d occurrences, want over old ceiling %d",
			occurrences,
			previousOccurrenceCeiling,
		)
	}
}

func TestProjectLogicalPlanHonorsExactFinalOutputBoundary(t *testing.T) {
	outputs := fieldRefNames(
		repeatFieldRefs(int(maximumFinalOutputFields), "field"),
	)
	source := strings.Repeat("x", 32)
	sourceRange := testSourceRange()
	projected, err := projectLogicalPlan(
		context.Background(),
		&plan.Query{
			Operators:    []plan.Operator{&plan.Scan{Range: sourceRange}},
			OutputFields: outputs,
		},
		source,
	)
	if err != nil {
		t.Fatalf("exact final-output bound: %v", err)
	}
	if len(projected.Output.Fields) != int(maximumFinalOutputFields) {
		t.Fatalf(
			"projected final fields = %d, want %d",
			len(projected.Output.Fields),
			maximumFinalOutputFields,
		)
	}

	overBound := append(slices.Clone(outputs), "one_too_many")
	projected, err = projectLogicalPlan(
		context.Background(),
		&plan.Query{
			Operators:    []plan.Operator{&plan.Scan{Range: sourceRange}},
			OutputFields: overBound,
		},
		source,
	)
	if !errors.Is(err, searchjobs.ErrInvalidResult) {
		t.Fatalf("over final-output bound error = %v, want ErrInvalidResult", err)
	}
	if !reflect.DeepEqual(projected, LogicalPlan{}) {
		t.Fatalf("over-bound final projection published partial state: %#v", projected)
	}
}

func TestProjectionBudgetHonorsExactFieldOccurrenceBoundary(t *testing.T) {
	budget := projectionBudget{
		fieldOccurrences: maximumProjectedFieldOccurrences - 1,
	}
	projected, err := budget.projectFields([]string{"last"}, 1)
	if err != nil {
		t.Fatalf("exact occurrence bound: %v", err)
	}
	assertStringsEqual(t, projected, []string{"last"})
	if budget.fieldOccurrences != maximumProjectedFieldOccurrences {
		t.Fatalf(
			"field occurrences = %d, want %d",
			budget.fieldOccurrences,
			maximumProjectedFieldOccurrences,
		)
	}
	if _, err := budget.projectFields(
		[]string{"over"},
		1,
	); !errors.Is(err, searchjobs.ErrInvalidResult) {
		t.Fatalf("over occurrence bound error = %v, want ErrInvalidResult", err)
	}
}

func TestProjectSourceRangeUsesExactUTF8Coordinates(t *testing.T) {
	const source = "α\nβ"
	value := spl.Range{
		Start: spl.Position{Offset: len("α\n"), Line: 2, Column: 1},
		End:   spl.Position{Offset: len(source), Line: 2, Column: 2},
	}
	projected, err := projectSourceRange(source, value)
	if err != nil {
		t.Fatal(err)
	}
	if projected != (SourceRange{
		Start: SourcePosition{
			ByteOffset: uint64(len("α\n")),
			Line:       2,
			Column:     1,
		},
		End: SourcePosition{
			ByteOffset: uint64(len(source)),
			Line:       2,
			Column:     2,
		},
	}) {
		t.Fatalf("projected range = %#v", projected)
	}

	tests := []struct {
		name  string
		value spl.Range
	}{
		{
			name: "mid-rune start",
			value: spl.Range{
				Start: spl.Position{Offset: 1, Line: 1, Column: 2},
				End:   value.End,
			},
		},
		{
			name: "wrong start line",
			value: spl.Range{
				Start: spl.Position{Offset: len("α\n"), Line: 1, Column: 3},
				End:   value.End,
			},
		},
		{
			name: "wrong end column",
			value: spl.Range{
				Start: value.Start,
				End:   spl.Position{Offset: len(source), Line: 2, Column: 3},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := projectSourceRange(source, test.value); !errors.Is(
				err,
				searchjobs.ErrInvalidResult,
			) {
				t.Fatalf("error = %v, want ErrInvalidResult", err)
			}
		})
	}
}

func TestProjectLogicalPlanHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	projected, err := projectLogicalPlan(
		ctx,
		&plan.Query{Operators: []plan.Operator{&plan.Scan{Range: testSourceRange()}}},
		strings.Repeat("x", 32),
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if !reflect.DeepEqual(projected, LogicalPlan{}) {
		t.Fatalf("partial projection published: %#v", projected)
	}
}

func testSourceRange() spl.Range {
	return spl.Range{
		Start: spl.Position{Offset: 0, Line: 1, Column: 1},
		End:   spl.Position{Offset: 1, Line: 1, Column: 2},
	}
}

func sourceRangeProjection(value spl.Range) SourceRange {
	return SourceRange{
		Start: SourcePosition{
			ByteOffset: uint64(value.Start.Offset),
			Line:       uint32(value.Start.Line),
			Column:     uint32(value.Start.Column),
		},
		End: SourcePosition{
			ByteOffset: uint64(value.End.Offset),
			Line:       uint32(value.End.Line),
			Column:     uint32(value.End.Column),
		},
	}
}

func repeatLimitOperators(count int, sourceRange spl.Range) []plan.Operator {
	operators := make([]plan.Operator, count)
	for index := range operators {
		operators[index] = &plan.Limit{Count: 1, Range: sourceRange}
	}
	return operators
}

func repeatFieldRefs(count int, prefix string) []plan.FieldRef {
	fields := make([]plan.FieldRef, count)
	for index := range fields {
		fields[index] = plan.FieldRef{Name: fmt.Sprintf("%s_%04d", prefix, index)}
	}
	return fields
}

func assertStringsEqual(t *testing.T, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("strings = %v, want %v", got, want)
	}
}
