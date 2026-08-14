package plan

import (
	"errors"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestValidateTimelineEligibilityAcceptsEventPipelines(t *testing.T) {
	queries := []string{
		`index=gradethis level=error`,
		`index=gradethis | search level=error | where status>=500`,
		`index=gradethis | eval duration_ms=tonumber(duration) | where duration_ms>10`,
		`index=gradethis | rex "(?<request_id>request_id=\w+)"`,
		`index=gradethis | rename logger AS component | search component=api`,
		`index=gradethis | fields message`,
		`index=gradethis | fields - host`,
		`index=gradethis | table _time, message`,
		`index=gradethis | bin severity span=10`,
		`index=gradethis | bin _time span=5m AS bucket_time`,
		`index=gradethis | sort -_time`,
		`index=gradethis | sort 0 -_time`,
		`index=gradethis | dedup 2 host | head 20`,
		`index=gradethis | tail 20`,
		`index=gradethis | regex message!="^debug$"`,
		`index=gradethis | reverse`,
		`index=gradethis | strcat host "/" source route`,
		`index=gradethis | fillnull value="unknown" optional`,
		`index=gradethis | addtotals fieldname=total bytes duration`,
		`index=gradethis | sort 0 +_time | delta bytes AS change p=2`,
		`index=gradethis | makemv delim="," allowempty=true tags`,
	}
	for _, source := range queries {
		t.Run(source, func(t *testing.T) {
			logical, err := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil))
			if err != nil {
				t.Fatalf("Build() error = %v", err)
			}
			if err := ValidateTimelineEligibility(logical); err != nil {
				t.Fatalf("ValidateTimelineEligibility() error = %v", err)
			}
		})
	}
}

func TestValidateTimelineEligibilityLocatesV03TimeReplacementExactly(t *testing.T) {
	tests := []struct {
		name        string
		source      string
		targetRange func(*spl.Query) spl.Range
	}{
		{
			name:   "strcat destination",
			source: `index=gradethis | strcat host source _time`,
			targetRange: func(parsed *spl.Query) spl.Range {
				return parsed.Commands[0].(*spl.StrcatCommand).DestinationRange
			},
		},
		{
			name:   "fillnull field",
			source: `index=gradethis | fillnull value="zero" host _time source`,
			targetRange: func(parsed *spl.Query) spl.Range {
				return parsed.Commands[0].(*spl.FillNullCommand).Fields[1].Range
			},
		},
		{
			name:   "addtotals destination",
			source: `index=gradethis | addtotals fieldname=_time bytes duration`,
			targetRange: func(parsed *spl.Query) spl.Range {
				return parsed.Commands[0].(*spl.AddTotalsCommand).OutputRange
			},
		},
		{
			name:   "delta destination",
			source: `index=gradethis | delta bytes AS _time p=2`,
			targetRange: func(parsed *spl.Query) spl.Range {
				return parsed.Commands[0].(*spl.DeltaCommand).OutputRange
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parsed := mustParse(t, test.source)
			wantRange := test.targetRange(parsed)
			logical, err := Build(parsed, testScope([]string{"gradethis"}, nil))
			if err != nil {
				t.Fatalf("Build() error = %v", err)
			}
			err = ValidateTimelineEligibility(logical)
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != timelineTimeDiagnosticCode {
				t.Fatalf("ValidateTimelineEligibility() error = %v, want %s", err, timelineTimeDiagnosticCode)
			}
			if diagnostic.Range != wantRange {
				t.Fatalf("diagnostic range = %+v, want exact destination %+v", diagnostic.Range, wantRange)
			}
		})
	}
}

func TestValidateTimelineEligibilityRejectsMakeMVTimeAndMVExpand(t *testing.T) {
	base, err := Build(
		mustParse(t, `index=gradethis`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	timeRange := spl.Range{
		Start: spl.Position{Offset: 37, Line: 1, Column: 38},
		End:   spl.Position{Offset: 42, Line: 1, Column: 43},
	}
	commandRange := spl.Range{
		Start: spl.Position{Offset: 30, Line: 1, Column: 31},
		End:   timeRange.End,
	}
	makeMV := *base
	makeMV.Operators = append(slices.Clone(base.Operators), &MakeMultivalue{
		Input:     FieldRef{Name: "_time", Range: timeRange},
		Delimiter: ",",
		Range:     commandRange,
	})
	err = ValidateTimelineEligibility(&makeMV)
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != timelineTimeDiagnosticCode ||
		diagnostic.Range != timeRange {
		t.Fatalf("makemv _time diagnostic = %#v, %v", diagnostic, err)
	}

	expand := *base
	expand.Operators = append(slices.Clone(base.Operators), &ExpandMultivalue{
		Input:        FieldRef{Name: "tags", Range: timeRange},
		QueryOrdinal: 1,
		Range:        commandRange,
	})
	err = ValidateTimelineEligibility(&expand)
	diagnostic = nil
	if !errors.As(err, &diagnostic) || diagnostic.Code != timelinePipelineDiagnosticCode ||
		diagnostic.Range != commandRange {
		t.Fatalf("mvexpand diagnostic = %#v, %v", diagnostic, err)
	}
}

func TestValidateTimelineEligibilityRejectsTransformedOrSyntheticTime(t *testing.T) {
	tests := []struct {
		source string
		code   string
	}{
		{`index=gradethis | fields - _time`, "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"},
		{`index=gradethis | table message`, "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"},
		{`index=gradethis | eval _time=_indextime`, "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"},
		{`index=gradethis | eval _time=_time`, "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"},
		{`index=gradethis | rex "(?<_time>\d+)"`, "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"},
		{`index=gradethis | rename _time AS observed_at`, "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"},
		{`index=gradethis | rename observed_at AS _time`, "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"},
		{`index=gradethis | bin _time span=5m`, "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"},
		{`index=gradethis | bin severity span=10 AS _time`, "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"},
		{`index=gradethis | fields - _time | table _time`, "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"},
		{`index=gradethis | rename _time AS observed_at | rename observed_at AS _time`, "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"},
		{`index=gradethis | stats count`, "SPL_UNSUPPORTED_TIMELINE_PIPELINE"},
		{`index=gradethis | stats count BY _time`, "SPL_UNSUPPORTED_TIMELINE_PIPELINE"},
		{`index=gradethis | top level`, "SPL_UNSUPPORTED_TIMELINE_PIPELINE"},
		{`index=gradethis | rare level`, "SPL_UNSUPPORTED_TIMELINE_PIPELINE"},
		{`index=gradethis | timechart span=5m count BY level`, "SPL_UNSUPPORTED_TIMELINE_PIPELINE"},
		{`index=gradethis | chart count OVER path BY level`, "SPL_UNSUPPORTED_TIMELINE_PIPELINE"},
		{`index=gradethis | bin _time span=5m | chart count OVER _time BY level`, "SPL_UNSUPPORTED_TIMELINE_PIPELINE"},
	}
	for _, test := range tests {
		t.Run(test.source, func(t *testing.T) {
			logical, err := Build(mustParse(t, test.source), testScope([]string{"gradethis"}, nil))
			if err != nil {
				t.Fatalf("Build() error = %v", err)
			}
			err = ValidateTimelineEligibility(logical)
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != test.code {
				t.Fatalf("ValidateTimelineEligibility() error = %v, want diagnostic %q", err, test.code)
			}
			if diagnostic.Range.Start.Offset < 0 || diagnostic.Range.End.Offset < diagnostic.Range.Start.Offset {
				t.Fatalf("diagnostic range = %+v", diagnostic.Range)
			}
		})
	}
}

func TestValidateTimelineEligibilityRejectsForgedPlans(t *testing.T) {
	valid, err := Build(mustParse(t, `index=gradethis`), testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	tests := []struct {
		name  string
		query *Query
	}{
		{name: "nil", query: nil},
		{name: "empty", query: &Query{}},
		{name: "no scan", query: &Query{Operators: []Operator{&Filter{}}}},
		{name: "late scan", query: &Query{Operators: []Operator{valid.Operators[0], valid.Operators[0]}}},
		{name: "nil operator", query: &Query{Operators: []Operator{valid.Operators[0], nil}}},
		{name: "unknown", query: &Query{Operators: []Operator{valid.Operators[0], &timelineUnknownOperator{}}}},
		{name: "dynamic output", query: &Query{Operators: valid.Operators, DynamicOutput: &DynamicSeriesOutput{FixedFields: []string{"_time"}, MaxSeries: 1}}},
		{name: "invalid project", query: &Query{Operators: []Operator{valid.Operators[0], &Project{Mode: ProjectModeInvalid}}}},
		{name: "chart without dynamic output", query: &Query{Operators: []Operator{valid.Operators[0], &Chart{}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateTimelineEligibility(test.query)
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_UNSUPPORTED_TIMELINE_PIPELINE" {
				t.Fatalf("ValidateTimelineEligibility() error = %v", err)
			}
		})
	}
}

type timelineUnknownOperator struct{}

func (*timelineUnknownOperator) operator()              {}
func (*timelineUnknownOperator) LogicalName() string    { return "Unknown" }
func (*timelineUnknownOperator) SourceRange() spl.Range { return spl.Range{} }
