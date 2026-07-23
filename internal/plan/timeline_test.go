package plan

import (
	"errors"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestValidateTimelineEligibilityAcceptsEventPipelines(t *testing.T) {
	queries := []string{
		`index=gradethis level=error`,
		`index=gradethis | search level=error | where status>=500`,
		`index=gradethis | eval duration_ms=tonumber(duration) | where duration_ms>10`,
		`index=gradethis | rename logger AS component | search component=api`,
		`index=gradethis | fields message`,
		`index=gradethis | fields - host`,
		`index=gradethis | table _time, message`,
		`index=gradethis | sort -_time`,
		`index=gradethis | sort 0 -_time`,
		`index=gradethis | dedup 2 host | head 20`,
		`index=gradethis | tail 20`,
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

func TestValidateTimelineEligibilityRejectsTransformedOrSyntheticTime(t *testing.T) {
	tests := []struct {
		source string
		code   string
	}{
		{`index=gradethis | fields - _time`, "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"},
		{`index=gradethis | table message`, "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"},
		{`index=gradethis | eval _time=_indextime`, "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"},
		{`index=gradethis | eval _time=_time`, "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"},
		{`index=gradethis | rename _time AS observed_at`, "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"},
		{`index=gradethis | rename observed_at AS _time`, "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"},
		{`index=gradethis | fields - _time | table _time`, "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"},
		{`index=gradethis | rename _time AS observed_at | rename observed_at AS _time`, "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"},
		{`index=gradethis | stats count`, "SPL_UNSUPPORTED_TIMELINE_PIPELINE"},
		{`index=gradethis | stats count BY _time`, "SPL_UNSUPPORTED_TIMELINE_PIPELINE"},
		{`index=gradethis | top level`, "SPL_UNSUPPORTED_TIMELINE_PIPELINE"},
		{`index=gradethis | rare level`, "SPL_UNSUPPORTED_TIMELINE_PIPELINE"},
		{`index=gradethis | timechart span=5m count BY level`, "SPL_UNSUPPORTED_TIMELINE_PIPELINE"},
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
