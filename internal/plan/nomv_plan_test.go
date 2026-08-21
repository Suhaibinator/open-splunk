package plan

import (
	"errors"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildNoMultivaluePresentationOperator(t *testing.T) {
	t.Parallel()
	parsed, err := spl.Parse(`index=gradethis | table _time event_id users | nomv users`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	logical, err := Build(parsed, testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	operator, ok := logical.Operators[len(logical.Operators)-1].(*NoMultivalue)
	if !ok || operator == nil {
		t.Fatalf("last operator = %T, want *NoMultivalue", logical.Operators[len(logical.Operators)-1])
	}
	if operator.LogicalName() != "NoMultivalue" || operator.Input.Name != "users" ||
		operator.Input.Range == (spl.Range{}) || operator.Range == (spl.Range{}) {
		t.Fatalf("operator = %#v", operator)
	}
	if want := []string{"_time", "event_id", "users"}; !slices.Equal(logical.OutputFields, want) {
		t.Fatalf("OutputFields = %v, want %v", logical.OutputFields, want)
	}
	analysis, err := Analyze(logical)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if !slices.Contains(analysis.ReferencedFields, "users") {
		t.Fatalf("ReferencedFields = %v, want users", analysis.ReferencedFields)
	}
	if err := ValidateTimelineEligibility(logical); err != nil {
		t.Fatalf("ValidateTimelineEligibility: %v", err)
	}
	if err := ValidateFieldAnalysisEligibility(logical); err != nil {
		t.Fatalf("ValidateFieldAnalysisEligibility: %v", err)
	}
}

func TestNoMVDoesNotCreateAMissingClosedSchemaField(t *testing.T) {
	t.Parallel()
	parsed, err := spl.Parse(`index=gradethis | table event_id | nomv missing`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	logical, err := Build(parsed, testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if want := []string{"event_id"}; !slices.Equal(logical.OutputFields, want) {
		t.Fatalf("OutputFields = %v, want unchanged %v", logical.OutputFields, want)
	}
}

func TestNoMVReservedFieldsPayloadRequiresExactSchema(t *testing.T) {
	t.Parallel()
	raw := `index=gradethis | nomv fields`
	parsed, err := spl.Parse(raw)
	if err != nil {
		t.Fatalf("Parse raw: %v", err)
	}
	_, err = Build(parsed, testScope([]string{"gradethis"}, nil))
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_AMBIGUOUS_NOMV_FIELD" {
		t.Fatalf("Build raw error = %v, want SPL_AMBIGUOUS_NOMV_FIELD", err)
	}
	if got := raw[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset]; got != "fields" {
		t.Fatalf("diagnostic range text = %q", got)
	}

	exact, err := spl.Parse(`index=gradethis | table fields | nomv fields`)
	if err != nil {
		t.Fatalf("Parse exact: %v", err)
	}
	if _, err := Build(exact, testScope([]string{"gradethis"}, nil)); err != nil {
		t.Fatalf("Build exact: %v", err)
	}
}

func TestBuildNoMVRejectsForgedMetadata(t *testing.T) {
	t.Parallel()
	r := spl.Range{Start: spl.Position{Offset: 2}, End: spl.Position{Offset: 8}}
	for _, command := range []*spl.NoMVCommand{
		nil,
		{Field: "users*", FieldRange: r, Range: r},
		{Field: "_time", FieldRange: r, Range: r},
		{Field: "__os_pipeline_ordinal", FieldRange: r, Range: r},
	} {
		if _, err := buildNoMultivalue(command); err == nil {
			t.Fatalf("buildNoMultivalue(%#v) unexpectedly succeeded", command)
		}
	}

	var typedNil *NoMultivalue
	query := &Query{Operators: []Operator{typedNil}}
	if _, err := Analyze(query); err == nil {
		t.Fatal("Analyze accepted typed-nil NoMultivalue")
	}
	forged := &Query{Operators: []Operator{
		&Scan{TenantID: "tenant", Indexes: []string{"gradethis"}},
		&NoMultivalue{Input: FieldRef{Name: "_time", Range: r}, Range: r},
	}}
	if _, err := Analyze(forged); err == nil {
		t.Fatal("Analyze accepted internal-field NoMultivalue")
	}
}
