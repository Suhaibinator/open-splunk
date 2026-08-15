package plan

import (
	"errors"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildLookupProducesOrderedRowPreservingEnrichment(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, "index=gradethis | lookup service_catalog service_id AS service_key environment AS env OUTPUTNEW owner AS service_owner tier | table service_key env service_owner tier"),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	lookup, ok := logical.Operators[2].(*Lookup)
	if !ok {
		t.Fatalf("operator 2 = %T, want *Lookup", logical.Operators[2])
	}
	if lookup.LogicalName() != "Lookup" || lookup.SourceRange() != lookup.Range ||
		lookup.DefinitionName != "service_catalog" ||
		lookup.WriteMode != LookupWriteModePreserveExisting {
		t.Fatalf("lookup header = %#v", lookup)
	}
	if len(lookup.Keys) != 2 ||
		lookup.Keys[0].LookupField != "service_id" ||
		lookup.Keys[0].EventField.Name != "service_key" ||
		lookup.Keys[1].LookupField != "environment" ||
		lookup.Keys[1].EventField.Name != "env" {
		t.Fatalf("lookup keys = %#v", lookup.Keys)
	}
	if len(lookup.Outputs) != 2 ||
		lookup.Outputs[0].LookupField != "owner" ||
		lookup.Outputs[0].EventField.Name != "service_owner" ||
		lookup.Outputs[1].LookupField != "tier" ||
		lookup.Outputs[1].EventField.Name != "tier" {
		t.Fatalf("lookup outputs = %#v", lookup.Outputs)
	}
	if !slices.Equal(logical.OutputFields, []string{"service_key", "env", "service_owner", "tier"}) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}
	if shape := spl.ClassifyResultShape(mustParse(t, "index=gradethis | lookup service_catalog service_id AS service_key OUTPUT owner")); shape.Kind != spl.ResultKindEvents || shape.RuntimeNamedColumns {
		t.Fatalf("lookup result shape = %#v", shape)
	}
	if err := ValidateFieldAnalysisEligibility(logical); err != nil {
		t.Fatalf("field analysis rejected lookup event relation: %v", err)
	}
}

func TestBuildLookupUpdatesKnownSchemaWithoutReorderingExistingFields(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, "index=gradethis | stats count AS service_key | lookup catalog id AS service_key OUTPUT owner AS service_owner service_key AS service_key"),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"service_key", "service_owner"}) {
		t.Fatalf("output fields = %v, want existing field then appended output", logical.OutputFields)
	}
}

func TestAnalyzeStagesLookupReadsOnlyEventKeyFields(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, "index=gradethis | lookup catalog id AS service_key env AS environment OUTPUT owner AS service_owner tier"),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	analysis, err := AnalyzeStages(logical)
	if err != nil {
		t.Fatalf("AnalyzeStages: %v", err)
	}
	if got := analysis.Stages[2].ReferencedFields; !slices.Equal(got, []string{"environment", "service_key"}) {
		t.Fatalf("lookup stage fields = %v, want event key inputs only", got)
	}
}

func TestBuildLookupRejectsReservedOpenEventPayload(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		"index=gradethis | lookup catalog id AS fields OUTPUT owner",
		"index=gradethis | lookup catalog id AS service_key OUTPUT owner AS fields",
	} {
		_, err := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil))
		assertDiagnosticCode(t, err, "SPL_AMBIGUOUS_LOOKUP_FIELD")
	}
	for _, source := range []string{
		"index=gradethis | stats count AS fields | lookup catalog id AS fields OUTPUT owner",
		"index=gradethis | stats count AS service_key | lookup catalog id AS service_key OUTPUT owner AS fields",
	} {
		if _, err := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil)); err != nil {
			t.Fatalf("Build(%q): %v", source, err)
		}
	}
}

func TestBuildLookupOverwriteEndsPhysicalIndexScopeRecognition(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, "index=gradethis | lookup catalog id AS service_key OUTPUT source_index AS index | search index=secret"),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build calculated index search: %v", err)
	}
	if !slices.Equal(logical.EffectiveIndexes, []string{"gradethis"}) {
		t.Fatalf("effective indexes = %v", logical.EffectiveIndexes)
	}

	_, err = Build(
		mustParse(t, "index=gradethis | search index=secret | lookup catalog id AS service_key OUTPUT source_index AS index"),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_INDEX_FORBIDDEN")

	_, err = Build(
		mustParse(t, "index=gradethis | lookup catalog id AS service_key OUTPUTNEW source_index AS index | search index=secret"),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_INDEX_FORBIDDEN")
}

func TestBuildLookupOverwriteInvalidatesCanonicalTimeButOutputNewPreservesIt(t *testing.T) {
	t.Parallel()

	_, err := Build(
		mustParse(t, "index=gradethis | lookup catalog id AS service_key OUTPUT timestamp AS _time | timechart span=5m count"),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_TIMECHART_TIME_FIELD")

	logical, err := Build(
		mustParse(t, "index=gradethis | lookup catalog id AS service_key OUTPUTNEW timestamp AS _time | timechart span=5m count"),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("OUTPUTNEW canonical time Build: %v", err)
	}
	if _, ok := logical.Operators[3].(*Timechart); !ok {
		t.Fatalf("operator 3 = %T, want *Timechart", logical.Operators[3])
	}
}

func TestBuildLookupRevalidatesForgedAST(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*spl.LookupCommand)
	}{
		{name: "empty definition", mutate: func(command *spl.LookupCommand) { command.DefinitionName = "" }},
		{name: "no keys", mutate: func(command *spl.LookupCommand) { command.Keys = nil }},
		{name: "too many keys", mutate: func(command *spl.LookupCommand) {
			command.Keys = append(command.Keys, command.Keys[0], command.Keys[0], command.Keys[0], command.Keys[0])
		}},
		{name: "no outputs", mutate: func(command *spl.LookupCommand) { command.Outputs = nil }},
		{name: "invalid mode", mutate: func(command *spl.LookupCommand) { command.OutputMode = spl.LookupOutputModeInvalid }},
		{name: "duplicate event output", mutate: func(command *spl.LookupCommand) {
			command.Outputs = append(command.Outputs, spl.LookupOutputMapping{
				LookupField: "tier", EventField: command.Outputs[0].EventField,
			})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			query := mustParse(t, "index=gradethis | lookup catalog id AS service_key OUTPUT owner")
			test.mutate(query.Commands[0].(*spl.LookupCommand))
			_, err := Build(query, testScope([]string{"gradethis"}, nil))
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) ||
				(diagnostic.Code != "SPL_UNSUPPORTED_LOOKUP_SYNTAX" &&
					diagnostic.Code != "SPL_QUERY_TOO_COMPLEX") {
				t.Fatalf("Build error = %v, want lookup diagnostic", err)
			}
		})
	}
}

func TestAnalyzeAndFieldAnalysisRejectForgedLookupContract(t *testing.T) {
	t.Parallel()

	validField, err := ResolveField("service_key", spl.Range{})
	if err != nil {
		t.Fatalf("ResolveField: %v", err)
	}
	lookup := &Lookup{
		DefinitionName: "catalog",
		Keys: []LookupKey{{
			LookupField: "id",
			EventField:  validField,
		}},
		Outputs: []LookupOutput{{
			LookupField: "owner",
			EventField:  FieldRef{},
		}},
		WriteMode: LookupWriteModeOverwrite,
	}
	query := &Query{Operators: []Operator{&Scan{}, lookup}}
	if _, err := Analyze(query); err == nil {
		t.Fatal("Analyze accepted forged lookup output")
	}
	var diagnostic *Diagnostic
	if !errors.As(ValidateFieldAnalysisEligibility(query), &diagnostic) ||
		diagnostic.Code != fieldAnalysisPipelineDiagnosticCode {
		t.Fatalf("field analysis error = %v", diagnostic)
	}
}
