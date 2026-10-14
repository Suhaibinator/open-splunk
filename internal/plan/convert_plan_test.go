package plan

import (
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildConvertIssue133AndExactTypes(t *testing.T) {
	t.Parallel()
	source := `index=gradethis | stats min(_time) AS first_seen latest(_time) AS last_seen values(upload_batch_id) AS upload_batch_id values(source_file_id) AS source_file_id values(workflow_type) AS workflow_types values(event_name) AS events BY section_assessment_submission_id | sort - last_seen | convert ctime(first_seen) ctime(last_seen) | head 20`
	logical, err := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatal(err)
	}
	var conversion *Convert
	for _, operator := range logical.Operators {
		if candidate, ok := operator.(*Convert); ok {
			conversion = candidate
		}
	}
	if conversion == nil || len(conversion.Assignments) != 2 || conversion.TimeFormat != spl.DefaultConvertTimeFormat {
		t.Fatalf("conversion = %#v", conversion)
	}
	if conversion.Assignments[0].Input.Name != "first_seen" || conversion.Assignments[1].Output.Name != "last_seen" {
		t.Fatalf("assignments = %#v", conversion.Assignments)
	}
	if _, err := Analyze(logical); err != nil {
		t.Fatalf("Analyze: %v", err)
	}
}

func TestBuildConvertWildcardsExclusionsAndSequentialAliases(t *testing.T) {
	t.Parallel()
	logical, err := Build(mustParse(t, `index=gradethis | table _time,a,b | convert num(a) AS copied num(*) none(b) num(b) AS final none(copied)`), testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatal(err)
	}
	conversion := logical.Operators[len(logical.Operators)-1].(*Convert)
	inputs := make([]string, len(conversion.Assignments))
	outputs := make([]string, len(conversion.Assignments))
	for index, assignment := range conversion.Assignments {
		inputs[index], outputs[index] = assignment.Input.Name, assignment.Output.Name
	}
	if !slices.Equal(inputs, []string{"a", "a", "b"}) || !slices.Equal(outputs, []string{"copied", "a", "final"}) {
		t.Fatalf("inputs/outputs = %v/%v", inputs, outputs)
	}
	if !slices.Equal(logical.OutputFields, []string{"_time", "a", "b", "copied", "final"}) {
		t.Fatalf("fields = %v", logical.OutputFields)
	}
	empty, err := Build(mustParse(t, `index=gradethis | table a | convert num(z*)`), testScope([]string{"gradethis"}, nil))
	if err != nil || len(empty.Operators[len(empty.Operators)-1].(*Convert).Assignments) != 0 {
		t.Fatalf("zero matches = %#v/%v", empty, err)
	}
	sequential, err := Build(mustParse(t, `index=gradethis | table a | convert num(a) AS copied num(cop*) num(a) AS copied`), testScope([]string{"gradethis"}, nil))
	if err != nil || len(sequential.Operators[len(sequential.Operators)-1].(*Convert).Assignments) != 3 {
		t.Fatalf("sequential = %#v/%v", sequential, err)
	}
}

func TestBuildConvertPreservesAnalysisAndProtectsTimeAndIndex(t *testing.T) {
	t.Parallel()
	logical, err := Build(mustParse(t, `index=gradethis | convert num(bytes) AS parsed`), testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateTimelineEligibility(logical); err != nil {
		t.Fatal(err)
	}
	if err := ValidateFieldAnalysisEligibility(logical); err != nil {
		t.Fatal(err)
	}
	rewritten, err := Build(mustParse(t, `index=gradethis | convert ctime(_time)`), testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatal(err)
	}
	assertDiagnosticCode(t, ValidateTimelineEligibility(rewritten), "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD")
	renamedIndex, err := Build(mustParse(t, `index=gradethis | convert rmunit(bytes) AS index | search index=other`), testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(renamedIndex.EffectiveIndexes, []string{"gradethis"}) {
		t.Fatalf("index scope = %v", renamedIndex.EffectiveIndexes)
	}
	_, err = Build(mustParse(t, `index=gradethis | convert auto(*)`), testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_CONVERT_WILDCARD")
}

func TestBuildConvertRejectsForgedCommandsAndWriteOverflow(t *testing.T) {
	t.Parallel()
	for _, mutate := range []func(*spl.ConvertCommand){
		func(command *spl.ConvertCommand) { command.Clauses = nil },
		func(command *spl.ConvertCommand) { command.Clauses[0].Function = spl.ConvertFunction(200) },
		func(command *spl.ConvertCommand) { command.Clauses[0].Output = "changed" },
		func(command *spl.ConvertCommand) {
			command.Clauses[0].Input = "__os_private"
			command.Clauses[0].Output = "__os_private"
		},
		func(command *spl.ConvertCommand) { command.TimeFormat = "%c" },
	} {
		parsed := mustParse(t, `index=gradethis | convert ctime(value)`)
		mutate(parsed.Commands[0].(*spl.ConvertCommand))
		if _, err := Build(parsed, testScope([]string{"gradethis"}, nil)); err == nil {
			t.Fatal("accepted forged command")
		}
	}
	fields := make([]string, 33)
	for index := range fields {
		fields[index] = "f" + strings.Repeat("x", index+1)
	}
	_, err := Build(mustParse(t, "index=gradethis | table "+strings.Join(fields, ",")+" | convert num(f*) num(f*)"), testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_QUERY_TOO_COMPLEX")
}

func TestBuildConvertRuntimeWildcardIncludesQuotedAlias(t *testing.T) {
	t.Parallel()
	parsed := mustParse(t, `index=gradethis | convert num(bytes) AS "First Seen" num(First*)`)
	scope := testScope([]string{"gradethis"}, nil)
	preparation, err := PrepareStatsWildcard(parsed, scope)
	if err != nil {
		t.Fatal(err)
	}
	matches := []StatsWildcardInventoryMatch{{Ordinal: 1, Field: "First Seen"}}
	if !slices.Equal(preparation.Request().AdditionalMatches(), matches) {
		t.Fatalf("alias matches = %#v", preparation.Request().AdditionalMatches())
	}
	expansion, err := ValidateStatsWildcardInventory(preparation.Request(), matches)
	if err != nil {
		t.Fatalf("quoted alias discovery: %v", err)
	}
	logical, err := BuildWithStatsWildcardExpansion(parsed, scope, expansion)
	if err != nil {
		t.Fatalf("quoted alias replay: %v", err)
	}
	conversion := logical.Operators[len(logical.Operators)-1].(*Convert)
	if len(conversion.Assignments) != 2 || conversion.Assignments[1].Input.Name != "First Seen" {
		t.Fatalf("conversion = %#v", conversion)
	}
}

func TestBuildConvertWildcardIncludesOrdinaryFieldsColumn(t *testing.T) {
	t.Parallel()
	logical, err := Build(mustParse(t, `index=gradethis | stats count AS fields | convert ctime(*)`), testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatal(err)
	}
	conversion := logical.Operators[len(logical.Operators)-1].(*Convert)
	if len(conversion.Assignments) != 1 || conversion.Assignments[0].Input.Name != "fields" || conversion.Assignments[0].Output.Name != "fields" {
		t.Fatalf("ordinary fields output was excluded: %#v", conversion)
	}
}

func TestBuildConvertPreservesLiteralStatisticsFields(t *testing.T) {
	t.Parallel()
	scope := testScope([]string{"gradethis"}, nil)
	closed, err := Build(mustParse(t, `index=gradethis | stats count AS ".com" | convert num(*)`), scope)
	if err != nil {
		t.Fatal(err)
	}
	conversion := closed.Operators[len(closed.Operators)-1].(*Convert)
	if len(conversion.Assignments) != 1 || !slices.Equal(conversion.Assignments[0].Input.Path, []string{".com"}) || !slices.Equal(conversion.Assignments[0].Output.Path, []string{".com"}) {
		t.Fatalf("literal statistics field = %#v", conversion)
	}
	if _, err := Analyze(closed); err != nil {
		t.Fatalf("literal analysis: %v", err)
	}
	parsed := mustParse(t, `index=gradethis | convert num(value) AS ".com" num(.*)`)
	preparation, err := PrepareStatsWildcard(parsed, scope)
	if err != nil {
		t.Fatal(err)
	}
	expansion, err := ValidateStatsWildcardInventory(preparation.Request(), []StatsWildcardInventoryMatch{{Ordinal: 1, Field: ".com"}})
	if err != nil {
		t.Fatalf("literal alias discovery: %v", err)
	}
	reopened, err := BuildWithStatsWildcardExpansion(parsed, scope, expansion)
	if err != nil {
		t.Fatalf("literal alias replay: %v", err)
	}
	conversion = reopened.Operators[len(reopened.Operators)-1].(*Convert)
	if len(conversion.Assignments) != 2 || !slices.Equal(conversion.Assignments[1].Input.Path, []string{".com"}) {
		t.Fatalf("literal alias replay = %#v", conversion)
	}
}
