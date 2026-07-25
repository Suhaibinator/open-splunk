package plan

import (
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/splpath"
)

func TestBuildSpathProducesRowPreservingJSONExtraction(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | spath input=payload output=first_price path=vendor.products{0}.price | table first_price`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	extract, ok := logical.Operators[2].(*ExtractJSON)
	if !ok {
		t.Fatalf("operator 2 = %T, want *ExtractJSON", logical.Operators[2])
	}
	if extract.Input.Name != "payload" || extract.Output.Name != "first_price" ||
		extract.Path != "vendor.products{0}.price" {
		t.Fatalf("extract = %#v", extract)
	}
	wantSteps := []splpath.Step{
		{Key: "vendor"},
		{Key: "products", HasIndex: true, Index: 0},
		{Key: "price"},
	}
	if !slices.Equal(extract.Steps, wantSteps) {
		t.Fatalf("steps = %#v, want %#v", extract.Steps, wantSteps)
	}
	if !slices.Equal(logical.OutputFields, []string{"first_price"}) {
		t.Fatalf("output fields = %v, want [first_price]", logical.OutputFields)
	}
}

func TestBuildSpathDefaultsAndKnownSchemaOutput(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | stats count AS payload | spath path=server.name | spath input=payload output=parsed path=value`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	first := logical.Operators[3].(*ExtractJSON)
	if first.Input.Name != "_raw" || first.Output.Name != "server.name" {
		t.Fatalf("defaulted spath = %#v", first)
	}
	second := logical.Operators[4].(*ExtractJSON)
	if second.Input.Name != "payload" || second.Output.Name != "parsed" {
		t.Fatalf("explicit spath = %#v", second)
	}
	if !slices.Equal(logical.OutputFields, []string{"payload", "server.name", "parsed"}) {
		t.Fatalf("known output schema = %v", logical.OutputFields)
	}
}

func TestBuildSpathRejectsReservedOpenEventFieldsPayload(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | spath input=fields output=value path=child`,
		`index=gradethis | spath input=_raw output=fields path=child`,
	} {
		_, err := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil))
		assertDiagnosticCode(t, err, "SPL_AMBIGUOUS_SPATH_FIELD")
	}

	for _, source := range []string{
		`index=gradethis | stats count AS fields | spath input=fields output=value path=child`,
		`index=gradethis | stats count AS value | spath input=_raw output=fields path=child`,
	} {
		if _, err := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil)); err != nil {
			t.Fatalf("Build(%q): %v", source, err)
		}
	}
}

func TestBuildSpathOutputIndexEndsPhysicalScopeRecognition(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | spath output=index path=payload.index | search index=secret`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build calculated index search: %v", err)
	}
	if !slices.Equal(logical.EffectiveIndexes, []string{"gradethis"}) {
		t.Fatalf("effective indexes = %v", logical.EffectiveIndexes)
	}

	_, err = Build(
		mustParse(t, `index=gradethis | search index=secret | spath output=index path=payload.index`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_INDEX_FORBIDDEN")
}

func TestBuildSpathOutputTimeInvalidatesCanonicalClock(t *testing.T) {
	t.Parallel()

	_, err := Build(
		mustParse(t, `index=gradethis | spath output=_time path=timestamp | timechart span=5m count BY level`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_TIMECHART_TIME_FIELD")
}

func TestBuildSpathRevalidatesForgedPathMetadata(t *testing.T) {
	t.Parallel()

	query := mustParse(t, `index=gradethis | spath output=value path=payload.value`)
	command := query.Commands[0].(*spl.SpathCommand)
	command.Steps = []splpath.Step{{Key: "different"}}
	_, err := Build(query, testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_INVALID_QUERY")

	query = mustParse(t, `index=gradethis | spath output=value path=payload.value`)
	command = query.Commands[0].(*spl.SpathCommand)
	command.Path = strings.Repeat("x", splpath.MaximumPathBytes+1)
	_, err = Build(query, testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_QUERY_TOO_COMPLEX")
}

func TestBuildSpathSharesCalculatedOutputCeilingWithRex(t *testing.T) {
	t.Parallel()

	var source strings.Builder
	source.WriteString(`index=gradethis | spath output=json_value path=value`)
	captureIndex := 0
	appendRex := func(count int) {
		source.WriteString(` | rex "`)
		for range count {
			source.WriteString(`(?<capture_`)
			source.WriteString(strconv.Itoa(captureIndex))
			source.WriteString(`>x)`)
			captureIndex++
		}
		source.WriteString(`"`)
	}
	for _, count := range []int{16, 16, 16, 15} {
		appendRex(count)
	}
	accepted := source.String()
	if _, err := Build(mustParse(t, accepted), testScope([]string{"gradethis"}, nil)); err != nil {
		t.Fatalf("Build(%d calculated outputs): %v", maxExtractionOutputsPerQuery, err)
	}

	rejected := source.String() + ` | rex "(?<overflow>y)"`
	_, err := Build(mustParse(t, rejected), testScope([]string{"gradethis"}, nil))
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
		t.Fatalf("Build(over output ceiling) error = %v, want SPL_QUERY_TOO_COMPLEX", err)
	}
}

func TestBuildSpathBoundsCumulativeJSONEvaluationWork(t *testing.T) {
	t.Parallel()

	var source strings.Builder
	source.WriteString(`index=gradethis`)
	acceptedStages := splpath.MaximumEvaluationWorkUnits / splpath.EvaluationWorkUnits(
		[]splpath.Step{{Key: "value"}},
	)
	for index := 0; index < acceptedStages; index++ {
		source.WriteString(` | spath output=value_`)
		source.WriteString(strconv.Itoa(index))
		source.WriteString(` path=value`)
	}
	if _, err := Build(mustParse(t, source.String()), testScope([]string{"gradethis"}, nil)); err != nil {
		t.Fatalf("Build(maximum bounded spath work): %v", err)
	}

	source.WriteString(` | spath output=overflow path=value`)
	_, err := Build(mustParse(t, source.String()), testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_QUERY_TOO_COMPLEX")
}

func TestBuildSpathDefaultOutputStillObeysFieldNameBounds(t *testing.T) {
	t.Parallel()

	key := strings.Repeat("x", splpath.MaximumKeyBytes)
	_, err := Build(
		mustParse(t, `index=gradethis | spath path=`+key+`{0}`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_QUERY_TOO_COMPLEX")

	if _, err := Build(
		mustParse(t, `index=gradethis | spath output=value path=`+key+`{0}`),
		testScope([]string{"gradethis"}, nil),
	); err != nil {
		t.Fatalf("Build explicit bounded output: %v", err)
	}
}
