package clickhouse

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/knowledge"
)

func TestCompileKnowledgeSelectorDimensionsDistinguishesEmptyAndInvalid(t *testing.T) {
	dimensions := [knowledge.MaximumSelectorDimensions]knowledgeSelectorDimension{}
	compiled, err := compileKnowledgeSelectorDimensions(true, false, dimensions)
	if err != nil {
		t.Fatalf("compile unrestricted: %v", err)
	}
	if compiled.sql != knowledgeSelectorUniversalTuple() || len(compiled.args) != 0 {
		t.Fatalf("unrestricted selector = %s / %#v", compiled.sql, compiled.args)
	}
	if _, err := compileKnowledgeSelectorDimensions(false, false, dimensions); err == nil {
		t.Fatal("invalid absent selector compiled")
	}
	if _, err := compileKnowledgeSelectorDimensions(false, true, dimensions); err == nil {
		t.Fatal("invented constrained inventory compiled")
	}
	for _, hidden := range []knowledge.DimensionRuntimeProgram{
		{ExactLiterals: []string{"main"}},
		{WildcardRE2: `(?s:\A(?:.*)\z)`},
		{Assessment: knowledge.MatcherTransitionAssessment{Initial: 1}},
	} {
		dimensions[0].program = hidden
		if _, err := compileKnowledgeSelectorDimensions(true, false, dimensions); err == nil {
			t.Fatalf("hidden unconstrained authority compiled: %+v", hidden)
		}
	}
}

func TestCompileKnowledgeSelectorDimensionsPinsLazyOrderChargesAndParameters(t *testing.T) {
	dimensions := [knowledge.MaximumSelectorDimensions]knowledgeSelectorDimension{}
	indexExact := []string{"audit", "main"}
	dimensions[0] = knowledgeSelectorDimension{
		constrained: true,
		program: knowledge.DimensionRuntimeProgram{
			ExactLiterals: indexExact,
			WildcardRE2:   `(?s:\A(?:prod.*)\z)`,
			Assessment: knowledge.MatcherTransitionAssessment{
				Initial: 2, PerInputByte: 13, Final: 5,
			},
		},
	}
	dimensions[knowledge.DimensionHost-knowledge.DimensionIndex] = knowledgeSelectorDimension{
		constrained: true,
		program: knowledge.DimensionRuntimeProgram{
			ExactLiterals: []string{"api-1"},
		},
	}
	compiled, err := compileKnowledgeSelectorDimensions(false, true, dimensions)
	if err != nil {
		t.Fatalf("compile selector: %v", err)
	}
	for _, required := range []string{
		`assumeNotNull("index")`, `assumeNotNull("host")`,
		"isValidUTF8", "toUInt128(length", "toUInt128(13)",
		KnowledgeSelectorInvalidUTF8Marker, KnowledgeSelectorValueLimitMarker,
	} {
		if !strings.Contains(compiled.sql, required) {
			t.Fatalf("selector SQL omits %q:\n%s", required, compiled.sql)
		}
	}
	if strings.Count(compiled.sql, "match(") != 1 {
		t.Fatalf("wildcard matcher occurrences = %d, want 1\n%s", strings.Count(compiled.sql, "match("), compiled.sql)
	}
	if strings.Index(compiled.sql, KnowledgeSelectorValueLimitMarker) >
		strings.Index(compiled.sql, KnowledgeSelectorInvalidUTF8Marker) {
		t.Fatalf("dual-invalid value precedence is not size before UTF-8:\n%s", compiled.sql)
	}
	if strings.Index(compiled.sql, `assumeNotNull("host")`) >
		strings.Index(compiled.sql, `assumeNotNull("index")`) {
		t.Fatalf("later dimension is not nested in the earlier match branch:\n%s", compiled.sql)
	}
	wantArgs := []any{[]string{"api-1"}, `(?s:\A(?:prod.*)\z)`, []string{"audit", "main"}}
	if !reflect.DeepEqual(compiled.args, wantArgs) {
		t.Fatalf("selector args = %#v, want %#v", compiled.args, wantArgs)
	}
	indexExact[0] = "mutated"
	if !slices.Equal(compiled.args[2].([]string), []string{"audit", "main"}) {
		t.Fatal("selector arguments alias caller exact literals")
	}
}

func TestCompileKnowledgeSelectorDimensionRejectsImpossibleRuntimeAuthority(t *testing.T) {
	for _, test := range []struct {
		name    string
		program knowledge.DimensionRuntimeProgram
	}{
		{name: "no matcher"},
		{name: "unsorted literals", program: knowledge.DimensionRuntimeProgram{ExactLiterals: []string{"z", "a"}}},
		{name: "duplicate literal", program: knowledge.DimensionRuntimeProgram{ExactLiterals: []string{"a", "a"}}},
		{name: "literal work", program: knowledge.DimensionRuntimeProgram{
			ExactLiterals: []string{"a"}, Assessment: knowledge.MatcherTransitionAssessment{Initial: 1},
		}},
		{name: "wildcard missing work", program: knowledge.DimensionRuntimeProgram{WildcardRE2: "(?s:\\A(?:.*)\\z)"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := compileKnowledgeSelectorDimension(`"index"`, test.program); err == nil {
				t.Fatal("invalid dimension compiled")
			}
		})
	}
}
