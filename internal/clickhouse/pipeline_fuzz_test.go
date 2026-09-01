package clickhouse

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/testsupport/officialspl"
)

const pipelineFuzzCompatibilityCorpusPath = "../spl/testdata/compatibility.json"

// FuzzParseBuildCompileIsDeterministicAndAtomic drives arbitrary SPL text
// through the whole parse → plan → compile chain. The spl package fuzzes its
// own parser; this target lives in the compiler package because it is the
// first package that may import every stage. Every stage must be
// deterministic, must publish nothing on failure, must not mutate its input,
// and every success must carry a valid execution seal with one bind argument
// per placeholder.
func FuzzParseBuildCompileIsDeterministicAndAtomic(f *testing.F) {
	for _, seed := range pipelineFuzzSeeds(f) {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, source string) {
		pipelineFuzzCheck(t, source)
	})
}

// pipelineFuzzSeeds is every official corpus query plus every source-backed
// compatibility corpus case, so the fuzzer starts from the full supported
// surface rather than a hand-picked slice of it. Expression-only cases are
// wrapped in the eval form the corpus itself uses for them.
func pipelineFuzzSeeds(f *testing.F) []string {
	f.Helper()
	corpus, err := officialspl.Load(officialGoldenCorpusPath)
	if err != nil {
		f.Fatalf("load official SPL corpus: %v", err)
	}
	seeds := make([]string, 0, len(corpus.Cases)+256)
	for _, testCase := range corpus.Cases {
		seeds = append(seeds, testCase.Query)
	}

	encoded, err := os.ReadFile(pipelineFuzzCompatibilityCorpusPath)
	if err != nil {
		f.Fatalf("read compatibility corpus: %v", err)
	}
	var compatibility struct {
		Rules []struct {
			Cases []struct {
				Source     string `json:"source"`
				Expression string `json:"expression"`
			} `json:"cases"`
		} `json:"rules"`
	}
	if err := json.NewDecoder(bytes.NewReader(encoded)).Decode(&compatibility); err != nil {
		f.Fatalf("decode compatibility corpus: %v", err)
	}
	for _, rule := range compatibility.Rules {
		for _, testCase := range rule.Cases {
			switch {
			case testCase.Source != "":
				seeds = append(seeds, testCase.Source)
			case testCase.Expression != "":
				seeds = append(seeds, "index=main | eval value="+testCase.Expression)
			}
		}
	}
	seeds = append(seeds,
		"",
		"|",
		"index=main |",
		"index=main | stats count BY",
		"index=main | rex field=_raw \"(?<host>[a-z]+)\"",
		"index=main | where match(host, \"^(a|b)*$\") | rename host AS 'the host'",
		"index=main earliest=-1d@d latest=@d | timechart span=1h count",
		"index=main | eval when=strftime(_time, \"%Y-%m-%dT%H:%M:%S.%3N%z\")",
		"index=main | eval when=strptime(stamp, \"%Y-%m-%d %H:%M:%S\")",
		"index=main | stats count sparkline(count,1h) AS trend BY host",
		"index=main | stats delim=\";\" values(host) AS hosts",
		"index=main | stats partitions=4 count BY host",
		"index=main | eval x=mvjoin(mvappend(\"a\",\"b\"), \",\")",
		"index=gradethis | lookup users user AS uid OUTPUT name",
		"\x00\xff",
	)
	return seeds
}

func pipelineFuzzScope() plan.Scope {
	scope := testChartScope()
	scope.RequestedIndexes = []string{"main"}
	scope.AuthorizedIndexes = []string{"main", "gradethis"}
	// The fuzzer is a non-executing validation caller: addinfo must not be
	// the only command family the chain can never exercise.
	scope.AllowUnboundSearchJobID = true
	return scope
}

func pipelineFuzzCheck(t *testing.T, source string) {
	t.Helper()

	first, firstErr := spl.Parse(source)
	second, secondErr := spl.Parse(source)
	if (firstErr == nil) != (secondErr == nil) {
		t.Fatalf("parse outcome changed across identical input: first=%v second=%v", firstErr, secondErr)
	}
	if firstErr != nil {
		if first != nil || second != nil {
			t.Fatalf("parse error published a partial query: first=%#v second=%#v", first, second)
		}
		pipelineFuzzCheckFailure(t, source, "parse", firstErr, secondErr, true)
		return
	}
	if first == nil || second == nil {
		t.Fatal("successful parse returned a nil query")
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("AST changed across identical reparses")
	}

	scope := pipelineFuzzScope()
	firstPlan, firstBuildErr := plan.Build(first, scope)
	secondPlan, secondBuildErr := plan.Build(first, scope)
	if !reflect.DeepEqual(first, second) {
		t.Fatal("planning mutated the parsed query")
	}
	if (firstBuildErr == nil) != (secondBuildErr == nil) {
		t.Fatalf("plan outcome changed across identical input: first=%v second=%v", firstBuildErr, secondBuildErr)
	}
	if firstBuildErr != nil {
		if firstPlan != nil || secondPlan != nil {
			t.Fatalf("plan error published a partial plan: first=%#v second=%#v", firstPlan, secondPlan)
		}
		pipelineFuzzCheckFailure(t, source, "plan", firstBuildErr, secondBuildErr, false)
		return
	}
	if firstPlan == nil || secondPlan == nil {
		t.Fatal("successful plan returned a nil query")
	}
	if !reflect.DeepEqual(firstPlan, secondPlan) {
		t.Fatal("logical plan changed across identical builds")
	}

	firstCompiled, firstCompileErr := (Compiler{}).Compile(firstPlan)
	secondCompiled, secondCompileErr := (Compiler{}).Compile(firstPlan)
	if !reflect.DeepEqual(firstPlan, secondPlan) {
		t.Fatal("compilation mutated the logical plan")
	}
	if (firstCompileErr == nil) != (secondCompileErr == nil) {
		t.Fatalf("compile outcome changed across identical input: first=%v second=%v", firstCompileErr, secondCompileErr)
	}
	if firstCompileErr != nil {
		for label, compiled := range map[string]CompiledQuery{"first": firstCompiled, "second": secondCompiled} {
			if !reflect.DeepEqual(compiled, CompiledQuery{}) {
				t.Fatalf("compile error published a partial %s query: %#v", label, compiled)
			}
		}
		pipelineFuzzCheckFailure(t, source, "compile", firstCompileErr, secondCompileErr, false)
		return
	}
	if !reflect.DeepEqual(firstCompiled, secondCompiled) {
		t.Fatalf("compiled query changed across identical compiles:\nfirst: %#v\nsecond: %#v", firstCompiled, secondCompiled)
	}
	if strings.TrimSpace(firstCompiled.SQL) == "" {
		t.Fatal("successful compile produced empty SQL")
	}
	if got, want := strings.Count(firstCompiled.SQL, "?"), len(firstCompiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, firstCompiled.SQL, firstCompiled.Args)
	}
	if !firstCompiled.HasValidExecutionSeal() {
		t.Fatalf("successful compile has no valid execution seal\nSQL: %s", firstCompiled.SQL)
	}
	firstDigest, firstSealed := firstCompiled.ExecutionAuthorityDigest()
	secondDigest, secondSealed := secondCompiled.ExecutionAuthorityDigest()
	if !firstSealed || !secondSealed || firstDigest != secondDigest {
		t.Fatalf("execution authority digest is not stable: first=%x/%t second=%x/%t", firstDigest, firstSealed, secondDigest, secondSealed)
	}
}

// pipelineFuzzCheckFailure holds every stage to the same failure contract:
// identical inputs produce identical errors, and any diagnostic points inside
// the authored source with a suggestion list every consumer can carry. The
// parser owns its errors completely, so it must always speak in diagnostics;
// later stages may also surface plain scope or resource errors.
func pipelineFuzzCheckFailure(t *testing.T, source, stage string, first, second error, requireDiagnostic bool) {
	t.Helper()
	if first.Error() != second.Error() {
		t.Fatalf("%s error changed across identical input: first=%v second=%v", stage, first, second)
	}
	var firstDiagnostic, secondDiagnostic *spl.Diagnostic
	firstIsDiagnostic := errors.As(first, &firstDiagnostic)
	secondIsDiagnostic := errors.As(second, &secondDiagnostic)
	if firstIsDiagnostic != secondIsDiagnostic {
		t.Fatalf("%s diagnostic classification changed across identical input: first=%v second=%v", stage, first, second)
	}
	if !firstIsDiagnostic {
		if requireDiagnostic {
			t.Fatalf("%s returned non-diagnostic error %T: %v", stage, first, first)
		}
		return
	}
	if firstDiagnostic == nil || secondDiagnostic == nil {
		t.Fatalf("%s returned a nil diagnostic: %v", stage, first)
	}
	if !reflect.DeepEqual(firstDiagnostic, secondDiagnostic) {
		t.Fatalf("%s diagnostic changed across identical input: first=%#v second=%#v", stage, firstDiagnostic, secondDiagnostic)
	}
	if firstDiagnostic.Code == "" || firstDiagnostic.Message == "" {
		t.Fatalf("%s diagnostic is missing its code or message: %#v", stage, firstDiagnostic)
	}
	if len(firstDiagnostic.Suggestions) > spl.MaximumDiagnosticSuggestions {
		t.Fatalf("%s diagnostic carries %d suggestions, more than the %d every consumer accepts",
			stage, len(firstDiagnostic.Suggestions), spl.MaximumDiagnosticSuggestions)
	}
	pipelineFuzzValidateRange(t, source, firstDiagnostic.Range, stage+" diagnostic")
}

// pipelineFuzzValidateRange mirrors the parser's own offset → line/column
// derivation (one column per rune, invalid bytes one column wide) so a
// diagnostic from any stage can be checked against the authored source
// without reaching into spl internals.
func pipelineFuzzValidateRange(t *testing.T, source string, sourceRange spl.Range, label string) {
	t.Helper()
	if sourceRange.Start.Offset < 0 ||
		sourceRange.End.Offset < sourceRange.Start.Offset ||
		sourceRange.End.Offset > len(source) {
		t.Fatalf("%s range %#v is outside a %d-byte source", label, sourceRange, len(source))
	}
	wantStart := pipelineFuzzPositionAtOffset(source, sourceRange.Start.Offset)
	wantEnd := pipelineFuzzPositionAtOffset(source, sourceRange.End.Offset)
	if sourceRange.Start != wantStart || sourceRange.End != wantEnd {
		t.Fatalf("%s range positions are inconsistent: got=%#v want=%#v..%#v", label, sourceRange, wantStart, wantEnd)
	}
}

func pipelineFuzzPositionAtOffset(source string, offset int) spl.Position {
	position := spl.Position{Line: 1, Column: 1}
	for position.Offset < offset {
		r, width := utf8.DecodeRuneInString(source[position.Offset:])
		if position.Offset+width > offset {
			position.Offset = offset
			return position
		}
		position.Offset += width
		if r == '\n' {
			position.Line++
			position.Column = 1
		} else {
			position.Column++
		}
	}
	return position
}
