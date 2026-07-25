package clickhouse

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// relationalDepthEvalPipeline builds parser-valid SPL without a base search
// predicate. Each one-assignment eval and each assignment in the final eval
// contributes exactly one generated relational level. terminal, when nonempty,
// is a command without its leading pipe.
func relationalDepthEvalPipeline(
	t *testing.T,
	singleCommands int,
	finalAssignments int,
	terminal string,
) string {
	t.Helper()
	terminal = strings.TrimSpace(terminal)
	if singleCommands < 0 || finalAssignments < 0 || finalAssignments > 64 ||
		strings.HasPrefix(terminal, "|") {
		t.Fatalf(
			"invalid relational-depth fixture: singles=%d final=%d terminal=%q",
			singleCommands,
			finalAssignments,
			terminal,
		)
	}
	commandCount := singleCommands
	if finalAssignments > 0 {
		commandCount++
	}
	if terminal != "" {
		commandCount++
	}
	if commandCount == 0 || commandCount > 64 {
		t.Fatalf("relational-depth fixture has %d commands, want 1 through 64", commandCount)
	}

	var source strings.Builder
	appendCommand := func(command string) {
		if source.Len() > 0 {
			source.WriteByte(' ')
		}
		source.WriteString("| ")
		source.WriteString(command)
	}
	for command := 0; command < singleCommands; command++ {
		appendCommand("eval single" + strconv.Itoa(command) + "=1")
	}
	if finalAssignments > 0 {
		var final strings.Builder
		final.WriteString("eval ")
		for assignment := 0; assignment < finalAssignments; assignment++ {
			if assignment > 0 {
				final.WriteByte(',')
			}
			final.WriteString("final")
			final.WriteString(strconv.Itoa(assignment))
			final.WriteString("=1")
		}
		appendCommand(final.String())
	}
	if terminal != "" {
		appendCommand(terminal)
	}
	if source.Len() >= 16<<10 {
		t.Fatalf("relational-depth fixture is %d bytes, want it below the parser ceiling", source.Len())
	}
	return source.String()
}

func TestRelationalNodeDepthUsesMaximumDependencyHeight(t *testing.T) {
	t.Parallel()

	if got := relationalNodeDepth(); got != 1 {
		t.Fatalf("input-free relation depth = %d, want 1", got)
	}
	if got := relationalNodeDepth(3, 17, 5); got != 18 {
		t.Fatalf("asymmetric dependency depth = %d, want max(3,17,5)+1 = 18", got)
	}
	siblings := make([]int, 128)
	for index := range siblings {
		siblings[index] = 9
	}
	if got := relationalNodeDepth(siblings...); got != 10 {
		t.Fatalf("128 sibling relations produced depth %d, want dependency height 10", got)
	}
}

func TestCompiledRelationalDepthPinsRepresentativeOperatorCosts(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
		depth  int
	}{
		{name: "ordinary", source: `| head 1`, depth: 3},
		{name: "multi-assignment eval", source: `| eval first=1,second=2,third=3`, depth: 5},
		{name: "fixed bin", source: `| bin severity span=10`, depth: 3},
		{name: "dynamic bin", source: `| bin latency span=10`, depth: 12},
		{
			name:   "dynamic bin predicate fence",
			source: `| bin latency span=10 | where latency>=7`,
			depth:  13,
		},
		{
			name:   "durable dynamic bin predicate fence",
			source: `| bin latency span=10 | where latency>=7 | where latency<100`,
			depth:  14,
		},
		{
			name:   "dynamic re-bin",
			source: `| bin latency span=10 | bin latency span=10`,
			depth:  23,
		},
		{name: "rex", source: `| rex field=_raw "(?<word>[a-z]+)"`, depth: 7},
		{name: "spath", source: `| spath output=value path=payload.value`, depth: 6},
		{name: "fixed dedup", source: `| dedup host`, depth: 4},
		{name: "dynamic dedup", source: `| dedup latency`, depth: 5},
		{name: "dynamic aggregate", source: `| stats count BY latency`, depth: 6},
		{name: "values aggregate", source: `| stats values(user) AS users`, depth: 6},
		{
			name:   "shared dc and values aggregate",
			source: `| stats dc(user) AS user_count values(user) AS users`,
			depth:  6,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled, err := (Compiler{}).Compile(relationalDepthPlan(t, test.source))
			if err != nil {
				t.Fatalf("Compile(%q): %v", test.source, err)
			}
			if compiled.relationalDepth != test.depth {
				t.Fatalf(
					"Compile(%q) relational depth = %d, want %d",
					test.source,
					compiled.relationalDepth,
					test.depth,
				)
			}
		})
	}
}

func TestStatsValuesRelationalDepthBoundaryIsSourceLocated(t *testing.T) {
	t.Parallel()

	acceptedSource := relationalDepthEvalPipeline(
		t,
		26,
		64,
		"stats values(user) AS users",
	)
	accepted, err := (Compiler{}).Compile(relationalDepthPlan(t, acceptedSource))
	if err != nil {
		t.Fatalf("Compile(values at depth 96): %v", err)
	}
	if accepted.relationalDepth != maximumCompiledRelationalDepth {
		t.Fatalf(
			"accepted values relational depth = %d, want %d",
			accepted.relationalDepth,
			maximumCompiledRelationalDepth,
		)
	}

	rejectedSource := relationalDepthEvalPipeline(
		t,
		27,
		64,
		"stats values(user) AS users",
	)
	rejected := relationalDepthPlan(t, rejectedSource)
	statsRange := rejected.Operators[len(rejected.Operators)-1].SourceRange()
	_, err = (Compiler{}).Compile(rejected)
	relationalDepthRequireLimitDiagnostic(t, err, statsRange)
}

func TestSpathRelationalDepthBoundaryIsSourceLocated(t *testing.T) {
	t.Parallel()

	acceptedSource := relationalDepthEvalPipeline(
		t,
		26,
		64,
		"spath output=value path=payload.value",
	)
	accepted, err := (Compiler{}).Compile(relationalDepthPlan(t, acceptedSource))
	if err != nil {
		t.Fatalf("Compile(spath at depth 96): %v", err)
	}
	if accepted.relationalDepth != 96 {
		t.Fatalf("accepted spath relational depth = %d, want 96", accepted.relationalDepth)
	}

	rejectedSource := relationalDepthEvalPipeline(
		t,
		27,
		64,
		"spath output=value path=payload.value",
	)
	rejected := relationalDepthPlan(t, rejectedSource)
	spathRange := rejected.Operators[len(rejected.Operators)-1].SourceRange()
	_, err = (Compiler{}).Compile(rejected)
	relationalDepthRequireLimitDiagnostic(t, err, spathRange)
}

func TestCalculatedDynamicPredicateFencePinsOrdinaryDepth(t *testing.T) {
	t.Parallel()

	logical := relationalDepthPlan(
		t,
		`index=gradethis | bin latency span=10 | where latency>=7`,
	)
	ordinary, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if ordinary.relationalDepth != 14 {
		t.Fatalf("ordinary relational depth = %d, want 14", ordinary.relationalDepth)
	}
}

func TestCompiledRelationalDepthPinsTerminalWideOperatorCosts(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		terminal  string
		depth     int
		wantChart bool
	}{
		{
			name:     "timechart",
			terminal: "timechart span=5m count BY level",
			depth:    13,
		},
		{
			name:      "chart",
			terminal:  "chart count OVER path BY level",
			depth:     14,
			wantChart: true,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			source := relationalDepthEvalPipeline(t, 0, 0, test.terminal)
			compiled, err := (Compiler{}).Compile(relationalDepthPlan(t, source))
			if err != nil {
				t.Fatalf("Compile(%q): %v", source, err)
			}
			if compiled.relationalDepth != test.depth {
				t.Fatalf(
					"Compile(%q) relational depth = %d, want %d",
					source,
					compiled.relationalDepth,
					test.depth,
				)
			}
			if test.wantChart {
				if compiled.Chart == nil || compiled.Timechart != nil {
					t.Fatalf("chart terminal contract = chart:%#v timechart:%#v", compiled.Chart, compiled.Timechart)
				}
			} else if compiled.Timechart == nil || compiled.Chart != nil {
				t.Fatalf("timechart terminal contract = timechart:%#v chart:%#v", compiled.Timechart, compiled.Chart)
			}
		})
	}
}

func TestAnalysisFinalizerCannotUnderreportItsRelationalWrapper(t *testing.T) {
	t.Parallel()

	// The event input itself is already at the accepted ceiling:
	// Scan(1) + 95 eval assignments = 96. The custom finalizer emits one real
	// SELECT above that input but dishonestly reuses depth 96. Accepting this
	// result would let a private analysis finalizer bypass the fail-closed
	// relational budget.
	logical := relationalDepthPlan(t, relationalDepthEvalPipeline(t, 62, 33, ""))
	_, err := (Compiler{}).compileEventAnalysis(
		logical,
		func(
			relation compiledRelation,
			_ compileState,
			args []any,
			_ *plan.Scan,
			_ int,
		) (CompiledQuery, error) {
			return withCompiledRelationalDepth(
				CompiledQuery{
					SQL:  "SELECT * FROM (" + relation.sql + ") AS \"underreported_finalizer\"",
					Args: args,
				},
				relation.depth,
				relation.ownerRange,
			), nil
		},
	)
	if err == nil {
		t.Fatal("compileEventAnalysis accepted a finalizer that underreported its SELECT wrapper")
	}
	if !strings.Contains(err.Error(), "relational") {
		t.Fatalf("underreported finalizer error = %v, want a relational-depth rejection", err)
	}
}

func TestTerminalWideRelationalDepthBoundariesAreSourceLocated(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name            string
		terminal        string
		acceptedSingles int
		rejectedSingles int
		wantChart       bool
	}{
		{
			name:            "timechart",
			terminal:        "timechart span=5m count BY level",
			acceptedSingles: 19,
			rejectedSingles: 20,
		},
		{
			name:            "chart",
			terminal:        "chart count OVER path BY level",
			acceptedSingles: 18,
			rejectedSingles: 19,
			wantChart:       true,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			acceptedSource := relationalDepthEvalPipeline(
				t,
				test.acceptedSingles,
				64,
				test.terminal,
			)
			accepted, err := (Compiler{}).Compile(relationalDepthPlan(t, acceptedSource))
			if err != nil {
				t.Fatalf("Compile(depth 96 %s): %v", test.name, err)
			}
			if accepted.relationalDepth != 96 {
				t.Fatalf(
					"accepted %s relational depth = %d, want 96",
					test.name,
					accepted.relationalDepth,
				)
			}
			if test.wantChart {
				if accepted.Chart == nil || accepted.Timechart != nil {
					t.Fatalf(
						"accepted chart contract = chart:%#v timechart:%#v",
						accepted.Chart,
						accepted.Timechart,
					)
				}
			} else if accepted.Timechart == nil || accepted.Chart != nil {
				t.Fatalf(
					"accepted timechart contract = timechart:%#v chart:%#v",
					accepted.Timechart,
					accepted.Chart,
				)
			}

			rejectedSource := relationalDepthEvalPipeline(
				t,
				test.rejectedSingles,
				64,
				test.terminal,
			)
			rejected := relationalDepthPlan(t, rejectedSource)
			terminalRange := rejected.Operators[len(rejected.Operators)-1].SourceRange()
			_, err = (Compiler{}).Compile(rejected)
			relationalDepthRequireLimitDiagnostic(t, err, terminalRange)
		})
	}
}

func TestFieldAnalysisRelationalDepthBoundariesIncludePrivateFinalizers(t *testing.T) {
	t.Parallel()

	t.Run("field catalog", func(t *testing.T) {
		t.Parallel()
		accepted := relationalDepthPlan(t, relationalDepthEvalPipeline(t, 21, 64, ""))
		if _, err := (Compiler{}).CompileFieldCatalog(
			accepted,
			FieldCatalogSpec{MaximumFields: 10},
		); err != nil {
			t.Fatalf("CompileFieldCatalog(depth 96): %v", err)
		}

		rejected := relationalDepthPlan(t, relationalDepthEvalPipeline(t, 22, 64, ""))
		_, err := (Compiler{}).CompileFieldCatalog(
			rejected,
			FieldCatalogSpec{MaximumFields: 10},
		)
		relationalDepthRequireLimitDiagnostic(t, err, rejected.Operators[0].SourceRange())
	})

	t.Run("field summary", func(t *testing.T) {
		t.Parallel()
		spec := FieldSummarySpec{
			FieldName:             "level",
			MaximumValues:         10,
			MaximumDistinctValues: 100,
			MaximumValueBytes:     4_096,
		}
		accepted := relationalDepthPlan(t, relationalDepthEvalPipeline(t, 24, 64, ""))
		if _, err := (Compiler{}).CompileFieldSummary(accepted, spec); err != nil {
			t.Fatalf("CompileFieldSummary(depth 96): %v", err)
		}

		rejected := relationalDepthPlan(t, relationalDepthEvalPipeline(t, 25, 64, ""))
		_, err := (Compiler{}).CompileFieldSummary(rejected, spec)
		relationalDepthRequireLimitDiagnostic(t, err, rejected.Operators[0].SourceRange())
	})

	t.Run("timeline", func(t *testing.T) {
		t.Parallel()
		accepted := relationalDepthPlan(t, relationalDepthEvalPipeline(t, 26, 64, ""))
		if _, err := (Compiler{}).CompileTimeline(accepted, validTimelineSpec()); err != nil {
			t.Fatalf("CompileTimeline(depth 96): %v", err)
		}

		rejected := relationalDepthPlan(t, relationalDepthEvalPipeline(t, 27, 64, ""))
		_, err := (Compiler{}).CompileTimeline(rejected, validTimelineSpec())
		relationalDepthRequireLimitDiagnostic(t, err, rejected.Operators[0].SourceRange())
	})
}

func TestCompilerBoundsForgedAssignmentCounts(t *testing.T) {
	t.Parallel()

	t.Run("extend", func(t *testing.T) {
		t.Parallel()
		logical := relationalDepthPlan(t, `| eval seed=1`)
		extend := logical.Operators[1].(*plan.Extend)
		prototype := extend.Assignments[0]
		extend.Range = relationalDepthForgedRange()
		extend.Assignments = make([]plan.ExtendAssignment, 65)
		for index := range extend.Assignments {
			output, err := plan.ResolveField(
				"forged_eval_"+strconv.Itoa(index),
				extend.Range,
			)
			if err != nil {
				t.Fatalf("ResolveField: %v", err)
			}
			assignment := prototype
			assignment.Output = output
			assignment.Range = extend.Range
			extend.Assignments[index] = assignment
		}
		_, err := (Compiler{}).Compile(logical)
		relationalDepthRequireAssignmentDiagnostic(t, err, "eval", extend.Range)
	})

	t.Run("rename", func(t *testing.T) {
		t.Parallel()
		logical := relationalDepthPlan(t, `| rename host AS renamed_host`)
		rename := logical.Operators[1].(*plan.Rename)
		rename.Range = relationalDepthForgedRange()
		rename.Assignments = make([]plan.RenameAssignment, 65)
		for index := range rename.Assignments {
			source, err := plan.ResolveField(
				"forged_source_"+strconv.Itoa(index),
				rename.Range,
			)
			if err != nil {
				t.Fatalf("ResolveField(source): %v", err)
			}
			destination, err := plan.ResolveField(
				"forged_destination_"+strconv.Itoa(index),
				rename.Range,
			)
			if err != nil {
				t.Fatalf("ResolveField(destination): %v", err)
			}
			rename.Assignments[index] = plan.RenameAssignment{
				Source: source, Destination: destination, Range: rename.Range,
			}
		}
		_, err := (Compiler{}).Compile(logical)
		relationalDepthRequireAssignmentDiagnostic(t, err, "rename", rename.Range)
	})
}

func relationalDepthPlan(t *testing.T, source string) *plan.Query {
	t.Helper()
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatalf("Parse(%q): %v", source, err)
	}
	logical, err := plan.Build(parsed, testChartScope())
	if err != nil {
		t.Fatalf("Build(%q): %v", source, err)
	}
	return logical
}

func relationalDepthRequireLimitDiagnostic(
	t *testing.T,
	err error,
	wantRange spl.Range,
) {
	t.Helper()
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
		t.Fatalf("relational-depth error = %#v, want SPL_QUERY_TOO_COMPLEX", err)
	}
	if diagnostic.Range != wantRange {
		t.Fatalf("relational-depth range = %#v, want %#v", diagnostic.Range, wantRange)
	}
	if !strings.Contains(diagnostic.Message, "96 relational levels") {
		t.Fatalf("relational-depth message = %q, want it to name 96 relational levels", diagnostic.Message)
	}
}

func relationalDepthRequireAssignmentDiagnostic(
	t *testing.T,
	err error,
	command string,
	wantRange spl.Range,
) {
	t.Helper()
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
		t.Fatalf("%s assignment error = %#v, want SPL_QUERY_TOO_COMPLEX", command, err)
	}
	if diagnostic.Range != wantRange {
		t.Fatalf("%s assignment range = %#v, want %#v", command, diagnostic.Range, wantRange)
	}
	if !strings.Contains(diagnostic.Message, command) ||
		!strings.Contains(diagnostic.Message, "more than 64 assignments") {
		t.Fatalf("%s assignment message = %q, want the fixed 64-assignment ceiling", command, diagnostic.Message)
	}
}

func relationalDepthForgedRange() spl.Range {
	return spl.Range{
		Start: spl.Position{Offset: 37, Line: 2, Column: 5},
		End:   spl.Position{Offset: 59, Line: 2, Column: 27},
	}
}
