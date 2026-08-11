package clickhouse

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

const centralKnowledgeAuthoredAuthorityError = "seal compiled ClickHouse execution: knowledge authority changed during compilation"

type centralKnowledgeFinalizerCapture struct {
	called        bool
	relation      compiledRelation
	state         compileState
	args          []any
	aliasSequence int
	compiled      CompiledQuery
}

func TestCentralKnowledgeCompositionPreservesAbsentAndPresentEmptyParity(t *testing.T) {
	legacyPlan := buildPlan(t, `index=gradethis | where host="FixtureHost"`)
	legacyCapture, legacy, err := compileCentralKnowledgeCapture(legacyPlan)
	if err != nil {
		t.Fatalf("compile absent knowledge plan: %v", err)
	}

	empty, err := knowledgeprogram.Prepare(knowledgeprogram.Input{})
	if err != nil {
		t.Fatalf("prepare empty knowledge program: %v", err)
	}
	emptyPlan, err := plan.InjectKnowledgePrelude(legacyPlan, empty)
	if err != nil {
		t.Fatalf("inject empty knowledge prelude: %v", err)
	}
	emptyCapture, admitted, err := compileCentralKnowledgeCapture(emptyPlan)
	if err != nil {
		t.Fatalf("compile present-empty knowledge plan: %v", err)
	}

	if !legacyCapture.called || !emptyCapture.called ||
		legacyCapture.relation != emptyCapture.relation ||
		!reflect.DeepEqual(legacyCapture.state, emptyCapture.state) ||
		!reflect.DeepEqual(legacyCapture.args, emptyCapture.args) ||
		legacyCapture.aliasSequence != emptyCapture.aliasSequence ||
		len(legacyCapture.state.chronologicalBarriers) != 0 ||
		len(emptyCapture.state.chronologicalBarriers) != 0 {
		t.Fatalf(
			"absent/present-empty lowering differs:\n absent: %#v\n empty: %#v",
			legacyCapture,
			emptyCapture,
		)
	}
	if legacy.SQL != admitted.SQL ||
		!reflect.DeepEqual(legacy.Args, admitted.Args) ||
		!slices.Equal(legacy.OutputFields, admitted.OutputFields) ||
		legacy.SparseFields != admitted.SparseFields ||
		legacy.relationalDepth != admitted.relationalDepth ||
		legacy.relationalDepthRange != admitted.relationalDepthRange {
		t.Fatalf("absent/present-empty executable shape differs:\n absent: %#v\n empty: %#v", legacy, admitted)
	}
	legacyEvidence, legacyOK := legacy.KnowledgeSnapshotEvidence()
	emptyEvidence, emptyOK := admitted.KnowledgeSnapshotEvidenceFor(empty)
	if !legacyOK || legacyEvidence.KnowledgeProgramPresent() || !emptyOK ||
		!emptyEvidence.KnowledgeProgramPresent() ||
		emptyEvidence.KnowledgeProgramObjectCount() != 0 {
		t.Fatalf(
			"absent/present-empty evidence = (%#v, %t)/(%#v, %t)",
			legacyEvidence,
			legacyOK,
			emptyEvidence,
			emptyOK,
		)
	}
}

func TestCentralKnowledgeCompositionLowersOnceBeforeAuthoredSuffixAtCompilerBoundary(t *testing.T) {
	program := deferredMixedKnowledgeProgramForTest(t)
	authored := buildPlan(t,
		`index=gradethis | where calculated_value="fixturesource" | eval authored_value=lower(alias_value)`,
	)
	logical, err := plan.InjectKnowledgePrelude(authored, program)
	if err != nil {
		t.Fatalf("inject mixed knowledge prelude: %v", err)
	}

	capture, compiled, compileErr := compileCentralKnowledgeCapture(logical)
	requireCentralKnowledgeCompilerBoundary(t, compiled.HasValidExecutionSeal(), compileErr)
	if !capture.called {
		t.Fatal("central finalizer was not reached after knowledge and authored lowering")
	}
	if len(capture.state.chronologicalBarriers) != 1 {
		t.Fatalf("central knowledge barriers = %d, want 1", len(capture.state.chronologicalBarriers))
	}
	barrier := capture.state.chronologicalBarriers[0]
	if barrier.name != knowledgeRuntimeGuardResultName ||
		len(barrier.prerequisiteDefinitions) != 1 ||
		len(barrier.validationColumns) != 1 ||
		barrier.validationColumns[0] != knowledgeRuntimeGuardValidationColumn {
		t.Fatalf("central knowledge barrier = %#v", barrier)
	}
	for _, field := range []string{"regex_value", "alias_value", "calculated_value", "authored_value"} {
		if _, ok := capture.state.visible[field]; !ok {
			t.Fatalf("central composed state omits visible field %q", field)
		}
	}
	if !strings.Contains(capture.relation.sql, knowledgeRuntimeGuardResultName) ||
		!strings.Contains(capture.relation.sql, `"authored_value"`) ||
		strings.Contains(
			strings.Join(barrier.prerequisiteDefinitions, "\x00")+barrier.sql,
			`"authored_value"`,
		) {
		t.Fatalf(
			"authored suffix is not strictly outside the knowledge barrier:\nrelation: %s\nbarrier: %#v",
			capture.relation.sql,
			barrier.prerequisiteDefinitions,
		)
	}
	if got := strings.Count(barrier.prerequisiteDefinitions[0], " ARRAY JOIN "); got != 3 {
		t.Fatalf(
			"central physical knowledge stage count = %d, want 3 (no omission or double lowering):\n%s",
			got,
			barrier.prerequisiteDefinitions[0],
		)
	}
	if got, want := strings.Count(
		barrier.prerequisiteDefinitions[0],
		"?",
	), len(barrier.args); got != want {
		t.Fatalf("barrier placeholders = %d, args = %d", got, want)
	}
	if got, want := strings.Count(capture.relation.sql, "?"), len(capture.args); got != want {
		t.Fatalf("active suffix placeholders = %d, args = %d", got, want)
	}
	wantFinalArgs := append(slices.Clone(barrier.args), capture.args...)
	if !reflect.DeepEqual(capture.compiled.Args, wantFinalArgs) {
		t.Fatalf(
			"final argument order = %#v, want barrier then active %#v",
			capture.compiled.Args,
			wantFinalArgs,
		)
	}
	if len(capture.args) == 0 || !slices.Contains(capture.args, any("fixturesource")) {
		t.Fatalf("authored filter arguments were not retained as active suffix args: %#v", capture.args)
	}
	if got, want := strings.Count(capture.compiled.SQL, "?"), len(capture.compiled.Args); got != want {
		t.Fatalf("final placeholders = %d, args = %d", got, want)
	}
}

func TestCentralKnowledgeCompositionRejectsTamperedPrefixBeforeFinalizer(t *testing.T) {
	program := deferredMixedKnowledgeProgramForTest(t)
	tests := []struct {
		name   string
		mutate func(*plan.Query)
	}{
		{
			name: "dropped",
			mutate: func(query *plan.Query) {
				query.Operators = append(
					append([]plan.Operator(nil), query.Operators[:1]...),
					query.Operators[2:]...,
				)
			},
		},
		{
			name: "substituted",
			mutate: func(query *plan.Query) {
				query.Operators[1] = &plan.Limit{Count: 1}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authored := buildPlan(t, `index=gradethis | where host="FixtureHost"`)
			logical, err := plan.InjectKnowledgePrelude(authored, program)
			if err != nil {
				t.Fatalf("inject mixed knowledge prelude: %v", err)
			}
			test.mutate(logical)
			capture, _, compileErr := compileCentralKnowledgeCapture(logical)
			if compileErr == nil {
				t.Fatal("tampered knowledge prefix compiled")
			}
			if capture.called {
				t.Fatalf("tampered knowledge prefix reached finalizer: %v", compileErr)
			}
		})
	}
}

func TestCentralKnowledgeCompositionSealRejectsPostLoweringAuthoredMutation(t *testing.T) {
	empty, err := knowledgeprogram.Prepare(knowledgeprogram.Input{})
	if err != nil {
		t.Fatalf("prepare empty knowledge program: %v", err)
	}
	logical, err := plan.InjectKnowledgePrelude(
		buildPlan(t, `index=gradethis | rex field=_raw "(?<word>[a-z]+)"`),
		empty,
	)
	if err != nil {
		t.Fatalf("inject empty knowledge prelude: %v", err)
	}
	var authored *plan.Extract
	for _, operator := range logical.Operators {
		if candidate, ok := operator.(*plan.Extract); ok {
			authored = candidate
			break
		}
	}
	if authored == nil {
		t.Fatal("authored rex fixture has no Extract operator")
	}
	original, err := validateExtractOperator(authored)
	if err != nil {
		t.Fatalf("validate original authored rex: %v", err)
	}
	mutated, err := splregex.CompileExtractionPattern(
		`(?<word>[a-z]+(?:[0-9]+|_[a-z]+)?)`,
	)
	if err != nil {
		t.Fatalf("compile mutated authored rex: %v", err)
	}
	if mutated.ProgramWorkUnits == original.ProgramWorkUnits ||
		mutated.Pattern == original.Pattern {
		t.Fatalf(
			"authored rex mutation does not change compiler charges: %#v / %#v",
			original,
			mutated,
		)
	}

	finalizerCalled := false
	_, compileErr := (Compiler{}).compileWithFinalizer(
		logical,
		func(
			relation compiledRelation,
			state compileState,
			args []any,
			scan *plan.Scan,
			aliasSequence int,
		) (CompiledQuery, error) {
			finalized, finalizeErr := finalizeOrdinaryQuery(
				relation,
				state,
				args,
				scan,
				aliasSequence,
			)
			if finalizeErr != nil {
				return CompiledQuery{}, finalizeErr
			}
			if !slices.Contains(finalized.Args, any(original.Pattern)) ||
				slices.Contains(finalized.Args, any(mutated.Pattern)) {
				return CompiledQuery{}, &centralKnowledgeFixtureError{
					message: "authored rex SQL was not finalized before mutation",
				}
			}
			finalizerCalled = true
			authored.Pattern = mutated.Pattern
			return finalized, nil
		},
		true,
	)
	if !finalizerCalled {
		t.Fatalf("post-lowering mutation finalizer was not reached: %v", compileErr)
	}
	if compileErr == nil || compileErr.Error() != centralKnowledgeAuthoredAuthorityError {
		t.Fatalf(
			"post-lowering authored mutation error = %v, want %q",
			compileErr,
			centralKnowledgeAuthoredAuthorityError,
		)
	}
}

func TestCentralKnowledgeCompositionTerminalAndAnalysisPathsReachCompilerBoundary(t *testing.T) {
	program := deferredMixedKnowledgeProgramForTest(t)
	for _, test := range []struct {
		name   string
		source string
	}{
		{name: "chart", source: `index=gradethis | chart count OVER level BY status`},
		{name: "timechart", source: `index=gradethis | timechart span=5m count BY level`},
	} {
		t.Run(test.name, func(t *testing.T) {
			logical, err := plan.InjectKnowledgePrelude(buildPlan(t, test.source), program)
			if err != nil {
				t.Fatalf("inject mixed knowledge prelude: %v", err)
			}
			compiled, compileErr := (Compiler{}).Compile(logical)
			requireCentralKnowledgeCompilerBoundary(t, compiled.HasValidExecutionSeal(), compileErr)
		})
	}

	for _, test := range []struct {
		name    string
		compile func(*plan.Query) (bool, error)
	}{
		{
			name: "field catalog",
			compile: func(logical *plan.Query) (bool, error) {
				compiled, err := (Compiler{}).CompileFieldCatalog(
					logical,
					FieldCatalogSpec{MaximumFields: 32},
				)
				return compiled.HasValidExecutionSeal(), err
			},
		},
		{
			name: "field suggestions",
			compile: func(logical *plan.Query) (bool, error) {
				compiled, err := (Compiler{}).CompileFieldSuggestions(
					logical,
					FieldSuggestionSpec{Prefix: "cal", MaximumFields: 16},
				)
				return compiled.HasValidExecutionSeal(), err
			},
		},
		{
			name: "field summary",
			compile: func(logical *plan.Query) (bool, error) {
				compiled, err := (Compiler{}).CompileFieldSummary(
					logical,
					FieldSummarySpec{
						FieldName:             "calculated_value",
						MaximumValues:         16,
						MaximumDistinctValues: 64,
						MaximumValueBytes:     4_096,
					},
				)
				return compiled.HasValidExecutionSeal(), err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			logical, err := plan.InjectKnowledgePrelude(
				buildPlan(t, `index=gradethis | where host="FixtureHost"`),
				program,
			)
			if err != nil {
				t.Fatalf("inject mixed knowledge prelude: %v", err)
			}
			validSeal, compileErr := test.compile(logical)
			requireCentralKnowledgeCompilerBoundary(t, validSeal, compileErr)
		})
	}
}

type centralKnowledgeFixtureError struct {
	message string
}

func (err *centralKnowledgeFixtureError) Error() string {
	return err.message
}

func compileCentralKnowledgeCapture(
	query *plan.Query,
) (centralKnowledgeFinalizerCapture, CompiledQuery, error) {
	capture := centralKnowledgeFinalizerCapture{}
	compiled, err := (Compiler{}).compileWithFinalizer(
		query,
		func(
			relation compiledRelation,
			state compileState,
			args []any,
			scan *plan.Scan,
			aliasSequence int,
		) (CompiledQuery, error) {
			capture.called = true
			capture.relation = relation
			capture.state = cloneCompileState(state)
			capture.aliasSequence = aliasSequence
			clonedArgs, cloneErr := cloneKnowledgeRelationArguments(args)
			if cloneErr != nil {
				return CompiledQuery{}, cloneErr
			}
			capture.args = clonedArgs
			finalized, finalizeErr := finalizeOrdinaryQuery(
				relation,
				state,
				args,
				scan,
				aliasSequence,
			)
			capture.compiled = finalized
			return finalized, finalizeErr
		},
		true,
	)
	return capture, compiled, err
}

func requireCentralKnowledgeCompilerBoundary(t *testing.T, validSeal bool, err error) {
	t.Helper()
	if err != nil || !validSeal {
		t.Fatalf("central knowledge compile = (sealed:%t, error:%v)", validSeal, err)
	}
}
