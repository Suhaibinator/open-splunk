package clickhouse

import (
	"errors"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestCompileDeferredKnowledgeRelationBuildsFlatGuardBarrier(t *testing.T) {
	program := deferredMixedKnowledgeProgramForTest(t)
	preparation := knowledgePreludePreparationForTest(program)
	owner := spl.Range{
		Start: spl.Position{Offset: 3, Line: 1, Column: 4},
		End:   spl.Position{Offset: 17, Line: 1, Column: 18},
	}
	input := compiledRelation{
		sql:        `SELECT ? AS "tenant", ? AS "seed"`,
		depth:      3,
		ownerRange: owner,
	}
	compiled, err := compileDeferredKnowledgeRelation(
		input,
		knowledgeExtractionStageState(),
		[]any{"tenant-one", "seed-one"},
		preparation,
	)
	if err != nil {
		t.Fatalf("compile deferred knowledge relation: %v", err)
	}
	if compiled.args != nil || len(compiled.state.chronologicalBarriers) != 1 ||
		len(compiled.prelude.state.chronologicalBarriers) != 0 {
		t.Fatalf("deferred result ownership = %#v", compiled)
	}
	barrier := compiled.state.chronologicalBarriers[0]
	staged := composeDeferredKnowledgeStagesForTest(input, compiled.prelude)
	violation, validation := deferredKnowledgeGuardExpressionsForTest(compiled.prelude)
	wantDefinitions := []string{
		`"__os_ko_guard_input" AS MATERIALIZED (` + staged.sql + `)`,
		`"__os_ko_guard_totals" AS (SELECT ` + violation +
			` AS "__os_ko_guard_violation" FROM "__os_ko_guard_input")`,
	}
	if !slices.Equal(barrier.prerequisiteDefinitions, wantDefinitions) {
		t.Fatalf(
			"guard prerequisite definitions = %#v, want %#v",
			barrier.prerequisiteDefinitions,
			wantDefinitions,
		)
	}
	if barrier.name != `"__os_ko_guard_result"` || barrier.fanout != 2 ||
		barrier.depth != staged.depth+2 || barrier.ownerRange != owner ||
		len(barrier.validationColumns) != 1 ||
		barrier.validationColumns[0] != `"__os_ko_guard_validation"` {
		t.Fatalf("guard barrier = %#v", barrier)
	}
	hiddenValidation := barrier.validationColumns[0]
	wantPublication := "SELECT * EXCEPT (" + compiled.prelude.selectorCharges.inputBytes +
		", " + compiled.prelude.selectorCharges.queryUnits +
		`, "__os_ko_guard_violation"), toUInt8(` + validation + ") AS " + hiddenValidation
	for _, fragment := range []string{
		wantPublication,
		`FROM "__os_ko_guard_input" AS "__os_ko_guard_event" CROSS JOIN ` +
			`"__os_ko_guard_totals" AS "__os_ko_guard_total"`,
	} {
		if !strings.Contains(barrier.sql, fragment) {
			t.Fatalf("guard barrier omits %q:\n%s", fragment, barrier.sql)
		}
	}
	if strings.Contains(barrier.sql, "WITH ") ||
		strings.Contains(barrier.sql, " AS MATERIALIZED (") ||
		strings.Contains(barrier.sql, " WHERE ") {
		t.Fatalf("guard barrier is not a flat row definition:\n%s", barrier.sql)
	}
	if compiled.relation.depth != staged.depth+3 ||
		compiled.relation.ownerRange != owner ||
		!strings.HasPrefix(
			compiled.relation.sql,
			"SELECT * EXCEPT ("+hiddenValidation+") FROM "+barrier.name+" AS ",
		) {
		t.Fatalf("published deferred relation = %#v", compiled.relation)
	}
	stateWithoutBarrier := cloneCompileState(compiled.state)
	stateWithoutBarrier.chronologicalBarriers = nil
	if !reflect.DeepEqual(stateWithoutBarrier, compiled.prelude.state) {
		t.Fatalf(
			"published state differs from prelude state:\n got: %#v\nwant: %#v",
			stateWithoutBarrier,
			compiled.prelude.state,
		)
	}
}

func TestCompileDeferredKnowledgeRelationOwnsArgumentsAndProofOnce(t *testing.T) {
	program := deferredMixedKnowledgeProgramForTest(t)
	preparation := knowledgePreludePreparationForTest(program)
	charges := program.Charges()
	preparation.authored = authoredKnowledgeCompilation{
		regexPrograms:      knowledgeprogram.MaximumRegexPrograms - charges.RegexPrograms,
		regexWorkUnits:     knowledgeprogram.MaximumRegexWorkUnits - charges.RegexWorkUnits,
		extractionOutputs:  knowledgeprogram.MaximumExtractionOutputs - charges.ExtractionOutputs,
		jsonEvaluationWork: knowledgeprogram.MaximumJSONEvaluationWork - charges.JSONEvaluationWork,
	}
	preparation.authoredScalarPredicates =
		knowledgeprogram.MaximumScalarPredicates - charges.ScalarPredicates
	preparation.authoredScalarPredicatesExact = true

	seed := []string{"caller-owned"}
	existing := []any{
		compiledReadScopeArgument{ordinal: 0, value: "tenant-one"},
		seed,
	}
	input := compiledRelation{sql: `SELECT ? AS "tenant", ? AS "seed"`, depth: 2}
	compiled, err := compileDeferredKnowledgeRelation(
		input,
		knowledgeExtractionStageState(),
		existing,
		preparation,
	)
	if err != nil {
		t.Fatalf("compile deferred knowledge relation: %v", err)
	}
	if compiled.args != nil || len(compiled.state.chronologicalBarriers) != 1 {
		t.Fatalf("active argument ownership = %#v", compiled.args)
	}
	barrier := compiled.state.chronologicalBarriers[0]
	wantArgs := []any{
		compiledReadScopeArgument{ordinal: 0, value: "tenant-one"},
		[]string{"caller-owned"},
	}
	for _, stage := range compiled.prelude.stages {
		wantArgs = append(wantArgs, stage.suffixArgs...)
	}
	if !reflect.DeepEqual(barrier.args, wantArgs) {
		t.Fatalf("barrier arguments = %#v, want %#v", barrier.args, wantArgs)
	}
	definitionsSQL := strings.Join(
		append(slices.Clone(barrier.prerequisiteDefinitions), barrier.sql),
		" ",
	)
	if got, want := strings.Count(definitionsSQL, "?"), len(barrier.args); got != want {
		t.Fatalf("barrier placeholders = %d, args = %d\n%s", got, want, definitionsSQL)
	}
	stagedDefinition := barrier.prerequisiteDefinitions[0]
	if got, want := strings.Count(stagedDefinition, " ARRAY JOIN "), len(compiled.prelude.stages); got != want {
		t.Fatalf("physical stage count = %d, want %d\n%s", got, want, stagedDefinition)
	}
	for stageIndex, stage := range compiled.prelude.stages {
		binding := " ARRAY JOIN " + strings.Join(stage.arrayJoinBindings, ", ")
		if got := strings.Count(stagedDefinition, binding); got != 1 {
			t.Fatalf("stage %d binding occurrences = %d, want 1\n%s", stageIndex, got, stagedDefinition)
		}
	}
	if compiled.prelude.proof.objectCount != program.ObjectCount() ||
		compiled.prelude.proof.charges != charges ||
		!slices.Equal(compiled.prelude.proof.operatorKinds, program.OperatorKinds()) {
		t.Fatalf(
			"deferred lowering proof = %#v, program charges = %#v",
			compiled.prelude.proof,
			charges,
		)
	}

	seed[0] = "mutated-caller"
	if barrier.args[1].([]string)[0] != "caller-owned" {
		t.Fatal("barrier arguments alias caller-owned arguments")
	}
	argumentOffset := len(existing)
	mutatedStageArgument := false
	for stageIndex := range compiled.prelude.stages {
		stage := &compiled.prelude.stages[stageIndex]
		for argumentIndex, argument := range stage.suffixArgs {
			values, ok := argument.([]string)
			if !ok || len(values) == 0 {
				argumentOffset++
				continue
			}
			barrierValues := barrier.args[argumentOffset].([]string)
			barrierValues[0] = "mutated-barrier"
			if stage.suffixArgs[argumentIndex].([]string)[0] == "mutated-barrier" {
				t.Fatal("barrier arguments alias retained prelude arguments")
			}
			values[0] = "mutated-prelude"
			if barrierValues[0] == "mutated-prelude" {
				t.Fatal("retained prelude arguments alias barrier arguments")
			}
			mutatedStageArgument = true
			break
		}
		if mutatedStageArgument {
			break
		}
	}
	if !mutatedStageArgument {
		t.Fatal("fixture has no mutable staged argument")
	}
	compiled.state.visible["state-only"] = canonicalState("state-only")
	if _, aliased := compiled.prelude.state.visible["state-only"]; aliased {
		t.Fatal("published state aliases retained prelude state")
	}
}

func TestCompileDeferredKnowledgeRelationValidationSurvivesDownstreamEmptyFilter(t *testing.T) {
	program := deferredMixedKnowledgeProgramForTest(t)
	preparation := knowledgePreludePreparationForTest(program)
	owner := spl.Range{
		Start: spl.Position{Offset: 8, Line: 1, Column: 9},
		End:   spl.Position{Offset: 30, Line: 1, Column: 31},
	}
	input := compiledRelation{
		sql:        `SELECT ? AS "__deferred_scan_seed"`,
		depth:      2,
		ownerRange: owner,
	}
	deferred, err := compileDeferredKnowledgeRelation(
		input,
		knowledgeExtractionStageState(),
		[]any{"scan-first"},
		preparation,
	)
	if err != nil {
		t.Fatalf("compile deferred knowledge relation: %v", err)
	}
	barrier := deferred.state.chronologicalBarriers[0]
	downstreamArgument := "downstream-last"
	downstream := deferred.relation.selectFrom(
		`SELECT *, toString(?) AS "downstream_marker" FROM (`+
			deferred.relation.sql+
			`) AS "__os_deferred_downstream" WHERE 0`,
		owner,
	)
	compiled, err := wrapChronologicalValidation(
		downstream.sql,
		downstream.depth,
		owner,
		deferred.state.chronologicalBarriers,
		[]string{`"downstream_marker"`},
		[]string{"downstream_marker"},
		"",
		eventStatsOrdinarySourceFanout,
		CompiledQuery{Args: []any{downstreamArgument}},
		0,
	)
	if err != nil {
		t.Fatalf("wrap deferred validation: %v", err)
	}
	if strings.LastIndex(compiled.SQL, "UNION ALL") < strings.LastIndex(compiled.SQL, "WHERE 0") {
		t.Fatalf("downstream empty filter escaped knowledge validation:\n%s", compiled.SQL)
	}
	hiddenValidation := barrier.validationColumns[0]
	validationRead := "SELECT toUInt8((" + hiddenValidation + " != 0)) AS " +
		quoteIdentifier("__os_chronological_invalid") + " FROM " + barrier.name
	for _, fragment := range []string{
		barrier.prerequisiteDefinitions[0],
		barrier.name + " AS (" + barrier.sql + ")",
		validationRead,
		materializedCTESettingsSQL,
	} {
		if !strings.Contains(compiled.SQL, fragment) {
			t.Fatalf("final validation graph omits %q:\n%s", fragment, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, " AS MATERIALIZED ("); got != 1 {
		t.Fatalf("flat graph materializations = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, " ARRAY JOIN "), len(deferred.prelude.stages); got != want {
		t.Fatalf("final graph physical stages = %d, want %d:\n%s", got, want, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, input.sql); got != 1 {
		t.Fatalf("final graph staged producers = %d, want 1:\n%s", got, compiled.SQL)
	}
	wantArgs := append(slices.Clone(barrier.args), downstreamArgument)
	if !reflect.DeepEqual(compiled.Args, wantArgs) {
		t.Fatalf("final arguments = %#v, want %#v", compiled.Args, wantArgs)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("final placeholders = %d, args = %d\n%s", got, want, compiled.SQL)
	}
}

func TestCompileDeferredKnowledgeRelationMarkerPrecedenceAndIdentity(t *testing.T) {
	t.Run("event rex query precedence", func(t *testing.T) {
		program := deferredMixedKnowledgeProgramForTest(t)
		preparation := knowledgePreludePreparationForTest(program)
		compiled, err := compileDeferredKnowledgeRelation(
			compiledRelation{sql: "SELECT 1", depth: 1},
			knowledgeExtractionStageState(),
			nil,
			preparation,
		)
		if err != nil {
			t.Fatalf("compile deferred knowledge relation: %v", err)
		}
		barrier := compiled.state.chronologicalBarriers[0]
		violation, validation := deferredKnowledgeGuardExpressionsForTest(compiled.prelude)
		if !strings.Contains(barrier.prerequisiteDefinitions[1], violation) ||
			!strings.Contains(
				barrier.sql,
				"toUInt8("+validation+") AS "+barrier.validationColumns[0],
			) {
			t.Fatalf(
				"guard precedence expressions are incomplete:\n%s\n%s",
				barrier.prerequisiteDefinitions[1],
				barrier.sql,
			)
		}
		for _, code := range []string{" = toUInt8(1)", " = toUInt8(2)", " = toUInt8(3)"} {
			if !strings.Contains(barrier.sql, code) {
				t.Fatalf("guard validation omits exact code predicate %q:\n%s", code, barrier.sql)
			}
		}
		eventMarker := strings.Index(barrier.sql, KnowledgeSelectorEventLimitMarker)
		rexMarker := strings.Index(barrier.sql, RexCaptureLimitMarker)
		queryMarker := strings.Index(barrier.sql, KnowledgeSelectorQueryLimitMarker)
		if eventMarker < 0 || !(eventMarker < rexMarker && rexMarker < queryMarker) ||
			strings.Count(barrier.sql, KnowledgeSelectorEventLimitMarker) != 1 ||
			strings.Count(barrier.sql, RexCaptureLimitMarker) != 1 ||
			strings.Count(barrier.sql, KnowledgeSelectorQueryLimitMarker) != 1 ||
			strings.Contains(barrier.sql, " WHERE ") || strings.Contains(barrier.sql, ">=") {
			t.Fatalf("guard marker precedence = %d, %d, %d\n%s", eventMarker, rexMarker, queryMarker, barrier.sql)
		}
	})

	t.Run("event query precedence without rex", func(t *testing.T) {
		program := knowledgePreludeProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{
			knowledgeJSONStageDefinition("deferred-json", "json_value", "payload.value", "east"),
		})
		compiled, err := compileDeferredKnowledgeRelation(
			compiledRelation{sql: "SELECT 1", depth: 1},
			knowledgeExtractionStageState(),
			nil,
			knowledgePreludePreparationForTest(program),
		)
		if err != nil {
			t.Fatalf("compile capture-free deferred relation: %v", err)
		}
		barrier := compiled.state.chronologicalBarriers[0]
		eventMarker := strings.Index(barrier.sql, KnowledgeSelectorEventLimitMarker)
		queryMarker := strings.Index(barrier.sql, KnowledgeSelectorQueryLimitMarker)
		if eventMarker < 0 || queryMarker <= eventMarker ||
			strings.Contains(barrier.sql, RexCaptureLimitMarker) ||
			strings.Contains(barrier.sql, `"__os_ko_rex_captured_bytes_`) {
			t.Fatalf("capture-free guard marker precedence = %d, %d\n%s", eventMarker, queryMarker, barrier.sql)
		}
	})

	t.Run("absent and empty identity", func(t *testing.T) {
		emptyProgram, err := knowledgeprogram.Prepare(knowledgeprogram.Input{})
		if err != nil {
			t.Fatalf("prepare empty program: %v", err)
		}
		tests := []struct {
			name        string
			preparation preparedKnowledgeCompilation
			args        []any
			present     bool
		}{
			{name: "absent", preparation: preparedKnowledgeCompilation{}},
			{
				name:        "present empty",
				preparation: knowledgePreludePreparationForTest(emptyProgram),
				args:        make([]any, 0),
				present:     true,
			},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				input := compiledRelation{sql: "SELECT 1", depth: 4}
				scanState := knowledgeExtractionStageState()
				compiled, compileErr := compileDeferredKnowledgeRelation(
					input,
					scanState,
					test.args,
					test.preparation,
				)
				if compileErr != nil || compiled.relation != input ||
					(compiled.args == nil) != (test.args == nil) ||
					len(compiled.args) != 0 || len(compiled.state.chronologicalBarriers) != 0 ||
					compiled.prelude.present != test.present ||
					compiled.prelude.proof.charges != (knowledgeprogram.Charges{}) ||
					!reflect.DeepEqual(compiled.state, compiled.prelude.state) {
					t.Fatalf("%s identity = %#v, %v", test.name, compiled, compileErr)
				}
				compiled.state.visible["identity-only"] = canonicalState("identity-only")
				if _, aliased := compiled.prelude.state.visible["identity-only"]; aliased {
					t.Fatal("identity result state aliases retained prelude state")
				}
				if _, aliased := scanState.visible["identity-only"]; aliased {
					t.Fatal("identity result state aliases input state")
				}
			})
		}

		caller := []string{"original"}
		detached, compileErr := compileDeferredKnowledgeRelation(
			compiledRelation{sql: "SELECT ?", depth: 1},
			knowledgeExtractionStageState(),
			[]any{caller},
			preparedKnowledgeCompilation{},
		)
		if compileErr != nil {
			t.Fatalf("compile detached identity: %v", compileErr)
		}
		caller[0] = "mutated"
		if detached.args[0].([]string)[0] != "original" {
			t.Fatal("identity arguments alias caller-owned arguments")
		}
	})

	t.Run("rejects forged authority and placeholder mismatch", func(t *testing.T) {
		program := deferredMixedKnowledgeProgramForTest(t)
		forged := knowledgePreludePreparationForTest(program)
		forged.programCommitment[0] ^= 1
		if _, err := compileDeferredKnowledgeRelation(
			compiledRelation{sql: "SELECT 1", depth: 1},
			knowledgeExtractionStageState(),
			nil,
			forged,
		); err == nil {
			t.Fatal("forged preparation compiled")
		}
		if _, err := compileDeferredKnowledgeRelation(
			compiledRelation{sql: "SELECT ?", depth: 1},
			knowledgeExtractionStageState(),
			nil,
			knowledgePreludePreparationForTest(program),
		); err == nil {
			t.Fatal("placeholder-mismatched input compiled")
		}

		collisionState := knowledgeExtractionStageState()
		collisionName := strings.Trim(knowledgeRuntimeGuardValidationColumn, `"`)
		collisionState.visible[collisionName] = canonicalState(collisionName)
		collisionState.publicOrder = append(collisionState.publicOrder, collisionName)
		if _, err := compileDeferredKnowledgeRelation(
			compiledRelation{sql: "SELECT 1", depth: 1},
			collisionState,
			nil,
			knowledgePreludePreparationForTest(program),
		); err == nil {
			t.Fatal("validation-column collision compiled")
		}
	})

	t.Run("descriptor SQL byte boundary", func(t *testing.T) {
		barrier := compiledChronologicalBarrier{
			name: knowledgeRuntimeGuardResultName,
			prerequisiteDefinitions: []string{
				"",
				knowledgeRuntimeGuardTotalsName + " AS (SELECT 0)",
			},
			sql: "SELECT 1",
		}
		published := compiledRelation{sql: "SELECT 1", depth: 1}
		base, ok := deferredKnowledgeDescriptorSQLBytes(barrier, published)
		if !ok || base >= maxCompiledQueryBytes {
			t.Fatalf("descriptor base size = %d, %t", base, ok)
		}
		barrier.prerequisiteDefinitions[0] = strings.Repeat(
			"x",
			maxCompiledQueryBytes-base,
		)
		if got, exact := deferredKnowledgeDescriptorSQLBytes(barrier, published); !exact || got != maxCompiledQueryBytes {
			t.Fatalf("exact descriptor size = %d, %t", got, exact)
		}
		if err := validateDeferredKnowledgeDescriptorSize(barrier, published); err != nil {
			t.Fatalf("exact descriptor rejected: %v", err)
		}
		barrier.prerequisiteDefinitions[0] += "x"
		if _, within := deferredKnowledgeDescriptorSQLBytes(barrier, published); within {
			t.Fatal("over-limit descriptor reported within bound")
		}
		var diagnostic *plan.Diagnostic
		if err := validateDeferredKnowledgeDescriptorSize(barrier, published); !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
			t.Fatalf("over-limit descriptor error = %#v", err)
		}
	})
}

func deferredMixedKnowledgeProgramForTest(t *testing.T) knowledgeprogram.Program {
	t.Helper()
	return knowledgePreludeProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{
		knowledgeRegexStageDefinition(
			"deferred-regex",
			`(?P<regex_value>[a-z]+)`,
			[]string{"regex_value"},
			"west",
		),
		knowledgePreludeAliasDefinition("deferred-alias", "host", "alias_value"),
		knowledgePreludeCalculatedDefinition(
			"deferred-calculated",
			"calculated_value",
			"lower(source)",
		),
	})
}

func composeDeferredKnowledgeStagesForTest(
	input compiledRelation,
	prelude compiledKnowledgePrelude,
) compiledRelation {
	current := input
	for _, stage := range prelude.stages {
		inputAlias := quoteIdentifier(
			"__os_ko_stage_input_" + strconv.Itoa(stage.operatorOffset),
		)
		bindingAlias := quoteIdentifier(
			"__os_ko_stage_bound_" + strconv.Itoa(stage.operatorOffset),
		)
		bindingSQL := "SELECT " + strings.Join(stage.bindingProjection, ", ") +
			" FROM (" + current.sql + ") AS " + inputAlias + " ARRAY JOIN " +
			strings.Join(stage.arrayJoinBindings, ", ")
		current = current.selectFrom(bindingSQL, input.ownerRange)
		projectionSQL := "SELECT " + strings.Join(stage.projection, ", ") +
			" FROM (" + current.sql + ") AS " + bindingAlias
		current = current.selectFrom(projectionSQL, input.ownerRange)
	}
	return current
}

func deferredKnowledgeGuardExpressionsForTest(
	prelude compiledKnowledgePrelude,
) (string, string) {
	selectorEventMaximum := "maxOrDefault(toUInt128(" +
		prelude.selectorCharges.inputBytes + "))"
	selectorQueryTotal := "sum(toUInt128(" + prelude.selectorCharges.queryUnits + "))"
	selectorEventOver := selectorEventMaximum + " > toUInt128(" +
		strconv.Itoa(knowledge.MaximumSelectorRuntimeEventBytes) + ")"
	selectorQueryOver := selectorQueryTotal + " > toUInt128(" +
		strconv.Itoa(knowledge.MaximumSelectorRuntimeQueryUnits) + ")"
	violation := "multiIf(" + selectorEventOver + ", toUInt8(1), "
	if prelude.capturedBytes != "" {
		rexMaximum := "maxOrDefault(toUInt128(" + prelude.capturedBytes + "))"
		rexOver := rexMaximum + " > toUInt128(" +
			strconv.FormatUint(MaximumRexCapturedBytesPerRow, 10) + ")"
		violation += rexOver + ", toUInt8(2), "
	}
	violation += selectorQueryOver + ", toUInt8(3), toUInt8(0))"

	violationRef := `"__os_ko_guard_total"."__os_ko_guard_violation"`
	eventViolation := violationRef + " = toUInt8(1)"
	queryViolation := violationRef + " = toUInt8(3)"
	validation := "if(" + eventViolation + ", " +
		knowledgeRuntimeGuardThrow(eventViolation, KnowledgeSelectorEventLimitMarker) + ", "
	if prelude.capturedBytes != "" {
		rexViolation := violationRef + " = toUInt8(2)"
		validation += "if(" + rexViolation + ", " +
			knowledgeRuntimeGuardThrow(rexViolation, RexCaptureLimitMarker) + ", " +
			knowledgeRuntimeGuardThrow(queryViolation, KnowledgeSelectorQueryLimitMarker) + ")"
	} else {
		validation += knowledgeRuntimeGuardThrow(
			queryViolation,
			KnowledgeSelectorQueryLimitMarker,
		)
	}
	return violation, validation + ")"
}
