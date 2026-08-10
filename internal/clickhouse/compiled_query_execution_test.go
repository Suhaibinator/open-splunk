package clickhouse

import (
	"bytes"
	"errors"
	"reflect"
	"slices"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/splpath"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

const testNonemptyKnowledgeSealError = "seal compiled ClickHouse execution: nonempty knowledge lowering is absent"

func TestCompiledKnowledgeSnapshotEvidencePinsAuthoredWholeQueryCharges(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t,
		`index=gradethis | rex field=_raw "(?<word>[a-z]+)" `+
			`| spath input=_raw output=selected path=payload.value `+
			`| eval status=if(isnull(selected), "missing", "present") `+
			`| where status="present"`,
	)
	evidence, ok := compiled.KnowledgeSnapshotEvidence()
	if !ok {
		t.Fatal("compiler-produced evidence is absent")
	}
	pattern, err := splregex.CompileExtractionPattern(`(?<word>[a-z]+)`)
	if err != nil {
		t.Fatalf("CompileExtractionPattern: %v", err)
	}
	steps, err := splpath.ParseJSON("payload.value")
	if err != nil {
		t.Fatalf("ParseJSON: %v", err)
	}
	if evidence.TenantID() != "tenant-1" ||
		!slices.Equal(evidence.EffectiveIndexes(), []string{"gradethis"}) ||
		evidence.KnowledgeProgramPresent() ||
		evidence.KnowledgeProgramObjectCount() != 0 ||
		evidence.KnowledgeProgramCharges() != (knowledgeprogram.Charges{}) ||
		evidence.GeneratedOperators() != 0 || evidence.GeneratedFields() != 0 ||
		evidence.RegexPrograms() != 1 ||
		evidence.RegexWorkUnits() != uint64(pattern.ProgramWorkUnits) ||
		evidence.RegexCaptureBytes() != MaximumRexCapturedBytesPerRow ||
		evidence.ExtractionOutputs() != 2 ||
		evidence.JSONEvaluationWork() != uint32(splpath.EvaluationWorkUnits(steps)) ||
		evidence.ScalarExpressions() != 0 || evidence.ScalarExpressionNodes() != 0 ||
		evidence.ScalarPredicates() != 2 ||
		evidence.AuthoredRegexPrograms() != 1 ||
		evidence.AuthoredRegexWorkUnits() != uint64(pattern.ProgramWorkUnits) ||
		evidence.AuthoredExtractionOutputs() != 2 ||
		evidence.AuthoredJSONEvaluationWork() != uint32(splpath.EvaluationWorkUnits(steps)) ||
		evidence.AuthoredScalarPredicates() != 2 ||
		evidence.GeneratedSQLBytes() != uint64(len(compiled.SQL)) {
		t.Fatalf("knowledge compiler evidence = %#v", evidence)
	}
	if commitment, ok := evidence.KnowledgeProgramCommitment(); ok || commitment != ([32]byte{}) {
		t.Fatalf("legacy knowledge commitment = %x/%t", commitment, ok)
	}

	indexes := evidence.EffectiveIndexes()
	indexes[0] = "mutated"
	if evidence.EffectiveIndexes()[0] != "gradethis" {
		t.Fatal("knowledge evidence indexes alias caller memory")
	}
}

func TestCompiledKnowledgeSnapshotEvidenceDistinguishesLegacyAndPresentEmpty(t *testing.T) {
	t.Parallel()

	empty, err := knowledgeprogram.Prepare(knowledgeprogram.Input{})
	if err != nil {
		t.Fatalf("Prepare(empty): %v", err)
	}
	logical := buildPlan(t, `index=gradethis | where status=200`)
	legacy, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(legacy): %v", err)
	}
	admittedLogical, err := plan.InjectKnowledgePrelude(logical, empty)
	if err != nil {
		t.Fatalf("InjectKnowledgePrelude(empty): %v", err)
	}
	admitted, err := (Compiler{}).Compile(admittedLogical)
	if err != nil {
		t.Fatalf("Compile(present empty): %v", err)
	}

	legacyEvidence, legacyOK := legacy.KnowledgeSnapshotEvidence()
	admittedEvidence, admittedOK := admitted.KnowledgeSnapshotEvidenceFor(empty)
	wantCommitment, commitmentOK := empty.Commitment()
	gotCommitment, gotCommitmentOK := admittedEvidence.KnowledgeProgramCommitment()
	if !legacyOK || legacyEvidence.KnowledgeProgramPresent() || !admittedOK ||
		!admittedEvidence.KnowledgeProgramPresent() ||
		admittedEvidence.KnowledgeProgramObjectCount() != 0 ||
		admittedEvidence.KnowledgeProgramCharges() != (knowledgeprogram.Charges{}) ||
		!commitmentOK || !gotCommitmentOK || gotCommitment != wantCommitment {
		t.Fatalf("legacy/admitted evidence = (%#v, %t)/(%#v, %t)", legacyEvidence, legacyOK, admittedEvidence, admittedOK)
	}
	if _, ok := legacy.KnowledgeSnapshotEvidenceFor(empty); ok {
		t.Fatal("legacy compiled query satisfied a present-empty program")
	}
	if _, ok := admitted.KnowledgeSnapshotEvidenceFor(knowledgeprogram.Program{}); ok {
		t.Fatal("present-empty compiled query accepted an absent supplied program")
	}
	if legacy.EqualForExecution(admitted) {
		t.Fatal("absent and present-empty knowledge authority had equal execution seals")
	}

	cloned, ok := admitted.CloneForExecution()
	retained, retainedOK := admitted.RetainedBytes()
	clonedRetained, clonedRetainedOK := cloned.RetainedBytes()
	if !ok || !admitted.EqualForExecution(cloned) ||
		retained == 0 || !retainedOK || !clonedRetainedOK || retained != clonedRetained {
		t.Fatal("present-empty execution authority did not clone and retain exactly")
	}
	if _, ok := cloned.KnowledgeSnapshotEvidenceFor(empty); !ok {
		t.Fatal("detached clone lost its exact present-empty knowledge authority")
	}
}

func TestCompiledKnowledgeSnapshotEvidenceSealsProgramIdentityAndSplitCharges(t *testing.T) {
	t.Parallel()

	firstProgram := testKnowledgeEvidenceProgram(t, "first")
	secondProgram := testKnowledgeEvidenceProgram(t, "second")
	if firstProgram.Charges() != secondProgram.Charges() || firstProgram.Equal(secondProgram) {
		t.Fatalf("test programs do not have equal charges and different meaning: %+v/%+v", firstProgram.Charges(), secondProgram.Charges())
	}

	legacy := compileSPL(t,
		`index=gradethis | rex field=_raw "(?<authored>[0-9]+)" `+
			`| spath input=_raw output=selected path=payload.value `+
			`| where selected="present"`,
	)
	first := sealTestKnowledgeProgram(t, legacy, firstProgram)
	second := sealTestKnowledgeProgram(t, legacy, secondProgram)
	evidence, ok := first.KnowledgeSnapshotEvidenceFor(firstProgram)
	legacyEvidence, legacyOK := legacy.KnowledgeSnapshotEvidence()
	if !ok || !legacyOK {
		t.Fatal("sealed program or authored evidence is absent")
	}
	if _, ok := first.KnowledgeSnapshotEvidenceFor(secondProgram); ok {
		t.Fatal("same-charge program substituted for the sealed program")
	}
	if first.EqualForExecution(second) {
		t.Fatal("same-charge programs produced equal execution authority")
	}
	if evidence.KnowledgeProgramCharges() != firstProgram.Charges() ||
		evidence.KnowledgeProgramObjectCount() != firstProgram.ObjectCount() ||
		evidence.AuthoredRegexPrograms() != legacyEvidence.RegexPrograms() ||
		evidence.AuthoredRegexWorkUnits() != legacyEvidence.RegexWorkUnits() ||
		evidence.AuthoredExtractionOutputs() != legacyEvidence.ExtractionOutputs() ||
		evidence.AuthoredJSONEvaluationWork() != legacyEvidence.JSONEvaluationWork() ||
		evidence.AuthoredScalarPredicates() != legacyEvidence.ScalarPredicates() {
		t.Fatalf("split evidence = %#v, authored = %#v", evidence, legacyEvidence)
	}
	knowledge := firstProgram.Charges()
	if evidence.GeneratedOperators() != knowledge.GeneratedOperators ||
		evidence.GeneratedFields() != knowledge.GeneratedFields ||
		evidence.RegexPrograms() != knowledge.RegexPrograms+evidence.AuthoredRegexPrograms() ||
		evidence.RegexWorkUnits() != knowledge.RegexWorkUnits+evidence.AuthoredRegexWorkUnits() ||
		evidence.ExtractionOutputs() != knowledge.ExtractionOutputs+evidence.AuthoredExtractionOutputs() ||
		evidence.JSONEvaluationWork() != knowledge.JSONEvaluationWork+evidence.AuthoredJSONEvaluationWork() ||
		evidence.ScalarExpressions() != knowledge.ScalarExpressions ||
		evidence.ScalarExpressionNodes() != knowledge.ScalarExpressionNodes ||
		evidence.ScalarPredicates() != knowledge.ScalarPredicates+evidence.AuthoredScalarPredicates() ||
		evidence.RegexCaptureBytes() != MaximumRexCapturedBytesPerRow {
		t.Fatalf("whole-query totals = %#v, knowledge = %+v", evidence, knowledge)
	}
}

func TestCompileKnowledgeCompilationEvidenceDerivesExactNonemptyProof(t *testing.T) {
	t.Parallel()

	program := testKnowledgeEvidenceProgram(t, "derived")
	query, preparation, prelude := testKnowledgeCompilationEvidenceFixture(
		t,
		program,
		`index=gradethis | rex field=_raw "(?<authored>[0-9]+)" `+
			`| spath input=_raw output=selected path=payload.value `+
			`| where selected="present"`,
	)
	const generatedSQLBytes = 12345
	evidence, err := compileKnowledgeCompilationEvidence(
		preparation,
		prelude,
		generatedSQLBytes,
	)
	if err != nil {
		t.Fatalf("compile knowledge evidence: %v", err)
	}
	commitment, ok := program.Commitment()
	wantAuthored := authoredKnowledgeCompilationEvidence{
		regexPrograms:      preparation.authored.regexPrograms,
		regexWorkUnits:     preparation.authored.regexWorkUnits,
		extractionOutputs:  preparation.authored.extractionOutputs,
		jsonEvaluationWork: preparation.authored.jsonEvaluationWork,
		scalarPredicates:   preparation.authoredScalarPredicates,
	}
	if evidence == nil || !ok || !evidence.prelude.present ||
		evidence.prelude.commitment != commitment ||
		evidence.prelude.objectCount != prelude.proof.objectCount ||
		evidence.prelude.objectCount != program.ObjectCount() ||
		evidence.prelude.charges != prelude.proof.charges ||
		evidence.prelude.charges != program.Charges() ||
		evidence.authored != wantAuthored ||
		evidence.regexCaptureBytes != MaximumRexCapturedBytesPerRow ||
		evidence.generatedSQLBytes != generatedSQLBytes {
		t.Fatalf("derived nonempty evidence = %#v", evidence)
	}

	scan, ok := query.Operators[0].(*plan.Scan)
	if !ok || scan == nil || len(scan.Indexes) != 1 {
		t.Fatalf("fixture scan = %T", query.Operators[0])
	}
	sealed, sealErr := sealFinalCompiledQuery(
		CompiledQuery{
			SQL: "SELECT ?, ?",
			Args: []any{
				compiledReadScopeArgument{ordinal: 0, value: scan.TenantID},
				compiledReadScopeArgument{ordinal: 1, value: scan.Indexes[0]},
			},
		},
		query,
		scan,
		preparation,
		prelude,
	)
	if knowledgeRuntimeAcceptanceEnabled() {
		if sealErr != nil || !sealed.HasValidExecutionSeal() {
			t.Fatalf("acceptance-mode nonempty seal = (%#v, %v), want trusted execution", sealed, sealErr)
		}
		if sealedEvidence, sealedOK := sealed.KnowledgeSnapshotEvidenceFor(program); !sealedOK ||
			sealedEvidence.KnowledgeProgramObjectCount() != program.ObjectCount() {
			t.Fatalf("acceptance-mode sealed evidence = (%#v, %t)", sealedEvidence, sealedOK)
		}
	} else if sealErr == nil || sealErr.Error() != testNonemptyKnowledgeSealError {
		t.Fatalf("default nonempty seal error = %v", sealErr)
	}
}

func TestCompileKnowledgeCompilationEvidenceRejectsSameChargeProgramSubstitution(t *testing.T) {
	t.Parallel()

	firstProgram := testKnowledgeEvidenceProgram(t, "derived-first")
	secondProgram := testKnowledgeEvidenceProgram(t, "derived-second")
	if firstProgram.Charges() != secondProgram.Charges() || firstProgram.Equal(secondProgram) {
		t.Fatalf(
			"test programs are not equal-cost distinct authority: %+v/%+v",
			firstProgram.Charges(),
			secondProgram.Charges(),
		)
	}
	_, firstPreparation, firstPrelude := testKnowledgeCompilationEvidenceFixture(
		t,
		firstProgram,
		`index=gradethis | where status=200`,
	)
	secondQuery, secondPreparation, _ := testKnowledgeCompilationEvidenceFixture(
		t,
		secondProgram,
		`index=gradethis | where status=200`,
	)
	firstEvidence, err := compileKnowledgeCompilationEvidence(
		firstPreparation,
		firstPrelude,
		1,
	)
	if err != nil || firstEvidence == nil {
		t.Fatalf("compile first evidence = (%#v, %v)", firstEvidence, err)
	}
	if firstEvidence.matchesProgram(secondProgram) {
		t.Fatal("same-charge program matched the first physical evidence")
	}
	if _, err := compileKnowledgeCompilationEvidence(
		secondPreparation,
		firstPrelude,
		1,
	); err == nil {
		t.Fatal("first physical prelude derived evidence for the second program")
	}

	scan, ok := secondQuery.Operators[0].(*plan.Scan)
	if !ok || scan == nil {
		t.Fatalf("fixture scan = %T", secondQuery.Operators[0])
	}
	if _, err := sealFinalCompiledQuery(
		CompiledQuery{SQL: "SELECT 1"},
		secondQuery,
		scan,
		secondPreparation,
		firstPrelude,
	); err == nil || err.Error() == testNonemptyKnowledgeSealError {
		t.Fatalf("same-charge substituted seal error = %v", err)
	}
}

func TestCompileKnowledgeCompilationEvidenceRejectsEmittedProofMismatch(t *testing.T) {
	t.Parallel()

	program := testKnowledgeEvidenceProgram(t, "derived-forged")
	query, preparation, prelude := testKnowledgeCompilationEvidenceFixture(
		t,
		program,
		`index=gradethis | where status=200`,
	)
	if len(prelude.proof.calculated) == 0 {
		t.Fatal("fixture has no emitted calculated proof")
	}
	forged := prelude
	forged.proof.calculated = nil
	if _, err := compileKnowledgeCompilationEvidence(
		preparation,
		forged,
		1,
	); err == nil {
		t.Fatal("mismatched emitted proof derived evidence")
	}

	scan, ok := query.Operators[0].(*plan.Scan)
	if !ok || scan == nil {
		t.Fatalf("fixture scan = %T", query.Operators[0])
	}
	if _, err := sealFinalCompiledQuery(
		CompiledQuery{SQL: "SELECT 1"},
		query,
		scan,
		preparation,
		forged,
	); err == nil || err.Error() == testNonemptyKnowledgeSealError {
		t.Fatalf("forged emitted-proof seal error = %v", err)
	}
}

func TestCompileKnowledgeCompilationEvidencePreservesAuthoredSplitAndCeilings(t *testing.T) {
	t.Parallel()

	program := testKnowledgeEvidenceProgram(t, "derived-ceilings")
	_, preparation, prelude := testKnowledgeCompilationEvidenceFixture(
		t,
		program,
		`index=gradethis`,
	)
	charges := prelude.proof.charges
	preparation.authored = authoredKnowledgeCompilation{
		regexPrograms:      knowledgeprogram.MaximumRegexPrograms - charges.RegexPrograms,
		regexWorkUnits:     knowledgeprogram.MaximumRegexWorkUnits - charges.RegexWorkUnits,
		extractionOutputs:  knowledgeprogram.MaximumExtractionOutputs - charges.ExtractionOutputs,
		jsonEvaluationWork: knowledgeprogram.MaximumJSONEvaluationWork - charges.JSONEvaluationWork,
	}
	preparation.authoredScalarPredicates =
		knowledgeprogram.MaximumScalarPredicates - charges.ScalarPredicates
	preparation.authoredScalarPredicatesExact = true
	evidence, err := compileKnowledgeCompilationEvidence(preparation, prelude, 7)
	if err != nil {
		t.Fatalf("compile evidence at shared ceilings: %v", err)
	}
	if evidence == nil || evidence.prelude.charges != charges ||
		evidence.authored.regexPrograms != preparation.authored.regexPrograms ||
		evidence.authored.regexWorkUnits != preparation.authored.regexWorkUnits ||
		evidence.authored.extractionOutputs != preparation.authored.extractionOutputs ||
		evidence.authored.jsonEvaluationWork != preparation.authored.jsonEvaluationWork ||
		evidence.authored.scalarPredicates != preparation.authoredScalarPredicates {
		t.Fatalf("split ceiling evidence = %#v", evidence)
	}
	snapshot := KnowledgeSnapshotEvidence{compiled: *evidence}
	if snapshot.RegexPrograms() != knowledgeprogram.MaximumRegexPrograms ||
		snapshot.RegexWorkUnits() != knowledgeprogram.MaximumRegexWorkUnits ||
		snapshot.ExtractionOutputs() != knowledgeprogram.MaximumExtractionOutputs ||
		snapshot.JSONEvaluationWork() != knowledgeprogram.MaximumJSONEvaluationWork ||
		snapshot.ScalarPredicates() != knowledgeprogram.MaximumScalarPredicates {
		t.Fatalf("combined ceiling evidence = %#v", snapshot)
	}

	tests := []struct {
		name   string
		mutate func(*preparedKnowledgeCompilation)
	}{
		{name: "regex programs", mutate: func(value *preparedKnowledgeCompilation) {
			value.authored.regexPrograms++
		}},
		{name: "regex work", mutate: func(value *preparedKnowledgeCompilation) {
			value.authored.regexWorkUnits++
		}},
		{name: "extraction outputs", mutate: func(value *preparedKnowledgeCompilation) {
			value.authored.extractionOutputs++
		}},
		{name: "JSON work", mutate: func(value *preparedKnowledgeCompilation) {
			value.authored.jsonEvaluationWork++
		}},
		{name: "scalar predicates", mutate: func(value *preparedKnowledgeCompilation) {
			value.authoredScalarPredicates++
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := preparation
			test.mutate(&candidate)
			_, err := compileKnowledgeCompilationEvidence(candidate, prelude, 7)
			var diagnostic *plan.Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
				t.Fatalf("overflow evidence error = %v", err)
			}
		})
	}
}

func TestCompileKnowledgeCompilationEvidencePreservesPresentEmptyParity(t *testing.T) {
	t.Parallel()

	source := `index=gradethis | rex field=_raw "(?<word>[a-z]+)" ` +
		`| spath input=_raw output=selected path=payload.value ` +
		`| where selected="present"`
	legacyQuery := buildPlan(t, source)
	legacyPreparation, err := prepareKnowledgeCompilation(legacyQuery)
	if err != nil {
		t.Fatalf("prepare legacy: %v", err)
	}
	legacyPrelude, err := compileKnowledgePrelude(
		knowledgeExtractionStageState(),
		legacyPreparation,
	)
	if err != nil {
		t.Fatalf("compile legacy identity prelude: %v", err)
	}
	legacy, err := (Compiler{}).Compile(legacyQuery)
	if err != nil {
		t.Fatalf("compile legacy: %v", err)
	}
	legacyEvidence, err := compileKnowledgeCompilationEvidence(
		legacyPreparation,
		legacyPrelude,
		uint64(len(legacy.SQL)),
	)
	if err != nil {
		t.Fatalf("derive legacy evidence: %v", err)
	}

	empty, err := knowledgeprogram.Prepare(knowledgeprogram.Input{})
	if err != nil {
		t.Fatalf("prepare empty program: %v", err)
	}
	presentEmptyQuery, err := plan.InjectKnowledgePrelude(legacyQuery, empty)
	if err != nil {
		t.Fatalf("inject empty prelude: %v", err)
	}
	presentEmptyPreparation, err := prepareKnowledgeCompilation(presentEmptyQuery)
	if err != nil {
		t.Fatalf("prepare present-empty: %v", err)
	}
	presentEmptyPrelude, err := compileKnowledgePrelude(
		knowledgeExtractionStageState(),
		presentEmptyPreparation,
	)
	if err != nil {
		t.Fatalf("compile present-empty identity prelude: %v", err)
	}
	presentEmpty, err := (Compiler{}).Compile(presentEmptyQuery)
	if err != nil {
		t.Fatalf("compile present-empty: %v", err)
	}
	presentEmptyEvidence, err := compileKnowledgeCompilationEvidence(
		presentEmptyPreparation,
		presentEmptyPrelude,
		uint64(len(presentEmpty.SQL)),
	)
	if err != nil {
		t.Fatalf("derive present-empty evidence: %v", err)
	}

	if legacyEvidence == nil || presentEmptyEvidence == nil ||
		legacyEvidence.prelude.present || !presentEmptyEvidence.prelude.present ||
		legacyEvidence.authored != presentEmptyEvidence.authored ||
		legacyEvidence.regexCaptureBytes != presentEmptyEvidence.regexCaptureBytes ||
		legacyEvidence.generatedSQLBytes != presentEmptyEvidence.generatedSQLBytes ||
		legacy.SQL != presentEmpty.SQL ||
		!reflect.DeepEqual(legacy.knowledgeEvidence, legacyEvidence) ||
		!reflect.DeepEqual(presentEmpty.knowledgeEvidence, presentEmptyEvidence) {
		t.Fatalf(
			"identity evidence parity = legacy:%#v/%#v empty:%#v/%#v",
			legacy.knowledgeEvidence,
			legacyEvidence,
			presentEmpty.knowledgeEvidence,
			presentEmptyEvidence,
		)
	}
	commitment, ok := empty.Commitment()
	if !ok || presentEmptyEvidence.prelude.commitment != commitment ||
		presentEmptyEvidence.prelude.objectCount != 0 ||
		presentEmptyEvidence.prelude.charges != (knowledgeprogram.Charges{}) ||
		legacy.EqualForExecution(presentEmpty) {
		t.Fatalf("present-empty authority = %#v / equal=%v", presentEmptyEvidence, legacy.EqualForExecution(presentEmpty))
	}
}

func TestCompiledKnowledgeSnapshotEvidenceSealsEveryTerminalSearchShape(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | table event_id level`,
		`index=gradethis | timechart span=5m count BY level`,
		`index=gradethis | chart count OVER level BY status`,
	} {
		compiled := compileSPL(t, source)
		evidence, ok := compiled.KnowledgeSnapshotEvidence()
		if !ok || evidence.GeneratedSQLBytes() != uint64(len(compiled.SQL)) {
			t.Fatalf("%q terminal evidence = (%#v, %v)", source, evidence, ok)
		}
		cloned, ok := compiled.CloneForExecution()
		if !ok || !compiled.EqualForExecution(cloned) {
			t.Fatalf("%q terminal execution seal did not clone exactly", source)
		}
		if _, ok := compiled.RetainedBytes(); !ok {
			t.Fatalf("%q terminal execution has no retained-byte charge", source)
		}
	}
}

func TestCompiledKnowledgeSnapshotEvidenceRequiresParserOwnedUnmodifiedPredicatePlan(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis | where status=200`)
	if count, ok := logical.AuthoredScalarPredicateCount(); !ok || count != 1 {
		t.Fatalf("predicate provenance = (%d, %v), want (1, true)", count, ok)
	}
	// Duplicate the validated where filter after Build. Ordinary compilation is
	// still supported for internal plan fixtures, but parser provenance must no
	// longer mint snapshot evidence for the modified plan.
	logical.Operators = append(logical.Operators, logical.Operators[len(logical.Operators)-1])
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(mutated logical plan): %v", err)
	}
	if _, ok := compiled.KnowledgeSnapshotEvidence(); ok {
		t.Fatal("mutated logical plan retained parser-owned knowledge evidence")
	}
	if _, ok := compiled.CloneForExecution(); !ok {
		t.Fatal("ordinary sealed execution clone rejected a valid manual plan")
	}

	built := buildPlan(t, `index=gradethis`)
	manual := &plan.Query{
		Operators:        slices.Clone(built.Operators),
		EffectiveIndexes: slices.Clone(built.EffectiveIndexes),
		OutputFields:     slices.Clone(built.OutputFields),
		DynamicOutput:    built.DynamicOutput,
		SearchStart:      built.SearchStart,
		SearchTimezone:   built.SearchTimezone,
	}
	compiled, err = (Compiler{}).Compile(manual)
	if err != nil {
		t.Fatalf("Compile(manual logical plan): %v", err)
	}
	if _, ok := compiled.KnowledgeSnapshotEvidence(); ok {
		t.Fatal("direct logical plan minted parser-owned knowledge evidence")
	}
}

func TestCompiledQueryExecutionSealRejectsEveryPublicTamper(t *testing.T) {
	t.Parallel()

	program := testKnowledgeEvidenceProgram(t, "tamper")
	compiled := sealTestKnowledgeProgram(
		t,
		compileSPL(t, `index=gradethis status=200 | table event_id status`),
		program,
	)
	if _, ok := compiled.CloneForExecution(); !ok {
		t.Fatal("compiler output is not execution sealed")
	}
	if retained, ok := compiled.RetainedBytes(); !ok || retained <= uint64(len(compiled.SQL)) {
		t.Fatalf("RetainedBytes = (%d, %v)", retained, ok)
	}

	tests := []struct {
		name   string
		mutate func(*CompiledQuery)
	}{
		{name: "SQL", mutate: func(query *CompiledQuery) { query.SQL += " " }},
		{name: "non-scope argument", mutate: func(query *CompiledQuery) {
			query.Args[len(query.Args)-1] = "tampered"
		}},
		{name: "scope argument", mutate: func(query *CompiledQuery) {
			query.Args[query.readScope.argumentPositions[0]] = "other-tenant"
		}},
		{name: "output", mutate: func(query *CompiledQuery) { query.OutputFields[0] = "tampered" }},
		{name: "sparse contract", mutate: func(query *CompiledQuery) { query.SparseFields = !query.SparseFields }},
		{name: "knowledge present", mutate: func(query *CompiledQuery) { query.knowledgeEvidence.prelude.present = false }},
		{name: "knowledge commitment", mutate: func(query *CompiledQuery) { query.knowledgeEvidence.prelude.commitment[0] ^= 0xff }},
		{name: "knowledge object count", mutate: func(query *CompiledQuery) { query.knowledgeEvidence.prelude.objectCount++ }},
		{name: "knowledge operators", mutate: func(query *CompiledQuery) { query.knowledgeEvidence.prelude.charges.GeneratedOperators++ }},
		{name: "knowledge fields", mutate: func(query *CompiledQuery) { query.knowledgeEvidence.prelude.charges.GeneratedFields++ }},
		{name: "knowledge regex programs", mutate: func(query *CompiledQuery) { query.knowledgeEvidence.prelude.charges.RegexPrograms++ }},
		{name: "knowledge regex work", mutate: func(query *CompiledQuery) { query.knowledgeEvidence.prelude.charges.RegexWorkUnits++ }},
		{name: "knowledge extraction outputs", mutate: func(query *CompiledQuery) { query.knowledgeEvidence.prelude.charges.ExtractionOutputs++ }},
		{name: "knowledge JSON work", mutate: func(query *CompiledQuery) { query.knowledgeEvidence.prelude.charges.JSONEvaluationWork++ }},
		{name: "knowledge scalar expressions", mutate: func(query *CompiledQuery) { query.knowledgeEvidence.prelude.charges.ScalarExpressions++ }},
		{name: "knowledge scalar nodes", mutate: func(query *CompiledQuery) { query.knowledgeEvidence.prelude.charges.ScalarExpressionNodes++ }},
		{name: "knowledge scalar predicates", mutate: func(query *CompiledQuery) { query.knowledgeEvidence.prelude.charges.ScalarPredicates++ }},
		{name: "authored regex programs", mutate: func(query *CompiledQuery) { query.knowledgeEvidence.authored.regexPrograms++ }},
		{name: "authored regex work", mutate: func(query *CompiledQuery) { query.knowledgeEvidence.authored.regexWorkUnits++ }},
		{name: "authored extraction outputs", mutate: func(query *CompiledQuery) { query.knowledgeEvidence.authored.extractionOutputs++ }},
		{name: "authored JSON work", mutate: func(query *CompiledQuery) { query.knowledgeEvidence.authored.jsonEvaluationWork++ }},
		{name: "authored scalar predicates", mutate: func(query *CompiledQuery) { query.knowledgeEvidence.authored.scalarPredicates++ }},
		{name: "regex capture bytes", mutate: func(query *CompiledQuery) { query.knowledgeEvidence.regexCaptureBytes++ }},
		{name: "generated SQL bytes", mutate: func(query *CompiledQuery) { query.knowledgeEvidence.generatedSQLBytes++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mutated := compiled
			mutated.Args = slices.Clone(compiled.Args)
			mutated.OutputFields = slices.Clone(compiled.OutputFields)
			if compiled.knowledgeEvidence != nil {
				evidence := *compiled.knowledgeEvidence
				mutated.knowledgeEvidence = &evidence
			}
			test.mutate(&mutated)
			if _, ok := mutated.CloneForExecution(); ok {
				t.Fatal("tampered query cloned for execution")
			}
			if _, ok := mutated.RetainedBytes(); ok {
				t.Fatal("tampered query produced a retained-byte charge")
			}
			if _, ok := mutated.KnowledgeSnapshotEvidence(); ok {
				t.Fatal("tampered query opened knowledge evidence")
			}
			if _, ok := mutated.KnowledgeSnapshotEvidenceFor(program); ok {
				t.Fatal("tampered query opened exact-program knowledge evidence")
			}
		})
	}
}

func TestCompiledQueryCloneForExecutionIsDetachedAndPreservesSeal(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | chart count OVER level BY status`)
	cloned, ok := compiled.CloneForExecution()
	compiledDigest, digestOK := compiled.ExecutionAuthorityDigest()
	clonedDigest, clonedDigestOK := cloned.ExecutionAuthorityDigest()
	if !ok || !cloned.HasValidExecutionSeal() || !compiled.HasValidExecutionSeal() ||
		!compiled.EqualForExecution(cloned) || !digestOK || !clonedDigestOK ||
		compiledDigest != clonedDigest {
		t.Fatal("CloneForExecution rejected compiler output")
	}
	if cloned.Chart == compiled.Chart || &cloned.Args[0] == &compiled.Args[0] ||
		&cloned.OutputFields[0] == &compiled.OutputFields[0] {
		t.Fatal("execution clone retained caller-owned mutable storage")
	}
	cloned.Args[0] = "other-tenant"
	cloned.OutputFields[0] = "other-output"
	cloned.Chart.RowField = "other-row"
	if compiled.Args[0] == "other-tenant" || compiled.OutputFields[0] == "other-output" ||
		compiled.Chart.RowField == "other-row" {
		t.Fatal("execution clone mutation reached original compiler output")
	}
	if cloned.hasValidExecutionSeal() {
		t.Fatal("mutated detached clone retained its execution seal")
	}
	if _, ok := cloned.ExecutionAuthorityDigest(); ok {
		t.Fatal("mutated detached clone exposed an execution authority digest")
	}
	if compiled.EqualForExecution(cloned) || (CompiledQuery{}).EqualForExecution(CompiledQuery{}) {
		t.Fatal("invalid execution values compared equal")
	}
	if !compiled.hasValidExecutionSeal() {
		t.Fatal("detached clone mutation invalidated original execution seal")
	}
}

func TestSealCompiledQueryExecutionRejectsUnsupportedBindTypes(t *testing.T) {
	t.Parallel()

	compiled := CompiledQuery{SQL: "SELECT ?", Args: []any{map[string]string{"x": "y"}}}
	if sealed, err := sealCompiledQueryExecution(compiled); err == nil || sealed.executionSeal != nil {
		t.Fatalf("seal unsupported bind = (%#v, %v)", sealed, err)
	}
	if _, ok := (CompiledQuery{}).CloneForExecution(); ok {
		t.Fatal("zero query cloned for execution")
	}
	if _, ok := (CompiledQuery{}).RetainedBytes(); ok {
		t.Fatal("zero query produced retained bytes")
	}
}

func sealTestKnowledgeProgram(
	t *testing.T,
	compiled CompiledQuery,
	program knowledgeprogram.Program,
) CompiledQuery {
	t.Helper()
	if compiled.knowledgeEvidence == nil {
		t.Fatal("compiler output has no parser-owned evidence")
	}
	prelude, ok := compileKnowledgePreludeEvidence(program)
	if !ok {
		t.Fatal("test knowledge program is invalid")
	}
	evidence := *compiled.knowledgeEvidence
	evidence.prelude = prelude
	if evidence.prelude.charges.RegexPrograms+evidence.authored.regexPrograms > 0 {
		evidence.regexCaptureBytes = MaximumRexCapturedBytesPerRow
	} else {
		evidence.regexCaptureBytes = 0
	}
	compiled.knowledgeEvidence = &evidence
	compiled.executionSeal = nil
	sealed, err := sealCompiledQueryExecution(compiled)
	if err != nil {
		t.Fatalf("sealCompiledQueryExecution(test program): %v", err)
	}
	return sealed
}

func testKnowledgeCompilationEvidenceFixture(
	t *testing.T,
	program knowledgeprogram.Program,
	source string,
) (*plan.Query, preparedKnowledgeCompilation, compiledKnowledgePrelude) {
	t.Helper()
	query, err := plan.InjectKnowledgePrelude(buildPlan(t, source), program)
	if err != nil {
		t.Fatalf("InjectKnowledgePrelude: %v", err)
	}
	preparation, err := prepareKnowledgeCompilation(query)
	if err != nil {
		t.Fatalf("prepareKnowledgeCompilation: %v", err)
	}
	prelude, err := compileKnowledgePrelude(
		knowledgeExtractionStageState(),
		preparation,
	)
	if err != nil {
		t.Fatalf("compileKnowledgePrelude: %v", err)
	}
	return query, preparation, prelude
}

func testKnowledgeEvidenceProgram(t *testing.T, identity string) knowledgeprogram.Program {
	t.Helper()
	definitions := []*opensplunkv1.KnowledgeObjectDefinition{
		{
			AppId: "app-a", Name: "a-regex", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
				FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
					InputField:        "_raw",
					OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
					Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{
						Regex: &opensplunkv1.RegexFieldExtractionDefinition{
							Pattern: `(?P<knowledge_word>[a-z]+)`, OutputFields: []string{"knowledge_word"},
						},
					},
				},
			},
		},
		{
			AppId: "app-a", Name: "b-json", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
				FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
					InputField:        "_raw",
					OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
					Extraction: &opensplunkv1.FieldExtractionDefinition_Json{
						Json: &opensplunkv1.JsonFieldExtractionDefinition{
							Path: "payload.value", OutputField: "knowledge_json",
						},
					},
				},
			},
		},
		{
			AppId: "app-a", Name: "c-calculated", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Body: &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{
				CalculatedField: &opensplunkv1.CalculatedFieldDefinition{
					DestinationField:  "knowledge_calculated",
					Expression:        `if(isnull(_raw), "missing", "present")`,
					OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
				},
			},
		},
	}
	objects := make([]*opensplunkv1.KnowledgeSnapshotObject, len(definitions))
	stageOrdinals := make(map[opensplunkv1.KnowledgeSearchStage]uint32)
	for index, definition := range definitions {
		normalized, err := knowledgedefinition.Normalize(definition)
		if err != nil {
			t.Fatalf("Normalize(test definition %d): %v", index, err)
		}
		var stage opensplunkv1.KnowledgeSearchStage
		switch normalized.ObjectType {
		case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION:
			stage = opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION
		case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD:
			stage = opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD
		default:
			t.Fatalf("unexpected test object type %v", normalized.ObjectType)
		}
		objects[index] = &opensplunkv1.KnowledgeSnapshotObject{
			ResolutionOrdinal: uint32(index),
			Stage:             stage,
			StageOrdinal:      stageOrdinals[stage],
			KnowledgeObjectId: "evidence-" + identity + "-" + normalized.Name,
			Version:           1,
			ObjectType:        normalized.ObjectType,
			Name:              normalized.Name,
			AppId:             normalized.AppID,
			OwnerId:           "owner-a",
			SharingScope:      normalized.SharingScope,
			Definition:        normalized.Definition,
			DefinitionSha256:  bytes.Clone(normalized.Digest[:]),
		}
		stageOrdinals[stage]++
	}
	program, err := knowledgeprogram.Prepare(knowledgeprogram.Input{Objects: objects})
	if err != nil {
		t.Fatalf("Prepare(test evidence program): %v", err)
	}
	return program
}
