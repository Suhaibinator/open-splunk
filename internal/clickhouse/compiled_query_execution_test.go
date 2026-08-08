package clickhouse

import (
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/splpath"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

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
		evidence.GeneratedOperators() != 0 || evidence.GeneratedFields() != 0 ||
		evidence.RegexPrograms() != 1 ||
		evidence.RegexWorkUnits() != uint64(pattern.ProgramWorkUnits) ||
		evidence.RegexCaptureBytes() != MaximumRexCapturedBytesPerRow ||
		evidence.ExtractionOutputs() != 2 ||
		evidence.JSONEvaluationWork() != uint32(splpath.EvaluationWorkUnits(steps)) ||
		evidence.ScalarExpressions() != 0 || evidence.ScalarExpressionNodes() != 0 ||
		evidence.ScalarPredicates() != 2 ||
		evidence.GeneratedSQLBytes() != uint64(len(compiled.SQL)) {
		t.Fatalf("knowledge compiler evidence = %#v", evidence)
	}

	indexes := evidence.EffectiveIndexes()
	indexes[0] = "mutated"
	if evidence.EffectiveIndexes()[0] != "gradethis" {
		t.Fatal("knowledge evidence indexes alias caller memory")
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

	compiled := compileSPL(t, `index=gradethis status=200 | table event_id status`)
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
		{name: "knowledge charge", mutate: func(query *CompiledQuery) { query.knowledgeEvidence.regexPrograms++ }},
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
