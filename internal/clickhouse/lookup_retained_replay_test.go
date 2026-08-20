package clickhouse

import (
	"context"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestRetainedLookupAuthorityRestoresExplicitWithoutCatalogResolution(t *testing.T) {
	t.Parallel()

	original := buildPlan(
		t,
		`index=gradethis | lookup service_catalog service_id AS service OUTPUT owner`,
	)
	resolution := bindTestLookupResolution(
		t,
		testLookupResolution(t, "tenant-1", [][]string{{"api", "platform"}}),
		original,
	)
	compiled, err := (Compiler{}).CompileWithLookupResolutions(
		original,
		[]LookupResolution{resolution},
	)
	if err != nil {
		t.Fatalf("CompileWithLookupResolutions(): %v", err)
	}

	rebuilt := buildPlan(
		t,
		`index=gradethis | lookup service_catalog service_id AS service OUTPUT owner`,
	)
	restored, compiler, err := (Compiler{}).WithRetainedLookupAuthorityContext(
		context.Background(),
		compiled,
		rebuilt,
	)
	if err != nil {
		t.Fatalf("WithRetainedLookupAuthorityContext(): %v", err)
	}
	derived, err := compiler.Compile(restored)
	if err != nil {
		t.Fatalf("Compile(restored): %v", err)
	}
	if !derived.HasValidExecutionSeal() || len(derived.lookupTables) != 1 {
		t.Fatalf("restored execution = seal %v, tables %#v", derived.HasValidExecutionSeal(), derived.lookupTables)
	}
	if &derived.lookupTables[0].backing.values[0][0] !=
		&compiled.lookupTables[0].backing.values[0][0] {
		t.Fatal("retained replay cloned the sealed selected-cell backing")
	}
}

func TestRetainedLookupAuthorityRestoresAutomaticPlacementAndSealsReplay(t *testing.T) {
	t.Parallel()

	authored := buildPlan(t, `index=gradethis status=200`)
	empty, err := knowledgeprogram.Prepare(knowledgeprogram.Input{})
	if err != nil {
		t.Fatalf("Prepare(empty knowledge): %v", err)
	}
	original, err := plan.InjectKnowledgePrelude(authored, empty)
	if err != nil {
		t.Fatalf("InjectKnowledgePrelude(original): %v", err)
	}
	selector, err := knowledge.CompileSelector(knowledge.SelectorSpec{})
	if err != nil {
		t.Fatalf("CompileSelector(): %v", err)
	}
	binding := automaticLookupTestBinding(
		t,
		"auto-service",
		"auto_service",
		"service",
		"owner",
		selector,
		[]string{"service_id", "owner"},
		[][]string{{"api", "platform"}},
	)
	injected, configured, err := (Compiler{}).WithAutomaticLookupBindings(
		original,
		[]AutomaticLookupBinding{binding},
		nil,
	)
	if err != nil {
		t.Fatalf("WithAutomaticLookupBindings(): %v", err)
	}
	compiled, err := configured.Compile(injected)
	if err != nil {
		t.Fatalf("Compile(automatic): %v", err)
	}
	if len(compiled.automaticLookupReplay) != 1 {
		t.Fatalf("sealed automatic replay = %#v", compiled.automaticLookupReplay)
	}

	rebuiltAuthored := buildPlan(t, `index=gradethis status=200`)
	rebuilt, err := plan.InjectKnowledgePrelude(rebuiltAuthored, empty)
	if err != nil {
		t.Fatalf("InjectKnowledgePrelude(rebuilt): %v", err)
	}
	restored, replayCompiler, err := (Compiler{}).WithRetainedLookupAuthorityContext(
		context.Background(),
		compiled,
		rebuilt,
	)
	if err != nil {
		t.Fatalf("WithRetainedLookupAuthorityContext(): %v", err)
	}
	if len(restored.Operators) < 2 || restored.Operators[1].LogicalName() != "AutomaticLookupGroup" {
		t.Fatalf("restored operators = %#v", restored.Operators)
	}
	replayed, err := replayCompiler.Compile(restored)
	if err != nil || !replayed.HasValidExecutionSeal() {
		t.Fatalf("Compile(restored automatic) = seal %v, err %v", replayed.HasValidExecutionSeal(), err)
	}

	tampered := compiled
	tampered.automaticLookupReplay = cloneRetainedAutomaticLookups(
		compiled.automaticLookupReplay,
	)
	tampered.automaticLookupReplay[0].stableID = "changed"
	if tampered.HasValidExecutionSeal() {
		t.Fatal("automatic replay mutation preserved the execution seal")
	}
}
