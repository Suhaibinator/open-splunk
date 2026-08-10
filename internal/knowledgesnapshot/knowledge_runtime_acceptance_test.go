//go:build open_splunk_knowledge_runtime_acceptance && !open_splunk_knowledge_snapshot_acceptance

package knowledgesnapshot

import (
	"errors"
	"strings"
	"testing"
)

func TestKnowledgeRuntimeAcceptanceCompilerCannotFinalizeNonemptySnapshot(t *testing.T) {
	authority, err := Prepare(snapshotGoldenInput(t))
	if err != nil {
		t.Fatalf("Prepare(): %v", err)
	}
	prelude := authority.Prelude()
	compiled, _ := compileSnapshotQueryWithPrelude(
		t,
		"tenant-a",
		[]string{"alpha", "zeta"},
		`index=alpha OR index=zeta | table event_id alias_field calculated_field`,
		prelude,
	)
	if !compiled.HasValidExecutionSeal() {
		t.Fatalf("tagged Compile(nonempty) = %#v", compiled)
	}
	evidence, ok := compiled.KnowledgeSnapshotEvidenceFor(prelude)
	if !ok || evidence.KnowledgeProgramObjectCount() != prelude.ObjectCount() ||
		evidence.KnowledgeProgramCharges() != prelude.Charges() {
		t.Fatalf("tagged compiler evidence = (%#v, %t)", evidence, ok)
	}

	snapshot, finalizeErr := authority.Finalize(compiled)
	if !snapshot.IsZero() || !errors.Is(finalizeErr, ErrInvalidInput) ||
		!strings.Contains(finalizeErr.Error(), "nonempty authority requires the KO-1 knowledge prelude") {
		t.Fatalf(
			"tagged Finalize(nonempty) = (%#v, %v), want zero/closed snapshot gate",
			snapshot,
			finalizeErr,
		)
	}
}
