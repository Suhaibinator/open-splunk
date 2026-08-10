//go:build open_splunk_knowledge_runtime_acceptance

package knowledgesnapshot

import (
	"errors"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestKnowledgeRuntimeAcceptanceCompilerCannotFinalizeNonemptySnapshot(t *testing.T) {
	authority, err := Prepare(snapshotGoldenInput(t))
	if err != nil {
		t.Fatalf("Prepare(): %v", err)
	}
	logical := buildSnapshotQuery(
		t,
		"tenant-a",
		[]string{"alpha", "zeta"},
		`index=alpha OR index=zeta | table event_id alias_field calculated_field`,
	)
	logical, err = plan.InjectKnowledgePrelude(logical, authority.Prelude())
	if err != nil {
		t.Fatalf("InjectKnowledgePrelude(): %v", err)
	}
	compiled, err := (clickhouse.Compiler{}).Compile(logical)
	if err != nil || !compiled.HasValidExecutionSeal() {
		t.Fatalf("tagged Compile(nonempty) = (%#v, %v)", compiled, err)
	}
	evidence, ok := compiled.KnowledgeSnapshotEvidenceFor(authority.Prelude())
	if !ok || evidence.KnowledgeProgramObjectCount() != authority.Prelude().ObjectCount() ||
		evidence.KnowledgeProgramCharges() != authority.Prelude().Charges() {
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
