//go:build open_splunk_knowledge_runtime_acceptance

package queryexec

import (
	"slices"
	"testing"
	"time"
)

func TestKnowledgeRuntimeAcceptanceCompilerMatrixSealsWithoutDocker(t *testing.T) {
	const (
		indexName         = "knowledge-runtime"
		selectorIndexName = "selector-runtime"
		tenantID          = "knowledge-tenant"
	)
	base := time.Date(2026, time.August, 8, 12, 0, 0, 0, time.UTC)
	indexTime := base.Add(10 * time.Minute)
	program := knowledgeRuntimeProgram(t)
	matrix := compileKnowledgeRuntimeMatrix(
		t,
		program,
		tenantID,
		indexName,
		selectorIndexName,
		base,
		indexTime,
		base,
		base.Add(2*time.Minute),
	)
	if len(matrix.compilerCases)+4 != 13 {
		t.Fatalf("tagged compiler matrix cases = %d, want 13", len(matrix.compilerCases)+4)
	}
	for _, test := range matrix.compilerCases {
		t.Run(test.name, func(t *testing.T) {
			if !test.compiled.HasValidExecutionSeal() {
				t.Fatal("tagged public compiler case has no valid execution seal")
			}
		})
	}

	if !matrix.ordinary.HasValidExecutionSeal() ||
		!matrix.controls.HasValidExecutionSeal() ||
		!matrix.chart.HasValidExecutionSeal() ||
		!matrix.timechart.HasValidExecutionSeal() ||
		!matrix.stats.HasValidExecutionSeal() ||
		!matrix.timeline.HasValidExecutionSeal() ||
		!matrix.catalog.HasValidExecutionSeal() ||
		!matrix.summary.HasValidExecutionSeal() ||
		!matrix.suggestions.HasValidExecutionSeal() ||
		!matrix.overflow.HasValidExecutionSeal() {
		t.Fatal("tagged compiler matrix contains an unsealed execution")
	}

	commitment, commitmentOK := program.Commitment()
	evidence, evidenceOK := matrix.ordinary.KnowledgeSnapshotEvidenceFor(program)
	evidenceCommitment, evidenceCommitmentOK := evidence.KnowledgeProgramCommitment()
	if !commitmentOK || !evidenceOK || !evidenceCommitmentOK ||
		evidenceCommitment != commitment ||
		evidence.KnowledgeProgramObjectCount() != program.ObjectCount() ||
		evidence.KnowledgeProgramCharges() != program.Charges() ||
		evidence.GeneratedSQLBytes() != uint64(len(matrix.ordinary.SQL)) ||
		evidence.TenantID() != tenantID ||
		!slices.Equal(evidence.EffectiveIndexes(), []string{indexName}) {
		t.Fatalf(
			"tagged ordinary knowledge evidence = (%#v, %t), commitment=%x/%x",
			evidence,
			evidenceOK,
			evidenceCommitment,
			commitment,
		)
	}

	cloned, cloneOK := matrix.ordinary.CloneForExecution()
	if !cloneOK || !matrix.ordinary.EqualForExecution(cloned) ||
		!cloned.HasValidExecutionSeal() || len(cloned.OutputFields) == 0 {
		t.Fatalf("tagged ordinary execution clone = (%#v, %t)", cloned, cloneOK)
	}
	wantFirstOutput := matrix.ordinary.OutputFields[0]
	cloned.OutputFields[0] = "mutated-tagged-acceptance-output"
	if matrix.ordinary.OutputFields[0] != wantFirstOutput ||
		matrix.ordinary.EqualForExecution(cloned) || cloned.HasValidExecutionSeal() {
		t.Fatal("tagged ordinary execution clone aliases or survives mutation")
	}

	containerClone, cloneOK := matrix.ordinary.CloneForExecution()
	if !cloneOK || len(containerClone.ContainerOutputs) == 0 {
		t.Fatalf("tagged ordinary container clone = (%#v, %t)", containerClone, cloneOK)
	}
	wantFirstContainer := matrix.ordinary.ContainerOutputs[0]
	containerClone.ContainerOutputs[0].OutputIndex = 0
	if matrix.ordinary.ContainerOutputs[0] != wantFirstContainer ||
		matrix.ordinary.EqualForExecution(containerClone) ||
		containerClone.HasValidExecutionSeal() {
		t.Fatal("tagged ordinary container clone aliases or survives mutation")
	}
}
