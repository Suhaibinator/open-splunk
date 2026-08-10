//go:build !open_splunk_knowledge_runtime_acceptance || !open_splunk_knowledge_snapshot_acceptance

package knowledgesnapshot

import "testing"

func TestKnowledgeSnapshotAcceptanceGateIsClosedWithoutBothTags(t *testing.T) {
	if knowledgeSnapshotAcceptanceEnabled() {
		t.Fatal("nonempty snapshot acceptance gate opened without both build tags")
	}
}
