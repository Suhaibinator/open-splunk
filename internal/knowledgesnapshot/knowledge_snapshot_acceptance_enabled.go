//go:build open_splunk_knowledge_runtime_acceptance && open_splunk_knowledge_snapshot_acceptance

package knowledgesnapshot

import "testing"

// knowledgeSnapshotAcceptanceEnabled admits nonempty snapshot finalization
// only when both acceptance tags select this file and the binary was produced
// by go test. In particular, both tags on go build still leave the gate closed.
func knowledgeSnapshotAcceptanceEnabled() bool { return testing.Testing() }
