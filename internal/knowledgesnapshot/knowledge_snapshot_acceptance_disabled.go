//go:build !open_splunk_knowledge_runtime_acceptance || !open_splunk_knowledge_snapshot_acceptance

package knowledgesnapshot

// knowledgeSnapshotAcceptanceEnabled is false unless both explicit acceptance
// tags are present. The alternate implementation still requires a go-test
// process, so no ordinary or singly tagged build can finalize nonempty
// knowledge authority.
func knowledgeSnapshotAcceptanceEnabled() bool { return false }
