//go:build open_splunk_knowledge_runtime_acceptance

package clickhouse

import "testing"

// knowledgeRuntimeAcceptanceEnabled admits nonempty compiler sealing only in a
// binary produced by go test. In particular, adding the build tag to a server
// build is insufficient: testing.Testing reports false for go build binaries.
// Snapshot finalization remains independently closed in every build.
func knowledgeRuntimeAcceptanceEnabled() bool { return testing.Testing() }
