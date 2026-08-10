//go:build !open_splunk_knowledge_runtime_acceptance

package clickhouse

// knowledgeRuntimeAcceptanceEnabled is false in every ordinary build. The
// alternate implementation exists only so an explicitly tagged go test binary
// can exercise the otherwise unreachable public compiler/executor path.
func knowledgeRuntimeAcceptanceEnabled() bool { return false }
