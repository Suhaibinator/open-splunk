package knowledgecatalog_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/pgtest"
)

// The table mapping records actual PostgreSQL trigger targets from migrations.
func writerTriggerTable(name string) string {
	table, ok := writerTriggerTables[name]
	if !ok {
		panic("unknown writer fixture trigger: " + name)
	}
	return table
}

var writerTriggerTables = map[string]string{
	"app_catalog_revision_delete_is_forbidden":                       "app_catalog_revisions",
	"audit_event_update_is_forbidden":                                "audit_events",
	"knowledge_active_app_workspace_cannot_be_archived":              "app_workspaces",
	"knowledge_active_dependency_target_transition_is_blocked":       "knowledge_objects",
	"knowledge_definition_blob_delete_is_forbidden":                  "knowledge_definition_blobs",
	"knowledge_definition_blob_update_is_forbidden":                  "knowledge_definition_blobs",
	"knowledge_list_projection_current_seal_delete_is_forbidden":     "knowledge_object_list_projection_seals",
	"knowledge_mutation_commit_authority_collision_is_forbidden":     "knowledge_mutation_commit_authorities",
	"knowledge_mutation_commit_authority_is_exact":                   "knowledge_mutation_commit_authorities",
	"knowledge_mutation_commit_authority_update_is_forbidden":        "knowledge_mutation_commit_authorities",
	"knowledge_mutation_idempotency_after_insert":                    "knowledge_mutation_idempotency",
	"knowledge_mutation_idempotency_capacity_is_available":           "knowledge_mutation_idempotency",
	"knowledge_mutation_idempotency_identity_collision_is_forbidden": "knowledge_mutation_idempotency",
	"knowledge_mutation_idempotency_matches_audit_authority":         "knowledge_mutation_idempotency",
	"knowledge_mutation_idempotency_matches_commit_authority":        "knowledge_mutation_idempotency",
	"knowledge_mutation_idempotency_update_is_forbidden":             "knowledge_mutation_idempotency",
	"knowledge_object_registry_transition_is_valid":                  "knowledge_objects",
	"knowledge_object_update_requires_sealed_list_projection":        "knowledge_objects",
	"knowledge_object_version_lifecycle_update_is_forbidden":         "knowledge_object_version_lifecycle",
	"knowledge_object_version_update_is_forbidden":                   "knowledge_object_versions",
	"knowledge_object_version_writer_semantics_are_exact":            "knowledge_object_versions",
}

type writerNativePlanNode struct {
	NodeType     string `json:"Node Type"`
	RelationName string `json:"Relation Name"`
	Alias        string `json:"Alias"`
	IndexName    string `json:"Index Name"`
	IndexCond    string `json:"Index Cond"`
	Filter       string `json:"Filter"`
	Plans        []writerNativePlanNode
}

func writerNativeQueryPlan(t *testing.T, database *sql.DB, query string, args ...any) string {
	t.Helper()
	tx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(t.Context(), "SET LOCAL enable_seqscan=off"); err != nil {
		t.Fatal(err)
	}
	var encoded []byte
	if err := tx.QueryRowContext(t.Context(), pgtest.Parameters("EXPLAIN (FORMAT JSON) "+query), args...).Scan(&encoded); err != nil {
		t.Fatalf("explain native writer query: %v\n%s", err, query)
	}
	var roots []struct{ Plan writerNativePlanNode }
	if err := json.Unmarshal(encoded, &roots); err != nil {
		t.Fatal(err)
	}
	var details []string
	var walk func(writerNativePlanNode)
	walk = func(node writerNativePlanNode) {
		for _, child := range node.Plans {
			walk(child)
		}
		details = append(details, fmt.Sprintf("%s on %s table %s using %s condition (%s) filter (%s)", node.NodeType, node.Alias, node.RelationName, node.IndexName, node.IndexCond, node.Filter))
	}
	for _, root := range roots {
		walk(root.Plan)
	}
	return strings.ToUpper(strings.Join(details, "\n"))
}
