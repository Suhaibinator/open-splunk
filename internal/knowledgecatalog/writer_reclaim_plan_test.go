package knowledgecatalog

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"gorm.io/gorm"
)

type capturedReclaimQuery struct {
	SQL  string
	Args []any
}

func TestWriterIdempotencyReclaimPlansAreExactAndIndexed(t *testing.T) {
	harness := newWriterFaultHarness(t)
	request := writerFaultCreateRequest("reclaim-plan", "reclaim-plan-request-0001")
	if _, err := harness.writer.Create(harness.actorContext, harness.scope, request); err != nil {
		t.Fatalf("Create(): %v", err)
	}
	var cutoff int64
	if err := harness.database.SQLDB().QueryRowContext(t.Context(), `
		SELECT retain_until_unix_micro
		FROM knowledge_mutation_idempotency
		WHERE tenant_id = ? AND route = ? AND client_request_id = ?`,
		writerFaultTenant,
		mutationRouteCreate,
		request.GetClientRequestId(),
	).Scan(&cutoff); err != nil {
		t.Fatalf("read reclaim cutoff: %v", err)
	}

	var captured []capturedReclaimQuery
	const callbackName = "test:capture-exact-reclaim-queries"
	if err := harness.database.GORMDB().Callback().Query().After("gorm:query").Register(
		callbackName,
		func(tx *gorm.DB) {
			statement := tx.Statement.SQL.String()
			if !strings.Contains(statement, "knowledge_mutation_idempotency_retention_idx") {
				return
			}
			captured = append(captured, capturedReclaimQuery{
				SQL:  strings.Clone(statement),
				Args: append([]any(nil), tx.Statement.Vars...),
			})
		},
	); err != nil {
		t.Fatalf("register reclaim query capture: %v", err)
	}
	if err := preflightExpiredMutationReceiptReclaim(
		harness.database.GORMDB(), writerFaultTenant, 1, cutoff,
	); err != nil {
		t.Fatalf("preflightExpiredMutationReceiptReclaim(): %v", err)
	}
	if err := harness.database.GORMDB().Callback().Query().Remove(callbackName); err != nil {
		t.Fatalf("remove reclaim query capture: %v", err)
	}
	if len(captured) != 2 {
		t.Fatalf("captured reclaim query count = %d, want width and semantic queries", len(captured))
	}

	widthPlan := explainExactReclaimSQL(t, harness.database.SQLDB(), captured[0])
	assertExactReclaimOuterPlan(t, "width", widthPlan)
	semanticPlan := explainExactReclaimSQL(t, harness.database.SQLDB(), captured[1])
	assertExactReclaimOuterPlan(t, "semantic", semanticPlan)
	for _, required := range []string{
		"SEARCH COMMITTED USING INDEX SQLITE_AUTOINDEX_KNOWLEDGE_MUTATION_COMMIT_AUTHORITIES_3 (TENANT_ID=? AND CATALOG_REVISION=? AND CATALOG_STATE_TOKEN=?)",
		"SEARCH IMMUTABLE USING PRIMARY KEY (TENANT_ID=? AND KNOWLEDGE_OBJECT_ID=? AND OBJECT_VERSION=?)",
		"SEARCH LIFECYCLE USING PRIMARY KEY (TENANT_ID=? AND KNOWLEDGE_OBJECT_ID=? AND OBJECT_VERSION=?)",
		"SEARCH EVENT USING PRIMARY KEY (TENANT_ID=? AND SEQUENCE=?)",
		"SEARCH RECOVERY USING INDEX SQLITE_AUTOINDEX_KNOWLEDGE_RECOVERY_AUDIT_2 (TENANT_ID=? AND KNOWLEDGE_OBJECT_ID=?)",
	} {
		if !strings.Contains(semanticPlan, required) {
			t.Fatalf("semantic reclaim plan does not contain %q:\n%s", required, semanticPlan)
		}
	}
	for _, forbidden := range []string{
		"SCAN COMMITTED", "SCAN IMMUTABLE", "SCAN LIFECYCLE", "SCAN EVENT", "SCAN RECOVERY",
	} {
		if strings.Contains(semanticPlan, forbidden) {
			t.Fatalf("semantic reclaim plan contains %q:\n%s", forbidden, semanticPlan)
		}
	}

	deletePlan := explainExactReclaimSQL(t, harness.database.SQLDB(), capturedReclaimQuery{
		SQL:  mutationReceiptReclaimDeleteSQL,
		Args: []any{writerFaultTenant, cutoff, 1},
	})
	assertExactReclaimOuterPlan(t, "delete", deletePlan)
}

func explainExactReclaimSQL(
	t *testing.T,
	database *sql.DB,
	query capturedReclaimQuery,
) string {
	t.Helper()
	rows, err := database.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+query.SQL, query.Args...)
	if err != nil {
		t.Fatalf("explain exact reclaim query: %v\n%s", err, query.SQL)
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		if err := rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
			t.Fatalf("scan exact reclaim plan: %v", err)
		}
		lines = append(lines, strings.ToUpper(fmt.Sprintf("%d/%d %s", id, parent, detail)))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read exact reclaim plan: %v", err)
	}
	return strings.Join(lines, "\n")
}

func assertExactReclaimOuterPlan(t *testing.T, label, plan string) {
	t.Helper()
	if !strings.Contains(plan, "KNOWLEDGE_MUTATION_IDEMPOTENCY_RETENTION_IDX") {
		t.Fatalf("%s reclaim plan does not use the retention index:\n%s", label, plan)
	}
	if strings.Contains(plan, "USE TEMP B-TREE") {
		t.Fatalf("%s reclaim plan uses a temporary sorter:\n%s", label, plan)
	}
}
