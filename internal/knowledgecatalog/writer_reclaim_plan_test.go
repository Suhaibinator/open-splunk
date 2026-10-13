package knowledgecatalog

import (
	"database/sql"
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
		WHERE tenant_id = $1 AND route = $2 AND client_request_id = $3`,
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
			if !strings.Contains(statement, "knowledge_mutation_idempotency") || !strings.Contains(statement, "retain_until_unix_micro") || !strings.Contains(statement, "ORDER BY") {
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
	for _, probe := range []struct {
		alias string
		keys  []string
	}{
		{"COMMITTED", []string{"TENANT_ID", "CATALOG_REVISION", "CATALOG_STATE_TOKEN"}},
		{"IMMUTABLE", []string{"TENANT_ID", "KNOWLEDGE_OBJECT_ID", "OBJECT_VERSION"}},
		{"LIFECYCLE", []string{"TENANT_ID", "KNOWLEDGE_OBJECT_ID", "OBJECT_VERSION"}},
		{"EVENT", []string{"TENANT_ID", "SEQUENCE"}},
		{"RECOVERY", []string{"TENANT_ID", "KNOWLEDGE_OBJECT_ID"}},
	} {
		found := false
		for line := range strings.SplitSeq(semanticPlan, "\n") {
			if !strings.Contains(line, "ON "+probe.alias+" TABLE ") {
				continue
			}
			if strings.HasPrefix(line, "SEQ SCAN") || !strings.Contains(line, " USING ") {
				t.Fatalf("semantic reclaim %s is not indexed:\n%s", probe.alias, semanticPlan)
			}
			for _, key := range probe.keys {
				if !strings.Contains(line, key) {
					t.Fatalf("semantic reclaim %s lacks exact key%s:\n%s", probe.alias, key, semanticPlan)
				}
			}
			found = true
		}
		if !found {
			t.Fatalf("semantic reclaim lacks exact %s lookup:\n%s", probe.alias, semanticPlan)
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
	return strings.ToUpper(strings.Join(explainPostgreSQLQueryPlan(t, database, query.SQL, query.Args), "\n"))
}

func assertExactReclaimOuterPlan(t *testing.T, label, plan string) {
	t.Helper()
	if !strings.Contains(plan, "KNOWLEDGE_MUTATION_IDEMPOTENCY_RETENTION_IDX") {
		t.Fatalf("%s reclaim plan does not use the retention index:\n%s", label, plan)
	}
	if strings.Contains(plan, "SORT") {
		t.Fatalf("%s reclaim plan uses a temporary sorter:\n%s", label, plan)
	}
}
