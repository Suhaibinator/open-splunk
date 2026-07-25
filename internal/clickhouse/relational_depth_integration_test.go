package clickhouse

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
)

// TestRelationalDepthBoundaryAgainstClickHouse proves the compiler's accepted
// pure-chain boundary is analyzable by the pinned server under the production
// analyzer and resource ceilings.
func TestRelationalDepthBoundaryAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	_, connection := binEdgeNumericStore(t, ctx)

	source := relationalDepthEvalPipeline(t, 62, 32, "")
	compiled, err := (Compiler{}).Compile(relationalDepthPlan(t, source))
	if err != nil {
		t.Fatalf("compile exact relational-depth boundary: %v", err)
	}
	if compiled.relationalDepth != maximumCompiledRelationalDepth {
		t.Fatalf(
			"compiled relational depth = %d, want %d",
			compiled.relationalDepth,
			maximumCompiledRelationalDepth,
		)
	}

	queryContext, queryCancel := context.WithTimeout(
		clickhousedriver.Context(ctx, clickhousedriver.WithSettings(clickhousedriver.Settings{
			"max_subquery_depth": uint64(100),
			"max_execution_time": uint64(30),
			"max_memory_usage":   uint64(256 << 20),
			"max_threads":        uint64(1),
			"max_query_size":     uint64(1 << 20),
		})),
		45*time.Second,
	)
	defer queryCancel()
	rows, err := connection.Query(queryContext, compiled.SQL, compiled.Args...)
	if err != nil {
		t.Fatalf(
			"execute exact relational-depth boundary: %v\nSQL bytes: %d",
			err,
			len(compiled.SQL),
		)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && !t.Failed() {
			t.Errorf("close exact relational-depth rows: %v", closeErr)
		}
	}()
	if rows.Next() {
		t.Fatal("empty boundary fixture unexpectedly returned a row")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate exact relational-depth boundary: %v", err)
	}
}
