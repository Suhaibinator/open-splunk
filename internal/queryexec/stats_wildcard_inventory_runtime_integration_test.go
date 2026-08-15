package queryexec

import (
	"context"
	"os"
	"os/exec"
	"slices"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

// TestStatsWildcardInventoryAgainstClickHouse executes one open-schema stats
// wc-field discovery query end to end against the digest-pinned server, using
// the knowledge-bearing prefix shape searchjobs builds in production: the
// planner's prefix carries an injected knowledge prelude, so the compiled
// inventory relation embeds the whole program.
//
// The assertion this test exists for is the sealed per-query resource budget.
// maximumStatsWildcardInventoryMemoryBytes is overwhelmingly a planning budget
// for such a prefix, and the floor is a property of the server release. A
// budget below that floor makes every knowledge-bearing wc-field search fail
// at runtime while every unit test still passes, so the discovery query is
// exercised here under exactly the production ceilings rather than mocked.
//
// Run only this test with:
//
//	OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 go test -v ./internal/queryexec \
//	  -run '^TestStatsWildcardInventoryAgainstClickHouse$' -count=1 -timeout=2m
func TestStatsWildcardInventoryAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	image, err := testsupport.ResolvePinnedClickHouseImage(
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if err != nil {
		t.Fatalf("resolve pinned ClickHouse image: %v", err)
	}

	const (
		indexName         = "knowledge-runtime"
		selectorIndexName = "selector-runtime"
		tenantID          = "knowledge-tenant"
		source            = `index=knowledge-runtime service=matrix | stats max(float_*)`
	)
	base := time.Date(2026, time.August, 8, 12, 0, 0, 0, time.UTC)
	indexTime := base.Add(10 * time.Minute)
	visibility := uint64(1)
	scope := plan.Scope{
		TenantID:          tenantID,
		AuthorizedIndexes: []string{indexName},
		RequestedIndexes:  []string{indexName},
		Earliest:          base,
		Latest:            base.Add(2 * time.Minute),
		SearchStart:       indexTime,
		SearchTimezone:    "UTC",
		IndexTimeCutoff:   indexTime.Add(time.Millisecond),
		VisibilityCutoff:  &visibility,
	}

	// Compile before starting the container so an invalid prefix fails without
	// consuming Docker startup time.
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatalf("parse open-schema stats wildcard query: %v", err)
	}
	preparation, err := plan.PrepareStatsWildcard(parsed, scope)
	if err != nil {
		t.Fatalf("prepare stats wildcard: %v", err)
	}
	if preparation.FullPlan() != nil {
		t.Fatal("stats wildcard query planned without runtime discovery")
	}
	prefix := preparation.Prefix()
	request := preparation.Request()
	if prefix == nil || request.IsZero() {
		t.Fatal("stats wildcard preparation is incomplete")
	}
	inventoryPrefix, err := plan.InjectKnowledgePrelude(prefix, knowledgeRuntimeProgram(t))
	if err != nil {
		t.Fatalf("inject knowledge prelude into inventory prefix: %v", err)
	}
	inventory, err := (clickhouse.Compiler{}).CompileStatsWildcardInventory(
		inventoryPrefix,
		request,
	)
	if err != nil {
		t.Fatalf("compile production stats wildcard inventory: %v", err)
	}
	if !inventory.HasValidExecutionSeal() {
		t.Fatal("compiled stats wildcard inventory has no valid execution seal")
	}
	t.Logf(
		"stats wildcard inventory compiled: sql=%d bytes args=%d",
		len(inventory.SQL),
		len(inventory.Args),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	container, err := testsupport.StartClickHouse(ctx, image)
	if err != nil {
		t.Fatalf("start pinned ClickHouse fixture: %v", err)
	}
	t.Cleanup(func() {
		cleanupContext, cancelCleanup := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelCleanup()
		if closeErr := container.Close(cleanupContext); closeErr != nil {
			t.Errorf("close stats wildcard ClickHouse fixture: %v", closeErr)
		}
	})
	queryIntegrationMigrate(t, ctx, container.Name, container.Password)

	connection, err := clickhousedriver.Open(&clickhousedriver.Options{
		Protocol: clickhousedriver.Native,
		Addr:     []string{container.Address},
		Auth: clickhousedriver.Auth{
			Database: container.Database,
			Username: container.Username,
			Password: container.Password,
		},
		DialTimeout: 5 * time.Second,
		ReadTimeout: 20 * time.Second,
	})
	if err != nil {
		t.Fatalf("open stats wildcard ClickHouse connection: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := connection.Close(); closeErr != nil {
			t.Errorf("close stats wildcard ClickHouse connection: %v", closeErr)
		}
	})
	insertKnowledgeRuntimeEvents(
		t,
		ctx,
		connection,
		knowledgeRuntimeFixtures(tenantID, indexName, selectorIndexName, base, indexTime),
	)

	// The executor ceilings are deliberately far above the sealed inventory
	// budget so boundedExecutorSettings clamps to the per-query constant. This
	// query must succeed under that constant, not under the ordinary ceiling.
	executor, err := New(connection, Config{
		ReadAdmission:    indexread.UnfencedAdmission{},
		MaxExecutionTime: 15 * time.Second,
		MaxMemoryBytes:   uint64(512 << 20),
		MaxRowsToRead:    5_000_000,
		MaxBytesToRead:   1 << 30,
		MaxResultRows:    1_000,
		MaxResultBytes:   8 << 20,
		MaxRowsToGroupBy: 1_000,
		MaxThreads:       2,
	})
	if err != nil {
		t.Fatalf("create stats wildcard query executor: %v", err)
	}
	settings, err := settingsForStatsWildcardInventory(executor.settings, request.MaximumPairs())
	if err != nil {
		t.Fatalf("build stats wildcard inventory settings: %v", err)
	}
	if settings["max_memory_usage"] != maximumStatsWildcardInventoryMemoryBytes {
		t.Fatalf(
			"inventory max_memory_usage = %v, want the sealed per-query budget %d",
			settings["max_memory_usage"],
			maximumStatsWildcardInventoryMemoryBytes,
		)
	}

	expansion, err := executor.ExecuteStatsWildcardInventory(ctx, inventory)
	if err != nil {
		t.Fatalf("execute stats wildcard inventory: %v", err)
	}
	if expansion.IsZero() {
		t.Fatal("stats wildcard expansion is empty")
	}

	// OutputFields is the only public evidence of the discovered names, and it
	// proves the inventory found both matching event fields and neither the
	// knowledge-generated nor the reserved ones.
	logical, err := plan.BuildWithStatsWildcardExpansion(parsed, scope, expansion)
	if err != nil {
		t.Fatalf("rebuild plan with stats wildcard expansion: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"max(float_copy)", "max(float_source)"}) {
		t.Fatalf("expanded output fields = %v", logical.OutputFields)
	}
}
