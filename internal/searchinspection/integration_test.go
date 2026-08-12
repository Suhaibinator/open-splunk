package searchinspection

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

// TestServiceAgainstClickHouse is opt-in because it starts an ephemeral
// container using the repository-pinned ClickHouse image.
func TestServiceAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip(
			"set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test",
		)
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	container, err := testsupport.StartClickHouse(
		ctx,
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(
			context.Background(),
			15*time.Second,
		)
		defer cleanupCancel()
		if err := container.Close(cleanupContext); err != nil {
			t.Errorf("close ClickHouse fixture: %v", err)
		}
	})
	migrateInspectionClickHouse(t, ctx, container)
	insertInspectionEvent(t, ctx, container)

	options := &clickhousedriver.Options{
		Addr: []string{container.Address},
		Auth: clickhousedriver.Auth{
			Database: container.Database,
			Username: container.Username,
			Password: container.Password,
		},
		DialTimeout: 5 * time.Second,
	}
	explainer, err := queryexec.NewExplainer(options, queryexec.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := explainer.Close(); err != nil {
			t.Errorf("close ClickHouse Explainer: %v", err)
		}
	})

	snapshot := validInspectionSnapshot()
	searches := &inspectionSearches{
		snapshots: []searchjobs.ExecutionSnapshot{snapshot},
	}
	service := newInspectionTestService(t, inspectionTestConfig{
		Searches:  searches,
		Compiler:  &inspectionCompiler{},
		Explainer: explainer,
	})
	result, err := service.Inspect(
		ctx,
		searchjobs.AccessScope{
			TenantID: snapshot.TenantID,
			OwnerID:  snapshot.OwnerID,
		},
		Request{SearchJobID: snapshot.ID},
	)
	if err != nil {
		t.Fatal(err)
	}
	if searches.callCount() != 2 {
		t.Fatalf(
			"snapshot calls = %d, want preflight and postflight",
			searches.callCount(),
		)
	}
	if !strings.Contains(result.GeneratedSQL, `"open_splunk"."events"`) {
		t.Fatalf(
			"generated SQL does not reference the migrated event table:\n%s",
			result.GeneratedSQL,
		)
	}
	physical, err := queryexec.ParseExplainPlan(queryexec.ExplainResult{
		Text:    result.ExplainText,
		QueryID: result.DiagnosticQueryID,
	})
	if err != nil {
		t.Fatalf("parse ClickHouse structured plan: %v", err)
	}
	if !reflect.DeepEqual(result.PhysicalPlan, physical) {
		t.Fatalf(
			"service physical plan = %#v, parsed raw plan %#v",
			result.PhysicalPlan,
			physical,
		)
	}
	if !slices.Contains(physical.NodeTypes, "ReadFromMergeTree") ||
		len(physical.Reads) != 1 ||
		len(physical.Reads[0].Columns) == 0 ||
		len(physical.Reads[0].Indexes) == 0 {
		t.Fatalf("ClickHouse structured plan evidence = %#v", physical)
	}
	indexTypes := make([]string, len(physical.Reads[0].Indexes))
	for index, evidence := range physical.Reads[0].Indexes {
		indexTypes[index] = evidence.Type
	}
	if !slices.Contains(indexTypes, "MinMax") ||
		!slices.Contains(indexTypes, "PrimaryKey") {
		t.Fatalf("ClickHouse structured plan index types = %v", indexTypes)
	}
	if !strings.HasPrefix(
		result.DiagnosticQueryID,
		"open-splunk-explain-",
	) {
		t.Fatalf(
			"diagnostic query ID = %q",
			result.DiagnosticQueryID,
		)
	}
	// Generated SQL remains parameterized. EXPLAIN text is intentionally not
	// treated as safe because ClickHouse may render bound values into its plan;
	// that is why this service remains behind an internal administrator-only
	// boundary. The derived physical projection must still be literal-free.
	rendered := result.GeneratedSQL
	for _, secret := range []string{
		snapshot.TenantID,
		snapshot.OwnerID,
		snapshot.EffectiveIndexes[0],
		"sensitive-literal-7f2c",
	} {
		if strings.Contains(rendered, secret) {
			t.Fatalf("inspection diagnostics leaked %q", secret)
		}
	}
	physicalRendered := fmt.Sprintf("%#v", result.PhysicalPlan)
	for _, secret := range []string{
		snapshot.TenantID,
		snapshot.OwnerID,
		snapshot.EffectiveIndexes[0],
		"sensitive-literal-7f2c",
	} {
		if strings.Contains(physicalRendered, secret) {
			t.Fatalf(
				"safe physical projection leaked query metadata",
			)
		}
	}
}

func insertInspectionEvent(
	t *testing.T,
	ctx context.Context,
	container *testsupport.ClickHouseContainer,
) {
	t.Helper()
	const query = `
INSERT INTO open_splunk.events
(
    event_id, tenant_id, index_name, event_time, index_time, collected_at,
    event_time_source, host, source, sourcetype, service, severity, level,
    body, raw, raw_encoding, trace_id, span_id, fields, field_names,
    collector_id, batch_id, batch_sequence, expires_at, visibility_seq,
    field_types, field_metadata_version
)
VALUES
(
    'inspection-event', 'sensitive-tenant-7f2c', 'sensitive-index-7f2c',
    toDateTime64('2026-07-27 10:30:00', 9, 'UTC'),
    toDateTime64('2026-07-27 11:30:00', 3, 'UTC'),
    NULL, 0, 'host-a', '', 'inspection', NULL, 0, NULL,
    'event body', 'event body', 0, NULL, NULL,
    '{"status":"sensitive-literal-7f2c"}', ['status'],
    'collector', 'batch', 1,
    toDateTime64('2100-01-01 00:00:00', 3, 'UTC'),
    42, [1], 1
)`
	runInspectionClickHouseClient(t, ctx, container, query)
}

func migrateInspectionClickHouse(
	t *testing.T,
	ctx context.Context,
	container *testsupport.ClickHouseContainer,
) {
	t.Helper()
	paths, err := filepath.Glob(
		filepath.Join("..", "..", "migrations", "clickhouse", "[0-9][0-9][0-9][0-9]_*.sql"),
	)
	if err != nil || len(paths) == 0 {
		t.Fatalf("discover ClickHouse migrations: paths=%v err=%v", paths, err)
	}
	var migrations bytes.Buffer
	for _, path := range paths {
		migration, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatal(readErr)
		}
		migrations.Write(migration)
		migrations.WriteByte('\n')
	}
	runInspectionClickHouseClient(
		t,
		ctx,
		container,
		migrations.String(),
		"--multiquery",
	)
}

func runInspectionClickHouseClient(
	t *testing.T,
	ctx context.Context,
	container *testsupport.ClickHouseContainer,
	input string,
	extraArguments ...string,
) {
	t.Helper()
	arguments := []string{
		"exec",
		"--interactive",
		container.Name,
		"clickhouse-client",
		"--user",
		container.Username,
		"--password",
		container.Password,
	}
	arguments = append(arguments, extraArguments...)
	command := exec.CommandContext(
		ctx,
		"docker",
		arguments...,
	)
	command.Stdin = strings.NewReader(input)
	output, err := command.CombinedOutput()
	if err != nil {
		const maximumOutput = 4 << 10
		if len(output) > maximumOutput {
			output = output[len(output)-maximumOutput:]
		}
		t.Fatalf(
			"execute ClickHouse fixture SQL: %v: %s",
			err,
			strings.TrimSpace(string(output)),
		)
	}
}
