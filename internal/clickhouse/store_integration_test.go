package clickhouse

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

const storeIntegrationImage = "clickhouse/clickhouse-server:26.3.17.4"

// TestStoreAgainstClickHouse is opt-in because it starts an ephemeral Docker
// container and may pull the pinned ClickHouse image.
func TestStoreAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	container := "open-splunk-store-" + integrationRandomHex(t, 6)
	password := integrationRandomHex(t, 24)
	image := os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE")
	if image == "" {
		image = storeIntegrationImage
	}
	integrationDocker(t, ctx, nil,
		"run", "--detach", "--rm", "--name", container,
		"--publish", "127.0.0.1::9000",
		"--env", "CLICKHOUSE_DB=open_splunk",
		"--env", "CLICKHOUSE_USER=open_splunk",
		"--env", "CLICKHOUSE_PASSWORD="+password,
		"--env", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
		image,
	)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "--force", container).Run()
	})
	integrationWaitForClickHouse(t, ctx, container, password)

	migrationPaths, err := filepath.Glob(filepath.Join("..", "..", "migrations", "clickhouse", "[0-9][0-9][0-9][0-9]_*.sql"))
	if err != nil || len(migrationPaths) == 0 {
		t.Fatalf("discover migrations: paths=%v err=%v", migrationPaths, err)
	}
	var migrations bytes.Buffer
	for _, migrationPath := range migrationPaths {
		migration, readErr := os.ReadFile(migrationPath)
		if readErr != nil {
			t.Fatalf("read migration %s: %v", migrationPath, readErr)
		}
		migrations.Write(migration)
		migrations.WriteByte('\n')
	}
	integrationDocker(t, ctx, bytes.NewReader(migrations.Bytes()),
		"exec", "--interactive", container, "clickhouse-client",
		"--user", "open_splunk", "--password", password, "--multiquery",
	)
	address := integrationNativeAddress(t, ctx, container)
	config := DefaultConfig()
	config.Addresses = []string{address}
	config.Username = "open_splunk"
	config.Password = password
	controlDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("open visibility control database: %v", err)
	}
	t.Cleanup(func() { _ = controlDB.Close() })
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		t.Fatalf("create visibility sequencer: %v", err)
	}
	store, err := Open(config, fixedRetention(30*24*time.Hour), sequencer)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	indexTime := time.Date(2026, 7, 21, 3, 4, 6, 987654321, time.UTC)
	event := testStoredEvent("native-event", "main", indexTime)
	event.BatchID = "native-batch"
	event.Event.Raw = []byte{0xff, 0, 'b', 'y', 't', 'e', 's'}
	event.Event.RawEncoding = 2
	event.Event.Service = stringPointer("")
	event.Event.Level = nil
	event.Event.Fields = typedObjectValue(
		typedField("signed", typedSint(-1<<63)),
		typedField("unsigned", typedUint(^uint64(0))),
		typedField("nothing", typedNull()),
		typedField("literal.dot", typedString("literal")),
		typedField("nested", typedObject(typedField("value", typedString("nested")))),
		typedField("mixed", typedList(typedSint(1), typedString("two"), typedBool(true))),
		typedField("bytes", typedBytes([]byte{0, 0xff, 0x10})),
	)
	batch := ingest.StoreBatch{
		TenantID: "tenant", CollectorID: "collector", BatchID: "native-batch", BatchSequence: 1,
		SourceBatchSHA256: testSourceBatchDigest("native-batch"),
		ReceivedAt:        indexTime,
		Events:            []*ingest.StoredEvent{event},
	}
	for attempt := 1; attempt <= 2; attempt++ {
		result, storeErr := store.Store(ctx, batch)
		if storeErr != nil {
			t.Fatalf("Store attempt %d: %v", attempt, storeErr)
		}
		if attempt == 1 && (result.Accepted != 1 || result.Duplicate != 0) {
			t.Fatalf("initial Store result = %+v", result)
		}
		if attempt == 2 && (result.Accepted != 0 || result.Duplicate != 1) {
			t.Fatalf("retry Store result = %+v", result)
		}
	}

	options, _, err := config.clickHouseOptions()
	if err != nil {
		t.Fatal(err)
	}
	queryConnection, err := clickhousedriver.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = queryConnection.Close() })
	var count uint64
	if err := queryConnection.QueryRow(ctx, "SELECT count() FROM open_splunk.events WHERE event_id = ?", "native-event").Scan(&count); err != nil {
		t.Fatalf("query dedup count: %v", err)
	}
	if count != 1 {
		t.Fatalf("retry stored %d rows, want 1", count)
	}
	cutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture visibility cutoff: %v", err)
	}
	if cutoff != 1 {
		t.Fatalf("visibility cutoff after deduplicated retry = %d, want 1", cutoff)
	}
	late := testStoredEvent("late-event", "main", indexTime.Add(-time.Hour))
	late.BatchID = "late-batch"
	if _, err := store.Store(ctx, ingest.StoreBatch{
		TenantID: "tenant", CollectorID: "collector", BatchID: "late-batch", BatchSequence: 2,
		SourceBatchSHA256: testSourceBatchDigest("late-batch"),
		ReceivedAt:        indexTime.Add(-time.Hour),
		Events:            []*ingest.StoredEvent{late},
	}); err != nil {
		t.Fatalf("store post-snapshot event: %v", err)
	}
	var visibleLate uint64
	if err := queryConnection.QueryRow(ctx,
		"SELECT count() FROM open_splunk.events WHERE event_id = ? AND visibility_seq <= ?",
		"late-event", cutoff,
	).Scan(&visibleLate); err != nil {
		t.Fatalf("query immutable cutoff: %v", err)
	}
	if visibleLate != 0 {
		t.Fatalf("post-snapshot event is visible at cutoff %d", cutoff)
	}

	var (
		raw                                           []byte
		signedType, unsignedType, nullType, mixedType string
		bytesType, literalDot, nestedValue            string
		servicePresent, serviceEmpty, levelMissing    bool
		fieldNames                                    []string
		expiresAt                                     time.Time
	)
	query := "SELECT raw, dynamicType(fields.signed), dynamicType(fields.unsigned), " +
		"dynamicType(fields.nothing), dynamicType(fields.mixed), dynamicType(fields.bytes), " +
		"fields.`literal%2Edot`.:String, fields.nested.value.:String, " +
		"isNotNull(service), service = '', isNull(level), field_names, expires_at " +
		"FROM open_splunk.events WHERE event_id = ? LIMIT 1"
	if err := queryConnection.QueryRow(ctx, query, "native-event").Scan(
		&raw, &signedType, &unsignedType, &nullType, &mixedType, &bytesType,
		&literalDot, &nestedValue, &servicePresent, &serviceEmpty, &levelMissing,
		&fieldNames, &expiresAt,
	); err != nil {
		t.Fatalf("query stored event: %v", err)
	}
	if !bytes.Equal(raw, event.Event.Raw) {
		t.Fatalf("raw = %x, want %x", raw, event.Event.Raw)
	}
	if signedType != "Int64" || unsignedType != "UInt64" || nullType != "None" ||
		mixedType != "Array(Dynamic)" || bytesType != "Map(String, String)" {
		t.Fatalf("dynamic types = %q %q %q %q %q", signedType, unsignedType, nullType, mixedType, bytesType)
	}
	if literalDot != "literal" || nestedValue != "nested" || !servicePresent || !serviceEmpty || !levelMissing {
		t.Fatalf("stored values literal=%q nested=%q servicePresent=%v serviceEmpty=%v levelMissing=%v",
			literalDot, nestedValue, servicePresent, serviceEmpty, levelMissing)
	}
	wantNames := []string{"bytes", "literal\\.dot", "mixed", "nested.value", "nothing", "signed", "unsigned"}
	if strings.Join(fieldNames, "\n") != strings.Join(wantNames, "\n") {
		t.Fatalf("field_names = %#v, want %#v", fieldNames, wantNames)
	}
	if want := indexTime.Truncate(time.Millisecond).Add(30 * 24 * time.Hour); !expiresAt.Equal(want) {
		t.Fatalf("expires_at = %v, want %v", expiresAt, want)
	}

	testCompiledQueriesAgainstClickHouse(t, ctx, store, queryConnection, indexTime)
}

func testCompiledQueriesAgainstClickHouse(
	t *testing.T,
	ctx context.Context,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()
	one := compilerIntegrationEvent("n-one", "api", "prefix informational suffix", indexTime,
		typedField("status", typedSint(500)),
		typedField("ratio", typedSint(1)),
		typedField("n", typedSint(1)),
		typedField("literal.dot", typedString("needle")),
	)
	two := compilerIntegrationEvent("n-two", "500", "prefix error42 suffix", indexTime,
		typedField("status", typedString("500")),
		typedField("ratio", typedDouble(1)),
		typedField("n", typedSint(2)),
	)
	null := compilerIntegrationEvent("n-null", "nullable", "nothing here", indexTime,
		typedField("status", typedNull()),
		typedField("ratio", typedNull()),
		typedField("n", typedNull()),
		typedField("nothing", typedNull()),
	)
	batch := ingest.StoreBatch{
		TenantID: "tenant", CollectorID: "collector", BatchID: "compiler-batch", BatchSequence: 3,
		SourceBatchSHA256: testSourceBatchDigest("compiler-batch"),
		ReceivedAt:        indexTime,
		Events:            []*ingest.StoredEvent{one, two, null},
	}
	if _, err := store.Store(ctx, batch); err != nil {
		t.Fatalf("store compiler fixtures: %v", err)
	}
	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture compiler visibility cutoff: %v", err)
	}
	for eventID, wantType := range map[string]string{"n-one": "Int64", "n-two": "Float64", "n-null": "None"} {
		var gotType string
		if err := connection.QueryRow(ctx,
			"SELECT dynamicType(fields.ratio) FROM open_splunk.events WHERE event_id = ?", eventID,
		).Scan(&gotType); err != nil {
			t.Fatalf("query ratio type for %s: %v", eventID, err)
		}
		if gotType != wantType {
			t.Fatalf("ratio type for %s = %q, want %q", eventID, gotType, wantType)
		}
	}

	for source, want := range map[string]uint64{
		`index=compiler host=500`:            1,
		`index=compiler status>=500`:         2,
		`index=compiler ratio=1`:             1,
		`index=compiler ratio=1.0`:           1,
		`index=compiler nothing=null`:        1,
		`index=compiler nothing=*`:           0,
		`index=compiler error*`:              1,
		`index=compiler literal\.dot=needle`: 1,
	} {
		compiled := compileIntegrationSPL(t, source, indexTime.Add(10*time.Second), visibilityCutoff)
		var count uint64
		if err := connection.QueryRow(ctx, "SELECT count() FROM ("+compiled.SQL+")", compiled.Args...).Scan(&count); err != nil {
			t.Fatalf("execute compiled %q: %v\nSQL: %s\nargs: %#v", source, err, compiled.SQL, compiled.Args)
		}
		if count != want {
			t.Fatalf("compiled %q count = %d, want %d", source, count, want)
		}
	}

	globalStats := compileIntegrationSPL(t, `index=compiler | stats count`, indexTime.Add(10*time.Second), visibilityCutoff)
	var globalCount uint64
	if err := connection.QueryRow(ctx, globalStats.SQL, globalStats.Args...).Scan(&globalCount); err != nil {
		t.Fatalf("execute global stats: %v\nSQL: %s\nargs: %#v", err, globalStats.SQL, globalStats.Args)
	}
	if globalCount != 3 {
		t.Fatalf("global stats count = %d, want 3", globalCount)
	}
	emptyStats := compileIntegrationSPL(t, `index=compiler event_id=absent | stats count`, indexTime.Add(10*time.Second), visibilityCutoff)
	var emptyCount uint64
	if err := connection.QueryRow(ctx, emptyStats.SQL, emptyStats.Args...).Scan(&emptyCount); err != nil {
		t.Fatalf("execute empty global stats: %v\nSQL: %s\nargs: %#v", err, emptyStats.SQL, emptyStats.Args)
	}
	if emptyCount != 0 {
		t.Fatalf("empty global stats count = %d, want 0", emptyCount)
	}

	// SPL grouping is lexical: a typed integer 500 and a string "500" share a
	// group. The explicit-null status has no field value and is omitted from BY.
	groupedStats := compileIntegrationSPL(t, `index=compiler | stats count AS events by status`, indexTime.Add(10*time.Second), visibilityCutoff)
	groupedRows, err := connection.Query(ctx, groupedStats.SQL, groupedStats.Args...)
	if err != nil {
		t.Fatalf("execute grouped stats: %v\nSQL: %s\nargs: %#v", err, groupedStats.SQL, groupedStats.Args)
	}
	defer groupedRows.Close()
	var groupedKeys []string
	var groupedCounts []uint64
	for groupedRows.Next() {
		var key string
		var count uint64
		if err := groupedRows.Scan(&key, &count); err != nil {
			t.Fatalf("scan grouped stats: %v", err)
		}
		groupedKeys = append(groupedKeys, key)
		groupedCounts = append(groupedCounts, count)
	}
	if err := groupedRows.Err(); err != nil {
		t.Fatalf("iterate grouped stats: %v", err)
	}
	if strings.Join(groupedKeys, ",") != "500" || len(groupedCounts) != 1 || groupedCounts[0] != 2 {
		t.Fatalf("grouped stats = keys %v counts %v, want [500] [2]", groupedKeys, groupedCounts)
	}

	missingAndNull := compileIntegrationSPL(t, `index=compiler | stats count by nothing`, indexTime.Add(10*time.Second), visibilityCutoff)
	missingAndNullRows, err := connection.Query(ctx, missingAndNull.SQL, missingAndNull.Args...)
	if err != nil {
		t.Fatalf("execute missing/null stats: %v\nSQL: %s\nargs: %#v", err, missingAndNull.SQL, missingAndNull.Args)
	}
	defer missingAndNullRows.Close()
	if missingAndNullRows.Next() {
		t.Fatal("stats BY emitted a group for a missing or explicit-null field")
	}
	if err := missingAndNullRows.Err(); err != nil {
		t.Fatalf("iterate missing/null stats: %v", err)
	}

	compiled := compileIntegrationSPL(t, `index=compiler | sort n | tail 2 | table event_id`, indexTime.Add(10*time.Second), visibilityCutoff)
	rows, err := connection.Query(ctx, compiled.SQL, compiled.Args...)
	if err != nil {
		t.Fatalf("execute compiled tail: %v\nSQL: %s\nargs: %#v", err, compiled.SQL, compiled.Args)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan tail row: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate tail rows: %v", err)
	}
	if strings.Join(ids, ",") != "n-null,n-two" {
		t.Fatalf("tail IDs = %v, want [n-null n-two]", ids)
	}
}

func compilerIntegrationEvent(id, host, raw string, indexTime time.Time, fields ...*opensplunkv1.TypedObjectField) *ingest.StoredEvent {
	event := testStoredEvent(id, "compiler", indexTime)
	event.BatchID = "compiler-batch"
	event.Event.Host = host
	event.Event.Raw = []byte(raw)
	event.Event.Fields = typedObjectValue(fields...)
	return event
}

func compileIntegrationSPL(t *testing.T, source string, cutoff time.Time, visibilityCutoff uint64) CompiledQuery {
	t.Helper()
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatalf("parse integration SPL %q: %v", source, err)
	}
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID: "tenant", AuthorizedIndexes: []string{"compiler"},
		Earliest:         time.Date(2026, time.July, 20, 0, 0, 0, 0, time.UTC),
		Latest:           time.Date(2026, time.July, 22, 0, 0, 0, 0, time.UTC),
		IndexTimeCutoff:  cutoff,
		VisibilityCutoff: uint64PointerForIntegration(visibilityCutoff),
	})
	if err != nil {
		t.Fatalf("build integration SPL %q: %v", source, err)
	}
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("compile integration SPL %q: %v", source, err)
	}
	return compiled
}

func uint64PointerForIntegration(value uint64) *uint64 { return &value }

func integrationRandomHex(t *testing.T, size int) string {
	t.Helper()
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(value)
}

func integrationDocker(t *testing.T, ctx context.Context, stdin *bytes.Reader, args ...string) {
	t.Helper()
	command := exec.CommandContext(ctx, "docker", args...)
	if stdin != nil {
		command.Stdin = stdin
	}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, output)
	}
}

func integrationWaitForClickHouse(t *testing.T, ctx context.Context, container, password string) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	stable := 0
	var last string
	for time.Now().Before(deadline) {
		command := exec.CommandContext(ctx, "docker", "exec", container, "clickhouse-client",
			"--user", "open_splunk", "--password", password, "--query", "SELECT 1")
		output, err := command.CombinedOutput()
		last = fmt.Sprintf("%v: %s", err, output)
		if err == nil && strings.TrimSpace(string(output)) == "1" {
			stable++
			if stable == 4 {
				return
			}
		} else {
			stable = 0
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for ClickHouse: %v", ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
	t.Fatalf("ClickHouse did not become ready: %s", last)
}

func integrationNativeAddress(t *testing.T, ctx context.Context, container string) string {
	t.Helper()
	output, err := exec.CommandContext(ctx, "docker", "port", container, "9000/tcp").CombinedOutput()
	if err != nil {
		t.Fatalf("resolve native port: %v: %s", err, output)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "127.0.0.1:") {
			return line
		}
	}
	t.Fatalf("Docker returned no loopback native address: %s", output)
	return ""
}
