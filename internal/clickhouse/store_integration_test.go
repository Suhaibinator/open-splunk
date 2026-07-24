package clickhouse

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	t.Run("ambiguous committed insert recovers after server restart", func(t *testing.T) {
		testAmbiguousCommittedInsertRecovery(t, ctx, config, queryConnection, indexTime)
	})
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
		fieldTypes                                    []uint8
		fieldMetadataVersion                          uint8
		expiresAt                                     time.Time
	)
	query := "SELECT raw, dynamicType(fields.signed), dynamicType(fields.unsigned), " +
		"dynamicType(fields.nothing), dynamicType(fields.mixed), dynamicType(fields.bytes), " +
		"fields.`literal%2Edot`.:String, fields.nested.value.:String, " +
		"isNotNull(service), service = '', isNull(level), field_names, field_types, field_metadata_version, expires_at " +
		"FROM open_splunk.events WHERE event_id = ? LIMIT 1"
	if err := queryConnection.QueryRow(ctx, query, "native-event").Scan(
		&raw, &signedType, &unsignedType, &nullType, &mixedType, &bytesType,
		&literalDot, &nestedValue, &servicePresent, &serviceEmpty, &levelMissing,
		&fieldNames, &fieldTypes, &fieldMetadataVersion, &expiresAt,
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
	wantTypes := []uint8{7, 2, 10, 2, 1, 3, 4}
	if !slices.Equal(fieldTypes, wantTypes) || fieldMetadataVersion != 1 {
		t.Fatalf("field metadata = version %d types %#v, want version 1 types %#v", fieldMetadataVersion, fieldTypes, wantTypes)
	}
	if want := indexTime.Truncate(time.Millisecond).Add(30 * 24 * time.Hour); !expiresAt.Equal(want) {
		t.Fatalf("expires_at = %v, want %v", expiresAt, want)
	}

	testCompiledQueriesAgainstClickHouse(t, ctx, store, queryConnection, indexTime)
}

func testAmbiguousCommittedInsertRecovery(
	t *testing.T,
	ctx context.Context,
	config Config,
	queryConnection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	controlPath := filepath.Join(t.TempDir(), "restart-control.sqlite")
	firstControl, err := control.Open(ctx, controlPath)
	if err != nil {
		t.Fatalf("open first visibility control database: %v", err)
	}
	firstControlOpen := true
	defer func() {
		if firstControlOpen {
			_ = firstControl.Close()
		}
	}()
	firstSequencer, err := visibility.NewSQLite(ctx, firstControl)
	if err != nil {
		t.Fatalf("create first visibility sequencer: %v", err)
	}
	options, normalized, err := config.clickHouseOptions()
	if err != nil {
		t.Fatal(err)
	}
	firstNative, err := clickhousedriver.Open(options)
	if err != nil {
		t.Fatalf("open first native ClickHouse connection: %v", err)
	}
	firstStore, err := newStore(
		&commitThenErrorStoreConnection{delegate: &nativeStoreConnection{connection: firstNative}},
		normalized.Database,
		normalized.Table,
		fixedRetention(30*24*time.Hour),
		firstSequencer,
		time.Now,
		normalized.RetryAfter,
	)
	if err != nil {
		_ = firstNative.Close()
		t.Fatalf("create first store: %v", err)
	}
	firstStoreOpen := true
	defer func() {
		if firstStoreOpen {
			_ = firstStore.Close()
		}
	}()

	event := testStoredEvent("ambiguous-restart-event", "main", indexTime.Add(time.Second))
	event.CollectorID = "restart-collector"
	event.BatchID = "ambiguous-restart-batch"
	batch := ingest.StoreBatch{
		TenantID: "tenant", CollectorID: "restart-collector",
		BatchID: "ambiguous-restart-batch", BatchSequence: 1, OriginalEventCount: 1,
		SourceBatchSHA256: testSourceBatchDigest("ambiguous-restart-batch"),
		ReceivedAt:        indexTime.Add(time.Second),
		Events:            []*ingest.StoredEvent{event},
	}
	result, storeErr := firstStore.Store(ctx, batch)
	if !isTransient(storeErr) ||
		result.Accepted != 0 || result.Duplicate != 0 || result.AcknowledgedThrough != nil ||
		!result.CommittedAt.IsZero() || result.OriginalEventCount != 0 || len(result.RejectedEvents) != 0 {
		t.Fatalf("outcome-ambiguous Store = (%+v, %v), want zero result and transient error", result, storeErr)
	}
	if !errors.Is(storeErr, io.ErrUnexpectedEOF) {
		t.Fatalf("outcome-ambiguous Store error = %v, want wrapped %v", storeErr, io.ErrUnexpectedEOF)
	}
	digest, err := storePayloadDigest(batch)
	if err != nil {
		t.Fatal(err)
	}
	pending, found, err := firstSequencer.Lookup(
		ctx,
		deduplicationToken(batch),
		sequenceIdentityKey(batch),
		digest,
	)
	if err != nil || !found || pending.AlreadyCommitted || !pending.MayHaveReachedStorage ||
		pending.Sequence != 1 || len(pending.Outbox) == 0 || !pending.CommittedAt.IsZero() {
		t.Fatalf("ambiguous reservation before restart = %+v, found=%v, error=%v", pending, found, err)
	}
	if cutoff, cutoffErr := firstStore.VisibilityCutoff(ctx); cutoffErr != nil || cutoff != 0 {
		t.Fatalf("visibility cutoff before restart = %d, error = %v, want 0", cutoff, cutoffErr)
	}
	if err := firstStore.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}
	firstStoreOpen = false
	if err := firstControl.Close(); err != nil {
		t.Fatalf("close first visibility control database: %v", err)
	}
	firstControlOpen = false

	var count uint64
	if err := queryConnection.QueryRow(ctx,
		"SELECT count() FROM open_splunk.events WHERE event_id = ?",
		event.Event.GetEventId(),
	).Scan(&count); err != nil || count != 1 {
		t.Fatalf("committed row before restart recovery = %d, error = %v, want 1", count, err)
	}

	secondControl, err := control.Open(ctx, controlPath)
	if err != nil {
		t.Fatalf("reopen visibility control database: %v", err)
	}
	defer func() { _ = secondControl.Close() }()
	secondSequencer, err := visibility.NewSQLite(ctx, secondControl)
	if err != nil {
		t.Fatalf("create restarted visibility sequencer: %v", err)
	}
	secondStore, err := Open(config, fixedRetention(30*24*time.Hour), secondSequencer)
	if err != nil {
		t.Fatalf("open restarted store: %v", err)
	}
	defer func() { _ = secondStore.Close() }()

	deadline := time.Now().Add(10 * time.Second)
	for {
		cutoff, cutoffErr := secondStore.VisibilityCutoff(ctx)
		if cutoffErr != nil {
			t.Fatalf("read recovered visibility cutoff: %v", cutoffErr)
		}
		if cutoff == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("restart reconciliation did not advance visibility cutoff: got %d, want 1", cutoff)
		}
		time.Sleep(10 * time.Millisecond)
	}

	reservation, found, err := secondSequencer.Lookup(
		ctx,
		deduplicationToken(batch),
		sequenceIdentityKey(batch),
		digest,
	)
	if err != nil || !found || !reservation.AlreadyCommitted || reservation.Sequence != 1 || len(reservation.Outbox) != 0 {
		t.Fatalf("recovered reservation = %+v, found=%v, error=%v", reservation, found, err)
	}
	result, err = secondStore.Store(ctx, batch)
	if err != nil || result.Accepted != 0 || result.Duplicate != 1 || result.OriginalEventCount != 1 {
		t.Fatalf("collector retry after restart = (%+v, %v), want one duplicate", result, err)
	}
	if err := queryConnection.QueryRow(ctx,
		"SELECT count() FROM open_splunk.events WHERE event_id = ?",
		event.Event.GetEventId(),
	).Scan(&count); err != nil || count != 1 {
		t.Fatalf("row count after replay and collector retry = %d, error = %v, want 1", count, err)
	}

	nextEvent := testStoredEvent("ambiguous-restart-next", "main", indexTime.Add(2*time.Second))
	nextEvent.CollectorID = "restart-collector"
	nextEvent.BatchID = "ambiguous-restart-next-batch"
	nextBatch := ingest.StoreBatch{
		TenantID: "tenant", CollectorID: "restart-collector",
		BatchID: "ambiguous-restart-next-batch", BatchSequence: 2, OriginalEventCount: 1,
		SourceBatchSHA256: testSourceBatchDigest("ambiguous-restart-next-batch"),
		ReceivedAt:        indexTime.Add(2 * time.Second),
		Events:            []*ingest.StoredEvent{nextEvent},
	}
	result, err = secondStore.Store(ctx, nextBatch)
	if err != nil || result.Accepted != 1 || result.Duplicate != 0 {
		t.Fatalf("post-recovery Store = (%+v, %v), want one accepted event", result, err)
	}
	if cutoff, cutoffErr := secondStore.VisibilityCutoff(ctx); cutoffErr != nil || cutoff != 2 {
		t.Fatalf("post-recovery visibility cutoff = %d, error = %v, want 2", cutoff, cutoffErr)
	}
	nextDigest, err := storePayloadDigest(nextBatch)
	if err != nil {
		t.Fatal(err)
	}
	nextReservation, found, err := secondSequencer.Lookup(
		ctx,
		deduplicationToken(nextBatch),
		sequenceIdentityKey(nextBatch),
		nextDigest,
	)
	if err != nil || !found || !nextReservation.AlreadyCommitted ||
		nextReservation.Sequence != 2 || len(nextReservation.Outbox) != 0 {
		t.Fatalf("post-recovery reservation = %+v, found=%v, error=%v", nextReservation, found, err)
	}
	if err := queryConnection.QueryRow(ctx,
		"SELECT count() FROM open_splunk.events WHERE event_id = ?",
		nextEvent.Event.GetEventId(),
	).Scan(&count); err != nil || count != 1 {
		t.Fatalf("post-recovery row count = %d, error = %v, want 1", count, err)
	}
}

type commitThenErrorStoreConnection struct {
	delegate storeConnection
}

func (connection *commitThenErrorStoreConnection) prepare(
	ctx context.Context,
	query string,
	settings clickhousedriver.Settings,
) (writeBatch, error) {
	batch, err := connection.delegate.prepare(ctx, query, settings)
	if err != nil {
		return nil, err
	}
	return &commitThenErrorWriteBatch{writeBatch: batch}, nil
}

func (connection *commitThenErrorStoreConnection) Ping(ctx context.Context) error {
	return connection.delegate.Ping(ctx)
}

func (connection *commitThenErrorStoreConnection) Close() error {
	return connection.delegate.Close()
}

type commitThenErrorWriteBatch struct {
	writeBatch
}

func (batch *commitThenErrorWriteBatch) Send() error {
	if err := batch.writeBatch.Send(); err != nil {
		return err
	}
	return io.ErrUnexpectedEOF
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
		typedField("left", typedSint(10)),
		typedField("right", typedSint(2)),
		typedField("unsigned_max", typedUint(^uint64(0))),
		typedField("wide_sort", typedSint(-9_007_199_254_740_993)),
		typedField("dynamic_flag", typedBool(true)),
		typedField("other_flag", typedBool(false)),
		typedField("latency", typedString("NaN")),
		typedField("foo?bar", typedString("needle")),
		typedField("amount$1", typedString("money")),
		typedField("brace{x:y}", typedString("group")),
		typedField("mixed_by", typedString("scalar")),
		typedField("tenant_probe", typedString("visible")),
		typedField("sort_value", typedString("10")),
		typedField("literal.dot", typedString("needle")),
		typedField("category", typedString("alpha")),
		typedField("category_nullable", typedString("alpha")),
		typedField("empty_value", typedString("")),
		typedField("path", typedString("/slow")),
		typedField("duration", typedString("600ms")),
		typedField("date", typedString("1/14/2023")),
	)
	two := compilerIntegrationEvent("n-two", "500", "prefix error42 suffix", indexTime,
		typedField("status", typedString("500")),
		typedField("ratio", typedDouble(1)),
		typedField("n", typedSint(2)),
		typedField("left", typedSint(2)),
		typedField("right", typedSint(10)),
		typedField("wide_sort", typedSint(-9_007_199_254_740_992)),
		typedField("latency", typedString("200")),
		typedField("sort_value", typedString("2")),
		typedField("category", typedString("alpha")),
		typedField("category_nullable", typedString("alpha")),
		typedField("path", typedString("/slow")),
		typedField("duration", typedString("700ms")),
	)
	null := compilerIntegrationEvent("n-null", "nullable", "nothing here", indexTime,
		typedField("status", typedNull()),
		typedField("ratio", typedNull()),
		typedField("n", typedNull()),
		typedField("nothing", typedNull()),
		typedField("category", typedString("beta")),
		typedField("category_nullable", typedNull()),
		typedField("path", typedString("/fast")),
		typedField("duration", typedString("20ms")),
		typedField("duration_null", typedNull()),
	)
	extendedTimestamp := time.Date(2026, time.July, 21, 3, 4, 5, 123_456_789, time.UTC)
	complex := compilerIntegrationEvent("n-complex", "complex", "complex values", indexTime,
		typedField("n", typedSint(0)),
		typedField("multi", typedList(typedSint(1), typedSint(2))),
		typedField("bytes_value", typedBytes([]byte{0, 1, 2, 255})),
		typedField("timestamp_value", typedTimestamp(extendedTimestamp)),
		typedField("duration_value", typedDuration(3*time.Second+4*time.Nanosecond)),
		typedField("decimal_value", typedDecimal("123.4500")),
		typedField("latency", typedString("100")),
		typedField("sort_value", typedString("text")),
		typedField("object_value", typedObject()),
		typedField("object_parent", typedObject(typedField("child", typedString("nested")))),
		typedField("mixed_by", typedList(typedString("container"))),
		typedField("category", typedString("gamma")),
		typedField("path", typedString("/slow")),
		typedField("duration", typedString("5µs")),
		typedField("duration_container", typedList(typedString("800ms"))),
	)
	batch := ingest.StoreBatch{
		TenantID: "tenant", CollectorID: "collector", BatchID: "compiler-batch", BatchSequence: 3,
		SourceBatchSHA256: testSourceBatchDigest("compiler-batch"),
		ReceivedAt:        indexTime,
		Events:            []*ingest.StoredEvent{one, two, null, complex},
	}
	if _, err := store.Store(ctx, batch); err != nil {
		t.Fatalf("store compiler fixtures: %v", err)
	}
	foreign := compilerIntegrationEvent("foreign-complex", "foreign", "must remain out of scope", indexTime,
		typedField("tenant_probe", typedList(typedString("hidden"))),
	)
	foreign.TenantID = "other-tenant"
	foreign.BatchID = "foreign-compiler-batch"
	foreignBatch := ingest.StoreBatch{
		TenantID: "other-tenant", CollectorID: "collector", BatchID: "foreign-compiler-batch", BatchSequence: 4,
		SourceBatchSHA256: testSourceBatchDigest("foreign-compiler-batch"),
		ReceivedAt:        indexTime,
		Events:            []*ingest.StoredEvent{foreign},
	}
	if _, err := store.Store(ctx, foreignBatch); err != nil {
		t.Fatalf("store cross-tenant compiler fixture: %v", err)
	}
	// Force both tenant fixtures into one sorted part/granule. This makes the
	// regression prove that a post-aggregate unsupported-value check cannot be
	// pushed below tenant filtering and observe the foreign Array value.
	if err := connection.Exec(ctx, "OPTIMIZE TABLE open_splunk.events FINAL"); err != nil {
		t.Fatalf("merge cross-tenant compiler fixtures: %v", err)
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
		`index=compiler host=500`:                                                       1,
		`index=compiler status>=500`:                                                    2,
		`index=compiler ratio=1`:                                                        1,
		`index=compiler ratio=1.0`:                                                      1,
		`index=compiler nothing=null`:                                                   1,
		`index=compiler nothing=*`:                                                      0,
		`index=compiler error*`:                                                         1,
		`index=compiler literal\.dot=needle`:                                            1,
		`index=compiler | where left>right`:                                             1,
		`index=compiler | where unsigned_max>18446744073709551614`:                      1,
		`index=compiler | where unsigned_max=18446744073709551614`:                      0,
		`index=compiler | where _time>0`:                                                4,
		`index=compiler foo?bar=needle`:                                                 1,
		`index=compiler amount$1=money`:                                                 1,
		`index=compiler brace{x:y}=group`:                                               1,
		`index=compiler decimal_value>100`:                                              1,
		`index=compiler decimal_value=123.45`:                                           1,
		`index=compiler host>"b"`:                                                       2,
		`index=compiler category>"alpha"`:                                               2,
		`index=compiler | eval one=1 | where one=true`:                                  0,
		`index=compiler event_id=n-one | stats count | where count=true`:                0,
		`index=compiler | where n=true`:                                                 0,
		`index=compiler event_id=n-one | eval fixed_flag=true | where fixed_flag>false`: 0,
		`index=compiler | where dynamic_flag>false`:                                     0,
		`index=compiler | where dynamic_flag>other_flag`:                                0,
		`index=compiler | where dynamic_flag=true`:                                      1,
		`index=compiler | where dynamic_flag!=other_flag`:                               1,
		`index=compiler event_id=n-one | eval x=tonumber("bad") | search x=null`:        1,
		`index=compiler event_id=n-one | eval x=tonumber("bad") | search x=*`:           0,
		`index=compiler | stats p95(absent) AS p | search p=null`:                       1,
		`index=compiler | stats p95(absent) AS p | search p=*`:                          0,
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
	if globalCount != 4 {
		t.Fatalf("global stats count = %d, want 4", globalCount)
	}
	emptyStats := compileIntegrationSPL(t, `index=compiler event_id=absent | stats count`, indexTime.Add(10*time.Second), visibilityCutoff)
	var emptyCount uint64
	if err := connection.QueryRow(ctx, emptyStats.SQL, emptyStats.Args...).Scan(&emptyCount); err != nil {
		t.Fatalf("execute empty global stats: %v\nSQL: %s\nargs: %#v", err, emptyStats.SQL, emptyStats.Args)
	}
	if emptyCount != 0 {
		t.Fatalf("empty global stats count = %d, want 0", emptyCount)
	}

	for _, test := range []struct {
		name   string
		source string
		want   *float64
	}{
		{
			name:   "valid string",
			source: `index=compiler | eval duration_ms=tonumber(replace(duration, "ms$", "")) | where event_id="n-one" | table duration_ms`,
			want:   float64PointerForIntegration(600),
		},
		{
			name:   "failed conversion",
			source: `index=compiler | eval duration_ms=tonumber(replace(duration, "ms$", "")) | where event_id="n-complex" | table duration_ms`,
		},
		{
			name:   "container input",
			source: `index=compiler | eval duration_ms=tonumber(replace(duration_container, "ms$", "")) | where event_id="n-complex" | table duration_ms`,
		},
		{
			name:   "explicit null input",
			source: `index=compiler | eval duration_ms=tonumber(replace(duration_null, "ms$", "")) | where event_id="n-null" | table duration_ms`,
		},
	} {
		t.Run("eval "+test.name, func(t *testing.T) {
			compiled := compileIntegrationSPL(t, test.source, indexTime.Add(10*time.Second), visibilityCutoff)
			var got *float64
			if err := connection.QueryRow(ctx, compiled.SQL, compiled.Args...).Scan(&got); err != nil {
				t.Fatalf("execute eval: %v\nSQL: %s\nargs: %#v", err, compiled.SQL, compiled.Args)
			}
			if test.want == nil && got != nil {
				t.Fatalf("eval value = %v, want null", *got)
			}
			if test.want != nil && (got == nil || math.Abs(*got-*test.want) > 1e-12) {
				t.Fatalf("eval value = %v, want %v", got, *test.want)
			}
		})
	}

	sequentialEval := compileIntegrationSPL(t,
		`index=compiler | eval first=replace(duration, "ms$", ""), second=replace(first, "6", "9"), parsed=tonumber(second) | where event_id="n-one" | table parsed`,
		indexTime.Add(10*time.Second), visibilityCutoff,
	)
	var sequentialValue *float64
	if err := connection.QueryRow(ctx, sequentialEval.SQL, sequentialEval.Args...).Scan(&sequentialValue); err != nil {
		t.Fatalf("execute sequential eval: %v\nSQL: %s\nargs: %#v", err, sequentialEval.SQL, sequentialEval.Args)
	}
	if sequentialValue == nil || *sequentialValue != 900 {
		t.Fatalf("sequential eval = %v, want 900", sequentialValue)
	}

	captureReplace := compileIntegrationSPL(t,
		`index=compiler | eval formatted=replace(date, "^(\d{1,2})/(\d{1,2})/", "\2/\1/") | where event_id="n-one" | table formatted`,
		indexTime.Add(10*time.Second), visibilityCutoff,
	)
	var formattedDate *string
	if err := connection.QueryRow(ctx, captureReplace.SQL, captureReplace.Args...).Scan(&formattedDate); err != nil {
		t.Fatalf("execute capture replacement: %v\nSQL: %s\nargs: %#v", err, captureReplace.SQL, captureReplace.Args)
	}
	if formattedDate == nil || *formattedDate != "14/1/2023" {
		t.Fatalf("capture replacement = %v, want 14/1/2023", formattedDate)
	}

	overwriteEval := compileIntegrationSPL(t,
		`index=compiler | eval message=replace(message, "Request", "Updated") | where event_id="n-one" | table message`,
		indexTime.Add(10*time.Second), visibilityCutoff,
	)
	var overwrittenMessage *string
	if err := connection.QueryRow(ctx, overwriteEval.SQL, overwriteEval.Args...).Scan(&overwrittenMessage); err != nil {
		t.Fatalf("execute eval overwrite: %v\nSQL: %s\nargs: %#v", err, overwriteEval.SQL, overwriteEval.Args)
	}
	if overwrittenMessage == nil || *overwrittenMessage != "Updated metrics" {
		t.Fatalf("eval overwrite = %v, want Updated metrics", overwrittenMessage)
	}

	metacharEval := compileIntegrationSPL(t,
		`index=compiler event_id=n-one | eval result?=1 | table result?`,
		indexTime.Add(10*time.Second), visibilityCutoff,
	)
	metacharRows, err := connection.Query(ctx, metacharEval.SQL, metacharEval.Args...)
	if err != nil {
		t.Fatalf("execute bind-metachar eval: %v\nSQL: %s\nargs: %#v", err, metacharEval.SQL, metacharEval.Args)
	}
	if columns := metacharRows.Columns(); !slices.Equal(columns, []string{"result?"}) {
		_ = metacharRows.Close()
		t.Fatalf("bind-metachar eval columns = %v", columns)
	}
	if !metacharRows.Next() {
		_ = metacharRows.Close()
		t.Fatalf("bind-metachar eval returned no row: %v", metacharRows.Err())
	}
	var metacharValue int64
	if err := metacharRows.Scan(&metacharValue); err != nil {
		_ = metacharRows.Close()
		t.Fatalf("scan bind-metachar eval: %v", err)
	}
	if metacharValue != 1 || metacharRows.Next() {
		_ = metacharRows.Close()
		t.Fatalf("bind-metachar eval value = %d", metacharValue)
	}
	if err := metacharRows.Err(); err != nil {
		_ = metacharRows.Close()
		t.Fatalf("iterate bind-metachar eval: %v", err)
	}
	if err := metacharRows.Close(); err != nil {
		t.Fatalf("close bind-metachar eval: %v", err)
	}

	metacharStats := compileIntegrationSPL(t,
		`index=compiler | stats count AS total$1 BY brace{x:y}`,
		indexTime.Add(10*time.Second), visibilityCutoff,
	)
	metacharStatsRows, err := connection.Query(ctx, metacharStats.SQL, metacharStats.Args...)
	if err != nil {
		t.Fatalf("execute bind-metachar stats: %v\nSQL: %s\nargs: %#v", err, metacharStats.SQL, metacharStats.Args)
	}
	if columns := metacharStatsRows.Columns(); !slices.Equal(columns, []string{"brace{x:y}", "total$1"}) {
		_ = metacharStatsRows.Close()
		t.Fatalf("bind-metachar stats columns = %v", columns)
	}
	if !metacharStatsRows.Next() {
		_ = metacharStatsRows.Close()
		t.Fatalf("bind-metachar stats returned no row: %v", metacharStatsRows.Err())
	}
	var metacharGroup string
	var metacharCount uint64
	if err := metacharStatsRows.Scan(&metacharGroup, &metacharCount); err != nil {
		_ = metacharStatsRows.Close()
		t.Fatalf("scan bind-metachar stats: %v", err)
	}
	if metacharGroup != "group" || metacharCount != 1 || metacharStatsRows.Next() {
		_ = metacharStatsRows.Close()
		t.Fatalf("bind-metachar stats row = %q/%d", metacharGroup, metacharCount)
	}
	if err := metacharStatsRows.Err(); err != nil {
		_ = metacharStatsRows.Close()
		t.Fatalf("iterate bind-metachar stats: %v", err)
	}
	if err := metacharStatsRows.Close(); err != nil {
		t.Fatalf("close bind-metachar stats: %v", err)
	}

	typedEval := compileIntegrationSPL(t,
		`index=compiler event_id=n-one | eval signed=-7,unsigned=18446744073709551615,ratio_value=1.25,ok=true,text="x" | table signed,unsigned,ratio_value,ok,text`,
		indexTime.Add(10*time.Second), visibilityCutoff,
	)
	typedRows, err := connection.Query(ctx, typedEval.SQL, typedEval.Args...)
	if err != nil {
		t.Fatalf("execute typed eval: %v\nSQL: %s\nargs: %#v", err, typedEval.SQL, typedEval.Args)
	}
	if types := typedRows.ColumnTypes(); len(types) != 5 || types[0].DatabaseTypeName() != "Int64" ||
		types[1].DatabaseTypeName() != "UInt64" || types[2].DatabaseTypeName() != "Float64" ||
		types[3].DatabaseTypeName() != "Bool" || types[4].DatabaseTypeName() != "String" {
		_ = typedRows.Close()
		t.Fatalf("typed eval column types = %#v", types)
	}
	if !typedRows.Next() {
		_ = typedRows.Close()
		t.Fatalf("typed eval returned no row: %v", typedRows.Err())
	}
	var signed int64
	var unsigned uint64
	var ratioValue float64
	var okValue bool
	var textValue string
	if err := typedRows.Scan(&signed, &unsigned, &ratioValue, &okValue, &textValue); err != nil {
		_ = typedRows.Close()
		t.Fatalf("scan typed eval: %v", err)
	}
	if signed != -7 || unsigned != ^uint64(0) || ratioValue != 1.25 || !okValue || textValue != "x" || typedRows.Next() {
		_ = typedRows.Close()
		t.Fatalf("typed eval values = %d/%d/%g/%t/%q", signed, unsigned, ratioValue, okValue, textValue)
	}
	if err := typedRows.Err(); err != nil {
		_ = typedRows.Close()
		t.Fatalf("iterate typed eval: %v", err)
	}
	if err := typedRows.Close(); err != nil {
		t.Fatalf("close typed eval: %v", err)
	}

	slowRoutes := compileIntegrationSPL(t, `index=compiler message="Request metrics"
| eval duration_ms=tonumber(replace(duration, "ms$", ""))
| stats count p95(duration_ms) AS p95_ms BY path
| where p95_ms>500`, indexTime.Add(10*time.Second), visibilityCutoff)
	slowRows, err := connection.Query(ctx, slowRoutes.SQL, slowRoutes.Args...)
	if err != nil {
		t.Fatalf("execute slow-route pipeline: %v\nSQL: %s\nargs: %#v", err, slowRoutes.SQL, slowRoutes.Args)
	}
	if types := slowRows.ColumnTypes(); len(types) != 3 || types[0].DatabaseTypeName() != "String" ||
		types[1].DatabaseTypeName() != "UInt64" || types[2].DatabaseTypeName() != "Nullable(Float64)" {
		_ = slowRows.Close()
		t.Fatalf("slow-route column types = %#v", types)
	}
	if !slowRows.Next() {
		_ = slowRows.Close()
		t.Fatalf("slow-route pipeline returned no row: %v", slowRows.Err())
	}
	var slowPath string
	var slowCount uint64
	var slowP95 *float64
	if err := slowRows.Scan(&slowPath, &slowCount, &slowP95); err != nil {
		_ = slowRows.Close()
		t.Fatalf("scan slow-route row: %v", err)
	}
	if slowPath != "/slow" || slowCount != 3 || slowP95 == nil || math.Abs(*slowP95-700) > 1e-12 || slowRows.Next() {
		_ = slowRows.Close()
		t.Fatalf("slow-route row = %q/%d/%v, want /slow/3/700", slowPath, slowCount, slowP95)
	}
	if err := slowRows.Err(); err != nil {
		_ = slowRows.Close()
		t.Fatalf("iterate slow-route rows: %v", err)
	}
	if err := slowRows.Close(); err != nil {
		t.Fatalf("close slow-route rows: %v", err)
	}

	allNullP95 := compileIntegrationSPL(t,
		`index=compiler | stats count p95(duration_container) AS p95_ms BY path | where path="/slow"`,
		indexTime.Add(10*time.Second), visibilityCutoff,
	)
	var nullPath string
	var nullCount uint64
	var nullP95 *float64
	if err := connection.QueryRow(ctx, allNullP95.SQL, allNullP95.Args...).Scan(&nullPath, &nullCount, &nullP95); err != nil {
		t.Fatalf("execute all-null percentile: %v\nSQL: %s\nargs: %#v", err, allNullP95.SQL, allNullP95.Args)
	}
	if nullPath != "/slow" || nullCount != 3 || nullP95 != nil {
		t.Fatalf("all-null percentile = %q/%d/%v, want /slow/3/null", nullPath, nullCount, nullP95)
	}

	for _, test := range []struct {
		name   string
		source string
		want   float64
	}{
		{name: "tagged decimal", source: `index=compiler | stats p95(decimal_value) AS value`, want: 123.45},
		{name: "nonfinite string ignored", source: `index=compiler | stats p95(latency) AS value`, want: 200},
		{name: "canonical time", source: `index=compiler | stats p95(_time) AS value`, want: float64(time.Date(2026, 7, 21, 3, 4, 5, 123456789, time.FixedZone("event-offset", 5*60*60)).UnixNano()) / 1e9},
	} {
		compiled := compileIntegrationSPL(t, test.source, indexTime.Add(10*time.Second), visibilityCutoff)
		var value *float64
		if err := connection.QueryRow(ctx, compiled.SQL, compiled.Args...).Scan(&value); err != nil {
			t.Fatalf("execute %s percentile: %v\nSQL: %s\nargs: %#v", test.name, err, compiled.SQL, compiled.Args)
		}
		if value == nil || math.Abs(*value-test.want) > 1e-6 {
			t.Fatalf("%s percentile = %v, want %g", test.name, value, test.want)
		}
	}

	for _, source := range []string{
		`index=compiler | where NOT absent=1`,
		`index=compiler | where NOT duration_null=1`,
		`index=compiler | where _time=true`,
		`index=compiler | where NOT _time=true`,
		`index=compiler | where _time=null`,
		`index=compiler | where true=null`,
	} {
		compiled := compileIntegrationSPL(t, source, indexTime.Add(10*time.Second), visibilityCutoff)
		if err := executeCompiledExpectingNoRows(ctx, connection, compiled); err != nil {
			t.Fatalf("where three-valued NOT %q: %v\nSQL: %s\nargs: %#v", source, err, compiled.SQL, compiled.Args)
		}
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

	top := compileIntegrationSPL(t, `index=compiler | top limit=2 category`, indexTime.Add(10*time.Second), visibilityCutoff)
	topRows, err := connection.Query(ctx, top.SQL, top.Args...)
	if err != nil {
		t.Fatalf("execute top: %v\nSQL: %s\nargs: %#v", err, top.SQL, top.Args)
	}
	if types := topRows.ColumnTypes(); len(types) != 3 || types[0].DatabaseTypeName() != "String" ||
		types[1].DatabaseTypeName() != "UInt64" || types[2].DatabaseTypeName() != "Float64" {
		_ = topRows.Close()
		t.Fatalf("top column types = %#v", types)
	}
	var topKeys []string
	var topCounts []uint64
	var topPercents []float64
	for topRows.Next() {
		var key string
		var count uint64
		var percent float64
		if err := topRows.Scan(&key, &count, &percent); err != nil {
			_ = topRows.Close()
			t.Fatalf("scan top: %v", err)
		}
		topKeys = append(topKeys, key)
		topCounts = append(topCounts, count)
		topPercents = append(topPercents, percent)
	}
	if err := topRows.Err(); err != nil {
		_ = topRows.Close()
		t.Fatalf("iterate top: %v", err)
	}
	if err := topRows.Close(); err != nil {
		t.Fatalf("close top: %v", err)
	}
	if strings.Join(topKeys, ",") != "alpha,gamma" || !slices.Equal(topCounts, []uint64{2, 1}) ||
		len(topPercents) != 2 || math.Abs(topPercents[0]-50) > 1e-12 || math.Abs(topPercents[1]-25) > 1e-12 {
		t.Fatalf("top rows = keys %v counts %v percents %v, want alpha/2/50 then gamma/1/25", topKeys, topCounts, topPercents)
	}

	rare := compileIntegrationSPL(t, `index=compiler | rare limit=2 category`, indexTime.Add(10*time.Second), visibilityCutoff)
	rareRows, err := connection.Query(ctx, rare.SQL, rare.Args...)
	if err != nil {
		t.Fatalf("execute rare: %v\nSQL: %s\nargs: %#v", err, rare.SQL, rare.Args)
	}
	if types := rareRows.ColumnTypes(); len(types) != 3 || types[0].DatabaseTypeName() != "String" ||
		types[1].DatabaseTypeName() != "UInt64" || types[2].DatabaseTypeName() != "Float64" {
		_ = rareRows.Close()
		t.Fatalf("rare column types = %#v", types)
	}
	var rareKeys []string
	var rareCounts []uint64
	var rarePercents []float64
	for rareRows.Next() {
		var key string
		var count uint64
		var percent float64
		if err := rareRows.Scan(&key, &count, &percent); err != nil {
			_ = rareRows.Close()
			t.Fatalf("scan rare: %v", err)
		}
		rareKeys = append(rareKeys, key)
		rareCounts = append(rareCounts, count)
		rarePercents = append(rarePercents, percent)
	}
	if err := rareRows.Err(); err != nil {
		_ = rareRows.Close()
		t.Fatalf("iterate rare: %v", err)
	}
	if err := rareRows.Close(); err != nil {
		t.Fatalf("close rare: %v", err)
	}
	if strings.Join(rareKeys, ",") != "gamma,beta" || !slices.Equal(rareCounts, []uint64{1, 1}) ||
		len(rarePercents) != 2 || math.Abs(rarePercents[0]-25) > 1e-12 || math.Abs(rarePercents[1]-25) > 1e-12 {
		t.Fatalf("rare rows = keys %v counts %v percents %v, want gamma/1/25 then beta/1/25", rareKeys, rareCounts, rarePercents)
	}

	nullableTop := compileIntegrationSPL(t, `index=compiler | top limit=0 category_nullable`, indexTime.Add(10*time.Second), visibilityCutoff)
	var nullableKey string
	var nullableCount uint64
	var nullablePercent float64
	if err := connection.QueryRow(ctx, nullableTop.SQL, nullableTop.Args...).Scan(&nullableKey, &nullableCount, &nullablePercent); err != nil {
		t.Fatalf("execute missing/null top: %v\nSQL: %s\nargs: %#v", err, nullableTop.SQL, nullableTop.Args)
	}
	if nullableKey != "alpha" || nullableCount != 2 || nullablePercent != 100 {
		t.Fatalf("missing/null top = %q/%d/%v, want alpha/2/100", nullableKey, nullableCount, nullablePercent)
	}

	nullableRare := compileIntegrationSPL(t, `index=compiler | rare limit=0 category_nullable`, indexTime.Add(10*time.Second), visibilityCutoff)
	if err := connection.QueryRow(ctx, nullableRare.SQL, nullableRare.Args...).Scan(&nullableKey, &nullableCount, &nullablePercent); err != nil {
		t.Fatalf("execute missing/null rare: %v\nSQL: %s\nargs: %#v", err, nullableRare.SQL, nullableRare.Args)
	}
	if nullableKey != "alpha" || nullableCount != 2 || nullablePercent != 100 {
		t.Fatalf("missing/null rare = %q/%d/%v, want alpha/2/100", nullableKey, nullableCount, nullablePercent)
	}

	emptyTop := compileIntegrationSPL(t, `index=compiler | top empty_value`, indexTime.Add(10*time.Second), visibilityCutoff)
	var emptyKey string
	var emptyValueCount uint64
	var emptyPercent float64
	if err := connection.QueryRow(ctx, emptyTop.SQL, emptyTop.Args...).Scan(&emptyKey, &emptyValueCount, &emptyPercent); err != nil {
		t.Fatalf("execute empty-string top: %v", err)
	}
	if emptyKey != "" || emptyValueCount != 1 || emptyPercent != 100 {
		t.Fatalf("empty-string top = %q/%d/%v, want empty/1/100", emptyKey, emptyValueCount, emptyPercent)
	}

	missingTop := compileIntegrationSPL(t, `index=compiler | top absent`, indexTime.Add(10*time.Second), visibilityCutoff)
	if err := executeCompiledExpectingNoRows(ctx, connection, missingTop); err != nil {
		t.Fatalf("execute all-missing top: %v\nSQL: %s\nargs: %#v", err, missingTop.SQL, missingTop.Args)
	}

	projectedTop := compileIntegrationSPL(t, `index=compiler | fields host | top category`, indexTime.Add(10*time.Second), visibilityCutoff)
	if err := executeCompiledExpectingNoRows(ctx, connection, projectedTop); err != nil {
		t.Fatalf("execute projected-away top: %v\nSQL: %s\nargs: %#v", err, projectedTop.SQL, projectedTop.Args)
	}

	unsupportedTop := compileIntegrationSPL(t, `index=compiler | top mixed_by`, indexTime.Add(10*time.Second), visibilityCutoff)
	unsupportedTopErr := executeCompiledExpectingNoRows(ctx, connection, unsupportedTop)
	var unsupportedTopException *clickhousedriver.Exception
	if !errors.As(unsupportedTopErr, &unsupportedTopException) || unsupportedTopException.Code != 395 ||
		!strings.Contains(unsupportedTopException.Message, UnsupportedStatsByValueMarker) {
		t.Fatalf("non-scalar top error = %v, want guarded ClickHouse exception", unsupportedTopErr)
	}

	numericGroupSort := compileIntegrationSPL(t,
		`index=compiler | stats count by sort_value | sort sort_value`,
		indexTime.Add(10*time.Second), visibilityCutoff,
	)
	numericGroupRows, err := connection.Query(ctx, numericGroupSort.SQL, numericGroupSort.Args...)
	if err != nil {
		t.Fatalf("execute numeric-aware stats group sort: %v\nSQL: %s\nargs: %#v", err, numericGroupSort.SQL, numericGroupSort.Args)
	}
	var numericGroupKeys []string
	for numericGroupRows.Next() {
		var key string
		var count uint64
		if err := numericGroupRows.Scan(&key, &count); err != nil {
			_ = numericGroupRows.Close()
			t.Fatalf("scan numeric-aware stats group sort: %v", err)
		}
		if count != 1 {
			_ = numericGroupRows.Close()
			t.Fatalf("numeric-aware stats group %q count = %d, want 1", key, count)
		}
		numericGroupKeys = append(numericGroupKeys, key)
	}
	if err := numericGroupRows.Err(); err != nil {
		_ = numericGroupRows.Close()
		t.Fatalf("iterate numeric-aware stats group sort: %v", err)
	}
	if err := numericGroupRows.Close(); err != nil {
		t.Fatalf("close numeric-aware stats group sort: %v", err)
	}
	if strings.Join(numericGroupKeys, ",") != "2,10,text" {
		t.Fatalf("numeric-aware stats group order = %v, want [2 10 text]", numericGroupKeys)
	}

	wideSort := compileIntegrationSPL(t,
		`index=compiler wide_sort=* | sort wide_sort | table event_id`,
		indexTime.Add(10*time.Second), visibilityCutoff,
	)
	wideSortRows, err := connection.Query(ctx, wideSort.SQL, wideSort.Args...)
	if err != nil {
		t.Fatalf("execute exact wide-integer sort: %v\nSQL: %s\nargs: %#v", err, wideSort.SQL, wideSort.Args)
	}
	var wideSortIDs []string
	for wideSortRows.Next() {
		var eventID string
		if err := wideSortRows.Scan(&eventID); err != nil {
			_ = wideSortRows.Close()
			t.Fatalf("scan exact wide-integer sort: %v", err)
		}
		wideSortIDs = append(wideSortIDs, eventID)
	}
	if err := wideSortRows.Err(); err != nil {
		_ = wideSortRows.Close()
		t.Fatalf("iterate exact wide-integer sort: %v", err)
	}
	if err := wideSortRows.Close(); err != nil {
		t.Fatalf("close exact wide-integer sort: %v", err)
	}
	if !slices.Equal(wideSortIDs, []string{"n-one", "n-two"}) {
		t.Fatalf("exact wide-integer sort order = %v, want [n-one n-two]", wideSortIDs)
	}

	downstream := compileIntegrationSPL(t,
		`index=compiler | stats count AS events by status | search events>1 | sort -events | head 1 | table status, events`,
		indexTime.Add(10*time.Second), visibilityCutoff,
	)
	downstreamRows, err := connection.Query(ctx, downstream.SQL, downstream.Args...)
	if err != nil {
		t.Fatalf("execute post-stats pipeline: %v\nSQL: %s\nargs: %#v", err, downstream.SQL, downstream.Args)
	}
	if !downstreamRows.Next() {
		_ = downstreamRows.Close()
		t.Fatalf("post-stats pipeline returned no row: %v", downstreamRows.Err())
	}
	var downstreamStatus string
	var downstreamCount uint64
	if err := downstreamRows.Scan(&downstreamStatus, &downstreamCount); err != nil {
		_ = downstreamRows.Close()
		t.Fatalf("scan post-stats pipeline: %v", err)
	}
	if downstreamStatus != "500" || downstreamCount != 2 || downstreamRows.Next() {
		_ = downstreamRows.Close()
		t.Fatalf("post-stats pipeline = %q/%d, want 500/2", downstreamStatus, downstreamCount)
	}
	if err := downstreamRows.Err(); err != nil {
		_ = downstreamRows.Close()
		t.Fatalf("iterate post-stats pipeline: %v", err)
	}
	if err := downstreamRows.Close(); err != nil {
		t.Fatalf("close post-stats pipeline: %v", err)
	}

	globalTable := compileIntegrationSPL(t, `index=compiler | stats count | table count`, indexTime.Add(10*time.Second), visibilityCutoff)
	var globalTableCount uint64
	if err := connection.QueryRow(ctx, globalTable.SQL, globalTable.Args...).Scan(&globalTableCount); err != nil {
		t.Fatalf("execute projected global stats: %v\nSQL: %s\nargs: %#v", err, globalTable.SQL, globalTable.Args)
	}
	if globalTableCount != 4 {
		t.Fatalf("projected global stats count = %d, want 4", globalTableCount)
	}

	deterministic := compileIntegrationSPL(t,
		`index=compiler | stats count by host | sort -count | head 2`,
		indexTime.Add(10*time.Second), visibilityCutoff,
	)
	deterministicRows, err := connection.Query(ctx, deterministic.SQL, deterministic.Args...)
	if err != nil {
		t.Fatalf("execute deterministic post-stats sort: %v\nSQL: %s\nargs: %#v", err, deterministic.SQL, deterministic.Args)
	}
	var deterministicHosts []string
	for deterministicRows.Next() {
		var host string
		var count uint64
		if err := deterministicRows.Scan(&host, &count); err != nil {
			_ = deterministicRows.Close()
			t.Fatalf("scan deterministic post-stats sort: %v", err)
		}
		if count != 1 {
			_ = deterministicRows.Close()
			t.Fatalf("deterministic group %q count = %d, want 1", host, count)
		}
		deterministicHosts = append(deterministicHosts, host)
	}
	if err := deterministicRows.Err(); err != nil {
		_ = deterministicRows.Close()
		t.Fatalf("iterate deterministic post-stats sort: %v", err)
	}
	if err := deterministicRows.Close(); err != nil {
		t.Fatalf("close deterministic post-stats sort: %v", err)
	}
	if strings.Join(deterministicHosts, ",") != "500,api" {
		t.Fatalf("deterministic post-stats hosts = %v, want [500 api]", deterministicHosts)
	}

	hiddenUnsupported := compileIntegrationSPL(t,
		`index=compiler | stats count by mixed_by | search count>100 | head 1`,
		indexTime.Add(10*time.Second), visibilityCutoff,
	)
	hiddenUnsupportedErr := executeCompiledExpectingNoRows(ctx, connection, hiddenUnsupported)
	var hiddenUnsupportedException *clickhousedriver.Exception
	if !errors.As(hiddenUnsupportedErr, &hiddenUnsupportedException) ||
		hiddenUnsupportedException.Code != 395 ||
		!strings.Contains(hiddenUnsupportedException.Message, UnsupportedStatsByValueMarker) {
		t.Fatalf("post-stats filter hid unsupported BY value: %v", hiddenUnsupportedErr)
	}

	for field, wantKey := range map[string]string{
		"bytes_value":     "AAEC/w",
		"timestamp_value": extendedTimestamp.Format(time.RFC3339Nano),
		"duration_value":  "3:4",
		"decimal_value":   "123.4500",
	} {
		source := `index=compiler | stats count by ` + field
		extendedStats := compileIntegrationSPL(t, source, indexTime.Add(10*time.Second), visibilityCutoff)
		extendedRows, queryErr := connection.Query(ctx, extendedStats.SQL, extendedStats.Args...)
		if queryErr != nil {
			t.Fatalf("execute extended scalar stats %q: %v\nSQL: %s\nargs: %#v", field, queryErr, extendedStats.SQL, extendedStats.Args)
		}
		if !extendedRows.Next() {
			_ = extendedRows.Close()
			t.Fatalf("extended scalar stats %q returned no row: %v", field, extendedRows.Err())
		}
		var key string
		var count uint64
		if scanErr := extendedRows.Scan(&key, &count); scanErr != nil {
			_ = extendedRows.Close()
			t.Fatalf("scan extended scalar stats %q: %v", field, scanErr)
		}
		if key != wantKey || count != 1 || extendedRows.Next() {
			_ = extendedRows.Close()
			t.Fatalf("extended scalar stats %q = %q/%d, want %q/1", field, key, count, wantKey)
		}
		if iterationErr := extendedRows.Err(); iterationErr != nil {
			_ = extendedRows.Close()
			t.Fatalf("iterate extended scalar stats %q: %v", field, iterationErr)
		}
		if closeErr := extendedRows.Close(); closeErr != nil {
			t.Fatalf("close extended scalar stats %q: %v", field, closeErr)
		}
	}

	tenantScoped := compileIntegrationSPL(t, `index=compiler | stats count by tenant_probe`, indexTime.Add(10*time.Second), visibilityCutoff)
	tenantRows, err := connection.Query(ctx, tenantScoped.SQL, tenantScoped.Args...)
	if err != nil {
		t.Fatalf("execute tenant-scoped stats: %v\nSQL: %s\nargs: %#v", err, tenantScoped.SQL, tenantScoped.Args)
	}
	defer tenantRows.Close()
	if !tenantRows.Next() {
		t.Fatalf("tenant-scoped stats returned no row: %v", tenantRows.Err())
	}
	var tenantKey string
	var tenantCount uint64
	if err := tenantRows.Scan(&tenantKey, &tenantCount); err != nil {
		t.Fatalf("scan tenant-scoped stats: %v", err)
	}
	if tenantKey != "visible" || tenantCount != 1 || tenantRows.Next() {
		t.Fatalf("tenant-scoped stats = %q/%d, want visible/1", tenantKey, tenantCount)
	}
	if err := tenantRows.Err(); err != nil {
		t.Fatalf("iterate tenant-scoped stats: %v", err)
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

	for _, source := range []string{
		`index=compiler | fields host | stats count by status`,
		`index=compiler | table host | stats count by status`,
		`index=compiler | fields - status | stats count by status`,
	} {
		projected := compileIntegrationSPL(t, source, indexTime.Add(10*time.Second), visibilityCutoff)
		projectedRows, queryErr := connection.Query(ctx, projected.SQL, projected.Args...)
		if queryErr != nil {
			t.Fatalf("execute projected stats %q: %v\nSQL: %s\nargs: %#v", source, queryErr, projected.SQL, projected.Args)
		}
		if projectedRows.Next() {
			_ = projectedRows.Close()
			t.Fatalf("projected-away stats field emitted a group for %q", source)
		}
		if iterationErr := projectedRows.Err(); iterationErr != nil {
			_ = projectedRows.Close()
			t.Fatalf("iterate projected stats %q: %v", source, iterationErr)
		}
		if closeErr := projectedRows.Close(); closeErr != nil {
			t.Fatalf("close projected stats %q: %v", source, closeErr)
		}
	}

	retained := compileIntegrationSPL(t, `index=compiler | fields status | stats count AS events by status`, indexTime.Add(10*time.Second), visibilityCutoff)
	retainedRows, err := connection.Query(ctx, retained.SQL, retained.Args...)
	if err != nil {
		t.Fatalf("execute explicitly retained stats: %v\nSQL: %s\nargs: %#v", err, retained.SQL, retained.Args)
	}
	defer retainedRows.Close()
	if !retainedRows.Next() {
		t.Fatalf("explicitly retained stats returned no group: %v", retainedRows.Err())
	}
	var retainedKey string
	var retainedCount uint64
	if err := retainedRows.Scan(&retainedKey, &retainedCount); err != nil {
		t.Fatalf("scan explicitly retained stats: %v", err)
	}
	if retainedKey != "500" || retainedCount != 2 || retainedRows.Next() {
		t.Fatalf("explicitly retained stats = %q/%d, want 500/2", retainedKey, retainedCount)
	}
	if err := retainedRows.Err(); err != nil {
		t.Fatalf("iterate explicitly retained stats: %v", err)
	}

	for _, alias := range []string{"fields", "_raw"} {
		aliased := compileIntegrationSPL(t, `index=compiler | stats count AS `+alias, indexTime.Add(10*time.Second), visibilityCutoff)
		aliasedRows, queryErr := connection.Query(ctx, aliased.SQL, aliased.Args...)
		if queryErr != nil {
			t.Fatalf("execute stats alias %q: %v\nSQL: %s\nargs: %#v", alias, queryErr, aliased.SQL, aliased.Args)
		}
		if types := aliasedRows.ColumnTypes(); len(types) != 1 || types[0].DatabaseTypeName() != "UInt64" {
			_ = aliasedRows.Close()
			t.Fatalf("stats alias %q types = %#v, want UInt64", alias, types)
		}
		if !aliasedRows.Next() {
			_ = aliasedRows.Close()
			t.Fatalf("stats alias %q returned no row: %v", alias, aliasedRows.Err())
		}
		var count uint64
		if scanErr := aliasedRows.Scan(&count); scanErr != nil {
			_ = aliasedRows.Close()
			t.Fatalf("scan stats alias %q: %v", alias, scanErr)
		}
		if count != 4 || aliasedRows.Next() {
			_ = aliasedRows.Close()
			t.Fatalf("stats alias %q count = %d, want 4", alias, count)
		}
		if iterationErr := aliasedRows.Err(); iterationErr != nil {
			_ = aliasedRows.Close()
			t.Fatalf("iterate stats alias %q: %v", alias, iterationErr)
		}
		if closeErr := aliasedRows.Close(); closeErr != nil {
			t.Fatalf("close stats alias %q: %v", alias, closeErr)
		}
	}

	for _, field := range []string{"multi", "object_value", "object_parent", "mixed_by"} {
		source := `index=compiler | stats count by ` + field
		unsupported := compileIntegrationSPL(t, source, indexTime.Add(10*time.Second), visibilityCutoff)
		queryErr := executeCompiledExpectingNoRows(ctx, connection, unsupported)
		var exception *clickhousedriver.Exception
		if !errors.As(queryErr, &exception) || exception.Code != 395 || !strings.Contains(exception.Message, UnsupportedStatsByValueMarker) {
			t.Fatalf("non-scalar stats BY %q error = %v, want guarded ClickHouse exception", field, queryErr)
		}
	}

	for _, test := range []struct {
		name   string
		source string
		marker string
	}{
		{
			name:   "direct copy into dedup",
			source: `index=compiler | eval copied=object_parent | dedup copied | head 1`,
			marker: UnsupportedDedupValueMarker,
		},
		{
			name:   "chained copy into stats",
			source: `index=compiler | eval first=object_parent, copied=first | stats count BY copied | search count>100 | head 1`,
			marker: UnsupportedStatsByValueMarker,
		},
		{
			name:   "multi-key stats cannot hide container behind missing sibling",
			source: `index=compiler | eval copied=object_parent | stats count BY copied, absent | head 1`,
			marker: UnsupportedStatsByValueMarker,
		},
	} {
		unsupported := compileIntegrationSPL(t, test.source, indexTime.Add(10*time.Second), visibilityCutoff)
		queryErr := executeCompiledExpectingNoRows(ctx, connection, unsupported)
		var exception *clickhousedriver.Exception
		if !errors.As(queryErr, &exception) || exception.Code != 395 || !strings.Contains(exception.Message, test.marker) {
			t.Fatalf("%s error = %v, want guarded ClickHouse exception containing %q", test.name, queryErr, test.marker)
		}
	}

	ordinaryEval := compileIntegrationSPL(t,
		`index=compiler event_id=n-complex | eval copied="ordinary" | dedup copied | table event_id`,
		indexTime.Add(10*time.Second), visibilityCutoff,
	)
	var ordinaryEventID string
	if err := connection.QueryRow(ctx, ordinaryEval.SQL, ordinaryEval.Args...).Scan(&ordinaryEventID); err != nil {
		t.Fatalf("execute ordinary scalar eval through dedup: %v\nSQL: %s\nargs: %#v", err, ordinaryEval.SQL, ordinaryEval.Args)
	}
	if ordinaryEventID != "n-complex" {
		t.Fatalf("ordinary scalar eval event ID = %q, want n-complex", ordinaryEventID)
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
	if err := rows.Close(); err != nil {
		t.Fatalf("close tail rows: %v", err)
	}

	timeBin := compileIntegrationSPL(t,
		`index=compiler | bucket span=5m _time | stats count BY _time`,
		indexTime.Add(10*time.Second),
		visibilityCutoff,
	)
	var bucketStart time.Time
	var bucketCount uint64
	if err := connection.QueryRow(ctx, timeBin.SQL, timeBin.Args...).Scan(&bucketStart, &bucketCount); err != nil {
		t.Fatalf("execute time bin: %v\nSQL: %s\nargs: %#v", err, timeBin.SQL, timeBin.Args)
	}
	wantBucketStart := time.Date(2026, 7, 20, 22, 0, 0, 0, time.UTC)
	if !bucketStart.Equal(wantBucketStart) || bucketStart.Location() != time.UTC || bucketCount != 4 {
		t.Fatalf("time bin = %v/%d, want %v/4", bucketStart, bucketCount, wantBucketStart)
	}

	timeBinAnalysisPlan := buildIntegrationPlan(
		t,
		`index=compiler event_id=n-one | bin _time span=5m | table _time`,
		indexTime.Add(10*time.Second),
		visibilityCutoff,
	)
	timeBinCatalog, err := (Compiler{}).CompileFieldCatalog(
		timeBinAnalysisPlan,
		FieldCatalogSpec{MaximumFields: 10},
	)
	if err != nil {
		t.Fatalf("compile time-bin field catalog: %v", err)
	}
	timeBinCatalogControl := "SELECT count(), countIf(" +
		quoteIdentifier(FieldCatalogRowKindColumn) + " = 1 AND " +
		quoteIdentifier(FieldCatalogNameColumn) + " = '_time' AND has(" +
		quoteIdentifier(FieldCatalogObservedTypesColumn) + ", toUInt8(" +
		fmt.Sprint(uint8(eventfields.StoredValueTypeTimestamp)) + ")) AND " +
		quoteIdentifier(FieldCatalogEventCountColumn) + " = 1 AND " +
		quoteIdentifier(FieldCatalogNullCountColumn) + " = 0 AND " +
		quoteIdentifier(FieldCatalogMissingCountColumn) + " = 0), max(" +
		quoteIdentifier(FieldCatalogInvalidColumn) + ") FROM (" + timeBinCatalog.SQL + ")"
	var timeBinCatalogRows, timeBinTimestampProfiles uint64
	var timeBinCatalogInvalid uint8
	if err := connection.QueryRow(ctx, timeBinCatalogControl, timeBinCatalog.Args...).Scan(
		&timeBinCatalogRows,
		&timeBinTimestampProfiles,
		&timeBinCatalogInvalid,
	); err != nil {
		t.Fatalf("execute time-bin field catalog: %v\nSQL: %s\nargs: %#v", err, timeBinCatalog.SQL, timeBinCatalog.Args)
	}
	if timeBinCatalogRows != 2 || timeBinTimestampProfiles != 1 || timeBinCatalogInvalid != 0 {
		t.Fatalf(
			"time-bin field catalog = rows:%d timestamp_profiles:%d invalid:%d, want 2/1/0",
			timeBinCatalogRows,
			timeBinTimestampProfiles,
			timeBinCatalogInvalid,
		)
	}

	if err := connection.Exec(ctx, `
		INSERT INTO open_splunk.events
			(event_id, tenant_id, index_name, event_time, index_time, expires_at, visibility_seq)
		SELECT ?, ?, ?,
			parseDateTime64BestEffort(?, 9, 'UTC'),
			parseDateTime64BestEffort(?, 3, 'UTC'),
			parseDateTime64BestEffort(?, 3, 'UTC'),
			toUInt64(?)`,
		"bin-pre-epoch", "tenant", "compiler",
		"1969-12-31 23:59:59.999999999",
		indexTime.UTC().Format("2006-01-02 15:04:05.000"),
		"2099-01-01 00:00:00.000",
		uint64(1),
	); err != nil {
		t.Fatalf("insert pre-epoch time-bin fixture: %v", err)
	}
	parsed, err := spl.Parse(`index=compiler event_id=bin-pre-epoch | bin _time span=5m | table _time`)
	if err != nil {
		t.Fatalf("parse pre-epoch time bin: %v", err)
	}
	preEpochPlan, err := plan.Build(parsed, plan.Scope{
		TenantID:          "tenant",
		AuthorizedIndexes: []string{"compiler"},
		Earliest:          time.Date(1969, 12, 31, 23, 55, 0, 0, time.UTC),
		Latest:            time.Date(1970, 1, 1, 0, 0, 0, 1, time.UTC),
		IndexTimeCutoff:   indexTime.Add(10 * time.Second),
		VisibilityCutoff:  uint64PointerForIntegration(visibilityCutoff),
	})
	if err != nil {
		t.Fatalf("build pre-epoch time bin: %v", err)
	}
	preEpochQuery, err := (Compiler{}).Compile(preEpochPlan)
	if err != nil {
		t.Fatalf("compile pre-epoch time bin: %v", err)
	}
	var preEpochBucket time.Time
	if err := connection.QueryRow(ctx, preEpochQuery.SQL, preEpochQuery.Args...).Scan(&preEpochBucket); err != nil {
		t.Fatalf("execute pre-epoch time bin: %v\nSQL: %s\nargs: %#v", err, preEpochQuery.SQL, preEpochQuery.Args)
	}
	wantPreEpoch := time.Date(1969, 12, 31, 23, 55, 0, 0, time.UTC)
	if !preEpochBucket.Equal(wantPreEpoch) {
		t.Fatalf("pre-epoch time bin = %v, want %v", preEpochBucket, wantPreEpoch)
	}

	testNumericBinAgainstClickHouse(t, ctx, connection, indexTime, visibilityCutoff)
	testStatsSumAndAverageAgainstClickHouse(t, ctx, store, connection, indexTime)
	testDedupAgainstClickHouse(t, ctx, store, connection, indexTime)
	testRexAgainstClickHouse(t, ctx, store, connection, indexTime)
}

func testNumericBinAgainstClickHouse(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	indexTime time.Time,
	visibilityCutoff uint64,
) {
	t.Helper()

	t.Run("numeric bin", func(t *testing.T) {
		scalars := compileIntegrationSPL(
			t,
			`index=compiler event_id=n-one
| eval signed=-11,unsigned=18446744073709551615,latency=-11.5,nullable=tonumber("bad")
| bin signed span=10 AS signed_band
| bin unsigned span=7 AS unsigned_band
| bin latency span=4 AS latency_band
| bin nullable span=10 AS nullable_band
| table signed signed_band unsigned unsigned_band latency latency_band nullable nullable_band`,
			indexTime.Add(10*time.Second),
			visibilityCutoff,
		)
		var signedSource, signedBand int64
		var unsignedSource, unsignedBand uint64
		var floatingSource, floatingBand float64
		var nullableSource, nullableBand *float64
		if err := connection.QueryRow(ctx, scalars.SQL, scalars.Args...).Scan(
			&signedSource,
			&signedBand,
			&unsignedSource,
			&unsignedBand,
			&floatingSource,
			&floatingBand,
			&nullableSource,
			&nullableBand,
		); err != nil {
			t.Fatalf("execute scalar numeric bins: %v\nSQL: %s\nargs: %#v", err, scalars.SQL, scalars.Args)
		}
		if signedSource != -11 || signedBand != -20 ||
			unsignedSource != ^uint64(0) || unsignedBand != uint64(18_446_744_073_709_551_614) ||
			floatingSource != -11.5 || floatingBand != -12 ||
			nullableSource != nil || nullableBand != nil {
			t.Fatalf(
				"scalar numeric bins = %d/%d %d/%d %g/%g %v/%v, want -11/-20 max/max-1 -11.5/-12 nil/nil",
				signedSource,
				signedBand,
				unsignedSource,
				unsignedBand,
				floatingSource,
				floatingBand,
				nullableSource,
				nullableBand,
			)
		}

		transformed := compileIntegrationSPL(
			t,
			`index=compiler event_id=n-one | stats count | bin count span=3 AS band | table count band`,
			indexTime.Add(10*time.Second),
			visibilityCutoff,
		)
		var count, countBand uint64
		if err := connection.QueryRow(ctx, transformed.SQL, transformed.Args...).Scan(&count, &countBand); err != nil {
			t.Fatalf("execute transformed numeric bin: %v\nSQL: %s\nargs: %#v", err, transformed.SQL, transformed.Args)
		}
		if count != 1 || countBand != 0 {
			t.Fatalf("transformed numeric bin = %d/%d, want 1/0", count, countBand)
		}

		underflow := compileIntegrationSPL(
			t,
			`index=compiler event_id=n-one | eval signed=-9223372036854775808 | bin signed span=10 | table signed`,
			indexTime.Add(10*time.Second),
			visibilityCutoff,
		)
		queryErr := executeCompiledExpectingNoRows(ctx, connection, underflow)
		var exception *clickhousedriver.Exception
		if !errors.As(queryErr, &exception) ||
			exception.Code != 395 ||
			!strings.Contains(exception.Message, UnsupportedNumericBinValueMarker) {
			t.Fatalf("numeric-bin underflow error = %v, want guarded ClickHouse exception", queryErr)
		}
	})
}

func testRexAgainstClickHouse(
	t *testing.T,
	ctx context.Context,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()
	newEvent := func(id, raw string, fields ...*opensplunkv1.TypedObjectField) *ingest.StoredEvent {
		event := compilerIntegrationEvent(id, "rex-host", raw, indexTime, fields...)
		event.BatchID = "rex-batch"
		event.Event.Source = "rex-fixture"
		return event
	}
	events := []*ingest.StoredEvent{
		newEvent(
			"rex-match",
			"method=POST path=/api/v1/search/jobs status=500 duration=600ms",
			typedField("duration_text", typedString("527.182µs")),
			typedField("target", typedString("old-match")),
			typedField("optional_path", typedString("/api/v1/jobs")),
			typedField("numeric_source", typedSint(500)),
			typedField("numeric_target", typedSint(42)),
			typedField("binary_source", typedBytes([]byte{0, 255, 16})),
			typedField("list_source", typedList(typedString("x"))),
			typedField("object_source", typedObject(typedField("child", typedString("x")))),
			typedField("object_target", typedObject(typedField("child", typedString("keep")))),
		),
		newEvent(
			"rex-no-match",
			"prefix\nsuffix",
			typedField("duration_text", typedString("bad")),
			typedField("target", typedString("keep-me")),
			typedField("area", typedString("keep-area")),
			typedField("null_source", typedNull()),
			typedField("nullable_target", typedNull()),
		),
		newEvent(
			"rex-invalid-utf8",
			string([]byte{'p', 'r', 'e', 'f', 'i', 'x', 0xff, 's', 'u', 'f', 'f', 'i', 'x'}),
			typedField("target", typedString("keep-invalid")),
		),
		newEvent(
			"rex-large",
			strings.Repeat("a", 768<<10)+" status=503",
			typedField("target", typedString("keep-large")),
		),
		newEvent("rex-trailing-newline", "status=200\n"),
	}
	batch := ingest.StoreBatch{
		TenantID: "tenant", CollectorID: "collector", BatchID: "rex-batch", BatchSequence: 50,
		SourceBatchSHA256: testSourceBatchDigest("rex-batch"),
		ReceivedAt:        indexTime,
		Events:            events,
	}
	if _, err := store.Store(ctx, batch); err != nil {
		t.Fatalf("store rex fixtures: %v", err)
	}
	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture rex visibility cutoff: %v", err)
	}
	count := func(source string) uint64 {
		t.Helper()
		compiled := compileIntegrationSPL(t, source, indexTime.Add(10*time.Second), visibilityCutoff)
		var got uint64
		if err := connection.QueryRow(ctx, compiled.SQL, compiled.Args...).Scan(&got); err != nil {
			t.Fatalf("execute rex SPL %q: %v\nSQL: %s\nargs: %#v", source, err, compiled.SQL, compiled.Args)
		}
		return got
	}
	base := `index=compiler source="rex-fixture"`
	explained := compileIntegrationSPL(
		t,
		base+` | rex "method=(?<method>[A-Z]+)\s+path=(?<path>\S+)" | table method, path`,
		indexTime.Add(10*time.Second),
		visibilityCutoff,
	)
	explainRows, err := connection.Query(ctx, "EXPLAIN actions=1 "+explained.SQL, explained.Args...)
	if err != nil {
		t.Fatalf("explain rex execution: %v", err)
	}
	var explain strings.Builder
	for explainRows.Next() {
		var line string
		if err := explainRows.Scan(&line); err != nil {
			t.Fatalf("scan rex explain: %v", err)
		}
		explain.WriteString(line)
		explain.WriteByte('\n')
	}
	if err := explainRows.Err(); err != nil {
		t.Fatalf("iterate rex explain: %v", err)
	}
	if err := explainRows.Close(); err != nil {
		t.Fatalf("close rex explain: %v", err)
	}
	if calls := strings.Count(explain.String(), "FUNCTION extractGroups("); calls != 1 {
		t.Fatalf("rex physical explain contains %d extractGroups actions, want 1:\n%s", calls, explain.String())
	}

	tests := []struct {
		name   string
		source string
		want   uint64
	}{
		{
			name: "default raw simultaneous captures",
			source: base + ` | rex "method=(?<method>[A-Z]+)\s+path=(?<path>\S+)\s+status=(?<status>\d+)"` +
				` | where method="POST" AND path="/api/v1/search/jobs" AND status="500" | stats count`,
			want: 1,
		},
		{
			name: "unicode explicit source",
			source: base + ` | rex field=duration_text "^(?<duration_value>\d+(?:\.\d+)?)(?<duration_unit>µs|ms)$"` +
				` | where duration_value="527.182" AND duration_unit="µs" | stats count`,
			want: 1,
		},
		{
			name:   "new destination remains missing on no match",
			source: base + ` | rex field=duration_text "^(?<duration_value>\d+)" | search duration_value=* | stats count`,
			want:   1,
		},
		{
			name:   "existing destination preserved on no match",
			source: base + ` | rex "method=(?<target>[A-Z]+)" | where target="keep-me" | stats count`,
			want:   1,
		},
		{
			name:   "matching destination overwritten",
			source: base + ` | rex "method=(?<target>[A-Z]+)" | where target="POST" | stats count`,
			want:   1,
		},
		{
			name:   "invalid utf8 source is no match",
			source: base + ` event_id=rex-invalid-utf8 | rex "prefix(?<target>.)suffix" | where target="keep-invalid" | stats count`,
			want:   1,
		},
		{
			name:   "numeric source is no match",
			source: base + ` event_id=rex-match | rex field=numeric_source "^(?<target>\d+)$" | where target="old-match" | stats count`,
			want:   1,
		},
		{
			name: "explicit null destination remains present",
			source: base + ` event_id=rex-no-match | rex field=duration_text "^(?<nullable_target>\d+)$"` +
				` | search nullable_target=null | stats count`,
			want: 1,
		},
		{
			name: "missing destination remains missing",
			source: base + ` event_id=rex-large | rex field=duration_text "^(?<fresh_missing>\d+)$"` +
				` | search fresh_missing=* | stats count`,
			want: 0,
		},
		{
			name: "optional unmatched capture is present empty string",
			source: base + ` event_id=rex-match | rex field=optional_path "^/api/v1/(?<area>[^/?]+)(?:/(?<resource>[^/?]+))?"` +
				` | where area="jobs" AND resource="" | stats count`,
			want: 1,
		},
		{
			name:   "dot does not cross newline by default",
			source: base + ` event_id=rex-no-match | rex "prefix(?<gap>.)suffix" | search gap=* | stats count`,
			want:   0,
		},
		{
			name:   "explicit dotall crosses newline",
			source: base + ` event_id=rex-no-match | rex "(?s)prefix(?<gap>.)suffix" | search gap=* | stats count`,
			want:   1,
		},
		{
			name:   "large tail match remains linear and bounded",
			source: base + ` event_id=rex-large | rex "status=(?<status>\d{3})$" | where status="503" | stats count`,
			want:   1,
		},
		{
			name:   "source output collision reads old source",
			source: base + ` event_id=rex-match | rex field=duration_text "^(?<duration_text>\d+)" | where duration_text="527" | stats count`,
			want:   1,
		},
		{
			name:   "explicit null source is no match",
			source: base + ` event_id=rex-no-match | rex field=null_source "(?<captured>x)" | search captured=* | stats count`,
			want:   0,
		},
		{
			name:   "binary source is no match",
			source: base + ` event_id=rex-match | rex field=binary_source "(?<captured>.)" | search captured=* | stats count`,
			want:   0,
		},
		{
			name:   "list source is no match",
			source: base + ` event_id=rex-match | rex field=list_source "(?<captured>.)" | search captured=* | stats count`,
			want:   0,
		},
		{
			name:   "object source is no match",
			source: base + ` event_id=rex-match | rex field=object_source "(?<captured>.)" | search captured=* | stats count`,
			want:   0,
		},
		{
			name:   "zero width first match creates empty capture",
			source: base + ` event_id=rex-match | rex field=duration_text "^(?<empty>)" | where empty="" | stats count`,
			want:   1,
		},
		{
			name:   "strict end anchor does not match before trailing newline",
			source: base + ` event_id=rex-trailing-newline | rex "status=(?<strict_status>\d+)$" | search strict_status=* | stats count`,
			want:   0,
		},
		{
			name:   "multiline end anchor matches before trailing newline",
			source: base + ` event_id=rex-trailing-newline | rex "(?m)status=(?<line_status>\d+)$" | where line_status="200" | stats count`,
			want:   1,
		},
		{
			name: "consecutive rex stages consume the current pipeline value",
			source: base + ` event_id=rex-match | rex field=duration_text "^(?<first>\d+)"` +
				` | rex field=first "^(?<second>\d{3})$" | where first="527" AND second="527" | stats count`,
			want: 1,
		},
		{
			name: "mixed numeric destination is preserved on no match",
			source: base + ` event_id=rex-match | rex field=duration_text "^never(?<numeric_target>x)$"` +
				` | where numeric_target=42 | stats count`,
			want: 1,
		},
		{
			name: "mixed numeric destination is overwritten by a string",
			source: base + ` event_id=rex-match | rex field=duration_text "^(?<numeric_target>\d+)"` +
				` | where numeric_target="527" | stats count`,
			want: 1,
		},
		{
			name: "object parent destination remains present on no match",
			source: base + ` event_id=rex-match | rex field=duration_text "^never(?<object_target>x)$"` +
				` | search object_target=* | stats count`,
			want: 1,
		},
		{
			name: "object destination descendants remain available on no match",
			source: base + ` event_id=rex-match | rex field=duration_text "^never(?<object_target>x)$"` +
				` | where object_target.child="keep" | stats count`,
			want: 1,
		},
	}
	for _, test := range tests {
		t.Run("rex "+test.name, func(t *testing.T) {
			if got := count(test.source); got != test.want {
				t.Fatalf("count = %d, want %d", got, test.want)
			}
		})
	}

	oversizedCaptures := compileIntegrationSPL(
		t,
		base+` event_id=rex-large | rex "^(?<a>(?<b>(?<c>(?<d>(?<e>(?<f>.*))))))$"`+
			` | where a!="" | stats count`,
		indexTime.Add(10*time.Second),
		visibilityCutoff,
	)
	var ignoredCount uint64
	captureLimitErr := connection.QueryRow(
		ctx,
		oversizedCaptures.SQL,
		oversizedCaptures.Args...,
	).Scan(&ignoredCount)
	var captureLimitException *clickhousedriver.Exception
	if !errors.As(captureLimitErr, &captureLimitException) || captureLimitException.Code != 395 ||
		!strings.Contains(captureLimitException.Message, RexCaptureLimitMarker) {
		t.Fatalf("oversized rex captures error = %#v, want guarded code-395 marker", captureLimitErr)
	}

	analysisPlan := buildIntegrationPlan(
		t,
		base+` | rex field=duration_text "^(?<duration_value>\d+)" | table duration_value`,
		indexTime.Add(10*time.Second),
		visibilityCutoff,
	)
	catalog, err := (Compiler{}).CompileFieldCatalog(analysisPlan, FieldCatalogSpec{MaximumFields: 10})
	if err != nil {
		t.Fatalf("compile rex field catalog: %v", err)
	}
	var catalogRows uint64
	if err := connection.QueryRow(ctx, "SELECT count() FROM ("+catalog.SQL+")", catalog.Args...).Scan(&catalogRows); err != nil {
		t.Fatalf("execute rex field catalog: %v\nSQL: %s\nargs: %#v", err, catalog.SQL, catalog.Args)
	}
	if catalogRows != 2 {
		t.Fatalf("rex field catalog rows = %d, want header plus one field", catalogRows)
	}

	summary, err := (Compiler{}).CompileFieldSummary(analysisPlan, FieldSummarySpec{
		FieldName:             "duration_value",
		MaximumValues:         10,
		MaximumDistinctValues: 100,
		MaximumValueBytes:     4_096,
	})
	if err != nil {
		t.Fatalf("compile rex field summary: %v", err)
	}
	var summaryRows uint64
	if err := connection.QueryRow(ctx, "SELECT count() FROM ("+summary.SQL+")", summary.Args...).Scan(&summaryRows); err != nil {
		t.Fatalf("execute rex field summary: %v\nSQL: %s\nargs: %#v", err, summary.SQL, summary.Args)
	}
	if summaryRows != 2 {
		t.Fatalf("rex field summary rows = %d, want header plus one value", summaryRows)
	}

	mixedPlan := buildIntegrationPlan(
		t,
		base+` | rex field=duration_text "^(?<severity>\d+)" | table severity`,
		indexTime.Add(10*time.Second),
		visibilityCutoff,
	)
	mixedSummary, err := (Compiler{}).CompileFieldSummary(mixedPlan, FieldSummarySpec{
		FieldName:             "severity",
		MaximumValues:         10,
		MaximumDistinctValues: 100,
		MaximumValueBytes:     4_096,
	})
	if err != nil {
		t.Fatalf("compile mixed rex field summary: %v", err)
	}
	mixedSummaryControlSQL := "SELECT count(), max(" + quoteIdentifier(FieldSummaryMetadataInvalidColumn) +
		"), max(" + quoteIdentifier(FieldSummaryUnsupportedColumn) + "), countIf(" +
		quoteIdentifier(FieldSummaryRowKindColumn) + " = 1 AND " +
		quoteIdentifier(FieldSummaryValueTypeColumn) + " = toUInt8(" +
		fmt.Sprint(uint8(eventfields.StoredValueTypeString)) + ") AND " +
		quoteIdentifier(FieldSummaryEncodedValueColumn) + " = '527' AND " +
		quoteIdentifier(FieldSummaryValueCountColumn) + " = 1), countIf(" +
		quoteIdentifier(FieldSummaryRowKindColumn) + " = 1 AND " +
		quoteIdentifier(FieldSummaryValueTypeColumn) + " = toUInt8(" +
		fmt.Sprint(uint8(eventfields.StoredValueTypeUint64)) + ") AND " +
		quoteIdentifier(FieldSummaryEncodedValueColumn) + " = '" +
		fmt.Sprint(int32(opensplunkv1.LogSeverity_LOG_SEVERITY_INFO)) + "' AND " +
		quoteIdentifier(FieldSummaryValueCountColumn) + " = 4) FROM (" + mixedSummary.SQL + ")"
	var mixedSummaryRows, capturedProfileRows, preservedProfileRows uint64
	var metadataInvalid, unsupported uint8
	if err := connection.QueryRow(ctx, mixedSummaryControlSQL, mixedSummary.Args...).Scan(
		&mixedSummaryRows,
		&metadataInvalid,
		&unsupported,
		&capturedProfileRows,
		&preservedProfileRows,
	); err != nil {
		t.Fatalf("execute mixed rex field summary: %v\nSQL: %s\nargs: %#v", err, mixedSummary.SQL, mixedSummary.Args)
	}
	if mixedSummaryRows != 3 || metadataInvalid != 0 || unsupported != 0 ||
		capturedProfileRows != 1 || preservedProfileRows != 1 {
		t.Fatalf(
			"mixed rex field summary = rows:%d metadata_invalid:%d unsupported:%d captured:%d preserved:%d, want 3/0/0/1/1",
			mixedSummaryRows,
			metadataInvalid,
			unsupported,
			capturedProfileRows,
			preservedProfileRows,
		)
	}

	objectPlan := buildIntegrationPlan(
		t,
		base+` event_id=rex-match | rex field=duration_text "^never(?<object_target>x)$" | table object_target`,
		indexTime.Add(10*time.Second),
		visibilityCutoff,
	)
	objectCatalog, err := (Compiler{}).CompileFieldCatalog(objectPlan, FieldCatalogSpec{MaximumFields: 10})
	if err != nil {
		t.Fatalf("compile object-preserving rex field catalog: %v", err)
	}
	objectCatalogControlSQL := "SELECT countIf(" + quoteIdentifier(FieldCatalogRowKindColumn) +
		" = 1 AND " + quoteIdentifier(FieldCatalogNameColumn) + " = 'object_target' AND " +
		quoteIdentifier(FieldCatalogEventCountColumn) + " = 1 AND has(" +
		quoteIdentifier(FieldCatalogObservedTypesColumn) + ", toUInt8(" +
		fmt.Sprint(uint8(eventfields.StoredValueTypeObject)) + "))) FROM (" + objectCatalog.SQL + ")"
	var objectCatalogProfiles uint64
	if err := connection.QueryRow(ctx, objectCatalogControlSQL, objectCatalog.Args...).Scan(
		&objectCatalogProfiles,
	); err != nil {
		t.Fatalf("execute object-preserving rex field catalog: %v\nSQL: %s", err, objectCatalog.SQL)
	}
	if objectCatalogProfiles != 1 {
		t.Fatalf("object-preserving rex field catalog profiles = %d, want 1", objectCatalogProfiles)
	}

	objectSummary, err := (Compiler{}).CompileFieldSummary(objectPlan, FieldSummarySpec{
		FieldName:             "object_target",
		MaximumValues:         10,
		MaximumDistinctValues: 100,
		MaximumValueBytes:     4_096,
	})
	if err != nil {
		t.Fatalf("compile object-preserving rex field summary: %v", err)
	}
	objectSummaryControlSQL := "SELECT count(), max(" + quoteIdentifier(FieldSummaryMetadataInvalidColumn) +
		"), max(" + quoteIdentifier(FieldSummaryUnsupportedColumn) + ") FROM (" + objectSummary.SQL + ")"
	var objectSummaryRows uint64
	if err := connection.QueryRow(ctx, objectSummaryControlSQL, objectSummary.Args...).Scan(
		&objectSummaryRows,
		&metadataInvalid,
		&unsupported,
	); err != nil {
		t.Fatalf("execute object-preserving rex field summary: %v\nSQL: %s", err, objectSummary.SQL)
	}
	if objectSummaryRows != 1 || metadataInvalid != 0 || unsupported != 1 {
		t.Fatalf(
			"object-preserving rex field summary = rows:%d metadata_invalid:%d unsupported:%d, want 1/0/1",
			objectSummaryRows,
			metadataInvalid,
			unsupported,
		)
	}
}

func testStatsSumAndAverageAgainstClickHouse(
	t *testing.T,
	ctx context.Context,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()
	newEvent := func(id, group string, fields ...*opensplunkv1.TypedObjectField) *ingest.StoredEvent {
		fields = append([]*opensplunkv1.TypedObjectField{typedField("aggregate_group", typedString(group))}, fields...)
		event := compilerIntegrationEvent(id, "aggregate-host", "sum avg fixture", indexTime, fields...)
		event.BatchID = "stats-sum-avg-batch"
		event.Event.Source = "stats-sum-avg"
		return event
	}
	events := []*ingest.StoredEvent{
		newEvent("sum-avg-a-int", "A",
			typedField("aggregate_value", typedSint(10)),
			typedField("float_metric", typedDouble(1.25)),
			typedField("overflow_metric", typedDouble(math.MaxFloat64)),
			typedField("ignored_metric", typedBool(true)),
		),
		newEvent("sum-avg-a-string", "A",
			typedField("aggregate_value", typedString("20.5")),
			typedField("overflow_metric", typedDouble(math.MaxFloat64)),
			typedField("ignored_metric", typedBytes([]byte{1, 2, 3})),
		),
		newEvent("sum-avg-a-array", "A",
			typedField("aggregate_value", typedList(typedSint(1), typedString("2.5"), typedString("bad"), typedNull())),
			typedField("ignored_metric", typedObject(typedField("child", typedSint(9)))),
		),
		newEvent("sum-avg-a-missing", "A",
			typedField("decimal_metric", typedDecimal("3.25")),
			typedField("nested_metric", typedList(typedSint(5), typedList(typedSint(99)), typedObject(typedField("child", typedSint(100))))),
			typedField("ignored_metric", typedString("NaN")),
		),
		newEvent("sum-avg-b-bad", "B",
			typedField("aggregate_value", typedString("bad")),
			typedField("ignored_metric", typedString("Inf")),
		),
		newEvent("sum-avg-b-null", "B",
			typedField("aggregate_value", typedNull()),
			typedField("ignored_metric", typedString("")),
		),
	}
	batch := ingest.StoreBatch{
		TenantID: "tenant", CollectorID: "collector", BatchID: "stats-sum-avg-batch", BatchSequence: 5,
		SourceBatchSHA256: testSourceBatchDigest("stats-sum-avg-batch"),
		ReceivedAt:        indexTime,
		Events:            events,
	}
	if _, err := store.Store(ctx, batch); err != nil {
		t.Fatalf("store sum/avg fixtures: %v", err)
	}
	foreign := newEvent("sum-avg-foreign", "A", typedField("aggregate_value", typedList(typedSint(999999))))
	foreign.TenantID = "other-tenant"
	foreign.BatchID = "stats-sum-avg-foreign-batch"
	foreignBatch := ingest.StoreBatch{
		TenantID: "other-tenant", CollectorID: "collector", BatchID: "stats-sum-avg-foreign-batch", BatchSequence: 6,
		SourceBatchSHA256: testSourceBatchDigest("stats-sum-avg-foreign-batch"),
		ReceivedAt:        indexTime,
		Events:            []*ingest.StoredEvent{foreign},
	}
	if _, err := store.Store(ctx, foreignBatch); err != nil {
		t.Fatalf("store cross-tenant sum/avg fixture: %v", err)
	}
	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture sum/avg visibility cutoff: %v", err)
	}
	compile := func(source string) CompiledQuery {
		return compileIntegrationSPL(t, source, indexTime.Add(10*time.Second), visibilityCutoff)
	}
	base := `index=compiler source="stats-sum-avg"`

	grouped := compile(base + ` | stats count sum(aggregate_value) AS total avg(aggregate_value) AS mean BY aggregate_group | sort aggregate_group`)
	groupedRows, err := connection.Query(ctx, grouped.SQL, grouped.Args...)
	if err != nil {
		t.Fatalf("execute grouped sum/avg: %v\nSQL: %s\nargs: %#v", err, grouped.SQL, grouped.Args)
	}
	if types := groupedRows.ColumnTypes(); len(types) != 4 || types[0].DatabaseTypeName() != "String" ||
		types[1].DatabaseTypeName() != "UInt64" || types[2].DatabaseTypeName() != "Nullable(Float64)" ||
		types[3].DatabaseTypeName() != "Nullable(Float64)" {
		_ = groupedRows.Close()
		t.Fatalf("grouped sum/avg column types = %#v", types)
	}
	type aggregateRow struct {
		group       string
		count       uint64
		total, mean *float64
	}
	var got []aggregateRow
	for groupedRows.Next() {
		var row aggregateRow
		if err := groupedRows.Scan(&row.group, &row.count, &row.total, &row.mean); err != nil {
			_ = groupedRows.Close()
			t.Fatalf("scan grouped sum/avg: %v", err)
		}
		got = append(got, row)
	}
	if err := groupedRows.Err(); err != nil {
		_ = groupedRows.Close()
		t.Fatalf("iterate grouped sum/avg: %v", err)
	}
	if err := groupedRows.Close(); err != nil {
		t.Fatalf("close grouped sum/avg: %v", err)
	}
	if len(got) != 2 || got[0].group != "A" || got[0].count != 4 || got[0].total == nil || math.Abs(*got[0].total-34) > 1e-12 ||
		got[0].mean == nil || math.Abs(*got[0].mean-8.5) > 1e-12 || got[1].group != "B" || got[1].count != 2 ||
		got[1].total != nil || got[1].mean != nil {
		t.Fatalf("grouped sum/avg rows = %#v", got)
	}

	emptyGlobal := compile(`index=compiler source="sum-avg-absent" | stats sum(aggregate_value) AS total avg(aggregate_value) AS mean`)
	var emptyTotal, emptyMean *float64
	if err := connection.QueryRow(ctx, emptyGlobal.SQL, emptyGlobal.Args...).Scan(&emptyTotal, &emptyMean); err != nil {
		t.Fatalf("execute empty global sum/avg: %v\nSQL: %s\nargs: %#v", err, emptyGlobal.SQL, emptyGlobal.Args)
	}
	if emptyTotal != nil || emptyMean != nil {
		t.Fatalf("empty global sum/avg = %v/%v, want null/null", emptyTotal, emptyMean)
	}
	emptyGrouped := compile(`index=compiler source="sum-avg-absent" | stats sum(aggregate_value) BY aggregate_group`)
	if err := executeCompiledExpectingNoRows(ctx, connection, emptyGrouped); err != nil {
		t.Fatalf("execute empty grouped sum: %v\nSQL: %s\nargs: %#v", err, emptyGrouped.SQL, emptyGrouped.Args)
	}

	projected := compile(base + ` | fields aggregate_group | stats count sum(aggregate_value) AS total avg(aggregate_value) AS mean BY aggregate_group`)
	projectedRows, err := connection.Query(ctx, projected.SQL, projected.Args...)
	if err != nil {
		t.Fatalf("execute projected sum/avg: %v\nSQL: %s\nargs: %#v", err, projected.SQL, projected.Args)
	}
	projectedCount := 0
	projectedGroups := make(map[string]struct{}, 2)
	for projectedRows.Next() {
		var group string
		var count uint64
		var total, mean *float64
		if err := projectedRows.Scan(&group, &count, &total, &mean); err != nil {
			_ = projectedRows.Close()
			t.Fatalf("scan projected sum/avg: %v", err)
		}
		if total != nil || mean != nil || (group == "A" && count != 4) || (group == "B" && count != 2) {
			_ = projectedRows.Close()
			t.Fatalf("projected sum/avg row = %q/%d/%v/%v", group, count, total, mean)
		}
		projectedCount++
		projectedGroups[group] = struct{}{}
	}
	if err := projectedRows.Err(); err != nil {
		_ = projectedRows.Close()
		t.Fatalf("iterate projected sum/avg: %v", err)
	}
	if err := projectedRows.Close(); err != nil {
		t.Fatalf("close projected sum/avg: %v", err)
	}
	if projectedCount != 2 {
		t.Fatalf("projected sum/avg emitted %d rows for groups %v, want A and B", projectedCount, projectedGroups)
	}
	if _, ok := projectedGroups["A"]; !ok {
		t.Fatalf("projected sum/avg groups = %v, missing A", projectedGroups)
	}
	if _, ok := projectedGroups["B"]; !ok {
		t.Fatalf("projected sum/avg groups = %v, missing B", projectedGroups)
	}

	downstream := compile(base + ` | stats sum(aggregate_value) AS total BY aggregate_group | where total>30`)
	var downstreamGroup string
	var downstreamTotal *float64
	if err := connection.QueryRow(ctx, downstream.SQL, downstream.Args...).Scan(&downstreamGroup, &downstreamTotal); err != nil {
		t.Fatalf("execute downstream sum: %v\nSQL: %s\nargs: %#v", err, downstream.SQL, downstream.Args)
	}
	if downstreamGroup != "A" || downstreamTotal == nil || math.Abs(*downstreamTotal-34) > 1e-12 {
		t.Fatalf("downstream sum = %q/%v, want A/34", downstreamGroup, downstreamTotal)
	}

	repeated := compile(base + ` | stats sum(aggregate_value) AS total BY aggregate_group | stats avg(total) AS mean`)
	var repeatedMean *float64
	if err := connection.QueryRow(ctx, repeated.SQL, repeated.Args...).Scan(&repeatedMean); err != nil {
		t.Fatalf("execute repeated sum/avg: %v\nSQL: %s\nargs: %#v", err, repeated.SQL, repeated.Args)
	}
	if repeatedMean == nil || math.Abs(*repeatedMean-34) > 1e-12 {
		t.Fatalf("repeated sum/avg = %v, want 34", repeatedMean)
	}

	for _, test := range []struct {
		name  string
		field string
		want  float64
	}{
		{name: "float", field: "float_metric", want: 1.25},
		{name: "tagged decimal", field: "decimal_metric", want: 3.25},
		{name: "one-level array", field: "nested_metric", want: 5},
	} {
		query := compile(base + ` | stats sum(` + test.field + `) AS total avg(` + test.field + `) AS mean`)
		var total, mean *float64
		if err := connection.QueryRow(ctx, query.SQL, query.Args...).Scan(&total, &mean); err != nil {
			t.Fatalf("execute %s sum/avg: %v\nSQL: %s\nargs: %#v", test.name, err, query.SQL, query.Args)
		}
		if total == nil || mean == nil || math.Abs(*total-test.want) > 1e-12 || math.Abs(*mean-test.want) > 1e-12 {
			t.Fatalf("%s sum/avg = %v/%v, want %g/%g", test.name, total, mean, test.want, test.want)
		}
	}

	ignored := compile(base + ` | stats sum(ignored_metric) AS total avg(ignored_metric) AS mean`)
	if err := connection.QueryRow(ctx, ignored.SQL, ignored.Args...).Scan(&emptyTotal, &emptyMean); err != nil {
		t.Fatalf("execute ignored-type sum/avg: %v\nSQL: %s\nargs: %#v", err, ignored.SQL, ignored.Args)
	}
	if emptyTotal != nil || emptyMean != nil {
		t.Fatalf("ignored-type sum/avg = %v/%v, want null/null", emptyTotal, emptyMean)
	}

	overflow := compile(base + ` | stats sum(overflow_metric) AS total avg(overflow_metric) AS mean`)
	var overflowTotal, overflowMean *float64
	if err := connection.QueryRow(ctx, overflow.SQL, overflow.Args...).Scan(&overflowTotal, &overflowMean); err != nil {
		t.Fatalf("execute overflow sum/avg: %v\nSQL: %s\nargs: %#v", err, overflow.SQL, overflow.Args)
	}
	if overflowTotal == nil || !math.IsInf(*overflowTotal, 1) || overflowMean == nil || !math.IsInf(*overflowMean, 1) {
		t.Fatalf("overflow sum/avg = %v/%v, want +Inf/+Inf", overflowTotal, overflowMean)
	}

	canonicalTime := compile(base + ` | stats avg(_time) AS mean_time`)
	var meanTime *float64
	if err := connection.QueryRow(ctx, canonicalTime.SQL, canonicalTime.Args...).Scan(&meanTime); err != nil {
		t.Fatalf("execute canonical time average: %v\nSQL: %s\nargs: %#v", err, canonicalTime.SQL, canonicalTime.Args)
	}
	wantTime := float64(time.Date(2026, 7, 21, 3, 4, 5, 123456789, time.FixedZone("event-offset", 5*60*60)).UnixNano()) / 1e9
	if meanTime == nil || math.Abs(*meanTime-wantTime) > 1e-6 {
		t.Fatalf("canonical time average = %v, want %g", meanTime, wantTime)
	}
}

func testDedupAgainstClickHouse(
	t *testing.T,
	ctx context.Context,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()
	newEvent := func(id, source string, eventTime time.Time, fields ...*opensplunkv1.TypedObjectField) *ingest.StoredEvent {
		event := compilerIntegrationEvent(id, "dedup-host", "dedup fixture", indexTime, fields...)
		event.BatchID = "dedup-batch"
		event.Event.Source = source
		event.Event.EventTime = timestamppb.New(eventTime)
		return event
	}
	// Keep event time tied to the fixture's index time so a future tightening of
	// compileIntegrationSPL's event window cannot silently empty this corpus.
	t0 := indexTime.Add(-5 * time.Second)
	t1 := t0.Add(time.Second)
	t2 := t1.Add(time.Second)
	t3 := t2.Add(time.Second)
	t4 := t3.Add(time.Second)
	events := []*ingest.StoredEvent{
		// Default order is _time DESC then event_id DESC. These rows pin
		// non-consecutive global deduplication and the event-id tie-break.
		newEvent("dedup-a-y", "dedup-order", t0,
			typedField("dedup_key", typedString("A")), typedField("subkey", typedString("y"))),
		newEvent("dedup-a-old", "dedup-order", t1,
			typedField("dedup_key", typedString("A")), typedField("subkey", typedString("x"))),
		newEvent("dedup-b", "dedup-order", t2,
			typedField("dedup_key", typedString("B")), typedField("subkey", typedString("x"))),
		newEvent("dedup-a-new", "dedup-order", t3,
			typedField("dedup_key", typedString("A")), typedField("subkey", typedString("x"))),
		newEvent("dedup-tie-a", "dedup-order", t4,
			typedField("dedup_key", typedString("T")), typedField("subkey", typedString("tie"))),
		newEvent("dedup-tie-z", "dedup-order", t4,
			typedField("dedup_key", typedString("T")), typedField("subkey", typedString("tie"))),

		newEvent("dedup-optional-value", "dedup-presence", t0,
			typedField("optional_key", typedString("value"))),
		newEvent("dedup-optional-empty", "dedup-presence", t1,
			typedField("optional_key", typedString(""))),
		newEvent("dedup-optional-null", "dedup-presence", t2,
			typedField("optional_key", typedNull())),
		newEvent("dedup-optional-missing", "dedup-presence", t3),

		newEvent("dedup-number-string", "dedup-number", t1,
			typedField("numeric_key", typedString("500"))),
		newEvent("dedup-number-int", "dedup-number", t2,
			typedField("numeric_key", typedSint(500))),
		newEvent("dedup-case-lower", "dedup-case", t1,
			typedField("case_key", typedString("case"))),
		newEvent("dedup-case-upper", "dedup-case", t2,
			typedField("case_key", typedString("Case"))),

		newEvent("dedup-list-good", "dedup-list", t2,
			typedField("container_key", typedString("good"))),
		newEvent("dedup-list-bad", "dedup-list", t1,
			typedField("container_key", typedList(typedString("bad")))),
		newEvent("dedup-empty-object-good", "dedup-empty-object", t2,
			typedField("container_key", typedString("good"))),
		newEvent("dedup-empty-object-bad", "dedup-empty-object", t1,
			typedField("container_key", typedObject())),
		newEvent("dedup-parent-good", "dedup-parent", t2,
			typedField("container_key", typedString("good"))),
		newEvent("dedup-parent-bad", "dedup-parent", t1,
			typedField("container_key", typedObject(typedField("child", typedString("bad"))))),
		newEvent("dedup-tenant-visible", "dedup-tenant", t2,
			typedField("tenant_key", typedString("visible"))),
	}
	batch := ingest.StoreBatch{
		TenantID: "tenant", CollectorID: "collector", BatchID: "dedup-batch", BatchSequence: 7,
		SourceBatchSHA256: testSourceBatchDigest("dedup-batch"),
		ReceivedAt:        indexTime,
		Events:            events,
	}
	if _, err := store.Store(ctx, batch); err != nil {
		t.Fatalf("store dedup fixtures: %v", err)
	}
	foreign := newEvent("dedup-tenant-hidden", "dedup-tenant", t3,
		typedField("tenant_key", typedList(typedString("hidden"))))
	foreign.TenantID = "other-tenant"
	foreign.BatchID = "dedup-foreign-batch"
	if _, err := store.Store(ctx, ingest.StoreBatch{
		TenantID: "other-tenant", CollectorID: "collector", BatchID: "dedup-foreign-batch", BatchSequence: 8,
		SourceBatchSHA256: testSourceBatchDigest("dedup-foreign-batch"),
		ReceivedAt:        indexTime,
		Events:            []*ingest.StoredEvent{foreign},
	}); err != nil {
		t.Fatalf("store cross-tenant dedup fixture: %v", err)
	}
	if err := connection.Exec(ctx, "OPTIMIZE TABLE open_splunk.events FINAL"); err != nil {
		t.Fatalf("merge cross-tenant dedup fixtures: %v", err)
	}
	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture dedup visibility cutoff: %v", err)
	}
	compile := func(source string) CompiledQuery {
		return compileIntegrationSPL(t, source, indexTime.Add(10*time.Second), visibilityCutoff)
	}
	ids := func(source string) []string {
		t.Helper()
		query := compile(source + ` | table event_id`)
		rows, queryErr := connection.Query(ctx, query.SQL, query.Args...)
		if queryErr != nil {
			t.Fatalf("execute dedup query %q: %v\nSQL: %s\nargs: %#v", source, queryErr, query.SQL, query.Args)
		}
		defer rows.Close()
		var result []string
		for rows.Next() {
			var id string
			if scanErr := rows.Scan(&id); scanErr != nil {
				t.Fatalf("scan dedup query %q: %v", source, scanErr)
			}
			result = append(result, id)
		}
		if iterationErr := rows.Err(); iterationErr != nil {
			t.Fatalf("iterate dedup query %q: %v", source, iterationErr)
		}
		return result
	}
	assertIDs := func(source string, want ...string) {
		t.Helper()
		if got := ids(source); !slices.Equal(got, want) {
			t.Fatalf("dedup IDs for %q = %v, want %v", source, got, want)
		}
	}

	baseOrder := `index=compiler source="dedup-order"`
	assertIDs(baseOrder+` | dedup dedup_key`, "dedup-tie-z", "dedup-a-new", "dedup-b")
	assertIDs(baseOrder+` | sort 0 +_time | dedup dedup_key`, "dedup-a-y", "dedup-b", "dedup-tie-z")
	assertIDs(baseOrder+` | dedup 2 dedup_key`, "dedup-tie-z", "dedup-tie-a", "dedup-a-new", "dedup-b", "dedup-a-old")
	assertIDs(baseOrder+` | dedup dedup_key, subkey`, "dedup-tie-z", "dedup-a-new", "dedup-b", "dedup-a-y")
	assertIDs(baseOrder+` | dedup subkey | dedup dedup_key`, "dedup-tie-z", "dedup-a-new")
	assertIDs(`index=compiler source="dedup-presence" | dedup optional_key`, "dedup-optional-empty", "dedup-optional-value")
	assertIDs(`index=compiler source="dedup-number" | dedup numeric_key`, "dedup-number-int")
	assertIDs(`index=compiler source="dedup-case" | dedup case_key`, "dedup-case-upper", "dedup-case-lower")
	assertIDs(`index=compiler source="dedup-order" | fields event_id | dedup dedup_key`)
	assertIDs(`index=compiler source="dedup-tenant" | dedup tenant_key`, "dedup-tenant-visible")

	postStats := compile(baseOrder + ` | stats count BY dedup_key | sort 0 -count | dedup count | table dedup_key, count`)
	postStatsRows, err := connection.Query(ctx, postStats.SQL, postStats.Args...)
	if err != nil {
		t.Fatalf("execute post-stats dedup: %v\nSQL: %s\nargs: %#v", err, postStats.SQL, postStats.Args)
	}
	var counts []uint64
	for postStatsRows.Next() {
		var key string
		var count uint64
		if scanErr := postStatsRows.Scan(&key, &count); scanErr != nil {
			_ = postStatsRows.Close()
			t.Fatalf("scan post-stats dedup: %v", scanErr)
		}
		counts = append(counts, count)
	}
	if err := postStatsRows.Err(); err != nil {
		_ = postStatsRows.Close()
		t.Fatalf("iterate post-stats dedup: %v", err)
	}
	if err := postStatsRows.Close(); err != nil {
		t.Fatalf("close post-stats dedup: %v", err)
	}
	if !slices.Equal(counts, []uint64{3, 2, 1}) {
		t.Fatalf("post-stats dedup counts = %v, want [3 2 1]", counts)
	}

	for _, source := range []string{"dedup-list", "dedup-empty-object", "dedup-parent"} {
		query := compile(`index=compiler source="` + source + `" | dedup container_key | head 1`)
		queryErr := executeCompiledExpectingNoRows(ctx, connection, query)
		var exception *clickhousedriver.Exception
		if !errors.As(queryErr, &exception) || exception.Code != 395 || !strings.Contains(exception.Message, UnsupportedDedupValueMarker) {
			t.Fatalf("non-scalar dedup source %q error = %v, want guarded ClickHouse exception", source, queryErr)
		}
	}
	// A missing first key must not hide an unsupported value in another key:
	// validation covers the complete scoped input before eligibility filtering.
	hiddenByMissing := compile(`index=compiler source="dedup-list" | dedup absent_key, container_key | head 1`)
	hiddenErr := executeCompiledExpectingNoRows(ctx, connection, hiddenByMissing)
	var hiddenException *clickhousedriver.Exception
	if !errors.As(hiddenErr, &hiddenException) || hiddenException.Code != 395 ||
		!strings.Contains(hiddenException.Message, UnsupportedDedupValueMarker) {
		t.Fatalf("missing key hid unsupported dedup value: %v", hiddenErr)
	}
}

func executeCompiledExpectingNoRows(ctx context.Context, connection clickhousedriver.Conn, query CompiledQuery) (resultErr error) {
	rows, err := connection.Query(ctx, query.SQL, query.Args...)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := rows.Close(); resultErr == nil && closeErr != nil {
			resultErr = fmt.Errorf("close compiled result rows: %w", closeErr)
		}
	}()
	if rows.Next() {
		return errors.New("query unexpectedly emitted a row")
	}
	return rows.Err()
}

func compilerIntegrationEvent(id, host, raw string, indexTime time.Time, fields ...*opensplunkv1.TypedObjectField) *ingest.StoredEvent {
	event := testStoredEvent(id, "compiler", indexTime)
	event.BatchID = "compiler-batch"
	event.Event.Host = host
	event.Event.Raw = []byte(raw)
	event.Event.Message = stringPointer("Request metrics")
	event.Event.Fields = typedObjectValue(fields...)
	return event
}

func float64PointerForIntegration(value float64) *float64 { return &value }

func compileIntegrationSPL(t *testing.T, source string, cutoff time.Time, visibilityCutoff uint64) CompiledQuery {
	t.Helper()
	logical := buildIntegrationPlan(t, source, cutoff, visibilityCutoff)
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("compile integration SPL %q: %v", source, err)
	}
	return compiled
}

func buildIntegrationPlan(t *testing.T, source string, cutoff time.Time, visibilityCutoff uint64) *plan.Query {
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
	return logical
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
