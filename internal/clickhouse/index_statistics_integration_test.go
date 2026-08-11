package clickhouse_test

import (
	"context"
	"math/bits"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	internalclickhouse "github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/migrations"
)

// TestIndexStatisticsReaderAgainstClickHouse is opt-in because it starts the
// repository's digest-pinned ClickHouse image.
func TestIndexStatisticsReaderAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip(
			"set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test",
		)
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}

	image := strings.TrimSpace(
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if image == "" {
		image = testsupport.DefaultClickHouseImage
	}
	if !strings.Contains(image, "@sha256:") {
		t.Fatalf(
			"OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE must be digest-pinned, got %q",
			image,
		)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	container, err := testsupport.StartClickHouse(ctx, image)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(
			context.Background(),
			15*time.Second,
		)
		defer cleanupCancel()
		if closeErr := container.Close(cleanupContext); closeErr != nil {
			t.Errorf("close index statistics ClickHouse fixture: %v", closeErr)
		}
	})

	connection, err := clickhousedriver.Open(&clickhousedriver.Options{
		Protocol: clickhousedriver.Native,
		Addr:     []string{container.Address},
		Auth: clickhousedriver.Auth{
			Database: container.Database,
			Username: container.Username,
			Password: container.Password,
		},
		DialTimeout: 5 * time.Second,
		ReadTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := connection.Close(); closeErr != nil {
			t.Errorf("close index statistics connection: %v", closeErr)
		}
	})
	if err := connection.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := server.ApplyClickHouseMigrations(
		ctx,
		connection,
		migrations.ClickHouse(),
	); err != nil {
		t.Fatalf("apply embedded ClickHouse migrations: %v", err)
	}

	measuredAt := time.Date(2030, 7, 29, 12, 0, 0, 500_000_000, time.UTC)
	const (
		targetTenant     = "statistics-target-tenant"
		foreignTenant    = "statistics-foreign-tenant"
		targetIndexID    = "statistics-target-id"
		targetIndexName  = "statistics-target-index"
		emptyIndexID     = "statistics-empty-id"
		emptyIndexName   = "statistics-empty-index"
		neighborIndexID  = "statistics-neighbor-id"
		neighborIndex    = "statistics-neighbor-index"
		visibilityCutoff = uint64(41)
	)
	earliest := measuredAt.Add(-4 * time.Hour)
	latest := measuredAt.Add(-time.Hour)
	neighborEventTime := measuredAt.Add(10 * time.Hour)
	rows := []indexStatisticsFixtureRow{
		{
			eventID:       "included-earliest",
			tenantID:      targetTenant,
			indexName:     targetIndexName,
			eventTime:     earliest,
			indexTime:     measuredAt.Add(-time.Millisecond),
			expiresAt:     measuredAt.Add(time.Millisecond),
			visibilitySeq: visibilityCutoff - 1,
		},
		{
			eventID:       "included-at-inclusive-boundaries",
			tenantID:      targetTenant,
			indexName:     targetIndexName,
			eventTime:     latest,
			indexTime:     measuredAt,
			expiresAt:     measuredAt.Add(time.Millisecond),
			visibilitySeq: visibilityCutoff,
		},
		{
			eventID:       "excluded-after-index-time",
			tenantID:      targetTenant,
			indexName:     targetIndexName,
			eventTime:     measuredAt.Add(-8 * time.Hour),
			indexTime:     measuredAt.Add(time.Millisecond),
			expiresAt:     measuredAt.Add(time.Millisecond),
			visibilitySeq: visibilityCutoff - 1,
		},
		{
			eventID:       "excluded-at-expiry",
			tenantID:      targetTenant,
			indexName:     targetIndexName,
			eventTime:     measuredAt.Add(8 * time.Hour),
			indexTime:     measuredAt,
			expiresAt:     measuredAt,
			visibilitySeq: visibilityCutoff - 1,
		},
		{
			eventID:       "excluded-after-visibility-cutoff",
			tenantID:      targetTenant,
			indexName:     targetIndexName,
			eventTime:     measuredAt.Add(-7 * time.Hour),
			indexTime:     measuredAt,
			expiresAt:     measuredAt.Add(time.Millisecond),
			visibilitySeq: visibilityCutoff + 1,
		},
		{
			eventID:       "excluded-foreign-tenant",
			tenantID:      foreignTenant,
			indexName:     targetIndexName,
			eventTime:     measuredAt.Add(-10 * time.Hour),
			indexTime:     measuredAt,
			expiresAt:     measuredAt.Add(time.Millisecond),
			visibilitySeq: visibilityCutoff,
		},
		{
			eventID:       "excluded-foreign-tenant-empty-index",
			tenantID:      foreignTenant,
			indexName:     emptyIndexName,
			eventTime:     measuredAt.Add(-20 * time.Hour),
			indexTime:     measuredAt,
			expiresAt:     measuredAt.Add(time.Millisecond),
			visibilitySeq: visibilityCutoff,
		},
		{
			eventID:       "excluded-neighbor-index",
			tenantID:      targetTenant,
			indexName:     neighborIndex,
			eventTime:     neighborEventTime,
			indexTime:     measuredAt,
			expiresAt:     measuredAt.Add(time.Millisecond),
			visibilitySeq: visibilityCutoff,
		},
	}
	insertIndexStatisticsFixture(t, ctx, connection, rows)

	reader, err := internalclickhouse.NewIndexStatisticsReader(
		connection,
		internalclickhouse.IndexStatisticsConfig{
			Database:      container.Database,
			Table:         "events",
			ReadAdmission: indexread.UnfencedAdmission{},
		},
	)
	if err != nil {
		t.Fatalf("NewIndexStatisticsReader: %v", err)
	}
	request := internalclickhouse.IndexStatisticsRequest{
		TenantID:         targetTenant,
		IndexID:          targetIndexID,
		IndexName:        targetIndexName,
		MeasuredAt:       measuredAt,
		VisibilityCutoff: visibilityCutoff,
	}
	result, err := reader.GetIndexStatistics(ctx, request)
	if err != nil {
		t.Fatalf("GetIndexStatistics: %v", err)
	}
	assertIndexStatisticsScope(t, result, request)
	if result.EventCount != 2 {
		t.Fatalf("EventCount = %d, want 2", result.EventCount)
	}
	if result.EarliestEventTime == nil ||
		!result.EarliestEventTime.Equal(earliest) {
		t.Fatalf(
			"EarliestEventTime = %v, want %v",
			result.EarliestEventTime,
			earliest,
		)
	}
	if result.LatestEventTime == nil ||
		!result.LatestEventTime.Equal(latest) {
		t.Fatalf(
			"LatestEventTime = %v, want %v",
			result.LatestEventTime,
			latest,
		)
	}
	if !result.Estimates {
		t.Fatal("Estimates = false, want true")
	}

	var tablePhysicalRows, tableStorageBytes uint64
	if err := connection.QueryRow(
		ctx,
		`SELECT
		     coalesce(sum(rows), toUInt64(0)),
		     coalesce(sum(bytes_on_disk), toUInt64(0))
		   FROM system.parts
		  WHERE database = ?
		    AND table = ?
		    AND active = 1`,
		container.Database,
		"events",
	).Scan(&tablePhysicalRows, &tableStorageBytes); err != nil {
		t.Fatalf("read table storage bound: %v", err)
	}
	if tablePhysicalRows == 0 || tableStorageBytes == 0 {
		t.Fatalf(
			"active table sample = (%d rows, %d bytes), want nonzero counters",
			tablePhysicalRows,
			tableStorageBytes,
		)
	}
	targetStorageBytes := proportionalStorageFixtureEstimate(
		t,
		2,
		tablePhysicalRows,
		tableStorageBytes,
	)
	if result.StorageBytes != targetStorageBytes {
		t.Fatalf(
			"StorageBytes = %d, want shared-sample estimate %d",
			result.StorageBytes,
			targetStorageBytes,
		)
	}

	emptyRequest := request
	emptyRequest.VisibilityCutoff = 0
	emptyResult, err := reader.GetIndexStatistics(ctx, emptyRequest)
	if err != nil {
		t.Fatalf("GetIndexStatistics with zero visibility cutoff: %v", err)
	}
	assertIndexStatisticsScope(t, emptyResult, emptyRequest)
	if emptyResult.EventCount != 0 ||
		emptyResult.StorageBytes != 0 ||
		emptyResult.EarliestEventTime != nil ||
		emptyResult.LatestEventTime != nil ||
		!emptyResult.Estimates {
		t.Fatalf("empty statistics = %#v, want zero estimates and nil bounds", emptyResult)
	}

	batchRequest := internalclickhouse.IndexStatisticsBatchRequest{
		TenantID: targetTenant,
		Indexes: []internalclickhouse.IndexStatisticsScope{
			{
				IndexID:   neighborIndexID,
				IndexName: neighborIndex,
			},
			{
				IndexID:   emptyIndexID,
				IndexName: emptyIndexName,
			},
			{
				IndexID:   targetIndexID,
				IndexName: targetIndexName,
			},
		},
		MeasuredAt:       measuredAt,
		VisibilityCutoff: visibilityCutoff,
	}
	batchResults, err := reader.GetIndexStatisticsBatch(ctx, batchRequest)
	if err != nil {
		t.Fatalf("GetIndexStatisticsBatch: %v", err)
	}
	if len(batchResults) != len(batchRequest.Indexes) {
		t.Fatalf(
			"batch result count = %d, want %d",
			len(batchResults),
			len(batchRequest.Indexes),
		)
	}
	for index, scope := range batchRequest.Indexes {
		assertIndexStatisticsScope(
			t,
			batchResults[index],
			internalclickhouse.IndexStatisticsRequest{
				TenantID:         batchRequest.TenantID,
				IndexID:          scope.IndexID,
				IndexName:        scope.IndexName,
				MeasuredAt:       batchRequest.MeasuredAt,
				VisibilityCutoff: batchRequest.VisibilityCutoff,
			},
		)
		if !batchResults[index].Estimates {
			t.Fatalf("batch result %d Estimates = false, want true", index)
		}
	}

	neighborResult := batchResults[0]
	if neighborResult.EventCount != 1 ||
		neighborResult.EarliestEventTime == nil ||
		!neighborResult.EarliestEventTime.Equal(neighborEventTime) ||
		neighborResult.LatestEventTime == nil ||
		!neighborResult.LatestEventTime.Equal(neighborEventTime) {
		t.Fatalf(
			"neighbor statistics = %#v, want one isolated event at %v",
			neighborResult,
			neighborEventTime,
		)
	}
	neighborStorageBytes := proportionalStorageFixtureEstimate(
		t,
		1,
		tablePhysicalRows,
		tableStorageBytes,
	)
	if neighborResult.StorageBytes != neighborStorageBytes {
		t.Fatalf(
			"neighbor StorageBytes = %d, want shared-sample estimate %d",
			neighborResult.StorageBytes,
			neighborStorageBytes,
		)
	}

	batchEmptyResult := batchResults[1]
	if batchEmptyResult.EventCount != 0 ||
		batchEmptyResult.StorageBytes != 0 ||
		batchEmptyResult.EarliestEventTime != nil ||
		batchEmptyResult.LatestEventTime != nil {
		t.Fatalf(
			"missing-group statistics = %#v, want explicit empty result",
			batchEmptyResult,
		)
	}

	batchTargetResult := batchResults[2]
	if batchTargetResult.EventCount != 2 ||
		batchTargetResult.EarliestEventTime == nil ||
		!batchTargetResult.EarliestEventTime.Equal(earliest) ||
		batchTargetResult.LatestEventTime == nil ||
		!batchTargetResult.LatestEventTime.Equal(latest) ||
		batchTargetResult.StorageBytes != targetStorageBytes {
		t.Fatalf(
			"target batch statistics = %#v, want isolated count/range/storage",
			batchTargetResult,
		)
	}
}

type indexStatisticsFixtureRow struct {
	eventID       string
	tenantID      string
	indexName     string
	eventTime     time.Time
	indexTime     time.Time
	expiresAt     time.Time
	visibilitySeq uint64
}

func insertIndexStatisticsFixture(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	rows []indexStatisticsFixtureRow,
) {
	t.Helper()
	source := ingest.NativeCollectorSource("index-statistics-fixture-collector")

	batch, err := connection.PrepareBatch(
		ctx,
		`INSERT INTO open_splunk.events
			(event_id, tenant_id, index_name, event_time, index_time,
			 collector_id, ingest_source_kind, ingest_source_id,
			 expires_at, visibility_seq)`,
	)
	if err != nil {
		t.Fatalf("prepare index statistics fixture: %v", err)
	}
	for _, row := range rows {
		if err := batch.Append(
			row.eventID,
			row.tenantID,
			row.indexName,
			row.eventTime,
			row.indexTime,
			source.CollectorID,
			uint8(source.Kind),
			source.ID,
			row.expiresAt,
			row.visibilitySeq,
		); err != nil {
			t.Fatalf("append index statistics fixture row %q: %v", row.eventID, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send index statistics fixture: %v", err)
	}
}

func assertIndexStatisticsScope(
	t *testing.T,
	result internalclickhouse.IndexStatisticsResult,
	request internalclickhouse.IndexStatisticsRequest,
) {
	t.Helper()

	if result.TenantID != request.TenantID ||
		result.IndexID != request.IndexID ||
		result.IndexName != request.IndexName ||
		result.VisibilityCutoff != request.VisibilityCutoff ||
		!result.MeasuredAt.Equal(request.MeasuredAt) ||
		result.MeasuredAt.Location() != time.UTC {
		t.Fatalf(
			"result scope = %#v, want request scope %#v with UTC measurement",
			result,
			request,
		)
	}
}

func proportionalStorageFixtureEstimate(
	t *testing.T,
	eventCount uint64,
	physicalRows uint64,
	physicalBytes uint64,
) uint64 {
	t.Helper()

	high, low := bits.Mul64(eventCount, physicalBytes)
	if high >= physicalRows {
		t.Fatalf(
			"fixture storage estimate overflows: count=%d rows=%d bytes=%d",
			eventCount,
			physicalRows,
			physicalBytes,
		)
	}
	quotient, remainder := bits.Div64(high, low, physicalRows)
	if remainder != 0 {
		quotient++
	}
	return quotient
}
