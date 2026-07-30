package clickhouse

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"reflect"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/indexname"
	"github.com/Suhaibinator/open-splunk/internal/protocolid"
)

const (
	indexStatisticsMaximumTenantBytes = 255
	indexStatisticsMaximumIdentifier  = 255
	indexStatisticsQueryIDPrefix      = "open-splunk-index-statistics-"

	indexStatisticsMaxExecutionSeconds = uint64(15)
	indexStatisticsMaxMemoryBytes      = uint64(128 << 20)
	indexStatisticsMaxRowsToRead       = uint64(250_000_000)
	indexStatisticsMaxBytesToRead      = uint64(64 << 30)
	indexStatisticsMaxResultBytes      = uint64(64 << 10)
	indexStatisticsMaxQueryBytes       = uint64(16 << 10)
	indexStatisticsMaxThreads          = uint64(4)

	// clickhouse-go replaces max_execution_time with roughly the context
	// deadline plus five seconds. Keeping the entire two-query operation below
	// ten seconds prevents a long-lived caller context from widening the
	// configured fifteen-second server limit.
	indexStatisticsOperationTimeout = 10 * time.Second
)

// IndexStatisticsConfig identifies the shared MergeTree whose logical index
// statistics are read. Empty values select the canonical event table.
type IndexStatisticsConfig struct {
	Database string
	Table    string
}

// IndexStatisticsRequest is a trusted, fully resolved logical-index scope.
// MeasuredAt is both the retention/index-time boundary and the timestamp
// reported to the caller. VisibilityCutoff is an already-committed snapshot
// captured by the control plane and may legitimately be zero.
type IndexStatisticsRequest struct {
	TenantID         string
	IndexID          string
	IndexName        string
	MeasuredAt       time.Time
	VisibilityCutoff uint64
}

// IndexStatisticsResult contains exact logical row/time statistics and an
// estimated compressed-storage projection. Scope fields are echoed so the
// transport boundary can reject a mismatched dependency result.
type IndexStatisticsResult struct {
	TenantID          string
	IndexID           string
	IndexName         string
	VisibilityCutoff  uint64
	EventCount        uint64
	StorageBytes      uint64
	EarliestEventTime *time.Time
	LatestEventTime   *time.Time
	MeasuredAt        time.Time
	Estimates         bool
}

type indexStatisticsConnection interface {
	QueryRow(context.Context, string, ...any) driver.Row
}

// IndexStatisticsReader executes two bounded, read-only native ClickHouse
// queries at most. The first query returns exact logical statistics while
// reading only scope, visibility, retention, and event-time columns. A
// nonempty result performs one system.parts aggregate and applies the table's
// active compressed bytes per physical row to the logical event count.
type IndexStatisticsReader struct {
	connection indexStatisticsConnection
	database   string
	table      string
	settings   clickhousedriver.Settings
	newQueryID func() (string, error)
	operation  chan struct{}
}

// NewIndexStatisticsReader validates a borrowed native connection. The caller
// retains ownership and must keep it open for the reader's lifetime.
func NewIndexStatisticsReader(
	connection driver.Conn,
	config IndexStatisticsConfig,
) (*IndexStatisticsReader, error) {
	if isNilIndexStatisticsDependency(connection) {
		return nil, errors.New(
			"create ClickHouse index statistics reader: connection is required",
		)
	}
	return newIndexStatisticsReader(connection, config)
}

func newIndexStatisticsReader(
	connection indexStatisticsConnection,
	config IndexStatisticsConfig,
) (*IndexStatisticsReader, error) {
	if isNilIndexStatisticsDependency(connection) {
		return nil, errors.New(
			"create ClickHouse index statistics reader: connection is required",
		)
	}
	database := config.Database
	if database == "" {
		database = defaultDatabase
	}
	table := config.Table
	if table == "" {
		table = defaultTable
	}
	if len(database) > indexStatisticsMaximumIdentifier ||
		len(table) > indexStatisticsMaximumIdentifier ||
		!physicalIdentifier.MatchString(database) ||
		!physicalIdentifier.MatchString(table) {
		return nil, errors.New(
			"create ClickHouse index statistics reader: database and table identifiers are invalid",
		)
	}
	return &IndexStatisticsReader{
		connection: connection,
		database:   database,
		table:      table,
		settings:   indexStatisticsSettings(),
		newQueryID: randomIndexStatisticsQueryID,
		operation:  make(chan struct{}, 1),
	}, nil
}

// GetIndexStatistics returns an exact retained/committed logical count and
// event-time range for one index. StorageBytes is a proportional active-part
// estimate because one shared MergeTree cannot cheaply attribute compressed
// part bytes to an individual tenant/index key. Estimates is therefore always
// true, including the exact empty result.
func (reader *IndexStatisticsReader) GetIndexStatistics(
	ctx context.Context,
	request IndexStatisticsRequest,
) (IndexStatisticsResult, error) {
	if ctx == nil {
		return IndexStatisticsResult{}, errors.New(
			"read ClickHouse index statistics: context is nil",
		)
	}
	if reader == nil ||
		isNilIndexStatisticsDependency(reader.connection) ||
		reader.newQueryID == nil ||
		reader.settings == nil ||
		reader.operation == nil {
		return IndexStatisticsResult{}, errors.New(
			"read ClickHouse index statistics: reader is not initialized",
		)
	}
	if err := validateIndexStatisticsRequest(request); err != nil {
		return IndexStatisticsResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return IndexStatisticsResult{}, err
	}
	if err := reader.acquireOperation(ctx); err != nil {
		return IndexStatisticsResult{}, err
	}
	defer reader.releaseOperation()
	operationContext, cancel := context.WithTimeout(
		ctx,
		indexStatisticsOperationTimeout,
	)
	defer cancel()

	measuredAt := request.MeasuredAt.Round(0).UTC()
	queryID, err := reader.nextQueryID(operationContext)
	if err != nil {
		return IndexStatisticsResult{}, err
	}
	queryContext := clickhousedriver.Context(
		operationContext,
		clickhousedriver.WithQueryID(queryID),
		clickhousedriver.WithSettings(reader.settings),
	)
	cutoffText := formatDateTime64Milliseconds(measuredAt)
	row := reader.connection.QueryRow(
		queryContext,
		reader.logicalStatisticsSQL(),
		request.TenantID,
		request.IndexName,
		cutoffText,
		cutoffText,
		request.VisibilityCutoff,
	)
	if isNilIndexStatisticsDependency(row) {
		return IndexStatisticsResult{}, errors.New(
			"read ClickHouse index statistics: logical aggregate returned no row",
		)
	}

	var eventCount uint64
	var earliest, latest *time.Time
	if err := row.Scan(&eventCount, &earliest, &latest); err != nil {
		return IndexStatisticsResult{}, indexStatisticsOperationError(
			operationContext,
			"query logical aggregate",
			err,
		)
	}
	if err := operationContext.Err(); err != nil {
		return IndexStatisticsResult{}, err
	}
	earliest, latest, err = validatedIndexStatisticsBounds(
		eventCount,
		earliest,
		latest,
	)
	if err != nil {
		return IndexStatisticsResult{}, err
	}

	result := IndexStatisticsResult{
		TenantID:          request.TenantID,
		IndexID:           request.IndexID,
		IndexName:         request.IndexName,
		VisibilityCutoff:  request.VisibilityCutoff,
		EventCount:        eventCount,
		EarliestEventTime: earliest,
		LatestEventTime:   latest,
		MeasuredAt:        measuredAt,
		Estimates:         true,
	}
	if eventCount == 0 {
		return result, nil
	}

	queryID, err = reader.nextQueryID(operationContext)
	if err != nil {
		return IndexStatisticsResult{}, err
	}
	queryContext = clickhousedriver.Context(
		operationContext,
		clickhousedriver.WithQueryID(queryID),
		clickhousedriver.WithSettings(reader.settings),
	)
	row = reader.connection.QueryRow(
		queryContext,
		indexStatisticsActivePartsSQL,
		reader.database,
		reader.table,
	)
	if isNilIndexStatisticsDependency(row) {
		return IndexStatisticsResult{}, errors.New(
			"read ClickHouse index statistics: active-parts aggregate returned no row",
		)
	}
	var physicalRows, physicalBytes uint64
	if err := row.Scan(&physicalRows, &physicalBytes); err != nil {
		return IndexStatisticsResult{}, indexStatisticsOperationError(
			operationContext,
			"query active-part aggregate",
			err,
		)
	}
	if err := operationContext.Err(); err != nil {
		return IndexStatisticsResult{}, err
	}
	result.StorageBytes, err = proportionalIndexStorageBytes(
		eventCount,
		physicalRows,
		physicalBytes,
	)
	if err != nil {
		return IndexStatisticsResult{}, err
	}
	return result, nil
}

func (reader *IndexStatisticsReader) acquireOperation(
	ctx context.Context,
) error {
	select {
	case reader.operation <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		if err := ctx.Err(); err != nil {
			return err
		}
		return errors.New(
			"read ClickHouse index statistics: capacity is exhausted",
		)
	}
}

func (reader *IndexStatisticsReader) releaseOperation() {
	<-reader.operation
}

const indexStatisticsActivePartsSQL = `SELECT
    coalesce(sum("rows"), toUInt64(0)),
    coalesce(sum("bytes_on_disk"), toUInt64(0))
FROM system.parts
WHERE "database" = ? AND "table" = ? AND "active" = 1`

func (reader *IndexStatisticsReader) logicalStatisticsSQL() string {
	return `SELECT
    count(),
    minOrNull("event_time"),
    maxOrNull("event_time")
FROM ` + quoteIdentifier(reader.database) + `.` + quoteIdentifier(reader.table) + `
PREWHERE "tenant_id" = ? AND "index_name" = ?
WHERE "expires_at" > parseDateTime64BestEffort(?, 3, 'UTC')
  AND "index_time" <= parseDateTime64BestEffort(?, 3, 'UTC')
  AND "visibility_seq" <= ?`
}

func indexStatisticsSettings() clickhousedriver.Settings {
	return clickhousedriver.Settings{
		"readonly":              uint8(2),
		"max_execution_time":    indexStatisticsMaxExecutionSeconds,
		"timeout_overflow_mode": "throw",
		"max_memory_usage":      indexStatisticsMaxMemoryBytes,
		"max_rows_to_read":      indexStatisticsMaxRowsToRead,
		"max_bytes_to_read":     indexStatisticsMaxBytesToRead,
		"read_overflow_mode":    "throw",
		"max_result_rows":       uint64(1),
		"max_result_bytes":      indexStatisticsMaxResultBytes,
		"result_overflow_mode":  "throw",
		"max_threads":           indexStatisticsMaxThreads,
		"max_query_size":        indexStatisticsMaxQueryBytes,
		"use_query_cache":       uint8(0),
		"async_insert":          uint8(0),
		"max_subquery_depth":    uint64(1),
	}
}

func (reader *IndexStatisticsReader) nextQueryID(
	ctx context.Context,
) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	queryID, err := reader.newQueryID()
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return "", contextErr
		}
		return "", fmt.Errorf(
			"read ClickHouse index statistics: create query ID: %w",
			err,
		)
	}
	if !protocolid.Valid(queryID) {
		return "", errors.New(
			"read ClickHouse index statistics: query ID is invalid",
		)
	}
	return queryID, nil
}

func randomIndexStatisticsQueryID() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return indexStatisticsQueryIDPrefix + hex.EncodeToString(random[:]), nil
}

func validateIndexStatisticsRequest(request IndexStatisticsRequest) error {
	if request.TenantID == "" ||
		len(request.TenantID) > indexStatisticsMaximumTenantBytes ||
		!utf8.ValidString(request.TenantID) ||
		strings.TrimSpace(request.TenantID) != request.TenantID ||
		strings.ContainsFunc(request.TenantID, unicode.IsControl) {
		return errors.New(
			"read ClickHouse index statistics: tenant ID is invalid",
		)
	}
	if !protocolid.Valid(request.IndexID) {
		return errors.New(
			"read ClickHouse index statistics: index ID is invalid",
		)
	}
	if !indexname.ValidCanonical(request.IndexName) {
		return errors.New(
			"read ClickHouse index statistics: index name is invalid",
		)
	}
	if request.MeasuredAt.IsZero() ||
		request.MeasuredAt.Location() != time.UTC ||
		!request.MeasuredAt.Equal(
			request.MeasuredAt.Truncate(time.Millisecond),
		) ||
		!SupportsSearchTimeRange(request.MeasuredAt, request.MeasuredAt) {
		return errors.New(
			"read ClickHouse index statistics: measurement time is invalid",
		)
	}
	return nil
}

func validatedIndexStatisticsBounds(
	eventCount uint64,
	earliest *time.Time,
	latest *time.Time,
) (*time.Time, *time.Time, error) {
	if eventCount == 0 {
		if earliest != nil || latest != nil {
			return nil, nil, errors.New(
				"read ClickHouse index statistics: empty aggregate returned event-time bounds",
			)
		}
		return nil, nil, nil
	}
	if earliest == nil || latest == nil {
		return nil, nil, errors.New(
			"read ClickHouse index statistics: nonempty aggregate omitted event-time bounds",
		)
	}
	normalizedEarliest := earliest.Round(0).UTC()
	normalizedLatest := latest.Round(0).UTC()
	if normalizedEarliest.After(normalizedLatest) ||
		!SupportsSearchTimeRange(normalizedEarliest, normalizedLatest) {
		return nil, nil, errors.New(
			"read ClickHouse index statistics: aggregate event-time bounds are invalid",
		)
	}
	return &normalizedEarliest, &normalizedLatest, nil
}

func proportionalIndexStorageBytes(
	eventCount uint64,
	physicalRows uint64,
	physicalBytes uint64,
) (uint64, error) {
	if eventCount == 0 {
		return 0, nil
	}
	if physicalRows == 0 ||
		physicalBytes == 0 ||
		eventCount > physicalRows {
		return 0, errors.New(
			"read ClickHouse index statistics: active-part counters are inconsistent",
		)
	}
	high, low := bits.Mul64(eventCount, physicalBytes)
	if high >= physicalRows {
		return 0, errors.New(
			"read ClickHouse index statistics: storage estimate overflows",
		)
	}
	quotient, remainder := bits.Div64(high, low, physicalRows)
	if remainder != 0 {
		if quotient == math.MaxUint64 {
			return 0, errors.New(
				"read ClickHouse index statistics: storage estimate overflows",
			)
		}
		quotient++
	}
	return quotient, nil
}

func indexStatisticsOperationError(
	ctx context.Context,
	operation string,
	err error,
) error {
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("read ClickHouse index statistics: %s: %w", operation, err)
}

func isNilIndexStatisticsDependency(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
