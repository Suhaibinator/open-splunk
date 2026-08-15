package clickhouse

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"math"
	"math/bits"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/indexname"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"github.com/Suhaibinator/open-splunk/internal/protocolid"
)

const (
	indexStatisticsMaximumTenantBytes = 255
	indexStatisticsMaximumIdentifier  = 255
	indexStatisticsMaximumBatchSize   = 64
	indexStatisticsQueryIDPrefix      = "open-splunk-index-statistics-"

	indexStatisticsMaxExecutionSeconds = uint64(15)
	indexStatisticsMaxMemoryBytes      = uint64(128 << 20)
	indexStatisticsMaxRowsToRead       = uint64(250_000_000)
	indexStatisticsMaxBytesToRead      = uint64(64 << 30)
	indexStatisticsMaxResultBytes      = uint64(64 << 10)
	indexStatisticsMaxQueryBytes       = uint64(16 << 10)
	indexStatisticsBatchMaxQueryBytes  = uint64(64 << 10)
	indexStatisticsMaxThreads          = uint64(4)

	// clickhouse-go replaces max_execution_time with roughly the context
	// deadline plus five seconds. Keeping the entire two-query operation below
	// ten seconds prevents a long-lived caller context from widening the
	// configured fifteen-second server limit.
	indexStatisticsOperationTimeout = 10 * time.Second
)

// IndexStatisticsConfig identifies the shared MergeTree whose logical index
// statistics are read. Empty database and table values select the canonical
// event table. ReadAdmission is required.
type IndexStatisticsConfig struct {
	Database      string
	Table         string
	ReadAdmission indexread.Admission
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

// IndexStatisticsScope is one trusted logical index in a bounded batch
// statistics request.
type IndexStatisticsScope struct {
	IndexID   string
	IndexName string
}

// IndexStatisticsBatchRequest is a trusted, fully resolved set of logical
// index scopes measured at one committed visibility snapshot.
type IndexStatisticsBatchRequest struct {
	TenantID         string
	Indexes          []IndexStatisticsScope
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

type indexStatisticsRowsConnection interface {
	Query(context.Context, string, ...any) (driver.Rows, error)
}

// IndexStatisticsReader executes two bounded, read-only native ClickHouse
// queries at most. The first query returns exact logical statistics while
// reading only scope, visibility, retention, and event-time columns. A
// nonempty result performs one system.parts aggregate and applies the table's
// active compressed bytes per physical row to the logical event count.
type IndexStatisticsReader struct {
	connection    indexStatisticsConnection
	database      string
	table         string
	settings      clickhousedriver.Settings
	newQueryID    func() (string, error)
	operation     chan struct{}
	readAdmission indexread.Admission
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
	if config.ReadAdmission == nil ||
		isNilIndexStatisticsDependency(config.ReadAdmission) {
		return nil, errors.New(
			"create ClickHouse index statistics reader: read admission is required",
		)
	}
	return &IndexStatisticsReader{
		connection:    connection,
		database:      database,
		table:         table,
		settings:      indexStatisticsSettings(),
		newQueryID:    randomIndexStatisticsQueryID,
		operation:     make(chan struct{}, 1),
		readAdmission: config.ReadAdmission,
	}, nil
}

func (reader *IndexStatisticsReader) acquireIndexStatisticsRead(
	ctx context.Context,
	tenantID string,
	indexNames []string,
) (context.Context, func(), error) {
	if reader.readAdmission == nil ||
		isNilIndexStatisticsDependency(reader.readAdmission) {
		return nil, nil, errors.New(
			"read ClickHouse index statistics: read admission is required",
		)
	}
	admittedContext, release, err := reader.readAdmission.Acquire(
		ctx,
		tenantID,
		indexNames,
	)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"read ClickHouse index statistics: admit index read: %w",
			err,
		)
	}
	if admittedContext == nil || release == nil {
		if release != nil {
			release()
		}
		return nil, nil, errors.New(
			"read ClickHouse index statistics: read admission returned an incomplete lease",
		)
	}
	return admittedContext, release, nil
}

func preserveIndexStatisticsReadCause(ctx context.Context, err error) error {
	cause := context.Cause(ctx)
	if cause == nil || errors.Is(cause, context.Canceled) ||
		errors.Is(cause, context.DeadlineExceeded) {
		return err
	}
	if err == nil {
		return cause
	}
	if errors.Is(err, cause) {
		return err
	}
	return errors.Join(cause, err)
}

// GetIndexStatistics returns an exact retained/committed logical count and
// event-time range for one index. StorageBytes is a proportional active-part
// estimate because one shared MergeTree cannot cheaply attribute compressed
// part bytes to an individual tenant/index key. Estimates is therefore always
// true, including the exact empty result.
func (reader *IndexStatisticsReader) GetIndexStatistics(
	ctx context.Context,
	request IndexStatisticsRequest,
) (result IndexStatisticsResult, resultErr error) {
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
	operationContext, cancel := context.WithTimeout(
		ctx,
		indexStatisticsOperationTimeout,
	)
	defer cancel()
	operationContext, releaseRead, err := reader.acquireIndexStatisticsRead(
		operationContext,
		request.TenantID,
		[]string{request.IndexName},
	)
	if err != nil {
		return IndexStatisticsResult{}, err
	}
	defer releaseRead()
	defer func() {
		resultErr = preserveIndexStatisticsReadCause(operationContext, resultErr)
	}()
	// Catalog-backed admission may wait on SQLite. Keep that work outside the
	// single native-session gate while retaining one overall operation deadline.
	if err := reader.acquireOperation(operationContext); err != nil {
		return IndexStatisticsResult{}, err
	}
	defer reader.releaseOperation()

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

	result = IndexStatisticsResult{
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

// GetIndexStatisticsBatch returns one result for each requested index, in
// request order. It uses one grouped logical query for the entire bounded
// batch and, when any logical rows exist, one shared active-part sample.
func (reader *IndexStatisticsReader) GetIndexStatisticsBatch(
	ctx context.Context,
	request IndexStatisticsBatchRequest,
) (results []IndexStatisticsResult, resultErr error) {
	if ctx == nil {
		return nil, errors.New(
			"read ClickHouse index statistics batch: context is nil",
		)
	}
	if reader == nil ||
		isNilIndexStatisticsDependency(reader.connection) ||
		reader.newQueryID == nil ||
		reader.settings == nil ||
		reader.operation == nil {
		return nil, errors.New(
			"read ClickHouse index statistics batch: reader is not initialized",
		)
	}
	if err := validateIndexStatisticsBatchRequest(request); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(request.Indexes) == 0 {
		return []IndexStatisticsResult{}, nil
	}
	rowsConnection, ok := reader.connection.(indexStatisticsRowsConnection)
	if !ok || isNilIndexStatisticsDependency(rowsConnection) {
		return nil, errors.New(
			"read ClickHouse index statistics batch: reader does not support row queries",
		)
	}
	operationContext, cancel := context.WithTimeout(
		ctx,
		indexStatisticsOperationTimeout,
	)
	defer cancel()
	indexNames := make([]string, len(request.Indexes))
	for index, scope := range request.Indexes {
		indexNames[index] = scope.IndexName
	}
	operationContext, releaseRead, err := reader.acquireIndexStatisticsRead(
		operationContext,
		request.TenantID,
		indexNames,
	)
	if err != nil {
		return nil, err
	}
	defer releaseRead()
	defer func() {
		resultErr = preserveIndexStatisticsReadCause(operationContext, resultErr)
	}()
	// Catalog-backed admission may wait on SQLite. Keep that work outside the
	// single native-session gate while retaining one overall operation deadline.
	if err := reader.acquireOperation(operationContext); err != nil {
		return nil, err
	}
	defer reader.releaseOperation()

	measuredAt := request.MeasuredAt.Round(0).UTC()
	queryID, err := reader.nextQueryID(operationContext)
	if err != nil {
		return nil, err
	}
	queryContext := clickhousedriver.Context(
		operationContext,
		clickhousedriver.WithQueryID(queryID),
		clickhousedriver.WithSettings(indexStatisticsBatchSettings(reader.settings)),
	)
	queryArguments := make([]any, 0, len(request.Indexes)+4)
	queryArguments = append(queryArguments, request.TenantID)
	requestedIndexes := make(map[string]int, len(request.Indexes))
	for index, scope := range request.Indexes {
		queryArguments = append(queryArguments, scope.IndexName)
		requestedIndexes[scope.IndexName] = index
	}
	cutoffText := formatDateTime64Milliseconds(measuredAt)
	queryArguments = append(
		queryArguments,
		cutoffText,
		cutoffText,
		request.VisibilityCutoff,
	)
	rows, err := rowsConnection.Query(
		queryContext,
		reader.logicalStatisticsBatchSQL(len(request.Indexes)),
		queryArguments...,
	)
	if err != nil {
		return nil, indexStatisticsOperationError(
			operationContext,
			"query batched logical aggregate",
			err,
		)
	}
	if isNilIndexStatisticsDependency(rows) {
		return nil, errors.New(
			"read ClickHouse index statistics batch: logical aggregate returned no rows handle",
		)
	}

	aggregates, totalEventCount, err := readIndexStatisticsBatchRows(
		operationContext,
		rows,
		requestedIndexes,
	)
	if err != nil {
		return nil, err
	}
	results = make([]IndexStatisticsResult, len(request.Indexes))
	for index, scope := range request.Indexes {
		result := IndexStatisticsResult{
			TenantID:         request.TenantID,
			IndexID:          scope.IndexID,
			IndexName:        scope.IndexName,
			VisibilityCutoff: request.VisibilityCutoff,
			MeasuredAt:       measuredAt,
			Estimates:        true,
		}
		if aggregate, exists := aggregates[scope.IndexName]; exists {
			result.EventCount = aggregate.eventCount
			result.EarliestEventTime = aggregate.earliest
			result.LatestEventTime = aggregate.latest
		}
		results[index] = result
	}
	if totalEventCount == 0 {
		return results, nil
	}

	physicalRows, physicalBytes, err := reader.readIndexStatisticsActiveParts(
		operationContext,
	)
	if err != nil {
		return nil, err
	}
	if _, err := proportionalIndexStorageBytes(
		totalEventCount,
		physicalRows,
		physicalBytes,
	); err != nil {
		return nil, err
	}
	for index := range results {
		results[index].StorageBytes, err = proportionalIndexStorageBytes(
			results[index].EventCount,
			physicalRows,
			physicalBytes,
		)
		if err != nil {
			return nil, err
		}
	}
	return results, nil
}

type indexStatisticsBatchAggregate struct {
	eventCount uint64
	earliest   *time.Time
	latest     *time.Time
}

func readIndexStatisticsBatchRows(
	ctx context.Context,
	rows driver.Rows,
	requestedIndexes map[string]int,
) (
	aggregates map[string]indexStatisticsBatchAggregate,
	totalEventCount uint64,
	err error,
) {
	aggregates = make(
		map[string]indexStatisticsBatchAggregate,
		len(requestedIndexes),
	)
	defer func() {
		closeErr := rows.Close()
		if err == nil && closeErr != nil {
			err = indexStatisticsOperationError(
				ctx,
				"close batched logical aggregate",
				closeErr,
			)
			aggregates = nil
			totalEventCount = 0
		}
	}()

	for rows.Next() {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, 0, contextErr
		}
		var (
			indexName        string
			eventCount       uint64
			earliest, latest *time.Time
		)
		if scanErr := rows.Scan(
			&indexName,
			&eventCount,
			&earliest,
			&latest,
		); scanErr != nil {
			return nil, 0, indexStatisticsOperationError(
				ctx,
				"scan batched logical aggregate",
				scanErr,
			)
		}
		if _, exists := requestedIndexes[indexName]; !exists {
			return nil, 0, errors.New(
				"read ClickHouse index statistics batch: logical aggregate returned an unknown index",
			)
		}
		if _, exists := aggregates[indexName]; exists {
			return nil, 0, errors.New(
				"read ClickHouse index statistics batch: logical aggregate returned a duplicate index",
			)
		}
		if eventCount == 0 {
			return nil, 0, errors.New(
				"read ClickHouse index statistics batch: grouped logical aggregate returned an empty group",
			)
		}
		earliest, latest, boundsErr := validatedIndexStatisticsBounds(
			eventCount,
			earliest,
			latest,
		)
		if boundsErr != nil {
			return nil, 0, boundsErr
		}
		if math.MaxUint64-totalEventCount < eventCount {
			return nil, 0, errors.New(
				"read ClickHouse index statistics batch: logical event total overflows",
			)
		}
		totalEventCount += eventCount
		aggregates[indexName] = indexStatisticsBatchAggregate{
			eventCount: eventCount,
			earliest:   earliest,
			latest:     latest,
		}
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return nil, 0, contextErr
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, 0, indexStatisticsOperationError(
			ctx,
			"iterate batched logical aggregate",
			rowsErr,
		)
	}
	return aggregates, totalEventCount, nil
}

func (reader *IndexStatisticsReader) readIndexStatisticsActiveParts(
	ctx context.Context,
) (uint64, uint64, error) {
	queryID, err := reader.nextQueryID(ctx)
	if err != nil {
		return 0, 0, err
	}
	queryContext := clickhousedriver.Context(
		ctx,
		clickhousedriver.WithQueryID(queryID),
		clickhousedriver.WithSettings(reader.settings),
	)
	row := reader.connection.QueryRow(
		queryContext,
		indexStatisticsActivePartsSQL,
		reader.database,
		reader.table,
	)
	if isNilIndexStatisticsDependency(row) {
		return 0, 0, errors.New(
			"read ClickHouse index statistics batch: active-parts aggregate returned no row",
		)
	}
	var physicalRows, physicalBytes uint64
	if err := row.Scan(&physicalRows, &physicalBytes); err != nil {
		return 0, 0, indexStatisticsOperationError(
			ctx,
			"query batched active-part aggregate",
			err,
		)
	}
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	return physicalRows, physicalBytes, nil
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

func (reader *IndexStatisticsReader) logicalStatisticsBatchSQL(
	indexCount int,
) string {
	placeholders := strings.TrimSuffix(
		strings.Repeat("?, ", indexCount),
		", ",
	)
	return `SELECT
    "index_name",
    count(),
    minOrNull("event_time"),
    maxOrNull("event_time")
FROM ` + quoteIdentifier(reader.database) + `.` + quoteIdentifier(reader.table) + `
PREWHERE "tenant_id" = ? AND "index_name" IN (` + placeholders + `)
WHERE "expires_at" > parseDateTime64BestEffort(?, 3, 'UTC')
  AND "index_time" <= parseDateTime64BestEffort(?, 3, 'UTC')
  AND "visibility_seq" <= ?
GROUP BY "index_name"
ORDER BY "index_name"`
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

func indexStatisticsBatchSettings(
	base clickhousedriver.Settings,
) clickhousedriver.Settings {
	settings := make(clickhousedriver.Settings, len(base)+2)
	maps.Copy(settings, base)
	settings["max_result_rows"] = uint64(indexStatisticsMaximumBatchSize)
	settings["max_rows_to_group_by"] = uint64(indexStatisticsMaximumBatchSize)
	settings["group_by_overflow_mode"] = "throw"
	settings["max_query_size"] = indexStatisticsBatchMaxQueryBytes
	return settings
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
	if err := validateIndexStatisticsCommon(
		request.TenantID,
		request.MeasuredAt,
	); err != nil {
		return err
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
	return nil
}

func validateIndexStatisticsBatchRequest(
	request IndexStatisticsBatchRequest,
) error {
	if err := validateIndexStatisticsCommon(
		request.TenantID,
		request.MeasuredAt,
	); err != nil {
		return err
	}
	if len(request.Indexes) > indexStatisticsMaximumBatchSize {
		return errors.New(
			"read ClickHouse index statistics batch: too many indexes",
		)
	}
	indexIDs := make(map[string]struct{}, len(request.Indexes))
	indexNames := make(map[string]struct{}, len(request.Indexes))
	for _, scope := range request.Indexes {
		if !protocolid.Valid(scope.IndexID) {
			return errors.New(
				"read ClickHouse index statistics batch: index ID is invalid",
			)
		}
		if !indexname.ValidCanonical(scope.IndexName) {
			return errors.New(
				"read ClickHouse index statistics batch: index name is invalid",
			)
		}
		if _, exists := indexIDs[scope.IndexID]; exists {
			return errors.New(
				"read ClickHouse index statistics batch: index ID is duplicated",
			)
		}
		if _, exists := indexNames[scope.IndexName]; exists {
			return errors.New(
				"read ClickHouse index statistics batch: index name is duplicated",
			)
		}
		indexIDs[scope.IndexID] = struct{}{}
		indexNames[scope.IndexName] = struct{}{}
	}
	return nil
}

func validateIndexStatisticsCommon(tenantID string, measuredAt time.Time) error {
	if tenantID == "" ||
		len(tenantID) > indexStatisticsMaximumTenantBytes ||
		!utf8.ValidString(tenantID) ||
		strings.TrimSpace(tenantID) != tenantID ||
		strings.ContainsFunc(tenantID, unicode.IsControl) {
		return errors.New(
			"read ClickHouse index statistics: tenant ID is invalid",
		)
	}
	if measuredAt.IsZero() ||
		measuredAt.Location() != time.UTC ||
		!measuredAt.Equal(measuredAt.Truncate(time.Millisecond)) ||
		!SupportsSearchTimeRange(measuredAt, measuredAt) {
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
	return nilcheck.IsNil(value)
}
