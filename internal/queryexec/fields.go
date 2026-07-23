package queryexec

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode/utf8"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const (
	maximumFieldCatalogNameBytes = eventfields.MaximumNormalizedFieldNameBytes
	maximumFieldCatalogBytes     = uint64(32 << 20)
)

var ErrFieldMetadataUnavailable = errors.New("field catalog metadata is unavailable")

// FieldProfileRow contains exact presence counters and all semantic value
// types observed for one field in a completed event search.
type FieldProfileRow struct {
	FieldName     string
	ObservedTypes []eventfields.StoredValueType
	EventCount    uint64
	NullCount     uint64
	MissingCount  uint64
}

// FieldCatalogResult is a completely validated field catalog. TotalEvents is
// shared by every profile and Fields are strictly bytewise sorted by name.
type FieldCatalogResult struct {
	TotalEvents uint64
	Fields      []FieldProfileRow
}

// ExecuteFieldCatalog reads and validates a compiler-produced field catalog.
// It buffers the complete response so invalid, oversized, or interrupted
// ClickHouse results are never returned partially.
func (executor *Executor) ExecuteFieldCatalog(ctx context.Context, query clickhouse.CompiledFieldCatalog) (result FieldCatalogResult, resultErr error) {
	if ctx == nil {
		return FieldCatalogResult{}, errors.New("execute ClickHouse field catalog: context is nil")
	}
	if executor == nil || isNilDriverValue(executor.connection) {
		return FieldCatalogResult{}, errors.New("execute ClickHouse field catalog: executor connection is required")
	}
	if executor.newQueryID == nil {
		return FieldCatalogResult{}, errors.New("execute ClickHouse field catalog: query ID generator is required")
	}
	if err := validateCompiledFieldCatalog(query); err != nil {
		return FieldCatalogResult{}, err
	}
	settings, err := settingsForFieldCatalog(executor.settings, query.Spec.MaximumFields)
	if err != nil {
		return FieldCatalogResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return FieldCatalogResult{}, err
	}

	queryID, err := executor.newQueryID()
	if err != nil {
		return FieldCatalogResult{}, fmt.Errorf("execute ClickHouse field catalog: create query ID: %w", err)
	}
	if queryID == "" {
		return FieldCatalogResult{}, errors.New("execute ClickHouse field catalog: query ID is empty")
	}
	if err := ctx.Err(); err != nil {
		return FieldCatalogResult{}, err
	}
	queryContext := clickhousedriver.Context(ctx,
		clickhousedriver.WithQueryID(queryID),
		clickhousedriver.WithSettings(settings),
	)
	rows, err := executor.connection.Query(queryContext, query.SQL, query.Args...)
	if err != nil {
		return FieldCatalogResult{}, classifyQueryError(ctx, fmt.Errorf("query ClickHouse field catalog: %w", err))
	}
	if isNilDriverValue(rows) {
		return FieldCatalogResult{}, fmt.Errorf("%w: ClickHouse field catalog returned no result stream", searchjobs.ErrInvalidResult)
	}

	rowsClosed := false
	defer func() {
		if rowsClosed {
			return
		}
		if closeErr := rows.Close(); resultErr == nil && closeErr != nil {
			result = FieldCatalogResult{}
			resultErr = classifyQueryError(ctx, fmt.Errorf("close ClickHouse field catalog result stream: %w", closeErr))
		}
	}()

	if err := ctx.Err(); err != nil {
		return FieldCatalogResult{}, err
	}
	if err := validateFieldCatalogColumns(rows.Columns(), rows.ColumnTypes()); err != nil {
		return FieldCatalogResult{}, err
	}

	profiles := make([]FieldProfileRow, 0, query.Spec.MaximumFields)
	var totalEvents uint64
	var metadataInvalid bool
	var previousName string
	rowIndex := uint32(0)
	profileCount := uint32(0)
	for {
		if err := ctx.Err(); err != nil {
			return FieldCatalogResult{}, err
		}
		if !rows.Next() {
			break
		}
		if err := ctx.Err(); err != nil {
			return FieldCatalogResult{}, err
		}

		var rowKind uint8
		var fieldName string
		var observedTypeCodes []uint8
		var eventCount, nullCount, missingCount, rowTotalEvents uint64
		var invalid uint8
		if err := rows.Scan(
			&rowKind,
			&fieldName,
			&observedTypeCodes,
			&eventCount,
			&nullCount,
			&missingCount,
			&rowTotalEvents,
			&invalid,
		); err != nil {
			return FieldCatalogResult{}, classifyQueryError(ctx, fmt.Errorf("scan ClickHouse field catalog result row: %w", err))
		}
		if err := ctx.Err(); err != nil {
			return FieldCatalogResult{}, err
		}

		if rowIndex == 0 {
			if rowKind != 0 || fieldName != "" || len(observedTypeCodes) != 0 ||
				eventCount != 0 || nullCount != 0 || missingCount != 0 || invalid > 1 {
				return FieldCatalogResult{}, invalidFieldCatalogResult("header row is invalid")
			}
			totalEvents = rowTotalEvents
			metadataInvalid = invalid == 1
			rowIndex++
			continue
		}

		if rowKind != 1 {
			return FieldCatalogResult{}, invalidFieldCatalogResult("profile row kind is invalid")
		}
		profileCount++
		if profileCount > query.Spec.MaximumFields+1 {
			return FieldCatalogResult{}, invalidFieldCatalogResult("too many profile rows were returned")
		}
		if invalid != 0 || rowTotalEvents != totalEvents {
			return FieldCatalogResult{}, invalidFieldCatalogResult("profile control values are invalid")
		}
		if metadataInvalid {
			rowIndex++
			continue
		}
		if err := validateFieldCatalogProfile(
			fieldName,
			observedTypeCodes,
			eventCount,
			nullCount,
			missingCount,
			totalEvents,
			previousName,
		); err != nil {
			return FieldCatalogResult{}, err
		}
		previousName = fieldName
		profiles = append(profiles, FieldProfileRow{
			FieldName:     fieldName,
			ObservedTypes: storedValueTypes(observedTypeCodes),
			EventCount:    eventCount,
			NullCount:     nullCount,
			MissingCount:  missingCount,
		})
		rowIndex++
	}
	if err := rows.Err(); err != nil {
		return FieldCatalogResult{}, classifyQueryError(ctx, fmt.Errorf("iterate ClickHouse field catalog results: %w", err))
	}
	if err := ctx.Err(); err != nil {
		return FieldCatalogResult{}, err
	}
	if rowIndex == 0 {
		return FieldCatalogResult{}, invalidFieldCatalogResult("header row is missing")
	}

	rowsClosed = true
	if err := rows.Close(); err != nil {
		return FieldCatalogResult{}, classifyQueryError(ctx, fmt.Errorf("close ClickHouse field catalog result stream: %w", err))
	}
	if err := ctx.Err(); err != nil {
		return FieldCatalogResult{}, err
	}
	if metadataInvalid {
		return FieldCatalogResult{}, ErrFieldMetadataUnavailable
	}
	if profileCount > query.Spec.MaximumFields {
		return FieldCatalogResult{}, fmt.Errorf("%w: ClickHouse field catalog exceeded the field limit", searchjobs.ErrExecutionLimit)
	}
	return FieldCatalogResult{TotalEvents: totalEvents, Fields: profiles}, nil
}

func validateCompiledFieldCatalog(query clickhouse.CompiledFieldCatalog) error {
	if strings.TrimSpace(query.SQL) == "" || query.Spec.MaximumFields == 0 || query.Spec.MaximumFields > clickhouse.MaximumFieldCatalogFields {
		return invalidFieldCatalogResult("compiled field catalog is invalid")
	}
	return nil
}

func settingsForFieldCatalog(base clickhousedriver.Settings, maximumFields uint32) (clickhousedriver.Settings, error) {
	if maximumFields == 0 || maximumFields > clickhouse.MaximumFieldCatalogFields {
		return nil, errors.New("execute ClickHouse field catalog: field limit is invalid")
	}
	if base == nil || base["readonly"] != uint8(2) {
		return nil, errors.New("execute ClickHouse field catalog: executor does not have read-only settings")
	}
	for _, name := range []string{
		"max_execution_time",
		"max_memory_usage",
		"max_rows_to_read",
		"max_bytes_to_read",
		"max_result_rows",
		"max_result_bytes",
		"max_rows_to_group_by",
		"max_threads",
		"max_query_size",
	} {
		value, ok := base[name].(uint64)
		if !ok || value == 0 {
			return nil, fmt.Errorf("execute ClickHouse field catalog: executor setting %s is invalid", name)
		}
	}
	for _, name := range []string{"timeout_overflow_mode", "read_overflow_mode", "result_overflow_mode", "group_by_overflow_mode"} {
		if base[name] != "throw" {
			return nil, fmt.Errorf("execute ClickHouse field catalog: executor setting %s is unsafe", name)
		}
	}

	settings := maps.Clone(base)
	settings["max_result_rows"] = min(base["max_result_rows"].(uint64), uint64(maximumFields)+2)
	settings["max_result_bytes"] = min(base["max_result_bytes"].(uint64), maximumFieldCatalogBytes)
	settings["max_rows_to_group_by"] = min(base["max_rows_to_group_by"].(uint64), uint64(maximumFields)+1)
	return settings, nil
}

func validateFieldCatalogColumns(columns []string, columnTypes []driver.ColumnType) error {
	expectedColumns := []string{
		clickhouse.FieldCatalogRowKindColumn,
		clickhouse.FieldCatalogNameColumn,
		clickhouse.FieldCatalogObservedTypesColumn,
		clickhouse.FieldCatalogEventCountColumn,
		clickhouse.FieldCatalogNullCountColumn,
		clickhouse.FieldCatalogMissingCountColumn,
		clickhouse.FieldCatalogTotalEventsColumn,
		clickhouse.FieldCatalogInvalidColumn,
	}
	if !slices.Equal(columns, expectedColumns) || len(columnTypes) != len(expectedColumns) {
		return invalidFieldCatalogResult("columns do not match the compiled output")
	}
	expectedTypes := []string{"UInt8", "String", "Array(UInt8)", "UInt64", "UInt64", "UInt64", "UInt64", "UInt8"}
	for index, columnType := range columnTypes {
		if isNilDriverValue(columnType) || columnType.Name() != expectedColumns[index] || columnType.Nullable() ||
			columnType.DatabaseTypeName() != expectedTypes[index] {
			return invalidFieldCatalogResult(fmt.Sprintf("column %q has an invalid type", expectedColumns[index]))
		}
	}
	return nil
}

func validateFieldCatalogProfile(
	fieldName string,
	observedTypeCodes []uint8,
	eventCount, nullCount, missingCount, totalEvents uint64,
	previousName string,
) error {
	if fieldName == "" || len(fieldName) > maximumFieldCatalogNameBytes || !utf8.ValidString(fieldName) {
		return invalidFieldCatalogResult("profile field name is invalid")
	}
	if previousName != "" && fieldName <= previousName {
		return invalidFieldCatalogResult("profile field names are not strictly bytewise sorted")
	}
	if nullCount > eventCount || eventCount > totalEvents || missingCount != totalEvents-eventCount {
		return invalidFieldCatalogResult("profile counts are invalid")
	}
	if (eventCount == 0) != (len(observedTypeCodes) == 0) {
		return invalidFieldCatalogResult("profile observed types do not match its event count")
	}
	hasNull := false
	var previousCode uint8
	for index, code := range observedTypeCodes {
		if !eventfields.IsStoredValueType(code) || (index > 0 && code <= previousCode) {
			return invalidFieldCatalogResult("profile observed types are invalid")
		}
		hasNull = hasNull || code == uint8(eventfields.StoredValueTypeNull)
		previousCode = code
	}
	if hasNull != (nullCount > 0) {
		return invalidFieldCatalogResult("profile null type does not match its null count")
	}
	return nil
}

func storedValueTypes(codes []uint8) []eventfields.StoredValueType {
	types := make([]eventfields.StoredValueType, len(codes))
	for index, code := range codes {
		types[index] = eventfields.StoredValueType(code)
	}
	return types
}

func invalidFieldCatalogResult(message string) error {
	return fmt.Errorf("%w: ClickHouse field catalog %s", searchjobs.ErrInvalidResult, message)
}
