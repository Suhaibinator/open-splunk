package queryexec

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	maximumStatsWildcardInventoryExecutionTime = 15 * time.Second
	maximumStatsWildcardInventoryMemoryBytes   = uint64(128 << 20)
	maximumStatsWildcardInventoryRowsToRead    = uint64(5_000_000)
	maximumStatsWildcardInventoryBytesToRead   = uint64(1 << 30)
	maximumStatsWildcardInventoryThreads       = uint64(2)
)

var _ searchjobs.StatsWildcardInventoryExecutor = (*Executor)(nil)

// ExecuteStatsWildcardInventory atomically consumes one compiler-sealed,
// name-only stream and returns opaque planner expansion evidence. No partial
// names escape on cancellation, metadata poison, overflow, or malformed rows.
func (executor *Executor) ExecuteStatsWildcardInventory(
	ctx context.Context,
	query clickhouse.CompiledStatsWildcardInventory,
) (result plan.StatsWildcardExpansion, resultErr error) {
	if ctx == nil {
		return plan.StatsWildcardExpansion{}, errors.New(
			"execute ClickHouse stats wildcard inventory: context is nil",
		)
	}
	if executor == nil || isNilDriverValue(executor.connection) {
		return plan.StatsWildcardExpansion{}, errors.New(
			"execute ClickHouse stats wildcard inventory: executor connection is required",
		)
	}
	if executor.newQueryID == nil {
		return plan.StatsWildcardExpansion{}, errors.New(
			"execute ClickHouse stats wildcard inventory: query ID generator is required",
		)
	}
	detached, ok := query.CloneForExecution()
	if !ok {
		return plan.StatsWildcardExpansion{}, invalidStatsWildcardInventoryResult(
			"compiled execution authority is invalid",
		)
	}
	query = detached
	request := query.Request()
	maximumPairs := request.MaximumPairs()
	if strings.TrimSpace(query.SQL) == "" || request.IsZero() || maximumPairs < 2 ||
		maximumPairs > spl.MaximumStatsMeasures+1 {
		return plan.StatsWildcardExpansion{}, invalidStatsWildcardInventoryResult(
			"compiled query is invalid",
		)
	}

	admittedContext, releaseRead, err := executor.acquireRead(
		ctx,
		query,
		"execute ClickHouse stats wildcard inventory",
	)
	if err != nil {
		return plan.StatsWildcardExpansion{}, err
	}
	defer releaseRead()
	defer func() {
		resultErr = preserveReadCancellationCause(admittedContext, resultErr)
		if resultErr != nil {
			result = plan.StatsWildcardExpansion{}
		}
	}()
	ctx = admittedContext

	settings, err := settingsForStatsWildcardInventory(executor.settings, maximumPairs)
	if err != nil {
		return plan.StatsWildcardExpansion{}, err
	}
	if err := ctx.Err(); err != nil {
		return plan.StatsWildcardExpansion{}, err
	}
	queryID, err := executor.newQueryID()
	if err != nil {
		return plan.StatsWildcardExpansion{}, fmt.Errorf(
			"execute ClickHouse stats wildcard inventory: create query ID: %w",
			err,
		)
	}
	if queryID == "" {
		return plan.StatsWildcardExpansion{}, errors.New(
			"execute ClickHouse stats wildcard inventory: query ID is empty",
		)
	}
	queryContext := clickhousedriver.Context(
		ctx,
		clickhousedriver.WithQueryID(queryID),
		clickhousedriver.WithSettings(settings),
	)
	rows, err := executor.connection.Query(queryContext, query.SQL, query.Args...)
	if err != nil {
		return plan.StatsWildcardExpansion{}, classifyQueryError(
			ctx,
			fmt.Errorf("query ClickHouse stats wildcard inventory: %w", err),
		)
	}
	if isNilDriverValue(rows) {
		return plan.StatsWildcardExpansion{}, invalidStatsWildcardInventoryResult(
			"returned no result stream",
		)
	}

	rowsClosed := false
	defer func() {
		if rowsClosed {
			return
		}
		if closeErr := rows.Close(); resultErr == nil && closeErr != nil {
			result = plan.StatsWildcardExpansion{}
			resultErr = classifyQueryError(
				ctx,
				fmt.Errorf("close ClickHouse stats wildcard inventory result stream: %w", closeErr),
			)
		}
	}()

	if err := validateStatsWildcardInventoryColumns(rows.Columns(), rows.ColumnTypes()); err != nil {
		return plan.StatsWildcardExpansion{}, err
	}
	matches := make([]plan.StatsWildcardInventoryMatch, 0, maximumPairs)
	rowIndex := 0
	metadataInvalid := false
	var previousOrdinal uint8
	previousField := ""
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return plan.StatsWildcardExpansion{}, err
		}
		var ordinal uint8
		var field string
		var invalid uint8
		if err := rows.Scan(&ordinal, &field, &invalid); err != nil {
			return plan.StatsWildcardExpansion{}, classifyQueryError(
				ctx,
				fmt.Errorf("scan ClickHouse stats wildcard inventory result row: %w", err),
			)
		}
		if rowIndex == 0 {
			if ordinal != 0 || field != "" || invalid > 1 {
				return plan.StatsWildcardExpansion{}, invalidStatsWildcardInventoryResult(
					"header row is invalid",
				)
			}
			metadataInvalid = invalid == 1
			rowIndex++
			continue
		}
		if invalid != 0 || field == "" {
			return plan.StatsWildcardExpansion{}, invalidStatsWildcardInventoryResult(
				"match row control values are invalid",
			)
		}
		if rowIndex > int(maximumPairs) {
			return plan.StatsWildcardExpansion{}, invalidStatsWildcardInventoryResult(
				"too many match rows were returned",
			)
		}
		if err := validateStatsWildcardInventoryField(field); err != nil {
			return plan.StatsWildcardExpansion{}, err
		}
		if len(matches) > 0 && (ordinal < previousOrdinal ||
			(ordinal == previousOrdinal && strings.Compare(field, previousField) <= 0)) {
			return plan.StatsWildcardExpansion{}, invalidStatsWildcardInventoryResult(
				"match rows are not strictly ordered",
			)
		}
		previousOrdinal = ordinal
		previousField = field
		matches = append(matches, plan.StatsWildcardInventoryMatch{
			Ordinal: ordinal,
			Field:   strings.Clone(field),
		})
		rowIndex++
	}
	if err := rows.Err(); err != nil {
		return plan.StatsWildcardExpansion{}, classifyQueryError(
			ctx,
			fmt.Errorf("iterate ClickHouse stats wildcard inventory results: %w", err),
		)
	}
	if err := ctx.Err(); err != nil {
		return plan.StatsWildcardExpansion{}, err
	}
	if rowIndex == 0 {
		return plan.StatsWildcardExpansion{}, invalidStatsWildcardInventoryResult(
			"header row is missing",
		)
	}
	rowsClosed = true
	if err := rows.Close(); err != nil {
		return plan.StatsWildcardExpansion{}, classifyQueryError(
			ctx,
			fmt.Errorf("close ClickHouse stats wildcard inventory result stream: %w", err),
		)
	}
	if metadataInvalid {
		return plan.StatsWildcardExpansion{}, ErrFieldMetadataUnavailable
	}
	expansion, err := plan.ValidateStatsWildcardInventory(request, matches)
	if err != nil {
		return plan.StatsWildcardExpansion{}, err
	}
	return expansion.Clone(), nil
}

func validateStatsWildcardInventoryField(field string) error {
	if field == "fields" || len(field) > eventfields.MaximumNormalizedFieldNameBytes ||
		!utf8.ValidString(field) || strings.HasPrefix(strings.ToLower(field), "__os_") {
		return invalidStatsWildcardInventoryResult("field name is invalid")
	}
	if !spl.IsExactUnquotedFieldName(field) && !spl.IsStatsLiteralFieldReference(field) {
		return invalidStatsWildcardInventoryResult("field name is not a valid query field")
	}
	if !eventfields.IsCanonicalSPLField(field) {
		path, err := eventfields.ParseNormalizedSearchFieldPath(field)
		if err == nil && len(path) > 0 && eventfields.IsReservedDynamicRoot(path[0]) {
			return invalidStatsWildcardInventoryResult("field name uses a reserved event root")
		}
	}
	return nil
}

func validateStatsWildcardInventoryColumns(
	columns []string,
	columnTypes []driver.ColumnType,
) error {
	expectedColumns := []string{
		clickhouse.StatsWildcardInventoryOrdinalColumn,
		clickhouse.StatsWildcardInventoryFieldColumn,
		clickhouse.StatsWildcardInventoryInvalidColumn,
	}
	if !slices.Equal(columns, expectedColumns) || len(columnTypes) != len(expectedColumns) {
		return invalidStatsWildcardInventoryResult("columns do not match the compiled output")
	}
	expectedTypes := []struct {
		database string
		scan     reflect.Type
	}{
		{database: "UInt8", scan: reflect.TypeOf(uint8(0))},
		{database: "String", scan: reflect.TypeOf("")},
		{database: "UInt8", scan: reflect.TypeOf(uint8(0))},
	}
	for index, columnType := range columnTypes {
		if isNilDriverValue(columnType) || columnType.Name() != expectedColumns[index] ||
			columnType.Nullable() || columnType.DatabaseTypeName() != expectedTypes[index].database ||
			columnType.ScanType() != expectedTypes[index].scan {
			return invalidStatsWildcardInventoryResult(
				fmt.Sprintf("column %q has an invalid type", expectedColumns[index]),
			)
		}
	}
	return nil
}

func settingsForStatsWildcardInventory(
	base clickhousedriver.Settings,
	maximumPairs uint8,
) (clickhousedriver.Settings, error) {
	if maximumPairs < 2 || maximumPairs > spl.MaximumStatsMeasures+1 {
		return nil, errors.New(
			"execute ClickHouse stats wildcard inventory: pair limit is invalid",
		)
	}
	if base == nil || base["readonly"] != uint8(2) {
		return nil, errors.New(
			"execute ClickHouse stats wildcard inventory: executor does not have read-only settings",
		)
	}
	for _, name := range []string{
		"max_execution_time", "max_memory_usage", "max_rows_to_read",
		"max_bytes_to_read", "max_result_rows", "max_result_bytes",
		"max_rows_to_group_by", "max_threads", "max_query_size", "max_subquery_depth",
	} {
		value, ok := base[name].(uint64)
		if !ok || value == 0 {
			return nil, fmt.Errorf(
				"execute ClickHouse stats wildcard inventory: executor setting %s is invalid",
				name,
			)
		}
	}
	for _, name := range []string{
		"timeout_overflow_mode", "read_overflow_mode", "result_overflow_mode",
		"group_by_overflow_mode",
	} {
		if base[name] != "throw" {
			return nil, fmt.Errorf(
				"execute ClickHouse stats wildcard inventory: executor setting %s is unsafe",
				name,
			)
		}
	}
	if base["enable_materialized_cte"] != uint8(1) ||
		base["short_circuit_function_evaluation"] != "enable" ||
		base["async_insert"] != uint8(0) {
		return nil, errors.New(
			"execute ClickHouse stats wildcard inventory: executor safety settings are invalid",
		)
	}
	settings := maps.Clone(base)
	settings["max_execution_time"] = min(
		base["max_execution_time"].(uint64),
		uint64(maximumStatsWildcardInventoryExecutionTime/time.Second),
	)
	settings["max_memory_usage"] = min(
		base["max_memory_usage"].(uint64), maximumStatsWildcardInventoryMemoryBytes,
	)
	settings["max_rows_to_read"] = min(
		base["max_rows_to_read"].(uint64), maximumStatsWildcardInventoryRowsToRead,
	)
	settings["max_bytes_to_read"] = min(
		base["max_bytes_to_read"].(uint64), maximumStatsWildcardInventoryBytesToRead,
	)
	settings["max_result_rows"] = min(
		base["max_result_rows"].(uint64), uint64(maximumPairs)+1,
	)
	// Include the String offset/length, two scalar cells, and conservative
	// block bookkeeping per row. Legal maximum-length names must not trip the
	// transport guard before semantic validation sees them.
	maximumResultBytes := uint64(maximumPairs+1) *
		uint64(eventfields.MaximumNormalizedFieldNameBytes+64)
	settings["max_result_bytes"] = min(
		base["max_result_bytes"].(uint64), maximumResultBytes,
	)
	// Prefix operators (notably eventstats) and the pre-LIMIT distinct-name
	// relation may legitimately exceed the final 17-pair transport. Preserve
	// the executor's ordinary hard group ceiling; SQL LIMIT and the bounded
	// result stream independently enforce inventory width.
	settings["max_rows_to_group_by"] = base["max_rows_to_group_by"].(uint64)
	settings["max_threads"] = min(
		base["max_threads"].(uint64), maximumStatsWildcardInventoryThreads,
	)
	settings["use_query_cache"] = uint8(0)
	return settings, nil
}

func invalidStatsWildcardInventoryResult(message string) error {
	return fmt.Errorf(
		"%w: ClickHouse stats wildcard inventory %s",
		searchjobs.ErrInvalidResult,
		message,
	)
}
