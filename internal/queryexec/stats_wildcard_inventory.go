package queryexec

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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
	detached, ok, cloneErr := query.CloneForExecutionContext(ctx)
	if cloneErr != nil {
		return plan.StatsWildcardExpansion{}, cloneErr
	}
	if !ok {
		return plan.StatsWildcardExpansion{}, invalidStatsWildcardInventoryResult(
			"compiled execution authority is invalid",
		)
	}
	query = detached
	request, requestOK, requestErr := query.RequestContext(ctx)
	if requestErr != nil {
		return plan.StatsWildcardExpansion{}, requestErr
	}
	maximumPairs := request.MaximumPairs()
	if !requestOK || strings.TrimSpace(query.SQL) == "" || request.IsZero() || maximumPairs < 2 ||
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
	externalTables, err := query.ExternalTablesForExecution(ctx)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return plan.StatsWildcardExpansion{}, ctxErr
		}
		return plan.StatsWildcardExpansion{}, invalidStatsWildcardInventoryResult(
			"compiled lookup transport is invalid",
		)
	}
	rows, err := executor.issueRead(
		ctx,
		"stats wildcard inventory",
		query.SQL,
		query.Args,
		settings,
		externalTables,
	)
	if err != nil {
		return plan.StatsWildcardExpansion{}, err
	}

	rowsClosed := false
	defer closeReadStream(ctx, rows, "stats wildcard inventory", &rowsClosed, &result, &resultErr)

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
	contracts := []resultColumnContract{
		{name: clickhouse.StatsWildcardInventoryOrdinalColumn, databaseType: "UInt8", scanType: reflect.TypeFor[uint8]()},
		{name: clickhouse.StatsWildcardInventoryFieldColumn, databaseType: "String", scanType: reflect.TypeFor[string]()},
		{name: clickhouse.StatsWildcardInventoryInvalidColumn, databaseType: "UInt8", scanType: reflect.TypeFor[uint8]()},
	}
	return validateResultColumns(
		columns,
		columnTypes,
		contracts,
		resultColumnRequireScanType,
		"ClickHouse stats wildcard inventory",
	)
}

func settingsForStatsWildcardInventory(
	base *validatedExecutorSettings,
	maximumPairs uint8,
) (clickhousedriver.Settings, error) {
	if maximumPairs < 2 || maximumPairs > spl.MaximumStatsMeasures+1 {
		return nil, errors.New(
			"execute ClickHouse stats wildcard inventory: pair limit is invalid",
		)
	}
	if base == nil {
		return nil, errors.New(
			"execute ClickHouse stats wildcard inventory: executor does not have read-only settings",
		)
	}
	// Include the String offset/length, two scalar cells, and conservative
	// block bookkeeping per row. Legal maximum-length names must not trip the
	// transport guard before semantic validation sees them.
	maximumResultBytes := uint64(maximumPairs+1) *
		uint64(eventfields.MaximumNormalizedFieldNameBytes+64)
	// max_rows_to_group_by is deliberately absent below: prefix operators
	// (notably eventstats) and the pre-LIMIT distinct-name relation may
	// legitimately exceed the final 17-pair transport, so the executor's
	// ordinary hard group ceiling is inherited unchanged. SQL LIMIT and the
	// bounded result stream independently enforce inventory width.
	settings := boundedExecutorSettings(
		base,
		settingLimit{
			name:    "max_execution_time",
			maximum: uint64(maximumStatsWildcardInventoryExecutionTime / time.Second),
		},
		settingLimit{name: "max_memory_usage", maximum: maximumStatsWildcardInventoryMemoryBytes},
		settingLimit{name: "max_rows_to_read", maximum: maximumStatsWildcardInventoryRowsToRead},
		settingLimit{name: "max_bytes_to_read", maximum: maximumStatsWildcardInventoryBytesToRead},
		settingLimit{name: "max_result_rows", maximum: uint64(maximumPairs) + 1},
		settingLimit{name: "max_result_bytes", maximum: maximumResultBytes},
		settingLimit{name: "max_threads", maximum: maximumStatsWildcardInventoryThreads},
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
