package queryexec

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const (
	maximumFieldCatalogExecutionTime = 15 * time.Second
	// The pinned production knowledge matrix peaks near 331 MiB while parsing,
	// analyzing, and executing a standard-width catalog. Keep fixed allocator
	// headroom without falling back to the executor's much larger ordinary-query
	// default.
	maximumFieldCatalogMemoryBytes = uint64(384 << 20)
	// MaximumFieldCatalogMemoryBytes is the largest sealed per-query catalog
	// memory class. The maximum generated-field matrix peaks near 379 MiB on the
	// pinned server. Production admission uses this exported worst case to bound
	// concurrent catalog work; field summaries have an independent query cap.
	MaximumFieldCatalogMemoryBytes             = uint64(448 << 20)
	maximumStandardFieldCatalogKnowledgeFields = uint32(13)
	maximumFieldCatalogRowsToRead              = uint64(5_000_000)
	maximumFieldCatalogBytesToRead             = uint64(1 << 30)
	maximumFieldCatalogNameBytes               = eventfields.MaximumNormalizedFieldNameBytes
	maximumFieldCatalogBytes                   = uint64(32 << 20)
	maximumFieldCatalogThreads                 = uint64(2)
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
	if executor.readAdmission != nil {
		detached, ok, cloneErr := query.CloneForExecutionContext(ctx)
		if cloneErr != nil {
			return FieldCatalogResult{}, cloneErr
		}
		if !ok {
			return FieldCatalogResult{}, fmt.Errorf(
				"%w: compiled field catalog execution authority is invalid",
				searchjobs.ErrInvalidResult,
			)
		}
		query = detached
	}
	query.Args = slices.Clone(query.Args)
	if err := validateCompiledFieldCatalog(query); err != nil {
		return FieldCatalogResult{}, err
	}
	knowledgeGeneratedFields, sealedResourceEvidence, evidenceErr :=
		query.KnowledgeGeneratedFieldsContext(ctx)
	if evidenceErr != nil {
		return FieldCatalogResult{}, evidenceErr
	}
	if !sealedResourceEvidence {
		// Executor.New always installs read admission and therefore rejects an
		// unsealed query above. The fallback exists only for same-package
		// transport diagnostics that deliberately construct an executor without
		// admission; they exercise the zero-knowledge resource class.
		if executor.readAdmission != nil {
			return FieldCatalogResult{}, invalidFieldCatalogResult(
				"compiled field catalog resource evidence is invalid",
			)
		}
		knowledgeGeneratedFields = 0
	}
	admittedContext, releaseRead, err := executor.acquireRead(ctx, query, "execute ClickHouse field catalog")
	if err != nil {
		return FieldCatalogResult{}, err
	}
	defer releaseRead()
	defer func() {
		resultErr = preserveReadCancellationCause(admittedContext, resultErr)
		if resultErr != nil {
			result = FieldCatalogResult{}
		}
	}()
	ctx = admittedContext
	settings, err := settingsForFieldCatalog(
		executor.baseSettings(),
		query.Spec.MaximumFields,
		knowledgeGeneratedFields,
	)
	if err != nil {
		return FieldCatalogResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return FieldCatalogResult{}, err
	}
	externalTables, err := query.ExternalTablesForExecution(ctx)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return FieldCatalogResult{}, ctxErr
		}
		return FieldCatalogResult{}, invalidFieldCatalogResult(
			"compiled lookup transport is invalid",
		)
	}

	rows, err := executor.issueRead(ctx, "field catalog", query.SQL, query.Args, settings, externalTables)
	if err != nil {
		return FieldCatalogResult{}, err
	}

	rowsClosed := false
	defer closeReadStream(ctx, rows, "field catalog", &rowsClosed, &result, &resultErr)

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

func settingsForFieldCatalog(
	base *validatedExecutorSettings,
	maximumFields uint32,
	knowledgeGeneratedFields uint32,
) (clickhousedriver.Settings, error) {
	if maximumFields == 0 || maximumFields > clickhouse.MaximumFieldCatalogFields {
		return nil, errors.New("execute ClickHouse field catalog: field limit is invalid")
	}
	if knowledgeGeneratedFields > clickhouse.MaximumClickHouseKnowledgeGeneratedFields {
		return nil, errors.New(
			"execute ClickHouse field catalog: generated-field resource evidence is invalid",
		)
	}
	if base == nil {
		return nil, errors.New("execute ClickHouse field catalog: executor does not have read-only settings")
	}
	memoryLimit := maximumFieldCatalogMemoryBytes
	if knowledgeGeneratedFields > maximumStandardFieldCatalogKnowledgeFields {
		memoryLimit = MaximumFieldCatalogMemoryBytes
	}
	settings := boundedExecutorSettings(
		base,
		settingLimit{
			name:    "max_execution_time",
			maximum: uint64(maximumFieldCatalogExecutionTime / time.Second),
		},
		settingLimit{name: "max_memory_usage", maximum: memoryLimit},
		settingLimit{name: "max_rows_to_read", maximum: maximumFieldCatalogRowsToRead},
		settingLimit{name: "max_bytes_to_read", maximum: maximumFieldCatalogBytesToRead},
		settingLimit{name: "max_result_rows", maximum: uint64(maximumFields) + 2},
		settingLimit{name: "max_result_bytes", maximum: maximumFieldCatalogBytes},
		// The prerequisite catalog groups one synthetic/header key plus up to
		// MaximumFields+1 profile keys. The latter extra key is the executor's exact
		// overflow sentinel; neither may be rejected before the atomic result is
		// observed.
		settingLimit{name: "max_rows_to_group_by", maximum: uint64(maximumFields) + 2},
		settingLimit{name: "max_threads", maximum: maximumFieldCatalogThreads},
	)
	settings["use_query_cache"] = uint8(0)
	return settings, nil
}

func validateFieldCatalogColumns(columns []string, columnTypes []driver.ColumnType) error {
	contracts := []resultColumnContract{
		{name: clickhouse.FieldCatalogRowKindColumn, databaseType: "UInt8"},
		{name: clickhouse.FieldCatalogNameColumn, databaseType: "String"},
		{name: clickhouse.FieldCatalogObservedTypesColumn, databaseType: "Array(UInt8)"},
		{name: clickhouse.FieldCatalogEventCountColumn, databaseType: "UInt64"},
		{name: clickhouse.FieldCatalogNullCountColumn, databaseType: "UInt64"},
		{name: clickhouse.FieldCatalogMissingCountColumn, databaseType: "UInt64"},
		{name: clickhouse.FieldCatalogTotalEventsColumn, databaseType: "UInt64"},
		{name: clickhouse.FieldCatalogInvalidColumn, databaseType: "UInt8"},
	}
	return validateResultColumns(columns, columnTypes, contracts, 0, "ClickHouse field catalog")
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
