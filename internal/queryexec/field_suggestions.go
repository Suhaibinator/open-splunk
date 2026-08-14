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
	"unicode"
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
	maximumFieldSuggestionExecutionTime = 15 * time.Second
	maximumFieldSuggestionMemoryBytes   = uint64(128 << 20)
	maximumFieldSuggestionRowsToRead    = uint64(5_000_000)
	maximumFieldSuggestionBytesToRead   = uint64(1 << 30)
	maximumFieldSuggestionResultBytes   = uint64(
		(eventfields.MaximumNormalizedFieldNameBytes + 3) *
			(int(clickhouse.MaximumFieldSuggestions) + 2),
	)
	// The prerequisite single-source graph groups one synthetic header in
	// addition to the catalog-sized dynamic-name domain and its overflow slot.
	maximumFieldSuggestionGroups  = uint64(clickhouse.MaximumFieldCatalogFields) + 2
	maximumFieldSuggestionThreads = uint64(2)
)

// FieldSuggestionResult is a fully validated list of field names sorted by
// ASCII-folded spelling and then exact spelling. Truncated is true only when
// the compiler's one-row overflow sentinel proves that at least one additional
// matching name exists.
type FieldSuggestionResult struct {
	FieldNames []string
	Truncated  bool
}

// ExecuteFieldSuggestions reads a compiler-produced name-only suggestion
// stream. It validates and buffers the complete bounded stream before
// returning, so cancellation or malformed storage results are always atomic.
func (executor *Executor) ExecuteFieldSuggestions(
	ctx context.Context,
	query clickhouse.CompiledFieldSuggestions,
) (result FieldSuggestionResult, resultErr error) {
	if ctx == nil {
		return FieldSuggestionResult{}, errors.New(
			"execute ClickHouse field suggestions: context is nil",
		)
	}
	if executor == nil || isNilDriverValue(executor.connection) {
		return FieldSuggestionResult{}, errors.New(
			"execute ClickHouse field suggestions: executor connection is required",
		)
	}
	if executor.newQueryID == nil {
		return FieldSuggestionResult{}, errors.New(
			"execute ClickHouse field suggestions: query ID generator is required",
		)
	}
	if executor.readAdmission != nil {
		detached, ok, cloneErr := query.CloneForExecutionContext(ctx)
		if cloneErr != nil {
			return FieldSuggestionResult{}, cloneErr
		}
		if !ok {
			return FieldSuggestionResult{}, fmt.Errorf(
				"%w: compiled field suggestions execution authority is invalid",
				searchjobs.ErrInvalidResult,
			)
		}
		query = detached
	}
	query.Args = slices.Clone(query.Args)
	if err := validateCompiledFieldSuggestions(query); err != nil {
		return FieldSuggestionResult{}, err
	}
	admittedContext, releaseRead, err := executor.acquireRead(
		ctx,
		query,
		"execute ClickHouse field suggestions",
	)
	if err != nil {
		return FieldSuggestionResult{}, err
	}
	defer releaseRead()
	defer func() {
		resultErr = preserveReadCancellationCause(admittedContext, resultErr)
		if resultErr != nil {
			result = FieldSuggestionResult{}
		}
	}()
	ctx = admittedContext
	settings, err := settingsForFieldSuggestions(executor.settings, query.Spec.MaximumFields)
	if err != nil {
		return FieldSuggestionResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return FieldSuggestionResult{}, err
	}
	externalTables, err := query.ExternalTablesForExecution(ctx)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return FieldSuggestionResult{}, ctxErr
		}
		return FieldSuggestionResult{}, fmt.Errorf(
			"%w: compiled field suggestions lookup transport is invalid",
			searchjobs.ErrInvalidResult,
		)
	}

	queryID, err := executor.newQueryID()
	if err != nil {
		return FieldSuggestionResult{}, fmt.Errorf(
			"execute ClickHouse field suggestions: create query ID: %w",
			err,
		)
	}
	if queryID == "" {
		return FieldSuggestionResult{}, errors.New(
			"execute ClickHouse field suggestions: query ID is empty",
		)
	}
	if err := ctx.Err(); err != nil {
		return FieldSuggestionResult{}, err
	}
	queryOptions := []clickhousedriver.QueryOption{
		clickhousedriver.WithQueryID(queryID),
		clickhousedriver.WithSettings(settings),
	}
	if len(externalTables) != 0 {
		queryOptions = append(
			queryOptions,
			clickhousedriver.WithExternalTable(externalTables...),
		)
	}
	queryContext := clickhousedriver.Context(ctx, queryOptions...)
	rows, err := executor.connection.Query(queryContext, query.SQL, query.Args...)
	if err != nil {
		return FieldSuggestionResult{}, classifyQueryError(
			ctx,
			fmt.Errorf("query ClickHouse field suggestions: %w", err),
		)
	}
	if isNilDriverValue(rows) {
		return FieldSuggestionResult{}, fmt.Errorf(
			"%w: ClickHouse field suggestions returned no result stream",
			searchjobs.ErrInvalidResult,
		)
	}

	rowsClosed := false
	defer func() {
		if rowsClosed {
			return
		}
		if closeErr := rows.Close(); resultErr == nil && closeErr != nil {
			result = FieldSuggestionResult{}
			resultErr = classifyQueryError(
				ctx,
				fmt.Errorf("close ClickHouse field suggestion result stream: %w", closeErr),
			)
		}
	}()

	if err := ctx.Err(); err != nil {
		return FieldSuggestionResult{}, err
	}
	if err := validateFieldSuggestionColumns(rows.Columns(), rows.ColumnTypes()); err != nil {
		return FieldSuggestionResult{}, err
	}

	names := make([]string, 0, query.Spec.MaximumFields)
	var previousName string
	var metadataInvalid bool
	rowIndex := uint32(0)
	nameCount := uint32(0)
	for {
		if err := ctx.Err(); err != nil {
			return FieldSuggestionResult{}, err
		}
		if !rows.Next() {
			break
		}
		if err := ctx.Err(); err != nil {
			return FieldSuggestionResult{}, err
		}

		var rowKind uint8
		var fieldName string
		var invalid uint8
		if err := rows.Scan(&rowKind, &fieldName, &invalid); err != nil {
			return FieldSuggestionResult{}, classifyQueryError(
				ctx,
				fmt.Errorf("scan ClickHouse field suggestion result row: %w", err),
			)
		}
		if err := ctx.Err(); err != nil {
			return FieldSuggestionResult{}, err
		}

		if rowIndex == 0 {
			if rowKind != 0 || fieldName != "" || invalid > 1 {
				return FieldSuggestionResult{}, invalidFieldSuggestionResult(
					"header row is invalid",
				)
			}
			metadataInvalid = invalid == 1
			rowIndex++
			continue
		}

		if rowKind != 1 || invalid != 0 {
			return FieldSuggestionResult{}, invalidFieldSuggestionResult(
				"name row control values are invalid",
			)
		}
		nameCount++
		if nameCount > query.Spec.MaximumFields+1 {
			return FieldSuggestionResult{}, invalidFieldSuggestionResult(
				"too many name rows were returned",
			)
		}
		if metadataInvalid {
			rowIndex++
			continue
		}
		if err := validateFieldSuggestionName(
			fieldName,
			query.Spec.Prefix,
			previousName,
		); err != nil {
			return FieldSuggestionResult{}, err
		}
		previousName = fieldName
		if nameCount <= query.Spec.MaximumFields {
			names = append(names, fieldName)
		}
		rowIndex++
	}
	if err := rows.Err(); err != nil {
		return FieldSuggestionResult{}, classifyQueryError(
			ctx,
			fmt.Errorf("iterate ClickHouse field suggestion results: %w", err),
		)
	}
	if err := ctx.Err(); err != nil {
		return FieldSuggestionResult{}, err
	}
	if rowIndex == 0 {
		return FieldSuggestionResult{}, invalidFieldSuggestionResult("header row is missing")
	}

	rowsClosed = true
	if err := rows.Close(); err != nil {
		return FieldSuggestionResult{}, classifyQueryError(
			ctx,
			fmt.Errorf("close ClickHouse field suggestion result stream: %w", err),
		)
	}
	if err := ctx.Err(); err != nil {
		return FieldSuggestionResult{}, err
	}
	if metadataInvalid {
		return FieldSuggestionResult{}, ErrFieldMetadataUnavailable
	}
	return FieldSuggestionResult{
		FieldNames: names,
		Truncated:  nameCount > query.Spec.MaximumFields,
	}, nil
}

func validateCompiledFieldSuggestions(query clickhouse.CompiledFieldSuggestions) error {
	if strings.TrimSpace(query.SQL) == "" ||
		query.Spec.MaximumFields == 0 ||
		query.Spec.MaximumFields > clickhouse.MaximumFieldSuggestions ||
		len(query.Spec.Prefix) > eventfields.MaximumNormalizedFieldNameBytes ||
		!utf8.ValidString(query.Spec.Prefix) ||
		fieldSuggestionContainsControl(query.Spec.Prefix) {
		return invalidFieldSuggestionResult("compiled query is invalid")
	}
	return nil
}

func fieldSuggestionContainsControl(value string) bool {
	for _, character := range value {
		if unicode.IsControl(character) {
			return true
		}
	}
	return false
}

func settingsForFieldSuggestions(
	base clickhousedriver.Settings,
	maximumFields uint32,
) (clickhousedriver.Settings, error) {
	if maximumFields == 0 || maximumFields > clickhouse.MaximumFieldSuggestions {
		return nil, errors.New("execute ClickHouse field suggestions: field limit is invalid")
	}
	if base == nil || base["readonly"] != uint8(2) {
		return nil, errors.New(
			"execute ClickHouse field suggestions: executor does not have read-only settings",
		)
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
		"max_subquery_depth",
	} {
		value, ok := base[name].(uint64)
		if !ok || value == 0 {
			return nil, fmt.Errorf(
				"execute ClickHouse field suggestions: executor setting %s is invalid",
				name,
			)
		}
	}
	for _, name := range []string{
		"timeout_overflow_mode",
		"read_overflow_mode",
		"result_overflow_mode",
		"group_by_overflow_mode",
	} {
		if base[name] != "throw" {
			return nil, fmt.Errorf(
				"execute ClickHouse field suggestions: executor setting %s is unsafe",
				name,
			)
		}
	}
	if base["enable_materialized_cte"] != uint8(1) {
		return nil, errors.New(
			"execute ClickHouse field suggestions: materialized CTEs are not enabled",
		)
	}
	if base["short_circuit_function_evaluation"] != "enable" {
		return nil, errors.New(
			"execute ClickHouse field suggestions: short-circuit evaluation is not enabled",
		)
	}
	if base["async_insert"] != uint8(0) {
		return nil, errors.New(
			"execute ClickHouse field suggestions: asynchronous inserts must remain disabled",
		)
	}

	settings := maps.Clone(base)
	settings["max_execution_time"] = min(
		base["max_execution_time"].(uint64),
		uint64(maximumFieldSuggestionExecutionTime/time.Second),
	)
	settings["max_memory_usage"] = min(
		base["max_memory_usage"].(uint64),
		maximumFieldSuggestionMemoryBytes,
	)
	settings["max_rows_to_read"] = min(
		base["max_rows_to_read"].(uint64),
		maximumFieldSuggestionRowsToRead,
	)
	settings["max_bytes_to_read"] = min(
		base["max_bytes_to_read"].(uint64),
		maximumFieldSuggestionBytesToRead,
	)
	settings["max_result_rows"] = min(
		base["max_result_rows"].(uint64),
		uint64(maximumFields)+2,
	)
	settings["max_result_bytes"] = min(
		base["max_result_bytes"].(uint64),
		maximumFieldSuggestionResultBytes,
	)
	settings["max_rows_to_group_by"] = min(
		base["max_rows_to_group_by"].(uint64),
		maximumFieldSuggestionGroups,
	)
	settings["max_threads"] = min(
		base["max_threads"].(uint64),
		maximumFieldSuggestionThreads,
	)
	return settings, nil
}

func validateFieldSuggestionColumns(
	columns []string,
	columnTypes []driver.ColumnType,
) error {
	expectedColumns := []string{
		clickhouse.FieldSuggestionRowKindColumn,
		clickhouse.FieldSuggestionNameColumn,
		clickhouse.FieldSuggestionInvalidColumn,
	}
	if !slices.Equal(columns, expectedColumns) || len(columnTypes) != len(expectedColumns) {
		return invalidFieldSuggestionResult("columns do not match the compiled output")
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
		if isNilDriverValue(columnType) ||
			columnType.Name() != expectedColumns[index] ||
			columnType.Nullable() ||
			columnType.DatabaseTypeName() != expectedTypes[index].database ||
			columnType.ScanType() != expectedTypes[index].scan {
			return invalidFieldSuggestionResult(
				fmt.Sprintf("column %q has an invalid type", expectedColumns[index]),
			)
		}
	}
	return nil
}

func validateFieldSuggestionName(
	fieldName,
	prefix,
	previousName string,
) error {
	resolved, err := plan.ResolveField(fieldName, spl.Range{})
	if err != nil || resolved.Name != fieldName {
		return invalidFieldSuggestionResult("field name is not a valid query field")
	}
	if !fieldSuggestionEditorName(fieldName) {
		return invalidFieldSuggestionResult(
			"field name cannot be represented as an SPL editor token",
		)
	}
	if !strings.HasPrefix(fieldName, prefix) {
		return invalidFieldSuggestionResult(
			"field name does not match the compiled prefix",
		)
	}
	if previousName != "" && !fieldSuggestionNameBefore(previousName, fieldName) {
		return invalidFieldSuggestionResult(
			"field names are not strictly sorted by ASCII-folded and exact spelling",
		)
	}
	return nil
}

func fieldSuggestionEditorName(name string) bool {
	if name == "" ||
		name[0] == '+' ||
		name[0] == '-' ||
		fieldSuggestionHasPrivatePrefix(name) ||
		strings.ContainsAny(name, "|(),=!<>\"*") {
		return false
	}
	for _, character := range name {
		if unicode.IsSpace(character) || !unicode.IsGraphic(character) {
			return false
		}
	}
	return true
}

func fieldSuggestionHasPrivatePrefix(name string) bool {
	const privatePrefix = "__os_"
	if len(name) < len(privatePrefix) {
		return false
	}
	for index := range len(privatePrefix) {
		character := name[index]
		if character >= 'A' && character <= 'Z' {
			character += 'a' - 'A'
		}
		if character != privatePrefix[index] {
			return false
		}
	}
	return true
}

func fieldSuggestionNameBefore(left, right string) bool {
	switch fieldSuggestionFoldedCompare(left, right) {
	case -1:
		return true
	case 1:
		return false
	default:
		return left < right
	}
}

func fieldSuggestionFoldedCompare(left, right string) int {
	maximum := min(len(left), len(right))
	for index := range maximum {
		leftByte := left[index]
		if leftByte >= 'A' && leftByte <= 'Z' {
			leftByte += 'a' - 'A'
		}
		rightByte := right[index]
		if rightByte >= 'A' && rightByte <= 'Z' {
			rightByte += 'a' - 'A'
		}
		switch {
		case leftByte < rightByte:
			return -1
		case leftByte > rightByte:
			return 1
		}
	}
	switch {
	case len(left) < len(right):
		return -1
	case len(left) > len(right):
		return 1
	default:
		return 0
	}
}

func invalidFieldSuggestionResult(message string) error {
	return fmt.Errorf(
		"%w: ClickHouse field suggestions %s",
		searchjobs.ErrInvalidResult,
		message,
	)
}
