package queryexec

import (
	"context"
	"errors"
	"fmt"
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

	rows, err := executor.issueRead(
		ctx,
		"field suggestions",
		query.SQL,
		query.Args,
		settings,
		externalTables,
	)
	if err != nil {
		return FieldSuggestionResult{}, err
	}

	rowsClosed := false
	defer closeReadStream(ctx, rows, "field suggestion", &rowsClosed, &result, &resultErr)

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
	base *validatedExecutorSettings,
	maximumFields uint32,
) (clickhousedriver.Settings, error) {
	if maximumFields == 0 || maximumFields > clickhouse.MaximumFieldSuggestions {
		return nil, errors.New("execute ClickHouse field suggestions: field limit is invalid")
	}
	if base == nil {
		return nil, errors.New(
			"execute ClickHouse field suggestions: executor does not have read-only settings",
		)
	}
	settings := boundedExecutorSettings(
		base,
		settingLimit{
			name:    "max_execution_time",
			maximum: uint64(maximumFieldSuggestionExecutionTime / time.Second),
		},
		settingLimit{name: "max_memory_usage", maximum: maximumFieldSuggestionMemoryBytes},
		settingLimit{name: "max_rows_to_read", maximum: maximumFieldSuggestionRowsToRead},
		settingLimit{name: "max_bytes_to_read", maximum: maximumFieldSuggestionBytesToRead},
		settingLimit{name: "max_result_rows", maximum: uint64(maximumFields) + 2},
		settingLimit{name: "max_result_bytes", maximum: maximumFieldSuggestionResultBytes},
		settingLimit{name: "max_rows_to_group_by", maximum: maximumFieldSuggestionGroups},
		settingLimit{name: "max_threads", maximum: maximumFieldSuggestionThreads},
	)
	return settings, nil
}

func validateFieldSuggestionColumns(
	columns []string,
	columnTypes []driver.ColumnType,
) error {
	contracts := []resultColumnContract{
		{name: clickhouse.FieldSuggestionRowKindColumn, databaseType: "UInt8", scanType: reflect.TypeFor[uint8]()},
		{name: clickhouse.FieldSuggestionNameColumn, databaseType: "String", scanType: reflect.TypeFor[string]()},
		{name: clickhouse.FieldSuggestionInvalidColumn, databaseType: "UInt8", scanType: reflect.TypeFor[uint8]()},
	}
	return validateResultColumns(
		columns,
		columnTypes,
		contracts,
		resultColumnRequireScanType,
		"ClickHouse field suggestions",
	)
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
