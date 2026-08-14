package clickhouse

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"strings"
	"unicode/utf8"
	"unsafe"

	"github.com/ClickHouse/clickhouse-go/v2/ext"
)

// compiledLookupExternalTable is the private, compiler-owned transport for one
// exact lookup stage. Lookup rows must not be positional bind arguments:
// clickhouse-go expands those arguments into the SQL text before sending the
// query, while the admitted asset can be larger than the executor's independent
// max_query_size setting. External tables keep query text bounded and send the
// immutable rows as native ClickHouse blocks instead.
type compiledLookupExternalTable struct {
	name           string
	tenantID       string
	definitionName string
	logicalID      string
	logicalVersion uint64
	objectID       string
	version        uint64
	sizeBytes      uint64
	contentSHA256  [sha256.Size]byte
	matchedColumn  string
	columns        []compiledLookupExternalColumn
}

type compiledLookupExternalColumn struct {
	name   string
	values []string
}

func newCompiledLookupExternalTable(
	name string,
	matchedColumn string,
	stage preparedLookupStage,
	columnNames []string,
) (compiledLookupExternalTable, error) {
	return newCompiledLookupExternalTableContext(
		context.Background(),
		name,
		matchedColumn,
		stage,
		columnNames,
	)
}

func newCompiledLookupExternalTableContext(
	ctx context.Context,
	name string,
	matchedColumn string,
	stage preparedLookupStage,
	columnNames []string,
) (compiledLookupExternalTable, error) {
	if ctx == nil {
		return compiledLookupExternalTable{}, errors.New(
			"compile ClickHouse lookup external table: context is nil",
		)
	}
	if err := ctx.Err(); err != nil {
		return compiledLookupExternalTable{}, err
	}
	if len(columnNames) != len(stage.selectedColumns) {
		return compiledLookupExternalTable{}, errors.New(
			"compile ClickHouse lookup external table: selected schema is inconsistent",
		)
	}
	table := compiledLookupExternalTable{
		name:           strings.Clone(name),
		tenantID:       strings.Clone(stage.resolution.tenantID),
		definitionName: strings.Clone(stage.resolution.definitionName),
		logicalID:      strings.Clone(stage.resolution.logicalID),
		logicalVersion: stage.resolution.logicalVersion,
		objectID:       strings.Clone(stage.resolution.objectID),
		version:        stage.resolution.version,
		sizeBytes:      stage.resolution.sizeBytes,
		contentSHA256:  stage.resolution.contentSHA256,
		matchedColumn:  strings.Clone(matchedColumn),
		columns:        make([]compiledLookupExternalColumn, len(columnNames)),
	}
	for index, name := range columnNames {
		table.columns[index] = compiledLookupExternalColumn{
			name: strings.Clone(name),
			// prepareLookupStage created this private immutable column for the
			// external-table transport. Transfer its backing instead of retaining
			// another maximum-envelope String-header matrix.
			values: stage.selectedColumns[index].values,
		}
	}
	if err := validateCompiledLookupExternalTablesContext(
		ctx,
		[]compiledLookupExternalTable{table},
	); err != nil {
		return compiledLookupExternalTable{}, err
	}
	return table, nil
}

func validateCompiledLookupExternalTables(tables []compiledLookupExternalTable) error {
	return validateCompiledLookupExternalTablesContext(context.Background(), tables)
}

func validateCompiledLookupExternalTablesContext(
	ctx context.Context,
	tables []compiledLookupExternalTable,
) error {
	if ctx == nil {
		return errors.New("validate ClickHouse lookup transport: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(tables) > MaximumLookupStagesPerQuery {
		return fmt.Errorf(
			"compiled ClickHouse lookup transport contains more than %d tables",
			MaximumLookupStagesPerQuery,
		)
	}
	seenTables := make(map[string]struct{}, len(tables))
	type logicalVersionKey struct {
		tenantID string
		lookupID string
		version  uint64
	}
	type logicalVersionPhysicalAuthority struct {
		definitionName string
		objectID       string
		version        uint64
		sizeBytes      uint64
		digest         [sha256.Size]byte
	}
	logicalVersions := make(
		map[logicalVersionKey]logicalVersionPhysicalAuthority,
		len(tables),
	)
	var aggregatePayload uint64
	var aggregateCells uint64
	for tableIndex, table := range tables {
		if err := ctx.Err(); err != nil {
			return err
		}
		if table.name == "" || !physicalIdentifier.MatchString(table.name) ||
			table.matchedColumn == "" || !physicalIdentifier.MatchString(table.matchedColumn) ||
			table.tenantID == "" || table.definitionName == "" || table.logicalID == "" ||
			table.logicalVersion == 0 || table.objectID == "" ||
			table.version == 0 || table.sizeBytes == 0 ||
			table.sizeBytes > MaximumLookupAssetBytes ||
			table.contentSHA256 == ([sha256.Size]byte{}) ||
			len(table.columns) == 0 ||
			len(table.columns) > MaximumLookupAssetColumns {
			return fmt.Errorf(
				"compiled ClickHouse lookup transport table %d is invalid",
				tableIndex,
			)
		}
		for _, identity := range []string{
			table.tenantID,
			table.definitionName,
			table.logicalID,
			table.objectID,
		} {
			if !utf8.ValidString(identity) || len(identity) > MaximumLookupCellBytes ||
				strings.IndexByte(identity, 0) >= 0 {
				return fmt.Errorf(
					"compiled ClickHouse lookup transport table %d has invalid authority",
					tableIndex,
				)
			}
		}
		if _, duplicate := seenTables[table.name]; duplicate {
			return errors.New("compiled ClickHouse lookup transport repeats a table name")
		}
		seenTables[table.name] = struct{}{}
		logicalKey := logicalVersionKey{
			tenantID: table.tenantID,
			lookupID: table.logicalID,
			version:  table.logicalVersion,
		}
		physical := logicalVersionPhysicalAuthority{
			definitionName: table.definitionName,
			objectID:       table.objectID,
			version:        table.version,
			sizeBytes:      table.sizeBytes,
			digest:         table.contentSHA256,
		}
		if previous, duplicate := logicalVersions[logicalKey]; duplicate &&
			previous != physical {
			return errors.New(
				"compiled ClickHouse lookup transport has conflicting logical-version authority",
			)
		}
		logicalVersions[logicalKey] = physical
		seenColumns := map[string]struct{}{table.matchedColumn: {}}
		rowCount := len(table.columns[0].values)
		if rowCount > MaximumLookupAssetRows {
			return fmt.Errorf(
				"compiled ClickHouse lookup transport table %d exceeds the row limit",
				tableIndex,
			)
		}
		cells, ok := lookupExternalTableCellCount(rowCount, len(table.columns))
		if !ok || cells > MaximumLookupSelectedCellsPerQuery-aggregateCells {
			return errors.New(
				"compiled ClickHouse lookup transport exceeds the selected-cell work limit",
			)
		}
		aggregateCells += cells
		rowBytes := make([]uint64, rowCount)
		var tablePayload uint64
		for columnIndex, column := range table.columns {
			if column.name == "" || !physicalIdentifier.MatchString(column.name) ||
				len(column.values) != rowCount {
				return fmt.Errorf(
					"compiled ClickHouse lookup transport table %d column %d is invalid",
					tableIndex,
					columnIndex,
				)
			}
			if _, duplicate := seenColumns[column.name]; duplicate {
				return errors.New("compiled ClickHouse lookup transport repeats a column name")
			}
			seenColumns[column.name] = struct{}{}
			for rowIndex, value := range column.values {
				if rowIndex%lookupContextCheckRows == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
				if !utf8.ValidString(value) || len(value) > MaximumLookupCellBytes ||
					strings.IndexByte(value, 0) >= 0 {
					return fmt.Errorf(
						"compiled ClickHouse lookup transport table %d contains an invalid cell",
						tableIndex,
					)
				}
				var ok bool
				rowBytes[rowIndex], ok = checkedLookupBytesAdd(
					rowBytes[rowIndex],
					uint64(len(value)),
				)
				if !ok || rowBytes[rowIndex] > MaximumLookupRowBytes {
					return fmt.Errorf(
						"compiled ClickHouse lookup transport table %d contains an oversized row",
						tableIndex,
					)
				}
				aggregatePayload, ok = checkedLookupBytesAdd(
					aggregatePayload,
					uint64(len(value)),
				)
				if !ok || aggregatePayload >
					uint64(MaximumLookupStagesPerQuery)*MaximumLookupAssetBytes {
					return errors.New(
						"compiled ClickHouse lookup transport exceeds the aggregate byte limit",
					)
				}
				tablePayload, ok = checkedLookupBytesAdd(
					tablePayload,
					uint64(len(value)),
				)
				if !ok || tablePayload > MaximumLookupAssetBytes {
					return errors.New(
						"compiled ClickHouse lookup transport table exceeds the asset byte limit",
					)
				}
			}
		}
	}
	return nil
}

func compiledLookupExternalTablesReferenced(
	sql string,
	tables []compiledLookupExternalTable,
) bool {
	referenced, _ := compiledLookupExternalTablesReferencedContext(
		context.Background(),
		sql,
		tables,
	)
	return referenced
}

func compiledLookupExternalTablesReferencedContext(
	ctx context.Context,
	sql string,
	tables []compiledLookupExternalTable,
) (bool, error) {
	if ctx == nil {
		return false, errors.New("validate ClickHouse lookup references: context is nil")
	}
	if err := validateCompiledLookupExternalTablesContext(ctx, tables); err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return false, contextErr
		}
		return false, nil
	}
	for _, table := range tables {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if !strings.Contains(sql, quoteIdentifier(table.name)) {
			return false, nil
		}
	}
	return true, nil
}

func cloneCompiledLookupExternalTables(
	tables []compiledLookupExternalTable,
) []compiledLookupExternalTable {
	if tables == nil {
		return nil
	}
	cloned := make([]compiledLookupExternalTable, len(tables))
	for tableIndex, table := range tables {
		cloned[tableIndex] = compiledLookupExternalTable{
			name:           strings.Clone(table.name),
			tenantID:       strings.Clone(table.tenantID),
			definitionName: strings.Clone(table.definitionName),
			logicalID:      strings.Clone(table.logicalID),
			logicalVersion: table.logicalVersion,
			objectID:       strings.Clone(table.objectID),
			version:        table.version,
			sizeBytes:      table.sizeBytes,
			contentSHA256:  table.contentSHA256,
			matchedColumn:  strings.Clone(table.matchedColumn),
			columns:        make([]compiledLookupExternalColumn, len(table.columns)),
		}
		if table.columns == nil {
			cloned[tableIndex].columns = nil
		}
		for columnIndex, column := range table.columns {
			cloned[tableIndex].columns[columnIndex] = compiledLookupExternalColumn{
				name: strings.Clone(column.name),
				// Cell slices are compiler-private immutable backing. Public
				// execution values cannot reach them, so detached compiled-value
				// clones need only detach the table/column descriptors.
				values: column.values,
			}
		}
	}
	return cloned
}

func writeCompiledLookupExternalTables(
	writer hash.Hash,
	tables []compiledLookupExternalTable,
) bool {
	written, _ := writeCompiledLookupExternalTablesContext(
		context.Background(),
		writer,
		tables,
	)
	return written
}

func writeCompiledLookupExternalTablesContext(
	ctx context.Context,
	writer hash.Hash,
	tables []compiledLookupExternalTable,
) (bool, error) {
	if ctx == nil {
		return false, errors.New("hash ClickHouse lookup transport: context is nil")
	}
	if writer == nil {
		return false, nil
	}
	if err := validateCompiledLookupExternalTablesContext(ctx, tables); err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return false, contextErr
		}
		return false, nil
	}
	writeBool(writer, tables == nil)
	writeUint64(writer, uint64(len(tables)))
	for _, table := range tables {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		writeTokenPart(writer, table.name)
		writeTokenPart(writer, table.tenantID)
		writeTokenPart(writer, table.definitionName)
		writeTokenPart(writer, table.logicalID)
		writeUint64(writer, table.logicalVersion)
		writeTokenPart(writer, table.objectID)
		writeUint64(writer, table.version)
		writeUint64(writer, table.sizeBytes)
		_, _ = writer.Write(table.contentSHA256[:])
		writeTokenPart(writer, table.matchedColumn)
		writeBool(writer, table.columns == nil)
		writeUint64(writer, uint64(len(table.columns)))
		for _, column := range table.columns {
			writeTokenPart(writer, column.name)
			writeBool(writer, column.values == nil)
			writeUint64(writer, uint64(len(column.values)))
			for rowIndex, value := range column.values {
				if rowIndex%lookupContextCheckRows == 0 {
					if err := ctx.Err(); err != nil {
						return false, err
					}
				}
				writeTokenPart(writer, value)
			}
		}
	}
	return true, nil
}

func retainedCompiledLookupExternalTables(
	total uint64,
	tables []compiledLookupExternalTable,
) (uint64, bool) {
	retained, ok, _ := retainedCompiledLookupExternalTablesContext(
		context.Background(),
		total,
		tables,
	)
	return retained, ok
}

func retainedCompiledLookupExternalTablesContext(
	ctx context.Context,
	total uint64,
	tables []compiledLookupExternalTable,
) (uint64, bool, error) {
	if ctx == nil {
		return 0, false, errors.New("retain ClickHouse lookup transport: context is nil")
	}
	if err := validateCompiledLookupExternalTablesContext(ctx, tables); err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return 0, false, contextErr
		}
		return 0, false, nil
	}
	var ok bool
	total, ok = retainedAdd(
		total,
		uint64(cap(tables))*uint64(unsafe.Sizeof(compiledLookupExternalTable{})),
	)
	if !ok {
		return 0, false, nil
	}
	for _, table := range tables {
		if err := ctx.Err(); err != nil {
			return 0, false, err
		}
		for _, value := range []string{
			table.name,
			table.tenantID,
			table.definitionName,
			table.logicalID,
			table.objectID,
			table.matchedColumn,
		} {
			total, ok = retainedAdd(total, uint64(len(value)))
			if !ok {
				return 0, false, nil
			}
		}
		total, ok = retainedAdd(
			total,
			uint64(cap(table.columns))*uint64(unsafe.Sizeof(compiledLookupExternalColumn{})),
		)
		if !ok {
			return 0, false, nil
		}
		for _, column := range table.columns {
			total, ok = retainedAdd(total, uint64(len(column.name)))
			if !ok {
				return 0, false, nil
			}
			total, ok = retainedAdd(
				total,
				uint64(cap(column.values))*uint64(unsafe.Sizeof("")),
			)
			if !ok {
				return 0, false, nil
			}
			for rowIndex, value := range column.values {
				if rowIndex%lookupContextCheckRows == 0 {
					if err := ctx.Err(); err != nil {
						return 0, false, err
					}
				}
				total, ok = retainedAdd(total, uint64(len(value)))
				if !ok {
					return 0, false, nil
				}
			}
		}
	}
	return total, true, nil
}

func materializeCompiledLookupExternalTables(
	ctx context.Context,
	tables []compiledLookupExternalTable,
) ([]*ext.Table, error) {
	if ctx == nil {
		return nil, errors.New("materialize ClickHouse lookup tables: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(tables) == 0 {
		return nil, nil
	}
	if err := validateCompiledLookupExternalTablesContext(ctx, tables); err != nil {
		return nil, err
	}
	result := make([]*ext.Table, len(tables))
	for tableIndex, compiled := range tables {
		definitions := make([]func(*ext.Table) error, 0, len(compiled.columns)+1)
		definitions = append(definitions, ext.Column(compiled.matchedColumn, "UInt8"))
		for _, column := range compiled.columns {
			definitions = append(definitions, ext.Column(column.name, "String"))
		}
		table, err := ext.NewTable(compiled.name, definitions...)
		if err != nil {
			return nil, err
		}
		row := make([]any, len(compiled.columns)+1)
		row[0] = uint8(1)
		for rowIndex := range compiled.columns[0].values {
			if rowIndex%1024 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			for columnIndex := range compiled.columns {
				row[columnIndex+1] = compiled.columns[columnIndex].values[rowIndex]
			}
			if err := table.Append(row...); err != nil {
				return nil, err
			}
		}
		result[tableIndex] = table
	}
	return result, nil
}

// ExternalTablesForExecution materializes fresh native blocks for the exact
// compiler-sealed lookup payload. The returned tables never alias the retained
// authority. A query without lookup stages returns a successful nil slice so
// same-package diagnostic executors can continue to use hand-built fixtures.
func (compiled CompiledQuery) ExternalTablesForExecution(
	ctx context.Context,
) ([]*ext.Table, error) {
	if ctx == nil {
		return nil, errors.New("materialize ClickHouse lookup tables: context is nil")
	}
	if len(compiled.lookupTables) == 0 {
		return nil, ctx.Err()
	}
	valid, err := compiled.hasValidExecutionSealContext(ctx)
	if err != nil {
		return nil, err
	}
	if !valid {
		return nil, errors.New(
			"materialize ClickHouse lookup tables: execution authority is invalid",
		)
	}
	return materializeCompiledLookupExternalTables(ctx, compiled.lookupTables)
}

func materializeDerivedLookupExternalTables(
	ctx context.Context,
	valid bool,
	authority *derivedExecutionAuthority,
) ([]*ext.Table, error) {
	if ctx == nil {
		return nil, errors.New("materialize ClickHouse lookup tables: context is nil")
	}
	if authority == nil || len(authority.lookupTables) == 0 {
		return nil, ctx.Err()
	}
	if !valid {
		return nil, errors.New(
			"materialize ClickHouse lookup tables: derived execution authority is invalid",
		)
	}
	return materializeCompiledLookupExternalTables(ctx, authority.lookupTables)
}

func (compiled CompiledTimeline) ExternalTablesForExecution(
	ctx context.Context,
) ([]*ext.Table, error) {
	valid, err := compiled.hasValidExecutionSealContext(ctx)
	if err != nil {
		return nil, err
	}
	return materializeDerivedLookupExternalTables(
		ctx,
		valid,
		compiled.executionAuthority,
	)
}

func (compiled CompiledFieldCatalog) ExternalTablesForExecution(
	ctx context.Context,
) ([]*ext.Table, error) {
	valid, err := compiled.hasValidExecutionSealContext(ctx)
	if err != nil {
		return nil, err
	}
	return materializeDerivedLookupExternalTables(
		ctx,
		valid,
		compiled.executionAuthority,
	)
}

func (compiled CompiledFieldSummary) ExternalTablesForExecution(
	ctx context.Context,
) ([]*ext.Table, error) {
	valid, err := compiled.hasValidExecutionSealContext(ctx)
	if err != nil {
		return nil, err
	}
	return materializeDerivedLookupExternalTables(
		ctx,
		valid,
		compiled.executionAuthority,
	)
}

func (compiled CompiledFieldSuggestions) ExternalTablesForExecution(
	ctx context.Context,
) ([]*ext.Table, error) {
	valid, err := compiled.hasValidExecutionSealContext(ctx)
	if err != nil {
		return nil, err
	}
	return materializeDerivedLookupExternalTables(
		ctx,
		valid,
		compiled.executionAuthority,
	)
}

func (compiled CompiledStatsWildcardInventory) ExternalTablesForExecution(
	ctx context.Context,
) ([]*ext.Table, error) {
	if ctx == nil {
		return nil, errors.New("materialize ClickHouse lookup tables: context is nil")
	}
	if len(compiled.lookupTables) == 0 {
		return nil, ctx.Err()
	}
	valid, err := compiled.hasValidExecutionSealContext(ctx)
	if err != nil {
		return nil, err
	}
	if !valid {
		return nil, errors.New(
			"materialize ClickHouse lookup tables: inventory execution authority is invalid",
		)
	}
	return materializeCompiledLookupExternalTables(ctx, compiled.lookupTables)
}
